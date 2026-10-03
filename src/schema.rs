//! The registry, as data: [`DomainSchema`] and the JSON Schema export.
//!
//! A [`Domain`] already knows the exact shape of every resource
//! it will serve — that is what [`try_new`](crate::Domain::try_new) validated.
//! This module hands that knowledge back as an inspectable, serializable value,
//! so the things a resource-oriented framework is *supposed* to derive (an API
//! description, an admin UI, a client type, a doc page, a fixture generator) can
//! be generated from the same declaration the executor runs.
//!
//! It is a **pure projection of the registry**: no I/O, no clock, no data layer,
//! no policy evaluation. [`Domain::schema`](crate::Domain::schema) walks the
//! registered resources and returns; calling it twice returns the same thing.
//!
//! ```
//! use ash_domain::{erase, Domain, Record, Resource};
//! # use ash_domain::action::ActionDef;
//! # use ash_domain::attribute::Attribute;
//! struct Note;
//! impl Resource for Note {
//!     const NAME: &'static str = "note";
//!     type Data = Record;
//!     fn attributes() -> Vec<Attribute> {
//!         vec![Attribute::scalar::<String>("id"), Attribute::scalar::<i64>("size")]
//!     }
//!     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
//! }
//!
//! let domain = Domain::builder().register::<Note>().permissive().build();
//! let schema = domain.schema();
//!
//! let note = schema.resource("note").expect("registered");
//! assert_eq!(note.primary_key, "id");
//! assert_eq!(note.attributes.len(), 2);
//!
//! // …and the same thing as a JSON Schema document, for anything that speaks it.
//! let json = schema.to_json_schema();
//! assert!(json["$defs"]["note"]["properties"]["size"]["type"] == "integer");
//! ```
//!
//! ## What it does *not* tell you
//!
//! The schema is the **declared** shape, not a per-caller view. Attribute
//! redaction is decided per row, per actor, at read time — a policy can hide
//! `salary` on rows the caller doesn't own — so no static description can say
//! what a given caller will see. [`AttributeSchema::attribute_policy`] reports
//! the one fact that *is* static: whether a policy is scoped to that attribute
//! by name. Broader scopes (a resource- or domain-scoped policy) can redact it
//! too, and are not counted there.

use std::any::TypeId;
use std::collections::BTreeSet;

use serde::Serialize;

use crate::action::ActionKind;
use crate::aggregate::{Aggregate, AggregateKind};
use crate::attribute::{AttrType, Attribute};
use crate::domain::Domain;
use crate::policy::Scope;
use crate::resource::{Cardinality, Relationship, TenantStrategy};
use crate::value::Value;

impl Domain {
    /// Describe the whole registered domain as data — see [`DomainSchema`].
    ///
    /// Pure: it reads the registry and the policy scopes, and touches nothing
    /// else.
    pub fn schema(&self) -> DomainSchema {
        // Attribute-scoped policy targets, gathered once: the per-attribute
        // lookup below is a set probe rather than a scan of every policy.
        let gated: BTreeSet<(&str, &str)> = self
            .policies()
            .policies()
            .iter()
            .filter_map(|scoped| match &scoped.scope {
                Scope::Attribute {
                    resource,
                    attribute,
                } => Some((resource.as_str(), attribute.as_str())),
                _ => None,
            })
            .collect();

        let mut resources: Vec<ResourceSchema> = self
            .resource_names()
            .into_iter()
            .filter_map(|name| self.resource(name))
            .map(|resource| {
                let name = resource.name().to_string();
                let pk = resource.primary_key();
                ResourceSchema {
                    attributes: resource
                        .attributes()
                        .iter()
                        .map(|attr| AttributeSchema::of(attr, &pk, &name, &gated))
                        .collect(),
                    relationships: resource
                        .relationships()
                        .iter()
                        .map(RelationshipSchema::of)
                        .collect(),
                    aggregates: resource
                        .aggregates()
                        .iter()
                        .map(|agg| AggregateSchema::of(agg, &name, &gated))
                        .collect(),
                    computed: resource.computed().iter().map(|c| c.name.clone()).collect(),
                    actions: resource
                        .actions()
                        .iter()
                        .map(|def| ActionSchema {
                            name: def.name.clone(),
                            kind: kind_name(def.kind),
                        })
                        .collect(),
                    tenant: resource.tenant().as_ref().map(TenantSchema::of),
                    storage_name: resource.storage_name(),
                    version_attribute: resource.version_attribute(),
                    primary_key: pk,
                    name,
                }
            })
            .collect();
        // `resource_names` is already sorted; keep that guarantee explicit here
        // so the export is byte-stable regardless of registration order.
        resources.sort_by(|a, b| a.name.cmp(&b.name));
        DomainSchema { resources }
    }
}

/// Every registered resource, as data. Built by [`Domain::schema`].
#[derive(Debug, Clone, Serialize)]
pub struct DomainSchema {
    /// The registered resources, sorted by name — so two runs of the same
    /// config export byte-identical schemas.
    pub resources: Vec<ResourceSchema>,
}

impl DomainSchema {
    /// The schema for one resource by name, or `None` if it isn't registered.
    pub fn resource(&self, name: &str) -> Option<&ResourceSchema> {
        self.resources.iter().find(|r| r.name == name)
    }

    /// Render as a [JSON Schema](https://json-schema.org) 2020-12 document: one
    /// `$defs` entry per resource, each an `object` whose `properties` are the
    /// resource's attributes.
    ///
    /// This is the interop half — what a codegen, a validator, or an API
    /// description consumes. Relationships, actions, aggregates and computed
    /// fields have no JSON Schema equivalent, so they ride along under the
    /// `x-ash` extension key on each definition rather than being dropped.
    ///
    /// An attribute whose Rust type is not one of the mapped primitives
    /// ([`SchemaType::Opaque`]) is emitted with **no** `type` constraint and its
    /// Rust type name under `x-ash-rust-type`: a JSON Schema that admits
    /// anything is honest about an unknown shape, where guessing `string` would
    /// not be.
    pub fn to_json_schema(&self) -> serde_json::Value {
        let defs: serde_json::Map<String, serde_json::Value> = self
            .resources
            .iter()
            .map(|r| (r.name.clone(), r.to_json_schema()))
            .collect();
        serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": defs,
        })
    }
}

/// One resource: its attributes, relationships, derived values, and actions.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceSchema {
    /// The resource's registered name.
    pub name: String,
    /// The name a data layer stores it under (its `table` override, else `name`).
    pub storage_name: String,
    /// The primary-key attribute's name.
    pub primary_key: String,
    /// The row-version attribute's name, or `None` when the resource has no
    /// optimistic-concurrency control. A generated client can use this to know
    /// that updates must round-trip the version they read. See
    /// [`Resource::version_attribute`](crate::Resource::version_attribute).
    pub version_attribute: Option<String>,
    /// How the resource is tenant-scoped, or `None` for a global resource.
    pub tenant: Option<TenantSchema>,
    /// The declared attributes, in declaration order.
    pub attributes: Vec<AttributeSchema>,
    /// The declared relationships.
    pub relationships: Vec<RelationshipSchema>,
    /// The declared aggregates.
    pub aggregates: Vec<AggregateSchema>,
    /// The names of the declared computed fields. A computed field's type is a
    /// consumer's `Computer` returning a [`Value`], so there is no declared type
    /// to report — only the name it is requested under.
    pub computed: Vec<String>,
    /// The declared actions.
    pub actions: Vec<ActionSchema>,
}

impl ResourceSchema {
    /// This resource as one JSON Schema `object` definition. See
    /// [`DomainSchema::to_json_schema`].
    pub fn to_json_schema(&self) -> serde_json::Value {
        let properties: serde_json::Map<String, serde_json::Value> = self
            .attributes
            .iter()
            .map(|a| (a.name.clone(), a.to_json_schema()))
            .collect();
        serde_json::json!({
            "type": "object",
            "title": self.name,
            "properties": properties,
            "x-ash": {
                "primary_key": self.primary_key,
                "version_attribute": self.version_attribute,
                "storage_name": self.storage_name,
                "tenant": self.tenant,
                "relationships": self.relationships,
                "aggregates": self.aggregates,
                "computed": self.computed,
                "actions": self.actions,
            },
        })
    }
}

/// How a resource is scoped to a tenant.
#[derive(Debug, Clone, Serialize)]
pub struct TenantSchema {
    /// `"attribute"` (a discriminator column) or `"layer"` (the data layer
    /// partitions).
    pub strategy: &'static str,
    /// The discriminator attribute, for the `"attribute"` strategy.
    pub attribute: Option<String>,
}

impl TenantSchema {
    fn of(strategy: &TenantStrategy) -> Self {
        match strategy {
            TenantStrategy::Attribute(attribute) => Self {
                strategy: "attribute",
                attribute: Some(attribute.clone()),
            },
            TenantStrategy::Layer => Self {
                strategy: "layer",
                attribute: None,
            },
        }
    }
}

/// One attribute: its name, its type, and the two static facts about it.
#[derive(Debug, Clone, Serialize)]
pub struct AttributeSchema {
    /// The attribute name.
    pub name: String,
    /// Its declared type.
    pub ty: SchemaType,
    /// The default applied on create when the value is absent.
    pub default: Option<Value>,
    /// Whether this is the resource's primary key.
    pub primary_key: bool,
    /// Whether a policy is scoped to this attribute **by name**
    /// ([`Scope::Attribute`]) — a hint that reads of it may be redacted.
    ///
    /// It is not a promise in either direction: a broader resource- or
    /// domain-scoped policy can redact an attribute this flag leaves `false`,
    /// and an attribute-scoped policy may well allow the read. Redaction is
    /// per-row and per-actor; only the *scoping* is static.
    pub attribute_policy: bool,
}

impl AttributeSchema {
    fn of(attr: &Attribute, pk: &str, resource: &str, gated: &BTreeSet<(&str, &str)>) -> Self {
        Self {
            name: attr.name.clone(),
            ty: SchemaType::of(&attr.ty),
            default: attr.default.clone(),
            primary_key: attr.name == pk,
            attribute_policy: gated.contains(&(resource, attr.name.as_str())),
        }
    }

    fn to_json_schema(&self) -> serde_json::Value {
        let mut out = self.ty.to_json_schema();
        if let (Some(object), Some(default)) = (out.as_object_mut(), self.default.as_ref()) {
            object.insert("default".into(), value_to_json(default));
        }
        out
    }
}

/// An attribute's type, mapped off its Rust type where the mapping is exact.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaType {
    /// `bool`.
    Boolean,
    /// Any of the Rust integer types — all stored as an `i64`
    /// ([`Value::Int`]).
    Integer,
    /// `f32` / `f64`.
    Number,
    /// `String`.
    String,
    /// `Bytes` / `Vec<u8>`.
    Bytes,
    /// A reference to another resource by name — the storage side of a
    /// relationship.
    Reference {
        /// The referenced resource's name.
        resource: String,
    },
    /// An embedded resource, carrying its own attributes inline.
    Embed {
        /// The embedded resource's attributes.
        fields: Vec<AttributeSchema>,
    },
    /// A repeated embedded resource.
    EmbedList {
        /// The embedded resource's attributes.
        fields: Vec<AttributeSchema>,
    },
    /// A scalar whose Rust type is none of the above. Reported by name rather
    /// than guessed at — the core does not know how a consumer's type reaches
    /// storage.
    Opaque {
        /// `std::any::type_name` of the declared Rust type.
        rust_type: String,
    },
}

impl SchemaType {
    fn of(ty: &AttrType) -> Self {
        match ty {
            AttrType::Scalar { type_id, name } => scalar_type(*type_id, name),
            AttrType::_Ref(resource) => SchemaType::Reference {
                resource: resource.clone(),
            },
            AttrType::_Embed(fields) => SchemaType::Embed {
                fields: embedded(fields),
            },
            AttrType::_EmbedList(fields) => SchemaType::EmbedList {
                fields: embedded(fields),
            },
        }
    }

    fn to_json_schema(&self) -> serde_json::Value {
        match self {
            SchemaType::Boolean => serde_json::json!({ "type": "boolean" }),
            SchemaType::Integer => serde_json::json!({ "type": "integer" }),
            SchemaType::Number => serde_json::json!({ "type": "number" }),
            SchemaType::String => serde_json::json!({ "type": "string" }),
            // No JSON type for bytes; the conventional encoding is a base64
            // string, which is what a layer that serialises to JSON produces.
            SchemaType::Bytes => {
                serde_json::json!({ "type": "string", "contentEncoding": "base64" })
            }
            SchemaType::Reference { resource } => serde_json::json!({
                "$ref": format!("#/$defs/{resource}"),
            }),
            SchemaType::Embed { fields } => embed_json(fields, false),
            SchemaType::EmbedList { fields } => embed_json(fields, true),
            // Deliberately unconstrained: see `DomainSchema::to_json_schema`.
            SchemaType::Opaque { rust_type } => serde_json::json!({
                "x-ash-rust-type": rust_type,
            }),
        }
    }
}

/// Map a scalar's Rust [`TypeId`] onto a schema type, exactly — anything not in
/// this table is [`Opaque`](SchemaType::Opaque) rather than a guess.
fn scalar_type(type_id: TypeId, name: &str) -> SchemaType {
    if type_id == TypeId::of::<bool>() {
        SchemaType::Boolean
    } else if is_integer(type_id) {
        SchemaType::Integer
    } else if type_id == TypeId::of::<f32>() || type_id == TypeId::of::<f64>() {
        SchemaType::Number
    } else if type_id == TypeId::of::<String>() {
        SchemaType::String
    } else if type_id == TypeId::of::<bytes::Bytes>() || type_id == TypeId::of::<Vec<u8>>() {
        SchemaType::Bytes
    } else {
        SchemaType::Opaque {
            rust_type: name.to_string(),
        }
    }
}

/// Every Rust integer type the record conversion accepts — all of which land in
/// storage as an `i64`.
fn is_integer(type_id: TypeId) -> bool {
    [
        TypeId::of::<i8>(),
        TypeId::of::<i16>(),
        TypeId::of::<i32>(),
        TypeId::of::<i64>(),
        TypeId::of::<isize>(),
        TypeId::of::<u8>(),
        TypeId::of::<u16>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<usize>(),
    ]
    .contains(&type_id)
}

/// An embedded resource's fields. They are nested inside one column, so they
/// have no primary key of their own and no attribute scope can name them.
fn embedded(fields: &[Attribute]) -> Vec<AttributeSchema> {
    fields
        .iter()
        .map(|f| AttributeSchema {
            name: f.name.clone(),
            ty: SchemaType::of(&f.ty),
            default: f.default.clone(),
            primary_key: false,
            attribute_policy: false,
        })
        .collect()
}

/// A [`Value`] in its **plain JSON** form, for the JSON Schema export.
///
/// [`Value`]'s own `Serialize` is the externally-tagged Rust form
/// (`{"Int": 0}`) that the rest of the crate round-trips through; a JSON Schema
/// `default` has to be the value itself. Bytes are base64, matching the
/// `contentEncoding` declared for [`SchemaType::Bytes`].
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::from(*i),
        // A non-finite float has no JSON literal; null is the honest rendering.
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(base64(b)),
        // Milliseconds since the epoch — the value as stored, not a formatted date.
        Value::Timestamp(ms) => serde_json::Value::from(*ms),
        Value::Map(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
        Value::List(list) => serde_json::Value::Array(list.iter().map(value_to_json).collect()),
    }
}

/// Standard base64 (RFC 4648, padded). Small enough to spell out; a dependency
/// for one default value would not earn its place.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        // Pack the chunk into 24 bits, missing bytes reading as zero.
        let b = |i: usize| u32::from(chunk.get(i).copied().unwrap_or(0));
        let packed = (b(0) << 16) | (b(1) << 8) | b(2);
        for i in 0..4 {
            // Every 6-bit group is a character, except those that lie entirely
            // past the input, which pad.
            if i <= chunk.len() {
                let index = (packed >> (18 - 6 * i)) & 0b11_1111;
                out.push(ALPHABET[index as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn embed_json(fields: &[AttributeSchema], repeated: bool) -> serde_json::Value {
    let properties: serde_json::Map<String, serde_json::Value> = fields
        .iter()
        .map(|f| (f.name.clone(), f.to_json_schema()))
        .collect();
    let object = serde_json::json!({ "type": "object", "properties": properties });
    if repeated {
        serde_json::json!({ "type": "array", "items": object })
    } else {
        object
    }
}

/// One relationship, as declared.
#[derive(Debug, Clone, Serialize)]
pub struct RelationshipSchema {
    /// The relationship name (what `load` asks for).
    pub name: String,
    /// The destination resource's name.
    pub destination: String,
    /// `"belongs_to"` / `"has_one"` / `"has_many"` / `"many_to_many"`.
    pub cardinality: &'static str,
    /// The attribute on this resource used to match.
    pub source_attribute: String,
    /// The attribute on the destination resource used to match.
    pub destination_attribute: String,
    /// The join resource, for a many-to-many.
    pub through: Option<ThroughSchema>,
}

impl RelationshipSchema {
    fn of(rel: &Relationship) -> Self {
        Self {
            name: rel.name.clone(),
            destination: rel.destination.clone(),
            cardinality: match rel.cardinality {
                Cardinality::BelongsTo => "belongs_to",
                Cardinality::HasOne => "has_one",
                Cardinality::HasMany => "has_many",
                Cardinality::ManyToMany => "many_to_many",
            },
            source_attribute: rel.source_attribute.clone(),
            destination_attribute: rel.destination_attribute.clone(),
            through: rel.through.as_ref().map(|t| ThroughSchema {
                resource: t.resource.clone(),
                source_attribute: t.source_attribute.clone(),
                destination_attribute: t.destination_attribute.clone(),
            }),
        }
    }
}

/// The join resource of a many-to-many relationship.
#[derive(Debug, Clone, Serialize)]
pub struct ThroughSchema {
    /// The join resource's name.
    pub resource: String,
    /// The join attribute pointing at the source.
    pub source_attribute: String,
    /// The join attribute pointing at the destination.
    pub destination_attribute: String,
}

/// One declared aggregate.
#[derive(Debug, Clone, Serialize)]
pub struct AggregateSchema {
    /// The name it is requested and returned under.
    pub name: String,
    /// The relationship whose records are rolled up.
    pub relationship: String,
    /// `"count"` / `"sum"` / `"min"` / `"max"` / `"exists"`.
    pub kind: &'static str,
    /// The related-resource field rolled up, where the kind takes one.
    pub field: Option<String>,
    /// Whether a policy is scoped to this aggregate's name — a derived output is
    /// gated exactly like an attribute. Same caveats as
    /// [`AttributeSchema::attribute_policy`].
    pub attribute_policy: bool,
}

impl AggregateSchema {
    fn of(agg: &Aggregate, resource: &str, gated: &BTreeSet<(&str, &str)>) -> Self {
        Self {
            name: agg.name.clone(),
            relationship: agg.relationship.clone(),
            kind: match agg.kind {
                AggregateKind::Count => "count",
                AggregateKind::Sum => "sum",
                AggregateKind::Min => "min",
                AggregateKind::Max => "max",
                AggregateKind::Exists => "exists",
            },
            field: agg.field.clone(),
            attribute_policy: gated.contains(&(resource, agg.name.as_str())),
        }
    }
}

/// One declared action.
#[derive(Debug, Clone, Serialize)]
pub struct ActionSchema {
    /// The action name — what `handle_action` is called with.
    pub name: String,
    /// `"read"` / `"write"` / `"generic"`.
    pub kind: &'static str,
}

fn kind_name(kind: ActionKind) -> &'static str {
    match kind {
        ActionKind::Read => "read",
        ActionKind::Write => "write",
        ActionKind::Generic => "generic",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::action::ActionDef;
    use crate::attribute::Attribute;
    use crate::context::DomainContext;
    use crate::domain::DomainConfig;
    use crate::policy::{Deny, PolicySet, ScopedPolicy};
    use crate::resource::{Cardinality, Relationship, Resource, erase};
    use crate::value::Record;

    struct Author;
    impl Resource for Author {
        const NAME: &'static str = "author";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("id"),
                Attribute::scalar::<String>("name"),
                Attribute {
                    default: Some(Value::Int(0)),
                    ..Attribute::scalar::<u32>("age")
                },
                Attribute::scalar::<f64>("rating"),
                Attribute::scalar::<bool>("active"),
                Attribute::scalar::<Vec<u8>>("avatar"),
                Attribute::scalar::<std::time::Duration>("tenure"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::write("create"), ActionDef::read("read")]
        }
        fn relationships() -> Vec<Relationship> {
            vec![Relationship {
                name: "posts".into(),
                destination: "post".into(),
                cardinality: Cardinality::HasMany,
                source_attribute: "id".into(),
                destination_attribute: "author_id".into(),
                through: None,
            }]
        }
        fn aggregates() -> Vec<Aggregate> {
            vec![Aggregate::count("post_count", "posts")]
        }
    }

    struct Post;
    impl Resource for Post {
        const NAME: &'static str = "post";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("id"),
                Attribute::scalar::<String>("author_id"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
    }

    fn domain(policies: PolicySet) -> Domain {
        Domain::new(
            DomainConfig {
                resources: vec![erase::<Post>(), erase::<Author>()],
                policies,
                ..DomainConfig::default()
            },
            DomainContext::new(),
        )
    }

    #[test]
    fn the_schema_describes_every_registered_resource_sorted() {
        let schema = domain(PolicySet::permissive()).schema();
        let names: Vec<&str> = schema.resources.iter().map(|r| r.name.as_str()).collect();
        // Registered post-first, exported author-first: the order is the name's,
        // not the registration's, so two runs export the same bytes.
        assert_eq!(names, vec!["author", "post"]);
    }

    #[test]
    fn scalar_rust_types_map_to_schema_types() {
        let schema = domain(PolicySet::permissive()).schema();
        let author = schema.resource("author").expect("registered");
        let ty = |name: &str| {
            author
                .attributes
                .iter()
                .find(|a| a.name == name)
                .map(|a| a.ty.clone())
                .expect("declared")
        };
        assert!(matches!(ty("name"), SchemaType::String));
        assert!(matches!(ty("age"), SchemaType::Integer));
        assert!(matches!(ty("rating"), SchemaType::Number));
        assert!(matches!(ty("active"), SchemaType::Boolean));
        assert!(matches!(ty("avatar"), SchemaType::Bytes));
        // An unmapped Rust type is reported, never guessed at.
        let SchemaType::Opaque { rust_type } = ty("tenure") else {
            panic!("expected Opaque for an unmapped type");
        };
        assert!(rust_type.contains("Duration"), "{rust_type}");
    }

    #[test]
    fn the_primary_key_default_and_shape_come_through() {
        let schema = domain(PolicySet::permissive()).schema();
        let author = schema.resource("author").expect("registered");
        assert_eq!(author.primary_key, "id");
        assert_eq!(author.storage_name, "author");
        assert!(author.tenant.is_none());

        let id = author.attributes.iter().find(|a| a.name == "id").unwrap();
        assert!(id.primary_key);
        let age = author.attributes.iter().find(|a| a.name == "age").unwrap();
        assert_eq!(age.default, Some(Value::Int(0)));

        assert_eq!(author.relationships[0].cardinality, "has_many");
        assert_eq!(author.relationships[0].destination, "post");
        assert_eq!(author.aggregates[0].kind, "count");
        let kinds: Vec<&str> = author.actions.iter().map(|a| a.kind).collect();
        assert_eq!(kinds, vec!["write", "read"]);
    }

    #[test]
    fn an_attribute_scoped_policy_is_flagged() {
        let policies = PolicySet::permissive().with(ScopedPolicy::attribute(
            "author",
            "name",
            "name-hidden",
            Arc::new(Deny("hidden".into())),
        ));
        let schema = domain(policies).schema();
        let author = schema.resource("author").expect("registered");
        let flagged: Vec<&str> = author
            .attributes
            .iter()
            .filter(|a| a.attribute_policy)
            .map(|a| a.name.as_str())
            .collect();
        assert_eq!(
            flagged,
            vec!["name"],
            "only the scoped attribute is flagged"
        );
    }

    #[test]
    fn json_schema_renders_definitions_and_types() {
        let schema = domain(PolicySet::permissive()).schema();
        let json = schema.to_json_schema();

        assert_eq!(
            json["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        let author = &json["$defs"]["author"];
        assert_eq!(author["type"], "object");
        assert_eq!(author["properties"]["name"]["type"], "string");
        assert_eq!(author["properties"]["age"]["type"], "integer");
        assert_eq!(author["properties"]["age"]["default"], 0);
        assert_eq!(author["properties"]["avatar"]["contentEncoding"], "base64");
        // An opaque type constrains nothing but says what it is.
        assert!(author["properties"]["tenure"]["type"].is_null());
        assert!(author["properties"]["tenure"]["x-ash-rust-type"].is_string());
        // What JSON Schema has no word for rides along rather than vanishing.
        assert_eq!(author["x-ash"]["primary_key"], "id");
        assert_eq!(author["x-ash"]["relationships"][0]["name"], "posts");
    }

    #[test]
    fn values_render_as_plain_json() {
        assert_eq!(value_to_json(&Value::Int(7)), serde_json::json!(7));
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            value_to_json(&Value::Str("hi".into())),
            serde_json::json!("hi")
        );
        assert_eq!(
            value_to_json(&Value::List(vec![Value::Bool(true)])),
            serde_json::json!([true])
        );
        // A non-finite float has no JSON literal.
        assert_eq!(
            value_to_json(&Value::Float(f64::NAN)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn bytes_render_as_padded_base64() {
        // The RFC 4648 test vectors, including every padding length.
        let vectors = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ];
        for (input, want) in vectors {
            assert_eq!(base64(input.as_bytes()), want, "base64({input:?})");
        }
    }

    #[test]
    fn the_export_is_pure_and_repeatable() {
        let domain = domain(PolicySet::permissive());
        let first = serde_json::to_string(&domain.schema()).expect("serializable");
        let second = serde_json::to_string(&domain.schema()).expect("serializable");
        assert_eq!(first, second);
    }
}
