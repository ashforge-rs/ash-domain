//! The [`Resource`] trait — the declarative domain entity everything derives
//! from.
//!
//! A resource is defined entirely by introspectable metadata: its attributes,
//! its relationships, and the actions it supports. It carries no per-instance
//! state, so a resource is a *type*, not a value: its name is an associated
//! `const`, and its schema comes from associated functions. You never build a
//! resource — you name it, `create::<Note>(…)`, and the compiler checks it.
//!
//! Two traits, split the way `serde` splits `Serialize`:
//!
//! * [`Resource`] — the **static** side you implement (or derive). It is not
//!   object-safe (associated const, associated type, no `self`), which is what
//!   lets it be resolved by type at the call site.
//! * [`ErasedResource`] — the **object-safe** side the [`Domain`](crate::Domain)
//!   stores for introspection. It is blanket-implemented for every `Resource`
//!   via [`ResourceHandle`]; you never implement it by hand. Turn a `Resource`
//!   type into one with [`erase`].

use std::marker::PhantomData;
use std::sync::Arc;

use crate::action::ActionDef;
use crate::attribute::Attribute;
use crate::value::{FromRecord, IntoRecord};

/// The multiplicity of a [`Relationship`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    /// This resource holds the foreign key to one parent.
    BelongsTo,
    /// One related record points back at this one.
    HasOne,
    /// Many related records point back at this one.
    HasMany,
    /// A many-to-many link (through a join).
    ManyToMany,
}

/// A declared link from one resource to another.
///
/// For a direct relationship (`belongs_to` / `has_one` / `has_many`), the source
/// and destination attributes join the two resources directly. For a
/// [`ManyToMany`](Cardinality::ManyToMany), fill [`through`](Relationship::through)
/// with the join resource so the domain can resolve it in two hops.
#[derive(Clone, Debug)]
pub struct Relationship {
    /// The relationship name.
    pub name: String,
    /// The destination resource name.
    pub destination: String,
    /// The multiplicity.
    pub cardinality: Cardinality,
    /// The attribute on this resource used to match.
    ///
    /// For a `many_to_many`, this is the source resource's key that the join
    /// resource's [`through_source_attribute`](Through::source_attribute) points
    /// at.
    pub source_attribute: String,
    /// The attribute on the destination resource used to match.
    ///
    /// For a `many_to_many`, this is the destination key that the join
    /// resource's
    /// [`through_destination_attribute`](Through::destination_attribute) points
    /// at.
    pub destination_attribute: String,
    /// The join resource for a [`ManyToMany`](Cardinality::ManyToMany), or `None`
    /// for a direct relationship.
    pub through: Option<Through>,
}

/// The join (through) resource of a [`ManyToMany`](Cardinality::ManyToMany)
/// [`Relationship`]: the intermediate table whose rows pair a source key with a
/// destination key.
#[derive(Clone, Debug)]
pub struct Through {
    /// The join resource name.
    pub resource: String,
    /// The join attribute matching the source resource's
    /// [`source_attribute`](Relationship::source_attribute).
    pub source_attribute: String,
    /// The join attribute matching the destination resource's
    /// [`destination_attribute`](Relationship::destination_attribute).
    pub destination_attribute: String,
}

/// A read row together with the relationships loaded alongside it.
///
/// Returned by [`Domain::read_loaded`](crate::Domain::read_loaded). Because the
/// flat [`Record`](crate::Record) / [`Value`](crate::Value) representation has no
/// nested variant, loaded relations live *beside* the row rather than inside it:
/// [`row`](Loaded::row) is the resource's own [`Data`](Resource::Data), and
/// [`related`](Loaded::related) maps each requested relationship name to the
/// [`Record`](crate::Record)s fetched for it (empty for a `belongs_to`/`has_one`
/// with no match, one or more for a `has_many`).
#[derive(Clone, Debug)]
pub struct Loaded<T> {
    /// The row itself.
    pub row: T,
    /// Loaded relations, keyed by relationship name. Each related record is
    /// itself a [`Loaded`] so a **nested load** (`"comments.author"`) carries the
    /// grandchild relations on the child records.
    pub related: std::collections::HashMap<String, Vec<Loaded<crate::value::Record>>>,
    /// Computed aggregate values, keyed by aggregate name.
    pub aggregates: std::collections::HashMap<String, crate::value::Value>,
    /// Computed-field values, keyed by [`Computed`](crate::aggregate::Computed) name.
    pub computed: std::collections::HashMap<String, crate::value::Value>,
}

impl<T> Loaded<T> {
    /// The related rows loaded for `relationship` (each itself a [`Loaded`], so
    /// its own nested loads are reachable), or an empty slice if the
    /// relationship was not requested or had no matches.
    pub fn get(&self, relationship: &str) -> &[Loaded<crate::value::Record>] {
        self.related.get(relationship).map_or(&[], Vec::as_slice)
    }

    /// The single related row loaded for a to-one `relationship`, if any.
    pub fn one(&self, relationship: &str) -> Option<&Loaded<crate::value::Record>> {
        self.get(relationship).first()
    }

    /// The bare [`Record`](crate::Record)s loaded for `relationship`, dropping
    /// their nested loads — a convenience when you don't need the sub-relations.
    pub fn records(&self, relationship: &str) -> Vec<&crate::value::Record> {
        self.get(relationship).iter().map(|l| &l.row).collect()
    }

    /// The related rows of `relationship`, projected into a typed `D`
    /// (typically the destination resource's [`Data`](Resource::Data)) — the
    /// typed sibling of [`records`](Loaded::records). Nested loads are
    /// dropped, as there; fails if a related record does not convert.
    pub fn related_as<D: crate::value::FromRecord>(
        &self,
        relationship: &str,
    ) -> crate::error::Result<Vec<D>> {
        self.get(relationship)
            .iter()
            .map(|l| D::from_record(&l.row))
            .collect()
    }

    /// The single related row of a to-one `relationship`, projected into a
    /// typed `D` — the typed sibling of [`one`](Loaded::one). `Ok(None)` when
    /// nothing was loaded.
    pub fn one_as<D: crate::value::FromRecord>(
        &self,
        relationship: &str,
    ) -> crate::error::Result<Option<D>> {
        self.one(relationship)
            .map(|l| D::from_record(&l.row))
            .transpose()
    }

    /// The computed value of `aggregate`, if it was requested.
    pub fn aggregate(&self, aggregate: &str) -> Option<&crate::value::Value> {
        self.aggregates.get(aggregate)
    }

    /// The value of the computed field `name`, if it was requested.
    pub fn computed(&self, name: &str) -> Option<&crate::value::Value> {
        self.computed.get(name)
    }
}

/// How a resource is partitioned by tenant.
///
/// A resource returns one from [`Resource::tenant`] to opt into multitenancy;
/// the [`Domain`](crate::Domain) then scopes every action on it to the
/// [`Context`](crate::Context)'s [`tenant`](crate::Context::tenant) — stamping
/// created records, and filtering reads, updates, and destroys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantStrategy {
    /// The tenant is stored as an attribute on each record — the common
    /// "discriminator column" model (e.g. an `org_id` on every row). The domain
    /// enforces isolation itself, so this works over *any* data layer.
    Attribute(String),
    /// The data layer partitions storage by tenant itself (e.g. a schema or
    /// database per tenant). The domain does **not** filter by an attribute; it
    /// only propagates the tenant to the layer (via
    /// [`Query::tenant`](crate::query::Query::tenant) and the changeset), and the
    /// layer is responsible for the actual isolation.
    Layer,
}

impl TenantStrategy {
    /// The discriminator attribute name, for [`Attribute`](TenantStrategy::Attribute).
    pub fn attribute(&self) -> Option<&str> {
        match self {
            TenantStrategy::Attribute(name) => Some(name),
            TenantStrategy::Layer => None,
        }
    }
}

/// A structured value embedded *inside* another resource, not stored in a table
/// of its own.
///
/// An embedded type has its own [`Attribute`]s — with their constraints,
/// `required` flags, and defaults — but no `NAME`, actions, or storage: it lives
/// in one column of its parent as a [`Value::Map`](crate::Value::Map), and the
/// write pipeline validates the nested value against
/// [`embed_attributes`](Embeddable::embed_attributes). Declare a field of an
/// embeddable type on a resource with `#[attribute(embed)]` (or `Vec<T>` for a
/// repeated embed); the parent's derive reads this trait to fill the field's
/// [`AttrType::_Embed`](crate::attribute::AttrType::_Embed).
///
/// Derive it with `#[derive(Embeddable)]`, which also generates the
/// [`FromRecord`]/[`IntoRecord`] conversions so the type round-trips through a
/// [`Value::Map`](crate::Value::Map).
pub trait Embeddable {
    /// The embedded type's attributes — the schema the parent validates the
    /// nested value against.
    fn embed_attributes() -> Vec<Attribute>;
}

/// A declarative domain entity, resolved by type.
///
/// Implement this on a struct describing your entity — or derive it with
/// `#[derive(Resource)]`. Everything is static: the [`NAME`](Resource::NAME) is
/// an associated const, the schema comes from associated functions, and
/// [`Data`](Resource::Data) is the concrete row type an action reads and writes.
///
/// Register the type with a [`Domain`](crate::Domain) (via [`erase`]) and drive
/// it by naming it: `domain.handle_action::<Note>(&mut ctx, "create", ActionInput::create(params))`. A typo
/// in the resource is a compile error, not a runtime one.
///
/// A resource that does not want a typed row keeps the dynamic [`Record`] as its
/// `Data` — the identity [`FromRecord`]/[`IntoRecord`] impls make that a no-op:
///
/// ```
/// use ash_domain::{Record, Resource};
/// use ash_domain::action::ActionDef;
/// use ash_domain::attribute::Attribute;
///
/// struct Note;
/// impl Resource for Note {
///     const NAME: &'static str = "note";
///     type Data = Record;
///     fn attributes() -> Vec<Attribute> {
///         vec![Attribute::scalar::<String>("id")]
///     }
///     fn actions() -> Vec<ActionDef> {
///         vec![ActionDef::read("read")]
///     }
/// }
/// ```
///
/// [`Record`]: crate::Record
pub trait Resource: Send + Sync + 'static {
    /// The resource's unique name (the domain registry key and storage table
    /// name).
    const NAME: &'static str;

    /// The concrete row type actions return. Use `Self` for a derived struct, or
    /// [`Record`](crate::Record) to stay dynamic.
    type Data: FromRecord + IntoRecord;

    /// The resource's attributes.
    fn attributes() -> Vec<Attribute>;

    /// The actions this resource supports.
    fn actions() -> Vec<ActionDef>;

    /// The resource's relationships. Defaults to none.
    fn relationships() -> Vec<Relationship> {
        Vec::new()
    }

    /// The resource's declared [`Aggregate`](crate::aggregate::Aggregate)s —
    /// relationship roll-ups requestable by name. Defaults to none.
    fn aggregates() -> Vec<crate::aggregate::Aggregate> {
        Vec::new()
    }

    /// The resource's declared [`Computed`](crate::aggregate::Computed) fields —
    /// per-record derived values requestable by name. Defaults to none.
    fn computed() -> Vec<crate::aggregate::Computed> {
        Vec::new()
    }

    /// The resource's tenant strategy, or `None` for a global (non-tenant)
    /// resource. Defaults to `None`. See [`TenantStrategy`].
    fn tenant() -> Option<TenantStrategy> {
        None
    }

    /// The storage-side name (e.g. the SQL table) for this resource, when it
    /// differs from [`NAME`](Resource::NAME). Defaults to `None` = "same as
    /// `NAME`".
    ///
    /// This is **only** for a [`DataLayer`](crate::DataLayer) to map onto a
    /// schema it does not own. The core always addresses the resource by `NAME`;
    /// a layer
    /// consults [`storage_name`](ErasedResource::storage_name) at the storage
    /// boundary. No consumer is forced onto a core-defined table name.
    fn table() -> Option<String> {
        None
    }

    /// The primary-key attribute name. Defaults to `"id"`; a resource whose key
    /// is named otherwise overrides this. `#[derive(Resource)]` emits an override
    /// returning the name of the `#[attribute(primary_key)]` field.
    fn primary_key() -> String {
        "id".to_string()
    }

    /// The attribute holding this resource's **row version**, or `None` (the
    /// default) for a resource with no optimistic-concurrency control.
    ///
    /// Naming one opts the resource into **lost-update protection**. Every
    /// update then carries the version it read, the write only lands if the
    /// stored row still holds that version, and the domain bumps it on the way
    /// through. A concurrent writer that got there first makes the second write
    /// fail with [`Error::Conflict`](crate::Error::Conflict) instead of silently
    /// overwriting the change it never saw.
    ///
    /// Like [`primary_key`](Resource::primary_key), the version is identified
    /// **by name** rather than by a flag on the attribute. The named attribute
    /// must be declared and integer-typed; [`Domain::try_new`](crate::Domain::try_new)
    /// rejects a resource where it is missing or the wrong type, so a typo fails
    /// at construction rather than silently disabling the protection.
    ///
    /// ```
    /// # use ash_domain::{Record, Resource};
    /// # use ash_domain::action::ActionDef;
    /// # use ash_domain::attribute::Attribute;
    /// struct Account;
    /// impl Resource for Account {
    ///     const NAME: &'static str = "account";
    ///     type Data = Record;
    ///     fn attributes() -> Vec<Attribute> {
    ///         vec![
    ///             Attribute::scalar::<String>("id"),
    ///             Attribute::scalar::<i64>("balance"),
    ///             Attribute::scalar::<i64>("version"),
    ///         ]
    ///     }
    ///     fn actions() -> Vec<ActionDef> { vec![ActionDef::write("update")] }
    ///     fn version_attribute() -> Option<String> { Some("version".into()) }
    /// }
    /// assert_eq!(Account::version_attribute().as_deref(), Some("version"));
    /// ```
    fn version_attribute() -> Option<String> {
        None
    }
}

/// The object-safe face of a [`Resource`], stored by a [`Domain`](crate::Domain) for
/// introspection and existence checks.
///
/// It mirrors [`Resource`]'s schema methods but takes `&self`, so it can live
/// behind `Arc<dyn ErasedResource>`. It is blanket-implemented for every
/// `Resource` (through [`ResourceHandle`]) and is **never** implemented by hand;
/// obtain one with [`erase`].
pub trait ErasedResource: Send + Sync {
    /// The resource's unique name.
    fn name(&self) -> &str;
    /// The resource's attributes.
    fn attributes(&self) -> Vec<Attribute>;
    /// The actions this resource supports.
    fn actions(&self) -> Vec<ActionDef>;
    /// The resource's relationships.
    fn relationships(&self) -> Vec<Relationship>;
    /// The primary-key attribute name.
    fn primary_key(&self) -> String;
    /// The row-version attribute name, or `None` when the resource has no
    /// optimistic-concurrency control. See [`Resource::version_attribute`].
    fn version_attribute(&self) -> Option<String>;
    /// The resource's tenant strategy, or `None` for a global resource.
    fn tenant(&self) -> Option<TenantStrategy>;
    /// The storage-side name a data layer should use for this resource: its
    /// [`table`](Resource::table) override if set, else [`NAME`](Resource::NAME).
    fn storage_name(&self) -> String;
    /// The resource's declared aggregates.
    fn aggregates(&self) -> Vec<crate::aggregate::Aggregate>;
    /// The resource's declared computed fields.
    fn computed(&self) -> Vec<crate::aggregate::Computed>;
}

/// A zero-sized carrier that turns a static [`Resource`] type into an
/// object-safe [`ErasedResource`]. You do not name it directly — [`erase`]
/// produces one.
pub struct ResourceHandle<R>(PhantomData<fn() -> R>);

impl<R: Resource> ErasedResource for ResourceHandle<R> {
    fn name(&self) -> &str {
        R::NAME
    }
    fn attributes(&self) -> Vec<Attribute> {
        R::attributes()
    }
    fn actions(&self) -> Vec<ActionDef> {
        R::actions()
    }
    fn relationships(&self) -> Vec<Relationship> {
        R::relationships()
    }
    fn primary_key(&self) -> String {
        R::primary_key()
    }
    fn version_attribute(&self) -> Option<String> {
        R::version_attribute()
    }
    fn tenant(&self) -> Option<TenantStrategy> {
        R::tenant()
    }
    fn storage_name(&self) -> String {
        R::table().unwrap_or_else(|| R::NAME.to_string())
    }
    fn aggregates(&self) -> Vec<crate::aggregate::Aggregate> {
        R::aggregates()
    }
    fn computed(&self) -> Vec<crate::aggregate::Computed> {
        R::computed()
    }
}

/// Erase a [`Resource`] type into an `Arc<dyn ErasedResource>` for registration
/// in a [`DomainConfig`](crate::domain::DomainConfig).
///
/// ```
/// use ash_domain::{erase, Record, Resource};
/// # use ash_domain::action::ActionDef;
/// # use ash_domain::attribute::Attribute;
/// struct Note;
/// impl Resource for Note {
///     const NAME: &'static str = "note";
///     type Data = Record;
///     fn attributes() -> Vec<Attribute> { vec![Attribute::scalar::<String>("id")] }
///     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
/// }
///
/// let handle = erase::<Note>();
/// assert_eq!(handle.name(), "note");
/// ```
pub fn erase<R: Resource>() -> Arc<dyn ErasedResource> {
    Arc::new(ResourceHandle::<R>(PhantomData))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::ActionDef;

    struct LegacyUser;
    impl Resource for LegacyUser {
        const NAME: &'static str = "user";
        type Data = crate::value::Record;
        fn attributes() -> Vec<Attribute> {
            vec![Attribute::scalar::<String>("id")]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
        // Domain name is `user`; the real table is `legacy_accounts`.
        fn table() -> Option<String> {
            Some("legacy_accounts".into())
        }
    }

    struct PlainThing;
    impl Resource for PlainThing {
        const NAME: &'static str = "thing";
        type Data = crate::value::Record;
        fn attributes() -> Vec<Attribute> {
            vec![Attribute::scalar::<String>("id")]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
    }

    #[test]
    fn storage_name_uses_table_override_when_set() {
        let h = erase::<LegacyUser>();
        assert_eq!(h.name(), "user"); // domain name
        assert_eq!(h.storage_name(), "legacy_accounts"); // layer-facing
    }

    #[test]
    fn storage_name_defaults_to_resource_name() {
        let h = erase::<PlainThing>();
        assert_eq!(h.storage_name(), "thing");
    }
}
