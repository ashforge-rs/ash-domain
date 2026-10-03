//! Registry validation — the checks [`Domain::try_new`](super::Domain::try_new)
//! runs before a domain exists.
//!
//! A misconfiguration (a relationship to an unregistered resource, an aggregate
//! over an unknown relationship, a policy scoped to a resource that isn't there)
//! is a programmer error, so it fails at construction rather than at request
//! time — when it would be an outage instead of a startup failure.

use std::collections::HashMap;
use std::sync::Arc;

use crate::action::ActionKind;
use crate::error::{Error, Result};
use crate::policy::{PolicySet, Scope};
use crate::resource::{ErasedResource, TenantStrategy};

/// Registration-time validation of every resource's declarations, so an
/// inconsistent schema fails at [`Domain::try_new`] instead of surfacing as a
/// runtime error on some future request. See [`Domain::try_new`] for the rules.
pub(super) fn validate_resources(
    resources: &HashMap<String, Arc<dyn ErasedResource>>,
) -> Result<()> {
    use std::collections::HashSet;

    let attribute_names = |name: &str| -> HashSet<String> {
        resources
            .get(name)
            .map(|r| r.attributes().into_iter().map(|a| a.name).collect())
            .unwrap_or_default()
    };

    for (name, resource) in resources {
        let attrs = resource.attributes();
        let mut attr_names: HashSet<&str> = HashSet::new();
        for attr in &attrs {
            if !attr_names.insert(attr.name.as_str()) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: duplicate attribute `{}`",
                    attr.name
                )));
            }
        }

        // A declared version attribute must exist and be integer-typed: the
        // update path compares and bumps it as an integer, so a typo or a
        // `String` version would otherwise fail at request time — or, worse,
        // silently disable the lost-update protection the resource asked for.
        if let Some(version) = resource.version_attribute() {
            match attrs.iter().find(|a| a.name == version) {
                None => {
                    return Err(Error::invalid(format!(
                        "resource `{name}`: version attribute `{version}` is not a declared attribute"
                    )));
                }
                Some(attr) if !is_integer_scalar(&attr.ty) => {
                    return Err(Error::invalid(format!(
                        "resource `{name}`: version attribute `{version}` must be an integer \
                         scalar (i64/i32/u32/u64), but is {:?}",
                        attr.ty
                    )));
                }
                Some(_) => {}
            }
        }

        let actions = resource.actions();
        let mut action_names: HashSet<&str> = HashSet::new();
        for action in &actions {
            if !action_names.insert(action.name.as_str()) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: duplicate action `{}`",
                    action.name
                )));
            }
            if action.kind == ActionKind::Generic && action.handler.is_none() {
                return Err(Error::invalid(format!(
                    "resource `{name}`: generic action `{}` has no handler",
                    action.name
                )));
            }
        }

        if let Some(TenantStrategy::Attribute(attr)) = resource.tenant()
            && !attr_names.contains(attr.as_str())
        {
            return Err(Error::invalid(format!(
                "resource `{name}`: tenant discriminator `{attr}` is not a declared attribute"
            )));
        }

        let relationships = resource.relationships();
        let mut rel_names: HashSet<&str> = HashSet::new();
        for rel in &relationships {
            if !rel_names.insert(rel.name.as_str()) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: duplicate relationship `{}`",
                    rel.name
                )));
            }
            if !attr_names.contains(rel.source_attribute.as_str()) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: relationship `{}` names source attribute `{}`, \
                     which is not declared on `{name}`",
                    rel.name, rel.source_attribute
                )));
            }
            if !resources.contains_key(&rel.destination) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: relationship `{}` points at unregistered resource `{}`",
                    rel.name, rel.destination
                )));
            }
            if !attribute_names(&rel.destination).contains(&rel.destination_attribute) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: relationship `{}` names destination attribute `{}`, \
                     which is not declared on `{}`",
                    rel.name, rel.destination_attribute, rel.destination
                )));
            }
            if let Some(through) = &rel.through {
                if !resources.contains_key(&through.resource) {
                    return Err(Error::invalid(format!(
                        "resource `{name}`: relationship `{}` goes through unregistered \
                         resource `{}`",
                        rel.name, through.resource
                    )));
                }
                let join_attrs = attribute_names(&through.resource);
                for join_attr in [&through.source_attribute, &through.destination_attribute] {
                    if !join_attrs.contains(join_attr) {
                        return Err(Error::invalid(format!(
                            "resource `{name}`: relationship `{}` names join attribute \
                             `{join_attr}`, which is not declared on `{}`",
                            rel.name, through.resource
                        )));
                    }
                }
            }
        }

        // Aggregates and computed fields are requested and returned by name
        // beside the attributes, so their names must not shadow an attribute or
        // one another.
        let mut derived_names: HashSet<String> = HashSet::new();
        for agg in resource.aggregates() {
            if attr_names.contains(agg.name.as_str()) || !derived_names.insert(agg.name.clone()) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: aggregate `{}` collides with another declared name",
                    agg.name
                )));
            }
            if !rel_names.contains(agg.relationship.as_str()) {
                return Err(Error::invalid(format!(
                    "resource `{name}`: aggregate `{}` rolls up unknown relationship `{}`",
                    agg.name, agg.relationship
                )));
            }
        }
        for computed in resource.computed() {
            if attr_names.contains(computed.name.as_str())
                || !derived_names.insert(computed.name.clone())
            {
                return Err(Error::invalid(format!(
                    "resource `{name}`: computed field `{}` collides with another declared name",
                    computed.name
                )));
            }
        }
    }
    Ok(())
}

/// Registration-time validation of every policy's [`Scope`]: a policy anchored
/// to a resource, action, or attribute that does not exist would never match
/// anything — under default-deny that is a misconfiguration worth failing at
/// startup, not a silent no-op.
pub(super) fn validate_policies(
    resources: &HashMap<String, Arc<dyn ErasedResource>>,
    policies: &PolicySet,
) -> Result<()> {
    for scoped in policies.policies() {
        let (resource, checked): (&str, Option<String>) = match &scoped.scope {
            Scope::Domain => continue,
            Scope::Resource(r) => (r, None),
            Scope::Action { resource, action } => (resource, Some(format!("action `{action}`"))),
            Scope::Attribute {
                resource,
                attribute,
            } => (resource, Some(format!("attribute `{attribute}`"))),
        };
        let Some(registered) = resources.get(resource) else {
            return Err(Error::invalid(format!(
                "policy `{}` is scoped to unregistered resource `{resource}`",
                scoped.label
            )));
        };
        let exists = match &scoped.scope {
            Scope::Action { action, .. } => registered.actions().iter().any(|a| &a.name == action),
            // Attribute policies also gate derived values (aggregates and
            // computed fields) under their declared names, so all three
            // namespaces are valid anchors.
            Scope::Attribute { attribute, .. } => {
                registered.attributes().iter().any(|a| &a.name == attribute)
                    || registered.aggregates().iter().any(|a| &a.name == attribute)
                    || registered.computed().iter().any(|c| &c.name == attribute)
            }
            _ => true,
        };
        if !exists {
            return Err(Error::invalid(format!(
                "policy `{}` is scoped to {}, which `{resource}` does not declare",
                scoped.label,
                checked.unwrap_or_default()
            )));
        }
    }
    Ok(())
}

/// Whether `ty` is an integer scalar the version path can compare and bump.
///
/// The row version is read through [`Value::as_int`](crate::Value::as_int) and
/// incremented, so it must be one of the integer Rust types a
/// [`Value::Int`](crate::Value::Int) round-trips through.
fn is_integer_scalar(ty: &crate::attribute::AttrType) -> bool {
    use std::any::TypeId;
    let crate::attribute::AttrType::Scalar { type_id, .. } = ty else {
        return false;
    };
    [
        TypeId::of::<i64>(),
        TypeId::of::<i32>(),
        TypeId::of::<i16>(),
        TypeId::of::<i8>(),
        TypeId::of::<u64>(),
        TypeId::of::<u32>(),
        TypeId::of::<u16>(),
        TypeId::of::<u8>(),
    ]
    .contains(type_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::action::{ActionDef, GenericHandler};
    use crate::aggregate::{Aggregate, Computed, Computer};
    use crate::attribute::Attribute;
    use crate::context::DomainContext;
    use crate::domain::{Domain, DomainConfig};
    use crate::policy::{Admit, PolicySet, ScopedPolicy};
    use crate::resource::{Cardinality, Relationship, Through};
    use crate::value::{Record, Value};

    /// A resource whose declarations are **values**, so one test can vary a
    /// single one of them. Implementing [`ErasedResource`] by hand is what the
    /// erased registry stores anyway — a `Resource` type per defect would be a
    /// hundred lines of boilerplate for the same coverage.
    #[derive(Clone)]
    struct Declared {
        name: String,
        attributes: Vec<Attribute>,
        actions: Vec<ActionDef>,
        relationships: Vec<Relationship>,
        aggregates: Vec<Aggregate>,
        computed: Vec<Computed>,
        primary_key: String,
        version_attribute: Option<String>,
        tenant: Option<TenantStrategy>,
    }

    impl Declared {
        /// A valid resource: one string attribute, one read action. Every case
        /// below starts here and breaks exactly one thing.
        fn new(name: &str) -> Self {
            Self {
                name: name.into(),
                attributes: vec![Attribute::scalar::<String>("id")],
                actions: vec![ActionDef::read("read")],
                relationships: Vec::new(),
                aggregates: Vec::new(),
                computed: Vec::new(),
                primary_key: "id".into(),
                version_attribute: None,
                tenant: None,
            }
        }
        fn attribute(mut self, name: &str) -> Self {
            self.attributes.push(Attribute::scalar::<String>(name));
            self
        }
        fn action(mut self, action: ActionDef) -> Self {
            self.actions.push(action);
            self
        }
        fn relationship(mut self, rel: Relationship) -> Self {
            self.relationships.push(rel);
            self
        }
        fn aggregate(mut self, agg: Aggregate) -> Self {
            self.aggregates.push(agg);
            self
        }
        fn computed(mut self, name: &str) -> Self {
            self.computed.push(Computed::new(name, Arc::new(Nothing)));
            self
        }
        fn tenant(mut self, strategy: TenantStrategy) -> Self {
            self.tenant = Some(strategy);
            self
        }
        fn erase(self) -> Arc<dyn ErasedResource> {
            Arc::new(self)
        }
    }

    impl ErasedResource for Declared {
        fn name(&self) -> &str {
            &self.name
        }
        fn attributes(&self) -> Vec<Attribute> {
            self.attributes.clone()
        }
        fn actions(&self) -> Vec<ActionDef> {
            self.actions.clone()
        }
        fn relationships(&self) -> Vec<Relationship> {
            self.relationships.clone()
        }
        fn primary_key(&self) -> String {
            self.primary_key.clone()
        }
        fn version_attribute(&self) -> Option<String> {
            self.version_attribute.clone()
        }
        fn tenant(&self) -> Option<TenantStrategy> {
            self.tenant.clone()
        }
        fn storage_name(&self) -> String {
            self.name.clone()
        }
        fn aggregates(&self) -> Vec<Aggregate> {
            self.aggregates.clone()
        }
        fn computed(&self) -> Vec<Computed> {
            self.computed.clone()
        }
    }

    /// A computed field that computes nothing; only its declared name matters
    /// to registry validation.
    struct Nothing;
    #[async_trait::async_trait]
    impl Computer for Nothing {
        async fn compute(&self, _record: &Record) -> Result<Value> {
            Ok(Value::Null)
        }
    }

    fn has_many(name: &str, destination: &str, source: &str, dest: &str) -> Relationship {
        Relationship {
            name: name.into(),
            destination: destination.into(),
            cardinality: Cardinality::HasMany,
            source_attribute: source.into(),
            destination_attribute: dest.into(),
            through: None,
        }
    }

    fn build(resources: Vec<Arc<dyn ErasedResource>>, policies: PolicySet) -> Result<Domain> {
        Domain::try_new(
            DomainConfig {
                resources,
                policies,
                ..DomainConfig::default()
            },
            DomainContext::new(),
        )
    }

    /// Assert `try_new` rejected the config, and that the message names the
    /// defect — a startup error nobody can act on is only half a check.
    #[track_caller]
    fn rejected(result: Result<Domain>, needle: &str) {
        let Err(Error::Invalid { message, .. }) = result else {
            panic!("expected Invalid mentioning {needle:?}, got a domain");
        };
        assert!(
            message.contains(needle),
            "message {message:?} does not name {needle:?}"
        );
    }

    #[test]
    fn a_well_formed_registry_builds() {
        // The control: everything the cases below break, unbroken.
        let post = Declared::new("post").attribute("author_id");
        let author = Declared::new("author")
            .relationship(has_many("posts", "post", "id", "author_id"))
            .aggregate(Aggregate::count("post_count", "posts"))
            .computed("shout");
        assert!(build(vec![post.erase(), author.erase()], PolicySet::permissive()).is_ok());
    }

    #[test]
    fn a_duplicate_resource_is_rejected() {
        let result = build(
            vec![Declared::new("note").erase(), Declared::new("note").erase()],
            PolicySet::permissive(),
        );
        rejected(result, "duplicate resource `note`");
    }

    #[test]
    fn a_duplicate_attribute_is_rejected() {
        let note = Declared::new("note").attribute("title").attribute("title");
        rejected(
            build(vec![note.erase()], PolicySet::permissive()),
            "duplicate attribute `title`",
        );
    }

    #[test]
    fn a_duplicate_action_is_rejected() {
        let note = Declared::new("note").action(ActionDef::read("read"));
        rejected(
            build(vec![note.erase()], PolicySet::permissive()),
            "duplicate action `read`",
        );
    }

    #[test]
    fn a_generic_action_without_a_handler_is_rejected() {
        // `ActionDef::generic` demands a handler, so only a hand-built def can
        // reach this state — which is exactly why the check exists.
        let headless = ActionDef {
            name: "ping".into(),
            kind: ActionKind::Generic,
            preparations: Vec::new(),
            changes: Vec::new(),
            validations: Vec::new(),
            handler: None::<Arc<dyn GenericHandler>>,
        };
        let note = Declared::new("note").action(headless);
        rejected(
            build(vec![note.erase()], PolicySet::permissive()),
            "generic action `ping` has no handler",
        );
    }

    #[test]
    fn a_tenant_discriminator_that_is_not_an_attribute_is_rejected() {
        let note = Declared::new("note").tenant(TenantStrategy::Attribute("org_id".into()));
        rejected(
            build(vec![note.erase()], PolicySet::permissive()),
            "tenant discriminator `org_id` is not a declared attribute",
        );
    }

    #[test]
    fn a_layer_tenant_strategy_needs_no_attribute() {
        let note = Declared::new("note").tenant(TenantStrategy::Layer);
        assert!(build(vec![note.erase()], PolicySet::permissive()).is_ok());
    }

    #[test]
    fn a_duplicate_relationship_is_rejected() {
        let post = Declared::new("post").attribute("author_id");
        let author = Declared::new("author")
            .relationship(has_many("posts", "post", "id", "author_id"))
            .relationship(has_many("posts", "post", "id", "author_id"));
        rejected(
            build(vec![post.erase(), author.erase()], PolicySet::permissive()),
            "duplicate relationship `posts`",
        );
    }

    #[test]
    fn a_relationship_to_an_unregistered_resource_is_rejected() {
        let author =
            Declared::new("author").relationship(has_many("posts", "post", "id", "author_id"));
        rejected(
            build(vec![author.erase()], PolicySet::permissive()),
            "points at unregistered resource `post`",
        );
    }

    #[test]
    fn a_relationship_with_an_undeclared_source_attribute_is_rejected() {
        let post = Declared::new("post").attribute("author_id");
        let author =
            Declared::new("author").relationship(has_many("posts", "post", "nope", "author_id"));
        rejected(
            build(vec![post.erase(), author.erase()], PolicySet::permissive()),
            "names source attribute `nope`",
        );
    }

    #[test]
    fn a_relationship_with_an_undeclared_destination_attribute_is_rejected() {
        let post = Declared::new("post");
        let author = Declared::new("author").relationship(has_many("posts", "post", "id", "nope"));
        rejected(
            build(vec![post.erase(), author.erase()], PolicySet::permissive()),
            "names destination attribute `nope`",
        );
    }

    #[test]
    fn a_many_to_many_through_an_unregistered_join_is_rejected() {
        let tag = Declared::new("tag");
        let mut rel = has_many("tags", "tag", "id", "id");
        rel.cardinality = Cardinality::ManyToMany;
        rel.through = Some(Through {
            resource: "post_tag".into(),
            source_attribute: "post_id".into(),
            destination_attribute: "tag_id".into(),
        });
        let post = Declared::new("post").relationship(rel);
        rejected(
            build(vec![tag.erase(), post.erase()], PolicySet::permissive()),
            "goes through unregistered resource `post_tag`",
        );
    }

    #[test]
    fn a_many_to_many_with_an_undeclared_join_attribute_is_rejected() {
        let tag = Declared::new("tag");
        // The join resource exists but does not declare `post_id`.
        let join = Declared::new("post_tag").attribute("tag_id");
        let mut rel = has_many("tags", "tag", "id", "id");
        rel.cardinality = Cardinality::ManyToMany;
        rel.through = Some(Through {
            resource: "post_tag".into(),
            source_attribute: "post_id".into(),
            destination_attribute: "tag_id".into(),
        });
        let post = Declared::new("post").relationship(rel);
        rejected(
            build(
                vec![tag.erase(), join.erase(), post.erase()],
                PolicySet::permissive(),
            ),
            "names join attribute `post_id`",
        );
    }

    #[test]
    fn an_aggregate_over_an_unknown_relationship_is_rejected() {
        let author = Declared::new("author").aggregate(Aggregate::count("post_count", "posts"));
        rejected(
            build(vec![author.erase()], PolicySet::permissive()),
            "rolls up unknown relationship `posts`",
        );
    }

    #[test]
    fn an_aggregate_colliding_with_an_attribute_is_rejected() {
        let post = Declared::new("post").attribute("author_id");
        let author = Declared::new("author")
            .attribute("post_count")
            .relationship(has_many("posts", "post", "id", "author_id"))
            .aggregate(Aggregate::count("post_count", "posts"));
        rejected(
            build(vec![post.erase(), author.erase()], PolicySet::permissive()),
            "aggregate `post_count` collides",
        );
    }

    #[test]
    fn a_computed_field_colliding_with_an_attribute_is_rejected() {
        let note = Declared::new("note").attribute("shout").computed("shout");
        rejected(
            build(vec![note.erase()], PolicySet::permissive()),
            "computed field `shout` collides",
        );
    }

    #[test]
    fn a_policy_scoped_to_an_unregistered_resource_is_rejected() {
        let policies = PolicySet::permissive().with(ScopedPolicy::resource(
            "ghost",
            "ghost-policy",
            Arc::new(Admit),
        ));
        rejected(
            build(vec![Declared::new("note").erase()], policies),
            "scoped to unregistered resource `ghost`",
        );
    }

    #[test]
    fn a_policy_scoped_to_an_unknown_action_is_rejected() {
        let policies = PolicySet::permissive().with(ScopedPolicy::action(
            "note",
            "vanish",
            "vanish-policy",
            Arc::new(Admit),
        ));
        rejected(
            build(vec![Declared::new("note").erase()], policies),
            "action `vanish`",
        );
    }

    #[test]
    fn a_policy_scoped_to_an_unknown_attribute_is_rejected() {
        let policies = PolicySet::permissive().with(ScopedPolicy::attribute(
            "note",
            "ghost",
            "ghost-field",
            Arc::new(Admit),
        ));
        rejected(
            build(vec![Declared::new("note").erase()], policies),
            "attribute `ghost`",
        );
    }

    #[test]
    fn a_policy_may_be_scoped_to_a_derived_value() {
        // An aggregate and a computed field are gated like attributes, so both
        // are valid anchors — this is the case the "unknown attribute" check
        // must not catch.
        let post = Declared::new("post").attribute("author_id");
        let author = Declared::new("author")
            .relationship(has_many("posts", "post", "id", "author_id"))
            .aggregate(Aggregate::count("post_count", "posts"))
            .computed("shout");
        let policies = PolicySet::permissive()
            .with(ScopedPolicy::attribute(
                "author",
                "post_count",
                "count-policy",
                Arc::new(Admit),
            ))
            .with(ScopedPolicy::attribute(
                "author",
                "shout",
                "shout-policy",
                Arc::new(Admit),
            ));
        assert!(build(vec![post.erase(), author.erase()], policies).is_ok());
    }

    #[test]
    fn a_domain_scoped_policy_needs_no_target() {
        let policies =
            PolicySet::permissive().with(ScopedPolicy::domain("everywhere", Arc::new(Admit)));
        assert!(build(vec![Declared::new("note").erase()], policies).is_ok());
    }
}
