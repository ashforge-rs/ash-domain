//! Attributes: the typed fields that make up a [`Resource`](crate::Resource).
//!
//! Attributes are pure metadata. Their introspection (`resource.attributes()`)
//! is the seam every extension reads — an API generator, an admin UI, or a
//! database-backed data layer all work purely from the declared attributes.
//!
//! An attribute is minimal: a `name`, a [`ty`](Attribute::ty), and an optional
//! `default`. A scalar's type **is** its Rust type — [`AttrType::Scalar`] carries
//! a [`TypeId`], produced from the field's own type
//! (`Attribute::scalar::<T>`, or inferred by `#[derive(Resource)]`). The core
//! neither generates ids by type, stamps timestamps, nor maps storage columns:
//! those are consumer concerns (an [`IdGenerator`](crate::IdGenerator), a
//! [`Change`](crate::action::Change), a [`DataLayer`](crate::DataLayer)).

use std::any::TypeId;

use crate::value::Value;

/// The type of an [`Attribute`].
///
/// A scalar carries its **Rust type** rather than a framework-defined primitive;
/// the three underscore-prefixed variants are the framework-relational kinds
/// (a reference, an embed, a repeated embed).
// Not `Eq`: an embed's carried attributes can hold a `Value::Float` default,
// and float equality is only `PartialEq`.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrType {
    /// A scalar value, typed by its Rust type. `type_id` is the identity used
    /// for any type dispatch; `name` is [`type_name`](std::any::type_name) for
    /// diagnostics. Build with [`AttrType::scalar`] / [`Attribute::scalar`].
    Scalar {
        /// The [`TypeId`] of the scalar's Rust type.
        type_id: TypeId,
        /// `std::any::type_name::<T>()`, for diagnostics only.
        name: &'static str,
    },
    /// A reference to another resource (by name) — the storage side of a
    /// relationship.
    _Ref(String),
    /// An **embedded resource**: a structured value stored *inside* the parent
    /// (in one column, as a [`Value::Map`](crate::Value::Map)), not in a table of
    /// its own. Carries the embedded resource's own attributes inline, so the
    /// write pipeline applies their declared defaults to the nested value — the
    /// embedded shape is known, not opaque.
    _Embed(Vec<Attribute>),
    /// A repeated embedded resource: a [`Value::List`](crate::Value::List) of
    /// [`Map`](crate::Value::Map)s, each carrying the embedded attributes.
    _EmbedList(Vec<Attribute>),
}

impl AttrType {
    /// A [`Scalar`](AttrType::Scalar) for the Rust type `T`.
    pub fn scalar<T: 'static>() -> Self {
        AttrType::Scalar {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }
}

/// A named, typed field of a resource.
#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    /// The attribute name.
    pub name: String,
    /// Its type.
    pub ty: AttrType,
    /// A default applied on create when the value is absent.
    pub default: Option<Value>,
}

impl Attribute {
    /// Declare a plain attribute with `name` and `ty`: no default.
    ///
    /// This is a base to refine with struct-update syntax — the Rust way, in
    /// place of a builder chain. Set the fields you want and inherit the rest:
    ///
    /// ```
    /// use ash_domain::attribute::{Attribute, AttrType};
    /// use ash_domain::Value;
    ///
    /// // A string primary key (identified by name via `Resource::primary_key`,
    /// // not by any flag on the attribute).
    /// let id = Attribute::scalar::<String>("id");
    ///
    /// // A bool with a default.
    /// let done = Attribute {
    ///     default: Some(Value::Bool(false)),
    ///     ..Attribute::scalar::<bool>("done")
    /// };
    /// # let _ = (id, done);
    /// ```
    ///
    /// The core validates none of these values on write — presence, ranges, and
    /// formats are enforced by a [`Validation`](crate::action::Validation)
    /// registered on the action, not by attribute metadata.
    pub fn new(name: impl Into<String>, ty: AttrType) -> Self {
        Self {
            name: name.into(),
            ty,
            default: None,
        }
    }

    /// A scalar attribute of Rust type `T` — shorthand for
    /// `Attribute::new(name, AttrType::scalar::<T>())`.
    pub fn scalar<T: 'static>(name: impl Into<String>) -> Self {
        Self::new(name, AttrType::scalar::<T>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_carries_its_rust_type() {
        let a = Attribute::scalar::<String>("owner");
        assert_eq!(a.name, "owner");
        assert_eq!(
            a.ty,
            AttrType::Scalar {
                type_id: TypeId::of::<String>(),
                name: std::any::type_name::<String>(),
            }
        );
    }

    #[test]
    fn distinct_rust_types_are_distinct_scalars() {
        assert_ne!(
            Attribute::scalar::<String>("x").ty,
            Attribute::scalar::<i64>("x").ty
        );
    }
}
