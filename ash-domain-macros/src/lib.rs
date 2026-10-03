//! Derive macros for [`ash-domain`](https://docs.rs/ash-domain).
//!
//! This crate is a companion to `ash-domain`; you normally do not depend on it
//! directly. Enable `ash-domain`'s `derive` feature (on by default) and use the
//! re-exported macro:
//!
//! ```ignore
//! use ash_domain::Resource;
//!
//! #[derive(Resource, Default)]
//! #[resource(name = "note")]
//! struct Note {
//!     #[attribute(primary_key, uuid)]
//!     id: String,
//!     title: String,
//!     #[attribute(default = false)]
//!     done: bool,
//! }
//! ```
//!
//! The generated code is three impls plus a typed method set, resolved by type:
//!
//! * `impl ash_domain::Resource` — the resource name (from `#[resource(name =
//!   "…")]`, defaulting to the lower-cased type name), one [`Attribute`] per
//!   field (its type inferred from the Rust type and refined by
//!   `#[attribute(...)]`), a default set of CRUD actions, and `type Data =
//!   Self`.
//! * `impl ash_domain::FromRecord` / `impl ash_domain::IntoRecord` — field-by-field
//!   conversions between the struct and the neutral `Record`, so the typed
//!   `Note::create` hands back a typed `Note`.
//! * Typed action methods — `Note::create` / `read` / `get` / `update` /
//!   `destroy`, each wrapping the single `Domain::handle_action` entry point and
//!   projecting its outcome back into the typed row (`get` fetches one row by
//!   primary key, as an authorized read). Declare **custom actions** with a repeatable
//!   `#[action(update, name = "completed")]` (kinds: `create` / `update` /
//!   `destroy` / `read`) and the derive emits the matching `ActionDef` and a
//!   `Note::completed(…)` method; the action's behavior is attached separately as
//!   a `Change`, since the attribute only *declares* it.
//!
//! Hand-write the impls when you need generic actions (which carry a handler) or
//! bespoke pipelines beyond what `#[action(...)]` declares.
//!
//! For a plain data type that is *not* a resource — a typed action-params
//! struct, a read projection, a generic action's output — derive just the
//! `Record` conversions with [`FromRecord`](macro@FromRecord) /
//! [`IntoRecord`](macro@IntoRecord), so it flows through the pipeline without a
//! hand-built `Record`.
//!
//! Two further derives cut the boilerplate around **typed queries** (a `read`
//! reached through `Domain::query` that returns a custom shape):
//!
//! * [`Projection`](macro@Projection) — a read-only [`Resource`](macro@Resource)
//!   (one `read` action, no CRUD verbs): the shape a typed query returns.
//! * [`TypedQuery`](macro@TypedQuery) — the whole `impl ash_domain::TypedQuery<B>`
//!   block from one `#[query(resource = …, backend = …, run = …)]` attribute.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Lit, Path, Type, parse_macro_input};

/// The conversion strategy for a field, chosen from its Rust type. It decides
/// how the field is read from / written to a [`Value`] in the generated
/// `FromRecord` / `IntoRecord` impls.
#[derive(Clone, Copy)]
enum Conv {
    Str,
    Bool,
    Int,
    Float,
    Bytes,
    /// A `#[derive(ValueEnum)]` field: round-trips through the field type's own
    /// `FromValue` / `From<T> for Value` impls (string-backed), so a fixed set of
    /// variants is stored as a [`Value::Str`] but typed as the enum in Rust.
    Enum,
    /// An embedded resource: round-trips through `FromRecord`/`IntoRecord` and a
    /// [`Value::Map`], carrying `is_list` for a `Vec<T>` embed.
    Embed {
        is_list: bool,
    },
}

/// Derive `ash_domain::Resource` (plus `FromRecord` / `IntoRecord`) for a struct
/// with named fields.
///
/// Container attribute: `#[resource(name = "...")]`.
/// Field attributes: `#[attribute(primary_key, private, uuid, timestamp,
/// created_timestamp, updated_timestamp, column = "...", embed,
/// default = <literal>)]`. The core runs no built-in attribute validation;
/// enforce presence/ranges/formats in a
/// [`Validation`](ash_domain::action::Validation) on the action.
#[proc_macro_derive(
    Resource,
    attributes(resource, attribute, relationship, aggregate, action)
)]
pub fn derive_resource(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input, ActionSet::Crud) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand(input: DeriveInput, actions: ActionSet) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Container: #[resource(name = "...", table = "...", tenant = "...",
    // tenant_layer)], name defaulting to the lower-cased type.
    let mut resource_name = ident.to_string().to_lowercase();
    let mut table: Option<String> = None;
    let mut tenant: Option<proc_macro2::TokenStream> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("resource") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    resource_name = lit.value();
                } else if meta.path.is_ident("table") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    table = Some(lit.value());
                } else if meta.path.is_ident("tenant") {
                    // Attribute (discriminator-column) strategy.
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    let name = lit.value();
                    tenant = Some(quote!(::ash_domain::TenantStrategy::Attribute(
                        #name.to_string()
                    )));
                } else if meta.path.is_ident("tenant_layer") {
                    // Layer (schema/DB-per-tenant) strategy.
                    tenant = Some(quote!(::ash_domain::TenantStrategy::Layer));
                } else {
                    return Err(meta.error(
                        "unknown `resource` option (expected `name`, `table`, `tenant`, or `tenant_layer`)",
                    ));
                }
                Ok(())
            })?;
        }
    }

    // Container-level relationships, aggregates, and custom actions (all
    // repeatable attributes).
    let relationship_tokens = parse_relationships(&input.attrs)?;
    let aggregate_tokens = parse_aggregates(&input.attrs)?;
    let (custom_action_defs, custom_action_methods) = parse_custom_actions(&input.attrs, ident)?;

    let fields = named_fields(&input, "Resource")?;
    let (attribute_tokens, from_fields, into_fields, primary_key_name) = build_fields(fields)?;

    // Emit a `primary_key()` override when a field carried `#[attribute(primary_key)]`;
    // otherwise inherit the trait default (`"id"`).
    let primary_key_method = primary_key_name.map(|name| {
        quote! {
            fn primary_key() -> ::std::string::String {
                #name.to_string()
            }
        }
    });

    // Only emit `table()` / `tenant()` overrides when the resource declared them,
    // otherwise inherit the trait defaults.
    let table_method = table.map(|t| {
        quote! {
            fn table() -> ::std::option::Option<::std::string::String> {
                ::std::option::Option::Some(#t.to_string())
            }
        }
    });
    let tenant_method = tenant.map(|strategy| {
        quote! {
            fn tenant() -> ::std::option::Option<::ash_domain::TenantStrategy> {
                ::std::option::Option::Some(#strategy)
            }
        }
    });

    // Typed code interface: one associated fn per default CRUD action, the action
    // name baked in, dispatching through the single `Domain::handle_action` entry
    // point and projecting its raw-record outcome back into the typed row. This
    // wraps the string-based API so the resource and action are both compile-checked
    // at the call site. A read-only projection has no write verbs, so it gets no
    // typed interface (it's read through `Domain::query`, not these CRUD helpers).
    let typed_interface = match actions {
        ActionSet::ReadOnly => quote!(),
        ActionSet::Crud => quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            /// Create a record through the `create` action, returning the typed row.
            pub async fn create(
                __domain: &::ash_domain::Domain,
                __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                __params: impl ::ash_domain::IntoRecord,
            ) -> ::ash_domain::Result<Self> {
                __domain
                    .handle_action::<Self>(__ctx, "create", ::ash_domain::ActionInput::create(__params)?)
                    .await?
                    .into_data::<Self>()
            }

            /// Create several records through the `create` action in one batch,
            /// returning the typed rows in the order they were given.
            ///
            /// Every row runs the whole per-row pipeline and nothing is
            /// persisted until all of them pass, so one denied or invalid row
            /// persists none of the batch. The batch size is bounded by the
            /// domain's `max_batch`.
            pub async fn create_many<__I, __T>(
                __domain: &::ash_domain::Domain,
                __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                __rows: __I,
            ) -> ::ash_domain::Result<::std::vec::Vec<Self>>
            where
                __I: ::core::iter::IntoIterator<Item = __T>,
                __T: ::ash_domain::IntoRecord,
            {
                __domain
                    .handle_action::<Self>(
                        __ctx,
                        "create",
                        ::ash_domain::ActionInput::create_many(__rows)?,
                    )
                    .await?
                    .into_data_vec::<Self>()
            }

            /// Read records through the `read` action.
            pub async fn read(
                __domain: &::ash_domain::Domain,
                __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                __query: ::ash_domain::Query,
            ) -> ::ash_domain::Result<::std::vec::Vec<Self>> {
                __domain
                    .handle_action::<Self>(__ctx, "read", ::ash_domain::ActionInput::read(__query))
                    .await?
                    .into_data_vec::<Self>()
            }

            /// Fetch a single record by primary-key value, or `None` if no such
            /// row is visible.
            ///
            /// This is a **read** by primary key: it runs the `read` action, so it
            /// is authorized, tenant-scoped, and redacted exactly like any other
            /// read — never a raw storage `get` that would bypass the gate. It
            /// issues the reserved key-set query (`primary_key ∈ [id]`), which
            /// every data layer must honour, and returns the first matching row.
            pub async fn get(
                __domain: &::ash_domain::Domain,
                __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                __id: impl ::std::convert::Into<::ash_domain::Value>,
            ) -> ::ash_domain::Result<::std::option::Option<Self>> {
                let __query = ::ash_domain::Query::key_set(
                    <Self as ::ash_domain::Resource>::NAME,
                    <Self as ::ash_domain::Resource>::primary_key(),
                    ::std::vec![__id.into()],
                );
                ::std::result::Result::Ok(Self::read(__domain, __ctx, __query).await?.into_iter().next())
            }

            /// Update the record `__id` through the `update` action.
            pub async fn update(
                __domain: &::ash_domain::Domain,
                __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                __id: ::ash_domain::Value,
                __params: impl ::ash_domain::IntoRecord,
            ) -> ::ash_domain::Result<Self> {
                __domain
                    .handle_action::<Self>(__ctx, "update", ::ash_domain::ActionInput::update(__id, __params)?)
                    .await?
                    .into_data::<Self>()
            }

            /// Destroy the record `__id` through the `destroy` action.
            pub async fn destroy(
                __domain: &::ash_domain::Domain,
                __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                __id: ::ash_domain::Value,
            ) -> ::ash_domain::Result<()> {
                __domain
                    .handle_action::<Self>(__ctx, "destroy", ::ash_domain::ActionInput::destroy(__id))
                    .await?;
                ::std::result::Result::Ok(())
            }
        }
        },
    };

    // The resource's full action list: the set's defaults plus any declared
    // custom actions.
    let mut action_defs = actions.action_defs();
    action_defs.extend(custom_action_defs);

    // Typed methods for custom actions go in their own `impl` block so a read-only
    // resource (no CRUD typed interface) can still declare custom read actions.
    let custom_interface = if custom_action_methods.is_empty() {
        quote!()
    } else {
        quote! {
            impl #impl_generics #ident #ty_generics #where_clause {
                #(#custom_action_methods)*
            }
        }
    };

    Ok(quote! {
        #typed_interface
        #custom_interface

        impl #impl_generics ::ash_domain::resource::Resource for #ident #ty_generics #where_clause {
            const NAME: &'static str = #resource_name;
            type Data = Self;

            fn attributes() -> ::std::vec::Vec<::ash_domain::attribute::Attribute> {
                ::std::vec![ #(#attribute_tokens),* ]
            }

            fn actions() -> ::std::vec::Vec<::ash_domain::action::ActionDef> {
                ::std::vec![ #(#action_defs),* ]
            }

            #table_method
            #tenant_method
            #primary_key_method

            fn relationships() -> ::std::vec::Vec<::ash_domain::Relationship> {
                ::std::vec![ #(#relationship_tokens),* ]
            }

            fn aggregates() -> ::std::vec::Vec<::ash_domain::Aggregate> {
                ::std::vec![ #(#aggregate_tokens),* ]
            }
        }

        impl #impl_generics ::ash_domain::FromRecord for #ident #ty_generics #where_clause {
            fn from_record(record: &::ash_domain::Record) -> ::ash_domain::Result<Self> {
                ::std::result::Result::Ok(Self { #(#from_fields),* })
            }
        }

        impl #impl_generics ::ash_domain::IntoRecord for #ident #ty_generics #where_clause {
            fn into_record(self) -> ::ash_domain::Result<::ash_domain::Record> {
                let mut __record = ::ash_domain::Record::new();
                #(#into_fields)*
                ::ash_domain::Result::Ok(__record)
            }
        }
    })
}

/// Derive `ash_domain::TypedQuery<B>` — the boilerplate `impl` block for a typed
/// query reached through [`Domain::query`](ash_domain::Domain::query).
///
/// A typed query is an ordinary read whose result is a **custom resource** (a
/// projection or aggregation). Writing the trait impl by hand is repetitive — the
/// associated result type, the backend capability bound, and the one-line body
/// that dispatches to a backend method. This derive writes all of that from one
/// attribute:
///
/// ```ignore
/// use ash_domain::{Context, Record, Result, Store, TypedQuery};
///
/// // A backend capability the query needs; a concrete layer implements it.
/// #[async_trait::async_trait]
/// trait TodoStats {
///     async fn owner_stats(&self, ctx: &Context<Self>) -> Result<Vec<Record>>
///     where Self: Sized;
/// }
///
/// #[derive(TypedQuery)]
/// #[query(resource = OwnerStats, backend = TodoStats, run = owner_stats)]
/// struct StatsByOwner;
/// ```
///
/// which expands to roughly:
///
/// ```ignore
/// #[async_trait::async_trait]
/// impl<B> TypedQuery<B> for StatsByOwner
/// where B: Store + TodoStats {
///     type Resource = OwnerStats;
///     async fn run(&self, ctx: &Context<B>) -> Result<Vec<Record>> {
///         ctx.backend().owner_stats(ctx).await
///     }
/// }
/// ```
///
/// Attributes on `#[query(...)]`:
///
/// - `resource = <Type>` (**required**) — the result [`Resource`](macro@Resource)
///   the query returns and is authorized / redacted as.
/// - `run = <method>` (**required**) — the backend method the generated `run`
///   calls, as `ctx.backend().<method>(ctx).await`. Its receiver is the backend,
///   and it is handed the `&Context<B>` so it can read the tenant / actor / args
///   it needs.
/// - `backend = <Trait>` (optional, repeatable) — an extra bound on the backend
///   `B` beyond [`Store`](ash_domain::Store), naming the capability trait that
///   carries `run`. Omit when the method lives on `Store` itself.
/// - `action = "<name>"` (optional) — authorize under this named read action of
///   the result resource instead of its first-declared read action.
/// - `tenant_aware` (optional, bare flag) — assert that `run` scopes its own read
///   by [`ctx.tenant()`](ash_domain::Context::tenant), so the domain admits it.
///   **Required** when the result `resource` is tenant-scoped, or the query is
///   refused with `MissingTenant` (see
///   [`TypedQuery::tenant_aware`](ash_domain::TypedQuery::tenant_aware)); the
///   `run` method must actually filter by the tenant.
#[proc_macro_derive(TypedQuery, attributes(query))]
pub fn derive_typed_query(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_typed_query(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_typed_query(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;

    let mut resource: Option<Path> = None;
    let mut run: Option<syn::Ident> = None;
    let mut backends: Vec<Path> = Vec::new();
    let mut action: Option<String> = None;
    let mut tenant_aware = false;

    let mut saw_query = false;
    for attr in &input.attrs {
        if !attr.path().is_ident("query") {
            continue;
        }
        saw_query = true;
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("resource") {
                resource = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("run") {
                run = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("backend") {
                backends.push(meta.value()?.parse()?);
            } else if meta.path.is_ident("action") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                action = Some(lit.value());
            } else if meta.path.is_ident("tenant_aware") {
                // A bare flag (no `= value`): the query asserts it scopes its own
                // read by tenant, so the domain's tenant guard admits it.
                tenant_aware = true;
            } else {
                return Err(meta.error(
                    "unknown `query` option (expected `resource`, `run`, `backend`, \
                     `action`, or `tenant_aware`)",
                ));
            }
            Ok(())
        })?;
    }

    if !saw_query {
        return Err(syn::Error::new_spanned(
            ident,
            "`#[derive(TypedQuery)]` requires a `#[query(resource = ..., run = ...)]` attribute",
        ));
    }
    let resource = resource.ok_or_else(|| {
        syn::Error::new_spanned(ident, "`#[query(...)]` requires `resource = <Type>`")
    })?;
    let run = run.ok_or_else(|| {
        syn::Error::new_spanned(ident, "`#[query(...)]` requires `run = <method>`")
    })?;

    // The bound on the context backend `B`: always `Store`, plus each declared
    // capability trait carrying the `run` method.
    let backend_bounds = quote!(::ash_domain::Store #(+ #backends)*);

    // `action()` override, emitted only when the query names a specific read
    // action to authorize under.
    let action_method = action.map(|name| {
        quote! {
            fn action(&self) -> ::std::option::Option<&str> {
                ::std::option::Option::Some(#name)
            }
        }
    });

    // `tenant_aware()` override, emitted only when the query opts in — its `run`
    // scopes by `ctx.tenant()`, so the domain's tenant guard admits it (a
    // tenant-scoped result resource is otherwise refused with `MissingTenant`).
    let tenant_aware_method = tenant_aware.then(|| {
        quote! {
            fn tenant_aware(&self) -> bool {
                true
            }
        }
    });

    Ok(quote! {
        #[::ash_domain::async_trait]
        impl<B> ::ash_domain::TypedQuery<B> for #ident
        where
            B: #backend_bounds,
        {
            type Resource = #resource;

            #action_method
            #tenant_aware_method

            async fn run(
                &self,
                ctx: &::ash_domain::Context<B>,
            ) -> ::ash_domain::Result<::std::vec::Vec<::ash_domain::Record>> {
                ctx.backend().#run(ctx).await
            }
        }
    })
}

/// Derive a **read-only projection** [`Resource`](macro@Resource) — the shape a
/// [`TypedQuery`](macro@TypedQuery) returns.
///
/// A projection / aggregation resource is not stored and not written: it exists
/// only as the *result* of a typed query, so it needs no `create` / `update` /
/// `destroy` actions — just a single `read` action for its policies to gate and
/// redact under. This derive is exactly [`Resource`](macro@Resource) with that
/// action set: one field per attribute (same `#[attribute(...)]` options), the
/// `FromRecord` / `IntoRecord` conversions, and a lone `read` action.
///
/// ```ignore
/// use ash_domain::Projection;
///
/// #[derive(Projection, Default)]
/// #[resource(name = "owner_stats")]
/// struct OwnerStats {
///     owner_id: String,
///     open_count: i64,
/// }
/// ```
///
/// Use plain [`Resource`](macro@Resource) instead when the type is stored and
/// needs the full CRUD action set.
#[proc_macro_derive(
    Projection,
    attributes(resource, attribute, relationship, aggregate, action)
)]
pub fn derive_projection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input, ActionSet::ReadOnly) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Which action set a `Resource`-shaped derive emits.
#[derive(Clone, Copy)]
enum ActionSet {
    /// The default CRUD verbs: `create` / `read` / `update` / `destroy`.
    Crud,
    /// A lone `read` — for a projection / aggregation returned by a typed query.
    ReadOnly,
}

impl ActionSet {
    /// The default `ActionDef` elements this set generates, as individual
    /// expressions. `expand` splices these together with any custom-action defs
    /// into the resource's `actions()` list.
    fn action_defs(self) -> Vec<proc_macro2::TokenStream> {
        match self {
            ActionSet::Crud => vec![
                quote!(::ash_domain::action::ActionDef::write("create")),
                quote!(::ash_domain::action::ActionDef::read("read")),
                quote!(::ash_domain::action::ActionDef::write("update")),
                quote!(::ash_domain::action::ActionDef::write("destroy")),
            ],
            ActionSet::ReadOnly => vec![quote!(::ash_domain::action::ActionDef::read("read"))],
        }
    }
}

/// Borrow the named fields of a struct `DeriveInput`, erroring (named after the
/// deriving `trait_name`) for a non-struct or a tuple/unit struct. Shared by
/// every derive in this crate, all of which are field-driven.
fn named_fields<'a>(
    input: &'a DeriveInput,
    trait_name: &str,
) -> syn::Result<&'a syn::punctuated::Punctuated<syn::Field, syn::token::Comma>> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => Ok(&named.named),
            _ => Err(syn::Error::new_spanned(
                &input.ident,
                format!("`{trait_name}` can only be derived for structs with named fields"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            &input.ident,
            format!("`{trait_name}` can only be derived for structs"),
        )),
    }
}

/// The `(attribute_tokens, from_fields, into_fields, primary_key_name)` tuple
/// built from a struct's named fields — the per-field work shared by the
/// `Resource` and `Embeddable` derives. `primary_key_name` is `Some(field)` when
/// a field carried `#[attribute(primary_key)]`, so the `Resource` derive can emit
/// a `primary_key()` override; `Embeddable` ignores it.
type FieldTokens = (
    Vec<proc_macro2::TokenStream>,
    Vec<proc_macro2::TokenStream>,
    Vec<proc_macro2::TokenStream>,
    Option<String>,
);

fn build_fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> syn::Result<FieldTokens> {
    let mut attribute_tokens = Vec::new();
    let mut from_fields = Vec::new();
    let mut into_fields = Vec::new();
    let mut primary_key_name: Option<String> = None;

    for field in fields {
        let field_ident = field.ident.as_ref().expect("named field has an ident");
        let field_name = field_ident.to_string();

        let mut is_embed = false;
        let mut is_enum = false;
        let mut ty_override: Option<proc_macro2::TokenStream> = None;
        let mut default_value: Option<proc_macro2::TokenStream> = None;

        for attr in &field.attrs {
            if !attr.path().is_ident("attribute") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("primary_key") {
                    // No longer an attribute field — it names the resource's PK,
                    // emitted as a `Resource::primary_key()` override below.
                    primary_key_name = Some(field_name.clone());
                } else if meta.path.is_ident("default") {
                    let lit: Lit = meta.value()?.parse()?;
                    default_value = Some(lit_to_value(&lit)?);
                } else if meta.path.is_ident("embed") {
                    is_embed = true;
                } else if meta.path.is_ident("enumerate") {
                    // The field's type is a `#[derive(ValueEnum)]` enum: store it
                    // as a string, but type it as the enum in Rust.
                    is_enum = true;
                } else if meta.path.is_ident("uuid")
                    || meta.path.is_ident("timestamp")
                    || meta.path.is_ident("created_timestamp")
                    || meta.path.is_ident("updated_timestamp")
                    || meta.path.is_ident("private")
                    || meta.path.is_ident("column")
                {
                    // These were removed. A scalar's type is now its Rust type
                    // (no `uuid`/`timestamp` markers); the core no longer stamps
                    // timestamps or maps storage columns, and `private` was
                    // cosmetic. Timestamps are a consumer `Change`; column
                    // mapping is the `DataLayer`'s concern.
                    return Err(meta.error(
                        "`uuid`/`timestamp`/`created_timestamp`/`updated_timestamp`/`private`/`column` \
                         are no longer accepted. A scalar's type is its Rust type; a primary key is a \
                         plain field marked `primary_key`; timestamps are a consumer `Change`; storage \
                         column mapping is the `DataLayer`'s job",
                    ));
                } else if meta.path.is_ident("required")
                    || meta.path.is_ident("min")
                    || meta.path.is_ident("max")
                    || meta.path.is_ident("min_len")
                    || meta.path.is_ident("max_len")
                    || meta.path.is_ident("non_empty")
                    || meta.path.is_ident("one_of")
                {
                    // Built-in attribute validation was removed: the core no
                    // longer checks presence, ranges, lengths, or membership.
                    // Enforce these in a `Validation` registered on the write
                    // action instead (see `ash_domain::action::Validation`).
                    return Err(meta.error(
                        "built-in attribute validation was removed; \
                         `required`/`min`/`max`/`min_len`/`max_len`/`non_empty`/`one_of` \
                         are no longer accepted. Register a `Validation` on the action instead \
                         (see ash_domain::action::Validation)",
                    ));
                } else {
                    return Err(meta.error("unknown `attribute` option"));
                }
                Ok(())
            })?;
        }

        // A field typed `Option<T>` is nullable: its attribute type and its
        // record conversion are driven by the inner `T`, and an absent or null
        // value maps to `None` instead of erroring.
        let is_option = option_inner(&field.ty).is_some();
        let value_ty = option_inner(&field.ty).unwrap_or(&field.ty);

        // An `#[attribute(embed)]` field carries another `Embeddable` type. Its
        // `AttrType` and conversion come from that type's `Embeddable` /
        // `FromRecord` impls; a `Vec<T>` embed becomes an `_EmbedList`. The
        // innermost `T` drives the conversion.
        let embed_list = is_embed && vec_inner(value_ty).is_some();
        let embed_ty = vec_inner(value_ty).unwrap_or(value_ty);
        if is_embed {
            let variant = if embed_list {
                quote!(_EmbedList)
            } else {
                quote!(_Embed)
            };
            ty_override = Some(quote!(
                ::ash_domain::attribute::AttrType::#variant(
                    <#embed_ty as ::ash_domain::Embeddable>::embed_attributes()
                )
            ));
        }

        // Attribute metadata: a base `Attribute::new(...)`, refined by mutating
        // its public fields. A scalar's type is its Rust type (`AttrType::scalar`).
        let ty_expr = ty_override
            .clone()
            .unwrap_or_else(|| scalar_attr_type(value_ty));
        let mut refinements = Vec::new();
        if let Some(default) = &default_value {
            refinements.push(quote!(__attr.default = ::std::option::Option::Some(#default);));
        }
        attribute_tokens.push(quote! {
            {
                let mut __attr = ::ash_domain::attribute::Attribute::new(#field_name, #ty_expr);
                #(#refinements)*
                __attr
            }
        });

        // Record <-> struct conversions, chosen from the (inner) Rust type. An
        // embed round-trips through the embedded type's `FromRecord`/`IntoRecord`
        // and a `Value::Map`; the conversion type is the innermost `T`.
        if is_enum && is_embed {
            return Err(syn::Error::new_spanned(
                field,
                "a field cannot be both `#[attribute(enumerate)]` and `#[attribute(embed)]`",
            ));
        }
        let conv = if is_enum {
            Conv::Enum
        } else if is_embed {
            Conv::Embed {
                is_list: embed_list,
            }
        } else {
            rust_type_to_conv(value_ty)
        };
        let conv_ty = if is_embed { embed_ty } else { value_ty };
        // Reads a `Value` bound to `__v` into the inner type — with a checked
        // (non-truncating) cast for integers.
        let read_inner = read_inner_tokens(conv, conv_ty, &field_name);
        // Turns an owned inner value into a `Result<Value>` (fallible: an
        // out-of-`i64`-range integer or a failing nested embed errors here).
        let write_owned =
            |owned: proc_macro2::TokenStream| write_owned_tokens(conv, owned, &field_name);

        let from_body = if is_option {
            quote! {
                match record.get(#field_name) {
                    ::std::option::Option::Some(__v) if !__v.is_null() => {
                        ::std::option::Option::Some({ #read_inner })
                    }
                    _ => ::std::option::Option::None,
                }
            }
        } else {
            quote! {
                {
                    let __v = record.get(#field_name).ok_or_else(|| {
                        ::ash_domain::Error::Serialization(
                            ::std::format!("missing field `{}`", #field_name),
                        )
                    })?;
                    #read_inner
                }
            }
        };
        from_fields.push(quote!(#field_ident: #from_body));

        let into_stmt = if is_option {
            let some_value = write_owned(quote!(__x));
            quote! {
                match self.#field_ident {
                    ::std::option::Option::Some(__x) => {
                        __record.insert(#field_name, (#some_value)?);
                    }
                    ::std::option::Option::None => {
                        __record.insert(#field_name, ::ash_domain::Value::Null);
                    }
                }
            }
        } else {
            let value = write_owned(quote!(self.#field_ident));
            quote!(__record.insert(#field_name, (#value)?);)
        };
        into_fields.push(into_stmt);
    }

    Ok((attribute_tokens, from_fields, into_fields, primary_key_name))
}

/// Derive `ash_domain::Embeddable` (plus `FromRecord` / `IntoRecord`) for a struct
/// with named fields — a resource embedded *inside* another, with no name,
/// actions, or storage of its own. Uses the same `#[attribute(...)]` field
/// options as `Resource` (types, defaults, `column`).
#[proc_macro_derive(Embeddable, attributes(attribute))]
pub fn derive_embeddable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_embeddable(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand_embeddable(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = named_fields(&input, "Embeddable")?;
    let (attribute_tokens, from_fields, into_fields, _pk) = build_fields(fields)?;

    Ok(quote! {
        impl #impl_generics ::ash_domain::Embeddable for #ident #ty_generics #where_clause {
            fn embed_attributes() -> ::std::vec::Vec<::ash_domain::attribute::Attribute> {
                ::std::vec![ #(#attribute_tokens),* ]
            }
        }

        impl #impl_generics ::ash_domain::FromRecord for #ident #ty_generics #where_clause {
            fn from_record(record: &::ash_domain::Record) -> ::ash_domain::Result<Self> {
                ::std::result::Result::Ok(Self { #(#from_fields),* })
            }
        }

        impl #impl_generics ::ash_domain::IntoRecord for #ident #ty_generics #where_clause {
            fn into_record(self) -> ::ash_domain::Result<::ash_domain::Record> {
                let mut __record = ::ash_domain::Record::new();
                #(#into_fields)*
                ::ash_domain::Result::Ok(__record)
            }
        }
    })
}

/// Derive `ash_domain::FromRecord` for a struct with named fields — read a neutral
/// `Record` back into a typed struct without making it a full [`Resource`](macro@Resource).
///
/// A [`Resource`](macro@Resource) already generates this impl; derive it on its
/// own for a **plain data type** that isn't a resource: a read projection, a
/// generic action's typed output, or any struct you want to hydrate from the
/// dynamic `Record` the pipeline flows. The field conversions are identical to a
/// resource's — `Option<T>` fields are nullable, integers are range-checked, and
/// `#[attribute(embed)]` fields round-trip through a nested type's `FromRecord`:
///
/// ```ignore
/// use ash_domain::{FromRecord, Record, Value};
///
/// #[derive(FromRecord)]
/// struct AuthorRow {
///     id: i64,
///     name: String,
///     bio: Option<String>,
/// }
///
/// let rec = Record::from_iter([("id", Value::Int(7)), ("name", Value::from("Ada"))]);
/// let row = AuthorRow::from_record(&rec).unwrap();
/// assert_eq!(row.id, 7);
/// assert_eq!(row.name, "Ada");
/// assert_eq!(row.bio, None); // absent → None
/// ```
#[proc_macro_derive(FromRecord, attributes(attribute))]
pub fn derive_from_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_record_conv(input, RecordConv::From) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive `ash_domain::IntoRecord` for a struct with named fields — flatten a typed
/// struct into the neutral `Record` the action pipeline consumes.
///
/// A [`Resource`](macro@Resource) already generates this impl; derive it on its
/// own for a **typed action-params type** so you can pass a struct where the
/// `Domain` write methods want `impl IntoRecord`, instead of hand-building a
/// `Record::from_iter([...])`:
///
/// ```ignore
/// use ash_domain::{IntoRecord, Value};
///
/// #[derive(IntoRecord)]
/// struct NewNote {
///     title: String,
///     done: bool,
/// }
///
/// // `into_record` is fallible (an out-of-`i64` integer field errors); these
/// // string/bool fields can only succeed.
/// let rec = NewNote { title: "hi".into(), done: false }.into_record().unwrap();
/// assert_eq!(rec.get("title"), Some(&Value::from("hi")));
/// assert_eq!(rec.get("done"), Some(&Value::Bool(false)));
/// // `Note::create(&domain, &mut ctx, NewNote { .. })` now type-checks.
/// ```
#[proc_macro_derive(IntoRecord, attributes(attribute))]
pub fn derive_into_record(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_record_conv(input, RecordConv::Into) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive `ash_domain::FromValue` and `From<Self> for ash_domain::Value` for a
/// **fieldless enum** — a closed set of variants stored as a string.
///
/// This is how a resource field gets a real Rust enum instead of a bare
/// `String`: mark the field `#[attribute(enumerate)]` on the resource and derive
/// `ValueEnum` on its type. Each variant round-trips as a [`Value::Str`] of its
/// name (lower-cased by default, or a `#[value(rename = "...")]` override), and a
/// string that matches no variant is an [`Error::Serialization`] on read — so an
/// unknown status can never enter a typed row.
///
/// Only enums whose variants are all unit (no fields) are supported; a variant
/// with data is a compile error.
///
/// ```ignore
/// use ash_domain::{FromValue, Value};
///
/// #[derive(ValueEnum, PartialEq, Debug)]
/// enum Status {
///     Open,
///     Done,
///     #[value(rename = "in_progress")]
///     InProgress,
/// }
///
/// assert_eq!(Value::from(Status::Open), Value::from("open"));
/// assert_eq!(Value::from(Status::InProgress), Value::from("in_progress"));
/// assert_eq!(Status::from_value(&Value::from("done")).unwrap(), Status::Done);
/// assert!(Status::from_value(&Value::from("nope")).is_err());
/// ```
#[proc_macro_derive(ValueEnum, attributes(value))]
pub fn derive_value_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_value_enum(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Emit the `FromValue` + `From<Self> for Value` impls for a fieldless enum.
fn expand_value_enum(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            ident,
            "`ValueEnum` can only be derived for an enum",
        ));
    };
    if data.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            ident,
            "`ValueEnum` needs at least one variant",
        ));
    }

    // (variant ident, wire string) for every unit variant.
    let mut pairs: Vec<(syn::Ident, String)> = Vec::new();
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "`ValueEnum` supports only fieldless (unit) variants",
            ));
        }
        let wire = variant_wire_name(variant)?;
        pairs.push((variant.ident.clone(), wire));
    }

    // Reject two variants that would serialize to the same string — otherwise a
    // round-trip is ambiguous and `from_value` would silently pick the first.
    for i in 0..pairs.len() {
        for j in (i + 1)..pairs.len() {
            if pairs[i].1 == pairs[j].1 {
                return Err(syn::Error::new_spanned(
                    &data.variants[j],
                    format!("two variants map to the same string `{}`", pairs[j].1),
                ));
            }
        }
    }

    let into_arms = pairs
        .iter()
        .map(|(v, wire)| quote!(#ident::#v => ::ash_domain::Value::Str(#wire.to_string())));
    let from_arms = pairs
        .iter()
        .map(|(v, wire)| quote!(#wire => ::std::result::Result::Ok(#ident::#v)));
    let type_name = ident.to_string();
    let known: Vec<&str> = pairs.iter().map(|(_, w)| w.as_str()).collect();
    let known_list = known.join(", ");

    Ok(quote! {
        impl #impl_generics ::std::convert::From<#ident #ty_generics> for ::ash_domain::Value #where_clause {
            fn from(__v: #ident #ty_generics) -> Self {
                match __v {
                    #(#into_arms),*
                }
            }
        }

        impl #impl_generics ::ash_domain::FromValue for #ident #ty_generics #where_clause {
            fn from_value(__value: &::ash_domain::Value) -> ::ash_domain::Result<Self> {
                let __s = <::std::string::String as ::ash_domain::FromValue>::from_value(__value)?;
                match __s.as_str() {
                    #(#from_arms,)*
                    __other => ::std::result::Result::Err(::ash_domain::Error::Serialization(
                        ::std::format!(
                            "`{}` is not a valid {} (expected one of: {})",
                            __other, #type_name, #known_list,
                        ),
                    )),
                }
            }
        }
    })
}

/// The wire string for a `ValueEnum` variant: `#[value(rename = "...")]` if given,
/// otherwise the variant name lower-cased.
fn variant_wire_name(variant: &syn::Variant) -> syn::Result<String> {
    let mut rename: Option<String> = None;
    for attr in &variant.attrs {
        if !attr.path().is_ident("value") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let lit: Lit = meta.value()?.parse()?;
                match lit {
                    Lit::Str(s) => rename = Some(s.value()),
                    other => {
                        return Err(syn::Error::new_spanned(other, "`rename` expects a string"));
                    }
                }
                Ok(())
            } else {
                Err(meta.error("unknown `value` option (expected `rename`)"))
            }
        })?;
    }
    Ok(rename.unwrap_or_else(|| variant.ident.to_string().to_lowercase()))
}

/// Which half of the `Record` conversion a standalone derive emits.
#[derive(Clone, Copy)]
enum RecordConv {
    From,
    Into,
}

/// Shared body of the standalone [`FromRecord`](macro@FromRecord) /
/// [`IntoRecord`](macro@IntoRecord) derives: reuse [`build_fields`] and emit only
/// the requested conversion impl.
fn expand_record_conv(
    input: DeriveInput,
    which: RecordConv,
) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let trait_name = match which {
        RecordConv::From => "FromRecord",
        RecordConv::Into => "IntoRecord",
    };
    let fields = named_fields(&input, trait_name)?;
    let (_attribute_tokens, from_fields, into_fields, _pk) = build_fields(fields)?;

    Ok(match which {
        RecordConv::From => quote! {
            impl #impl_generics ::ash_domain::FromRecord for #ident #ty_generics #where_clause {
                fn from_record(record: &::ash_domain::Record) -> ::ash_domain::Result<Self> {
                    ::std::result::Result::Ok(Self { #(#from_fields),* })
                }
            }
        },
        RecordConv::Into => quote! {
            impl #impl_generics ::ash_domain::IntoRecord for #ident #ty_generics #where_clause {
                fn into_record(self) -> ::ash_domain::Result<::ash_domain::Record> {
                    let mut __record = ::ash_domain::Record::new();
                    #(#into_fields)*
                    ::ash_domain::Result::Ok(__record)
                }
            }
        },
    })
}

/// Tokens that read the `Value` bound to `__v` into the field's inner type.
/// Integers go through `TryFrom<i64>` so an out-of-range value is a
/// `Serialization` error rather than a silent truncating `as` cast.
fn read_inner_tokens(conv: Conv, target: &Type, field_name: &str) -> proc_macro2::TokenStream {
    match conv {
        Conv::Str => quote!(<::std::string::String as ::ash_domain::FromValue>::from_value(__v)?),
        Conv::Bool => quote!(<bool as ::ash_domain::FromValue>::from_value(__v)?),
        Conv::Int => quote! {{
            let __n = <i64 as ::ash_domain::FromValue>::from_value(__v)?;
            <#target as ::core::convert::TryFrom<i64>>::try_from(__n).map_err(|_| {
                ::ash_domain::Error::Serialization(::std::format!(
                    "field `{}`: {} is out of range for {}",
                    #field_name,
                    __n,
                    ::std::stringify!(#target),
                ))
            })?
        }},
        Conv::Float => quote!(<f64 as ::ash_domain::FromValue>::from_value(__v)? as #target),
        Conv::Bytes => quote!(<::std::vec::Vec<u8> as ::ash_domain::FromValue>::from_value(__v)?),
        // A `ValueEnum` field reads through its own `FromValue` — `#target` is the
        // enum type, whose derive rejects any string that isn't a known variant.
        Conv::Enum => quote!(<#target as ::ash_domain::FromValue>::from_value(__v)?),
        // A single embed reads its `Value::Map` back through the embedded type's
        // `FromRecord`; `#target` is the innermost embedded type `T`.
        Conv::Embed { is_list: false } => quote! {{
            let __m = __v.as_map().ok_or_else(|| {
                ::ash_domain::Error::Serialization(::std::format!(
                    "field `{}`: expected an embedded map", #field_name,
                ))
            })?;
            let __rec = ::ash_domain::Record(__m.clone());
            <#target as ::ash_domain::FromRecord>::from_record(&__rec)?
        }},
        // A repeated embed reads a `Value::List` of maps into a `Vec<T>`.
        Conv::Embed { is_list: true } => quote! {{
            let __items = __v.as_list().ok_or_else(|| {
                ::ash_domain::Error::Serialization(::std::format!(
                    "field `{}`: expected a list of embedded maps", #field_name,
                ))
            })?;
            let mut __out = ::std::vec::Vec::with_capacity(__items.len());
            for __it in __items {
                let __m = __it.as_map().ok_or_else(|| {
                    ::ash_domain::Error::Serialization(::std::format!(
                        "field `{}`: expected embedded maps", #field_name,
                    ))
                })?;
                __out.push(<#target as ::ash_domain::FromRecord>::from_record(
                    &::ash_domain::Record(__m.clone()),
                )?);
            }
            __out
        }},
    }
}

/// Tokens that turn an owned inner value (`owned`) into a **`Result<Value>`** —
/// fallible because integer and embed conversions can fail (an out-of-`i64`-range
/// integer; a nested embed whose own `IntoRecord` fails). Every arm therefore
/// yields a `Result<::ash_domain::Value, ::ash_domain::Error>`, so the caller
/// binds it with `?`.
fn write_owned_tokens(
    conv: Conv,
    owned: proc_macro2::TokenStream,
    field_name: &str,
) -> proc_macro2::TokenStream {
    match conv {
        // Infallible conversions, lifted into `Ok` so every arm has one type. A
        // `ValueEnum` writes through its generated `From<Enum> for Value` — also
        // infallible, since every variant maps to a fixed string.
        Conv::Str | Conv::Bool | Conv::Bytes | Conv::Enum => {
            quote!(::ash_domain::Result::<::ash_domain::Value>::Ok(::ash_domain::Value::from(#owned)))
        }
        // `Value::Int` is `i64`. A widening source (`i8..i32`, `u8..u32`) converts
        // losslessly and this `try_into` never errors; a `u64`/`usize`/`isize`
        // above `i64::MAX` errors here instead of silently wrapping — mirroring the
        // checked *read* path (see `read_inner_tokens`).
        Conv::Int => quote! {
            match ::core::convert::TryInto::<i64>::try_into(#owned) {
                ::core::result::Result::Ok(__n) => ::ash_domain::Result::Ok(::ash_domain::Value::Int(__n)),
                ::core::result::Result::Err(_) => ::ash_domain::Result::Err(
                    ::ash_domain::Error::Serialization(::std::format!(
                        "field `{}`: integer value is out of range for i64 storage",
                        #field_name,
                    )),
                ),
            }
        },
        // `f32 -> f64` widens losslessly; `f64 -> f64` is identity. No overflow.
        Conv::Float => {
            quote!(::ash_domain::Result::<::ash_domain::Value>::Ok(::ash_domain::Value::Float(#owned as f64)))
        }
        // A single embed writes as a `Value::Map` via its (now fallible) `IntoRecord`.
        Conv::Embed { is_list: false } => quote! {
            ::ash_domain::IntoRecord::into_record(#owned).map(::ash_domain::Value::from)
        },
        // A repeated embed writes as a `Value::List` of maps; any element's
        // conversion failing fails the whole list.
        Conv::Embed { is_list: true } => quote! {{
            let mut __items = ::std::vec::Vec::new();
            for __e in #owned.into_iter() {
                __items.push(::ash_domain::Value::from(
                    ::ash_domain::IntoRecord::into_record(__e)?,
                ));
            }
            ::ash_domain::Result::<::ash_domain::Value>::Ok(::ash_domain::Value::List(__items))
        }},
    }
}

/// If `ty` is `Option<T>`, return `T`; otherwise `None`.
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

/// If `ty` is `Vec<T>`, return `T`; otherwise `None`. Used to tell a single
/// embed (`T`) from a repeated one (`Vec<T>`).
fn vec_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

/// Convert a literal default into an `ash_domain::Value` construction.
fn lit_to_value(lit: &Lit) -> syn::Result<proc_macro2::TokenStream> {
    Ok(match lit {
        Lit::Bool(b) => quote!(::ash_domain::Value::Bool(#b)),
        Lit::Int(i) => quote!(::ash_domain::Value::Int(#i as i64)),
        Lit::Float(f) => quote!(::ash_domain::Value::Float(#f as f64)),
        Lit::Str(s) => quote!(::ash_domain::Value::Str(#s.to_string())),
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "unsupported default literal (expected bool, int, float, or string)",
            ));
        }
    })
}

/// A scalar `AttrType` carrying the field's own Rust type — the type *is* the
/// Rust type, so there is no primitive-mapping table. A `&str` field (rare for an
/// owned resource) maps to `String` since `str` is unsized; every other type is
/// used as-is via `AttrType::scalar::<T>()`.
fn scalar_attr_type(ty: &Type) -> proc_macro2::TokenStream {
    match last_segment_ident(ty).as_deref() {
        Some("str") => quote!(::ash_domain::attribute::AttrType::scalar::<
            ::std::string::String,
        >()),
        _ => quote!(::ash_domain::attribute::AttrType::scalar::<#ty>()),
    }
}

/// Choose the record-conversion strategy from a field's Rust type. Keyed to the
/// concrete Rust type so the generated `as` casts are correct.
fn rust_type_to_conv(ty: &Type) -> Conv {
    match last_segment_ident(ty).as_deref() {
        Some("bool") => Conv::Bool,
        Some("i64") | Some("i32") | Some("i16") | Some("i8") | Some("u64") | Some("u32")
        | Some("u16") | Some("u8") | Some("usize") | Some("isize") => Conv::Int,
        Some("f64") | Some("f32") => Conv::Float,
        Some("Bytes") | Some("Vec") => Conv::Bytes,
        // `String`, `str`, and anything unknown round-trip as a string.
        _ => Conv::Str,
    }
}

fn last_segment_ident(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) => path.path.segments.last().map(|s| s.ident.to_string()),
        _ => None,
    }
}

/// Parse each `#[relationship(name = "...", <cardinality>, destination = "...",
/// source = "...", destination_attr = "...")]` into a `Relationship` literal.
/// `<cardinality>` is one of `belongs_to` / `has_one` / `has_many` /
/// `many_to_many`.
fn parse_relationships(attrs: &[syn::Attribute]) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("relationship") {
            continue;
        }
        let mut name: Option<String> = None;
        let mut destination: Option<String> = None;
        let mut source: Option<String> = None;
        let mut destination_attr: Option<String> = None;
        let mut cardinality: Option<proc_macro2::TokenStream> = None;
        // many_to_many join resource, set via `through`, `through_source`,
        // `through_destination`.
        let mut through_resource: Option<String> = None;
        let mut through_source: Option<String> = None;
        let mut through_destination: Option<String> = None;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(str_value(&meta)?);
            } else if meta.path.is_ident("destination") {
                destination = Some(str_value(&meta)?);
            } else if meta.path.is_ident("source") {
                source = Some(str_value(&meta)?);
            } else if meta.path.is_ident("destination_attr") {
                destination_attr = Some(str_value(&meta)?);
            } else if meta.path.is_ident("through") {
                through_resource = Some(str_value(&meta)?);
            } else if meta.path.is_ident("through_source") {
                through_source = Some(str_value(&meta)?);
            } else if meta.path.is_ident("through_destination") {
                through_destination = Some(str_value(&meta)?);
            } else if meta.path.is_ident("belongs_to") {
                cardinality = Some(quote!(::ash_domain::Cardinality::BelongsTo));
            } else if meta.path.is_ident("has_one") {
                cardinality = Some(quote!(::ash_domain::Cardinality::HasOne));
            } else if meta.path.is_ident("has_many") {
                cardinality = Some(quote!(::ash_domain::Cardinality::HasMany));
            } else if meta.path.is_ident("many_to_many") {
                cardinality = Some(quote!(::ash_domain::Cardinality::ManyToMany));
            } else {
                return Err(meta.error("unknown `relationship` option"));
            }
            Ok(())
        })?;

        let name = name.ok_or_else(|| err(attr, "`relationship` requires `name = \"...\"`"))?;
        let destination = destination
            .ok_or_else(|| err(attr, "`relationship` requires `destination = \"...\"`"))?;
        let source =
            source.ok_or_else(|| err(attr, "`relationship` requires `source = \"...\"`"))?;
        let destination_attr = destination_attr
            .ok_or_else(|| err(attr, "`relationship` requires `destination_attr = \"...\"`"))?;
        let cardinality = cardinality.ok_or_else(|| {
            err(
                attr,
                "`relationship` requires a cardinality (`belongs_to`, `has_one`, `has_many`, or `many_to_many`)",
            )
        })?;

        // The `through` join resource is required iff any through-field is given;
        // all three must appear together.
        let through_tokens = match (through_resource, through_source, through_destination) {
            (None, None, None) => quote!(::std::option::Option::None),
            (Some(r), Some(s), Some(d)) => quote!(::std::option::Option::Some(
                ::ash_domain::Through {
                    resource: #r.to_string(),
                    source_attribute: #s.to_string(),
                    destination_attribute: #d.to_string(),
                }
            )),
            _ => {
                return Err(err(
                    attr,
                    "a `through` relationship needs all of `through`, `through_source`, and `through_destination`",
                ));
            }
        };

        out.push(quote! {
            ::ash_domain::Relationship {
                name: #name.to_string(),
                destination: #destination.to_string(),
                cardinality: #cardinality,
                source_attribute: #source.to_string(),
                destination_attribute: #destination_attr.to_string(),
                through: #through_tokens,
            }
        });
    }
    Ok(out)
}

/// Parse each `#[aggregate(<kind>, name = "...", relationship = "...", field =
/// "...")]` into an `Aggregate` literal. `<kind>` is one of `count` / `sum` /
/// `min` / `max` / `exists`; `field` is required for `sum`/`min`/`max`.
fn parse_aggregates(attrs: &[syn::Attribute]) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("aggregate") {
            continue;
        }
        let mut name: Option<String> = None;
        let mut relationship: Option<String> = None;
        let mut field: Option<String> = None;
        let mut kind: Option<proc_macro2::TokenStream> = None;
        let mut needs_field = false;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(str_value(&meta)?);
            } else if meta.path.is_ident("relationship") {
                relationship = Some(str_value(&meta)?);
            } else if meta.path.is_ident("field") {
                field = Some(str_value(&meta)?);
            } else if meta.path.is_ident("count") {
                kind = Some(quote!(::ash_domain::AggregateKind::Count));
            } else if meta.path.is_ident("exists") {
                kind = Some(quote!(::ash_domain::AggregateKind::Exists));
            } else if meta.path.is_ident("sum") {
                kind = Some(quote!(::ash_domain::AggregateKind::Sum));
                needs_field = true;
            } else if meta.path.is_ident("min") {
                kind = Some(quote!(::ash_domain::AggregateKind::Min));
                needs_field = true;
            } else if meta.path.is_ident("max") {
                kind = Some(quote!(::ash_domain::AggregateKind::Max));
                needs_field = true;
            } else {
                return Err(meta.error("unknown `aggregate` option"));
            }
            Ok(())
        })?;

        let name = name.ok_or_else(|| err(attr, "`aggregate` requires `name = \"...\"`"))?;
        let relationship = relationship
            .ok_or_else(|| err(attr, "`aggregate` requires `relationship = \"...\"`"))?;
        let kind = kind.ok_or_else(|| {
            err(
                attr,
                "`aggregate` requires a kind (`count`, `sum`, `min`, `max`, or `exists`)",
            )
        })?;
        if needs_field && field.is_none() {
            return Err(err(attr, "this aggregate kind requires `field = \"...\"`"));
        }
        let field_tokens = match field {
            Some(f) => quote!(::std::option::Option::Some(#f.to_string())),
            None => quote!(::std::option::Option::None),
        };

        out.push(quote! {
            ::ash_domain::Aggregate {
                name: #name.to_string(),
                relationship: #relationship.to_string(),
                kind: #kind,
                field: #field_tokens,
            }
        });
    }
    Ok(out)
}

/// The typed-method signature a custom action's kind implies.
enum CustomKind {
    /// A create-shaped write: `(domain, ctx, params) -> Self`.
    Create,
    /// An update-shaped write: `(domain, ctx, id, params) -> Self`.
    Update,
    /// A destroy-shaped write: `(domain, ctx, id) -> ()`.
    Destroy,
    /// A read: `(domain, ctx, query) -> Vec<Self>`.
    Read,
}

/// Parse repeatable container attributes declaring **custom actions** beyond the
/// default CRUD set:
///
/// ```ignore
/// #[action(update, name = "completed")]   // Note::completed(&d, &mut ctx, id, params)
/// #[action(read,   name = "recent")]      // Note::recent(&d, &mut ctx, query)
/// ```
///
/// The attribute is *declarative*: it names the action and its shape so the derive
/// can emit the matching [`ActionDef`] and a compile-checked typed method that
/// dispatches through `Domain::handle_action`. The action's **behavior** (e.g. the
/// `Change` that sets `completed = true`) is attached separately as explicit code —
/// the derive declares, it does not implement.
///
/// Generic actions are deliberately *not* declarable here: a generic action needs
/// its handler at declaration time, so it stays on the hand-written `actions()`
/// path. Returns `(action_defs, typed_methods)`.
#[allow(clippy::type_complexity)]
fn parse_custom_actions(
    attrs: &[syn::Attribute],
    ident: &syn::Ident,
) -> syn::Result<(Vec<proc_macro2::TokenStream>, Vec<proc_macro2::TokenStream>)> {
    let mut defs = Vec::new();
    let mut methods = Vec::new();

    for attr in attrs {
        if !attr.path().is_ident("action") {
            continue;
        }
        let mut name: Option<String> = None;
        let mut kind: Option<CustomKind> = None;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(str_value(&meta)?);
            } else if meta.path.is_ident("create") {
                kind = Some(CustomKind::Create);
            } else if meta.path.is_ident("update") {
                kind = Some(CustomKind::Update);
            } else if meta.path.is_ident("destroy") {
                kind = Some(CustomKind::Destroy);
            } else if meta.path.is_ident("read") {
                kind = Some(CustomKind::Read);
            } else {
                return Err(meta.error(
                    "unknown `action` option (expected a kind — `create`, `update`, `destroy`, or `read` — or `name = \"...\"`)",
                ));
            }
            Ok(())
        })?;

        let name = name.ok_or_else(|| err(attr, "`action` requires `name = \"...\"`"))?;
        let kind = kind.ok_or_else(|| {
            err(
                attr,
                "`action` requires a kind (`create`, `update`, `destroy`, or `read`)",
            )
        })?;
        // The generated method carries the action name; it must be a valid Rust
        // identifier so the caller can write `Note::completed(…)`.
        let method_ident = syn::Ident::new(&name, ident.span());

        // Every write kind declares a `write` ActionDef; read declares `read`.
        let def = match kind {
            CustomKind::Read => quote!(::ash_domain::action::ActionDef::read(#name)),
            _ => quote!(::ash_domain::action::ActionDef::write(#name)),
        };
        defs.push(def);

        let method = match kind {
            CustomKind::Create => quote! {
                #[doc = concat!("Run the custom `", #name, "` action, returning the typed row.")]
                pub async fn #method_ident(
                    __domain: &::ash_domain::Domain,
                    __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                    __params: impl ::ash_domain::IntoRecord,
                ) -> ::ash_domain::Result<Self> {
                    __domain
                        .handle_action::<Self>(__ctx, #name, ::ash_domain::ActionInput::create(__params)?)
                        .await?
                        .into_data::<Self>()
                }
            },
            CustomKind::Update => quote! {
                #[doc = concat!("Run the custom `", #name, "` action on record `__id`, returning the typed row.")]
                pub async fn #method_ident(
                    __domain: &::ash_domain::Domain,
                    __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                    __id: ::ash_domain::Value,
                    __params: impl ::ash_domain::IntoRecord,
                ) -> ::ash_domain::Result<Self> {
                    __domain
                        .handle_action::<Self>(__ctx, #name, ::ash_domain::ActionInput::update(__id, __params)?)
                        .await?
                        .into_data::<Self>()
                }
            },
            CustomKind::Destroy => quote! {
                #[doc = concat!("Run the custom `", #name, "` action on record `__id`.")]
                pub async fn #method_ident(
                    __domain: &::ash_domain::Domain,
                    __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                    __id: ::ash_domain::Value,
                ) -> ::ash_domain::Result<()> {
                    __domain
                        .handle_action::<Self>(__ctx, #name, ::ash_domain::ActionInput::destroy(__id))
                        .await?;
                    ::std::result::Result::Ok(())
                }
            },
            CustomKind::Read => quote! {
                #[doc = concat!("Run the custom `", #name, "` read action.")]
                pub async fn #method_ident(
                    __domain: &::ash_domain::Domain,
                    __ctx: &mut ::ash_domain::Context<impl ::ash_domain::Store>,
                    __query: ::ash_domain::Query,
                ) -> ::ash_domain::Result<::std::vec::Vec<Self>> {
                    __domain
                        .handle_action::<Self>(__ctx, #name, ::ash_domain::ActionInput::read(__query))
                        .await?
                        .into_data_vec::<Self>()
                }
            },
        };
        methods.push(method);
    }

    Ok((defs, methods))
}

/// Read a `name = "value"` string from a nested-meta entry.
fn str_value(meta: &syn::meta::ParseNestedMeta) -> syn::Result<String> {
    let lit: syn::LitStr = meta.value()?.parse()?;
    Ok(lit.value())
}

fn err(attr: &syn::Attribute, msg: &str) -> syn::Error {
    syn::Error::new_spanned(attr, msg)
}
