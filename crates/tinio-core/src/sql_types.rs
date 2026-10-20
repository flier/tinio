//! The contract types' PostgreSQL mapping, behind the crate's `sqlx`
//! feature.
//!
//! A contract type owns its PostgreSQL mapping, so the attributes that must
//! sit on a type definition stay beside it (`acl::OwnerId`, `object::Key`,
//! `version::{Id, State}`, `meta::version::Kind` and `bucket::Name` keep
//! their `cfg_attr(feature = "sqlx", derive(sqlx::Type))`), and every
//! hand-written `Type`/`Encode`/`Decode`/`PgHasArrayType` implementation,
//! its imports, and its sqlx-only helpers live here in one module. A backend
//! binds the contract types themselves and keeps no mirror.
//!
//! The mappings keep the domain's own guarantees:
//!
//! - [`Acl`] is the `acl` composite (its grants the `acl_grant` array, each
//!   grant's grantee the `acl_grantee` composite): a write is canonical (an
//!   unknown group URI dropped, the grant set sorted by the wire codec's key
//!   and deduped) and a read self-heals (an out-of-domain element dropped, an
//!   out-of-domain owner read as absent).
//! - [`Tags`] is `jsonb`: a stored value that is not an object of strings, or
//!   whose pairs violate the tag rules, reads as the empty set. The count cap
//!   on that read is [`crate::object::BUCKET_TAGS_MAX`], so a bucket row of up
//!   to 50 tags round-trips; object writes still reject above
//!   [`crate::object::OBJECT_TAGS_MAX`].
//! - [`Algorithm`] forwards to the column's `text` and its read is strict: a
//!   stored label outside the ten API names fails rather than inventing an
//!   algorithm.

use std::collections::BTreeMap;

use serde_json::{Value as JsonValue, from_value};
use sqlx::{
    Database, Decode, Encode, Postgres, Type,
    encode::IsNull,
    error::BoxDynError,
    postgres::{
        PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef,
        types::{PgRecordDecoder, PgRecordEncoder},
    },
    types::Json,
};

use crate::{
    acl::{Acl, Grant, Grantee, GroupUri, OwnerId, Permission},
    checksum::Algorithm,
    object::{BUCKET_TAGS_MAX, Tags},
};

/// `Acl` is a record type rather than a fieldless enum or a newtype, so its
/// composite carries the canonicalizing write and the healing read by hand:
/// the derive cannot drop an out-of-domain array element or rewrite an
/// out-of-domain owner.
impl Type<Postgres> for Acl {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("acl")
    }
}

impl<'q> Encode<'q, Postgres> for Acl {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut encoder = PgRecordEncoder::new(buf);
        encoder.encode(self.owner.as_deref())?;
        encoder.encode(self.canonical_grants())?;
        encoder.finish();
        Ok(IsNull::No)
    }
}

/// The `acl[]` array a batch write builds: `Acl` is a composite, so the array
/// type is derived from the element's own name.
impl PgHasArrayType for Acl {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::array_of("acl")
    }
}

impl<'r> Decode<'r, Postgres> for Acl {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut decoder = PgRecordDecoder::new(value)?;
        let owner: Option<String> = decoder.try_decode()?;
        let grants: Vec<Grant> = decoder.try_decode()?;
        // The two row elements re-join through the contract's own pair: a
        // stored owner outside the domain reads as absent, and a grant whose
        // grantee has no stored form is dropped -- the same self-heal the
        // grants wire applies.
        Ok(Self::from_parts(
            owner.as_deref().and_then(|owner| OwnerId::new(owner).ok()),
            grants
                .into_iter()
                .filter(|grant| Self::grant_key(grant).is_some())
                .collect(),
        ))
    }
}

/// The `acl_grant` composite: one [`Grant`]. The derive cannot reach it for
/// the same reason as [`Acl`] (the read must drop an out-of-domain element),
/// so the traits are hand-written.
impl Type<Postgres> for Grant {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("acl_grant")
    }
}

impl PgHasArrayType for Grant {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::array_of("acl_grant")
    }
}

impl<'q> Encode<'q, Postgres> for Grant {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let mut encoder = PgRecordEncoder::new(buf);
        encoder.encode(&self.grantee)?;
        encoder.encode(self.permission.as_wire())?;
        encoder.finish();
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Postgres> for Grant {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut decoder = PgRecordDecoder::new(value)?;
        let grantee: Grantee = decoder.try_decode()?;
        let permission: Option<String> = decoder.try_decode()?;
        Ok(
            match permission
                .as_deref()
                .and_then(|wire| Permission::from_wire(wire).ok())
            {
                Some(permission) => Self {
                    grantee,
                    permission,
                },
                // A stored permission outside the five AWS wires has no grant
                // to name: the sentinel grantee makes `Acl`'s read drop it.
                None => Self {
                    grantee: Grantee::unrepresentable(),
                    permission: Permission::Read,
                },
            },
        )
    }
}

/// The `acl_grantee` composite: one [`Grantee`]. `Grantee` is an enum with
/// data, which sqlx's `Type` derive cannot express, so the traits are
/// hand-written -- with the lenient read the composite's self-heal needs.
impl Type<Postgres> for Grantee {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("acl_grantee")
    }
}

impl<'q> Encode<'q, Postgres> for Grantee {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let (kind, value) = match self {
            Grantee::Canonical(id) => ("canonical", id.as_str()),
            Grantee::Group(uri) => ("group", uri.0.as_str()),
        };
        let mut encoder = PgRecordEncoder::new(buf);
        encoder.encode(kind)?;
        encoder.encode(value)?;
        encoder.finish();
        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Postgres> for Grantee {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut decoder = PgRecordDecoder::new(value)?;
        let kind: Option<String> = decoder.try_decode()?;
        let raw: Option<String> = decoder.try_decode()?;
        Ok(match (kind.as_deref(), raw) {
            (Some("canonical"), Some(raw)) => match OwnerId::new(raw) {
                Ok(owner) => Self::Canonical(owner),
                // An invalid canonical ID has no stored form; the sentinel
                // makes `Acl`'s read drop the grant.
                Err(_) => Self::unrepresentable(),
            },
            (Some("group"), Some(raw)) => Self::Group(GroupUri(raw)),
            _ => Self::unrepresentable(),
        })
    }
}

impl Grantee {
    /// The self-heal sentinel: `Group` with the empty URI, which is never one
    /// of the three documented constants, so `Acl::grant_key` rejects it and
    /// the surrounding grant is dropped.
    fn unrepresentable() -> Self {
        Self::Group(GroupUri(String::new()))
    }
}

// The traits a backend binds a statement parameter with and decodes a column
// into. The `sqlx::Type` derive cannot reach `Tags`: `#[sqlx(transparent)]`
// would forward to the invariant-carrying `BTreeMap`, which no backend spells
// as a statement parameter, and the column is PostgreSQL's `jsonb` -- an object
// of string to string. The three traits forward to sqlx's own JSON adapter by
// hand, and `Decode` keeps the read path's self-heal: a value that is not an
// object of strings, or one whose pairs violate the tag rules, reads as the
// empty set, never an error. The count cap is the bucket cap: one decode
// serves object and bucket columns, and the object cap stays on
// [`Tags::from_pairs`].
impl<DB: Database> Type<DB> for Tags
where
    Json<BTreeMap<String, String>>: Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <Json<BTreeMap<String, String>> as Type<DB>>::type_info()
    }

    fn compatible(ty: &DB::TypeInfo) -> bool {
        <Json<BTreeMap<String, String>> as Type<DB>>::compatible(ty)
    }
}

/// The `jsonb[]` array a batch write builds: the element's own type is the
/// adapter's, so the array type is derived by name.
impl PgHasArrayType for Tags {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::array_of("jsonb")
    }
}

impl<'q, DB: Database> Encode<'q, DB> for Tags
where
    for<'a> Json<&'a BTreeMap<String, String>>: Encode<'q, DB>,
{
    fn encode_by_ref(&self, buf: &mut DB::ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <Json<&BTreeMap<String, String>> as Encode<'q, DB>>::encode_by_ref(&Json(&self.0), buf)
    }
}

impl<'r, DB: Database> Decode<'r, DB> for Tags
where
    JsonValue: Decode<'r, DB>,
{
    fn decode(value: DB::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let stored = <JsonValue as Decode<'r, DB>>::decode(value)?;
        Ok(from_value::<BTreeMap<String, String>>(stored)
            .ok()
            .and_then(|pairs| Self::from_pairs_limited(pairs, BUCKET_TAGS_MAX).ok())
            .unwrap_or_default())
    }
}

// The traits a backend binds the checksum-algorithm column with. The column
// stores the uppercase wire name (`wire_name`), which is also the derived
// `Display`/`FromStr` spelling, so the three traits forward to `String` by
// hand. `Decode` is strict, unlike the composite's self-heal: a stored label
// outside the API's ten names has no algorithm to fall back to, so the read
// fails rather than inventing one.
impl<DB: Database> Type<DB> for Algorithm
where
    String: Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <String as Type<DB>>::type_info()
    }

    fn compatible(ty: &DB::TypeInfo) -> bool {
        <String as Type<DB>>::compatible(ty)
    }
}

impl<'q, DB: Database> Encode<'q, DB> for Algorithm
where
    String: Encode<'q, DB>,
{
    fn encode_by_ref(&self, buf: &mut DB::ArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <String as Encode<'q, DB>>::encode_by_ref(&self.wire_name().to_owned(), buf)
    }
}

impl<'r, DB: Database> Decode<'r, DB> for Algorithm
where
    String: Decode<'r, DB>,
{
    fn decode(value: DB::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let wire = <String as Decode<'r, DB>>::decode(value)?;
        wire.parse().map_err(Into::into)
    }
}
