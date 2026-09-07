//! Space identity and authorization enforcement, via newtypes.
//!
//! Two invariants, and they arrive together. [`SpaceId`] is a space id that is
//! a UUID *by construction*; [`AuthorizedSpace`] is one that has additionally
//! passed an ACL check, and can only be built by `Relay::authorize_space`.
//!
//! # Why the id is a type and not a `&str`
//!
//! Space ids are relay-minted UUIDs — `SpaceBackend::register` mints
//! `Uuid::new_v4()`, and the hosted deployment types the column `uuid`. Nothing
//! enforced that at the boundary, so every consumer defended: a dozen sites
//! called `Uuid::parse_str(space_id)` and invented an error for a shape the
//! system cannot produce.
//!
//! This is the fix migration `0028_signals_document_id_uuid` applied to
//! `document_id` one layer down, for the reason it gives: bridging the types at
//! each use site is worse than aligning the type once. Strict by design.

/// A space id, guaranteed to be a UUID.
///
/// Constructible only by [`SpaceId::parse`], so holding one is proof the id is
/// well-formed and no consumer needs to re-check.
///
/// Carries the text alongside the UUID deliberately. A space id is used as a
/// map key, a SQL bind and a log field at least as often as it is used to
/// address a segment directory, so storing both trades 16 bytes for never
/// re-formatting and never re-parsing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpaceId {
    text: String,
    uuid: uuid::Uuid,
}

impl SpaceId {
    /// Parse a space id, rejecting anything that is not a UUID.
    ///
    /// # Errors
    ///
    /// [`MalformedSpaceId`] when the string is not a UUID.
    pub fn parse(space_id: &str) -> Result<Self, MalformedSpaceId> {
        uuid::Uuid::parse_str(space_id).map_or_else(
            |_| {
                Err(MalformedSpaceId {
                    got: space_id.to_owned(),
                })
            },
            |uuid| {
                Ok(Self {
                    text: space_id.to_owned(),
                    uuid,
                })
            },
        )
    }

    /// The id as text — for SQL binds, map keys and log fields.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The id as a UUID — for addressing a space's segment directory.
    #[must_use]
    pub fn uuid(&self) -> uuid::Uuid {
        self.uuid
    }
}

impl std::fmt::Display for SpaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

/// A space id that is not a UUID.
#[derive(Debug)]
pub struct MalformedSpaceId {
    /// The string that was offered.
    pub got: String,
}

impl std::fmt::Display for MalformedSpaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "space_id must be a UUID; got {:?}", self.got)
    }
}

impl std::error::Error for MalformedSpaceId {}

/// A space ID that has passed authorization.
///
/// Can only be constructed via [`Relay::authorize_space`]. Functions that
/// operate on space-scoped resources take `&AuthorizedSpace` instead of
/// `&str`, making authorization bypass a compile error — and because it holds a
/// [`SpaceId`], holding one is also proof the id is well-formed.
#[derive(Debug, Clone)]
#[must_use]
pub struct AuthorizedSpace(SpaceId);

impl AuthorizedSpace {
    /// The authorized space ID.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The authorized space id as a UUID — no re-parse at the use site.
    #[must_use]
    pub fn uuid(&self) -> uuid::Uuid {
        self.0.uuid()
    }

    /// Construct an `AuthorizedSpace` without checking ACL.
    ///
    /// Only for use inside `Relay::authorize_space`. Not public outside the crate.
    pub(crate) fn new_unchecked(space_id: SpaceId) -> Self {
        Self(space_id)
    }
}

impl std::fmt::Display for AuthorizedSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// Authorization check failure.
#[derive(Debug)]
pub enum AuthError {
    /// Connection is not authenticated.
    NotAuthenticated,
    /// Identity is not authorized for this space.
    NotAuthorized { space_id: String },
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAuthenticated => write!(f, "not authenticated"),
            Self::NotAuthorized { space_id } => {
                write!(f, "not authorized for space {space_id}")
            }
        }
    }
}
