//! Space authorization enforcement via newtype.
//!
//! [`AuthorizedSpace`] can only be constructed through [`Relay::authorize_space`],
//! ensuring that space-scoped operations cannot bypass ACL checks.

/// A space ID that has passed authorization.
///
/// Can only be constructed via [`Relay::authorize_space`]. Functions that
/// operate on space-scoped resources take `&AuthorizedSpace` instead of
/// `&str`, making authorization bypass a compile error.
#[derive(Debug, Clone)]
#[must_use]
pub struct AuthorizedSpace(String);

impl AuthorizedSpace {
    /// The authorized space ID.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Construct an `AuthorizedSpace` without checking ACL.
    ///
    /// Only for use inside `Relay::authorize_space`. Not public outside the crate.
    pub(crate) fn new_unchecked(space_id: String) -> Self {
        Self(space_id)
    }
}

impl std::fmt::Display for AuthorizedSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
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
