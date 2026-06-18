//! Relay onboarding policy (RFD 0086): derive the `RelayPolicy` advertised to
//! clients from the relay's configuration and wired backends.

use kutl_proto::sync::{AuthModel, InviteModel, NextStep, RelayPolicy};

/// Inputs for deriving a relay's onboarding policy, captured at app-build time
/// (backend presence is not available on [`crate::config::RelayConfig`]).
pub struct RelayPolicyInputs {
    /// Whether the relay requires an authenticated account (vs. an anonymous DID).
    pub require_auth: bool,
    /// Whether an HTTP invite backend is wired (OSS capability-URL invites).
    pub has_invite_backend: bool,
    /// Whether a membership backend is wired (account-scoped membership).
    pub has_membership_backend: bool,
    /// Relay HTTP origin; the client derives the device-flow path from it.
    pub auth_url: String,
}

/// Build the [`RelayPolicy`] advertised to a client from the relay's
/// configuration inputs and the next step the client must take.
pub fn build_relay_policy(inputs: &RelayPolicyInputs, next: NextStep) -> RelayPolicy {
    let auth_model = if inputs.require_auth {
        AuthModel::AccountRequired
    } else {
        AuthModel::AnonymousDid
    };
    // A membership-backed relay with no HTTP invite backend redeems invites as
    // authenticated membership grants; otherwise invites are anonymous
    // capability URLs.
    let invite_model = if inputs.has_membership_backend && !inputs.has_invite_backend {
        InviteModel::MembershipGrant
    } else {
        InviteModel::CapabilityUrl
    };
    RelayPolicy {
        auth_model: i32::from(auth_model),
        invite_model: i32::from(invite_model),
        next: i32::from(next),
        auth_url: inputs.auth_url.clone(),
        // A direct relay does not know whether it sits behind a proxy, so it
        // advertises no explicit endpoint; the client falls back to
        // {discovery_origin}/ws. A front door (ux-server) populates this with the
        // proxied ws URL in its own policy response.
        relay_endpoint: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{RelayPolicyInputs, build_relay_policy};
    use kutl_proto::sync::{AuthModel, InviteModel, NextStep};

    #[test]
    fn test_build_relay_policy_oss_advertises_anonymous_capability() {
        let inputs = RelayPolicyInputs {
            require_auth: false,
            has_invite_backend: true,
            has_membership_backend: false,
            auth_url: String::new(),
        };
        let policy = build_relay_policy(&inputs, NextStep::Proceed);
        assert_eq!(policy.auth_model(), AuthModel::AnonymousDid);
        assert_eq!(policy.invite_model(), InviteModel::CapabilityUrl);
        assert_eq!(policy.next(), NextStep::Proceed);
    }

    #[test]
    fn test_build_relay_policy_membership_with_invite_backend_stays_capability() {
        // The only case where the `&& !has_invite_backend` guard is load-bearing:
        // a relay with BOTH an HTTP invite backend and a membership backend still
        // advertises capability-URL invites (the anonymous HTTP path), not a
        // membership grant. Dropping the guard would wrongly flip this to grant.
        let inputs = RelayPolicyInputs {
            require_auth: true,
            has_invite_backend: true,
            has_membership_backend: true,
            auth_url: String::new(),
        };
        let policy = build_relay_policy(&inputs, NextStep::Authenticate);
        assert_eq!(policy.invite_model(), InviteModel::CapabilityUrl);
    }

    #[test]
    fn test_build_relay_policy_commercial_advertises_account_membership() {
        let inputs = RelayPolicyInputs {
            require_auth: true,
            has_invite_backend: false,
            has_membership_backend: true,
            auth_url: "https://relay.kutlhub.com".to_owned(),
        };
        let policy = build_relay_policy(&inputs, NextStep::Authenticate);
        assert_eq!(policy.auth_model(), AuthModel::AccountRequired);
        assert_eq!(policy.invite_model(), InviteModel::MembershipGrant);
        assert_eq!(policy.next(), NextStep::Authenticate);
        assert_eq!(policy.auth_url, "https://relay.kutlhub.com");
    }

    #[test]
    fn test_relay_policy_json_shape_is_snake_case_int_enums() {
        // The CLI deserializes the policy with prost+serde (snake_case keys,
        // INTEGER enum values), and the ux-server gateway must reproduce that
        // exact shape. Pin it so a serde-config drift (camelCase / string enums /
        // rename) fails here, not silently in the CLI's resp.json() -> None.
        let policy = build_relay_policy(
            &RelayPolicyInputs {
                require_auth: true,
                has_invite_backend: false,
                has_membership_backend: true,
                auth_url: "https://relay".to_owned(),
            },
            NextStep::Authenticate,
        );
        let value = serde_json::to_value(&policy).expect("serialize policy");
        let obj = value
            .as_object()
            .expect("policy serializes to a JSON object");
        for key in [
            "auth_model",
            "invite_model",
            "next",
            "auth_url",
            "relay_endpoint",
        ] {
            assert!(
                obj.contains_key(key),
                "missing snake_case key {key}: {value}"
            );
        }
        // Enums must be integers, not strings.
        assert_eq!(obj["auth_model"], serde_json::json!(2)); // ACCOUNT_REQUIRED
        assert_eq!(obj["invite_model"], serde_json::json!(2)); // MEMBERSHIP_GRANT
        assert_eq!(obj["next"], serde_json::json!(2)); // AUTHENTICATE
    }
}
