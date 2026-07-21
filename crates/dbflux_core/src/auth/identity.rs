use uuid::Uuid;

/// Namespace UUID for all DBFlux-derived AWS auth profile identities.
///
/// Every AWS profile UUID in DBFlux is derived as `UUIDv5(AWS_AUTH_NAMESPACE,
/// provider_id + ":" + profile_name)`. This constant is **frozen**: changing it
/// after the first release would silently orphan every stored
/// `ConnectionProfile.auth_profile_id` that references an AWS profile, breaking
/// all existing connections. Do not change this value.
pub const AWS_AUTH_NAMESPACE: Uuid = Uuid::from_u128(0x5fab674f_2a8b_4668_9b06_9d0e1ad74f7a);

/// Derive a deterministic, stable UUID for an AWS auth profile.
///
/// Identity is `UUIDv5(AWS_AUTH_NAMESPACE, "{provider_id}:{name}")`.
///
/// ## Stability guarantees
///
/// - Same `provider_id` + `name` always yields the same UUID across process
///   restarts, machines, and DBFlux versions (as long as `AWS_AUTH_NAMESPACE`
///   remains frozen).
/// - Different `provider_id` values for the same `name` produce different UUIDs,
///   preventing collisions between `aws-sso`, `aws-sso-session`, and
///   `aws-shared-credentials` providers.
/// - Name comparison is case-sensitive: `"Prod"` and `"prod"` produce different
///   UUIDs, matching the AWS CLI's own case-sensitive profile naming.
pub fn aws_profile_uuid(provider_id: &str, name: &str) -> Uuid {
    Uuid::new_v5(
        &AWS_AUTH_NAMESPACE,
        format!("{provider_id}:{name}").as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // T-1.3a: Determinism — identical inputs always produce the same UUID.
    #[test]
    fn aws_profile_uuid_is_deterministic() {
        let first = aws_profile_uuid("aws-sso", "dev");
        let second = aws_profile_uuid("aws-sso", "dev");

        assert_eq!(first, second);
    }

    // T-1.3b: Provider-id scoping — same profile name under different providers
    // must produce distinct UUIDs (spec R2.2, S17).
    #[test]
    fn aws_profile_uuid_scoped_by_provider_id() {
        let sso = aws_profile_uuid("aws-sso", "shared");
        let sso_session = aws_profile_uuid("aws-sso-session", "shared");
        let shared_creds = aws_profile_uuid("aws-shared-credentials", "shared");

        assert_ne!(sso, sso_session);
        assert_ne!(sso, shared_creds);
        assert_ne!(sso_session, shared_creds);
    }

    // T-1.3c: Name case sensitivity — "Foo" and "foo" must produce distinct UUIDs
    // (spec R2.3).
    #[test]
    fn aws_profile_uuid_is_case_sensitive() {
        let upper = aws_profile_uuid("aws-sso", "Foo");
        let lower = aws_profile_uuid("aws-sso", "foo");

        assert_ne!(upper, lower);
    }

    // T-1.3d: Stability regression fixture — hard-coded expected UUID for a known
    // (provider_id, name) pair. If AWS_AUTH_NAMESPACE is ever accidentally changed,
    // or the derivation algorithm drifts, this test catches it immediately.
    //
    // Expected value computed from:
    //   Uuid::new_v5(&Uuid::from_u128(0x5fab674f_2a8b_4668_9b06_9d0e1ad74f7a),
    //                b"aws-sso:prod")
    // = e18201b8-45ff-5b27-a2ad-224a8e25728e
    #[test]
    fn aws_profile_uuid_regression_fixture() {
        let result = aws_profile_uuid("aws-sso", "prod");
        let expected: Uuid = "e18201b8-45ff-5b27-a2ad-224a8e25728e"
            .parse()
            .expect("hard-coded UUID must be valid");

        assert_eq!(
            result, expected,
            "UUID derivation changed — AWS_AUTH_NAMESPACE or algorithm may have been modified"
        );
    }
}
