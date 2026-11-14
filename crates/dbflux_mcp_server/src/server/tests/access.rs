//! Managed-access regressions for the MCP access manager.

use super::*;

#[tokio::test]
async fn ssm_managed_access_names_the_missing_field_before_any_aws_call() {
    let manager = McpAccessManager::new(None);
    let params = std::collections::HashMap::from([
        ("instance_id".to_string(), "i-abc".to_string()),
        ("remote_port".to_string(), "5432".to_string()),
    ]);

    let error = match manager
        .open_managed("aws-ssm", &params, "db.internal")
        .await
    {
        Ok(_) => panic!("a missing region must be rejected"),
        Err(error) => error,
    };

    assert!(
        matches!(&error, DbError::InvalidProfile(message) if message == "AWS SSM access requires a region"),
        "unexpected error: {error:?}"
    );
}
