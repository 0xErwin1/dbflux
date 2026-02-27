use dbflux_core::{ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest};
use dbflux_driver_mongodb::MongoDriver;
use dbflux_test_support::containers;

#[test]
#[ignore = "requires Docker daemon"]
fn mongodb_live_connect_ping_query_and_schema() -> Result<(), dbflux_core::DbError> {
    containers::with_mongodb_url(|uri| {
        let driver = MongoDriver::new();
        let profile = ConnectionProfile::new(
            "live-mongodb",
            DbConfig::MongoDB {
                use_uri: true,
                uri: Some(uri),
                host: String::new(),
                port: 27017,
                user: None,
                database: Some("admin".to_string()),
                auth_database: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let connection =
            containers::retry_db_operation(std::time::Duration::from_secs(30), || {
                let connection = driver.connect(&profile)?;
                connection.ping()?;
                Ok(connection)
            })?;

        let result = connection.execute(&QueryRequest::new("db.runCommand({\"ping\": 1})"))?;
        assert!(!result.rows.is_empty());

        let (handle, _) =
            connection.execute_with_handle(&QueryRequest::new("db.runCommand({\"ping\": 1})"))?;
        let cancel = connection.cancel(&handle);
        assert!(matches!(cancel, Err(DbError::NotSupported(_))));

        let schema = connection.schema()?;
        let _ = schema.databases();

        Ok(())
    })
}

#[test]
#[ignore = "TODO: mongodb query cancellation support"]
fn pending_mongodb_cancel_support() {
    panic!("TODO: implement MongoDB query cancellation and replace this pending test");
}
