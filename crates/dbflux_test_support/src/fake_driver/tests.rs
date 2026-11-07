use super::{FakeDriver, FakeQueryOutcome};
use crate::fixtures;
use dbflux_core::{
    ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind, DriverCapabilities,
    QueryRequest, ReadOnlyEnforcement, SchemaLoadingStrategy, TransferFamily,
};

#[test]
fn sqlite_build_config_requires_path() {
    let driver = FakeDriver::new(DbKind::SQLite);
    let result = driver.build_config(&dbflux_core::FormValues::new());

    assert!(matches!(result, Err(DbError::InvalidProfile(_))));
}

#[test]
fn clickhouse_metadata_matches_read_only_contract() {
    let driver = FakeDriver::new(DbKind::ClickHouse);
    let metadata = driver.metadata();
    let expected = DriverCapabilities::MULTIPLE_DATABASES
        | DriverCapabilities::SSL
        | DriverCapabilities::AUTHENTICATION
        | DriverCapabilities::VIEWS
        | DriverCapabilities::PAGINATION
        | DriverCapabilities::SORTING
        | DriverCapabilities::FILTERING
        | DriverCapabilities::EXPORT_CSV
        | DriverCapabilities::EXPORT_JSON
        | DriverCapabilities::CHART_AUTHORING;

    assert_eq!(metadata.category, DatabaseCategory::Relational);
    assert_eq!(metadata.transfer_family, TransferFamily::Incompatible);
    assert_eq!(metadata.capabilities, expected);
    assert!(!metadata.syntax.as_ref().expect("syntax").supports_schemas);
    assert!(metadata.query.is_some());
    assert!(metadata.mutation.is_none());
    assert!(metadata.enforces_read_only());
}

#[test]
fn mongodb_metadata_matches_the_real_driver_read_only_contract() {
    let driver = FakeDriver::new(DbKind::MongoDB);

    assert!(driver.metadata().enforces_read_only());
    assert_eq!(
        driver.metadata().enforces_read_only(),
        dbflux_driver_mongodb::MONGODB_METADATA.enforces_read_only()
    );
}

#[test]
fn redis_metadata_matches_the_real_driver_read_only_contract() {
    let driver = FakeDriver::new(DbKind::Redis);

    assert!(driver.metadata().enforces_read_only());
    assert_eq!(
        driver.metadata().enforces_read_only(),
        dbflux_driver_redis::REDIS_METADATA.enforces_read_only()
    );
}

#[test]
fn execute_uses_configured_outcome_and_records_stats() {
    let driver = FakeDriver::new(DbKind::Postgres)
        .with_query_error("SELECT boom", "boom")
        .with_default_result(dbflux_core::QueryResult::text(
            "ok".to_string(),
            std::time::Duration::ZERO,
        ));

    driver.set_query_outcome(
        "SELECT 1",
        FakeQueryOutcome::success(dbflux_core::QueryResult::table(
            vec![],
            vec![],
            None,
            std::time::Duration::ZERO,
        )),
    );

    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake connection should work");

    let query_ok = connection.execute(&QueryRequest::new("SELECT 1"));
    assert!(query_ok.is_ok());

    let query_err = connection.execute(&QueryRequest::new("SELECT boom"));
    assert!(matches!(query_err, Err(DbError::QueryFailed(_))));

    let stats = driver.stats();
    assert_eq!(stats.executed_requests.len(), 2);
}

#[test]
fn execute_with_database_switches_active_database() {
    let driver = FakeDriver::new(DbKind::MySQL);
    let profile = ConnectionProfile::new(
        "fake",
        DbConfig::MySQL {
            use_uri: false,
            uri: None,
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            database: Some("default_db".to_string()),
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection = driver
        .connect(&profile)
        .expect("fake connection should work");

    assert_eq!(connection.active_database().as_deref(), Some("default_db"));

    let request = QueryRequest::new("SELECT 1").with_database(Some("analytics".to_string()));
    let _ = connection.execute(&request).expect("query should execute");

    assert_eq!(connection.active_database().as_deref(), Some("analytics"));
}

#[test]
fn cancel_and_cancel_active_update_stats() {
    let driver = FakeDriver::new(DbKind::Postgres);
    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake connection should work");

    let (handle, _) = connection
        .execute_with_handle(&QueryRequest::new("SELECT 1"))
        .expect("query should execute with handle");

    connection.cancel(&handle).expect("cancel should succeed");
    connection
        .cancel_active()
        .expect("cancel active should succeed");

    let stats = driver.stats();
    assert_eq!(stats.cancelled_handle_count, 1);
    assert_eq!(stats.cancel_active_calls, 1);
}

#[test]
fn schema_and_list_databases_use_configured_snapshot() {
    let driver = FakeDriver::new(DbKind::Postgres).with_schema(
        fixtures::relational_schema_with_table("app", "public", "users"),
    );
    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake connection should work");

    let schema = connection.schema().expect("schema should be available");
    assert_eq!(schema.databases().len(), 1);
    assert_eq!(schema.databases()[0].name, "app");

    let databases = connection
        .list_databases()
        .expect("list databases should succeed");
    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0].name, "app");
}

#[test]
fn connection_reports_expected_schema_loading_strategy_by_kind() {
    let cases = vec![
        (
            DbKind::Postgres,
            SchemaLoadingStrategy::ConnectionPerDatabase,
        ),
        (DbKind::MySQL, SchemaLoadingStrategy::LazyPerDatabase),
        (DbKind::MariaDB, SchemaLoadingStrategy::LazyPerDatabase),
        (DbKind::SQLite, SchemaLoadingStrategy::SingleDatabase),
        (DbKind::Turso, SchemaLoadingStrategy::SingleDatabase),
        (DbKind::DuckDB, SchemaLoadingStrategy::LazyPerDatabase),
        (DbKind::MongoDB, SchemaLoadingStrategy::SingleDatabase),
        (DbKind::Redis, SchemaLoadingStrategy::SingleDatabase),
        (DbKind::ClickHouse, SchemaLoadingStrategy::LazyPerDatabase),
        (DbKind::DynamoDB, SchemaLoadingStrategy::SingleDatabase),
        (DbKind::InfluxDB, SchemaLoadingStrategy::LazyPerDatabase),
        (
            DbKind::CloudWatchLogs,
            SchemaLoadingStrategy::SingleDatabase,
        ),
        (DbKind::SqlServer, SchemaLoadingStrategy::LazyPerDatabase),
        (
            DbKind::Redshift,
            SchemaLoadingStrategy::ConnectionPerDatabase,
        ),
    ];

    for (kind, expected_strategy) in cases {
        let driver = FakeDriver::new(kind);

        let config = match kind {
            DbKind::Postgres => DbConfig::default_postgres(),
            DbKind::SQLite => DbConfig::SQLite {
                path: "/tmp/fake.db".into(),
                connection_id: None,
            },
            DbKind::MySQL | DbKind::MariaDB => DbConfig::MySQL {
                use_uri: false,
                uri: None,
                host: "localhost".to_string(),
                port: 3306,
                user: "root".to_string(),
                database: Some("app".to_string()),
                ssl_mode: None,
                ssl_root_cert_path: None,
                ssl_client_cert_path: None,
                ssl_client_key_path: None,
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
            DbKind::MongoDB => DbConfig::default_mongodb(),
            DbKind::Redis => DbConfig::default_redis(),
            DbKind::DynamoDB => DbConfig::default_dynamodb(),
            DbKind::CloudWatchLogs => DbConfig::default_cloudwatch_logs(),
            DbKind::InfluxDB => DbConfig::default_influxdb(),
            DbKind::SqlServer => DbConfig::default_sqlserver(),
            DbKind::Redshift => DbConfig::default_redshift(),
            DbKind::S3 => DbConfig::default_s3(),
            DbKind::AwsSecrets => DbConfig::default_aws_secrets(),
            DbKind::ClickHouse => DbConfig::default_clickhouse(),
            DbKind::Turso => DbConfig::default_turso(),
            DbKind::DuckDB => DbConfig::default_duckdb(),
        };

        let profile = ConnectionProfile::new("fake", config);
        let connection = driver
            .connect(&profile)
            .expect("fake connection should work");

        assert_eq!(connection.schema_loading_strategy(), expected_strategy);
    }
}

#[test]
fn turso_fake_driver_roundtrips_url_and_keeps_transactions_disabled() {
    let driver = FakeDriver::new(DbKind::Turso);
    let values = dbflux_core::FormValues::from([(
        "url".to_string(),
        "https://example.turso.io".to_string(),
    )]);
    let config = driver.build_config(&values).expect("build Turso config");

    assert!(matches!(
        config,
        DbConfig::Turso { ref url } if url == "https://example.turso.io"
    ));
    assert_eq!(driver.extract_values(&config), values);
    assert!(
        !driver
            .metadata()
            .capabilities
            .contains(DriverCapabilities::TRANSACTIONS)
    );
    assert!(driver.metadata().transactions.is_none());
}

fn connect_postgres(driver: &FakeDriver) -> Box<dyn dbflux_core::Connection> {
    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    driver
        .connect(&profile)
        .expect("fake connection should work")
}

fn read_only_request() -> QueryRequest {
    QueryRequest::new("SELECT 1").with_read_only(ReadOnlyEnforcement::Required)
}

#[test]
fn fake_without_enforcement_records_and_refuses_read_only_requests() {
    let driver = FakeDriver::new(DbKind::Postgres);
    let connection = connect_postgres(&driver);

    assert!(!connection.metadata().enforces_read_only());

    let result = connection.execute(&read_only_request());

    assert!(matches!(result, Err(DbError::NotSupported(_))));
    let executed = driver.stats().executed_requests;
    assert_eq!(executed.len(), 1);
    assert_eq!(executed[0].read_only, ReadOnlyEnforcement::Required);
}

#[test]
fn fake_with_read_only_enforcement_declares_it_and_runs_the_request() {
    let driver = FakeDriver::new(DbKind::Postgres).with_read_only_enforcement();
    let connection = connect_postgres(&driver);

    assert!(driver.metadata().enforces_read_only());
    assert!(connection.metadata().enforces_read_only());
    assert!(connection.execute(&read_only_request()).is_ok());
    assert_eq!(
        driver.stats().executed_requests[0].read_only,
        ReadOnlyEnforcement::Required
    );
}

#[test]
fn fake_with_read_only_refusal_declares_enforcement_but_refuses_at_execution() {
    let driver = FakeDriver::new(DbKind::Postgres).with_read_only_refusal();
    let connection = connect_postgres(&driver);

    assert!(connection.metadata().enforces_read_only());
    assert!(matches!(
        connection.execute(&read_only_request()),
        Err(DbError::NotSupported(_))
    ));
    assert!(connection.execute(&QueryRequest::new("SELECT 1")).is_ok());
}
use dbflux_core::secrecy::ExposeSecret;

use super::*;

fn connected() -> (FakeDriver, Box<dyn Connection>) {
    let driver = FakeDriver::new(DbKind::AwsSecrets);
    let profile = ConnectionProfile::new_with_driver(
        "fake-aws-secrets",
        DbKind::AwsSecrets,
        "builtin:fake-aws-secrets",
        DbConfig::default_aws_secrets(),
    );
    let connection = driver.connect(&profile).expect("the fake driver connects");
    (driver, connection)
}

fn exposed_text(value: &SecretValue) -> String {
    match &value.payload {
        SecretPayload::Text(text) => text.expose_secret().to_string(),
        SecretPayload::Binary(_) => panic!("expected a text payload"),
    }
}

fn exposed_bytes(value: &SecretValue) -> Vec<u8> {
    match &value.payload {
        SecretPayload::Binary(bytes) => bytes.expose_secret().to_vec(),
        SecretPayload::Text(_) => panic!("expected a binary payload"),
    }
}

/// A write becomes the current version and is recorded as the `secrecy`
/// type it traveled in.
#[test]
fn a_secret_write_is_recorded_and_becomes_the_current_value() {
    let (driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let response = secrets
        .put_value(&SecretPutRequest::text(
            "prod/notify/webhook-token",
            "new webhook token",
        ))
        .expect("the fake secrets store advertises WRITE");

    assert_eq!(response.version.as_deref(), Some("v-written-1"));

    let calls = driver.secret_store_calls();
    assert_eq!(calls.put_requests.len(), 1);
    assert_eq!(calls.put_requests[0].0, FAKE_SECRETS_STORE_ID);
    assert_eq!(calls.put_requests[0].1.name, "prod/notify/webhook-token");
    assert_eq!(calls.put_requests[0].1.stage, None);
    assert!(!format!("{calls:?}").contains("new webhook token"));

    let current = secrets
        .get_value("prod/notify/webhook-token", None)
        .expect("the written value is readable");
    assert_eq!(exposed_text(&current), "new webhook token");
    assert_eq!(current.version.as_deref(), Some("v-written-1"));
}

#[test]
fn a_staged_write_is_recorded_without_moving_the_current_value() {
    let (driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let response = secrets
        .put_value(
            &SecretPutRequest::text("prod/notify/webhook-token", "pending value")
                .with_stage(SecretVersionStage::Pending),
        )
        .expect("the fake secrets store advertises WRITE");

    assert_eq!(response.version.as_deref(), Some("v-written-1"));
    assert_eq!(driver.secret_store_calls().put_requests.len(), 1);

    let current = secrets
        .get_value("prod/notify/webhook-token", None)
        .expect("the fake keeps the current value readable");
    assert_eq!(exposed_text(&current), FAKE_SECRET_PLAINTEXT);
}

#[test]
fn a_managed_secret_refuses_the_write_and_keeps_its_value() {
    let (driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let error = secrets
        .put_value(&SecretPutRequest::text("prod/db/master", "replaced"))
        .expect_err("a managed secret is read only");
    let rendered = error.to_string();
    assert!(rendered.contains("rds"), "{rendered}");
    assert!(rendered.contains("prod/db/master"), "{rendered}");
    assert!(!rendered.contains("replaced"), "{rendered}");
    assert!(
        driver.secret_store_calls().put_requests.is_empty(),
        "the driver sends no put at all: the refusal precedes the write"
    );

    let current = secrets
        .get_value("prod/db/master", None)
        .expect("the value survived the refused write");
    assert_ne!(exposed_text(&current), "replaced");
}

#[test]
fn a_scheduled_deletion_refuses_the_write() {
    let (_, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let error = secrets
        .put_value(&SecretPutRequest::text(
            "prod/deploy/legacy-token",
            "replaced",
        ))
        .expect_err("a deleted secret accepts no writes");

    let rendered = error.to_string();
    assert!(rendered.contains("deletion"), "{rendered}");
    assert!(rendered.contains("Secrets Manager"), "{rendered}");
    assert!(!rendered.contains("replaced"), "{rendered}");
}

#[test]
fn a_write_to_a_missing_secret_is_not_found() {
    let (_, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let error = secrets
        .put_value(&SecretPutRequest::text("prod/missing", "value"))
        .expect_err("a missing secret accepts no writes");

    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
}

#[test]
fn a_write_can_be_told_to_fail() {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error("prod/notify/webhook-token", "put was denied by the fixture");
    let profile = ConnectionProfile::new_with_driver(
        "fake-aws-secrets",
        DbKind::AwsSecrets,
        "builtin:fake-aws-secrets",
        DbConfig::default_aws_secrets(),
    );
    let connection = driver.connect(&profile).expect("the fake driver connects");
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let error = secrets
        .put_value(&SecretPutRequest::text(
            "prod/notify/webhook-token",
            "replaced",
        ))
        .expect_err("the fixture refuses the write");

    assert!(
        matches!(error, DbError::PermissionDenied(ref formatted) if formatted.message.contains("denied by the fixture")),
        "{error}"
    );

    let current = secrets
        .get_value("prod/notify/webhook-token", None)
        .expect("the failed write did not change the value");
    assert_eq!(exposed_text(&current), FAKE_SECRET_PLAINTEXT);
}

#[test]
fn a_binary_write_reaches_the_fake_store_as_bytes() {
    let (_, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let response = secrets
        .put_value(&SecretPutRequest::binary(
            "prod/tls/signing-key",
            vec![1, 2, 3],
        ))
        .expect("the fake secrets store advertises WRITE");

    assert_eq!(response.version.as_deref(), Some("v-written-1"));

    let current = secrets
        .get_value("prod/tls/signing-key", None)
        .expect("the written bytes are readable");
    assert_eq!(exposed_bytes(&current), vec![1, 2, 3]);
}

#[test]
fn a_parameter_write_overwrites_and_bumps_the_version() {
    let (driver, connection) = connected();
    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let response = parameters
        .put_value(&SecretPutRequest::text(
            "/app/prod/db/host",
            "db.new.internal",
        ))
        .expect("the fake parameter store advertises WRITE");

    assert_eq!(response.version.as_deref(), Some("4"));
    assert!(driver.secret_store_calls().put_requests.len() == 1);

    let current = parameters
        .get_value("/app/prod/db/host", None)
        .expect("the overwritten value is readable");
    assert_eq!(exposed_text(&current), "db.new.internal");
    assert_eq!(current.version.as_deref(), Some("4"));
}

#[test]
fn a_parameter_store_refuses_binary_and_pending_writes() {
    let (_, connection) = connected();
    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let binary = SecretPutRequest::binary("/app/prod/db/host", vec![0]);
    let Err(DbError::NotSupported(refusal)) = parameters.put_value(&binary) else {
        panic!("a binary value has no parameter-store shape");
    };
    assert!(refusal.contains("Parameter Store"), "{refusal}");
    assert!(refusal.contains("/app/prod/db/host"), "{refusal}");

    let staged =
        SecretPutRequest::text("/app/prod/db/host", "db").with_stage(SecretVersionStage::Pending);
    let Err(DbError::NotSupported(refusal)) = parameters.put_value(&staged) else {
        panic!("a pending write has no parameter-store shape");
    };
    assert!(refusal.contains("Parameter Store"), "{refusal}");
    assert!(refusal.contains("/app/prod/db/host"), "{refusal}");
}

/// A pending write keeps its payload under its own version id: the
/// version answers by id, the current value is untouched, and the
/// summary still names the current version.
#[test]
fn a_pending_written_version_answers_by_its_version_id() {
    let (_, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let response = secrets
        .put_value(
            &SecretPutRequest::text("prod/notify/webhook-token", "pending value")
                .with_stage(SecretVersionStage::Pending),
        )
        .expect("the fake secrets store advertises WRITE");
    let pending = response.version.expect("a put returns the version id");

    let staged = secrets
        .get_value("prod/notify/webhook-token", Some(&pending))
        .expect("the pending version is readable by its id");
    assert_eq!(exposed_text(&staged), "pending value");
    assert_eq!(staged.version.as_deref(), Some(pending.as_str()));

    let current = secrets
        .get_value("prod/notify/webhook-token", None)
        .expect("the current value is readable");
    assert_eq!(exposed_text(&current), FAKE_SECRET_PLAINTEXT);
}

/// Every written version keeps its payload: after two `Current` writes
/// the first written version still answers by its id with its own
/// value, and the current one with the latest.
#[test]
fn every_written_version_keeps_its_payload() {
    let (_, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let first = secrets
        .put_value(&SecretPutRequest::text(
            "prod/notify/webhook-token",
            "first write",
        ))
        .expect("the first write lands")
        .version
        .expect("a version id");
    let second = secrets
        .put_value(&SecretPutRequest::text(
            "prod/notify/webhook-token",
            "second write",
        ))
        .expect("the second write lands")
        .version
        .expect("a version id");

    let earlier = secrets
        .get_value("prod/notify/webhook-token", Some(&first))
        .expect("the earlier written version is readable");
    assert_eq!(exposed_text(&earlier), "first write");
    assert_eq!(earlier.version.as_deref(), Some(first.as_str()));

    let current = secrets
        .get_value("prod/notify/webhook-token", None)
        .expect("the current value is readable");
    assert_eq!(exposed_text(&current), "second write");
    assert_eq!(current.version.as_deref(), Some(second.as_str()));
}

/// A version the fake never stored is not found: `get_value` answers
/// with the version asked for or reports it missing, never with
/// whatever was written last.
#[test]
fn an_unknown_version_is_not_found() {
    let (_, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let error = secrets
        .get_value("prod/notify/webhook-token", Some("v-written-999"))
        .expect_err("an unknown version is not found");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
}

/// An overwrite does not clear a decrypt denial: read authorization is
/// independent of the stored value, so the write succeeds and the read
/// still fails the way it did before.
#[test]
fn an_overwrite_keeps_an_existing_decrypt_denial() {
    let (_, connection) = connected();
    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let response = parameters
        .put_value(&SecretPutRequest::text("/app/prod/db/password", "rotated"))
        .expect("the overwrite lands");
    assert_eq!(response.version.as_deref(), Some("4"));

    let error = parameters
        .get_value("/app/prod/db/password", None)
        .expect_err("the denial survives the overwrite");
    match &error {
        DbError::PermissionDenied(formatted) => {
            assert_eq!(formatted.message, FAKE_KMS_DENIED_MESSAGE);
        }
        other => panic!("expected the decrypt denial, got {other:?}"),
    }
}

fn version_ids(versions: &[dbflux_core::SecretVersionInfo]) -> Vec<&str> {
    versions.iter().map(|version| version.id.as_str()).collect()
}

#[test]
fn the_flat_store_lists_versions_newest_first_with_staging_labels() {
    let (driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let versions = secrets
        .list_versions("prod/api/credentials")
        .expect("the fake flat store lists versions");

    assert_eq!(
        version_ids(&versions),
        ["v-current", "v-previous", "v-oldest"]
    );
    assert_eq!(versions[0].labels, ["AWSCURRENT"]);
    assert_eq!(versions[1].labels, ["AWSPREVIOUS"]);
    assert!(versions[2].labels.is_empty());
    assert!(
        versions
            .windows(2)
            .all(|pair| pair[0].created > pair[1].created)
    );

    let details = secrets
        .describe("prod/api/credentials")
        .expect("the secret is described");
    assert_eq!(details.version_count, Some(3));
    assert_eq!(details.version_labels, ["AWSCURRENT", "AWSPREVIOUS"]);

    assert_eq!(
        driver.secret_store_calls().version_listings,
        [(
            FAKE_SECRETS_STORE_ID.to_string(),
            "prod/api/credentials".to_string()
        )]
    );
}

#[test]
fn every_listed_version_id_fetches_its_own_value() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    for version in secrets
        .list_versions("prod/api/credentials")
        .expect("versions")
    {
        let value = secrets
            .get_value("prod/api/credentials", Some(&version.id))
            .expect("a listed id is a selector get_value accepts");
        assert_eq!(value.version.as_deref(), Some(version.id.as_str()));
    }
}

#[test]
fn the_hierarchical_store_lists_numbered_versions_with_their_author() {
    let (_driver, connection) = connected();
    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let versions = parameters
        .list_versions("/app/prod/db/host")
        .expect("the fake parameter store lists history");

    assert_eq!(version_ids(&versions), ["3", "2", "1"]);
    assert!(
        versions
            .iter()
            .all(|version| version.modified_by.is_some() && version.created.is_some())
    );
}

#[test]
fn a_write_adds_the_new_version_at_the_top_of_the_history() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    secrets
        .put_value(&SecretPutRequest::text("prod/api/credentials", "rotated"))
        .expect("write");

    let versions = secrets
        .list_versions("prod/api/credentials")
        .expect("versions");
    assert_eq!(
        version_ids(&versions),
        ["v-written-1", "v-current", "v-previous", "v-oldest"]
    );
    assert_eq!(versions[0].labels, ["AWSCURRENT"]);
    assert_eq!(versions[1].labels, ["AWSPREVIOUS"]);
    assert!(versions[2].labels.is_empty());
}

#[test]
fn listing_the_versions_of_a_missing_secret_is_not_found() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    assert!(secrets.list_versions("prod/nope").is_err());
}

#[test]
fn a_pending_write_updates_the_details_and_the_history_but_not_the_current_version() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    let mut request = SecretPutRequest::text("prod/api/credentials", "staged value");
    request.stage = Some(dbflux_core::SecretVersionStage::Pending);
    secrets.put_value(&request).expect("a staged write");

    let details = secrets.describe("prod/api/credentials").expect("describe");
    let versions = secrets
        .list_versions("prod/api/credentials")
        .expect("versions");

    assert_eq!(details.summary.version.as_deref(), Some("v-current"));
    assert_eq!(details.version_count, Some(4));
    assert_eq!(
        details.version_labels,
        ["AWSCURRENT", "AWSPENDING", "AWSPREVIOUS"]
    );
    assert_eq!(
        usize::try_from(details.version_count.unwrap_or(0)),
        Ok(versions.len())
    );
    assert_eq!(versions[0].id, "v-written-1");
    assert_eq!(versions[0].labels, ["AWSPENDING"]);
    assert_eq!(versions[1].labels, ["AWSCURRENT"]);

    let staged = secrets
        .get_value("prod/api/credentials", Some("v-written-1"))
        .expect("the staged version is readable by id");
    assert_eq!(exposed_text(&staged), "staged value");
}
