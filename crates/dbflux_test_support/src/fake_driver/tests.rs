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
    let matching =
        dbflux_core::QueryResult::text("matching".to_string(), std::time::Duration::ZERO);
    let fallback = dbflux_core::QueryResult::text("default".to_string(), std::time::Duration::ZERO);

    let driver = FakeDriver::new(DbKind::Postgres)
        .with_query_error("SELECT boom", "boom")
        .with_default_result(fallback);

    driver.set_query_outcome("SELECT 1", FakeQueryOutcome::success(matching));

    let profile = ConnectionProfile::new("fake", DbConfig::default_postgres());
    let connection = driver
        .connect(&profile)
        .expect("fake connection should work");

    let matched = connection
        .execute(&QueryRequest::new("SELECT 1"))
        .expect("the configured query should succeed");
    assert_eq!(matched.text_body.as_deref(), Some("matching"));

    let unmatched = connection
        .execute(&QueryRequest::new("SELECT 2"))
        .expect("an unmatched query should use the default result");
    assert_eq!(unmatched.text_body.as_deref(), Some("default"));

    let query_err = connection.execute(&QueryRequest::new("SELECT boom"));
    assert!(matches!(query_err, Err(DbError::QueryFailed(_))));

    let stats = driver.stats();
    assert_eq!(stats.executed_requests.len(), 3);
    assert_eq!(stats.executed_requests[1].sql, "SELECT 2");
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
fn enforcement_after_refusal_clears_the_refusal() {
    let driver = FakeDriver::new(DbKind::Postgres)
        .with_read_only_refusal()
        .with_read_only_enforcement();
    let connection = connect_postgres(&driver);

    assert!(connection.metadata().enforces_read_only());
    assert!(connection.execute(&read_only_request()).is_ok());
}

#[test]
fn refusal_after_enforcement_refuses() {
    let driver = FakeDriver::new(DbKind::Postgres)
        .with_read_only_enforcement()
        .with_read_only_refusal();
    let connection = connect_postgres(&driver);

    assert!(matches!(
        connection.execute(&read_only_request()),
        Err(DbError::NotSupported(_))
    ));
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
    connected_with(FakeDriver::new(DbKind::AwsSecrets))
}

fn connected_with(driver: FakeDriver) -> (FakeDriver, Box<dyn Connection>) {
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

        let seeded = match version.id.as_str() {
            "v-current" => FAKE_SECRET_PASSWORD,
            "v-previous" => "fake-previous-2b8d",
            "v-oldest" => "fake-oldest-90e1",
            other => panic!("unexpected version {other}"),
        };
        assert!(exposed_text(&value).contains(seeded), "{}", version.id);
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
fn a_value_request_records_the_version_it_asked_for() {
    let (driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    secrets
        .get_value("prod/api/credentials", None)
        .expect("the current value");
    secrets
        .get_value("prod/api/credentials", Some("v-previous"))
        .expect("an older version");

    assert_eq!(
        driver.secret_store_calls().value_version_requests,
        [
            (
                FAKE_SECRETS_STORE_ID.to_string(),
                "prod/api/credentials".to_string(),
                None
            ),
            (
                FAKE_SECRETS_STORE_ID.to_string(),
                "prod/api/credentials".to_string(),
                Some("v-previous".to_string())
            ),
        ]
    );
}

#[test]
fn a_configured_history_failure_fails_only_that_secrets_listing() {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_versions_error("prod/api/credentials", "not authorized");
    let (_driver, connection) = connected_with(driver);
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    assert!(matches!(
        secrets.list_versions("prod/api/credentials"),
        Err(DbError::PermissionDenied(_))
    ));
    assert!(secrets.list_versions("prod/db/master").is_ok());
    assert!(secrets.get_value("prod/api/credentials", None).is_ok());
}

#[test]
fn listing_the_versions_of_a_missing_secret_is_not_found() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    assert!(matches!(
        secrets.list_versions("prod/nope"),
        Err(dbflux_core::DbError::ObjectNotFound(_))
    ));
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

#[test]
fn a_filtered_path_listing_only_includes_branches_with_a_matching_descendant() {
    let (_driver, connection) = connected();
    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let page = parameters
        .list(&SecretListRequest {
            name_filter: Some("endpoint".to_string()),
            ..SecretListRequest::default()
        })
        .expect("the filtered listing succeeds");

    assert!(page.entries.is_empty());
    assert_eq!(page.child_paths, vec!["/shared/".to_string()]);
}

#[test]
fn a_page_token_at_the_end_of_the_index_range_is_an_error_not_an_overflow() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake flat store exists");

    let result = secrets.list(&SecretListRequest {
        page_token: Some(usize::MAX.to_string()),
        ..SecretListRequest::default()
    });

    assert!(matches!(result, Err(DbError::QueryFailed(_))));
}

#[test]
fn a_second_pending_write_moves_the_pending_label() {
    let (_driver, connection) = connected();
    let secrets = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake secrets store exists");

    for value in ["first staged", "second staged"] {
        let mut request = SecretPutRequest::text("prod/api/credentials", value);
        request.stage = Some(dbflux_core::SecretVersionStage::Pending);
        secrets.put_value(&request).expect("a staged write");
    }

    let versions = secrets
        .list_versions("prod/api/credentials")
        .expect("versions");
    let holders: Vec<&str> = versions
        .iter()
        .filter(|version| version.labels.iter().any(|label| label == "AWSPENDING"))
        .map(|version| version.id.as_str())
        .collect();

    assert_eq!(holders, ["v-written-2"]);
    assert_eq!(versions.len(), 5);
}

// --- Rolling back a version ---

fn labels_of(versions: &[SecretVersionInfo]) -> Vec<(String, Vec<String>)> {
    versions
        .iter()
        .map(|version| (version.id.clone(), version.labels.clone()))
        .collect()
}

fn current_ids(versions: &[dbflux_core::SecretVersionInfo]) -> Vec<&str> {
    versions
        .iter()
        .filter(|version| version.is_current)
        .map(|version| version.id.as_str())
        .collect()
}

#[test]
fn rolling_back_moves_current_to_the_chosen_version_and_previous_to_the_old_current() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let rollback = store
        .roll_back(&SecretRollbackRequest::new(
            "prod/api/credentials",
            "v-oldest",
        ))
        .expect("v-oldest is an earlier version");

    assert_eq!(rollback.current_version, "v-oldest");
    assert_eq!(rollback.previous_version.as_deref(), Some("v-current"));

    // Both labels moved and the label the old previous held is gone; no
    // version was created.
    let versions = store
        .list_versions("prod/api/credentials")
        .expect("history");
    assert_eq!(
        labels_of(&versions),
        vec![
            ("v-current".to_string(), vec!["AWSPREVIOUS".to_string()]),
            ("v-previous".to_string(), Vec::new()),
            ("v-oldest".to_string(), vec!["AWSCURRENT".to_string()]),
        ]
    );
    // The flag a listing reads follows the label, so a history read after a
    // roll back names the version that is current now.
    assert_eq!(current_ids(&versions), vec!["v-oldest"]);

    let details = store.describe("prod/api/credentials").expect("describe");
    assert_eq!(details.summary.version.as_deref(), Some("v-oldest"));
    assert_eq!(details.version_count, Some(3));

    // The current value is the rolled-back version's own, unchanged.
    let current = store
        .get_value("prod/api/credentials", None)
        .expect("current value");
    assert_eq!(current.version.as_deref(), Some("v-oldest"));
    assert!(exposed_text(&current).contains("fake-oldest-90e1"));

    let calls = driver.secret_store_calls();
    assert_eq!(calls.mutation_attempts.len(), 1);
    assert_eq!(calls.mutations.len(), 1);
    assert!(
        calls.put_attempts.is_empty(),
        "a roll back is not a value write"
    );
}

#[test]
fn rolling_back_to_the_current_version_is_refused_and_changes_nothing() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let before = labels_of(
        &store
            .list_versions("prod/api/credentials")
            .expect("history"),
    );

    let error = store
        .roll_back(&SecretRollbackRequest::new(
            "prod/api/credentials",
            "v-current",
        ))
        .expect_err("the current version is not an earlier one");

    assert!(error.to_string().contains("already the current"), "{error}");

    let versions = store
        .list_versions("prod/api/credentials")
        .expect("history");
    assert_eq!(
        labels_of(&versions)[0],
        ("v-current".to_string(), vec!["AWSCURRENT".to_string()])
    );
    assert_eq!(labels_of(&versions), before);

    let calls = driver.secret_store_calls();
    assert_eq!(calls.mutation_attempts.len(), 1);
    assert!(
        calls.mutations.is_empty(),
        "a refused roll back is not accepted"
    );
}

#[test]
fn rolling_back_to_an_unknown_version_or_secret_is_not_found() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    for request in [
        SecretRollbackRequest::new("prod/api/credentials", "v-missing"),
        SecretRollbackRequest::new("prod/missing", "v-oldest"),
    ] {
        let error = store.roll_back(&request).expect_err("nothing to move to");
        assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
    }
}

#[test]
fn a_managed_secret_and_a_store_without_labels_refuse_a_roll_back() {
    let (driver, connection) = connected();

    let error = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("store")
        .roll_back(&SecretRollbackRequest::new("prod/db/master", "v-current"))
        .expect_err("a managed secret is read only");
    assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");

    let error = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("store")
        .roll_back(&SecretRollbackRequest::new("/app/prod/db/host", "2"))
        .expect_err("a numbered store has no labels to move");
    assert!(matches!(error, DbError::NotSupported(_)), "{error}");

    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn the_write_error_knob_fails_a_roll_back_without_moving_labels() {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error("prod/api/credentials", "roll back denied by the fixture");
    let profile = ConnectionProfile::new_with_driver(
        "fake-aws-secrets",
        DbKind::AwsSecrets,
        "builtin:fake-aws-secrets",
        DbConfig::default_aws_secrets(),
    );
    let connection = driver.connect(&profile).expect("connect");
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("store");

    let error = store
        .roll_back(&SecretRollbackRequest::new(
            "prod/api/credentials",
            "v-oldest",
        ))
        .expect_err("the knob denies the roll back");
    assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");

    let details = store.describe("prod/api/credentials").expect("describe");
    assert_eq!(details.summary.version.as_deref(), Some("v-current"));
    assert!(driver.secret_store_calls().mutations.is_empty());
}

// --- Deleting and restoring ---

#[test]
fn a_scheduled_deletion_marks_the_secret_and_hides_its_value_until_restored() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let deletion = store
        .delete_secret(&SecretDeleteRequest::scheduled(
            "prod/notify/webhook-token",
            14,
        ))
        .expect("a plain secret can be scheduled for deletion");

    let scheduled_for = deletion
        .deletion_scheduled_for
        .expect("a scheduled deletion reports when it takes effect");
    let details = store
        .describe("prod/notify/webhook-token")
        .expect("describe");
    assert_eq!(details.summary.deletion_scheduled_for, Some(scheduled_for));
    assert!(details.deleted_at.is_some());
    assert!(details.summary.is_pending_deletion());
    assert_eq!(
        (scheduled_for - details.deleted_at.expect("requested at")).num_days(),
        14
    );

    // The service refuses the value of a secret pending deletion.
    assert!(store.get_value("prod/notify/webhook-token", None).is_err());

    // It still lists, flagged, like `ListSecrets` with planned deletions.
    let listed = store
        .list(&SecretListRequest::default())
        .expect("list")
        .entries
        .into_iter()
        .chain(
            store
                .list(&SecretListRequest {
                    page_token: Some("4".to_string()),
                    ..SecretListRequest::default()
                })
                .expect("second page")
                .entries,
        )
        .find(|entry| entry.name == "prod/notify/webhook-token")
        .expect("still listed");
    assert!(listed.is_pending_deletion());

    store
        .restore_secret("prod/notify/webhook-token")
        .expect("a scheduled deletion can be restored");

    let details = store
        .describe("prod/notify/webhook-token")
        .expect("describe");
    assert!(!details.summary.is_pending_deletion());
    assert!(details.deleted_at.is_none());
    assert_eq!(
        exposed_text(
            &store
                .get_value("prod/notify/webhook-token", None)
                .expect("readable again")
        ),
        FAKE_SECRET_PLAINTEXT
    );

    let calls = driver.secret_store_calls();
    assert_eq!(calls.mutation_attempts.len(), 2);
    assert_eq!(calls.mutations.len(), 2);
}

#[test]
fn deleting_twice_restoring_a_live_secret_and_managed_secrets_are_refused() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let error = store
        .delete_secret(&SecretDeleteRequest::scheduled(
            "prod/deploy/legacy-token",
            7,
        ))
        .expect_err("already scheduled for deletion");
    assert!(error.to_string().contains("already scheduled"), "{error}");

    let error = store
        .restore_secret("prod/notify/webhook-token")
        .expect_err("nothing to restore");
    assert!(error.to_string().contains("not scheduled"), "{error}");

    for error in [
        store
            .delete_secret(&SecretDeleteRequest::scheduled("prod/db/master", 7))
            .expect_err("managed"),
        store.restore_secret("prod/db/master").expect_err("managed"),
    ] {
        assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");
    }

    let error = store
        .delete_secret(&SecretDeleteRequest::scheduled("prod/missing", 7))
        .expect_err("missing");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");

    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn the_fixture_secret_pending_deletion_restores_and_becomes_readable() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    assert!(store.get_value("prod/deploy/legacy-token", None).is_err());
    store
        .restore_secret("prod/deploy/legacy-token")
        .expect("restore");

    assert_eq!(
        exposed_text(
            &store
                .get_value("prod/deploy/legacy-token", None)
                .expect("readable")
        ),
        "legacy-token-value"
    );
}

#[test]
fn a_parameter_deletion_needs_its_name_and_removes_it_for_good() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let error = store
        .delete_secret(&SecretDeleteRequest::irreversible(
            "/app/prod/db/port",
            "/app/prod/db/por",
        ))
        .expect_err("the confirmation must be the exact name");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(store.describe("/app/prod/db/port").is_ok());
    assert!(driver.secret_store_calls().mutations.is_empty());

    let deletion = store
        .delete_secret(&SecretDeleteRequest::irreversible(
            "/app/prod/db/port",
            "/app/prod/db/port",
        ))
        .expect("a confirmed deletion goes through");
    assert!(deletion.deletion_scheduled_for.is_none());

    let error = store.describe("/app/prod/db/port").expect_err("deleted");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");
    assert!(store.get_value("/app/prod/db/port", None).is_err());
    let names: Vec<String> = store
        .list(&SecretListRequest {
            path: Some("/app/prod/db/".to_string()),
            ..SecretListRequest::default()
        })
        .expect("list")
        .entries
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(
        !names.contains(&"/app/prod/db/port".to_string()),
        "{names:?}"
    );

    // A second deletion finds nothing, with the service's own answer.
    let error = store
        .delete_secret(&SecretDeleteRequest::irreversible(
            "/app/prod/db/port",
            "/app/prod/db/port",
        ))
        .expect_err("already deleted");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");

    let calls = driver.secret_store_calls();
    assert_eq!(calls.mutations.len(), 1);
}

#[test]
fn each_store_refuses_the_deletion_it_does_not_offer() {
    let (driver, connection) = connected();

    let error = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("store")
        .delete_secret(&SecretDeleteRequest::scheduled("/app/prod/db/host", 30))
        .expect_err("a parameter has no recovery window");
    assert!(matches!(error, DbError::NotSupported(_)), "{error}");

    let error = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("store")
        .delete_secret(&SecretDeleteRequest::irreversible(
            "prod/notify/webhook-token",
            "prod/notify/webhook-token",
        ))
        .expect_err("a secret is never deleted without recovery");
    assert!(matches!(error, DbError::NotSupported(_)), "{error}");

    let error = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("store")
        .restore_secret("/app/prod/db/host")
        .expect_err("a parameter cannot be restored");
    assert!(matches!(error, DbError::NotSupported(_)), "{error}");

    assert!(driver.secret_store_calls().mutations.is_empty());
    assert!(
        connection
            .secret_store(FAKE_SECRETS_STORE_ID)
            .expect("store")
            .describe("prod/notify/webhook-token")
            .is_ok()
    );
}

// --- Rotation ---

const FAKE_LAMBDA: &str = "arn:aws:lambda:eu-west-1:441012345678:function:rotate-credentials";

#[test]
fn rotating_now_creates_a_version_that_becomes_current_and_stamps_the_rotation() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let started = store
        .rotate_now("prod/api/credentials")
        .expect("rotation is enabled on this secret");

    let version = started.version.expect("the rotation creates a version");
    let versions = store
        .list_versions("prod/api/credentials")
        .expect("history");
    assert_eq!(
        labels_of(&versions),
        vec![
            (version.clone(), vec!["AWSCURRENT".to_string()]),
            ("v-current".to_string(), vec!["AWSPREVIOUS".to_string()]),
            ("v-previous".to_string(), Vec::new()),
            ("v-oldest".to_string(), Vec::new()),
        ],
        "the new version replaced AWSCURRENT, and no label is left on AWSPENDING"
    );

    let details = store.describe("prod/api/credentials").expect("describe");
    assert_eq!(details.summary.version.as_deref(), Some(version.as_str()));
    assert_eq!(details.version_count, Some(4));
    let rotation = details.summary.rotation.expect("rotation state");
    assert!(rotation.enabled);
    assert_eq!(rotation.schedule.as_deref(), Some("rate(30 days)"));
    let last = rotation.last_rotated.expect("stamped");
    let next = rotation.next_rotation.expect("rescheduled");
    assert_eq!((next - last).num_days(), 30);
    assert!(last > chrono::Utc::now() - chrono::Duration::days(3650));

    assert!(store.get_value("prod/api/credentials", None).is_ok());
    assert_eq!(driver.secret_store_calls().mutations.len(), 1);
}

#[test]
fn rotating_now_is_refused_unless_rotation_is_enabled_and_changes_nothing() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let error = store
        .rotate_now("prod/notify/webhook-token")
        .expect_err("rotation was never configured");
    assert!(error.to_string().contains("not enabled"), "{error}");

    let error = store
        .rotate_now("prod/db/master")
        .expect_err("a managed secret is read only");
    assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");

    let error = store.rotate_now("prod/missing").expect_err("missing");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");

    assert_eq!(
        store
            .list_versions("prod/notify/webhook-token")
            .expect("history")
            .len(),
        1
    );
    assert!(driver.secret_store_calls().mutations.is_empty());

    let error = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("store")
        .rotate_now("/app/prod/db/host")
        .expect_err("a parameter does not rotate");
    assert!(matches!(error, DbError::NotSupported(_)), "{error}");
}

#[test]
fn editing_rotation_turns_it_off_and_on_without_rotating() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    store
        .update_rotation(&SecretRotationUpdate::disable("prod/api/credentials"))
        .expect("disable");
    let rotation = store
        .describe("prod/api/credentials")
        .expect("describe")
        .summary
        .rotation
        .expect("rotation state");
    assert!(!rotation.enabled);
    assert!(rotation.next_rotation.is_none(), "nothing is scheduled");

    // Off, so rotating now is refused rather than switching it back on.
    assert!(store.rotate_now("prod/api/credentials").is_err());

    store
        .update_rotation(
            &SecretRotationUpdate::enable("prod/api/credentials", "rate(7 days)")
                .with_window("2h")
                .with_function(FAKE_LAMBDA),
        )
        .expect("enable with new settings");
    let rotation = store
        .describe("prod/api/credentials")
        .expect("describe")
        .summary
        .rotation
        .expect("rotation state");
    assert!(rotation.enabled);
    assert_eq!(rotation.schedule.as_deref(), Some("rate(7 days)"));
    assert_eq!(rotation.window.as_deref(), Some("2h"));
    assert_eq!(rotation.function.as_deref(), Some(FAKE_LAMBDA));
    assert!(rotation.next_rotation.is_some());

    // Editing the configuration rotates nothing: still the original three
    // versions.
    assert_eq!(
        store
            .list_versions("prod/api/credentials")
            .expect("history")
            .len(),
        3
    );
    assert_eq!(driver.secret_store_calls().mutations.len(), 2);
}

#[test]
fn enabling_rotation_on_a_secret_without_a_function_needs_one() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let error = store
        .update_rotation(&SecretRotationUpdate::enable(
            "prod/notify/webhook-token",
            "rate(30 days)",
        ))
        .expect_err("no rotation function is configured");
    assert!(error.to_string().contains("function"), "{error}");
    assert!(driver.secret_store_calls().mutations.is_empty());

    store
        .update_rotation(
            &SecretRotationUpdate::enable("prod/notify/webhook-token", "rate(30 days)")
                .with_function(FAKE_LAMBDA),
        )
        .expect("a function makes it valid");
    let rotation = store
        .describe("prod/notify/webhook-token")
        .expect("describe")
        .summary
        .rotation
        .expect("rotation state");
    assert!(rotation.enabled);
    assert!(store.rotate_now("prod/notify/webhook-token").is_ok());
}

#[test]
fn an_invalid_rotation_edit_and_a_pointless_disable_are_refused() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let error = store
        .update_rotation(&SecretRotationUpdate::enable(
            "prod/api/credentials",
            "every month",
        ))
        .expect_err("a schedule the service rejects");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");

    let error = store
        .update_rotation(&SecretRotationUpdate::disable("prod/notify/webhook-token"))
        .expect_err("rotation is not on");
    assert!(error.to_string().contains("not enabled"), "{error}");

    let error = store
        .update_rotation(&SecretRotationUpdate::disable("prod/db/master"))
        .expect_err("managed");
    assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");

    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn a_cron_schedule_is_rescheduled_a_day_out_and_an_hourly_one_by_its_hours() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    for (schedule, hours) in [("cron(0 16 1 * ? *)", 24), ("rate(6 hours)", 6)] {
        store
            .update_rotation(&SecretRotationUpdate::enable(
                "prod/api/credentials",
                schedule,
            ))
            .expect("enable");
        let rotation = store
            .describe("prod/api/credentials")
            .expect("describe")
            .summary
            .rotation
            .expect("rotation");
        assert_eq!(
            rotation.window.as_deref(),
            Some("4h"),
            "an update that names no window keeps the one the secret has"
        );
        let next = rotation.next_rotation.expect("scheduled");

        let started = store.rotate_now("prod/api/credentials").expect("rotate");
        assert!(started.version.is_some());
        let rotation = store
            .describe("prod/api/credentials")
            .expect("describe")
            .summary
            .rotation
            .expect("rotation");
        let last = rotation.last_rotated.expect("stamped");
        assert_eq!(
            (rotation.next_rotation.expect("next") - last).num_hours(),
            hours
        );
        assert_eq!((next - last).num_hours(), hours, "{schedule}");
    }
}

// --- Tags and description ---

fn tag_pairs(details: &SecretDetails) -> Vec<(String, String)> {
    details
        .tags
        .iter()
        .map(|tag| (tag.key.clone(), tag.value.clone()))
        .collect()
}

fn pair(key: &str, value: &str) -> (String, String) {
    (key.to_string(), value.to_string())
}

#[test]
fn a_tag_edit_overwrites_adds_and_removes_and_touches_no_version() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    store
        .update_tags(
            &SecretTagsUpdate::new("prod/api/credentials")
                .set("env", "staging")
                .set("owner", "platform")
                .remove("team")
                .remove("never-there"),
        )
        .expect("a valid tag edit");

    let details = store.describe("prod/api/credentials").expect("describe");
    assert_eq!(
        tag_pairs(&details),
        vec![pair("env", "staging"), pair("owner", "platform")]
    );
    assert_eq!(details.version_count, Some(3));
    assert_eq!(details.summary.version.as_deref(), Some("v-current"));
    assert_eq!(
        store
            .list_versions("prod/api/credentials")
            .expect("history")
            .len(),
        3
    );
    assert_eq!(driver.secret_store_calls().mutations.len(), 1);
}

#[test]
fn a_tag_edit_on_a_parameter_works_the_same_way() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    store
        .update_tags(
            &SecretTagsUpdate::new("/app/prod/db/host")
                .set("env", "prod")
                .remove("app"),
        )
        .expect("a valid tag edit");

    let details = store.describe("/app/prod/db/host").expect("describe");
    assert_eq!(tag_pairs(&details), vec![pair("env", "prod")]);
    assert_eq!(details.summary.version.as_deref(), Some("3"));
}

#[test]
fn a_tag_edit_the_service_would_reject_changes_nothing() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");
    let before = tag_pairs(&store.describe("prod/api/credentials").expect("describe"));

    let error = store
        .update_tags(&SecretTagsUpdate::new("prod/api/credentials").set("aws:env", "x"))
        .expect_err("a reserved key");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");

    // Fifty tags in total is the ceiling; the secret already has two.
    let mut many = SecretTagsUpdate::new("prod/api/credentials");
    for index in 0..49 {
        many = many.set(format!("key-{index}"), "v");
    }
    let error = store.update_tags(&many).expect_err("too many tags overall");
    assert!(error.to_string().contains("50"), "{error}");

    for error in [
        store
            .update_tags(&SecretTagsUpdate::new("prod/db/master").set("a", "b"))
            .expect_err("managed"),
        store
            .update_tags(&SecretTagsUpdate::new("prod/deploy/legacy-token").set("a", "b"))
            .expect_err("pending deletion"),
    ] {
        assert!(
            matches!(
                error,
                DbError::PermissionDenied(_) | DbError::QueryFailed(_)
            ),
            "{error}"
        );
    }
    let error = store
        .update_tags(&SecretTagsUpdate::new("prod/missing").set("a", "b"))
        .expect_err("missing");
    assert!(matches!(error, DbError::ObjectNotFound(_)), "{error}");

    assert_eq!(
        tag_pairs(&store.describe("prod/api/credentials").expect("describe")),
        before
    );
    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn a_secret_description_changes_alone_and_clearing_it_empties_it() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let change = store
        .update_description(&SecretDescriptionUpdate::new(
            "prod/api/credentials",
            "Payments API credentials, rotated monthly",
        ))
        .expect("a description edit");

    assert!(change.version.is_none(), "no version is written");
    let details = store.describe("prod/api/credentials").expect("describe");
    assert_eq!(
        details.summary.description.as_deref(),
        Some("Payments API credentials, rotated monthly")
    );
    assert_eq!(details.version_count, Some(3));
    assert_eq!(details.summary.version.as_deref(), Some("v-current"));
    assert_eq!(
        exposed_text(
            &store
                .get_value("prod/api/credentials", None)
                .expect("value")
        ),
        format!(r#"{{"username":"api-user","password":"{FAKE_SECRET_PASSWORD}","port":5432}}"#)
    );

    store
        .update_description(&SecretDescriptionUpdate::new("prod/api/credentials", ""))
        .expect("clearing");
    assert!(
        store
            .describe("prod/api/credentials")
            .expect("describe")
            .summary
            .description
            .is_none()
    );

    // A description is listed too.
    assert_eq!(driver.secret_store_calls().mutations.len(), 2);

    let error = store
        .update_description(&SecretDescriptionUpdate::new(
            "prod/api/credentials",
            "d".repeat(2049),
        ))
        .expect_err("longer than the service takes");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
}

#[test]
fn a_parameter_description_is_a_value_preserving_write_that_creates_a_version() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");
    let before = exposed_text(&store.get_value("/app/prod/db/host", None).expect("value"));

    let change = store
        .update_description(&SecretDescriptionUpdate::new(
            "/app/prod/db/host",
            "Primary database host",
        ))
        .expect("a value-preserving write");

    assert_eq!(change.version.as_deref(), Some("4"));
    let details = store.describe("/app/prod/db/host").expect("describe");
    assert_eq!(
        details.summary.description.as_deref(),
        Some("Primary database host")
    );
    assert_eq!(details.summary.version.as_deref(), Some("4"));
    assert_eq!(details.summary.value_type.as_deref(), Some("String"));
    assert_eq!(details.data_type.as_deref(), Some("text"));

    let current = store.get_value("/app/prod/db/host", None).expect("value");
    assert_eq!(current.version.as_deref(), Some("4"));
    assert_eq!(exposed_text(&current), before, "the value is untouched");

    let versions = store.list_versions("/app/prod/db/host").expect("history");
    assert_eq!(versions.len(), 4);
    assert_eq!(versions[0].id, "4");
    assert_eq!(
        versions[0].modified_by.as_deref(),
        Some("arn:fake:user/editor")
    );

    let calls = driver.secret_store_calls();
    assert_eq!(calls.mutations.len(), 1);
    assert!(
        calls.put_attempts.is_empty(),
        "it is not a caller-supplied value write"
    );
}

#[test]
fn a_parameter_description_fails_closed_when_the_value_cannot_be_read() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let error = store
        .update_description(&SecretDescriptionUpdate::new(
            "/app/prod/db/password",
            "Database password",
        ))
        .expect_err("a SecureString the caller may not decrypt cannot be re-sent");

    assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");
    let details = store.describe("/app/prod/db/password").expect("describe");
    assert!(details.summary.description.is_none());
    assert_eq!(details.summary.version.as_deref(), Some("3"));
    assert!(driver.secret_store_calls().mutations.is_empty());

    let error = store
        .update_description(&SecretDescriptionUpdate::new(
            "/app/prod/db/host",
            "d".repeat(1025),
        ))
        .expect_err("longer than the parameter store takes");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
}

#[test]
fn a_parameter_description_cannot_be_cleared() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let error = store
        .update_description(&SecretDescriptionUpdate::new("/app/prod/db/host", ""))
        .expect_err("a rewrite cannot clear a description");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(driver.secret_store_calls().mutations.is_empty());
}

// --- Creating ---

#[test]
fn a_created_secret_is_listed_described_and_readable_with_one_current_version() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let created = store
        .create_secret(
            &SecretCreateRequest::text("prod/billing/api-key", "created-value-1")
                .with_description("Billing API key")
                .with_encryption_key("alias/billing")
                .with_tag("team", "billing")
                .with_tag("env", "prod"),
        )
        .expect("a new name is created");

    let version = created.version.expect("the creation reports its version");
    assert!(created.resource_id.is_some());

    let listed = store
        .list(&SecretListRequest {
            name_filter: Some("prod/billing".to_string()),
            ..SecretListRequest::default()
        })
        .expect("list");
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].name, "prod/billing/api-key");

    let details = store.describe("prod/billing/api-key").expect("describe");
    assert_eq!(
        details.summary.description.as_deref(),
        Some("Billing API key")
    );
    assert_eq!(details.encryption_key.as_deref(), Some("alias/billing"));
    assert_eq!(
        tag_pairs(&details),
        vec![pair("team", "billing"), pair("env", "prod")]
    );
    assert_eq!(details.version_count, Some(1));
    assert_eq!(details.summary.version.as_deref(), Some(version.as_str()));
    assert!(details.created.is_some());
    assert_eq!(
        labels_of(
            &store
                .list_versions("prod/billing/api-key")
                .expect("history")
        ),
        vec![(version.clone(), vec!["AWSCURRENT".to_string()])]
    );

    let value = store
        .get_value("prod/billing/api-key", None)
        .expect("value");
    assert_eq!(value.version.as_deref(), Some(version.as_str()));
    assert_eq!(exposed_text(&value), "created-value-1");

    let calls = driver.secret_store_calls();
    assert_eq!(calls.mutations.len(), 1);
    assert!(calls.put_attempts.is_empty());
    let rendered = format!("{:?}", calls.mutations);
    assert!(!rendered.contains("created-value-1"), "{rendered}");
}

#[test]
fn a_secret_created_without_a_key_uses_the_default_key() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    store
        .create_secret(&SecretCreateRequest::text("prod/plain", "created-value-2"))
        .expect("created");

    assert_eq!(
        store
            .describe("prod/plain")
            .expect("describe")
            .encryption_key
            .as_deref(),
        Some("aws/secretsmanager")
    );
}

#[test]
fn creating_never_overwrites_an_existing_name() {
    let (driver, connection) = connected();

    for (store_id, name) in [
        (FAKE_SECRETS_STORE_ID, "prod/api/credentials"),
        (FAKE_PARAMETER_STORE_ID, "/app/prod/db/host"),
    ] {
        let store = connection.secret_store(store_id).expect("store");
        let before = store.describe(name).expect("describe");
        let before_value = exposed_text(&store.get_value(name, None).expect("value"));

        let error = store
            .create_secret(&SecretCreateRequest::text(name, "created-value-3"))
            .expect_err("the name exists");

        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(error.to_string().contains("already exists"), "{error}");
        assert!(!error.to_string().contains("created-value-3"), "{error}");
        assert_eq!(
            store.describe(name).expect("describe").summary.version,
            before.summary.version
        );
        assert_eq!(
            exposed_text(&store.get_value(name, None).expect("value")),
            before_value
        );
    }

    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn a_secrets_manager_create_refuses_a_name_a_type_and_a_tier_it_does_not_have() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");

    let error = store
        .create_secret(&SecretCreateRequest::text("bad name", "created-value-4"))
        .expect_err("a space is not a secret name character");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");

    for request in [
        SecretCreateRequest::text("prod/typed", "created-value-4").with_value_type("String"),
        SecretCreateRequest::text("prod/tiered", "created-value-4").with_tier("Advanced"),
    ] {
        let error = store.create_secret(&request).expect_err("no type or tier");
        assert!(matches!(error, DbError::NotSupported(_)), "{error}");
    }

    let error = store
        .create_secret(&SecretCreateRequest::text("prod/api", ""))
        .expect_err("no value");
    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");

    assert!(driver.secret_store_calls().mutations.is_empty());
    assert!(store.describe("prod/typed").is_err());
}

#[test]
fn a_created_parameter_has_a_type_a_tier_a_version_and_appears_in_its_path() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    let created = store
        .create_secret(
            &SecretCreateRequest::text("/app/prod/db/replica", "created-value-5")
                .with_value_type("SecureString")
                .with_tier("Advanced")
                .with_encryption_key("alias/app-prod")
                .with_description("Replica password")
                .with_tag("app", "shop"),
        )
        .expect("created");

    assert_eq!(created.version.as_deref(), Some("1"));
    let details = store.describe("/app/prod/db/replica").expect("describe");
    assert_eq!(details.summary.value_type.as_deref(), Some("SecureString"));
    assert_eq!(details.summary.tier.as_deref(), Some("Advanced"));
    assert_eq!(
        details.summary.description.as_deref(),
        Some("Replica password")
    );
    assert_eq!(details.encryption_key.as_deref(), Some("alias/app-prod"));
    assert_eq!(details.data_type.as_deref(), Some("text"));
    assert_eq!(tag_pairs(&details), vec![pair("app", "shop")]);
    assert_eq!(details.summary.version.as_deref(), Some("1"));

    let mut names: Vec<String> = Vec::new();
    let mut token = None;
    loop {
        let page = store
            .list(&SecretListRequest {
                path: Some("/app/prod/db/".to_string()),
                recursive: true,
                page_token: token,
                ..SecretListRequest::default()
            })
            .expect("list");
        names.extend(page.entries.into_iter().map(|entry| entry.name));
        token = page.next_page_token;
        if token.is_none() {
            break;
        }
    }
    assert!(
        names.contains(&"/app/prod/db/replica".to_string()),
        "{names:?}"
    );

    assert_eq!(
        exposed_text(
            &store
                .get_value("/app/prod/db/replica", None)
                .expect("value")
        ),
        "created-value-5"
    );
    assert_eq!(driver.secret_store_calls().mutations.len(), 1);
}

#[test]
fn a_parameter_created_with_defaults_is_a_standard_string() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    store
        .create_secret(&SecretCreateRequest::text(
            "/app/prod/plain",
            "created-value-6",
        ))
        .expect("created");

    let details = store.describe("/app/prod/plain").expect("describe");
    assert_eq!(details.summary.value_type.as_deref(), Some("String"));
    assert_eq!(details.summary.tier.as_deref(), Some("Standard"));
    assert!(details.encryption_key.is_none());
}

#[test]
fn a_parameter_create_refuses_what_the_service_would() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    for request in [
        SecretCreateRequest::text("/app/x", "created-value-7").with_value_type("Binary"),
        SecretCreateRequest::text("/app/x", "created-value-7").with_tier("Platinum"),
        SecretCreateRequest::text("/app/x", "created-value-7").with_encryption_key("alias/k"),
        SecretCreateRequest::text("/app/has space", "created-value-7"),
        SecretCreateRequest::text("/aws/reserved", "created-value-7"),
        SecretCreateRequest::text("ssm-prefixed", "created-value-7"),
    ] {
        let error = store.create_secret(&request).expect_err("refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
        assert!(!error.to_string().contains("created-value-7"), "{error}");
    }

    assert!(driver.secret_store_calls().mutations.is_empty());
    assert!(store.describe("/app/x").is_err());
}

#[test]
fn the_write_error_knob_fails_a_create_without_creating_anything() {
    let driver = FakeDriver::new(DbKind::AwsSecrets)
        .with_secret_write_error("prod/denied", "create denied by the fixture");
    let profile = ConnectionProfile::new_with_driver(
        "fake-aws-secrets",
        DbKind::AwsSecrets,
        "builtin:fake-aws-secrets",
        DbConfig::default_aws_secrets(),
    );
    let connection = driver.connect(&profile).expect("connect");
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("store");

    let error = store
        .create_secret(&SecretCreateRequest::text("prod/denied", "created-value-8"))
        .expect_err("the knob denies the create");

    assert!(matches!(error, DbError::PermissionDenied(_)), "{error}");
    assert!(store.describe("prod/denied").is_err());
    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn rolling_back_onto_a_pending_version_is_refused_and_moves_no_label() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");
    store
        .put_value(
            &SecretPutRequest::text("prod/api/credentials", "pending-probe")
                .with_stage(SecretVersionStage::Pending),
        )
        .expect("a staged write");
    let before = labels_of(
        &store
            .list_versions("prod/api/credentials")
            .expect("history"),
    );
    let pending = before
        .iter()
        .find(|(_, labels)| labels.iter().any(|label| label == "AWSPENDING"))
        .map(|(id, _)| id.clone())
        .expect("a pending version exists");

    let error = store
        .roll_back(&SecretRollbackRequest::new("prod/api/credentials", pending))
        .expect_err("an unfinished rotation is not a roll back target");

    assert!(error.to_string().contains("AWSPENDING"), "{error}");
    assert_eq!(
        labels_of(
            &store
                .list_versions("prod/api/credentials")
                .expect("history")
        ),
        before
    );
    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn the_history_marks_exactly_one_current_version_across_writes() {
    let (_driver, connection) = connected();
    let flat = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake flat store exists");
    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");
    let name = "prod/api/credentials";
    let parameter = "/app/prod/db/host";

    assert_eq!(
        current_ids(&flat.list_versions(name).unwrap()),
        ["v-current"]
    );
    assert_eq!(
        current_ids(&parameters.list_versions(parameter).unwrap()),
        ["3"]
    );

    let mut pending = SecretPutRequest::text(name, "staged value");
    pending.stage = Some(dbflux_core::SecretVersionStage::Pending);
    flat.put_value(&pending).expect("a staged write");
    assert_eq!(
        current_ids(&flat.list_versions(name).unwrap()),
        ["v-current"],
        "a pending write moves no current version"
    );

    let written = flat
        .put_value(&SecretPutRequest::text(name, "new current"))
        .expect("a current write")
        .version
        .expect("version");
    assert_eq!(
        current_ids(&flat.list_versions(name).unwrap()),
        [written.as_str()]
    );

    parameters
        .put_value(&SecretPutRequest::text(parameter, "db2"))
        .expect("a parameter write");
    assert_eq!(
        current_ids(&parameters.list_versions(parameter).unwrap()),
        ["4"]
    );
}

// --- Review follow-ups: pending rotations, descriptions, names ---

#[test]
fn rotating_is_refused_while_another_version_is_pending_and_changes_nothing() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");
    let mut pending = SecretPutRequest::text("prod/api/credentials", "staged value");
    pending.stage = Some(SecretVersionStage::Pending);
    store.put_value(&pending).expect("a staged write");
    let before = labels_of(
        &store
            .list_versions("prod/api/credentials")
            .expect("history"),
    );
    let mutations = driver.secret_store_calls().mutations.len();

    let error = store
        .rotate_now("prod/api/credentials")
        .expect_err("a rotation is already unfinished");

    assert!(error.to_string().contains("in progress"), "{error}");
    assert_eq!(
        labels_of(
            &store
                .list_versions("prod/api/credentials")
                .expect("history")
        ),
        before
    );
    assert_eq!(driver.secret_store_calls().mutations.len(), mutations);
}

#[test]
fn turning_rotation_off_reports_the_version_an_unfinished_rotation_left() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");
    let mut pending = SecretPutRequest::text("prod/api/credentials", "staged value");
    pending.stage = Some(SecretVersionStage::Pending);
    let staged = store
        .put_value(&pending)
        .expect("a staged write")
        .version
        .expect("version");

    let change = store
        .update_rotation(&SecretRotationUpdate::disable("prod/api/credentials"))
        .expect("disable");
    assert_eq!(change.unfinished_version.as_deref(), Some(staged.as_str()));

    let change = store
        .update_rotation(
            &SecretRotationUpdate::enable("prod/api/credentials", "rate(30 days)")
                .with_function(FAKE_LAMBDA),
        )
        .expect("enable");
    assert_eq!(change.unfinished_version, None, "turning it on leaves none");

    let (_driver, connection) = connected();
    let clean = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists")
        .update_rotation(&SecretRotationUpdate::disable("prod/api/credentials"))
        .expect("disable");
    assert_eq!(clean.unfinished_version, None);
}

#[test]
fn a_rotation_whose_next_date_overflows_is_refused_and_changes_nothing() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");
    let before = store
        .describe("prod/api/credentials")
        .expect("describe")
        .summary
        .rotation;

    let error = store
        .update_rotation(&SecretRotationUpdate::enable(
            "prod/api/credentials",
            "rate(100000000 days)",
        ))
        .expect_err("the next date does not exist");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert_eq!(
        store
            .describe("prod/api/credentials")
            .expect("describe")
            .summary
            .rotation,
        before
    );
    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn a_flat_description_edit_stamps_the_change_time() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake store exists");
    let before = store
        .describe("prod/api/credentials")
        .expect("describe")
        .summary
        .last_changed;

    store
        .update_description(&SecretDescriptionUpdate::new(
            "prod/api/credentials",
            "A new description",
        ))
        .expect("edit");

    let after = store
        .describe("prod/api/credentials")
        .expect("describe")
        .summary
        .last_changed;
    assert!(after > before, "{after:?} should be later than {before:?}");
}

#[test]
fn a_parameter_name_of_only_slashes_or_with_levels_and_no_leading_slash_is_refused() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    for name in ["/", "app/prod/token"] {
        let error = store
            .create_secret(&SecretCreateRequest::text(name, "created-value-9"))
            .expect_err("refused");
        assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    }

    assert!(driver.secret_store_calls().mutations.is_empty());
}

#[test]
fn an_unprefixed_parameter_is_listed_at_the_root_directly_and_recursively() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");
    store
        .create_secret(&SecretCreateRequest::text("plain", "created-value-10"))
        .expect("a flat name is valid");

    for recursive in [false, true] {
        let listed = store
            .list(&SecretListRequest {
                path: Some("/".to_string()),
                recursive,
                name_filter: Some("plain".to_string()),
                ..SecretListRequest::default()
            })
            .expect("list");

        assert!(
            listed.entries.iter().any(|entry| entry.name == "plain"),
            "recursive={recursive}: {:?}",
            listed.entries
        );
    }
    assert!(store.describe("plain").is_ok());
}

#[test]
fn intelligent_tiering_resolves_to_the_tier_the_value_needs() {
    let (_driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    store
        .create_secret(
            &SecretCreateRequest::text("/app/small", "small").with_tier("Intelligent-Tiering"),
        )
        .expect("created");
    store
        .create_secret(
            &SecretCreateRequest::text("/app/large", "l".repeat(4097))
                .with_tier("Intelligent-Tiering"),
        )
        .expect("created");

    let tier_of = |name: &str| store.describe(name).expect("describe").summary.tier;
    assert_eq!(tier_of("/app/small").as_deref(), Some("Standard"));
    assert_eq!(tier_of("/app/large").as_deref(), Some("Advanced"));
}

#[test]
fn a_standard_tier_value_over_4096_bytes_is_refused() {
    let (driver, connection) = connected();
    let store = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");

    store
        .create_secret(&SecretCreateRequest::text("/app/fits", "v".repeat(4096)))
        .expect("4096 bytes fit");
    let mutations = driver.secret_store_calls().mutations.len();
    let error = store
        .create_secret(&SecretCreateRequest::text("/app/over", "v".repeat(4097)))
        .expect_err("one byte over");

    assert!(matches!(error, DbError::ConstraintViolation(_)), "{error}");
    assert!(store.describe("/app/over").is_err());
    assert_eq!(driver.secret_store_calls().mutations.len(), mutations);

    store
        .create_secret(
            &SecretCreateRequest::text("/app/advanced", "v".repeat(4097)).with_tier("Advanced"),
        )
        .expect("the advanced tier holds more");
}

#[test]
fn the_fake_parameter_store_declares_choices_and_the_manager_store_none() {
    let (_driver, connection) = connected();

    let parameters = connection
        .secret_store(FAKE_PARAMETER_STORE_ID)
        .expect("the fake parameter store exists");
    let choices = parameters
        .descriptor()
        .create_choices
        .as_ref()
        .expect("the typed fake declares choices");
    assert_eq!(
        choices.value_types(),
        ["String", "StringList", "SecureString"]
    );
    assert_eq!(choices.default_value_type(), "SecureString");
    assert!(choices.takes_encryption_key("SecureString"));
    assert_eq!(
        choices.tiers(),
        ["Standard", "Advanced", "Intelligent-Tiering"]
    );
    assert_eq!(choices.default_tier(), Some("Standard"));

    let manager = connection
        .secret_store(FAKE_SECRETS_STORE_ID)
        .expect("the fake manager store exists");
    assert_eq!(manager.descriptor().create_choices, None);
}
