#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use std::time::Duration;

use aws_sdk_secretsmanager::config::{Credentials, Region};
use aws_sdk_secretsmanager::primitives::Blob;
use aws_sdk_ssm::types::{ParameterType, Tag as ParameterTag};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, ConnectionProfile, DbConfig, DbDriver, DbError, DbKind, SecretListRequest,
    SecretPayload, SecretPutRequest, SecretStoreCapabilities, SecretStoreSource, SecretValue,
    SecretVersionStage,
};
use dbflux_driver_aws_secrets::{AwsSecretsDriver, PARAMETER_STORE_ID, SECRETS_MANAGER_ID};
use dbflux_test_support::containers;

const REGION: &str = "us-east-1";
const ACCESS_KEY_ID: &str = "test";
const SECRET_ACCESS_KEY: &str = "test";

fn profile(endpoint: &str) -> ConnectionProfile {
    ConnectionProfile::new_with_driver(
        "live-localstack-secrets",
        DbKind::AwsSecrets,
        "builtin:aws_secrets",
        DbConfig::AwsSecrets {
            region: REGION.to_string(),
            profile: None,
            access_key_id: Some(ACCESS_KEY_ID.to_string()),
            endpoint: Some(endpoint.to_string()),
        },
    )
}

fn connect(endpoint: &str) -> Result<Box<dyn Connection>, DbError> {
    let driver = AwsSecretsDriver::new();
    let profile = profile(endpoint);
    let secret = SecretString::from(SECRET_ACCESS_KEY.to_string());

    containers::retry_db_operation(Duration::from_secs(30), || {
        driver.connect_with_secrets(&profile, Some(&secret), None)
    })
}

fn store<'a>(connection: &'a dyn Connection, id: &str) -> &'a dyn SecretStoreSource {
    connection
        .secret_store(id)
        .unwrap_or_else(|| panic!("connection must expose the {id} store"))
}

/// Seeding goes through the SDK directly, because the driver only reads.
struct Seeder {
    runtime: tokio::runtime::Runtime,
    secrets: aws_sdk_secretsmanager::Client,
    ssm: aws_sdk_ssm::Client,
}

impl Seeder {
    fn new(endpoint: &str) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("seeder runtime");
        let credentials = Credentials::new(ACCESS_KEY_ID, SECRET_ACCESS_KEY, None, None, "seed");

        let shared = runtime.block_on(
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new(REGION))
                .credentials_provider(credentials)
                .endpoint_url(endpoint)
                .load(),
        );

        Self {
            secrets: aws_sdk_secretsmanager::Client::new(&shared),
            ssm: aws_sdk_ssm::Client::new(&shared),
            runtime,
        }
    }

    fn secret_string(&self, name: &str, value: &str) -> String {
        let output = self
            .runtime
            .block_on(
                self.secrets
                    .create_secret()
                    .name(name)
                    .secret_string(value)
                    .send(),
            )
            .expect("create_secret");

        output.version_id().expect("version id").to_string()
    }

    fn put_secret_string(&self, name: &str, value: &str) -> String {
        let output = self
            .runtime
            .block_on(
                self.secrets
                    .put_secret_value()
                    .secret_id(name)
                    .secret_string(value)
                    .send(),
            )
            .expect("put_secret_value");

        output.version_id().expect("version id").to_string()
    }

    fn secret_binary(&self, name: &str, value: &[u8]) {
        self.runtime
            .block_on(
                self.secrets
                    .create_secret()
                    .name(name)
                    .secret_binary(Blob::new(value.to_vec()))
                    .send(),
            )
            .expect("create_secret binary");
    }

    fn tag_secret(&self, name: &str, key: &str, value: &str) {
        self.runtime
            .block_on(
                self.secrets
                    .tag_resource()
                    .secret_id(name)
                    .tags(
                        aws_sdk_secretsmanager::types::Tag::builder()
                            .key(key)
                            .value(value)
                            .build(),
                    )
                    .send(),
            )
            .expect("tag_resource");
    }

    fn parameter(&self, name: &str, value: &str, parameter_type: ParameterType) -> i64 {
        let output = self
            .runtime
            .block_on(
                self.ssm
                    .put_parameter()
                    .name(name)
                    .value(value)
                    .r#type(parameter_type)
                    .overwrite(true)
                    .send(),
            )
            .expect("put_parameter");

        output.version()
    }

    fn delete_secret(&self, name: &str) {
        self.runtime
            .block_on(
                self.secrets
                    .delete_secret()
                    .secret_id(name)
                    .recovery_window_in_days(7)
                    .send(),
            )
            .expect("delete_secret");
    }

    fn tag_parameter(&self, name: &str, key: &str, value: &str) {
        self.runtime
            .block_on(
                self.ssm
                    .add_tags_to_resource()
                    .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
                    .resource_id(name)
                    .tags(
                        ParameterTag::builder()
                            .key(key)
                            .value(value)
                            .build()
                            .unwrap(),
                    )
                    .send(),
            )
            .expect("add_tags_to_resource");
    }
}

fn text(value: &SecretValue) -> String {
    match &value.payload {
        SecretPayload::Text(text) => text.expose_secret().to_string(),
        SecretPayload::Binary(_) => panic!("expected a text value"),
    }
}

fn names(page: &dbflux_core::SecretListPage) -> Vec<String> {
    let mut names: Vec<String> = page
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    names.sort();
    names
}

#[test]
#[ignore = "requires Docker daemon"]
fn connection_exposes_secrets_manager_then_parameter_store() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let connection = connect(&endpoint)?;

        let descriptors: Vec<_> = connection
            .secret_stores()
            .iter()
            .map(|store| store.descriptor().clone())
            .collect();

        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].id, SECRETS_MANAGER_ID);
        assert_eq!(descriptors[0].label, "Secrets Manager");
        assert!(descriptors[0].supports(
            SecretStoreCapabilities::VERSIONS
                | SecretStoreCapabilities::VERSION_LABELS
                | SecretStoreCapabilities::ROTATION
                | SecretStoreCapabilities::SOFT_DELETE
                | SecretStoreCapabilities::WRITE
        ));
        assert_eq!(descriptors[1].id, PARAMETER_STORE_ID);
        assert_eq!(descriptors[1].label, "Parameter Store");
        assert!(descriptors[1].supports(
            SecretStoreCapabilities::VERSIONS
                | SecretStoreCapabilities::HIERARCHICAL
                | SecretStoreCapabilities::TYPED_VALUES
                | SecretStoreCapabilities::WRITE
        ));

        connection.ping()
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_listing_paginates_and_filters_by_name_prefix() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        seeder.secret_string("app/prod/db", "one");
        seeder.secret_string("app/prod/api", "two");
        seeder.secret_string("billing/key", "three");

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        let first = secrets.list(&SecretListRequest {
            page_size: Some(1),
            ..SecretListRequest::default()
        })?;
        assert_eq!(first.entries.len(), 1);
        let token = first
            .next_page_token
            .clone()
            .expect("a page of one must have a next page");

        let second = secrets.list(&SecretListRequest {
            page_size: Some(1),
            page_token: Some(token),
            ..SecretListRequest::default()
        })?;
        assert_eq!(second.entries.len(), 1);
        assert_ne!(first.entries[0].name, second.entries[0].name);

        let filtered = secrets.list(&SecretListRequest {
            name_filter: Some("app/prod".to_string()),
            ..SecretListRequest::default()
        })?;
        assert_eq!(names(&filtered), ["app/prod/api", "app/prod/db"]);
        assert!(filtered.child_paths.is_empty());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_describe_reports_metadata_tags_and_versions() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        seeder.secret_string("described", "first");
        let current = seeder.put_secret_string("described", "second");
        seeder.tag_secret("described", "team", "payments");

        let connection = connect(&endpoint)?;
        let details = store(connection.as_ref(), SECRETS_MANAGER_ID).describe("described")?;

        assert_eq!(details.summary.name, "described");
        assert_eq!(details.summary.version.as_deref(), Some(current.as_str()));
        assert!(
            details
                .resource_id
                .as_deref()
                .is_some_and(|arn| arn.starts_with("arn:aws:secretsmanager:"))
        );
        assert!(details.encryption_key.is_some());
        assert_eq!(details.version_count, Some(2));
        assert!(details.version_labels.contains(&"AWSCURRENT".to_string()));
        assert!(details.version_labels.contains(&"AWSPREVIOUS".to_string()));
        assert_eq!(details.tags.len(), 1);
        assert_eq!(details.tags[0].key, "team");
        assert_eq!(details.tags[0].value, "payments");
        assert!(details.summary.owning_service.is_none());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_lists_versions_by_label_and_each_id_fetches_its_value() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.secret_string("history", "first");
        let second = seeder.put_secret_string("history", "second");

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        let versions = secrets.list_versions("history")?;

        // LocalStack stores `CreatedDate` at whole seconds and lists oldest
        // first, so two back-to-back writes tie and the order is not
        // assertable here; versions are identified by label instead.
        let by_label = |label: &str| {
            versions
                .iter()
                .find(|version| version.labels.iter().any(|candidate| candidate == label))
                .map(|version| version.id.as_str())
        };
        assert_eq!(versions.len(), 2);
        assert_eq!(by_label("AWSCURRENT"), Some(second.as_str()));
        assert_eq!(by_label("AWSPREVIOUS"), Some(first.as_str()));
        assert!(versions.iter().all(|version| version.created.is_some()));
        assert!(
            versions
                .windows(2)
                .all(|pair| pair[0].created >= pair[1].created)
        );

        for (label, expected) in [("AWSCURRENT", "second"), ("AWSPREVIOUS", "first")] {
            let id = by_label(label).expect("a labeled version");
            let value = secrets.get_value("history", Some(id))?;
            assert_eq!(text(&value), expected);
        }

        assert!(matches!(
            secrets.list_versions("missing"),
            Err(DbError::ObjectNotFound(_))
        ));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_get_value_returns_text_json_binary_and_versions() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.secret_string("versioned", "first");
        seeder.put_secret_string("versioned", "second");
        seeder.secret_string("json", r#"{"user":"admin","port":5432}"#);
        seeder.secret_binary("binary", &[0, 159, 146, 150]);

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        assert_eq!(text(&secrets.get_value("versioned", None)?), "second");

        let previous = secrets.get_value("versioned", Some(&first))?;
        assert_eq!(text(&previous), "first");
        assert_eq!(previous.version.as_deref(), Some(first.as_str()));

        let pairs = secrets
            .get_value("json", None)?
            .key_value_pairs()
            .expect("a flat JSON object is key/value");
        let pairs: Vec<(String, String)> = pairs
            .into_iter()
            .map(|(key, value)| (key, value.expose_secret().to_string()))
            .collect();
        assert_eq!(
            pairs,
            [
                ("user".to_string(), "admin".to_string()),
                ("port".to_string(), "5432".to_string())
            ]
        );

        let binary = secrets.get_value("binary", None)?;
        match &binary.payload {
            SecretPayload::Binary(bytes) => assert_eq!(bytes.expose_secret(), &[0, 159, 146, 150]),
            SecretPayload::Text(_) => panic!("SecretBinary must stay binary"),
        }

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_missing_secret_is_object_not_found() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        assert!(matches!(
            secrets.get_value("does-not-exist", None),
            Err(DbError::ObjectNotFound(_))
        ));
        assert!(matches!(
            secrets.describe("does-not-exist"),
            Err(DbError::ObjectNotFound(_))
        ));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_put_value_writes_a_new_current_version() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.secret_string("writeable", "first");

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        let response = secrets.put_value(&SecretPutRequest::text("writeable", "second"))?;
        let new_version = response.version.expect("a put returns the new version id");
        assert_ne!(new_version, first);
        let SecretPayload::Text(_) = response.payload else {
            panic!("a text put stays text");
        };

        let current = secrets.get_value("writeable", None)?;
        assert_eq!(text(&current), "second");
        assert_eq!(current.version.as_deref(), Some(new_version.as_str()));

        let details = secrets.describe("writeable")?;
        assert_eq!(
            details.summary.version.as_deref(),
            Some(new_version.as_str())
        );
        assert!(details.version_labels.contains(&"AWSCURRENT".to_string()));

        Ok(())
    })
}

/// A staged write adds a version carrying `AWSPENDING` without moving the
/// current one: the previous version is still what `GetSecretValue` answers
/// by default, and the new one answers by its version id.
#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_put_value_to_a_pending_stage_moves_no_current_version() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.secret_string("staged", "first");

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        let response = secrets.put_value(
            &SecretPutRequest::text("staged", "second").with_stage(SecretVersionStage::Pending),
        )?;
        let pending = response.version.expect("a put returns the new version id");
        assert_ne!(pending, first);

        // The current version is untouched by a pending write.
        let current = secrets.get_value("staged", None)?;
        assert_eq!(text(&current), "first");
        assert_eq!(current.version.as_deref(), Some(first.as_str()));

        // The new version answers by its version id and carries the
        // pending label, while the secret's current version stays the first.
        let staged = secrets.get_value("staged", Some(&pending))?;
        assert_eq!(text(&staged), "second");
        assert_eq!(staged.version.as_deref(), Some(pending.as_str()));

        let details = secrets.describe("staged")?;
        assert!(
            details.version_labels.contains(&"AWSPENDING".to_string()),
            "version labels must name the pending write: {:?}",
            details.version_labels
        );
        assert_eq!(details.summary.version.as_deref(), Some(first.as_str()));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_put_value_writes_binary() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        Seeder::new(&endpoint).secret_binary("binary-writeable", &[1, 2, 3]);

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        let response =
            secrets.put_value(&SecretPutRequest::binary("binary-writeable", vec![4, 5, 6]))?;
        let new_version = response
            .version
            .clone()
            .expect("a put returns the new version id");
        let SecretPayload::Binary(bytes) = response.payload else {
            panic!("a binary put stays binary");
        };
        assert_eq!(bytes.expose_secret(), &[4, 5, 6]);

        // The write sends the exact bytes — the mapping is pinned by the
        // mapping unit test — but a binary round trip cannot be asserted
        // here. LocalStack's PutSecretValue passes the request blob to its
        // backend already base64-decoded, and its read path base64-decodes
        // the stored value a second time; with `validate=False` that silently
        // drops non-alphabet bytes, so [4, 5, 6] comes back empty. AWS stores
        // and returns the exact bytes; the gap is noted in the README's
        // Limitations. What this test can still assert honestly: the write
        // succeeds, the value the service reports is a binary payload, and
        // the put landed by becoming the current version.
        let current = secrets.get_value("binary-writeable", None)?;
        assert!(current.is_binary());
        assert_eq!(current.version.as_deref(), Some(new_version.as_str()));

        Ok(())
    })
}

/// The driver deliberately delegates the deleted-secret decision to the
/// service. AWS rejects `PutSecretValue` on a secret scheduled for deletion
/// with `InvalidRequestException`, which the error formatter maps to
/// `DbError::QueryFailed`. LocalStack's backend has no deleted-secret check
/// on `PutSecretValue` and accepts the write, so against LocalStack this
/// layer can only assert its actual guarantee: the write is sent and the
/// service's answer is surfaced unchanged. The refusal itself is only
/// assertable against real AWS; a local pre-check would be a policy change,
/// not a test fix.
#[test]
#[ignore = "requires Docker daemon"]
fn secrets_manager_put_value_on_a_scheduled_deletion_surfaces_the_service_answer()
-> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.secret_string("doomed", "value");
        seeder.delete_secret("doomed");

        let connection = connect(&endpoint)?;
        let secrets = store(connection.as_ref(), SECRETS_MANAGER_ID);

        let response = secrets
            .put_value(&SecretPutRequest::text("doomed", "replaced"))
            .expect("the write is sent to the service and its answer is surfaced");
        let new_version = response
            .version
            .expect("the service answered with the new version id");
        assert_ne!(new_version, first);

        // The service's answer is observable: the write landed and the new
        // version is the one the service now reports as current.
        let details = secrets.describe("doomed")?;
        assert_eq!(
            details.summary.version.as_deref(),
            Some(new_version.as_str())
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_put_value_overwrites_and_bumps_the_version() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.parameter("/svc/editable", "first", ParameterType::String);
        seeder.parameter("/svc/token", "first", ParameterType::SecureString);

        let connection = connect(&endpoint)?;
        let parameters = store(connection.as_ref(), PARAMETER_STORE_ID);

        let response = parameters.put_value(&SecretPutRequest::text("/svc/editable", "second"))?;
        assert_eq!(response.version, Some((first + 1).to_string()));
        let current = parameters.get_value("/svc/editable", None)?;
        assert_eq!(text(&current), "second");

        let secure = parameters.put_value(&SecretPutRequest::text("/svc/token", "second"))?;
        assert_eq!(secure.version, Some("2".to_string()));
        let details = parameters.describe("/svc/token")?;
        assert_eq!(details.summary.value_type.as_deref(), Some("SecureString"));

        // A comma list keeps its type: the driver sends the existing type
        // explicitly on every overwrite instead of relying on how the
        // service resolves an omitted `Type`.
        seeder.parameter("/svc/list", "a,b", ParameterType::StringList);
        parameters.put_value(&SecretPutRequest::text("/svc/list", "c,d"))?;
        let list = parameters.describe("/svc/list")?;
        assert_eq!(list.summary.value_type.as_deref(), Some("StringList"));
        assert_eq!(text(&parameters.get_value("/svc/list", None)?), "c,d");

        assert!(matches!(
            parameters.put_value(&SecretPutRequest::text("/missing", "value")),
            Err(DbError::ObjectNotFound(_))
        ));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_lists_history_newest_first_and_each_id_fetches_its_value() -> Result<(), DbError>
{
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        let first = seeder.parameter("/svc/history", "first", ParameterType::String);
        let second = seeder.parameter("/svc/history", "second", ParameterType::String);

        let connection = connect(&endpoint)?;
        let parameters = store(connection.as_ref(), PARAMETER_STORE_ID);

        let versions = parameters.list_versions("/svc/history")?;

        let ids: Vec<&str> = versions.iter().map(|version| version.id.as_str()).collect();
        assert_eq!(ids, [second.to_string(), first.to_string()]);
        assert!(versions.iter().all(|version| version.created.is_some()));

        for (version, expected) in versions.iter().zip(["second", "first"]) {
            let value = parameters.get_value("/svc/history", Some(&version.id))?;
            assert_eq!(text(&value), expected);
        }

        let rendered = format!("{versions:?}");
        assert!(!rendered.contains("first"));
        assert!(!rendered.contains("second"));

        Ok(())
    })
}

fn seed_parameter_tree(seeder: &Seeder) {
    seeder.parameter("root-plain", "a", ParameterType::String);
    seeder.parameter("/top", "b", ParameterType::String);
    seeder.parameter("/app/name", "c", ParameterType::String);
    seeder.parameter(
        "/app/prod/db/password",
        "hunter2",
        ParameterType::SecureString,
    );
    seeder.parameter("/app/prod/db/user", "admin", ParameterType::String);
    seeder.parameter("/app/prod/hosts", "a,b", ParameterType::StringList);
    seeder.parameter("/app/dev/url", "http://dev", ParameterType::String);
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_lists_one_level_with_child_paths() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        seed_parameter_tree(&Seeder::new(&endpoint));

        let connection = connect(&endpoint)?;
        let parameters = store(connection.as_ref(), PARAMETER_STORE_ID);

        let root = parameters.list(&SecretListRequest::default())?;
        // LocalStack stores a single-segment name such as `/top` without its
        // leading slash; either form is a root entry.
        let mut root_names: Vec<String> = names(&root)
            .iter()
            .map(|name| name.trim_start_matches('/').to_string())
            .collect();
        root_names.sort();
        assert_eq!(root_names, ["root-plain", "top"]);
        assert_eq!(root.child_paths, ["/app/"]);

        let app = parameters.list(&SecretListRequest {
            path: Some("/app/".to_string()),
            ..SecretListRequest::default()
        })?;
        assert_eq!(names(&app), ["/app/name"]);
        let mut app_children = app.child_paths.clone();
        app_children.sort();
        assert_eq!(app_children, ["/app/dev/", "/app/prod/"]);

        let prod = parameters.list(&SecretListRequest {
            path: Some("/app/prod/".to_string()),
            ..SecretListRequest::default()
        })?;
        assert_eq!(names(&prod), ["/app/prod/hosts"]);
        assert_eq!(prod.child_paths, ["/app/prod/db/"]);
        assert_eq!(prod.entries[0].value_type.as_deref(), Some("StringList"));
        assert_eq!(prod.entries[0].version.as_deref(), Some("1"));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_recursive_listing_returns_every_entry_and_no_child_paths() -> Result<(), DbError>
{
    containers::with_localstack_secrets_endpoint(|endpoint| {
        seed_parameter_tree(&Seeder::new(&endpoint));

        let connection = connect(&endpoint)?;
        let parameters = store(connection.as_ref(), PARAMETER_STORE_ID);

        let below_app = parameters.list(&SecretListRequest {
            path: Some("/app/".to_string()),
            recursive: true,
            ..SecretListRequest::default()
        })?;
        assert_eq!(
            names(&below_app),
            [
                "/app/dev/url",
                "/app/name",
                "/app/prod/db/password",
                "/app/prod/db/user",
                "/app/prod/hosts"
            ]
        );
        assert!(below_app.child_paths.is_empty());

        let filtered = parameters.list(&SecretListRequest {
            recursive: true,
            name_filter: Some("db".to_string()),
            ..SecretListRequest::default()
        })?;
        assert_eq!(
            names(&filtered),
            ["/app/prod/db/password", "/app/prod/db/user"]
        );

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_describe_reports_metadata_and_tags() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        seeder.parameter("/svc/token", "s3cr3t", ParameterType::SecureString);
        seeder.tag_parameter("/svc/token", "owner", "platform");

        let connection = connect(&endpoint)?;
        let details = store(connection.as_ref(), PARAMETER_STORE_ID).describe("/svc/token")?;

        assert_eq!(details.summary.name, "/svc/token");
        assert_eq!(details.summary.value_type.as_deref(), Some("SecureString"));
        assert_eq!(details.summary.version.as_deref(), Some("1"));
        assert!(details.encryption_key.is_some());
        assert_eq!(details.data_type.as_deref(), Some("text"));
        assert_eq!(details.tags.len(), 1);
        assert_eq!(details.tags[0].key, "owner");
        assert_eq!(details.tags[0].value, "platform");

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_get_value_decrypts_and_selects_versions() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        let seeder = Seeder::new(&endpoint);
        seeder.parameter("/svc/password", "first", ParameterType::SecureString);
        let latest = seeder.parameter("/svc/password", "second", ParameterType::SecureString);

        let connection = connect(&endpoint)?;
        let parameters = store(connection.as_ref(), PARAMETER_STORE_ID);

        let current = parameters.get_value("/svc/password", None)?;
        assert_eq!(text(&current), "second");
        assert_eq!(current.version, Some(latest.to_string()));

        let first = parameters.get_value("/svc/password", Some("1"))?;
        assert_eq!(text(&first), "first");
        assert_eq!(first.version.as_deref(), Some("1"));

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn parameter_store_missing_parameter_and_version_are_object_not_found() -> Result<(), DbError> {
    containers::with_localstack_secrets_endpoint(|endpoint| {
        Seeder::new(&endpoint).parameter("/exists", "x", ParameterType::String);

        let connection = connect(&endpoint)?;
        let parameters = store(connection.as_ref(), PARAMETER_STORE_ID);

        assert!(matches!(
            parameters.get_value("/missing", None),
            Err(DbError::ObjectNotFound(_))
        ));
        assert!(matches!(
            parameters.get_value("/exists", Some("9")),
            Err(DbError::ObjectNotFound(_))
        ));
        assert!(matches!(
            parameters.describe("/missing"),
            Err(DbError::ObjectNotFound(_))
        ));

        Ok(())
    })
}
