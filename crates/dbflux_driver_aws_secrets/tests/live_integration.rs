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
    SecretPayload, SecretStoreCapabilities, SecretStoreSource, SecretValue,
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
        ));
        assert_eq!(descriptors[1].id, PARAMETER_STORE_ID);
        assert_eq!(descriptors[1].label, "Parameter Store");
        assert!(descriptors[1].supports(
            SecretStoreCapabilities::VERSIONS
                | SecretStoreCapabilities::HIERARCHICAL
                | SecretStoreCapabilities::TYPED_VALUES
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
