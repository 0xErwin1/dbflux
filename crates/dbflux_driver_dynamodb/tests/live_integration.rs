use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::config::Credentials;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use dbflux_core::{
    CollectionCountRequest, CollectionRef, ConnectionProfile, DbConfig, DbDriver, DbError,
};
use dbflux_driver_dynamodb::DynamoDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn dynamo_client(endpoint: &str) -> Result<Client, DbError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    let sdk_config = runtime.block_on(
        aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .load(),
    );

    let conf = aws_sdk_dynamodb::config::Builder::from(&sdk_config)
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "dbflux-test"))
        .build();

    Ok(Client::from_conf(conf))
}

fn create_table(endpoint: &str, table_name: &str) -> Result<(), DbError> {
    let client = dynamo_client(endpoint)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    containers::retry_db_operation(Duration::from_secs(20), || {
        let create_result = runtime.block_on(
            client
                .create_table()
                .table_name(table_name)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("pk")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .map_err(|error| {
                            DbError::query_failed(format!(
                                "Failed to build attribute definition: {error}"
                            ))
                        })?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(|error| {
                            DbError::query_failed(format!("Failed to build key schema: {error}"))
                        })?,
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send(),
        );

        match create_result {
            Ok(_) => Ok(()),
            Err(error) => {
                if error.to_string().contains("ResourceInUseException") {
                    Ok(())
                } else {
                    Err(DbError::query_failed(format!(
                        "Create table failed: {error}"
                    )))
                }
            }
        }
    })?;

    containers::retry_db_operation(Duration::from_secs(20), || {
        let output = runtime
            .block_on(client.describe_table().table_name(table_name).send())
            .map_err(|error| DbError::query_failed(format!("Describe table failed: {error}")))?;

        let status = output
            .table()
            .and_then(|table| table.table_status())
            .map(|status| status.as_str().to_string())
            .unwrap_or_default();

        if status == "ACTIVE" {
            Ok(())
        } else {
            Err(DbError::query_failed(format!(
                "Table '{table_name}' is not active yet (status={status})"
            )))
        }
    })
}

fn seed_items(endpoint: &str, table_name: &str, count: usize) -> Result<(), DbError> {
    let client = dynamo_client(endpoint)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    for index in 0..count {
        runtime
            .block_on(
                client
                    .put_item()
                    .table_name(table_name)
                    .item("pk", AttributeValue::S(format!("item#{index}")))
                    .item("value", AttributeValue::N(index.to_string()))
                    .send(),
            )
            .map_err(|error| DbError::query_failed(format!("Seed item failed: {error}")))?;
    }

    Ok(())
}

fn connect_dynamodb(endpoint: &str) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = DynamoDriver::new();
    let profile = ConnectionProfile::new_with_driver(
        "live-dynamodb-local",
        dbflux_core::DbKind::DynamoDB,
        "builtin:dynamodb",
        DbConfig::DynamoDB {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: Some(endpoint.to_string()),
            table: None,
        },
    );

    containers::retry_db_operation(Duration::from_secs(30), || {
        let connection = driver.connect(&profile)?;
        connection.ping()?;
        Ok(connection)
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn dynamodb_local_container_and_fixtures_work() -> Result<(), DbError> {
    containers::with_dynamodb_endpoint(|endpoint| {
        let table_name = "dbflux_phase8_fixture";

        create_table(&endpoint, table_name)?;
        seed_items(&endpoint, table_name, 5)?;

        let connection = connect_dynamodb(&endpoint)?;
        let count = connection.count_collection(&CollectionCountRequest::new(
            CollectionRef::new("dynamodb", table_name),
        ))?;

        assert_eq!(count, 5);
        Ok(())
    })
}

#[test]
fn dynamodb_local_endpoint_failures_are_actionable() {
    let driver = DynamoDriver::new();
    let profile = ConnectionProfile::new_with_driver(
        "dynamodb-invalid-endpoint",
        dbflux_core::DbKind::DynamoDB,
        "builtin:dynamodb",
        DbConfig::DynamoDB {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: Some("http://127.0.0.1:9".to_string()),
            table: None,
        },
    );

    let error = driver
        .test_connection(&profile)
        .expect_err("test_connection should fail against unavailable endpoint");

    let text = error.to_string().to_ascii_lowercase();
    assert!(
        text.contains("endpoint") || text.contains("connection") || text.contains("timed out"),
        "unexpected failure text: {text}"
    );
}
