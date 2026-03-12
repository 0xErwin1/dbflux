use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::config::Credentials;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ConnectionProfile, DbConfig,
    DbDriver, DbError, Pagination, QueryRequest,
};
use dbflux_driver_dynamodb::DynamoDriver;
use dbflux_test_support::containers;
use serde_json::json;
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

fn seed_filter_fixture_items(endpoint: &str, table_name: &str) -> Result<(), DbError> {
    let client = dynamo_client(endpoint)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    let fixture = [
        ("user#1", "active", 12),
        ("user#2", "inactive", 4),
        ("user#3", "pending", 11),
        ("user#4", "active", 7),
    ];

    for (pk, status, score) in fixture {
        runtime
            .block_on(
                client
                    .put_item()
                    .table_name(table_name)
                    .item("pk", AttributeValue::S(pk.to_string()))
                    .item("status", AttributeValue::S(status.to_string()))
                    .item("score", AttributeValue::N(score.to_string()))
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

#[test]
#[ignore = "requires Docker daemon"]
fn dynamodb_logical_filter_browse_and_count_are_consistent() -> Result<(), DbError> {
    containers::with_dynamodb_endpoint(|endpoint| {
        let table_name = "dbflux_phase4_filter_fixture";

        create_table(&endpoint, table_name)?;
        seed_filter_fixture_items(&endpoint, table_name)?;

        let connection = connect_dynamodb(&endpoint)?;
        let collection = CollectionRef::new("dynamodb", table_name);
        let filter = json!({
            "$and": [
                {"score": {"$gte": 10}},
                {"$or": [{"status": "active"}, {"status": "pending"}]}
            ]
        });

        let browse_request = CollectionBrowseRequest::new(collection.clone())
            .with_pagination(Pagination::Offset {
                limit: 50,
                offset: 0,
            })
            .with_filter(filter.clone());

        let browsed = connection.browse_collection(&browse_request)?;
        assert_eq!(browsed.row_count(), 2);

        let count_request = CollectionCountRequest::new(collection).with_filter(filter);
        let count = connection.count_collection(&count_request)?;
        assert_eq!(count, 2);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn dynamodb_update_many_and_delete_many_apply_to_all_matches() -> Result<(), DbError> {
    containers::with_dynamodb_endpoint(|endpoint| {
        let table_name = "dbflux_phase4_many_mutation_fixture";

        create_table(&endpoint, table_name)?;
        seed_filter_fixture_items(&endpoint, table_name)?;

        let connection = connect_dynamodb(&endpoint)?;

        let update_many = QueryRequest::new(
            json!({
                "op": "update",
                "table": table_name,
                "key": { "status": "active" },
                "update": { "$set": { "status": "archived" } },
                "many": true
            })
            .to_string(),
        );

        let updated = connection.execute(&update_many)?;
        assert_eq!(updated.affected_rows, Some(2));

        let archived_count = connection.count_collection(
            &CollectionCountRequest::new(CollectionRef::new("dynamodb", table_name))
                .with_filter(json!({"status": "archived"})),
        )?;
        assert_eq!(archived_count, 2);

        let delete_many = QueryRequest::new(
            json!({
                "op": "delete",
                "table": table_name,
                "key": { "status": "archived" },
                "many": true
            })
            .to_string(),
        );

        let deleted = connection.execute(&delete_many)?;
        assert_eq!(deleted.affected_rows, Some(2));

        let remaining_archived_count = connection.count_collection(
            &CollectionCountRequest::new(CollectionRef::new("dynamodb", table_name))
                .with_filter(json!({"status": "archived"})),
        )?;
        assert_eq!(remaining_archived_count, 0);

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn dynamodb_upsert_updates_existing_and_inserts_missing_items() -> Result<(), DbError> {
    containers::with_dynamodb_endpoint(|endpoint| {
        let table_name = "dbflux_phase4_upsert_fixture";

        create_table(&endpoint, table_name)?;

        let client = dynamo_client(&endpoint)?;
        let runtime = tokio::runtime::Runtime::new().map_err(|error| {
            DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
        })?;

        runtime
            .block_on(
                client
                    .put_item()
                    .table_name(table_name)
                    .item("pk", AttributeValue::S("user#1".to_string()))
                    .item("status", AttributeValue::S("active".to_string()))
                    .send(),
            )
            .map_err(|error| DbError::query_failed(format!("Seed item failed: {error}")))?;

        let connection = connect_dynamodb(&endpoint)?;

        let upsert_existing = QueryRequest::new(
            json!({
                "op": "update",
                "table": table_name,
                "key": { "pk": "user#1" },
                "update": { "$set": { "status": "updated" } },
                "upsert": true
            })
            .to_string(),
        );

        let existing_result = connection.execute(&upsert_existing)?;
        assert_eq!(existing_result.affected_rows, Some(1));

        let upsert_missing = QueryRequest::new(
            json!({
                "op": "update",
                "table": table_name,
                "key": { "pk": "user#2" },
                "update": { "$set": { "status": "created" } },
                "upsert": true
            })
            .to_string(),
        );

        let missing_result = connection.execute(&upsert_missing)?;
        assert_eq!(missing_result.affected_rows, Some(1));

        let created_count = connection.count_collection(
            &CollectionCountRequest::new(CollectionRef::new("dynamodb", table_name))
                .with_filter(json!({"status": "created"})),
        )?;
        assert_eq!(created_count, 1);

        Ok(())
    })
}
