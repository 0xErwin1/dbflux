//! Live integration coverage for the CloudWatch driver against a LocalStack
//! community container.
//!
//! # Coverage boundary
//!
//! LocalStack community implements the CloudWatch Logs data-plane APIs used
//! for log-group/log-stream discovery and event reading, so those paths are
//! exercised end-to-end here. `DashboardImporter::import` is pure JSON
//! parsing with no AWS call, so it is exercised unconditionally.
//!
//! Two seams call AWS APIs whose community-tier support is not guaranteed:
//! `DashboardSource` (`PutDashboard`/`ListDashboards`/`GetDashboard`) and
//! CloudWatch Logs Insights (`StartQuery`/`GetQueryResults`). Both tests
//! attempt the real call against LocalStack and, if the response indicates
//! the operation is unimplemented in the community tier, the test logs the
//! boundary and returns instead of failing. A real AWS account (or
//! LocalStack Pro) is required to verify those two seams end-to-end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use aws_config::{BehaviorVersion, Region};
use aws_sdk_cloudwatch::config::Builder as MetricsConfigBuilder;
use aws_sdk_cloudwatchlogs::config::{Builder as LogsConfigBuilder, Credentials};
use aws_sdk_cloudwatchlogs::types::InputLogEvent;
use dbflux_core::{
    CollectionBrowseRequest, CollectionChildrenRequest, CollectionRef, ConnectionProfile,
    DashboardImporter, DbConfig, DbDriver, DbError, EventQuery, EventStreamTarget,
    ExecutionContext, ExecutionSourceContext, Pagination, QueryRequest,
};
use dbflux_driver_cloudwatch::{CloudWatchDashboardImporter, CloudWatchDriver};
use dbflux_test_support::containers;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as i64
}

fn logs_client(endpoint: &str) -> Result<aws_sdk_cloudwatchlogs::Client, DbError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    let sdk_config = runtime.block_on(
        aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .load(),
    );

    let conf = LogsConfigBuilder::from(&sdk_config)
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(
            "test",
            "test",
            None,
            None,
            "dbflux-cloudwatch-test",
        ))
        .build();

    Ok(aws_sdk_cloudwatchlogs::Client::from_conf(conf))
}

fn metrics_client(endpoint: &str) -> Result<aws_sdk_cloudwatch::Client, DbError> {
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    let sdk_config = runtime.block_on(
        aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .load(),
    );

    let conf = MetricsConfigBuilder::from(&sdk_config)
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_cloudwatch::config::Credentials::new(
            "test",
            "test",
            None,
            None,
            "dbflux-cloudwatch-test",
        ))
        .build();

    Ok(aws_sdk_cloudwatch::Client::from_conf(conf))
}

/// Creates a log group and log stream, then seeds `count` sequential events.
fn seed_log_group(
    endpoint: &str,
    log_group: &str,
    log_stream: &str,
    count: usize,
) -> Result<(), DbError> {
    let client = logs_client(endpoint)?;
    let runtime = tokio::runtime::Runtime::new().map_err(|error| {
        DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
    })?;

    containers::retry_db_operation(Duration::from_secs(30), || {
        match runtime.block_on(client.create_log_group().log_group_name(log_group).send()) {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("ResourceAlreadyExistsException") => Ok(()),
            Err(error) => Err(DbError::query_failed(format!(
                "CreateLogGroup failed: {error}"
            ))),
        }
    })?;

    containers::retry_db_operation(Duration::from_secs(30), || {
        match runtime.block_on(
            client
                .create_log_stream()
                .log_group_name(log_group)
                .log_stream_name(log_stream)
                .send(),
        ) {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("ResourceAlreadyExistsException") => Ok(()),
            Err(error) => Err(DbError::query_failed(format!(
                "CreateLogStream failed: {error}"
            ))),
        }
    })?;

    let base_ts = now_ms() - (count as i64) * 1000;
    let mut events = Vec::with_capacity(count);
    for index in 0..count {
        events.push(
            InputLogEvent::builder()
                .timestamp(base_ts + (index as i64) * 1000)
                .message(format!("dbflux live integration event #{index}"))
                .build()
                .map_err(|error| {
                    DbError::query_failed(format!("Failed to build log event: {error}"))
                })?,
        );
    }

    runtime
        .block_on(
            client
                .put_log_events()
                .log_group_name(log_group)
                .log_stream_name(log_stream)
                .set_log_events(Some(events))
                .send(),
        )
        .map_err(|error| DbError::query_failed(format!("PutLogEvents failed: {error}")))?;

    Ok(())
}

fn connect_cloudwatch(endpoint: &str) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = CloudWatchDriver::new();
    let profile = ConnectionProfile::new_with_driver(
        "live-cloudwatch-localstack",
        dbflux_core::DbKind::CloudWatchLogs,
        "builtin:cloudwatch",
        DbConfig::CloudWatchLogs {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: Some(endpoint.to_string()),
        },
    );

    containers::retry_db_operation(Duration::from_secs(30), || {
        let connection = driver.connect(&profile)?;
        connection.ping()?;
        Ok(connection)
    })
}

/// Returns `true` when an error message looks like LocalStack rejecting an
/// operation its community tier does not implement, rather than a genuine
/// test failure.
fn looks_like_unimplemented_in_community_tier(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("not yet implemented")
        || text.contains("not implemented")
        || text.contains("notimplementedexception")
        || text.contains("internalfailure")
        || text.contains("internal error")
        || text.contains("internalerror")
        || text.contains("unknownoperationexception")
        || text.contains("unsupportedoperation")
        || text.contains("not supported")
}

#[test]
#[ignore = "requires Docker daemon"]
fn cloudwatch_localstack_log_group_discovery_and_event_browsing() -> Result<(), DbError> {
    containers::with_localstack_cloudwatch_endpoint(|endpoint| {
        let log_group = "/dbflux/live-integration";
        let log_stream = "stream-a";

        seed_log_group(&endpoint, log_group, log_stream, 5)?;

        let connection = connect_cloudwatch(&endpoint)?;

        let schema = connection.schema()?;
        let document = schema
            .as_document()
            .expect("CloudWatch schema() must return a document snapshot");
        assert!(
            document
                .collections
                .iter()
                .any(|collection| collection.name == log_group),
            "expected discovered log groups to include {log_group}, got {:?}",
            document.collections
        );

        let children = connection.collection_children(&CollectionChildrenRequest {
            collection: CollectionRef::new("logs", log_group),
            limit: 50,
            page_token: None,
        })?;
        assert!(
            children.items.iter().any(|item| item.id == log_stream),
            "expected {log_stream} among collection_children, got {:?}",
            children.items
        );

        let browse_result = connection.browse_collection(
            &CollectionBrowseRequest::new(CollectionRef::new("logs", log_group)).with_pagination(
                Pagination::Offset {
                    limit: 50,
                    offset: 0,
                },
            ),
        )?;
        assert_eq!(
            browse_result.rows.len(),
            5,
            "expected 5 seeded events back from browse_collection"
        );

        let event_page = connection.browse_event_stream(
            &EventStreamTarget {
                collection: CollectionRef::new("logs", log_group),
                child_id: Some(log_stream.to_string()),
            },
            &EventQuery {
                limit: Some(50),
                ..EventQuery::default()
            },
        )?;
        assert_eq!(
            event_page.events.len(),
            5,
            "expected 5 seeded events back from browse_event_stream"
        );

        Ok(())
    })
}

#[test]
fn cloudwatch_dashboard_importer_parses_metric_widgets_without_docker() {
    // DashboardImporter::import is pure JSON parsing with no AWS call, so this
    // runs unconditionally rather than behind `--ignored`.
    let json = r#"{
        "widgets": [
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 6,
                "height": 4,
                "properties": {
                    "title": "CPU Utilization",
                    "metrics": [["AWS/EC2", "CPUUtilization", "InstanceId", "i-1234"]],
                    "period": 300,
                    "stat": "Average",
                    "region": "us-east-1"
                }
            }
        ]
    }"#;

    let importer = CloudWatchDashboardImporter;
    let specs = importer
        .import(json)
        .expect("importer should parse a valid metric-widget dashboard");

    assert_eq!(specs.len(), 1, "expected exactly one imported widget");
}

#[test]
#[ignore = "requires Docker daemon"]
fn cloudwatch_localstack_dashboard_source_roundtrip_or_documented_gap() -> Result<(), DbError> {
    containers::with_localstack_cloudwatch_endpoint(|endpoint| {
        let dashboard_name = "dbflux-live-dashboard";
        let dashboard_body = r#"{"widgets":[]}"#;

        let metrics = metrics_client(&endpoint)?;
        let runtime = tokio::runtime::Runtime::new().map_err(|error| {
            DbError::connection_failed(format!("Tokio runtime setup failed: {error}"))
        })?;

        let put_result = runtime.block_on(
            metrics
                .put_dashboard()
                .dashboard_name(dashboard_name)
                .dashboard_body(dashboard_body)
                .send(),
        );

        if let Err(error) = &put_result
            && looks_like_unimplemented_in_community_tier(&error.to_string())
        {
            eprintln!(
                "SKIP: LocalStack community tier does not implement PutDashboard; \
                 DashboardSource roundtrip requires a real AWS account or LocalStack Pro."
            );
            return Ok(());
        }
        put_result
            .map_err(|error| DbError::query_failed(format!("PutDashboard failed: {error}")))?;

        let connection = connect_cloudwatch(&endpoint)?;
        let dashboard_source = connection
            .dashboard_source()
            .expect("CloudWatch driver always exposes a DashboardSource");

        let listed = dashboard_source.list_dashboards()?;
        assert!(
            listed.iter().any(|entry| entry.name == dashboard_name),
            "expected {dashboard_name} in list_dashboards, got {:?}",
            listed
        );

        let fetched = dashboard_source.fetch_dashboard(dashboard_name)?;
        assert_eq!(fetched.name, dashboard_name);
        assert!(!fetched.body_json.is_empty());

        Ok(())
    })
}

#[test]
#[ignore = "requires Docker daemon"]
fn cloudwatch_localstack_logs_insights_query_or_documented_gap() -> Result<(), DbError> {
    containers::with_localstack_cloudwatch_endpoint(|endpoint| {
        let log_group = "/dbflux/live-insights";
        let log_stream = "stream-a";

        seed_log_group(&endpoint, log_group, log_stream, 3)?;

        let connection = connect_cloudwatch(&endpoint)?;

        let end_ms = now_ms();
        let start_ms = end_ms - 60 * 60 * 1000;

        let request =
            QueryRequest::new("fields @timestamp, @message | sort @timestamp desc | limit 20")
                .with_execution_context(Some(ExecutionContext {
                    source: Some(ExecutionSourceContext::CollectionWindow {
                        targets: vec![log_group.to_string()],
                        start_ms,
                        end_ms,
                        query_mode: Some("cwli".to_string()),
                    }),
                    ..ExecutionContext::default()
                }));

        match connection.execute(&request) {
            Ok(result) => {
                assert!(
                    !result.columns.is_empty(),
                    "Logs Insights query returned no columns"
                );
                Ok(())
            }
            Err(error) if looks_like_unimplemented_in_community_tier(&error.to_string()) => {
                eprintln!(
                    "SKIP: LocalStack community tier does not implement CloudWatch Logs Insights \
                     (StartQuery/GetQueryResults); this seam requires a real AWS account or \
                     LocalStack Pro."
                );
                Ok(())
            }
            Err(error) => Err(error),
        }
    })
}

#[test]
fn cloudwatch_invalid_endpoint_failures_are_actionable() {
    let driver = CloudWatchDriver::new();
    let profile = ConnectionProfile::new_with_driver(
        "cloudwatch-invalid-endpoint",
        dbflux_core::DbKind::CloudWatchLogs,
        "builtin:cloudwatch",
        DbConfig::CloudWatchLogs {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: Some("http://127.0.0.1:9".to_string()),
        },
    );

    let error = driver
        .test_connection(&profile)
        .expect_err("test_connection should fail against an unavailable endpoint");

    let text = error.to_string().to_ascii_lowercase();
    assert!(
        text.contains("endpoint") || text.contains("connection") || text.contains("timed out"),
        "unexpected failure text: {text}"
    );
}
