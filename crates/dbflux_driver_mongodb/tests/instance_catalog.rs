#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{ColumnKind, ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest};
use dbflux_driver_mongodb::MongoDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = MongoDriver::new();
    let profile = ConnectionProfile::new(
        "live-mongodb-catalog",
        DbConfig::MongoDB {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 27017,
            user: None,
            database: Some("admin".to_string()),
            auth_database: None,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            schema_settings: None,
        },
    );

    containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
        let conn = driver.connect(&profile)?;
        conn.ping()?;
        Ok(conn)
    })
}

fn metric_req(metric_id: &str) -> QueryRequest {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    QueryRequest {
        execution_context: Some(dbflux_core::ExecutionContext {
            source: Some(dbflux_core::ExecutionSourceContext::InstanceMetricQuery {
                metric_id: metric_id.to_string(),
                start_ms: now_ms - 60_000,
                end_ms: now_ms,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn inspector_req(metric_id: &str) -> QueryRequest {
    QueryRequest {
        execution_context: Some(dbflux_core::ExecutionContext {
            source: Some(dbflux_core::ExecutionSourceContext::InstanceInspectorQuery {
                metric_id: metric_id.to_string(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_opcounters_query_column_shape() {
    containers::with_mongodb_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("mongo.opcounters.query"))?;

        assert!(!result.columns.is_empty(), "must have columns");
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[1].kind, ColumnKind::Float);
        assert!(!result.rows.is_empty());

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_connections_current_metric() {
    containers::with_mongodb_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("mongo.connections.current"))?;

        assert!(!result.rows.is_empty());
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[1].kind, ColumnKind::Float);

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_current_op_inspector_has_columns() {
    containers::with_mongodb_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&inspector_req("mongo.current_op"))?;

        assert!(!result.columns.is_empty());
        let col_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"op"));
        assert!(col_names.contains(&"ns"));

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn bson_path_extraction_from_real_server_status() {
    containers::with_mongodb_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;

        // All SERVER_STATUS_PATHS should produce a valid (non-error) metric
        let paths = dbflux_driver_mongodb::instance_catalog::SERVER_STATUS_PATHS;
        for (_, metric_id, _, _, _) in paths {
            let result = conn.execute(&metric_req(metric_id));
            assert!(
                result.is_ok(),
                "metric {:?} must succeed on live server",
                metric_id
            );
        }

        Ok(())
    })
    .unwrap();
}
