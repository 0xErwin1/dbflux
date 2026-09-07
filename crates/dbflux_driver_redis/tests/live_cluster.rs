#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use std::collections::HashSet;
use std::time::Duration;

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, KeyGetRequest, KeyScanRequest, KeySetRequest,
    QueryRequest, ValueRepr,
};
use dbflux_driver_redis::RedisDriver;
use dbflux_test_support::containers;

/// The `grokzen/redis-cluster` image needs longer than a standalone Redis
/// node to finish carving up slots across its six nodes after they come up,
/// so connect attempts are retried for longer than `live_integration.rs`'s
/// 30-second budget before giving up.
const CLUSTER_CONNECT_TIMEOUT: Duration = Duration::from_secs(90);

fn connect_cluster(seed_uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = RedisDriver::new();
    let profile = ConnectionProfile::new(
        "live-redis-cluster",
        DbConfig::Redis {
            use_uri: true,
            uri: Some(seed_uri),
            host: String::new(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: Some("cluster".to_string()),
            sentinel_master_name: None,
            additional_nodes: None,
        },
    );

    containers::retry_db_operation(CLUSTER_CONNECT_TIMEOUT, || -> Result<_, DbError> {
        let connection = driver.connect(&profile)?;
        connection.ping()?;
        Ok(connection)
    })
}

fn connect_cluster_with_database(
    seed_uri: String,
    database: u32,
) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = RedisDriver::new();
    let profile = ConnectionProfile::new(
        "live-redis-cluster-nonzero-db",
        DbConfig::Redis {
            use_uri: true,
            uri: Some(seed_uri),
            host: String::new(),
            port: 6379,
            user: None,
            database: Some(database),
            tls: false,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: Some("cluster".to_string()),
            sentinel_master_name: None,
            additional_nodes: None,
        },
    );

    driver.connect(&profile)
}

fn connect_auto_detect(seed_uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = RedisDriver::new();
    let profile = ConnectionProfile::new(
        "live-redis-cluster-autodetect",
        DbConfig::Redis {
            use_uri: true,
            uri: Some(seed_uri),
            host: String::new(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssl_mode: None,
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
            topology: None,
            sentinel_master_name: None,
            additional_nodes: None,
        },
    );

    containers::retry_db_operation(CLUSTER_CONNECT_TIMEOUT, || -> Result<_, DbError> {
        let connection = driver.connect(&profile)?;
        connection.ping()?;
        Ok(connection)
    })
}

// ---------------------------------------------------------------------------
// Connect + keyspace-wide scan across masters
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cluster_connect_and_scan_across_masters() -> Result<(), DbError> {
    containers::with_redis_cluster_urls(|urls| {
        let connection = connect_cluster(urls[0].clone())?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        // Spread keys across hash slots (no hash tags) so they land on
        // different masters; the cluster scan must fan out across all of
        // them and still see every key.
        let seeded_keys: Vec<String> = (0..60).map(|i| format!("cluster:key:{i}")).collect();
        for key in &seeded_keys {
            kv.set_key(&KeySetRequest::new(key, b"v".to_vec()).with_repr(ValueRepr::Text))?;
        }

        let mut found: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut request = KeyScanRequest::new(20).with_filter("cluster:key:*");
            if let Some(c) = cursor.take() {
                request = request.with_cursor(c);
            }

            let page = kv.scan_keys(&request)?;
            found.extend(page.entries.into_iter().map(|entry| entry.key));

            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        let expected: HashSet<String> = seeded_keys.iter().cloned().collect();
        assert_eq!(found, expected);

        for key in &seeded_keys {
            kv.delete_key(&dbflux_core::KeyDeleteRequest::new(key))?;
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Nonzero database rejected on Cluster
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cluster_rejects_nonzero_database() -> Result<(), DbError> {
    containers::with_redis_cluster_urls(|urls| {
        // Make sure the cluster itself is reachable before asserting on the
        // nonzero-database rejection, so a cold cluster doesn't masquerade
        // as a rejection.
        let warmup = connect_cluster(urls[0].clone())?;
        warmup.ping()?;

        let result = connect_cluster_with_database(urls[0].clone(), 3);
        assert!(matches!(result, Err(DbError::NotSupported(_))));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Aggregated schema key count
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cluster_schema_reports_aggregated_key_count() -> Result<(), DbError> {
    containers::with_redis_cluster_urls(|urls| {
        let connection = connect_cluster(urls[0].clone())?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        let seeded_keys: Vec<String> = (0..30).map(|i| format!("cluster:schema:{i}")).collect();
        for key in &seeded_keys {
            kv.set_key(&KeySetRequest::new(key, b"v".to_vec()).with_repr(ValueRepr::Text))?;
        }

        let schema = connection.schema()?;
        assert!(schema.is_key_value());

        let keyspaces = schema.keyspaces();
        assert_eq!(keyspaces.len(), 1);
        let db0 = &keyspaces[0];
        assert_eq!(db0.db_index, 0);
        let key_count = db0
            .key_count
            .expect("cluster schema should report a key count");
        assert!(
            key_count >= seeded_keys.len() as u64,
            "expected at least {} keys, got {key_count}",
            seeded_keys.len()
        );

        for key in &seeded_keys {
            kv.delete_key(&dbflux_core::KeyDeleteRequest::new(key))?;
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Auto-detection connects as Cluster without an explicit topology
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cluster_auto_detection_connects_as_cluster() -> Result<(), DbError> {
    containers::with_redis_cluster_urls(|urls| {
        let connection = connect_auto_detect(urls[0].clone())?;

        // A Cluster connection only ever exposes database 0; if detection
        // had instead treated this node as standalone, `schema()` would
        // report per-database keyspaces derived from `INFO keyspace`
        // instead of the single aggregated `db0` entry Cluster handling
        // produces.
        let schema = connection.schema()?;
        let keyspaces = schema.keyspaces();
        assert_eq!(keyspaces.len(), 1);
        assert_eq!(keyspaces[0].db_index, 0);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// TYPE/TTL enrichment on scanned keys, routed across masters
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cluster_scan_type_and_ttl_enrichment() -> Result<(), DbError> {
    containers::with_redis_cluster_urls(|urls| {
        let connection = connect_cluster(urls[0].clone())?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        let seeded_keys: Vec<String> = (0..20).map(|i| format!("cluster:ttl:{i}")).collect();
        for key in &seeded_keys {
            kv.set_key(
                &KeySetRequest::new(key, b"v".to_vec())
                    .with_repr(ValueRepr::Text)
                    .with_ttl(300),
            )?;
        }

        let page = kv.scan_keys(&KeyScanRequest::new(100).with_filter("cluster:ttl:*"))?;
        assert_eq!(page.entries.len(), seeded_keys.len());

        for entry in &page.entries {
            assert_eq!(entry.key_type, Some(dbflux_core::KeyType::String));

            // `scan_keys` itself never populates TTL (see the driver's own
            // note on `KeyEntry::ttl_seconds`); TTL enrichment happens per
            // key via `get_key`, which must route correctly to whichever
            // master owns that key's slot.
            let get_result = kv.get_key(&KeyGetRequest::new(&entry.key))?;
            let ttl = get_result
                .entry
                .ttl_seconds
                .expect("seeded key should report a TTL");
            assert!(ttl > 0 && ttl <= 300);
        }

        for key in &seeded_keys {
            kv.delete_key(&dbflux_core::KeyDeleteRequest::new(key))?;
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// PING through the standalone-style QueryRequest path
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cluster_execute_ping() -> Result<(), DbError> {
    containers::with_redis_cluster_urls(|urls| {
        let connection = connect_cluster(urls[0].clone())?;
        let result = connection.execute(&QueryRequest::new("PING"))?;
        assert!(!result.rows.is_empty() || result.text_body.is_some());
        Ok(())
    })
}
