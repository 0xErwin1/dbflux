//! Live Docker-backed test proving `RdbAnalyzer` handles a dump.rdb file
//! actually produced by a real Redis server, closing the gap left by the
//! hand-built fixtures in `rdb.rs`'s unit tests.
//!
//! The dump is retrieved via `docker exec cat` rather than a host bind
//! mount: the official `redis` image declares `/data` as an anonymous
//! `VOLUME`, and under a rootless container runtime that volume's ownership
//! is mapped through a subordinate UID/GID range the host user may not have
//! configured, which makes bind-mounting or reading it directly from the
//! host fail. Executing `cat` inside the container's own user namespace
//! sidesteps that entirely.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, DumpAnalyzer, HashSetRequest, KeyExpireRequest,
    KeySetRequest, ListEnd, ListPushRequest, QueryRequest, SetAddRequest, StreamAddRequest,
    StreamEntryId, ValueRepr, ZSetAddRequest,
};
use dbflux_driver_redis::{RdbAnalyzer, RedisDriver};
use dbflux_test_support::containers;
use std::io::Write;
use std::time::Duration;
use testcontainers::core::ExecCommand;
use testcontainers::{Container, GenericImage};

fn connect_redis(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = RedisDriver::new();
    let profile = ConnectionProfile::new(
        "live-redis-rdb",
        DbConfig::Redis {
            use_uri: true,
            uri: Some(uri),
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

    let connection =
        containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
            let connection = driver.connect(&profile)?;
            connection.ping()?;
            Ok(connection)
        })?;

    Ok(connection)
}

/// Copies `/data/dump.rdb` out of the container's own filesystem via
/// `docker exec cat` and writes it to a fresh temp file on the host.
fn fetch_dump_rdb(container: &Container<GenericImage>) -> tempfile::NamedTempFile {
    let mut exec_result = container
        .exec(ExecCommand::new(["cat", "/data/dump.rdb"]))
        .expect("exec cat dump.rdb should succeed");
    let bytes = exec_result
        .stdout_to_vec()
        .expect("reading dump.rdb bytes from exec stdout should succeed");
    assert!(!bytes.is_empty(), "dump.rdb must not be empty after SAVE");

    let mut file = tempfile::NamedTempFile::new().expect("creating host temp file should succeed");
    file.write_all(&bytes)
        .expect("writing dump.rdb bytes to temp file should succeed");
    file.flush()
        .expect("flushing temp dump file should succeed");

    file
}

#[test]
#[ignore = "requires Docker daemon"]
fn rdb_analyzer_matches_real_redis_produced_dump() -> Result<(), DbError> {
    containers::with_redis_container(|uri, container| -> Result<(), DbError> {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        // One key of every type, including a prefix that repeats so the
        // prefix-rollup aggregation has something to group.
        kv.set_key(
            &KeySetRequest::new("fixture:a", b"plain-string".to_vec()).with_repr(ValueRepr::Text),
        )?;

        kv.set_key(
            &KeySetRequest::new("fixture:b", b"expiring-string".to_vec())
                .with_repr(ValueRepr::Text),
        )?;
        kv.expire_key(&KeyExpireRequest::new("fixture:b", 3600))?;

        kv.hash_set(&HashSetRequest {
            key: "fixture:hash".to_string(),
            fields: vec![
                ("field1".to_string(), "value1".to_string()),
                ("field2".to_string(), "value2".to_string()),
            ],
            keyspace: None,
        })?;

        kv.list_push(&ListPushRequest {
            key: "fixture:list".to_string(),
            values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            end: ListEnd::Tail,
            keyspace: None,
        })?;

        kv.set_add(&SetAddRequest {
            key: "fixture:set".to_string(),
            members: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            keyspace: None,
        })?;

        kv.zset_add(&ZSetAddRequest {
            key: "fixture:zset".to_string(),
            members: vec![
                ("alice".to_string(), 100.0),
                ("bob".to_string(), 200.0),
                ("charlie".to_string(), 150.0),
            ],
            keyspace: None,
        })?;

        kv.stream_add(&StreamAddRequest {
            key: "fixture:stream".to_string(),
            id: StreamEntryId::Auto,
            fields: vec![("field1".to_string(), "value1".to_string())],
            maxlen: None,
            keyspace: None,
        })?;

        // Create a consumer group and read through it so the stream carries
        // a pending-entries list (PEL) in the dump, exercising the v2/v3
        // stream-with-consumer-groups skipping path against real output.
        connection.execute(&QueryRequest::new(
            "XGROUP CREATE fixture:stream fixture-group 0",
        ))?;
        connection.execute(&QueryRequest::new(
            "XREADGROUP GROUP fixture-group fixture-consumer COUNT 1 STREAMS fixture:stream >",
        ))?;

        // SAVE (not BGSAVE) so dump.rdb is guaranteed complete and current
        // by the time this call returns.
        connection.execute(&QueryRequest::new("SAVE"))?;

        let dump_file = fetch_dump_rdb(container);

        let report = RdbAnalyzer
            .analyze(dump_file.path(), &|_bytes_read, _total| {}, &|| false)
            .expect("analyzing a real Redis-produced dump.rdb must succeed");

        let seeded_keys = [
            "fixture:a",
            "fixture:b",
            "fixture:hash",
            "fixture:list",
            "fixture:set",
            "fixture:zset",
            "fixture:stream",
        ];

        assert_eq!(report.total_keys, seeded_keys.len() as u64);

        let largest_key_names: Vec<&str> = report
            .largest_keys
            .iter()
            .map(|entry| entry.key.as_str())
            .collect();
        for key in seeded_keys {
            assert!(
                largest_key_names.contains(&key),
                "expected {key} in largest_keys, got {largest_key_names:?}"
            );
        }

        let types_by_key: std::collections::HashMap<&str, &str> = report
            .largest_keys
            .iter()
            .map(|entry| (entry.key.as_str(), entry.type_name.as_str()))
            .collect();
        assert_eq!(types_by_key.get("fixture:a"), Some(&"string"));
        assert_eq!(types_by_key.get("fixture:b"), Some(&"string"));
        assert_eq!(types_by_key.get("fixture:hash"), Some(&"hash"));
        assert_eq!(types_by_key.get("fixture:list"), Some(&"list"));
        assert_eq!(types_by_key.get("fixture:set"), Some(&"set"));
        assert_eq!(types_by_key.get("fixture:zset"), Some(&"zset"));
        assert_eq!(types_by_key.get("fixture:stream"), Some(&"stream"));

        let type_names_in_rollup: Vec<&str> = report
            .keys_by_type
            .iter()
            .map(|(type_name, _, _)| type_name.as_str())
            .collect();
        for expected_type in ["string", "hash", "list", "set", "zset", "stream"] {
            assert!(
                type_names_in_rollup.contains(&expected_type),
                "expected {expected_type} in keys_by_type, got {type_names_in_rollup:?}"
            );
        }

        let expiring_entry = report
            .largest_keys
            .iter()
            .find(|entry| entry.key == "fixture:b")
            .expect("fixture:b must be present");
        assert!(
            expiring_entry.expires_at_ms.is_some(),
            "fixture:b was given an EXPIRE and must report an expiry"
        );

        let non_expiring_entry = report
            .largest_keys
            .iter()
            .find(|entry| entry.key == "fixture:a")
            .expect("fixture:a must be present");
        assert!(
            non_expiring_entry.expires_at_ms.is_none(),
            "fixture:a was never given an EXPIRE and must not report one"
        );

        let fixture_bucket = report
            .prefix_rollup
            .iter()
            .find(|entry| entry.prefix == "fixture:")
            .expect("fixture: prefix bucket must exist");
        assert_eq!(fixture_bucket.key_count, seeded_keys.len() as u64);

        Ok(())
    })
}
