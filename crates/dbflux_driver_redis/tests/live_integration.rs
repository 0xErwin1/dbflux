use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, HashDeleteRequest, HashSetRequest,
    KeyBulkGetRequest, KeyDeleteRequest, KeyExistsRequest, KeyExpireRequest, KeyGetRequest,
    KeyPersistRequest, KeyRenameRequest, KeyScanRequest, KeySetRequest, KeyTtlRequest, KeyType,
    KeyTypeRequest, ListEnd, ListPushRequest, ListRemoveRequest, ListSetRequest, QueryRequest,
    SchemaLoadingStrategy, SetAddRequest, SetRemoveRequest, StreamAddRequest, StreamDeleteRequest,
    StreamEntryId, ValueRepr, ZSetAddRequest, ZSetRemoveRequest,
};
use dbflux_driver_redis::RedisDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect_redis(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = RedisDriver::new();
    let profile = ConnectionProfile::new(
        "live-redis",
        DbConfig::Redis {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 6379,
            user: None,
            database: Some(0),
            tls: false,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    let connection = containers::retry_db_operation(Duration::from_secs(30), || {
        let connection = driver.connect(&profile)?;
        connection.ping()?;
        Ok(connection)
    })?;

    Ok(connection)
}

// ---------------------------------------------------------------------------
// Basic connectivity
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_live_connect_ping_query_and_schema() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;

        let result = connection.execute(&QueryRequest::new("PING"))?;
        assert!(!result.rows.is_empty() || result.text_body.is_some());

        assert_eq!(
            connection.schema_loading_strategy(),
            SchemaLoadingStrategy::LazyPerDatabase
        );

        let databases = connection.list_databases()?;
        assert!(!databases.is_empty());

        let schema = connection.schema()?;
        assert!(schema.is_key_value());
        let _ = schema.databases();

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_schema_introspection() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;

        let schema = connection.schema()?;
        assert!(schema.is_key_value());

        let keyspaces = schema.keyspaces();
        assert!(!keyspaces.is_empty());

        let db_schema = connection.schema_for_database("db0")?;
        assert_eq!(db_schema.name, "db0");

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Key-value string operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_key_value_string_ops() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        kv.set_key(&KeySetRequest::new("test:str", b"hello".to_vec()).with_repr(ValueRepr::Text))?;

        let exists = kv.exists_key(&KeyExistsRequest::new("test:str"))?;
        assert!(exists);

        let result = kv.get_key(&KeyGetRequest::new("test:str"))?;
        assert_eq!(result.value, b"hello");
        assert_eq!(result.entry.key, "test:str");

        let key_type = kv.key_type(&KeyTypeRequest::new("test:str"))?;
        assert_eq!(key_type, KeyType::String);

        kv.rename_key(&KeyRenameRequest::new("test:str", "test:str_renamed"))?;

        let exists_old = kv.exists_key(&KeyExistsRequest::new("test:str"))?;
        assert!(!exists_old);
        let exists_new = kv.exists_key(&KeyExistsRequest::new("test:str_renamed"))?;
        assert!(exists_new);

        let deleted = kv.delete_key(&KeyDeleteRequest::new("test:str_renamed"))?;
        assert!(deleted);

        let exists_after = kv.exists_key(&KeyExistsRequest::new("test:str_renamed"))?;
        assert!(!exists_after);

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// TTL and expiry
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_key_ttl_and_expiry() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        kv.set_key(&KeySetRequest::new("test:ttl", b"value".to_vec()))?;

        let ttl = kv.key_ttl(&KeyTtlRequest::new("test:ttl"))?;
        assert_eq!(ttl, Some(-1));

        let expired = kv.expire_key(&KeyExpireRequest::new("test:ttl", 300))?;
        assert!(expired);

        let ttl = kv.key_ttl(&KeyTtlRequest::new("test:ttl"))?;
        let ttl_val = ttl.expect("should have TTL");
        assert!(ttl_val > 0 && ttl_val <= 300);

        let persisted = kv.persist_key(&KeyPersistRequest::new("test:ttl"))?;
        assert!(persisted);

        let ttl = kv.key_ttl(&KeyTtlRequest::new("test:ttl"))?;
        assert_eq!(ttl, Some(-1));

        kv.set_key(&KeySetRequest::new("test:ttl_set", b"value".to_vec()).with_ttl(60))?;
        let ttl = kv.key_ttl(&KeyTtlRequest::new("test:ttl_set"))?;
        let ttl_val = ttl.expect("should have TTL");
        assert!(ttl_val > 0 && ttl_val <= 60);

        kv.delete_key(&KeyDeleteRequest::new("test:ttl"))?;
        kv.delete_key(&KeyDeleteRequest::new("test:ttl_set"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Hash operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_hash_ops() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        kv.hash_set(&HashSetRequest {
            key: "test:hash".to_string(),
            fields: vec![
                ("field1".to_string(), "value1".to_string()),
                ("field2".to_string(), "value2".to_string()),
            ],
            keyspace: None,
        })?;

        let result = kv.get_key(&KeyGetRequest::new("test:hash"))?;
        assert_eq!(result.entry.key_type, Some(KeyType::Hash));
        assert!(!result.value.is_empty());

        let deleted = kv.hash_delete(&HashDeleteRequest {
            key: "test:hash".to_string(),
            fields: vec!["field1".to_string()],
            keyspace: None,
        })?;
        assert!(deleted);

        kv.delete_key(&KeyDeleteRequest::new("test:hash"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// List operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_list_ops() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        kv.list_push(&ListPushRequest {
            key: "test:list".to_string(),
            values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            end: ListEnd::Tail,
            keyspace: None,
        })?;

        let result = kv.get_key(&KeyGetRequest::new("test:list"))?;
        assert_eq!(result.entry.key_type, Some(KeyType::List));

        kv.list_set(&ListSetRequest {
            key: "test:list".to_string(),
            index: 1,
            value: "B".to_string(),
            keyspace: None,
        })?;

        kv.list_push(&ListPushRequest {
            key: "test:list".to_string(),
            values: vec!["d".to_string()],
            end: ListEnd::Head,
            keyspace: None,
        })?;

        let removed = kv.list_remove(&ListRemoveRequest {
            key: "test:list".to_string(),
            value: "d".to_string(),
            count: 1,
            keyspace: None,
        })?;
        assert!(removed);

        kv.delete_key(&KeyDeleteRequest::new("test:list"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Set operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_set_ops() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        let added = kv.set_add(&SetAddRequest {
            key: "test:set".to_string(),
            members: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            keyspace: None,
        })?;
        assert!(added);

        let result = kv.get_key(&KeyGetRequest::new("test:set"))?;
        assert_eq!(result.entry.key_type, Some(KeyType::Set));

        let removed = kv.set_remove(&SetRemoveRequest {
            key: "test:set".to_string(),
            members: vec!["b".to_string()],
            keyspace: None,
        })?;
        assert!(removed);

        kv.delete_key(&KeyDeleteRequest::new("test:set"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Sorted set operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_zset_ops() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        let added = kv.zset_add(&ZSetAddRequest {
            key: "test:zset".to_string(),
            members: vec![
                ("alice".to_string(), 100.0),
                ("bob".to_string(), 200.0),
                ("charlie".to_string(), 150.0),
            ],
            keyspace: None,
        })?;
        assert!(added);

        let result = kv.get_key(&KeyGetRequest::new("test:zset"))?;
        assert_eq!(result.entry.key_type, Some(KeyType::SortedSet));

        let removed = kv.zset_remove(&ZSetRemoveRequest {
            key: "test:zset".to_string(),
            members: vec!["bob".to_string()],
            keyspace: None,
        })?;
        assert!(removed);

        kv.delete_key(&KeyDeleteRequest::new("test:zset"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Stream operations
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_stream_ops() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        let id1 = kv.stream_add(&StreamAddRequest {
            key: "test:stream".to_string(),
            id: StreamEntryId::Auto,
            fields: vec![
                ("field1".to_string(), "value1".to_string()),
                ("field2".to_string(), "value2".to_string()),
            ],
            maxlen: None,
            keyspace: None,
        })?;
        assert!(!id1.is_empty());

        let id2 = kv.stream_add(&StreamAddRequest {
            key: "test:stream".to_string(),
            id: StreamEntryId::Auto,
            fields: vec![("field1".to_string(), "value3".to_string())],
            maxlen: None,
            keyspace: None,
        })?;
        assert!(!id2.is_empty());

        let result = kv.get_key(&KeyGetRequest::new("test:stream"))?;
        assert_eq!(result.entry.key_type, Some(KeyType::Stream));

        let deleted = kv.stream_delete(&StreamDeleteRequest {
            key: "test:stream".to_string(),
            ids: vec![id1],
            keyspace: None,
        })?;
        assert_eq!(deleted, 1);

        kv.delete_key(&KeyDeleteRequest::new("test:stream"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Scan keys
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_scan_keys() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        for i in 0..10 {
            kv.set_key(
                &KeySetRequest::new(format!("scan:key:{}", i), b"v".to_vec())
                    .with_repr(ValueRepr::Text),
            )?;
        }

        let page = kv.scan_keys(&KeyScanRequest::new(100))?;
        assert!(page.entries.len() >= 10);

        let filtered = kv.scan_keys(&KeyScanRequest::new(100).with_filter("scan:key:*"))?;
        assert_eq!(filtered.entries.len(), 10);

        for i in 0..10 {
            kv.delete_key(&KeyDeleteRequest::new(format!("scan:key:{}", i)))?;
        }

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Bulk get
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_bulk_get() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;
        let kv = connection
            .key_value_api()
            .expect("Redis should have KV API");

        kv.set_key(&KeySetRequest::new("bulk:a", b"val_a".to_vec()).with_repr(ValueRepr::Text))?;
        kv.set_key(&KeySetRequest::new("bulk:b", b"val_b".to_vec()).with_repr(ValueRepr::Text))?;

        let results = kv.bulk_get(&KeyBulkGetRequest::new(vec![
            "bulk:a".to_string(),
            "bulk:nonexistent".to_string(),
            "bulk:b".to_string(),
        ]))?;

        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_none());
        assert!(results[2].is_some());

        assert_eq!(results[0].as_ref().unwrap().value, b"val_a");
        assert_eq!(results[2].as_ref().unwrap().value, b"val_b");

        kv.delete_key(&KeyDeleteRequest::new("bulk:a"))?;
        kv.delete_key(&KeyDeleteRequest::new("bulk:b"))?;

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Cancel not supported
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn redis_cancel_not_supported() -> Result<(), DbError> {
    containers::with_redis_url(|uri| {
        let connection = connect_redis(uri)?;

        let (handle, _) = connection.execute_with_handle(&QueryRequest::new("PING"))?;
        let cancel = connection.cancel(&handle);
        assert!(matches!(cancel, Err(DbError::NotSupported(_))));

        Ok(())
    })
}
