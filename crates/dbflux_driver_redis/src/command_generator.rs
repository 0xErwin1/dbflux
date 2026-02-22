use dbflux_core::{
    GeneratedQuery, HashDeleteRequest, HashSetRequest, KeyDeleteRequest, KeySetRequest, ListEnd,
    ListPushRequest, ListRemoveRequest, ListSetRequest, MutationCategory, MutationRequest,
    QueryGenerator, QueryLanguage, SetAddRequest, SetCondition, SetRemoveRequest, StreamAddRequest,
    StreamDeleteRequest, StreamEntryId, ZSetAddRequest, ZSetRemoveRequest,
};

pub struct RedisCommandGenerator;

impl QueryGenerator for RedisCommandGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::KeyValue]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let text = match mutation {
            MutationRequest::KeyValueSet(req) => generate_set(req),
            MutationRequest::KeyValueDelete(req) => generate_del(req),
            MutationRequest::KeyValueHashSet(req) => generate_hset(req),
            MutationRequest::KeyValueHashDelete(req) => generate_hdel(req),
            MutationRequest::KeyValueListPush(req) => generate_list_push(req),
            MutationRequest::KeyValueListSet(req) => generate_lset(req),
            MutationRequest::KeyValueListRemove(req) => generate_lrem(req),
            MutationRequest::KeyValueSetAdd(req) => generate_sadd(req),
            MutationRequest::KeyValueSetRemove(req) => generate_srem(req),
            MutationRequest::KeyValueZSetAdd(req) => generate_zadd(req),
            MutationRequest::KeyValueZSetRemove(req) => generate_zrem(req),
            MutationRequest::KeyValueStreamAdd(req) => generate_xadd(req),
            MutationRequest::KeyValueStreamDelete(req) => generate_xdel(req),
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::RedisCommands,
            text,
        })
    }
}

fn escape_arg(s: &str) -> String {
    if s.is_empty() || s.contains(char::is_whitespace) || s.contains('"') || s.contains('\'') {
        let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        s.to_string()
    }
}

fn generate_set(req: &KeySetRequest) -> String {
    let key = escape_arg(&req.key);
    let value = escape_arg(&String::from_utf8_lossy(&req.value));

    let mut parts = vec!["SET".to_string(), key, value];

    if let Some(ttl) = req.ttl_seconds {
        parts.push("EX".to_string());
        parts.push(ttl.to_string());
    }

    match req.condition {
        SetCondition::IfNotExists => parts.push("NX".to_string()),
        SetCondition::IfExists => parts.push("XX".to_string()),
        SetCondition::Always => {}
    }

    parts.join(" ")
}

fn generate_del(req: &KeyDeleteRequest) -> String {
    format!("DEL {}", escape_arg(&req.key))
}

fn generate_hset(req: &HashSetRequest) -> String {
    let mut parts = vec!["HSET".to_string(), escape_arg(&req.key)];
    for (field, value) in &req.fields {
        parts.push(escape_arg(field));
        parts.push(escape_arg(value));
    }
    parts.join(" ")
}

fn generate_hdel(req: &HashDeleteRequest) -> String {
    let mut parts = vec!["HDEL".to_string(), escape_arg(&req.key)];
    for field in &req.fields {
        parts.push(escape_arg(field));
    }
    parts.join(" ")
}

fn generate_list_push(req: &ListPushRequest) -> String {
    let cmd = match req.end {
        ListEnd::Head => "LPUSH",
        ListEnd::Tail => "RPUSH",
    };

    let mut parts = vec![cmd.to_string(), escape_arg(&req.key)];
    for value in &req.values {
        parts.push(escape_arg(value));
    }
    parts.join(" ")
}

fn generate_lset(req: &ListSetRequest) -> String {
    format!(
        "LSET {} {} {}",
        escape_arg(&req.key),
        req.index,
        escape_arg(&req.value),
    )
}

fn generate_lrem(req: &ListRemoveRequest) -> String {
    format!(
        "LREM {} {} {}",
        escape_arg(&req.key),
        req.count,
        escape_arg(&req.value),
    )
}

fn generate_sadd(req: &SetAddRequest) -> String {
    let mut parts = vec!["SADD".to_string(), escape_arg(&req.key)];
    for member in &req.members {
        parts.push(escape_arg(member));
    }
    parts.join(" ")
}

fn generate_srem(req: &SetRemoveRequest) -> String {
    let mut parts = vec!["SREM".to_string(), escape_arg(&req.key)];
    for member in &req.members {
        parts.push(escape_arg(member));
    }
    parts.join(" ")
}

fn generate_zadd(req: &ZSetAddRequest) -> String {
    let mut parts = vec!["ZADD".to_string(), escape_arg(&req.key)];
    for (member, score) in &req.members {
        parts.push(score.to_string());
        parts.push(escape_arg(member));
    }
    parts.join(" ")
}

fn generate_zrem(req: &ZSetRemoveRequest) -> String {
    let mut parts = vec!["ZREM".to_string(), escape_arg(&req.key)];
    for member in &req.members {
        parts.push(escape_arg(member));
    }
    parts.join(" ")
}

fn generate_xadd(req: &StreamAddRequest) -> String {
    let key = escape_arg(&req.key);

    let id = match &req.id {
        StreamEntryId::Auto => "*".to_string(),
        StreamEntryId::Explicit(id) => escape_arg(id),
    };

    let mut parts = vec!["XADD".to_string(), key];

    if let Some(maxlen) = &req.maxlen {
        if maxlen.approximate {
            parts.push(format!("MAXLEN ~ {}", maxlen.count));
        } else {
            parts.push(format!("MAXLEN {}", maxlen.count));
        }
    }

    parts.push(id);

    for (field, value) in &req.fields {
        parts.push(escape_arg(field));
        parts.push(escape_arg(value));
    }

    parts.join(" ")
}

fn generate_xdel(req: &StreamDeleteRequest) -> String {
    let mut parts = vec!["XDEL".to_string(), escape_arg(&req.key)];

    for id in &req.ids {
        parts.push(escape_arg(id));
    }

    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::StreamMaxLen;

    #[test]
    fn set_simple() {
        let req = KeySetRequest::new("mykey", b"hello".to_vec());
        let mutation = MutationRequest::KeyValueSet(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.language, QueryLanguage::RedisCommands);
        assert_eq!(result.text, "SET mykey hello");
    }

    #[test]
    fn set_with_ttl_and_condition() {
        let req = KeySetRequest::new("mykey", b"hello".to_vec())
            .with_ttl(60)
            .if_not_exists();
        let mutation = MutationRequest::KeyValueSet(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "SET mykey hello EX 60 NX");
    }

    #[test]
    fn set_escapes_spaces() {
        let req = KeySetRequest::new("my key", b"hello world".to_vec());
        let mutation = MutationRequest::KeyValueSet(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, r#"SET "my key" "hello world""#);
    }

    #[test]
    fn del_key() {
        let req = KeyDeleteRequest::new("mykey");
        let mutation = MutationRequest::KeyValueDelete(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "DEL mykey");
    }

    #[test]
    fn hset_single_field() {
        let req = HashSetRequest {
            key: "myhash".to_string(),
            fields: vec![("name".to_string(), "Alice".to_string())],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueHashSet(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "HSET myhash name Alice");
    }

    #[test]
    fn hset_multiple_fields() {
        let req = HashSetRequest {
            key: "myhash".to_string(),
            fields: vec![
                ("name".to_string(), "Alice".to_string()),
                ("age".to_string(), "30".to_string()),
            ],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueHashSet(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "HSET myhash name Alice age 30");
    }

    #[test]
    fn hdel_single_field() {
        let req = HashDeleteRequest {
            key: "myhash".to_string(),
            fields: vec!["name".to_string()],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueHashDelete(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "HDEL myhash name");
    }

    #[test]
    fn hdel_multiple_fields() {
        let req = HashDeleteRequest {
            key: "myhash".to_string(),
            fields: vec!["name".to_string(), "age".to_string()],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueHashDelete(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "HDEL myhash name age");
    }

    #[test]
    fn lpush_single_value() {
        let req = ListPushRequest {
            key: "mylist".to_string(),
            values: vec!["item".to_string()],
            end: ListEnd::Head,
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueListPush(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "LPUSH mylist item");
    }

    #[test]
    fn rpush_multiple_values() {
        let req = ListPushRequest {
            key: "mylist".to_string(),
            values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            end: ListEnd::Tail,
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueListPush(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "RPUSH mylist a b c");
    }

    #[test]
    fn lset_index() {
        let req = ListSetRequest {
            key: "mylist".to_string(),
            index: 2,
            value: "newval".to_string(),
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueListSet(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "LSET mylist 2 newval");
    }

    #[test]
    fn sadd_single_member() {
        let req = SetAddRequest {
            key: "myset".to_string(),
            members: vec!["elem".to_string()],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueSetAdd(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "SADD myset elem");
    }

    #[test]
    fn sadd_multiple_members() {
        let req = SetAddRequest {
            key: "myset".to_string(),
            members: vec![
                "redis".to_string(),
                "rust".to_string(),
                "backend".to_string(),
            ],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueSetAdd(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "SADD myset redis rust backend");
    }

    #[test]
    fn zadd_single_member() {
        let req = ZSetAddRequest {
            key: "myzset".to_string(),
            members: vec![("elem".to_string(), 1.5)],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueZSetAdd(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "ZADD myzset 1.5 elem");
    }

    #[test]
    fn zadd_multiple_members() {
        let req = ZSetAddRequest {
            key: "myzset".to_string(),
            members: vec![("alice".to_string(), 10.0), ("bob".to_string(), 20.0)],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueZSetAdd(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "ZADD myzset 10 alice 20 bob");
    }

    #[test]
    fn xadd_auto_id() {
        let req = StreamAddRequest {
            key: "mystream".to_string(),
            id: StreamEntryId::Auto,
            fields: vec![
                ("name".to_string(), "Alice".to_string()),
                ("age".to_string(), "30".to_string()),
            ],
            maxlen: None,
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueStreamAdd(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "XADD mystream * name Alice age 30");
    }

    #[test]
    fn xadd_with_maxlen() {
        let req = StreamAddRequest {
            key: "mystream".to_string(),
            id: StreamEntryId::Auto,
            fields: vec![("k".to_string(), "v".to_string())],
            maxlen: Some(StreamMaxLen {
                count: 1000,
                approximate: true,
            }),
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueStreamAdd(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "XADD mystream MAXLEN ~ 1000 * k v");
    }

    #[test]
    fn xdel_entries() {
        let req = StreamDeleteRequest {
            key: "mystream".to_string(),
            ids: vec!["1-0".to_string(), "2-0".to_string()],
            keyspace: None,
        };
        let mutation = MutationRequest::KeyValueStreamDelete(req);

        let result = RedisCommandGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.text, "XDEL mystream 1-0 2-0");
    }

    #[test]
    fn sql_mutation_returns_none() {
        let patch = dbflux_core::RowPatch::new(
            dbflux_core::RecordIdentity::composite(
                vec!["id".to_string()],
                vec![dbflux_core::Value::Int(1)],
            ),
            "users".to_string(),
            None,
            vec![("name".to_string(), dbflux_core::Value::Text("test".into()))],
        );
        let mutation = MutationRequest::SqlUpdate(patch);

        assert!(RedisCommandGenerator.generate_mutation(&mutation).is_none());
    }
}
