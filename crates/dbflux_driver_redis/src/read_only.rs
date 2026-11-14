//! Read-only enforcement for Redis command requests.
//!
//! Redis has no read-only session mode, so DBFlux enforces
//! [`ReadOnlyEnforcement::Required`](dbflux_core::ReadOnlyEnforcement::Required)
//! itself, per command, from the flags the server reports for that command
//! in `COMMAND INFO`. The check runs on the connection that then sends the
//! command, with the same tokens, so the command that is checked is the
//! command that is sent. A command the server does not flag as a read, or
//! does not describe at all, is refused before it is sent; a server that
//! cannot answer `COMMAND INFO` makes the whole request unsupported.
//!
//! `execute` sends exactly one command per request: every line of the input
//! is tokenized into that single command. Redis Cluster connections refuse
//! the request as unsupported, because the node answering `COMMAND INFO` may
//! not be the node that runs the command. The driver-owned instance metric
//! and inspector requests run fixed `INFO` and `CLIENT LIST` reads and are
//! not checked here.

use dbflux_core::DbError;
use redis::Value;

use crate::driver::format_redis_query_error;

/// Commands refused by name whatever their flags: they change the
/// connection's state (transactions, the selected database, protocol and
/// authentication, cluster read routing), hold the connection
/// (subscriptions, `MONITOR`, replication), or run a script. A script is
/// refused even in its `_RO` form: Redis only rejects `write`,
/// `may_replicate` and `noscript` commands inside it, so it can still call
/// admin commands such as `SLOWLOG RESET`, and a script that never returns
/// makes the server answer `BUSY` to every client. `PFCOUNT` is flagged
/// `readonly` but rewrites the key's cached cardinality on Redis 6.0 and
/// earlier.
const REFUSED_COMMANDS: &[&str] = &[
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    "EVAL",
    "EVALSHA",
    "FCALL",
    "EVAL_RO",
    "EVALSHA_RO",
    "FCALL_RO",
    "PFCOUNT",
    "SELECT",
    "SWAPDB",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "SSUBSCRIBE",
    "UNSUBSCRIBE",
    "PUNSUBSCRIBE",
    "SUNSUBSCRIBE",
    "MONITOR",
    "SYNC",
    "PSYNC",
    "RESET",
    "HELLO",
    "AUTH",
    "QUIT",
    "READONLY",
    "READWRITE",
];

/// Random-member reads whose count is their second argument. A negative
/// count returns that many members, repeats allowed, whatever the key's
/// size, so `SRANDMEMBER key -9223372036854775807` builds an unbounded reply
/// on a key of one member. They are refused with a negative count; a positive
/// count is bounded by the key's size.
const RANDOM_MEMBER_COMMANDS: &[&str] = &["SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER"];

/// Commands whose effect depends on their subcommand. They are judged by the
/// subcommand's own `COMMAND INFO` entry (`name|subcommand`, Redis 7 and
/// later), and refused when the server cannot describe it. Servers that list
/// subcommands in `COMMAND INFO` mark further containers themselves.
const CONTAINER_COMMANDS: &[&str] = &[
    "ACL", "CLIENT", "CLUSTER", "COMMAND", "CONFIG", "FUNCTION", "LATENCY", "MEMORY", "MODULE",
    "OBJECT", "PUBSUB", "SCRIPT", "SLOWLOG", "XGROUP", "XINFO",
];

/// Commands that read server metadata rather than data, so Redis does not
/// flag them `readonly`. They are allowed when the server knows them and
/// reports no flag outside [`BENIGN_FLAGS`].
const METADATA_COMMANDS: &[&str] = &["PING", "ECHO", "TIME", "INFO"];

/// Flags that describe how a command may be called, not what it changes.
/// A command carrying any other flag (`write`, `admin`, `blocking`,
/// `pubsub`, `may_replicate`, `denyoom`, `no_multi`, `no_auth`, or a flag
/// this list does not know) is refused. `module` is not benign: a module
/// declares its own flags, and its command filters can rewrite a command
/// after this check, so DBFlux cannot classify what a module command does.
const BENIGN_FLAGS: &[&str] = &[
    "readonly",
    "fast",
    "random",
    "loading",
    "stale",
    "sort_for_script",
    "movablekeys",
    "skip_monitor",
    "skip_slowlog",
    "no_mandatory_keys",
    "allow_busy",
    "noscript",
    "sentinel",
    "allow_cross_slot",
];

/// What the server reported for one name in `COMMAND INFO`.
#[derive(Debug)]
enum CommandDescription {
    /// The server does not know the command.
    Unknown,

    /// The server knows the command.
    Known {
        /// The command's flags, lowercased.
        flags: Vec<String>,

        /// Whether the server lists subcommands for it.
        has_subcommands: bool,
    },
}

/// Refuses `parts` unless the server reports it as a read.
///
/// `parts` is the command name followed by its arguments, exactly as they
/// will be sent. Returns [`DbError::QueryFailed`] naming the command when it
/// is refused, and [`DbError::NotSupported`] when the server cannot describe
/// commands at all, so the gate cannot be evaluated. A connection-level
/// failure while asking is returned as a connection error.
pub(crate) fn ensure_read_only_command(
    conn: &mut dyn redis::ConnectionLike,
    parts: &[String],
) -> Result<(), DbError> {
    let Some((name, arguments)) = parts.split_first() else {
        return Ok(());
    };

    let name = name.to_ascii_uppercase();

    if name.contains('|') {
        return Err(refusal(&name, "a command name cannot name a subcommand"));
    }

    if REFUSED_COMMANDS.contains(&name.as_str()) {
        return Err(refusal(
            &name,
            "it changes the connection's state, holds the connection, or runs a script that may write",
        ));
    }

    let negative_count = arguments
        .get(1)
        .is_some_and(|count| count.trim_start().starts_with('-'));
    if negative_count && RANDOM_MEMBER_COMMANDS.contains(&name.as_str()) {
        return Err(refusal(
            &name,
            "a negative count returns that many members whatever the key's size",
        ));
    }

    let description = describe_command(conn, &name)?;
    let CommandDescription::Known {
        flags,
        has_subcommands,
    } = description
    else {
        return Err(refusal(&name, "the server does not describe this command"));
    };

    if !has_subcommands && !CONTAINER_COMMANDS.contains(&name.as_str()) {
        let metadata = METADATA_COMMANDS.contains(&name.as_str());
        return judge_flags(&name, &flags, metadata);
    }

    let Some(subcommand) = arguments.first() else {
        return Err(refusal(&name, "the command needs a subcommand"));
    };

    let display = format!("{name} {}", subcommand.to_ascii_uppercase());

    if subcommand.contains('|') {
        return Err(refusal(&display, "the subcommand name is not valid"));
    }

    match describe_command(conn, &format!("{name}|{subcommand}"))? {
        CommandDescription::Known { flags, .. } => judge_flags(&display, &flags, false),
        CommandDescription::Unknown => Err(refusal(
            &display,
            "the server does not describe this subcommand",
        )),
    }
}

/// Allows a command whose flags are all benign and include `readonly`, or
/// that is a metadata command; refuses everything else.
fn judge_flags(display: &str, flags: &[String], metadata: bool) -> Result<(), DbError> {
    if let Some(flag) = flags
        .iter()
        .find(|flag| !BENIGN_FLAGS.contains(&flag.as_str()))
    {
        return Err(refusal(display, &format!("the server flags it `{flag}`")));
    }

    if !metadata && !flags.iter().any(|flag| flag == "readonly") {
        return Err(refusal(display, "the server does not flag it `readonly`"));
    }

    Ok(())
}

fn refusal(display: &str, reason: &str) -> DbError {
    DbError::query_failed(format!(
        "Redis: `{display}` is refused in a read-only request because {reason}; \
         nothing was sent to the server"
    ))
}

/// Asks the server for `COMMAND INFO name`.
fn describe_command(
    conn: &mut dyn redis::ConnectionLike,
    name: &str,
) -> Result<CommandDescription, DbError> {
    let reply = redis::cmd("COMMAND")
        .arg("INFO")
        .arg(name)
        .query::<Value>(conn)
        .map_err(|error| {
            let formatted = format_redis_query_error(&error);
            if matches!(formatted, DbError::ConnectionFailed(_)) {
                return formatted;
            }

            DbError::NotSupported(format!(
                "Redis: read-only enforcement needs COMMAND INFO, which the server refused \
                 ({error}); the request was rejected before execution"
            ))
        })?;

    parse_command_info(reply).ok_or_else(|| {
        DbError::NotSupported(
            "Redis: read-only enforcement needs COMMAND INFO, whose reply this server does not \
             give in the expected shape; the request was rejected before execution"
                .to_string(),
        )
    })
}

/// Parses a `COMMAND INFO` reply for a single name. Returns `None` when the
/// reply does not have the documented shape.
fn parse_command_info(reply: Value) -> Option<CommandDescription> {
    let Value::Array(mut entries) = reply else {
        return None;
    };

    if entries.len() != 1 {
        return None;
    }

    let fields = match entries.pop()? {
        Value::Nil => return Some(CommandDescription::Unknown),
        Value::Array(fields) => fields,
        _ => return None,
    };

    let flags = match fields.get(2)? {
        Value::Array(flags) | Value::Set(flags) => flags
            .iter()
            .map(|flag| match flag {
                Value::SimpleString(text) => Some(text.to_ascii_lowercase()),
                Value::BulkString(bytes) => std::str::from_utf8(bytes)
                    .ok()
                    .map(|text| text.to_ascii_lowercase()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?,
        _ => return None,
    };

    let has_subcommands = match fields.get(9) {
        None => false,
        Some(Value::Array(subcommands)) | Some(Value::Set(subcommands)) => !subcommands.is_empty(),
        Some(_) => return None,
    };

    Some(CommandDescription::Known {
        flags,
        has_subcommands,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dbflux_core::{DbError, ReadOnlyEnforcement};
    use redis::{ConnectionLike, ErrorKind, RedisError, RedisResult, Value};

    use crate::driver::{REDIS_METADATA, parse_command, send_parsed_command};

    /// A Redis server that answers `COMMAND INFO` from a fixed table and
    /// records every other command it receives.
    struct FakeServer {
        table: HashMap<String, Value>,
        command_info: CommandInfoSupport,
        lookups: Vec<String>,
        sent: Vec<Vec<String>>,
    }

    enum CommandInfoSupport {
        Supported,
        Missing,
        Malformed,
    }

    impl FakeServer {
        /// Flags as Redis 7 reports them for the commands the tests use.
        fn redis_7() -> Self {
            let mut table = HashMap::new();

            for (name, flags) in [
                ("get", &["readonly", "fast"][..]),
                ("hgetall", &["readonly"]),
                ("scan", &["readonly"]),
                ("ttl", &["readonly", "fast"]),
                ("set", &["write", "denyoom"]),
                ("del", &["write"]),
                ("flushall", &["write"]),
                (
                    "eval",
                    &[
                        "noscript",
                        "skip_monitor",
                        "may_replicate",
                        "no_mandatory_keys",
                        "stale",
                    ],
                ),
                (
                    "eval_ro",
                    &[
                        "readonly",
                        "noscript",
                        "skip_monitor",
                        "no_mandatory_keys",
                        "stale",
                    ],
                ),
                (
                    "multi",
                    &["noscript", "loading", "stale", "fast", "allow_busy"],
                ),
                ("subscribe", &["pubsub", "noscript", "loading", "stale"]),
                ("select", &["loading", "stale", "fast"]),
                ("ping", &["fast", "sentinel"]),
                ("blpop", &["write", "blocking"]),
                ("pfcount", &["readonly"]),
                ("srandmember", &["readonly"]),
                ("hrandfield", &["readonly"]),
                ("zrandmember", &["readonly"]),
                (
                    "evalsha_ro",
                    &["readonly", "noscript", "skip_monitor", "stale"],
                ),
                (
                    "fcall_ro",
                    &["readonly", "noscript", "skip_monitor", "stale"],
                ),
                ("module.read", &["readonly", "module"]),
                ("xread", &["readonly", "blocking", "movablekeys"]),
            ] {
                table.insert(name.to_string(), entry(name, flags, Vec::new()));
            }

            let config_set = entry(
                "config|set",
                &["admin", "noscript", "loading", "stale"],
                vec![],
            );
            let config_get = entry(
                "config|get",
                &["admin", "noscript", "loading", "stale"],
                vec![],
            );
            table.insert("config|set".to_string(), config_set.clone());
            table.insert("config|get".to_string(), config_get.clone());
            table.insert(
                "config".to_string(),
                entry("config", &[], vec![config_set, config_get]),
            );

            let object_encoding = entry("object|encoding", &["readonly"], vec![]);
            table.insert("object|encoding".to_string(), object_encoding.clone());
            table.insert(
                "object".to_string(),
                entry("object", &[], vec![object_encoding]),
            );

            Self {
                table,
                command_info: CommandInfoSupport::Supported,
                lookups: Vec::new(),
                sent: Vec::new(),
            }
        }

        fn with_entry(mut self, name: &str, value: Value) -> Self {
            self.table.insert(name.to_string(), value);
            self
        }

        fn without_entry(mut self, name: &str) -> Self {
            self.table.remove(name);
            self
        }

        fn with_command_info(mut self, support: CommandInfoSupport) -> Self {
            self.command_info = support;
            self
        }

        fn answer_command_info(&mut self, names: &[String]) -> RedisResult<Value> {
            match self.command_info {
                CommandInfoSupport::Missing => Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "An error was signalled by the server",
                    "unknown command 'COMMAND'".to_string(),
                ))),
                CommandInfoSupport::Malformed => Ok(Value::Okay),
                CommandInfoSupport::Supported => {
                    let entries = names
                        .iter()
                        .map(|name| {
                            self.lookups.push(name.clone());
                            self.table
                                .get(&name.to_ascii_lowercase())
                                .cloned()
                                .unwrap_or(Value::Nil)
                        })
                        .collect();

                    Ok(Value::Array(entries))
                }
            }
        }
    }

    impl ConnectionLike for FakeServer {
        fn req_command(&mut self, command: &redis::Cmd) -> RedisResult<Value> {
            let arguments: Vec<String> = command
                .args_iter()
                .map(|argument| match argument {
                    redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    redis::Arg::Cursor => "0".to_string(),
                })
                .collect();

            let is_command_info = arguments.len() >= 2
                && arguments[0].eq_ignore_ascii_case("COMMAND")
                && arguments[1].eq_ignore_ascii_case("INFO");

            if is_command_info {
                return self.answer_command_info(&arguments[2..]);
            }

            self.sent.push(arguments);
            Ok(Value::Okay)
        }

        fn req_packed_command(&mut self, _command: &[u8]) -> RedisResult<Value> {
            unreachable!("the fake server only answers req_command")
        }

        fn req_packed_commands(
            &mut self,
            _commands: &[u8],
            _offset: usize,
            _count: usize,
        ) -> RedisResult<Vec<Value>> {
            unreachable!("the fake server only answers req_command")
        }

        fn get_db(&self) -> i64 {
            0
        }

        fn check_connection(&mut self) -> bool {
            true
        }

        fn is_open(&self) -> bool {
            true
        }
    }

    /// A `COMMAND INFO` entry in the Redis 7 layout: name, arity, flags,
    /// first key, last key, step, ACL categories, tips, key specs and
    /// subcommands.
    fn entry(name: &str, flags: &[&str], subcommands: Vec<Value>) -> Value {
        Value::Array(vec![
            Value::BulkString(name.as_bytes().to_vec()),
            Value::Int(-2),
            Value::Array(
                flags
                    .iter()
                    .map(|flag| Value::SimpleString(flag.to_string()))
                    .collect(),
            ),
            Value::Int(1),
            Value::Int(1),
            Value::Int(1),
            Value::Array(Vec::new()),
            Value::Array(Vec::new()),
            Value::Array(Vec::new()),
            Value::Array(subcommands),
        ])
    }

    /// A `COMMAND INFO` entry in the Redis 6 layout, which has no subcommands.
    fn legacy_entry(name: &str, flags: &[&str]) -> Value {
        Value::Array(vec![
            Value::BulkString(name.as_bytes().to_vec()),
            Value::Int(-2),
            Value::Array(
                flags
                    .iter()
                    .map(|flag| Value::SimpleString(flag.to_string()))
                    .collect(),
            ),
            Value::Int(1),
            Value::Int(1),
            Value::Int(1),
        ])
    }

    fn run(server: &mut FakeServer, input: &str) -> Result<Value, DbError> {
        let parts = parse_command(input)?;
        send_parsed_command(server, &parts, ReadOnlyEnforcement::Required)
    }

    fn assert_refused(server: &mut FakeServer, input: &str) {
        let outcome = run(server, input);

        assert!(
            matches!(&outcome, Err(DbError::QueryFailed(error)) if error.to_string().contains("read-only")),
            "{input} must be refused as a query failure, got {outcome:?}"
        );
        assert!(
            server.sent.is_empty(),
            "{input} must not reach the server, but sent {:?}",
            server.sent
        );
    }

    #[test]
    fn reads_flagged_readonly_by_the_server_are_sent_unchanged() {
        for input in [
            "GET session:1",
            "HGETALL user:1",
            "SCAN 0 MATCH user:* COUNT 10",
            "TTL session:1",
            "OBJECT ENCODING session:1",
            "PING",
        ] {
            let mut server = FakeServer::redis_7();

            run(&mut server, input).unwrap_or_else(|error| panic!("{input}: {error:?}"));

            let expected = parse_command(input).expect("command parses");
            assert_eq!(server.sent, vec![expected], "{input}");
        }
    }

    #[test]
    fn writes_and_state_changing_commands_are_refused_before_anything_is_sent() {
        for input in [
            "SET session:1 value",
            "DEL session:1",
            "FLUSHALL",
            "EVAL \"redis.call('set', KEYS[1], 'x')\" 1 session:1",
            "EVALSHA abc 1 session:1",
            "FCALL writer 1 session:1",
            "MULTI",
            "EXEC",
            "WATCH session:1",
            "SUBSCRIBE news",
            "MONITOR",
            "SELECT 1",
            "CONFIG SET maxmemory 1",
            "CONFIG GET maxmemory",
            "CONFIG",
            "BLPOP queue 0",
            "XREAD COUNT 1 STREAMS events 0",
            "NOSUCHCOMMAND session:1",
            "OBJECT|ENCODING session:1",
        ] {
            let mut server = FakeServer::redis_7();
            assert_refused(&mut server, input);
        }
    }

    #[test]
    fn a_write_on_the_first_line_of_multi_line_input_is_refused_as_a_whole() {
        let mut server = FakeServer::redis_7();

        assert_refused(&mut server, "SET session:1 value\nGET session:1");
    }

    #[test]
    fn multi_line_input_is_sent_as_the_single_command_that_was_checked() {
        let mut server = FakeServer::redis_7();

        run(&mut server, "GET session:1\nSET session:1 value").expect("GET is a read");

        assert_eq!(server.lookups, vec!["GET".to_string()]);
        assert_eq!(
            server.sent,
            vec![vec![
                "GET".to_string(),
                "session:1".to_string(),
                "SET".to_string(),
                "session:1".to_string(),
                "value".to_string(),
            ]],
            "the lines form one GET command; no SET command is sent"
        );
    }

    #[test]
    fn a_server_without_command_info_refuses_the_request_as_unsupported() {
        for support in [CommandInfoSupport::Missing, CommandInfoSupport::Malformed] {
            let mut server = FakeServer::redis_7().with_command_info(support);

            let outcome = run(&mut server, "GET session:1");

            assert!(
                matches!(outcome, Err(DbError::NotSupported(_))),
                "got {outcome:?}"
            );
            assert!(server.sent.is_empty());
        }
    }

    #[test]
    fn a_container_the_server_cannot_describe_by_subcommand_is_refused() {
        let mut server = FakeServer::redis_7()
            .with_entry("object", legacy_entry("object", &["readonly", "random"]))
            .without_entry("object|encoding");

        assert_refused(&mut server, "OBJECT ENCODING session:1");
    }

    #[test]
    fn a_read_with_a_flag_dbflux_does_not_know_is_refused() {
        let mut server = FakeServer::redis_7().with_entry(
            "get",
            entry("get", &["readonly", "new_side_effect"], vec![]),
        );

        assert_refused(&mut server, "GET session:1");
    }

    #[test]
    fn read_only_scripts_are_refused_even_when_the_server_flags_them_readonly() {
        for input in [
            "EVAL_RO \"return redis.call('get', KEYS[1])\" 1 session:1",
            "EVAL_RO \"while true do end\" 0",
            "EVALSHA_RO abc 0",
            "FCALL_RO reader 0",
        ] {
            let mut server = FakeServer::redis_7();
            assert_refused(&mut server, input);
            assert!(server.lookups.is_empty(), "{input} is refused by name");
        }
    }

    #[test]
    fn random_member_reads_with_a_negative_count_are_refused() {
        for input in [
            "SRANDMEMBER tags -9223372036854775807",
            "HRANDFIELD user:1 -5 WITHVALUES",
            "ZRANDMEMBER scores -1 WITHSCORES",
        ] {
            let mut server = FakeServer::redis_7();
            assert_refused(&mut server, input);
        }
    }

    #[test]
    fn random_member_reads_with_a_positive_or_no_count_are_sent() {
        for input in [
            "SRANDMEMBER tags",
            "SRANDMEMBER tags 5",
            "HRANDFIELD user:1 5 WITHVALUES",
            "ZRANDMEMBER scores 3 WITHSCORES",
        ] {
            let mut server = FakeServer::redis_7();

            run(&mut server, input).unwrap_or_else(|error| panic!("{input}: {error:?}"));

            assert_eq!(server.sent.len(), 1, "{input}");
        }
    }

    #[test]
    fn pfcount_is_refused_because_older_servers_write_its_cached_cardinality() {
        let mut server = FakeServer::redis_7();

        assert_refused(&mut server, "PFCOUNT visitors");
    }

    #[test]
    fn a_command_a_module_declares_readonly_is_refused() {
        let mut server = FakeServer::redis_7();

        assert_refused(&mut server, "MODULE.READ session:1");
    }

    #[test]
    fn a_script_command_the_server_does_not_flag_readonly_is_refused() {
        let mut server = FakeServer::redis_7().with_entry(
            "eval_ro",
            entry("eval_ro", &["noscript", "skip_monitor", "stale"], vec![]),
        );

        assert_refused(&mut server, "EVAL_RO \"return 1\" 0");
    }

    #[test]
    fn a_metadata_command_must_still_be_known_to_the_server() {
        let mut server = FakeServer::redis_7().without_entry("ping");

        assert_refused(&mut server, "PING");
    }

    #[test]
    fn requests_without_read_only_enforcement_are_not_checked() {
        let mut server = FakeServer::redis_7();
        let parts = parse_command("SET session:1 value").expect("command parses");

        send_parsed_command(&mut server, &parts, ReadOnlyEnforcement::None)
            .expect("the write runs");

        assert!(server.lookups.is_empty());
        assert_eq!(server.sent, vec![parts]);
    }

    #[test]
    fn redis_declares_read_only_enforcement() {
        assert!(REDIS_METADATA.enforces_read_only());
    }
}
