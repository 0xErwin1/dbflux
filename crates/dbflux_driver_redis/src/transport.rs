use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use dbflux_core::DbError;

/// The live connection backing a `RedisConnection`.
///
/// `Standalone` and `SentinelMaster` both wrap a single `redis::Connection` in
/// its own `Arc<Mutex<..>>` so the instance-catalog and instance-metric/
/// inspector bypasses (`Connection::instance_catalog`, `Connection::execute`)
/// can share that connection directly without going through `ConnectionLike`
/// dispatch. `Cluster` has no single connection to hand out; `standalone()`
/// returns `None` for it.
///
/// `SentinelMaster` also carries the `SentinelClient` (`resolver`) used to
/// resolve the master, so `with_connection_like` can re-resolve and swap in a
/// fresh connection after a failover instead of failing outright. Those
/// instance-catalog/metric bypasses go through `standalone()` directly and do
/// NOT get failover retry — see the batch 3 report for that known gap.
pub(crate) enum RedisTransport {
    Standalone(Arc<Mutex<redis::Connection>>),
    Cluster(Box<redis::cluster::ClusterConnection>),
    SentinelMaster {
        connection: Arc<Mutex<redis::Connection>>,
        resolver: Mutex<redis::sentinel::SentinelClient>,
    },
}

impl RedisTransport {
    /// Runs `f` against whichever transport variant is active, dispatching
    /// through `redis::ConnectionLike` so query call sites stay topology-agnostic.
    ///
    /// `f` must be callable more than once (`Fn`, not `FnOnce`): the
    /// `SentinelMaster` arm retries it exactly once, against a freshly
    /// re-resolved master connection, when the first attempt fails with a
    /// connection-class error (see `is_connection_level_error`).
    pub(crate) fn with_connection_like<T>(
        &mut self,
        f: impl Fn(&mut dyn redis::ConnectionLike) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        match self {
            RedisTransport::Standalone(conn) => {
                let mut guard = conn
                    .lock()
                    .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;
                f(&mut *guard)
            }
            RedisTransport::Cluster(conn) => f(conn.as_mut()),
            RedisTransport::SentinelMaster {
                connection,
                resolver,
            } => run_sentinel_master_command(connection, resolver, f),
        }
    }

    /// Returns the plain connection backing this transport, when one exists.
    ///
    /// Used by the instance-catalog and instance-metric/inspector bypasses,
    /// which are hard-typed to `Arc<Mutex<redis::Connection>>` and have no
    /// meaningful behavior against a cluster (`None` in that case).
    pub(crate) fn standalone(&self) -> Option<Arc<Mutex<redis::Connection>>> {
        match self {
            RedisTransport::Standalone(conn) => Some(Arc::clone(conn)),
            RedisTransport::SentinelMaster { connection, .. } => Some(Arc::clone(connection)),
            RedisTransport::Cluster(_) => None,
        }
    }
}

/// Runs `f` against the current Sentinel master connection, retrying exactly
/// once against a freshly re-resolved master when the first attempt fails
/// with a connection-class error.
///
/// A second failure (whether from the retried command or from re-resolution
/// itself) propagates without a further attempt — this is one re-resolve and
/// one retry, not a generic retry loop.
fn run_sentinel_master_command<T>(
    connection: &Arc<Mutex<redis::Connection>>,
    resolver: &Mutex<redis::sentinel::SentinelClient>,
    f: impl Fn(&mut dyn redis::ConnectionLike) -> Result<T, DbError>,
) -> Result<T, DbError> {
    let first_error = {
        let mut guard = connection
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        match f(&mut *guard) {
            Ok(value) => return Ok(value),
            Err(error) => error,
        }
    };

    if !should_retry_sentinel_command(&first_error) {
        return Err(first_error);
    }

    let fresh_connection = {
        let mut resolver_guard = resolver
            .lock()
            .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;

        resolver_guard.get_connection().map_err(|e| {
            DbError::query_failed(format!(
                "Redis Sentinel failover: could not re-resolve the master: {e}"
            ))
        })?
    };

    let mut guard = connection
        .lock()
        .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;
    *guard = fresh_connection;
    f(&mut *guard)
}

/// Decides whether a failed Sentinel-master command is worth retrying once
/// against a freshly re-resolved master.
///
/// Only a connection-class `DbError` (populated from a connection-class
/// `redis::RedisError` — see `is_connection_level_error`) triggers a retry.
/// A query-level error (bad command, wrong type, syntax) is not a failover
/// symptom and must propagate immediately.
pub(crate) fn should_retry_sentinel_command(error: &DbError) -> bool {
    matches!(error, DbError::ConnectionFailed(_))
}

/// Classifies a `redis::RedisError` as connection-class (worth reconnecting
/// over) versus a query-level error that a fresh connection would not fix.
///
/// Delegates to `RedisError::is_unrecoverable_error()`, which the crate
/// already derives from the error's `RetryMethod` (IO errors, dropped
/// connections, and similar transport failures map to `Reconnect` /
/// `ReconnectFromInitialConnections`; ordinary command-level errors do not).
pub(crate) fn is_connection_level_error(error: &redis::RedisError) -> bool {
    error.is_unrecoverable_error()
}

/// The sanity outcome of a `ROLE` check run against a freshly Sentinel-resolved
/// master connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MasterRoleSanity {
    /// The connection confirmed it is a master.
    Verified,
    /// `ROLE` failed or is unsupported by this server; some managed providers
    /// reject it. Tolerated as "cannot verify" rather than an error, mirroring
    /// `classify_role_reply`'s handling of the same case at connect time.
    CannotVerify,
    /// The connection reported it is a replica — Sentinel resolved a stale
    /// address, most likely because a failover is still in progress.
    ResolvedReplica,
}

/// Evaluates a `ROLE` reply against a Sentinel-resolved master connection.
///
/// Unlike `classify_role_reply` (used at connect time to distinguish a
/// Sentinel node from a regular one), this check already knows it is talking
/// to what Sentinel claims is the master: any error is tolerated as
/// unverifiable, and only an explicit replica reply is treated as a problem.
pub(crate) fn evaluate_master_role_sanity(
    reply: &Result<redis::Role, redis::RedisError>,
) -> MasterRoleSanity {
    match reply {
        Ok(redis::Role::Replica { .. }) => MasterRoleSanity::ResolvedReplica,
        Ok(_) => MasterRoleSanity::Verified,
        Err(_) => MasterRoleSanity::CannotVerify,
    }
}

/// The Redis deployment topology detected via `ROLE` and `INFO cluster` at
/// connect time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopologyProbe {
    Standalone,
    Cluster,
    SentinelService,
}

/// The deployment topology explicitly configured on `DbConfig::Redis::topology`.
///
/// `Standalone` means "no explicit mode": the driver falls back to the
/// historical detect-then-branch behavior (`ROLE` / `INFO cluster` probes).
/// `Cluster` and `Sentinel` are explicit and skip those probes entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfiguredTopology {
    Standalone,
    Cluster,
    Sentinel,
}

/// Parses `DbConfig::Redis::topology` into a `ConfiguredTopology`.
///
/// `None` or an empty string means "standalone with detection" (today's
/// behavior, and the only mode existing saved profiles have). Any other value
/// must be exactly `"standalone"`, `"cluster"`, or `"sentinel"`; anything else
/// is a clear, rejected configuration error rather than a silent fallback.
pub(crate) fn parse_configured_topology(raw: Option<&str>) -> Result<ConfiguredTopology, DbError> {
    match raw.map(str::trim) {
        None | Some("") => Ok(ConfiguredTopology::Standalone),
        Some("standalone") => Ok(ConfiguredTopology::Standalone),
        Some("cluster") => Ok(ConfiguredTopology::Cluster),
        Some("sentinel") => Ok(ConfiguredTopology::Sentinel),
        Some(other) => Err(DbError::InvalidProfile(format!(
            "Unknown Redis topology '{other}'; expected 'standalone', 'cluster', or 'sentinel'"
        ))),
    }
}

/// The classification of a `ROLE` reply, used to decide whether the node is
/// a Sentinel before falling through to the `INFO cluster` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoleClassification {
    /// The node identified itself as a Sentinel.
    Sentinel,
    /// The node is a regular master/replica, or replied with an error that
    /// indicates `ROLE` itself is unsupported (not that the transport failed).
    NotSentinel,
    /// A genuine transport/IO error occurred; detection cannot proceed.
    Aborted,
}

/// Classifies a `ROLE` command reply/error.
///
/// Some managed Redis providers reject `ROLE` with an "unknown command"
/// style error instead of answering it. That case must NOT abort detection:
/// it means "this is not a Sentinel", not "the connection is broken", so
/// detection falls through to the `INFO cluster` check. Only a genuine
/// transport/IO error (connection refused, reset, timeout, ...) aborts.
pub(crate) fn classify_role_reply(
    reply: &Result<redis::Role, redis::RedisError>,
) -> RoleClassification {
    match reply {
        Ok(redis::Role::Sentinel { .. }) => RoleClassification::Sentinel,
        Ok(_) => RoleClassification::NotSentinel,
        Err(error) if is_unknown_command_error(error) => RoleClassification::NotSentinel,
        Err(_) => RoleClassification::Aborted,
    }
}

fn is_unknown_command_error(error: &redis::RedisError) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("unknown command")
}

/// Parses `cluster_enabled:<0|1>` out of an `INFO cluster` response.
///
/// Returns `false` when the field is absent or malformed, treating those
/// cases as "not a cluster" rather than failing detection outright.
pub(crate) fn parse_cluster_enabled(info_text: &str) -> bool {
    info_text
        .lines()
        .find_map(|line| line.strip_prefix("cluster_enabled:"))
        .map(|value| value.trim() == "1")
        .unwrap_or(false)
}

/// Redis Cluster only ever exposes database 0 — `SELECT` to any other index
/// fails on a real cluster. Rejects an explicitly requested nonzero database
/// instead of silently running the request against db 0.
pub(crate) fn validate_cluster_database(database: Option<u32>) -> Result<(), DbError> {
    match database {
        None | Some(0) => Ok(()),
        Some(other) => Err(DbError::NotSupported(format!(
            "Redis Cluster only supports database 0; requested database index {other}"
        ))),
    }
}

/// Parses a `CLUSTER SLOTS` reply into the unique set of master `(host,
/// port)` pairs backing the cluster, preserving first-seen order.
///
/// The reply is an array of slot ranges, each shaped as
/// `[start_slot, end_slot, [master_ip, master_port, node_id, ...], replica...]`.
/// Multiple ranges commonly share the same master; those are deduplicated.
pub(crate) fn parse_cluster_slots_masters(
    value: &redis::Value,
) -> Result<Vec<(String, u16)>, DbError> {
    let redis::Value::Array(ranges) = value else {
        return Err(DbError::query_failed(
            "Unexpected CLUSTER SLOTS reply: expected an array of slot ranges".to_string(),
        ));
    };

    let mut masters = Vec::new();
    let mut seen = HashSet::new();

    for range in ranges {
        let redis::Value::Array(fields) = range else {
            return Err(DbError::query_failed(
                "Unexpected CLUSTER SLOTS reply: expected each slot range to be an array"
                    .to_string(),
            ));
        };

        let master_descriptor = fields.get(2).ok_or_else(|| {
            DbError::query_failed(
                "Unexpected CLUSTER SLOTS reply: slot range is missing the master descriptor"
                    .to_string(),
            )
        })?;

        let (host, port) = parse_cluster_node_descriptor(master_descriptor)?;

        if seen.insert(format!("{host}:{port}")) {
            masters.push((host, port));
        }
    }

    Ok(masters)
}

fn parse_cluster_node_descriptor(value: &redis::Value) -> Result<(String, u16), DbError> {
    let redis::Value::Array(parts) = value else {
        return Err(DbError::query_failed(
            "Unexpected CLUSTER SLOTS reply: expected the node descriptor to be an array"
                .to_string(),
        ));
    };

    let host = match parts.first() {
        Some(redis::Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
        Some(redis::Value::SimpleString(text)) => text.clone(),
        _ => {
            return Err(DbError::query_failed(
                "Unexpected CLUSTER SLOTS reply: node descriptor is missing a host".to_string(),
            ));
        }
    };

    let port = match parts.get(1) {
        Some(redis::Value::Int(port)) => u16::try_from(*port).map_err(|_| {
            DbError::query_failed(
                "Unexpected CLUSTER SLOTS reply: node descriptor has an invalid port".to_string(),
            )
        })?,
        _ => {
            return Err(DbError::query_failed(
                "Unexpected CLUSTER SLOTS reply: node descriptor is missing a port".to_string(),
            ));
        }
    };

    Ok((host, port))
}

/// Parses a comma-separated `host:port` node list.
///
/// Shared by both explicit topologies that carry an `additional_nodes` field
/// on `DbConfig::Redis`: as extra Cluster seed nodes (alongside the primary
/// host/port `ClusterClientBuilder` already connects with) and as extra
/// Sentinel nodes (alongside the primary host/port, which together form the
/// Sentinel node set).
pub(crate) fn parse_node_list(raw: &str) -> Result<Vec<(String, u16)>, DbError> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(split_host_port)
        .collect()
}

/// Splits a `"<host>:<port>"` address at the last colon, so a host containing
/// colons (an IPv6 literal without brackets) round-trips correctly.
pub(crate) fn split_host_port(address: &str) -> Result<(String, u16), DbError> {
    let (host, port) = address.rsplit_once(':').ok_or_else(|| {
        DbError::InvalidProfile(format!("Invalid Redis Cluster node address '{address}'"))
    })?;

    let port = port.parse::<u16>().map_err(|_| {
        DbError::InvalidProfile(format!("Invalid Redis Cluster node port in '{address}'"))
    })?;

    Ok((host.to_string(), port))
}

/// Per-node `SCAN` cursor state for a Redis Cluster scan page.
///
/// Encodes into the opaque `Option<String>` cursor `KeyScanPage` round-trips
/// through the UI as a JSON object mapping `"<host>:<port>"` to the pending
/// per-node `SCAN` cursor. A node is dropped from the map once its cursor
/// reaches 0 (exhausted); the overall scan is exhausted once the map is
/// empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ClusterScanCursor {
    pending: BTreeMap<String, u64>,
}

impl ClusterScanCursor {
    /// Starts a fresh scan: every discovered master begins at cursor 0.
    pub(crate) fn fresh(masters: &[(String, u16)]) -> Self {
        Self {
            pending: masters
                .iter()
                .map(|(host, port)| (format!("{host}:{port}"), 0))
                .collect(),
        }
    }

    /// Decodes a cursor previously returned by `encode`.
    pub(crate) fn decode(raw: &str) -> Result<Self, DbError> {
        let parsed: BTreeMap<String, String> = serde_json::from_str(raw).map_err(|error| {
            DbError::InvalidProfile(format!("Invalid Redis Cluster scan cursor: {error}"))
        })?;

        let mut pending = BTreeMap::new();
        for (address, cursor) in parsed {
            let cursor = cursor.parse::<u64>().map_err(|_| {
                DbError::InvalidProfile(format!(
                    "Invalid Redis Cluster scan cursor value for node '{address}'"
                ))
            })?;
            pending.insert(address, cursor);
        }

        Ok(Self { pending })
    }

    /// Encodes the pending per-node cursors, or `None` once every node is
    /// exhausted (mirrors the standalone `SCAN` cursor's "0 means done").
    pub(crate) fn encode(&self) -> Option<String> {
        if self.pending.is_empty() {
            return None;
        }

        let as_strings: BTreeMap<&String, String> = self
            .pending
            .iter()
            .map(|(address, cursor)| (address, cursor.to_string()))
            .collect();

        // Only fails on non-UTF8 map keys, which cannot occur here.
        serde_json::to_string(&as_strings).ok()
    }

    /// Drops nodes that no longer appear in the current master list.
    ///
    /// A master that left the cluster mid-scan cannot be resumed; its
    /// pending page is abandoned so the scan can still complete against the
    /// remaining nodes instead of stalling on a node that is gone.
    pub(crate) fn retain_known_nodes(&mut self, known_addresses: &HashSet<String>) {
        self.pending
            .retain(|address, _| known_addresses.contains(address));
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) fn addresses(&self) -> Vec<String> {
        self.pending.keys().cloned().collect()
    }

    pub(crate) fn cursor_for(&self, address: &str) -> u64 {
        self.pending.get(address).copied().unwrap_or(0)
    }

    /// Records a node's `SCAN` reply cursor, dropping the node once it
    /// reports 0 (exhausted).
    pub(crate) fn record_result(&mut self, address: &str, next_cursor: u64) {
        if next_cursor == 0 {
            self.pending.remove(address);
        } else {
            self.pending.insert(address.to_string(), next_cursor);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn unknown_command_error() -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            "An error was signalled by the server",
            "unknown command 'ROLE', with args beginning with: ".to_string(),
        ))
    }

    fn connection_refused_error() -> redis::RedisError {
        redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionRefused))
    }

    #[test]
    fn classify_role_reply_detects_master() {
        let reply = Ok(redis::Role::Primary {
            replication_offset: 0,
            replicas: Vec::new(),
        });
        assert_eq!(classify_role_reply(&reply), RoleClassification::NotSentinel);
    }

    #[test]
    fn classify_role_reply_detects_replica() {
        let reply = Ok(redis::Role::Replica {
            primary_ip: "127.0.0.1".to_string(),
            primary_port: 6379,
            replication_state: "connected".to_string(),
            data_received: 0,
        });
        assert_eq!(classify_role_reply(&reply), RoleClassification::NotSentinel);
    }

    #[test]
    fn classify_role_reply_detects_sentinel() {
        let reply = Ok(redis::Role::Sentinel {
            primary_names: vec!["mymaster".to_string()],
        });
        assert_eq!(classify_role_reply(&reply), RoleClassification::Sentinel);
    }

    #[test]
    fn classify_role_reply_treats_unknown_command_as_not_sentinel() {
        let reply = Err(unknown_command_error());
        assert_eq!(classify_role_reply(&reply), RoleClassification::NotSentinel);
    }

    #[test]
    fn classify_role_reply_aborts_on_connection_refused() {
        let reply = Err(connection_refused_error());
        assert_eq!(classify_role_reply(&reply), RoleClassification::Aborted);
    }

    #[test]
    fn parse_cluster_enabled_true_when_present_and_one() {
        let info = "# Cluster\r\ncluster_enabled:1\r\n";
        assert!(parse_cluster_enabled(info));
    }

    #[test]
    fn parse_cluster_enabled_false_when_present_and_zero() {
        let info = "# Cluster\r\ncluster_enabled:0\r\n";
        assert!(!parse_cluster_enabled(info));
    }

    #[test]
    fn parse_cluster_enabled_false_when_absent() {
        let info = "# Server\r\nredis_version:7.2.0\r\n";
        assert!(!parse_cluster_enabled(info));
    }

    #[test]
    fn parse_cluster_enabled_false_when_malformed() {
        let info = "# Cluster\r\ncluster_enabled_extra:1\r\ncluster_enabled:not-a-number\r\n";
        assert!(!parse_cluster_enabled(info));
    }

    #[test]
    fn validate_cluster_database_accepts_none_and_zero() {
        assert!(validate_cluster_database(None).is_ok());
        assert!(validate_cluster_database(Some(0)).is_ok());
    }

    #[test]
    fn validate_cluster_database_rejects_nonzero() {
        let error = validate_cluster_database(Some(3)).unwrap_err();
        assert!(matches!(error, DbError::NotSupported(_)));
    }

    fn node_descriptor(ip: &str, port: i64) -> redis::Value {
        redis::Value::Array(vec![
            redis::Value::BulkString(ip.as_bytes().to_vec()),
            redis::Value::Int(port),
            redis::Value::BulkString(b"node-id".to_vec()),
        ])
    }

    fn slot_range(
        start: i64,
        end: i64,
        master: redis::Value,
        replicas: Vec<redis::Value>,
    ) -> redis::Value {
        let mut fields = vec![redis::Value::Int(start), redis::Value::Int(end), master];
        fields.extend(replicas);
        redis::Value::Array(fields)
    }

    #[test]
    fn parse_cluster_slots_masters_empty_reply() {
        let reply = redis::Value::Array(vec![]);
        assert_eq!(parse_cluster_slots_masters(&reply).unwrap(), Vec::new());
    }

    #[test]
    fn parse_cluster_slots_masters_single_range() {
        let reply = redis::Value::Array(vec![slot_range(
            0,
            5460,
            node_descriptor("10.0.0.1", 6379),
            vec![node_descriptor("10.0.0.2", 6379)],
        )]);

        assert_eq!(
            parse_cluster_slots_masters(&reply).unwrap(),
            vec![("10.0.0.1".to_string(), 6379)]
        );
    }

    #[test]
    fn parse_cluster_slots_masters_dedupes_shared_master_across_ranges() {
        let reply = redis::Value::Array(vec![
            slot_range(0, 100, node_descriptor("10.0.0.1", 6379), vec![]),
            slot_range(101, 200, node_descriptor("10.0.0.1", 6379), vec![]),
            slot_range(201, 300, node_descriptor("10.0.0.3", 6379), vec![]),
        ]);

        assert_eq!(
            parse_cluster_slots_masters(&reply).unwrap(),
            vec![
                ("10.0.0.1".to_string(), 6379),
                ("10.0.0.3".to_string(), 6379)
            ]
        );
    }

    #[test]
    fn parse_cluster_slots_masters_handles_ipv6_host() {
        let reply = redis::Value::Array(vec![slot_range(
            0,
            16383,
            node_descriptor("::1", 6379),
            vec![],
        )]);

        assert_eq!(
            parse_cluster_slots_masters(&reply).unwrap(),
            vec![("::1".to_string(), 6379)]
        );
    }

    #[test]
    fn parse_cluster_slots_masters_rejects_unexpected_shape() {
        let reply = redis::Value::BulkString(b"not an array".to_vec());
        assert!(parse_cluster_slots_masters(&reply).is_err());
    }

    #[test]
    fn parse_cluster_slots_masters_rejects_range_missing_master() {
        let reply = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(0),
            redis::Value::Int(100),
        ])]);
        assert!(parse_cluster_slots_masters(&reply).is_err());
    }

    #[test]
    fn parse_node_list_parses_multiple_entries() {
        let parsed = parse_node_list("10.0.0.1:6379, 10.0.0.2:6380").unwrap();
        assert_eq!(
            parsed,
            vec![
                ("10.0.0.1".to_string(), 6379),
                ("10.0.0.2".to_string(), 6380)
            ]
        );
    }

    #[test]
    fn parse_node_list_skips_empty_entries() {
        let parsed = parse_node_list("10.0.0.1:6379,,  ").unwrap();
        assert_eq!(parsed, vec![("10.0.0.1".to_string(), 6379)]);
    }

    #[test]
    fn parse_node_list_rejects_malformed_entry() {
        assert!(parse_node_list("10.0.0.1").is_err());
        assert!(parse_node_list("10.0.0.1:not-a-port").is_err());
    }

    #[test]
    fn split_host_port_handles_ipv6_host() {
        let (host, port) = split_host_port("::1:6379").unwrap();
        assert_eq!(host, "::1");
        assert_eq!(port, 6379);
    }

    #[test]
    fn split_host_port_rejects_missing_port() {
        assert!(split_host_port("10.0.0.1").is_err());
    }

    #[test]
    fn cluster_scan_cursor_fresh_starts_every_master_at_zero() {
        let masters = vec![
            ("10.0.0.1".to_string(), 6379),
            ("10.0.0.2".to_string(), 6379),
        ];
        let cursor = ClusterScanCursor::fresh(&masters);

        assert_eq!(cursor.cursor_for("10.0.0.1:6379"), 0);
        assert_eq!(cursor.cursor_for("10.0.0.2:6379"), 0);
        assert!(!cursor.is_exhausted());
    }

    #[test]
    fn cluster_scan_cursor_encode_decode_round_trip() {
        let masters = vec![("10.0.0.1".to_string(), 6379)];
        let mut cursor = ClusterScanCursor::fresh(&masters);
        cursor.record_result("10.0.0.1:6379", 42);

        let encoded = cursor.encode().unwrap();
        let decoded = ClusterScanCursor::decode(&encoded).unwrap();

        assert_eq!(decoded.cursor_for("10.0.0.1:6379"), 42);
    }

    #[test]
    fn cluster_scan_cursor_encode_none_once_fully_exhausted() {
        let masters = vec![("10.0.0.1".to_string(), 6379)];
        let mut cursor = ClusterScanCursor::fresh(&masters);
        cursor.record_result("10.0.0.1:6379", 0);

        assert!(cursor.is_exhausted());
        assert_eq!(cursor.encode(), None);
    }

    #[test]
    fn cluster_scan_cursor_partial_exhaustion_keeps_only_pending_nodes() {
        let masters = vec![
            ("10.0.0.1".to_string(), 6379),
            ("10.0.0.2".to_string(), 6379),
        ];
        let mut cursor = ClusterScanCursor::fresh(&masters);
        cursor.record_result("10.0.0.1:6379", 0);
        cursor.record_result("10.0.0.2:6379", 7);

        assert!(!cursor.is_exhausted());
        assert_eq!(cursor.addresses(), vec!["10.0.0.2:6379".to_string()]);
    }

    #[test]
    fn cluster_scan_cursor_decode_rejects_malformed_json() {
        assert!(ClusterScanCursor::decode("not json").is_err());
    }

    #[test]
    fn cluster_scan_cursor_decode_rejects_non_numeric_cursor_value() {
        let raw = r#"{"10.0.0.1:6379":"not-a-number"}"#;
        assert!(ClusterScanCursor::decode(raw).is_err());
    }

    #[test]
    fn cluster_scan_cursor_retain_known_nodes_drops_vanished_master() {
        let masters = vec![
            ("10.0.0.1".to_string(), 6379),
            ("10.0.0.2".to_string(), 6379),
        ];
        let mut cursor = ClusterScanCursor::fresh(&masters);

        let known: HashSet<String> = ["10.0.0.1:6379".to_string()].into_iter().collect();
        cursor.retain_known_nodes(&known);

        assert_eq!(cursor.addresses(), vec!["10.0.0.1:6379".to_string()]);
    }

    #[test]
    fn parse_configured_topology_defaults_to_standalone() {
        assert_eq!(
            parse_configured_topology(None).unwrap(),
            ConfiguredTopology::Standalone
        );
        assert_eq!(
            parse_configured_topology(Some("")).unwrap(),
            ConfiguredTopology::Standalone
        );
    }

    #[test]
    fn parse_configured_topology_accepts_known_values() {
        assert_eq!(
            parse_configured_topology(Some("standalone")).unwrap(),
            ConfiguredTopology::Standalone
        );
        assert_eq!(
            parse_configured_topology(Some("cluster")).unwrap(),
            ConfiguredTopology::Cluster
        );
        assert_eq!(
            parse_configured_topology(Some("sentinel")).unwrap(),
            ConfiguredTopology::Sentinel
        );
    }

    #[test]
    fn parse_configured_topology_rejects_unknown_value() {
        let error = parse_configured_topology(Some("replica-set")).unwrap_err();
        assert!(matches!(error, DbError::InvalidProfile(_)));
    }

    #[test]
    fn is_connection_level_error_true_for_connection_refused() {
        assert!(is_connection_level_error(&connection_refused_error()));
    }

    #[test]
    fn is_connection_level_error_true_for_dropped_connection() {
        let error = redis::RedisError::from(io::Error::from(io::ErrorKind::ConnectionReset));
        assert!(is_connection_level_error(&error));
    }

    #[test]
    fn is_connection_level_error_false_for_response_error() {
        assert!(!is_connection_level_error(&unknown_command_error()));
    }

    #[test]
    fn should_retry_sentinel_command_true_for_connection_failed() {
        let error = DbError::ConnectionFailed("boom".to_string().into());
        assert!(should_retry_sentinel_command(&error));
    }

    #[test]
    fn should_retry_sentinel_command_false_for_query_failed() {
        let error = DbError::QueryFailed("boom".to_string().into());
        assert!(!should_retry_sentinel_command(&error));
    }

    #[test]
    fn evaluate_master_role_sanity_verified_for_primary() {
        let reply = Ok(redis::Role::Primary {
            replication_offset: 0,
            replicas: Vec::new(),
        });
        assert_eq!(
            evaluate_master_role_sanity(&reply),
            MasterRoleSanity::Verified
        );
    }

    #[test]
    fn evaluate_master_role_sanity_detects_resolved_replica() {
        let reply = Ok(redis::Role::Replica {
            primary_ip: "127.0.0.1".to_string(),
            primary_port: 6379,
            replication_state: "connected".to_string(),
            data_received: 0,
        });
        assert_eq!(
            evaluate_master_role_sanity(&reply),
            MasterRoleSanity::ResolvedReplica
        );
    }

    #[test]
    fn evaluate_master_role_sanity_tolerates_any_error_as_unverifiable() {
        assert_eq!(
            evaluate_master_role_sanity(&Err(unknown_command_error())),
            MasterRoleSanity::CannotVerify
        );
        assert_eq!(
            evaluate_master_role_sanity(&Err(connection_refused_error())),
            MasterRoleSanity::CannotVerify
        );
    }
}
