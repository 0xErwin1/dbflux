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
/// Batch 1 (this change) only ever constructs `Standalone`: topology
/// detection at connect time rejects Cluster and Sentinel deployments with
/// `DbError::NotSupported` before a `RedisConnection` is built. `Cluster` and
/// `SentinelMaster` are prepared seams for the batches that add real
/// cluster/sentinel connect logic.
// `Cluster` and `SentinelMaster` are prepared seams: batch 1 only ever
// constructs `Standalone` (topology detection at connect time rejects
// Cluster/Sentinel deployments before a `RedisConnection` is built). The
// batches that add real cluster/sentinel connect logic construct them.
#[allow(dead_code)]
pub(crate) enum RedisTransport {
    Standalone(Arc<Mutex<redis::Connection>>),
    Cluster(Box<redis::cluster::ClusterConnection>),
    SentinelMaster(Arc<Mutex<redis::Connection>>),
}

impl RedisTransport {
    /// Runs `f` against whichever transport variant is active, dispatching
    /// through `redis::ConnectionLike` so query call sites stay topology-agnostic.
    pub(crate) fn with_connection_like<T>(
        &mut self,
        f: impl FnOnce(&mut dyn redis::ConnectionLike) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        match self {
            RedisTransport::Standalone(conn) | RedisTransport::SentinelMaster(conn) => {
                let mut guard = conn
                    .lock()
                    .map_err(|e| DbError::query_failed(format!("Lock error: {}", e)))?;
                f(&mut *guard)
            }
            RedisTransport::Cluster(conn) => f(conn.as_mut()),
        }
    }

    /// Returns the plain connection backing this transport, when one exists.
    ///
    /// Used by the instance-catalog and instance-metric/inspector bypasses,
    /// which are hard-typed to `Arc<Mutex<redis::Connection>>` and have no
    /// meaningful behavior against a cluster (`None` in that case).
    pub(crate) fn standalone(&self) -> Option<Arc<Mutex<redis::Connection>>> {
        match self {
            RedisTransport::Standalone(conn) | RedisTransport::SentinelMaster(conn) => {
                Some(Arc::clone(conn))
            }
            RedisTransport::Cluster(_) => None,
        }
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
}
