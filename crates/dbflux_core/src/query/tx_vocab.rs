use crate::DbKind;

/// Per-driver transaction SQL strings used by the mutation executor.
///
/// Provides the exact SQL statements to begin, commit, and rollback a transaction
/// for each supported database kind. Drivers that need special BEGIN semantics
/// (SQLite's IMMEDIATE locking, MySQL's START TRANSACTION) differ here so the
/// executor never branches on driver ids at the UI layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionVocab {
    pub begin: &'static str,
    pub commit: &'static str,
    pub rollback: &'static str,
    /// SQL fragment to set a lock timeout. `None` when the driver does not support
    /// lock timeouts.
    pub lock_timeout_template: Option<&'static str>,
    /// When `true`, the lock timeout statement must be emitted BEFORE `BEGIN`.
    ///
    /// MySQL's `SET SESSION innodb_lock_wait_timeout` must be set outside a
    /// transaction to take effect reliably across MySQL 5.7 and 8.0. PostgreSQL's
    /// `SET LOCAL lock_timeout` and MSSQL's `SET LOCK_TIMEOUT` are effective inside
    /// the transaction and must be emitted after BEGIN.
    pub lock_timeout_before_begin: bool,

    /// SQL to reset the lock timeout to its default after the run completes.
    ///
    /// Some drivers use session-scoped lock timeout settings (MySQL's
    /// `SET SESSION innodb_lock_wait_timeout`) that persist for the connection
    /// lifetime. This statement is emitted after the run concludes (success,
    /// failure, or cancel) so pooled connections don't inherit the previous
    /// timeout. `None` when the driver's timeout scope is transaction-local
    /// (Postgres, SQL Server) and resets automatically.
    pub lock_timeout_reset_sql: Option<&'static str>,
}

impl TransactionVocab {
    /// Returns the transaction vocabulary for a given SQL database kind.
    ///
    /// Returns `None` for driver kinds that do not speak SQL (MongoDB, Redis,
    /// DynamoDB, CloudWatchLogs, InfluxDB). The mutation gate upstream already
    /// blocks non-SQL drivers; this provides typed defense-in-depth.
    ///
    /// Callers should retrieve this once per execution run and cache it.
    pub fn for_kind(kind: DbKind) -> Option<Self> {
        match kind {
            DbKind::Postgres => Some(Self {
                begin: "BEGIN",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: Some("SET LOCAL lock_timeout = '{ms}ms'"),
                lock_timeout_before_begin: false,
                // Postgres uses SET LOCAL — resets automatically at transaction end.
                lock_timeout_reset_sql: None,
            }),
            DbKind::MySQL | DbKind::MariaDB => Some(Self {
                begin: "START TRANSACTION",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                // Must use SESSION scope; applies to connections, not transactions.
                // Emitted BEFORE BEGIN so it takes effect before any lock acquisition.
                lock_timeout_template: Some("SET SESSION innodb_lock_wait_timeout = {seconds}"),
                lock_timeout_before_begin: true,
                // SESSION scope persists on pooled connections; reset to default after run.
                lock_timeout_reset_sql: Some("SET SESSION innodb_lock_wait_timeout = DEFAULT"),
            }),
            DbKind::SQLite => Some(Self {
                begin: "BEGIN IMMEDIATE",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: None,
                lock_timeout_before_begin: false,
                lock_timeout_reset_sql: None,
            }),
            DbKind::SqlServer => Some(Self {
                begin: "BEGIN TRANSACTION",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: Some("SET LOCK_TIMEOUT {ms}"),
                lock_timeout_before_begin: false,
                // SQL Server's SET LOCK_TIMEOUT is connection-scoped; reset it so pooled
                // connections don't silently inherit the timeout from a previous mutation.
                lock_timeout_reset_sql: Some("SET LOCK_TIMEOUT -1"),
            }),
            DbKind::MongoDB
            | DbKind::Redis
            | DbKind::DynamoDB
            | DbKind::CloudWatchLogs
            | DbKind::InfluxDB => None,
        }
    }

    /// Formats the lock timeout SQL for a given millisecond value.
    ///
    /// Returns `None` when the driver does not support lock timeouts (MySQL
    /// converts to whole seconds; values below 1000ms round up to 1s).
    pub fn lock_timeout_sql(&self, timeout_ms: u64) -> Option<String> {
        self.lock_timeout_template.map(|template| {
            let seconds = timeout_ms.div_ceil(1000).max(1);
            template
                .replace("{ms}", &timeout_ms.to_string())
                .replace("{seconds}", &seconds.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_uses_begin_commit_rollback() {
        let vocab = TransactionVocab::for_kind(DbKind::Postgres).unwrap();
        assert_eq!(vocab.begin, "BEGIN");
        assert_eq!(vocab.commit, "COMMIT");
        assert_eq!(vocab.rollback, "ROLLBACK");
    }

    #[test]
    fn sqlite_uses_begin_immediate() {
        let vocab = TransactionVocab::for_kind(DbKind::SQLite).unwrap();
        assert_eq!(vocab.begin, "BEGIN IMMEDIATE");
    }

    #[test]
    fn mysql_uses_start_transaction() {
        let vocab = TransactionVocab::for_kind(DbKind::MySQL).unwrap();
        assert_eq!(vocab.begin, "START TRANSACTION");
    }

    #[test]
    fn sqlite_has_no_lock_timeout() {
        let vocab = TransactionVocab::for_kind(DbKind::SQLite).unwrap();
        assert!(vocab.lock_timeout_template.is_none());
        assert!(vocab.lock_timeout_sql(5000).is_none());
    }

    #[test]
    fn postgres_lock_timeout_sql_formats_ms() {
        let vocab = TransactionVocab::for_kind(DbKind::Postgres).unwrap();
        let sql = vocab.lock_timeout_sql(2000).unwrap();
        assert!(sql.contains("2000"), "expected ms value in sql: {}", sql);
    }

    #[test]
    fn non_sql_kinds_return_none() {
        assert!(TransactionVocab::for_kind(DbKind::MongoDB).is_none());
        assert!(TransactionVocab::for_kind(DbKind::Redis).is_none());
        assert!(TransactionVocab::for_kind(DbKind::DynamoDB).is_none());
        assert!(TransactionVocab::for_kind(DbKind::CloudWatchLogs).is_none());
        assert!(TransactionVocab::for_kind(DbKind::InfluxDB).is_none());
    }

    // F-R2-5: MySQL lock_timeout must be marked as before-begin so the executor emits it
    // before START TRANSACTION, not inside the transaction body.
    #[test]
    fn mysql_lock_timeout_before_begin_is_true() {
        let vocab = TransactionVocab::for_kind(DbKind::MySQL).unwrap();
        assert!(
            vocab.lock_timeout_before_begin,
            "MySQL lock_timeout must be emitted before BEGIN"
        );
        let sql = vocab.lock_timeout_sql(5000).unwrap();
        assert!(
            sql.contains("SESSION"),
            "MySQL lock_timeout must use SESSION scope; got: {}",
            sql
        );
    }

    #[test]
    fn mariadb_lock_timeout_before_begin_is_true() {
        let vocab = TransactionVocab::for_kind(DbKind::MariaDB).unwrap();
        assert!(vocab.lock_timeout_before_begin);
    }

    #[test]
    fn postgres_lock_timeout_before_begin_is_false() {
        let vocab = TransactionVocab::for_kind(DbKind::Postgres).unwrap();
        assert!(
            !vocab.lock_timeout_before_begin,
            "Postgres lock_timeout must be emitted INSIDE the transaction"
        );
    }

    #[test]
    fn sqlserver_lock_timeout_before_begin_is_false() {
        let vocab = TransactionVocab::for_kind(DbKind::SqlServer).unwrap();
        assert!(
            !vocab.lock_timeout_before_begin,
            "MSSQL lock_timeout must be emitted INSIDE the transaction"
        );
    }
}
