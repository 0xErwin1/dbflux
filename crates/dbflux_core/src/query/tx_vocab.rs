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
    /// SQL fragment to set a lock timeout, emitted inside the transaction before
    /// the DML statement. `None` when the driver does not support lock timeouts.
    pub lock_timeout_template: Option<&'static str>,
}

impl TransactionVocab {
    /// Returns the transaction vocabulary for a given database kind.
    ///
    /// Callers should retrieve this once per execution run and cache it.
    pub fn for_kind(kind: DbKind) -> Self {
        match kind {
            DbKind::Postgres => Self {
                begin: "BEGIN",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: Some("SET LOCAL lock_timeout = '{ms}ms'"),
            },
            DbKind::MySQL => Self {
                begin: "START TRANSACTION",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: Some("SET innodb_lock_wait_timeout = {seconds}"),
            },
            DbKind::SQLite => Self {
                begin: "BEGIN IMMEDIATE",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: None,
            },
            _ => Self {
                begin: "BEGIN TRANSACTION",
                commit: "COMMIT",
                rollback: "ROLLBACK",
                lock_timeout_template: Some("SET LOCK_TIMEOUT {ms}"),
            },
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
        let vocab = TransactionVocab::for_kind(DbKind::Postgres);
        assert_eq!(vocab.begin, "BEGIN");
        assert_eq!(vocab.commit, "COMMIT");
        assert_eq!(vocab.rollback, "ROLLBACK");
    }

    #[test]
    fn sqlite_uses_begin_immediate() {
        let vocab = TransactionVocab::for_kind(DbKind::SQLite);
        assert_eq!(vocab.begin, "BEGIN IMMEDIATE");
    }

    #[test]
    fn mysql_uses_start_transaction() {
        let vocab = TransactionVocab::for_kind(DbKind::MySQL);
        assert_eq!(vocab.begin, "START TRANSACTION");
    }

    #[test]
    fn sqlite_has_no_lock_timeout() {
        let vocab = TransactionVocab::for_kind(DbKind::SQLite);
        assert!(vocab.lock_timeout_template.is_none());
        assert!(vocab.lock_timeout_sql(5000).is_none());
    }

    #[test]
    fn postgres_lock_timeout_sql_formats_ms() {
        let vocab = TransactionVocab::for_kind(DbKind::Postgres);
        let sql = vocab.lock_timeout_sql(2000).unwrap();
        assert!(sql.contains("2000"), "expected ms value in sql: {}", sql);
    }
}
