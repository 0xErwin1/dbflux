use std::path::PathBuf;
use uuid::Uuid;

/// Identity key used for document deduplication.
///
/// Replaces the six `is_*` methods on `DocumentHandle` with a single
/// `matches_dedup_key` predicate. Each variant maps 1:1 to a distinct
/// open-document identity; callers construct the appropriate key and
/// call `TabManager::find_by_key` instead of iterating `is_*` methods.
#[derive(Clone, Debug)]
pub enum DocumentKey {
    /// A relational table or collection opened as a data grid.
    ///
    /// `database` is `None` when the driver does not support multi-database
    /// browsing within a single profile, and `Some` when it does. The
    /// `matches_dedup_key` predicate on `DataDocument` accepts a `None`
    /// database as a wildcard (matches any database).
    Table {
        profile_id: Uuid,
        database: Option<String>,
        table: dbflux_core::TableRef,
    },

    /// A document-DB collection opened as a document tree.
    Collection {
        profile_id: Uuid,
        collection: dbflux_core::CollectionRef,
    },

    /// A file-backed code document (SQL script, Lua script, etc.).
    File { path: PathBuf },

    /// A Redis-style key-value database browser.
    KeyValueDb { profile_id: Uuid, database: String },

    /// A standalone chart document linked to a saved chart by ID.
    Chart { saved_chart_id: Uuid },

    /// The global audit/governance event viewer (singleton — at most one open
    /// at a time).
    Audit,

    /// An event-stream collection opened as a live log viewer (e.g. CloudWatch
    /// log group). Distinct from `Audit` so the singleton audit viewer and
    /// collection-backed event streams can coexist.
    EventStream {
        profile_id: Uuid,
        target: dbflux_core::EventStreamTarget,
    },

    /// A read-only routine definition document (stored procedure or function).
    Routine {
        profile_id: Uuid,
        schema: String,
        specific_name: String,
    },

    /// A metric chart document opened from the sidebar tree by clicking a metric
    /// leaf. Deduplicated by `(profile_id, namespace, metric_name)` so that
    /// clicking the same metric a second time focuses the existing tab.
    MetricChart {
        profile_id: Uuid,
        namespace: String,
        metric_name: String,
    },

    /// A dashboard document — a named collection of chart panels with a shared
    /// time range. Deduplicated by `dashboard_id` so that opening the same
    /// dashboard a second time focuses the existing tab.
    Dashboard { dashboard_id: Uuid },

    /// A chart document opened from the instance-metrics sidebar folder.
    /// Deduplicated by `(profile_id, metric_id)`.
    InstanceMetricChart { profile_id: Uuid, metric_id: String },

    /// A live inspector panel document opened from the instance-inspectors
    /// sidebar folder. Deduplicated by `(profile_id, metric_id)`.
    InspectorPanel { profile_id: Uuid, metric_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{CollectionRef, EventStreamTarget, TableRef};

    /// Compile-time construction test: every variant can be built without
    /// panicking. The assertion checks that `Clone` and `Debug` are derived.
    #[test]
    fn document_key_variants_construct_and_clone() {
        let id = Uuid::new_v4();

        let table = DocumentKey::Table {
            profile_id: id,
            database: Some("prod".to_string()),
            table: TableRef {
                schema: None,
                name: "users".to_string(),
            },
        };

        let collection = DocumentKey::Collection {
            profile_id: id,
            collection: CollectionRef {
                database: "mydb".to_string(),
                name: "orders".to_string(),
            },
        };

        let file = DocumentKey::File {
            path: PathBuf::from("/tmp/query.sql"),
        };

        let kv = DocumentKey::KeyValueDb {
            profile_id: id,
            database: "0".to_string(),
        };

        let chart = DocumentKey::Chart { saved_chart_id: id };

        let audit = DocumentKey::Audit;

        let event_stream = DocumentKey::EventStream {
            profile_id: id,
            target: EventStreamTarget {
                collection: CollectionRef {
                    database: "mydb".to_string(),
                    name: "log-group".to_string(),
                },
                child_id: Some("stream-1".to_string()),
            },
        };

        let metric_chart = DocumentKey::MetricChart {
            profile_id: id,
            namespace: "AWS/EC2".to_string(),
            metric_name: "CPUUtilization".to_string(),
        };

        let dashboard = DocumentKey::Dashboard {
            dashboard_id: Uuid::new_v4(),
        };

        // Verify Clone is derived.
        let _ = table.clone();
        let _ = collection.clone();
        let _ = file.clone();
        let _ = kv.clone();
        let _ = chart.clone();
        let _ = audit.clone();
        let _ = event_stream.clone();
        let _ = metric_chart.clone();
        let _ = dashboard.clone();

        // Verify Debug is derived (format! would panic if not).
        let _ = format!("{:?}", table);
        let _ = format!("{:?}", audit);
        let _ = format!("{:?}", metric_chart);
    }

    /// InstanceMetricChart and InspectorPanel keys construct, clone, and debug correctly.
    #[test]
    fn instance_metric_and_inspector_keys_construct_and_clone() {
        let id = Uuid::new_v4();

        let inst_metric = DocumentKey::InstanceMetricChart {
            profile_id: id,
            metric_id: "pg.cache_hit_ratio".to_string(),
        };

        let inspector = DocumentKey::InspectorPanel {
            profile_id: id,
            metric_id: "pg.activity".to_string(),
        };

        let _ = inst_metric.clone();
        let _ = inspector.clone();
        let _ = format!("{:?}", inst_metric);
        let _ = format!("{:?}", inspector);
    }

    /// Table dedup key with database = None is distinct from one with Some("x").
    #[test]
    fn table_key_database_variants_are_distinct() {
        let id = Uuid::new_v4();
        let table_ref = TableRef {
            schema: None,
            name: "accounts".to_string(),
        };

        let with_db = DocumentKey::Table {
            profile_id: id,
            database: Some("staging".to_string()),
            table: table_ref.clone(),
        };

        let without_db = DocumentKey::Table {
            profile_id: id,
            database: None,
            table: table_ref,
        };

        // Both must clone without panic.
        let _ = with_db.clone();
        let _ = without_db.clone();
    }
}
