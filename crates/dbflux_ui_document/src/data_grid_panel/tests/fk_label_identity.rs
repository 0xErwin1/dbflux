//! FK label identity across referenced columns: `users.id = 7` and
//! `users.legacy_id = 7` can be different rows (`Alice` and `Bob`), so the
//! referenced column is part of the identity of the grid's display cache,
//! the lookup dedup and the row inspector's label resolution — while the
//! cells keep carrying the typed raw id for copy and edit.

use super::*;
use crate::data_grid_panel::row_inspector::{
    FkReference, InspectorCell, InspectorSnapshot, ReferenceKind, RowInspectorContent,
    resolve_outgoing_labels,
};
use dbflux_core::Value;
use std::collections::HashMap;

/// A local `public.accounts` page whose two foreign keys point at different
/// columns of `public.users`, with both rows referencing the same id 7:
/// `user_id → users.id` (row `id = 7`, named Alice) and
/// `referrer_legacy → users.legacy_id` (row `legacy_id = 7`, named Bob).
fn accounts_result() -> QueryResult {
    let column = |name: &str, is_primary_key: bool| ColumnMeta {
        name: name.to_string(),
        type_name: "int4".to_string(),
        kind: ColumnKind::Integer,
        nullable: false,
        is_primary_key,
    };

    QueryResult::table(
        vec![
            column("id", true),
            column("user_id", false),
            column("referrer_legacy", false),
        ],
        vec![
            vec![Value::Int(1), Value::Int(7), Value::Int(7)],
            vec![Value::Int(2), Value::Int(7), Value::Int(7)],
        ],
        None,
        Duration::ZERO,
    )
}

fn users_foreign_key(column: &str, referenced_column: &str) -> dbflux_core::SchemaForeignKeyInfo {
    dbflux_core::SchemaForeignKeyInfo {
        name: format!("accounts_{column}_fk"),
        table_name: "accounts".to_string(),
        columns: vec![column.to_string()],
        referenced_schema: Some("public".to_string()),
        referenced_table: "users".to_string(),
        referenced_columns: vec![referenced_column.to_string()],
        on_delete: None,
        on_update: None,
    }
}

/// `public.users`: the `id` row 7 is Alice, the `legacy_id` row 7 is Bob,
/// and `name` is the display column the resolution picks.
fn users_table_info() -> dbflux_core::TableInfo {
    let column = |name: &str, type_name: &str, is_primary_key: bool| dbflux_core::ColumnInfo {
        name: name.to_string(),
        type_name: type_name.to_string(),
        nullable: true,
        is_primary_key,
        default_value: None,
        enum_values: None,
    };

    dbflux_core::TableInfo {
        name: "users".to_string(),
        schema: Some("public".to_string()),
        columns: Some(vec![
            column("id", "int4", true),
            column("legacy_id", "int8", false),
            column("name", "text", false),
        ]),
        indexes: None,
        foreign_keys: None,
        constraints: None,
        sample_fields: None,
        presentation: Default::default(),
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

/// `public.accounts` with the two foreign keys the inspector lists, so
/// `build_fk_references` sees both directions into `users`.
fn accounts_table_info() -> dbflux_core::TableInfo {
    let fk = |column: &str, referenced_column: &str| dbflux_core::ForeignKeyInfo {
        name: format!("accounts_{column}_fk"),
        columns: vec![column.to_string()],
        referenced_table: "users".to_string(),
        referenced_schema: Some("public".to_string()),
        referenced_columns: vec![referenced_column.to_string()],
        on_delete: None,
        on_update: None,
    };

    let mut info = users_table_info();
    info.name = "accounts".to_string();
    info.columns = Some(vec![
        dbflux_core::ColumnInfo {
            name: "id".to_string(),
            type_name: "int4".to_string(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            enum_values: None,
        },
        dbflux_core::ColumnInfo {
            name: "user_id".to_string(),
            type_name: "int4".to_string(),
            nullable: false,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        },
        dbflux_core::ColumnInfo {
            name: "referrer_legacy".to_string(),
            type_name: "int4".to_string(),
            nullable: false,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        },
    ]);
    info.foreign_keys = Some(vec![
        fk("user_id", "id"),
        fk("referrer_legacy", "legacy_id"),
    ]);
    info
}

/// One `fetch_rows_by_pks` call the stub answered: the referenced column it
/// batched over and the distinct ids it was asked for.
#[derive(Debug, Clone, PartialEq)]
struct RecordedBatch {
    pk_column: String,
    values: Vec<Value>,
}

/// Connection stub answering the users rows: `id` 7 is Alice, `legacy_id` 7
/// is Bob. Every batch and every per-row fetch is recorded, so the dedup of
/// the lookups is observable without a live database.
#[derive(Default)]
struct UsersIdentityConnection {
    batches: std::sync::Mutex<Vec<RecordedBatch>>,
    row_fetches: std::sync::atomic::AtomicUsize,
}

impl UsersIdentityConnection {
    fn batches(&self) -> Vec<RecordedBatch> {
        self.batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn row_fetches(&self) -> usize {
        self.row_fetches.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The row the referenced column names for value 7.
    fn referenced_row(pk_column: &str, value: &Value) -> std::collections::HashMap<String, Value> {
        let label = match pk_column {
            "id" => "Alice",
            "legacy_id" => "Bob",
            _ => return Default::default(),
        };

        std::collections::HashMap::from([
            (pk_column.to_string(), value.clone()),
            ("name".to_string(), Value::Text(label.to_string())),
        ])
    }
}

impl dbflux_core::Connection for UsersIdentityConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static META: std::sync::OnceLock<dbflux_core::DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| {
            dbflux_core::DriverMetadataBuilder::new(
                "stub-users-identity",
                "Stub",
                dbflux_core::DatabaseCategory::Relational,
                dbflux_core::QueryLanguage::Sql,
            )
            .build()
        })
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: the fk identity path never asks for a dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("UsersIdentityConnection::dialect not needed")
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(&self, _: &dbflux_core::QueryRequest) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn fetch_rows_by_pks(
        &self,
        _: &str,
        _: &str,
        _: &str,
        pk_column: &str,
        values: &[Value],
        _projection: Option<&[String]>,
    ) -> Result<Vec<std::collections::HashMap<String, Value>>, dbflux_core::DbError> {
        if let Ok(mut batches) = self.batches.lock() {
            batches.push(RecordedBatch {
                pk_column: pk_column.to_string(),
                values: values.to_vec(),
            });
        }

        Ok(values
            .iter()
            .map(|value| Self::referenced_row(pk_column, value))
            .collect())
    }

    fn fetch_row_by_pk(
        &self,
        _: &str,
        _: &str,
        _: &str,
        pk_column: &str,
        pk_value: &Value,
    ) -> Result<Option<std::collections::HashMap<String, Value>>, dbflux_core::DbError> {
        self.row_fetches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(Some(Self::referenced_row(pk_column, pk_value)))
    }
}

/// Caches the users and accounts details both resolution directions read.
fn cache_identity_details(
    cx: &mut TestAppContext,
    app_state: &gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
) {
    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            app.set_table_details(
                profile_id,
                "app".to_string(),
                Some("public".to_string()),
                "users".to_string(),
                users_table_info(),
            );
            app.set_table_details(
                profile_id,
                "app".to_string(),
                Some("public".to_string()),
                "accounts".to_string(),
                accounts_table_info(),
            );
        });
    });
}

/// A grid over `public.accounts` with both foreign keys ready, built the way
/// a landed page leaves the panel.
fn accounts_panel(
    cx: &mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
) -> (gpui::Entity<DataGridPanel>, &mut gpui::VisualTestContext) {
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("app".to_string()),
                table: dbflux_core::TableRef::with_schema("public", "accounts"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(2),
            };

            let mut panel =
                DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx);
            panel.result = accounts_result();
            panel.builder.fk_cache = super::super::FkLoadState::Ready(vec![
                users_foreign_key("user_id", "id"),
                users_foreign_key("referrer_legacy", "legacy_id"),
            ]);
            panel.rebuild_table(None, cx);
            panel
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    (panel, window)
}

/// The cell of the grid model as its label, editor text and typed raw value.
fn fk_cell_full(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
    row: usize,
    col: usize,
) -> (Option<String>, String, Value) {
    window.update(|_, app| {
        let table_state = panel
            .read(app)
            .grid_table
            .table_state
            .as_ref()
            .expect("rebuild builds the table")
            .read(app);
        let cell = table_state
            .model()
            .cell(row, col)
            .expect("the fk cell exists")
            .clone();

        (
            cell.fk_spans().map(|(label, _)| label.to_string()),
            cell.edit_text(),
            cell.to_value(),
        )
    })
}

#[gpui::test]
fn fk_label_identity_distinct_columns_get_distinct_labels(cx: &mut TestAppContext) {
    let connection = Arc::new(UsersIdentityConnection::default());
    let (app_state, profile_id) = register_stub_connection(cx, connection.clone());
    cache_identity_details(cx, &app_state, profile_id);
    enable_grid_fk_labels(cx, &app_state);

    let (panel, window) = accounts_panel(cx, app_state, profile_id);

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    let batches = connection.batches();
    assert_eq!(
        batches.len(),
        2,
        "one batch per referenced column, not one for the shared id: {batches:?}"
    );
    for batch in &batches {
        assert_eq!(
            batch.values,
            vec![Value::Int(7)],
            "the repeated id inside one referenced column dedups into one lookup"
        );
    }
    let batched_columns: Vec<&str> = batches
        .iter()
        .map(|batch| batch.pk_column.as_str())
        .collect();
    assert!(
        batched_columns.contains(&"id") && batched_columns.contains(&"legacy_id"),
        "both referenced columns were fetched: {batched_columns:?}"
    );

    for row in 0..2 {
        let (user_label, user_edit, user_raw) = fk_cell_full(&panel, window, row, 1);
        assert_eq!(
            user_label.as_deref(),
            Some("Alice"),
            "user_id → users.id = 7 names Alice"
        );
        assert_eq!(user_edit, "7", "the editor carries the raw id");
        assert_eq!(user_raw, Value::Int(7), "copy/edit keep the typed id");

        let (referrer_label, referrer_edit, referrer_raw) = fk_cell_full(&panel, window, row, 2);
        assert_eq!(
            referrer_label.as_deref(),
            Some("Bob"),
            "referrer_legacy → users.legacy_id = 7 names Bob, not Alice"
        );
        assert_eq!(referrer_edit, "7", "the editor carries the raw id");
        assert_eq!(referrer_raw, Value::Int(7), "copy/edit keep the typed id");
    }

    // A second round resolves both cells from the cache — per referenced
    // column — without touching the source again.
    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();

    assert_eq!(
        connection.batches().len(),
        2,
        "a resolved column-id pair is a cache hit and fetches nothing"
    );
    let (referrer_label, _, _) = fk_cell_full(&panel, window, 0, 2);
    assert_eq!(referrer_label.as_deref(), Some("Bob"));
}

#[gpui::test]
fn fk_label_identity_inspector_outgoing_labels_resolve_per_referenced_column(
    cx: &mut TestAppContext,
) {
    use std::sync::atomic::Ordering;

    let snapshot = InspectorSnapshot {
        row_number: 1,
        row_key: None,
        cells: Vec::<InspectorCell>::new(),
        can_edit: false,
    };
    let entity = cx.new(|cx| RowInspectorContent::new(snapshot, cx));

    let reference = |column: &str, target_pk: &str| FkReference {
        column: column.to_string(),
        target_schema: Some("public".to_string()),
        target_table: "users".to_string(),
        target_pk: target_pk.to_string(),
        value: Value::Int(7),
        display_label: None,
        kind: ReferenceKind::Outgoing,
    };
    let references = vec![
        reference("user_id", "id"),
        reference("referrer_legacy", "legacy_id"),
    ];

    cx.update(|cx| {
        entity.update(cx, |content, cx| {
            content.set_references(references.clone(), cx);
        });
    });
    let generation = cx.read(|cx| entity.read(cx).generation());

    let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let display_columns: HashMap<(Option<String>, String), Option<String>> = HashMap::from([(
        (Some("public".to_string()), "users".to_string()),
        Some("name".to_string()),
    )]);

    cx.spawn({
        let entity = entity.clone();
        let fetches = fetches.clone();
        move |mut cx| async move {
            resolve_outgoing_labels(
                &entity,
                generation,
                &references,
                &display_columns,
                |reference, executor| {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    let row = UsersIdentityConnection::referenced_row(
                        &reference.target_pk,
                        &reference.value,
                    );
                    executor.spawn(async move { Ok(Some(row)) })
                },
                &mut cx,
            )
            .await;
        }
    })
    .detach();
    cx.run_until_parked();

    cx.read(|cx| {
        let content = entity.read(cx);
        assert_eq!(
            content
                .reference(0)
                .expect("listed")
                .display_label
                .as_deref(),
            Some("Alice"),
            "the users.id reference names Alice"
        );
        assert_eq!(
            content
                .reference(1)
                .expect("listed")
                .display_label
                .as_deref(),
            Some("Bob"),
            "the users.legacy_id reference names Bob, not the id reference's label"
        );
    });
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        2,
        "distinct referenced columns fetch separately even at the same id"
    );
}

#[gpui::test]
fn fk_label_identity_inspector_seeds_labels_per_referenced_column(cx: &mut TestAppContext) {
    let connection = Arc::new(UsersIdentityConnection::default());
    let (app_state, profile_id) = register_stub_connection(cx, connection.clone());
    cache_identity_details(cx, &app_state, profile_id);
    enable_grid_fk_labels(cx, &app_state);

    let (panel, window) = accounts_panel(cx, app_state, profile_id);

    // The grid warms the display cache for both referenced columns.
    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.resolve_fk_display_values(cx));
    });
    window.run_until_parked();
    assert_eq!(connection.batches().len(), 2);

    window.update(|_, app| {
        panel.update(app, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let content = window.update(|_, app| {
        panel
            .read(app)
            .inspector
            .row_inspector_content
            .clone()
            .expect("the row inspector is mounted")
    });
    window.update(|_, app| {
        let content = content.read(app);
        assert_eq!(
            content
                .reference(0)
                .expect("listed")
                .display_label
                .as_deref(),
            Some("Alice"),
            "the users.id reference is seeded from the cache"
        );
        assert_eq!(
            content
                .reference(1)
                .expect("listed")
                .display_label
                .as_deref(),
            Some("Bob"),
            "the users.legacy_id reference is seeded by its own column, not the id hit"
        );
    });
    assert_eq!(
        connection.row_fetches(),
        0,
        "both labels come from the cache without a per-row fetch"
    );
}
