use super::rail_keys::host_in_rail;
use super::*;
use crate::data_grid_panel::ReferencedRowTarget;
use crate::data_grid_panel::context_menu::toolbar::ToolbarAction;
use crate::data_grid_panel::row_inspector::{FkReference, ReferenceKind, RowInspectorContentEvent};
use dbflux_components::primitives::LoadingState;
use dbflux_ui_base::keyboard_coverage::{FrameCapture, clickable_elements};

fn outgoing_reference() -> FkReference {
    FkReference {
        column: "customer_id".to_string(),
        target_schema: Some("public".to_string()),
        target_table: "customers".to_string(),
        target_pk: "id".to_string(),
        value: dbflux_core::Value::Int(2129),
        display_label: None,
        kind: ReferenceKind::Outgoing,
    }
}

#[gpui::test]
fn reference_target_maps_an_outgoing_fk_reference(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        let target = panel
            .read(app)
            .reference_target(&outgoing_reference())
            .expect("a table source maps the reference");

        assert_eq!(target.profile_id, Uuid::nil());
        assert_eq!(target.database.as_deref(), Some("app"));
        assert_eq!(target.table, TableRef::with_schema("public", "customers"));
        assert_eq!(target.key_column, "id");
        assert_eq!(target.key_value, dbflux_core::Value::Int(2129));
    });
}

#[gpui::test]
fn reference_target_without_a_schema_keeps_the_table_bare(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        let mut reference = outgoing_reference();
        reference.target_schema = None;

        let target = panel
            .read(app)
            .reference_target(&reference)
            .expect("a table source maps the reference");

        assert_eq!(target.table, TableRef::new("customers"));
    });
}

#[gpui::test]
fn reference_target_is_none_for_an_incoming_reference(cx: &mut TestAppContext) {
    init_test_runtime(cx);
    let app_state = isolated_test_app_state(cx);
    let window = cx.add_empty_window();
    let panel = table_panel(window, app_state);

    window.update(|_, app| {
        let mut reference = outgoing_reference();
        reference.kind = ReferenceKind::Incoming {
            count: LoadingState::Loaded(3),
        };

        assert!(panel.read(app).reference_target(&reference).is_none());
    });
}

#[gpui::test]
fn reference_target_is_none_without_a_table_source(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(StubConnection));
    let panel = build_panel_for_collection(cx, app_state, profile_id);

    cx.update(|app| {
        assert!(
            panel
                .read(app)
                .reference_target(&outgoing_reference())
                .is_none()
        );
    });
}

pub(super) fn orders_with_customer_fk() -> dbflux_core::TableInfo {
    dbflux_core::TableInfo {
        name: "orders".to_string(),
        schema: Some("public".to_string()),
        columns: None,
        indexes: None,
        foreign_keys: Some(vec![dbflux_core::ForeignKeyInfo {
            name: "fk_customer".to_string(),
            columns: vec!["customer_id".to_string()],
            referenced_table: "customers".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: None,
            on_update: None,
        }]),
        constraints: None,
        sample_fields: None,
        presentation: dbflux_core::CollectionPresentation::default(),
        child_items: None,
        storage_hints: None,
        pseudo_columns: Box::default(),
    }
}

pub(super) fn orders_result(customer_value: dbflux_core::Value) -> QueryResult {
    QueryResult::table(
        vec![key_column("id", true), key_column("customer_id", false)],
        vec![vec![dbflux_core::Value::Int(1), customer_value]],
        None,
        Duration::ZERO,
    )
}

// =========================================================================
// Key-focus reload leaves from the first page
// =========================================================================

/// Connection stub for the key-focus navigation tests: it speaks the core
/// default (ANSI, double-quote) dialect so the equality text can be pinned,
/// answers every browse with a fixed one-row page, and records the requests
/// so a reload's pagination and filter can be asserted.
struct RecordingBrowseConnection {
    browses: std::sync::Mutex<Vec<dbflux_core::TableBrowseRequest>>,
}

impl RecordingBrowseConnection {
    fn browses(&self) -> Vec<dbflux_core::TableBrowseRequest> {
        self.browses.lock().expect("browse log").clone()
    }
}

impl Default for RecordingBrowseConnection {
    fn default() -> Self {
        Self {
            browses: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl dbflux_core::Connection for RecordingBrowseConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static META: std::sync::OnceLock<dbflux_core::DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| {
            dbflux_core::DriverMetadataBuilder::new(
                "stub-fk-navigation",
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

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
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

    fn browse_table(
        &self,
        request: &dbflux_core::TableBrowseRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        self.browses
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(request.clone());
        Ok(orders_result(dbflux_core::Value::Int(8)))
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }
}

/// A reused tab that sits on a later page still lands on the referenced
/// row: the equality reload leaves from offset 0, and the cursor that
/// addressed the previous row set is dropped.
#[gpui::test]
fn focus_row_by_key_leaves_from_the_first_page_and_drops_the_cursor(cx: &mut TestAppContext) {
    let connection = Arc::new(RecordingBrowseConnection::default());
    let (app_state, profile_id) = register_stub_connection(cx, connection.clone());
    let window = cx.add_empty_window();

    let panel = window.update(|window, app| {
        let source = DataSource::Table {
            profile_id,
            database: Some("app".to_string()),
            table: TableRef::with_schema("public", "orders"),
            // A later page: offset 100 under the panel's default limit, so
            // the limit input holds no reset of its own.
            pagination: Pagination::default().next_page(),
            order_by: Vec::new(),
            total_rows: Some(200),
        };

        app.new(|cx| {
            let mut panel = DataGridPanel::new_internal(
                source,
                app_state,
                vec!["customer_id".to_string()],
                window,
                cx,
            );
            panel.set_result(orders_result(dbflux_core::Value::Int(8)), cx);
            panel
        })
    });
    window.run_until_parked();

    // A cursor on the rows the old page holds.
    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, 1), cx);
        });
    });
    window.run_until_parked();

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.focus_row_by_key("customer_id", &dbflux_core::Value::Int(8), window, cx);
        });
    });
    window.run_until_parked();

    let browses = connection.browses();
    let reload = browses.last().expect("the equality ran a table browse");
    assert_eq!(
        reload.pagination.offset(),
        0,
        "the reload must leave from the first page, not the offset the tab held"
    );
    assert_eq!(reload.filter.as_deref(), Some("\"customer_id\" = 8"));

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            panel.source.pagination().map(|page| page.offset()),
            Some(0),
            "the source is back on the first page"
        );
        assert_eq!(
            panel
                .grid_table
                .table_state
                .as_ref()
                .expect("table state")
                .read(app)
                .selection()
                .active,
            None,
            "the cursor addressed the replaced row set and must be dropped"
        );
    });
}

#[gpui::test]
fn fk_target_for_cell_resolves_a_single_column_fk_cell(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(StubConnection));

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub connection is registered");
            connected.table_details.insert(
                (
                    "app".to_string(),
                    Some("public".to_string()),
                    "orders".to_string(),
                ),
                orders_with_customer_fk(),
            );
        });
    });

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "orders"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(1),
            };

            DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            )
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("the window builder stores the grid");

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_result(orders_result(dbflux_core::Value::Int(2129)), cx);

            let target = panel
                .fk_target_for_cell(0, 1, cx)
                .expect("the customer_id cell resolves");

            assert_eq!(target.profile_id, profile_id);
            assert_eq!(target.database.as_deref(), Some("app"));
            assert_eq!(target.table, TableRef::new("customers"));
            assert_eq!(target.key_column, "id");
            assert_eq!(target.key_value, dbflux_core::Value::Int(2129));

            // The local PK column is not a foreign key.
            assert!(panel.fk_target_for_cell(0, 0, cx).is_none());

            // A null FK value has no row to navigate to.
            panel.set_result(orders_result(dbflux_core::Value::Null), cx);
            assert!(panel.fk_target_for_cell(0, 1, cx).is_none());
        });
    });
}

// =========================================================================
// Reference navigation through the inspector rail
// =========================================================================

/// A grid over `public.orders` with the customer FK metadata cached and the
/// customer cell under the cursor, subscribed to the panel's rail events.
fn panel_with_fk_cursor(
    cx: &mut TestAppContext,
) -> (
    gpui::Entity<DataGridPanel>,
    Rc<RefCell<Vec<bool>>>,
    &mut gpui::VisualTestContext,
) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(StubConnection));

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub connection is registered");
            connected.table_details.insert(
                (
                    "app".to_string(),
                    Some("public".to_string()),
                    "orders".to_string(),
                ),
                orders_with_customer_fk(),
            );
        });
    });

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let header_flags = Rc::new(RefCell::new(Vec::new()));

    let (_, window) = cx.add_window_view({
        let header_flags = header_flags.clone();
        move |window, cx| {
            let panel = cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id,
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "orders"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(1),
                };

                DataGridPanel::new_internal(
                    source,
                    app_state.clone(),
                    vec!["id".to_string()],
                    window,
                    cx,
                )
            });
            panel_handle.replace(Some(panel.clone()));

            let subscription = {
                let header_flags = header_flags.clone();
                cx.subscribe(&panel, move |_, _, event: &DataGridEvent, _| {
                    if let DataGridEvent::OpenInspector {
                        content_has_header, ..
                    } = event
                    {
                        header_flags.borrow_mut().push(*content_has_header);
                    }
                })
            };
            subscription.detach();

            Root::new(panel, window, cx)
        }
    });
    window.run_until_parked();

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("the window builder stores the grid");

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| {
            panel.set_result(orders_result(dbflux_core::Value::Int(2129)), cx);
        });

        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, 1), cx);
        });
    });
    window.run_until_parked();

    (panel, header_flags, window)
}

/// Subscribes to the panel's `OpenReferencedRow` requests and returns the
/// collector they land in.
fn opened_references(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
) -> Rc<RefCell<Vec<ReferencedRowTarget>>> {
    let opened: Rc<RefCell<Vec<ReferencedRowTarget>>> = Rc::default();

    window.update(|_, cx| {
        let opened = opened.clone();
        cx.subscribe(panel, move |_, event: &DataGridEvent, _| {
            if let DataGridEvent::OpenReferencedRow { target } = event {
                opened.borrow_mut().push(target.clone());
            }
        })
        .detach();
    });

    opened
}

#[gpui::test]
fn navigating_a_reference_entry_opens_the_referenced_row(cx: &mut TestAppContext) {
    let (panel, _header_flags, window) = panel_with_fk_cursor(cx);
    let opened = opened_references(&panel, window);

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let content = panel
            .read(cx)
            .inspector
            .row_inspector_content
            .clone()
            .expect("the row inspector is mounted");
        assert!(
            content.read(cx).first_outgoing_reference().is_some(),
            "the row inspector lists an outgoing reference"
        );

        panel.update(cx, |panel, cx| {
            panel.handle_row_inspector_event(
                RowInspectorContentEvent::NavigateReference { index: 0 },
                cx,
            );
        });
    });

    let targets = opened.borrow().clone();
    let target = targets
        .first()
        .expect("the reference entry opens the row it points at");
    assert_eq!(target.table, TableRef::new("customers"));
    assert_eq!(target.key_column, "id");
    assert_eq!(target.key_value, dbflux_core::Value::Int(2129));
}

#[gpui::test]
fn the_toolbar_lists_go_to_referenced_row_by_gating(cx: &mut TestAppContext) {
    let (panel, _header_flags, window) = panel_with_fk_cursor(cx);

    let listed = |action: ToolbarAction, window: &mut gpui::VisualTestContext| {
        window
            .update(|_, cx| panel.read(cx).toolbar_actions(cx))
            .contains(&action)
    };

    assert!(
        listed(ToolbarAction::GoToReferencedRow, window),
        "the cursor sits on the customer_id FK cell"
    );

    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, 0), cx);
        });
    });
    window.run_until_parked();

    assert!(
        !listed(ToolbarAction::GoToReferencedRow, window),
        "the local PK cell is not a foreign key and no inspector is open"
    );

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 0, cx));
    });
    window.run_until_parked();

    assert!(
        listed(ToolbarAction::GoToReferencedRow, window),
        "the open row inspector lists an outgoing reference"
    );
}

#[gpui::test]
fn the_toolbar_gates_go_to_referenced_row_on_outgoing_references_only(cx: &mut TestAppContext) {
    let (panel, _header_flags, window) = panel_with_fk_cursor(cx);

    window.update(|_, cx| {
        let table_state = panel
            .read(cx)
            .grid_table
            .table_state
            .clone()
            .expect("the result builds a table");
        table_state.update(cx, |state, cx| {
            state.select_cell(CellCoord::new(0, 0), cx);
        });

        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 0, cx));
    });
    window.run_until_parked();

    // The inspector's only entry is incoming: the action stays hidden, since
    // an incoming reference has no row to open.
    window.update(|_, cx| {
        let content = panel
            .read(cx)
            .inspector
            .row_inspector_content
            .clone()
            .expect("the row inspector is mounted");
        content.update(cx, |content, cx| {
            content.set_references(
                vec![FkReference {
                    column: "order_id".to_string(),
                    target_schema: None,
                    target_table: "order_items".to_string(),
                    target_pk: "id".to_string(),
                    value: dbflux_core::Value::Int(1),
                    display_label: None,
                    kind: ReferenceKind::Incoming {
                        count: LoadingState::Loaded(3),
                    },
                }],
                cx,
            );
        });
    });
    window.run_until_parked();

    assert!(
        !window
            .update(|_, cx| panel.read(cx).toolbar_actions(cx))
            .contains(&ToolbarAction::GoToReferencedRow),
        "an inspector that lists only incoming references has nothing to open"
    );
}

#[gpui::test]
fn the_fk_toolbar_action_opens_the_referenced_row(cx: &mut TestAppContext) {
    let (panel, _header_flags, window) = panel_with_fk_cursor(cx);
    let opened = opened_references(&panel, window);

    window.update(|window, cx| {
        panel.update(cx, |panel, cx| {
            panel.run_toolbar_action(ToolbarAction::GoToReferencedRow, window, cx);
        });
    });
    window.run_until_parked();

    let targets = opened.borrow().clone();
    assert_eq!(
        targets.len(),
        1,
        "the active cell's FK opens the row it points at"
    );
    for target in &targets {
        assert_eq!(target.table, TableRef::new("customers"));
        assert_eq!(target.key_column, "id");
        assert_eq!(target.key_value, dbflux_core::Value::Int(2129));
    }
}

/// An incoming REFERENCES entry renders as an informational row: no pointer,
/// no hover, no chevron, nothing to click. Only the outgoing entries carry
/// the click that opens the row they point at.
#[gpui::test]
fn an_incoming_reference_entry_is_not_clickable(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(StubConnection));

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub connection is registered");
            connected.table_details.insert(
                (
                    "app".to_string(),
                    Some("public".to_string()),
                    "orders".to_string(),
                ),
                orders_with_customer_fk(),
            );
            // The incoming direction: order_items points at orders, so the
            // row inspector lists an outgoing entry (customers) plus an
            // incoming one (order_items).
            connected.schema_foreign_keys.insert(
                dbflux_core::SchemaCacheKey::new("app", Some("public")),
                vec![dbflux_core::SchemaForeignKeyInfo {
                    name: "fk_order_items_order".to_string(),
                    table_name: "order_items".to_string(),
                    columns: vec!["order_id".to_string()],
                    referenced_schema: Some("public".to_string()),
                    referenced_table: "orders".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: None,
                    on_update: None,
                }],
            );
        });
    });

    let (panel, window) = host_in_rail(cx, {
        let app_state = app_state.clone();
        move |window, cx| {
            cx.new(|cx| {
                let source = DataSource::Table {
                    profile_id,
                    database: Some("app".to_string()),
                    table: TableRef::with_schema("public", "orders"),
                    pagination: Pagination::default(),
                    order_by: Vec::new(),
                    total_rows: Some(1),
                };

                let mut panel = DataGridPanel::new_internal(
                    source,
                    app_state,
                    vec!["id".to_string()],
                    window,
                    cx,
                );
                panel.set_result(orders_result(dbflux_core::Value::Int(2129)), cx);
                panel
            })
        }
    });

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let capture = FrameCapture::observe(window);
    let clickable: Vec<String> = clickable_elements(&capture.frame(window))
        .into_iter()
        .map(|element| element.id)
        .collect();

    assert!(
        clickable.iter().any(|id| id == "row-inspector-reference-0"),
        "the outgoing entry stays clickable: {clickable:?}"
    );
    assert!(
        !clickable.iter().any(|id| id == "row-inspector-reference-1"),
        "the incoming entry is informational, not clickable: {clickable:?}"
    );
}

// =========================================================================
// Outgoing reference labels
// =========================================================================

/// `orders_with_customer_fk` with the referenced schema recorded, so the
/// foreign-key metadata and the cached customers details key the same
/// table.
fn orders_pointing_at_public_customers() -> dbflux_core::TableInfo {
    let mut info = orders_with_customer_fk();
    if let Some(fk) = info.foreign_keys.as_mut().and_then(|fks| fks.first_mut()) {
        fk.referenced_schema = Some("public".to_string());
    }
    info
}

/// A grid over `public.orders` whose `customer_id` row points at customer 8
/// of `public.customers`, with the orders foreign-key metadata and the
/// customers details cached: the row inspector's outgoing reference can
/// resolve its label through the counting fetch stub. `labels_enabled`
/// flips `grid_fk_labels` the way Settings > General does.
fn panel_with_customer_label(
    cx: &mut TestAppContext,
    found: bool,
    labels_enabled: bool,
) -> (
    gpui::Entity<DataGridPanel>,
    &mut gpui::VisualTestContext,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connection = std::sync::Arc::new(super::builder_draft_and_fk::FkResolveConnection {
        fetches: fetches.clone(),
        found,
    });
    let (app_state, profile_id) = register_stub_connection(cx, connection);
    super::builder_draft_and_fk::cache_customers_details(cx, &app_state, profile_id);
    if labels_enabled {
        enable_grid_fk_labels(cx, &app_state);
    }

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub connection is registered");
            connected.table_details.insert(
                (
                    "app".to_string(),
                    Some("public".to_string()),
                    "orders".to_string(),
                ),
                orders_pointing_at_public_customers(),
            );
        });
    });

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(move |window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "orders"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(1),
            };

            DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            )
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("the window builder stores the grid");

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| {
            panel.set_result(orders_result(dbflux_core::Value::Int(8)), cx);
        });
    });
    window.run_until_parked();

    (panel, window, fetches)
}

/// The outgoing reference of the mounted row inspector.
fn inspector_reference(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut gpui::VisualTestContext,
) -> FkReference {
    window.update(|_, cx| {
        panel
            .read(cx)
            .inspector
            .row_inspector_content
            .clone()
            .expect("the row inspector is mounted")
            .read(cx)
            .reference(0)
            .cloned()
            .expect("the outgoing reference is listed")
    })
}

#[gpui::test]
fn the_row_inspector_resolves_a_reference_label_in_the_background(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let (panel, window, fetches) = panel_with_customer_label(cx, true, false);

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let reference = inspector_reference(&panel, window);
    assert_eq!(reference.display_label.as_deref(), Some("María López"));
    assert_eq!(reference.detail().as_deref(), Some("María López · #8"));
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}

#[gpui::test]
fn the_row_inspector_seeds_reference_labels_from_the_grid_cache(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let (panel, window, fetches) = panel_with_customer_label(cx, true, true);

    // The grid resolves its own FK cells first, warming the label cache the
    // inspector seeds from.
    window.update(|_, cx| {
        panel.update(cx, |panel, cx| {
            panel.builder.fk_cache =
                super::super::FkLoadState::Ready(vec![dbflux_core::SchemaForeignKeyInfo {
                    name: "orders_customer_fk".to_string(),
                    table_name: "orders".to_string(),
                    columns: vec!["customer_id".to_string()],
                    referenced_schema: Some("public".to_string()),
                    referenced_table: "customers".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: None,
                    on_update: None,
                }]);
            panel.resolve_fk_display_values(cx);
        });
    });
    window.run_until_parked();
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "the grid fetched the row"
    );

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let reference = inspector_reference(&panel, window);
    assert_eq!(
        reference.display_label.as_deref(),
        Some("María López"),
        "the cache hit names the row without a fetch"
    );
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "opening the inspector adds no fetch"
    );
}

#[gpui::test]
fn a_deleted_referenced_row_keeps_the_raw_id(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let (panel, window, fetches) = panel_with_customer_label(cx, false, false);

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let reference = inspector_reference(&panel, window);
    assert!(reference.display_label.is_none());
    assert_eq!(reference.detail().as_deref(), Some("id = 8"));
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}

/// `grid_fk_labels` off (the default): the grid keeps the raw id in the
/// foreign-key cell and starts no resolution fetch, while the row inspector
/// still resolves its outgoing reference in the background.
#[gpui::test]
fn grid_fk_labels_off_keeps_cells_raw_and_the_inspector_resolving(cx: &mut TestAppContext) {
    use std::sync::atomic::Ordering;

    let (panel, window, fetches) = panel_with_customer_label(cx, true, false);

    // Everything the grid needs to label the cell is in place; the setting is
    // the only thing holding the resolution back.
    window.update(|_, cx| {
        panel.update(cx, |panel, cx| {
            panel.builder.fk_cache =
                super::super::FkLoadState::Ready(vec![dbflux_core::SchemaForeignKeyInfo {
                    name: "orders_customer_fk".to_string(),
                    table_name: "orders".to_string(),
                    columns: vec!["customer_id".to_string()],
                    referenced_schema: Some("public".to_string()),
                    referenced_table: "customers".to_string(),
                    referenced_columns: vec!["id".to_string()],
                    on_delete: None,
                    on_update: None,
                }]);
            panel.resolve_fk_display_values(cx);
        });
    });
    window.run_until_parked();

    let (label, edit_text) = super::builder_draft_and_fk::fk_cell(&panel, window, 0);
    assert_eq!(label, None, "the cell keeps the raw id");
    assert_eq!(edit_text, "8");
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "the grid started no resolution fetch"
    );

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 1, cx));
    });
    window.run_until_parked();

    let reference = inspector_reference(&panel, window);
    assert_eq!(
        reference.display_label.as_deref(),
        Some("María López"),
        "the inspector resolves regardless of the setting"
    );
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        1,
        "the inspector resolved the label on its own"
    );
}

// =========================================================================
// Moving to another row drops the previous label lookup
// =========================================================================

/// The orders table with two single-column foreign keys, so one row's
/// inspector lookup needs the display column of two referenced tables.
fn orders_with_two_fks() -> dbflux_core::TableInfo {
    let fk = |name: &str, column: &str, table: &str| dbflux_core::ForeignKeyInfo {
        name: name.to_string(),
        columns: vec![column.to_string()],
        referenced_table: table.to_string(),
        referenced_schema: Some("public".to_string()),
        referenced_columns: vec!["id".to_string()],
        on_delete: None,
        on_update: None,
    };

    let mut info = orders_with_customer_fk();
    info.foreign_keys = Some(vec![
        fk("fk_customer", "customer_id", "customers"),
        fk("fk_region", "region_id", "regions"),
    ]);
    info
}

/// A page whose first row carries both foreign keys and whose second row
/// keeps only the customer one.
fn orders_result_with_two_fk_rows() -> QueryResult {
    QueryResult::table(
        vec![
            key_column("id", true),
            key_column("customer_id", false),
            key_column("region_id", false),
        ],
        vec![
            vec![
                dbflux_core::Value::Int(1),
                dbflux_core::Value::Int(8),
                dbflux_core::Value::Int(2),
            ],
            vec![
                dbflux_core::Value::Int(2),
                dbflux_core::Value::Int(9),
                dbflux_core::Value::Null,
            ],
        ],
        None,
        Duration::ZERO,
    )
}

/// Connection stub that records every `table_details` call by table and
/// counts `fetch_row_by_pk`, answering customer 9 with a name, so what a
/// row's label lookup read is visible after the executor ran.
#[derive(Default)]
struct CountingLabelConnection {
    tables: std::sync::Mutex<Vec<String>>,
    fetches: std::sync::atomic::AtomicUsize,
}

impl CountingLabelConnection {
    fn tables(&self) -> Vec<String> {
        self.tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn fetches(&self) -> usize {
        self.fetches.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl dbflux_core::Connection for CountingLabelConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        static META: std::sync::OnceLock<dbflux_core::DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| {
            dbflux_core::DriverMetadataBuilder::new(
                "stub-counting-label",
                "Stub",
                dbflux_core::DatabaseCategory::Relational,
                dbflux_core::QueryLanguage::Sql,
            )
            .build()
        })
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::Postgres
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: the label lookup path never asks for a dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("CountingLabelConnection::dialect not needed")
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

    fn table_details(
        &self,
        _: &str,
        _: Option<&str>,
        table: &str,
    ) -> Result<dbflux_core::TableInfo, dbflux_core::DbError> {
        self.tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(table.to_string());

        Ok(super::builder_draft_and_fk::referenced_table_info(table))
    }

    fn fetch_row_by_pk(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        pk_value: &dbflux_core::Value,
    ) -> Result<Option<std::collections::HashMap<String, dbflux_core::Value>>, dbflux_core::DbError>
    {
        self.fetches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if pk_value == &dbflux_core::Value::Int(9) {
            return Ok(Some(std::collections::HashMap::from([
                ("id".to_string(), dbflux_core::Value::Int(9)),
                (
                    "name".to_string(),
                    dbflux_core::Value::Text("María López".to_string()),
                ),
            ])));
        }

        Ok(None)
    }
}

/// Moving to another row before the executor runs the first row's label
/// lookup drops that lookup instead of leaving a detached task behind: the
/// referenced tables of the row that is no longer open are never read,
/// while the current row's lookup still resolves.
#[gpui::test]
fn moving_to_another_row_drops_the_previous_label_lookup(cx: &mut TestAppContext) {
    let connection = Arc::new(CountingLabelConnection::default());
    let (app_state, profile_id) = register_stub_connection(cx, connection.clone());

    cx.update(|cx| {
        app_state.update(cx, |app, _| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub connection is registered");
            connected.table_details.insert(
                (
                    "app".to_string(),
                    Some("public".to_string()),
                    "orders".to_string(),
                ),
                orders_with_two_fks(),
            );
        });
    });

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(move |window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Table {
                profile_id,
                database: Some("app".to_string()),
                table: TableRef::with_schema("public", "orders"),
                pagination: Pagination::default(),
                order_by: Vec::new(),
                total_rows: Some(2),
            };

            DataGridPanel::new_internal(
                source,
                app_state.clone(),
                vec!["id".to_string()],
                window,
                cx,
            )
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("the window builder stores the grid");

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| {
            panel.set_result(orders_result_with_two_fk_rows(), cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        panel.update(cx, |panel, cx| panel.open_row_inspector(0, 2, cx));
        // The cursor moves on before the executor runs the first row's
        // lookup.
        panel.update(cx, |panel, cx| panel.open_row_inspector(1, 2, cx));
    });
    window.run_until_parked();

    assert_eq!(
        connection.tables(),
        ["customers"],
        "the second referenced table of the row that is no longer open is never read"
    );
    assert_eq!(
        connection.fetches(),
        1,
        "only the current row's referenced row is fetched"
    );
    assert_eq!(
        inspector_reference(&panel, window).display_label.as_deref(),
        Some("María López"),
        "the current row's lookup still resolves"
    );
}
