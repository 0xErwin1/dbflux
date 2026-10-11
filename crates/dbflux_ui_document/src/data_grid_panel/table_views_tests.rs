use std::sync::{Arc, Mutex};
use std::time::Duration;

use dbflux_components::theme;
use dbflux_core::{
    ColumnBadge, ColumnMeta, ColumnProfile, ConnectedProfile, Connection, ConnectionProfile,
    DbConfig, DbError, ProfileSource, QueryResult, TableBrowseRequest, TablePartition,
    TablePartitions, TableProfile, TableRef, TableViewFeatures, Value,
};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext, Entity, TestAppContext, VisualTestContext};
use uuid::Uuid;

use dbflux_components::components::column_profile_view::ProfileAction;

use super::{TableTab, available_table_tabs, header_annotations};
use crate::data_grid_panel::DataGridPanel;

/// What the stub connection was asked, for the assertions.
#[derive(Default)]
struct Calls {
    browses: Vec<TableBrowseRequest>,
    profiles: usize,
    partitions: usize,
    estimates: Vec<TableBrowseRequest>,
    samples: usize,
    /// Makes every browse fail, as a driver error would.
    fail_browses: bool,
    /// Makes every sample fail, as a driver error would.
    fail_samples: bool,
}

struct StubColumnarConnection {
    features: TableViewFeatures,
    calls: Arc<Mutex<Calls>>,
}

fn profile() -> TableProfile {
    TableProfile {
        row_count: Some(1_000),
        total_compressed_bytes: Some(300),
        ..TableProfile::new(
            vec![
                ColumnProfile {
                    badges: vec![ColumnBadge::SortKey { position: Some(1) }],
                    codec: Some("LZ4".into()),
                    compressed_bytes: Some(100),
                    uncompressed_bytes: Some(400),
                    ..ColumnProfile::new("site_id", "UInt32")
                },
                ColumnProfile {
                    compressed_bytes: Some(200),
                    uncompressed_bytes: Some(800),
                    null_count: Some(250),
                    ..ColumnProfile::new("path", "String")
                },
                ColumnProfile::new("country", "String"),
            ],
            ProfileSource::Catalog("system.columns"),
        )
    }
}

fn result_for(request: &TableBrowseRequest) -> QueryResult {
    let names: Vec<String> = if request.columns.is_empty() {
        vec!["site_id".into(), "path".into(), "country".into()]
    } else {
        request.columns.clone()
    };

    QueryResult::table(
        names
            .iter()
            .map(|name| ColumnMeta {
                name: name.clone(),
                type_name: "String".into(),
                nullable: true,
                is_primary_key: false,
                kind: dbflux_core::ColumnKind::Text,
            })
            .collect(),
        vec![names.iter().map(|_| Value::Text("v".into())).collect()],
        None,
        Duration::ZERO,
    )
}

impl Connection for StubColumnarConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        use dbflux_core::{
            DatabaseCategory, DriverCapabilities, DriverMetadata, Icon as CoreIcon, QueryLanguage,
            TransferFamily,
        };

        static META: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();
        META.get_or_init(|| DriverMetadata {
            id: "stub-columnar".to_string(),
            display_name: "Stub".to_string(),
            description: "test".to_string(),
            category: DatabaseCategory::Relational,
            transfer_family: TransferFamily::Sql,
            deployment_class: None,
            query_language: QueryLanguage::Sql,
            capabilities: DriverCapabilities::empty(),
            default_port: None,
            uri_scheme: "stub".to_string(),
            icon: CoreIcon::Database,
            syntax: None,
            query: None,
            mutation: None,
            ddl: None,
            transactions: None,
            limits: None,
            ssl_modes: None,
            ssl_cert_fields: None,
            classification_override: None,
            default_chunk_size: None,
            supports_lock_timeout: false,
            editor_profile: None,
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

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _: &dbflux_core::QueryRequest) -> Result<QueryResult, DbError> {
        Err(DbError::NotSupported("stub".to_string()))
    }

    fn browse_table(&self, request: &TableBrowseRequest) -> Result<QueryResult, DbError> {
        let mut calls = self
            .calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?;
        calls.browses.push(request.clone());

        if calls.fail_browses {
            return Err(DbError::query_failed("browse failed"));
        }

        Ok(result_for(request))
    }

    fn count_table(&self, _: &dbflux_core::TableCountRequest) -> Result<u64, DbError> {
        Ok(1)
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn table_view_features(&self) -> TableViewFeatures {
        self.features
    }

    fn table_profile(&self, _: &TableRef) -> Result<TableProfile, DbError> {
        self.calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .profiles += 1;
        Ok(profile())
    }

    fn sample_table_profile(
        &self,
        _: &TableRef,
        _: &[String],
        _: u64,
    ) -> Result<TableProfile, DbError> {
        let mut calls = self
            .calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?;
        calls.samples += 1;

        if calls.fail_samples {
            return Err(DbError::query_failed("sample failed"));
        }

        Ok(profile())
    }

    fn estimate_table_read(
        &self,
        request: &TableBrowseRequest,
        _: &TableProfile,
    ) -> Result<dbflux_core::ReadEstimate, DbError> {
        self.calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .estimates
            .push(request.clone());

        Ok(dbflux_core::ReadEstimate {
            total_bytes: 300,
            per_column: Vec::new(),
            scopes: Vec::new(),
            rows: Some(1_000),
            exact: false,
        })
    }

    fn table_partitions(&self, _: &TableRef) -> Result<TablePartitions, DbError> {
        self.calls
            .lock()
            .map_err(|_| DbError::query_failed("poisoned"))?
            .partitions += 1;

        Ok(TablePartitions {
            key: Some("toYYYYMM(ts)".into()),
            partitions: vec![TablePartition::new("202609")],
            source: ProfileSource::Catalog("system.parts"),
            captured_at: None,
        })
    }
}

fn init_test_runtime(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(theme::init);
    cx.update(|cx| {
        let host = cx.new(|_cx| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
}

fn app_state_with(
    features: TableViewFeatures,
    calls: Arc<Mutex<Calls>>,
    cx: &mut TestAppContext,
) -> (Entity<AppStateEntity>, Uuid) {
    let profile_id = Uuid::new_v4();

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage_runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage_runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let connected = ConnectedProfile {
                profile: ConnectionProfile::new(
                    "columnar",
                    DbConfig::SQLite {
                        path: std::path::PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                ),
                connection: Arc::new(StubColumnarConnection { features, calls }),
                schema: None,
                mutation_policy: dbflux_core::MutationPolicy::default(),
                read_only_reason: None,
                database_schemas: Default::default(),
                table_details: Default::default(),
                collection_children: Default::default(),
                schema_types: Default::default(),
                schema_columns: Default::default(),
                schema_indexes: Default::default(),
                schema_foreign_keys: Default::default(),
                schema_routines: Default::default(),
                dependents_cache: Default::default(),
                active_database: None,
                redis_key_cache: Default::default(),
                database_connections: Default::default(),
                proxy_tunnel: None,
            };
            app.connections_mut().insert(profile_id, connected);
        });
    });

    (app_state, profile_id)
}

fn open_table(
    features: TableViewFeatures,
    cx: &mut TestAppContext,
) -> (
    Entity<DataGridPanel>,
    Arc<Mutex<Calls>>,
    &mut VisualTestContext,
) {
    init_test_runtime(cx);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let (app_state, profile_id) = app_state_with(features, calls.clone(), cx);

    let window = cx.add_empty_window();
    let panel = window.update(|window, app| {
        app.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::with_schema("events", "page_views"),
                None,
                app_state,
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    // Builds the column picker, which needs a window.
    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.ensure_table_view_controls(window, cx)
        })
    });
    window.run_until_parked();

    (panel, calls, window)
}

fn all_features() -> TableViewFeatures {
    TableViewFeatures::COLUMN_PROFILE
        | TableViewFeatures::PARTITIONS
        | TableViewFeatures::COLUMN_PROJECTION
}

#[test]
fn tabs_follow_the_features_in_header_order() {
    assert_eq!(
        available_table_tabs(TableViewFeatures::empty()),
        vec![TableTab::Data]
    );
    assert_eq!(
        available_table_tabs(TableViewFeatures::COLUMN_PROFILE),
        vec![TableTab::Data, TableTab::Columns]
    );
    assert_eq!(
        available_table_tabs(all_features()),
        vec![TableTab::Data, TableTab::Columns, TableTab::Partitions]
    );
}

#[test]
fn header_annotations_name_badges_sizes_and_nulls_by_column_name() {
    let annotations = header_annotations(
        &profile(),
        &[
            "path".to_string(),
            "unknown".to_string(),
            "site_id".to_string(),
            "country".to_string(),
        ],
    );

    let path = annotations[0].as_ref().expect("path is profiled");
    assert_eq!(path.leading.as_ref(), "4.0× · 200 B");
    assert_eq!(
        path.trailing.as_ref().map(|text| text.as_ref()),
        Some("25% null")
    );

    assert!(
        annotations[1].is_none(),
        "a column the profile does not know"
    );

    let site_id = annotations[2].as_ref().expect("site_id is profiled");
    assert_eq!(site_id.leading.as_ref(), "Sort 1  ·  4.0× · 100 B");
    assert_eq!(site_id.trailing, None);

    assert!(annotations[3].is_none(), "nothing known about country");
}

#[gpui::test]
fn a_columnar_table_opens_with_tabs_a_profile_and_annotated_headers(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features(), cx);

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            panel.table_tabs(),
            vec![TableTab::Data, TableTab::Columns, TableTab::Partitions]
        );
        assert_eq!(panel.table_tab(), TableTab::Data);
        assert!(panel.table_views.profile.is_some());
        assert!(panel.shows_table_column_picker());

        let table_state = panel.grid_table.table_state.as_ref().expect("a table");
        let annotation = table_state
            .read(app)
            .header_annotation(0)
            .cloned()
            .expect("site_id is annotated");
        assert!(
            annotation.leading.starts_with("Sort 1"),
            "{}",
            annotation.leading
        );
    });

    let calls = calls.lock().unwrap();
    assert_eq!(calls.profiles, 1, "the profile is read once on open");
    assert_eq!(calls.partitions, 0, "partitions wait for their tab");
    assert!(
        calls
            .browses
            .iter()
            .all(|request| request.columns.is_empty())
    );
}

#[gpui::test]
fn a_projection_reads_only_the_chosen_columns(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features(), cx);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.apply_table_projection(
                dbflux_core::ColumnProjection::from_indices(3, &[1]),
                window,
                cx,
            )
        })
    });
    window.run_until_parked();

    let last = calls
        .lock()
        .unwrap()
        .browses
        .last()
        .cloned()
        .expect("a browse");
    assert_eq!(last.columns, vec!["path".to_string()]);

    window.update(|_, app| {
        let panel = panel.read(app);
        let result_columns: Vec<&str> = panel
            .result
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(result_columns, vec!["path"]);
        assert_eq!(panel.table_projection_columns(), vec!["path".to_string()]);
    });

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.apply_table_projection(dbflux_core::ColumnProjection::all(3), window, cx)
        })
    });
    window.run_until_parked();

    let last = calls
        .lock()
        .unwrap()
        .browses
        .last()
        .cloned()
        .expect("a browse");
    assert!(
        last.columns.is_empty(),
        "every column selected reads them all"
    );
}

#[gpui::test]
fn the_partitions_tab_reads_its_list_once_when_first_shown(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features(), cx);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.set_table_tab(TableTab::Partitions, window, cx);
            panel.set_table_tab(TableTab::Columns, window, cx);
            panel.set_table_tab(TableTab::Partitions, window, cx);
        })
    });
    window.run_until_parked();

    assert_eq!(calls.lock().unwrap().partitions, 1);

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(panel.table_tab(), TableTab::Partitions);
        let view = panel
            .table_views
            .partitions_view
            .as_ref()
            .expect("a partitions view");
        assert!(view.read(app).is_loaded());
    });
}

#[gpui::test]
fn next_result_tab_steps_through_the_table_views(cx: &mut TestAppContext) {
    let (panel, _calls, window) = open_table(all_features(), cx);

    let tabs: Vec<TableTab> = (0..3)
        .map(|_| {
            window.update(|window, app| {
                panel.update(app, |panel, cx| {
                    panel.dispatch_command(dbflux_app::keymap::Command::NextResultTab, window, cx);
                    panel.table_tab()
                })
            })
        })
        .collect();

    assert_eq!(
        tabs,
        vec![TableTab::Columns, TableTab::Partitions, TableTab::Data]
    );
}

#[gpui::test]
fn a_connection_without_table_views_keeps_the_plain_grid(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(TableViewFeatures::empty(), cx);

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(panel.table_tabs(), vec![TableTab::Data]);
        assert!(panel.table_views.profile_view.is_none());
        assert!(!panel.shows_table_column_picker());
        assert!(panel.table_projection_columns().is_empty());
    });

    let calls = calls.lock().unwrap();
    assert_eq!(calls.profiles, 0);
    assert!(!calls.browses.is_empty(), "the table still reads its rows");
}

#[gpui::test]
fn a_primary_key_learned_after_a_projection_comes_back_into_it(cx: &mut TestAppContext) {
    let (panel, _calls, window) = open_table(all_features(), cx);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            assert!(panel.pk_columns.is_empty(), "the key is not known yet");
            panel.apply_table_projection(
                dbflux_core::ColumnProjection::from_indices(3, &[1]),
                window,
                cx,
            );
        })
    });
    window.run_until_parked();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            assert_eq!(panel.table_projection_columns(), vec!["path".to_string()]);
            panel.apply_pk_details(vec!["site_id".to_string()], cx);
            assert_eq!(
                panel.table_projection_columns(),
                vec!["site_id".to_string(), "path".to_string()],
                "the key column is read again"
            );
        })
    });
}

#[test]
fn restoring_keys_selects_only_missing_key_columns() {
    let (projection, restored) = super::restore_keys(
        &profile(),
        dbflux_core::ColumnProjection::from_indices(3, &[1]),
        &[
            "site_id".to_string(),
            "path".to_string(),
            "not_a_column".to_string(),
        ],
    );

    assert_eq!(projection.selected_indices(), vec![0, 1]);
    assert_eq!(restored, vec!["site_id".to_string()]);
}

#[test]
fn a_returned_row_follows_the_grid_columns_by_name() {
    let names = |list: &[&str]| list.iter().map(|name| name.to_string()).collect::<Vec<_>>();
    let table = names(&["order_id", "line_no", "product", "quantity", "note"]);
    let returned = vec![
        Value::Int(2),
        Value::Int(1),
        Value::Text("keyboard".into()),
        Value::Int(1),
        Value::Text("EDITED".into()),
    ];

    let projected = super::returned_row_for_grid(
        &returned,
        &names(&["order_id", "line_no", "product", "note"]),
        Some(&table),
    );
    assert_eq!(
        projected,
        Some(vec![
            Value::Int(2),
            Value::Int(1),
            Value::Text("keyboard".into()),
            Value::Text("EDITED".into()),
        ]),
        "a column left out of the projection is skipped, not shifted into the next one"
    );

    assert_eq!(
        super::returned_row_for_grid(&returned, &table, None),
        Some(returned.clone()),
        "a row matching the grid is used as it is"
    );

    assert_eq!(
        super::returned_row_for_grid(&returned, &names(&["order_id", "note"]), None),
        None,
        "a row that cannot be matched to the grid columns is not used"
    );
}

#[gpui::test]
fn the_read_estimate_follows_the_applied_filter_not_the_draft(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features() | TableViewFeatures::READ_ESTIMATE, cx);

    let last_estimate = |calls: &Arc<Mutex<Calls>>| {
        calls
            .lock()
            .unwrap()
            .estimates
            .last()
            .cloned()
            .expect("an estimate")
    };
    assert_eq!(last_estimate(&calls).filter, None);

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.filter_bar.filter_input.update(cx, |input, cx| {
                input.set_value("site_id = 7", window, cx);
            });
            panel.load_table_profile(cx);
        })
    });
    window.run_until_parked();

    assert_eq!(
        last_estimate(&calls).filter,
        None,
        "a filter typed but not run is not estimated"
    );

    window.update(|window, app| panel.update(app, |panel, cx| panel.refresh(window, cx)));
    window.run_until_parked();

    assert_eq!(
        last_estimate(&calls).filter.as_deref(),
        Some("site_id = 7"),
        "the estimate follows the filter once it runs"
    );
}

#[gpui::test]
fn a_failed_browse_leaves_the_read_estimate_on_the_rows_shown(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features() | TableViewFeatures::READ_ESTIMATE, cx);

    let estimates_before = calls.lock().unwrap().estimates.len();
    assert!(estimates_before > 0, "the opening read is estimated");

    calls.lock().unwrap().fail_browses = true;

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.filter_bar.filter_input.update(cx, |input, cx| {
                input.set_value("site_id = 7", window, cx);
            });
            panel.refresh(window, cx);
        })
    });
    window.run_until_parked();

    let calls = calls.lock().unwrap();
    assert_eq!(
        calls
            .browses
            .last()
            .and_then(|request| request.filter.clone())
            .as_deref(),
        Some("site_id = 7"),
        "the filtered browse ran"
    );
    assert_eq!(
        calls.estimates.len(),
        estimates_before,
        "a browse that failed is not estimated"
    );

    window.update(|_, app| {
        let last_browse = panel
            .read(app)
            .table_views
            .last_browse
            .clone()
            .expect("the opening read");
        assert_eq!(
            last_browse.filter, None,
            "the estimate still describes the rows shown"
        );
    });
}

/// Simulates a Refresh that started while the sample was in flight and has
/// not finished: the generation moved on and the view shows the Refresh.
fn start_sample_then_a_refresh(panel: &Entity<DataGridPanel>, window: &mut VisualTestContext) {
    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.sample_table_profile(cx);

            panel.table_views.profile_generation += 1;
            if let Some(view) = panel.table_views.profile_view.clone() {
                view.update(cx, |view, cx| {
                    view.set_running(Some(ProfileAction::Refresh), cx)
                });
            }
        })
    });
    window.run_until_parked();
}

fn running_profile_action(
    panel: &Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Option<ProfileAction> {
    window.update(|_, app| {
        panel
            .read(app)
            .table_views
            .profile_view
            .as_ref()
            .expect("a Columns view")
            .read(app)
            .running()
    })
}

#[gpui::test]
fn a_sample_finishing_during_a_refresh_leaves_the_refresh_running(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features(), cx);

    start_sample_then_a_refresh(&panel, window);

    assert_eq!(calls.lock().unwrap().samples, 1);
    assert_eq!(
        running_profile_action(&panel, window),
        Some(ProfileAction::Refresh)
    );
}

#[gpui::test]
fn a_sample_failing_during_a_refresh_leaves_the_refresh_running(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features(), cx);
    calls.lock().unwrap().fail_samples = true;

    start_sample_then_a_refresh(&panel, window);

    assert_eq!(calls.lock().unwrap().samples, 1);
    assert_eq!(
        running_profile_action(&panel, window),
        Some(ProfileAction::Refresh)
    );
}

#[gpui::test]
fn a_sample_clears_its_running_indicator_when_it_finishes(cx: &mut TestAppContext) {
    let (panel, calls, window) = open_table(all_features(), cx);

    window.update(|_, app| panel.update(app, |panel, cx| panel.sample_table_profile(cx)));
    window.run_until_parked();

    assert_eq!(calls.lock().unwrap().samples, 1);
    assert_eq!(running_profile_action(&panel, window), None);
}

#[gpui::test]
fn the_columns_view_offers_a_projection_only_while_one_applies(cx: &mut TestAppContext) {
    let (panel, _calls, window) = open_table(all_features(), cx);

    let projection_enabled = |window: &mut VisualTestContext| {
        window.update(|_, app| {
            panel
                .read(app)
                .table_views
                .profile_view
                .as_ref()
                .expect("a Columns view")
                .read(app)
                .projection_enabled()
        })
    };
    assert!(projection_enabled(window));

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.builder.visual_select = Some(dbflux_core::SelectQuery {
                sql: "SELECT path FROM events.page_views".to_string(),
                params: Vec::new(),
            });
            panel.ensure_table_view_controls(window, cx);
        })
    });
    window.run_until_parked();
    assert!(
        !projection_enabled(window),
        "a builder query chooses its own columns"
    );

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.builder.visual_select = None;
            panel.ensure_table_view_controls(window, cx);
        })
    });
    window.run_until_parked();
    assert!(projection_enabled(window));
}

#[gpui::test]
fn a_connection_that_cannot_project_shows_no_column_toggles(cx: &mut TestAppContext) {
    let (panel, _calls, window) = open_table(TableViewFeatures::COLUMN_PROFILE, cx);

    window.update(|_, app| {
        let view = panel
            .read(app)
            .table_views
            .profile_view
            .clone()
            .expect("a Columns view");
        assert!(!view.read(app).projection_enabled());
    });
}

#[gpui::test]
fn the_table_views_are_named_radio_buttons(cx: &mut TestAppContext) {
    use dbflux_components::accessibility::{FrameCapture, accessibility_node, accessible_name};

    init_test_runtime(cx);
    let calls = Arc::new(Mutex::new(Calls::default()));
    let (app_state, profile_id) = app_state_with(all_features(), calls, cx);

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::with_schema("events", "page_views"),
                None,
                app_state,
                window,
                cx,
            )
        });

        gpui_component::Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    let columns = dbflux_i18n::t!("document.data.table_views.tab.columns").to_string();
    assert_eq!(
        accessible_name(&frame, "segmented-table-view-columns"),
        Some(columns)
    );

    let data = accessibility_node(&frame, "segmented-table-view-data")
        .expect("the Data view is an accessible node");
    assert_eq!(data.role(), gpui::Role::RadioButton);
    assert_eq!(data.toggled(), Some(gpui::Toggled::True));

    assert!(
        accessibility_node(&frame, "segmented-table-view-partitions").is_some(),
        "every view the connection offers is announced"
    );
}
