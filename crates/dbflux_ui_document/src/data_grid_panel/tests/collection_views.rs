use super::*;

// ---- Time-series collections open as a chart, labelled in the driver's language ----

const STUB_FLUX_BROWSE: &str = "from(bucket: \"metrics\")\n  |> range(start: -24h)\n  |> filter(fn: (r) => r._measurement == \"system\")\n  |> limit(n: 100)";

/// Time, one numeric field and one text tag with two values (`a`, `b`),
/// interleaved newest first, as an InfluxQL browse of a measurement with
/// two series returns them.
fn time_series_rows() -> QueryResult {
    let column = |name: &str, kind: ColumnKind| ColumnMeta {
        name: name.to_string(),
        type_name: String::new(),
        kind,
        nullable: true,
        is_primary_key: false,
    };

    let now = chrono::Utc::now();

    QueryResult::table(
        vec![
            column("time", ColumnKind::Timestamp),
            column("load", ColumnKind::Float),
            column("host", ColumnKind::Text),
        ],
        (0..6)
            .map(|offset| {
                let host = if offset % 2 == 0 { "a" } else { "b" };
                vec![
                    dbflux_core::Value::DateTime(now - chrono::Duration::seconds(offset)),
                    dbflux_core::Value::Float(offset as f64),
                    dbflux_core::Value::Text(host.to_string()),
                ]
            })
            .collect(),
        None,
        Duration::ZERO,
    )
}

struct StubBrowseQueryGenerator;

impl dbflux_core::QueryGenerator for StubBrowseQueryGenerator {
    fn supported_categories(&self) -> &'static [dbflux_core::MutationCategory] {
        &[]
    }

    fn generate_mutation(
        &self,
        _mutation: &dbflux_core::MutationRequest,
    ) -> Option<dbflux_core::GeneratedQuery> {
        None
    }

    fn collection_browse_query(
        &self,
        _request: &dbflux_core::CollectionBrowseRequest,
    ) -> Option<dbflux_core::GeneratedQuery> {
        Some(dbflux_core::GeneratedQuery {
            language: dbflux_core::QueryLanguage::Flux,
            text: STUB_FLUX_BROWSE.to_string(),
        })
    }
}

/// Time-series connection whose collection browse answers with
/// `time_series_rows` and whose generator describes the browse in Flux.
struct StubTimeSeriesConnection {
    metadata: dbflux_core::DriverMetadata,
    generator: StubBrowseQueryGenerator,
}

impl dbflux_core::Connection for StubTimeSeriesConnection {
    /// Facets as a driver of the stub's category declares them.
    fn collection_facets(
        &self,
        _collection: &dbflux_core::CollectionRef,
    ) -> Vec<dbflux_core::ObjectFacet> {
        dbflux_core::category_collection_facets(self.metadata().category)
    }

    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &self.metadata
    }

    fn kind(&self) -> dbflux_core::DbKind {
        dbflux_core::DbKind::InfluxDB
    }

    fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
        dbflux_core::SchemaLoadingStrategy::SingleDatabase
    }

    #[expect(
        clippy::unimplemented,
        reason = "test-only stub connection: no test path in this suite calls the dialect"
    )]
    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        unimplemented!("StubTimeSeriesConnection::dialect not needed for this test")
    }

    fn ping(&self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn execute(
        &self,
        _req: &dbflux_core::QueryRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn browse_collection(
        &self,
        _request: &dbflux_core::CollectionBrowseRequest,
    ) -> Result<QueryResult, dbflux_core::DbError> {
        Ok(time_series_rows())
    }

    fn count_collection(
        &self,
        _request: &dbflux_core::CollectionCountRequest,
    ) -> Result<u64, dbflux_core::DbError> {
        Ok(6)
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        Some(&self.generator)
    }
}

fn register_time_series_connection(
    cx: &mut TestAppContext,
) -> (gpui::Entity<AppStateEntity>, Uuid) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::TimeSeries,
        dbflux_core::QueryLanguage::Flux,
        None,
    );

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let connected = app
                .connections_mut()
                .get_mut(&profile_id)
                .expect("stub profile is registered");
            let metadata = connected.connection.metadata().clone();

            connected.connection = Arc::new(StubTimeSeriesConnection {
                metadata,
                generator: StubBrowseQueryGenerator,
            });
        });
    });

    (app_state, profile_id)
}

fn open_collection_panel(
    cx: &mut TestAppContext,
    app_state: gpui::Entity<AppStateEntity>,
    profile_id: Uuid,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id,
                collection: CollectionRef::new("metrics", "system"),
                pagination: Pagination::default(),
                total_docs: None,
            };

            DataGridPanel::new_internal(source, app_state.clone(), vec![], window, cx)
        });

        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });

    let panel = panel_holder
        .borrow()
        .clone()
        .expect("panel should be created");

    (panel, window)
}

/// A time-series collection's chart tracks the row behind each point, and
/// "Show in tree" selects that row in the grid below the chart, keeping
/// the chart above it.
#[gpui::test]
fn a_time_series_collection_chart_shows_a_point_in_its_grid(cx: &mut TestAppContext) {
    use dbflux_components::chart::DataPointRef;

    let (app_state, profile_id) = register_time_series_connection(cx);
    let (panel, window) = open_collection_panel(cx, app_state.clone(), profile_id);

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.refresh(window, cx));
    });
    window.run_until_parked();

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.ensure_chart_view(cx);

            let point = DataPointRef {
                series_idx: 0,
                point_idx_in_series: 0,
            };
            let source = panel
                .chart_host_source_for_point(point, cx)
                .expect("a collection chart records the row behind each point");

            panel.chart_host_scroll_to_row(source.row_idx, window, cx);

            let active_row = panel
                .grid_table
                .table_state
                .as_ref()
                .and_then(|state| state.read(cx).selection().active)
                .map(|cell| cell.row);
            assert_eq!(active_row, Some(source.row_idx));
            assert_eq!(panel.result_view_mode(), super::super::ResultViewMode::Both);
        });
    });
}

#[gpui::test]
fn time_series_collection_browse_opens_as_chart_and_grid_with_the_driver_query(
    cx: &mut TestAppContext,
) {
    let (app_state, profile_id) = register_time_series_connection(cx);
    let (panel, window) = open_collection_panel(cx, app_state.clone(), profile_id);

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.refresh(window, cx));
    });
    window.run_until_parked();

    window.update(|_, app| {
        let panel = panel.read(app);

        assert_eq!(
            panel.result_view_mode(),
            super::super::ResultViewMode::Both,
            "a chartable time-series collection must open as the chart above the grid"
        );
        assert!(panel.uses_result_view(), "the chart view must render");
        assert_eq!(
            panel.available_result_view_modes(app),
            vec![
                super::super::ResultViewMode::Table,
                super::super::ResultViewMode::Json,
                super::super::ResultViewMode::Chart,
                super::super::ResultViewMode::Both,
            ],
            "the table and the chart alone stay one click away, Chart after the \
             shape's own views (Data | JSON | Chart)"
        );
        assert_eq!(
            panel.view_config.mode,
            crate::data_view::DataViewMode::Table,
            "the Data view of a time-series collection is the grid, not the document tree"
        );
        assert_eq!(
            panel.source_query_labels(app),
            (
                "",
                "from(bucket: \"metrics\") |> range(start: -24h) |> filter(fn: (r) => r._measurement == \"system\") |> limit(n: 100)"
                    .to_string()
            ),
            "the toolbar must show the driver's own browse query"
        );
    });

    let task = cx.update(|cx| {
        app_state
            .read(cx)
            .tasks()
            .recent_tasks(10)
            .into_iter()
            .find(|task| task.kind == dbflux_core::TaskKind::Query)
            .expect("the browse runs as a query task")
    });

    assert!(
        task.description.starts_with("from(bucket: \"metrics\")"),
        "the status bar must name the driver query, not a generic verb: {}",
        task.description
    );
    assert_eq!(task.query_text.as_deref(), Some(STUB_FLUX_BROWSE));
}

#[gpui::test]
fn time_series_collection_refresh_keeps_the_view_the_user_picked(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_time_series_connection(cx);
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.refresh(window, cx));
    });
    window.run_until_parked();

    window.update(|window, app| {
        panel.update(app, |panel, cx| {
            panel.set_result_view_mode(super::super::ResultViewMode::Table, cx);
            panel.refresh(window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, app| {
        assert_eq!(
            panel.read(app).result_view_mode(),
            super::super::ResultViewMode::Table,
            "a refresh must not flip the Data view back to the chart"
        );
    });
}

/// The chart stats rail leaves the grid for a workspace island only when
/// the host forwards the grid's side panels; otherwise it stays docked.
#[gpui::test]
fn the_stats_rail_is_a_side_panel_only_for_a_forwarding_host(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_time_series_connection(cx);
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.refresh(window, cx));
    });
    window.run_until_parked();
    window.update(|window, _| window.refresh());
    window.run_until_parked();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let shell = panel
                .chart
                .chart_shell
                .clone()
                .expect("the chart view builds its shell on render");
            shell.update(cx, |shell, _| shell.chart_rail_open = true);
        });
    });

    let panel_ids = |window: &mut gpui::VisualTestContext| {
        window.update(|_, app| {
            panel.update(app, |panel, cx| {
                panel
                    .side_panels(cx)
                    .into_iter()
                    .map(|side| (side.id.to_string(), side.width))
                    .collect::<Vec<_>>()
            })
        })
    };

    assert!(
        panel_ids(window).is_empty(),
        "a grid whose host does not forward side panels keeps the rail docked"
    );

    window.update(|_, app| {
        panel.update(app, |panel, _| panel.set_side_panels_hosted(true));
    });
    assert_eq!(
        panel_ids(window),
        vec![("grid-chart-stats".to_string(), gpui::px(320.0))]
    );

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.set_result_view_mode(super::super::ResultViewMode::Table, cx);
        });
    });
    assert!(
        panel_ids(window).is_empty(),
        "the rail belongs to the chart, so the table view shows none"
    );
}

#[gpui::test]
fn document_collection_opens_as_a_tree_without_result_views(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        None,
    );
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_collection_result(
                profile_id,
                CollectionRef::new("metrics", "system"),
                Pagination::default(),
                None,
                time_series_rows(),
                cx,
            );

            assert_eq!(
                panel.result_view_mode(),
                super::super::ResultViewMode::Table
            );
            assert!(!panel.uses_result_view());
            assert!(panel.available_result_view_modes(cx).is_empty());
            assert_eq!(
                panel.view_config.mode,
                crate::data_view::DataViewMode::Document,
                "a document collection opens in the tree"
            );
            assert!(
                panel.collection.raw.is_some(),
                "a document collection keeps the driver's page next to the flattened grid"
            );
            assert_eq!(
                panel.available_view_modes(cx),
                vec![
                    crate::data_view::DataViewMode::Document,
                    crate::data_view::DataViewMode::Table,
                    crate::data_view::DataViewMode::Json,
                ]
            );
            assert_eq!(
                panel.source_query_labels(cx),
                ("find", "metrics.system".to_string()),
                "a driver without a browse query keeps the generic label"
            );
        });
    });
}

#[gpui::test]
fn document_collection_keeps_the_view_the_user_picked_across_pages(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        None,
    );
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_collection_result(
                profile_id,
                CollectionRef::new("metrics", "system"),
                Pagination::default(),
                None,
                nested_document_rows(),
                cx,
            );
            panel.set_document_view_mode(crate::data_view::DataViewMode::Table, cx);

            panel.apply_collection_result(
                profile_id,
                CollectionRef::new("metrics", "system"),
                Pagination::default(),
                None,
                nested_document_rows(),
                cx,
            );

            assert_eq!(
                panel.view_config.mode,
                crate::data_view::DataViewMode::Table,
                "a refresh or a new page keeps the picked view"
            );
        });
    });
}

#[gpui::test]
fn empty_project_and_sort_slots_show_their_placeholders(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        None,
    );
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|_, app| {
        let collection = &panel.read(app).collection;

        let projection = collection.projection_input.read(app);
        assert!(projection.value().is_empty());
        assert_eq!(
            projection.presentation().placeholder().as_ref(),
            dbflux_i18n::t!("document.collection.slot.project_placeholder")
        );

        let sort = collection.sort_input.read(app);
        assert!(sort.value().is_empty());
        assert_eq!(
            sort.presentation().placeholder().as_ref(),
            dbflux_i18n::t!("document.collection.slot.sort_placeholder")
        );
    });
}

#[gpui::test]
fn non_document_collection_still_opens_as_a_table(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::KeyValue,
        dbflux_core::QueryLanguage::RedisCommands,
        None,
    );
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|_, app| {
        assert_eq!(
            panel.read(app).view_config.mode,
            crate::data_view::DataViewMode::Table
        );
    });
}

#[gpui::test]
fn document_collection_expands_objects_and_steps_into_arrays(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        None,
    );
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    let column_names = |panel: &DataGridPanel| -> Vec<String> {
        panel
            .result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    };

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_collection_result(
                profile_id,
                CollectionRef::new("metrics", "system"),
                Pagination::default(),
                None,
                nested_document_rows(),
                cx,
            );

            assert_eq!(column_names(panel), vec!["_id", "price", "items"]);

            panel.toggle_document_column_group(1, cx);
            assert_eq!(
                column_names(panel),
                vec!["_id", "price.amount", "price.currency", "items"]
            );

            let model = panel
                .grid_table
                .table_state
                .as_ref()
                .map(|state| state.read(cx).model().clone())
                .expect("table state");
            assert!(
                model.cell(1, 2).is_some_and(|cell| cell.is_missing()),
                "the second product has no currency"
            );
            assert!(model.cell(0, 3).is_some_and(|cell| cell.is_nested()));

            panel.step_into_document_value(0, 3, cx);
            assert!(panel.is_stepped_into());
            assert_eq!(column_names(panel), vec!["sku"]);
            assert_eq!(panel.result.rows.len(), 2);

            panel.step_out_of_document_value(cx);
            assert!(!panel.is_stepped_into());
            assert_eq!(column_names(panel), vec!["_id", "price", "items"]);
        });
    });
}

#[gpui::test]
fn local_sort_after_stepping_into_a_longer_array_keeps_every_row(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_builder_stub_connection(
        cx,
        dbflux_core::DatabaseCategory::Document,
        dbflux_core::QueryLanguage::MongoQuery,
        None,
    );
    use dbflux_core::Value;
    use std::collections::BTreeMap;

    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    let column = |name: &str, is_primary_key: bool| ColumnMeta {
        name: name.to_string(),
        type_name: "BSON".to_string(),
        kind: ColumnKind::Unknown,
        nullable: true,
        is_primary_key,
    };
    let items = Value::Array(
        ["a", "b", "c"]
            .into_iter()
            .map(|sku| {
                Value::Document(BTreeMap::from([(
                    "sku".to_string(),
                    Value::Text(sku.into()),
                )]))
            })
            .collect(),
    );
    // Two documents, but the first one's array has three elements, so
    // stepping into it yields more rows than the sorted page had.
    let documents = QueryResult::json(
        vec![column("_id", true), column("items", false)],
        vec![
            vec![Value::Int(1), items.clone()],
            vec![Value::Int(2), items],
        ],
        Duration::ZERO,
    );

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            panel.apply_collection_result(
                profile_id,
                CollectionRef::new("metrics", "system"),
                Pagination::default(),
                None,
                documents,
                cx,
            );
            assert_eq!(panel.result.rows.len(), 2);

            panel.apply_local_sort(0, dbflux_core::SortDirection::Descending, cx);
            panel.step_into_document_value(0, 1, cx);
            assert!(panel.is_stepped_into());
            assert_eq!(panel.result.rows.len(), 3);

            panel.apply_local_sort(0, dbflux_core::SortDirection::Descending, cx);
            assert_eq!(panel.result.rows.len(), 3);
        });
    });
}

#[gpui::test]
fn time_series_collection_chart_draws_one_line_per_tag_value(cx: &mut TestAppContext) {
    let (app_state, profile_id) = register_time_series_connection(cx);
    let (panel, window) = open_collection_panel(cx, app_state, profile_id);

    window.update(|window, app| {
        panel.update(app, |panel, cx| panel.refresh(window, cx));
    });
    window.run_until_parked();

    window.update(|_, app| {
        panel.update(app, |panel, cx| {
            let shell = panel
                .chart
                .chart_shell
                .clone()
                .expect("a chartable result creates the chart shell");
            let bindings = shell.read(cx).active_bindings();

            assert_eq!(bindings.x, 0, "time on X");
            assert_eq!(bindings.y, vec![1], "the first field on Y");
            assert_eq!(
                bindings.group_by,
                Some(2),
                "the first text column (the tag) groups the chart"
            );

            let chart = panel
                .ensure_chart_view(cx)
                .expect("the chart view builds from the browse result");
            let labels: Vec<String> = chart
                .read(cx)
                .spec_series()
                .iter()
                .map(|series| series.label.clone())
                .collect();

            assert_eq!(labels, vec!["a".to_string(), "b".to_string()]);
        });
    });
}
