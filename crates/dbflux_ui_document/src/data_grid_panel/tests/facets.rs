//! The grid reads views, gates and copy from the facets a connection
//! declares for the object it shows, not from the connection's category.

use super::*;
use dbflux_core::{
    CollectionPresentation, ColumnInfo, DatabaseCategory, DriverCapabilities, DriverMetadata,
    Icon as CoreIcon, ObjectFacet, QueryLanguage, TableBrowseRequest, TableInfo, TableRef,
    TimeSeriesFacet, TransferFamily, Value,
};

/// A connection whose category and facets are set independently, so a test
/// can tell which of the two the grid followed.
struct FacetStub {
    metadata: DriverMetadata,
    collection_facets: Vec<ObjectFacet>,
    table_facets: Vec<ObjectFacet>,
}

impl FacetStub {
    fn new(category: DatabaseCategory) -> Self {
        Self {
            metadata: DriverMetadata {
                id: "facet-stub".to_string(),
                display_name: "Facet stub".to_string(),
                description: "test".to_string(),
                category,
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
            },
            collection_facets: vec![ObjectFacet::Tabular],
            table_facets: vec![ObjectFacet::Tabular],
        }
    }

    fn with_collection_facets(mut self, facets: Vec<ObjectFacet>) -> Self {
        self.collection_facets = facets;
        self
    }

    fn with_table_facets(mut self, facets: Vec<ObjectFacet>) -> Self {
        self.table_facets = facets;
        self
    }
}

fn column(name: &str, type_name: &str, kind: ColumnKind) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        type_name: type_name.to_string(),
        kind,
        nullable: true,
        is_primary_key: name == "id",
    }
}

fn events_page() -> QueryResult {
    QueryResult::table(
        vec![
            column("id", "int8", ColumnKind::Integer),
            column("payload", "jsonb", ColumnKind::Unknown),
        ],
        vec![vec![
            Value::Int(1),
            Value::Json(r#"{"cart": {"total": 129}}"#.into()),
        ]],
        None,
        Duration::ZERO,
    )
}

impl dbflux_core::Connection for FacetStub {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
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
        Ok(events_page())
    }

    fn browse_table(&self, _: &TableBrowseRequest) -> Result<QueryResult, dbflux_core::DbError> {
        Ok(events_page())
    }

    fn count_table(&self, _: &dbflux_core::TableCountRequest) -> Result<u64, dbflux_core::DbError> {
        Ok(1)
    }

    fn cancel(&self, _: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn table_details(
        &self,
        _database: &str,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, dbflux_core::DbError> {
        Ok(TableInfo {
            name: table.to_string(),
            schema: schema.map(str::to_string),
            columns: Some(vec![
                ColumnInfo {
                    name: "id".into(),
                    type_name: "int8".into(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    enum_values: None,
                },
                ColumnInfo {
                    name: "payload".into(),
                    type_name: "jsonb".into(),
                    nullable: true,
                    is_primary_key: false,
                    default_value: None,
                    enum_values: None,
                },
            ]),
            pseudo_columns: Box::default(),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        })
    }

    fn object_facets(&self, _table: &TableInfo) -> Vec<ObjectFacet> {
        self.table_facets.clone()
    }

    fn collection_facets(&self, _collection: &CollectionRef) -> Vec<ObjectFacet> {
        self.collection_facets.clone()
    }
}

fn collection_panel(
    stub: FacetStub,
    cx: &mut TestAppContext,
) -> (gpui::Entity<DataGridPanel>, gpui::Entity<AppStateEntity>) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(stub));
    let panel = build_panel_for_collection(cx, app_state.clone(), profile_id);
    (panel, app_state)
}

#[gpui::test]
fn a_collection_declared_as_documents_is_one_whatever_the_category(cx: &mut TestAppContext) {
    let (panel, _) = collection_panel(
        FacetStub::new(DatabaseCategory::Relational)
            .with_collection_facets(vec![ObjectFacet::Document, ObjectFacet::Tabular]),
        cx,
    );

    cx.update(|cx| {
        let panel = panel.read(cx);
        assert!(panel.is_document_collection(cx));
        assert_eq!(
            panel.view_config.mode,
            crate::data_view::DataViewMode::Document
        );
        assert_eq!(
            DataGridPanel::filter_labels_for_source(&panel.source, &panel.app_state, cx),
            ("find", "WHERE")
        );
        assert_eq!(
            DataGridPanel::filter_placeholder_for_source(&panel.source, &panel.app_state, cx),
            r#"e.g. {"name": {"$regex": "test"}}"#
        );
    });
}

#[gpui::test]
fn a_document_category_without_the_facet_is_a_grid(cx: &mut TestAppContext) {
    let (panel, _) = collection_panel(FacetStub::new(DatabaseCategory::Document), cx);

    cx.update(|cx| {
        let panel = panel.read(cx);
        assert!(!panel.is_document_collection(cx));
        assert!(!panel.can_open_builder(cx));
        assert_eq!(
            panel.view_config.mode,
            crate::data_view::DataViewMode::Table
        );
        assert_eq!(
            DataGridPanel::filter_labels_for_source(&panel.source, &panel.app_state, cx),
            ("SELECT * FROM", "WHERE")
        );
    });
}

#[gpui::test]
fn a_time_series_collection_shows_no_filter_example(cx: &mut TestAppContext) {
    let (panel, _) = collection_panel(
        FacetStub::new(DatabaseCategory::Relational).with_collection_facets(vec![
            ObjectFacet::TimeSeries(TimeSeriesFacet {
                time_column: Some("time".into()),
            }),
            ObjectFacet::Tabular,
        ]),
        cx,
    );

    cx.update(|cx| {
        let panel = panel.read(cx);
        assert!(panel.chart.time_series_source);
        assert_eq!(panel.chart.time_series_column.as_deref(), Some("time"));
        assert_eq!(
            DataGridPanel::filter_placeholder_for_source(&panel.source, &panel.app_state, cx),
            ""
        );
    });
}

fn open_events_table(
    stub: FacetStub,
    cx: &mut TestAppContext,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(stub));
    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::with_schema("public", "events"),
                None,
                app_state,
                window,
                cx,
            )
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder.borrow().clone().expect("the panel is created");
    (panel, window)
}

#[gpui::test]
fn a_table_with_a_json_column_opens_its_rows_as_documents(cx: &mut TestAppContext) {
    let (panel, window) = open_events_table(
        FacetStub::new(DatabaseCategory::Relational)
            .with_table_facets(vec![ObjectFacet::Tabular, ObjectFacet::Document]),
        cx,
    );

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.can_toggle_view(app), "Table and Document");
        assert_eq!(
            panel.view_config.mode,
            crate::data_view::DataViewMode::Table
        );
    });

    window.update(|_, app| panel.update(app, |panel, cx| panel.toggle_view_mode(cx)));
    window.run_until_parked();

    window.update(|_, app| {
        let panel = panel.read(app);
        assert_eq!(
            panel.view_config.mode,
            crate::data_view::DataViewMode::Document
        );
        let tree = panel
            .document_view
            .document_tree_state
            .clone()
            .expect("the rows are shown as documents");
        assert!(tree.read(app).is_read_only(), "edits go through the grid");
    });
}

#[gpui::test]
fn a_table_without_a_document_facet_offers_one_view(cx: &mut TestAppContext) {
    let (panel, window) = open_events_table(FacetStub::new(DatabaseCategory::Relational), cx);

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(!panel.can_toggle_view(app));
        assert!(panel.document_view.document_tree_state.is_none());
    });
}

#[gpui::test]
fn a_hypertable_learns_its_time_axis_when_the_details_arrive(cx: &mut TestAppContext) {
    let (panel, window) = open_events_table(
        FacetStub::new(DatabaseCategory::Relational).with_table_facets(vec![
            ObjectFacet::Tabular,
            ObjectFacet::TimeSeries(TimeSeriesFacet {
                time_column: Some("occurred_at".into()),
            }),
        ]),
        cx,
    );

    window.update(|_, app| {
        let panel = panel.read(app);
        assert!(panel.chart.time_series_source);
        assert_eq!(
            panel.chart.time_series_column.as_deref(),
            Some("occurred_at")
        );
    });
}

#[gpui::test]
fn a_reload_keeps_the_document_view_of_a_json_table(cx: &mut TestAppContext) {
    let (panel, window) = open_events_table(
        FacetStub::new(DatabaseCategory::Relational)
            .with_table_facets(vec![ObjectFacet::Tabular, ObjectFacet::Document]),
        cx,
    );
    window.update(|_, app| panel.update(app, |panel, cx| panel.toggle_view_mode(cx)));

    window.update(|_, app| panel.update(app, |panel, cx| panel.set_result(events_page(), cx)));

    window.update(|_, app| {
        assert_eq!(
            panel.read(app).view_config.mode,
            crate::data_view::DataViewMode::Document
        );
    });
}

/// A key found in the schema snapshot skips the primary-key fetch, but the
/// table's facets still come with its details.
#[gpui::test]
fn facets_load_when_the_key_comes_from_the_schema_snapshot(cx: &mut TestAppContext) {
    let stub = FacetStub::new(DatabaseCategory::Relational)
        .with_table_facets(vec![ObjectFacet::Tabular, ObjectFacet::Document]);
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(stub));
    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            let connected = state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the stub is connected");
            let details = connected
                .connection
                .table_details("default", Some("public"), "events")
                .expect("stub details");
            connected.database_schemas.insert(
                "public".to_string(),
                dbflux_core::DbSchemaInfo {
                    name: "public".to_string(),
                    tables: vec![details],
                    views: Vec::new(),
                    custom_types: None,
                },
            );
        });
    });

    let panel_holder = Rc::new(RefCell::new(None));
    let panel_handle = panel_holder.clone();
    let (_, window) = cx.add_window_view(|window, cx| {
        let panel = cx.new(|cx| {
            DataGridPanel::new_for_table(
                profile_id,
                TableRef::with_schema("public", "events"),
                None,
                app_state,
                window,
                cx,
            )
        });
        panel_handle.replace(Some(panel.clone()));
        Root::new(panel, window, cx)
    });
    window.run_until_parked();

    let panel = panel_holder.borrow().clone().expect("the panel is created");
    window.update(|_, app| {
        assert!(
            panel.read(app).can_toggle_view(app),
            "the Document facet arrived"
        );
    });
}
