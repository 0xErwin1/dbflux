#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use dbflux_core::{
    CollectionRef, DatabaseCategory, DocumentFeatures, DocumentOperator, DocumentQueryCodec,
    DocumentQueryMode, Pagination,
};
use dbflux_driver_mongodb::MongoDocumentCodec;
use gpui::{AppContext, TestAppContext, VisualTestContext};

use super::super::CollectionTab;
use super::BuilderSupport;
use crate::data_grid_panel::tests::register_stub_connection;
use crate::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use crate::document_builder::AccumulatorOp;
use crate::document_builder::DocumentBuilderEvent;

mod rail;
mod slot_sync;

/// A document connection whose builder support is set per test. It
/// answers no query.
struct StubDocumentConnection {
    metadata: dbflux_core::DriverMetadata,
    features: DocumentFeatures,
    codec: Option<MongoDocumentCodec>,
    /// Offsets of the collection browses asked for.
    browse_offsets: Arc<Mutex<Vec<u64>>>,
}

impl StubDocumentConnection {
    fn new(category: DatabaseCategory, features: DocumentFeatures, codec: bool) -> Self {
        Self {
            metadata: dbflux_core::DriverMetadata {
                id: "stub-documents".to_string(),
                display_name: "Stub".to_string(),
                description: "test stub".to_string(),
                category,
                transfer_family: dbflux_core::TransferFamily::Incompatible,
                deployment_class: None,
                query_language: dbflux_core::QueryLanguage::MongoQuery,
                capabilities: dbflux_core::DriverCapabilities::empty(),
                default_port: None,
                uri_scheme: "stub".to_string(),
                icon: dbflux_core::Icon::Database,
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
            features,
            codec: codec.then_some(MongoDocumentCodec),
            browse_offsets: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn logging_browse_offsets(mut self, offsets: Arc<Mutex<Vec<u64>>>) -> Self {
        self.browse_offsets = offsets;
        self
    }
}

impl dbflux_core::Connection for StubDocumentConnection {
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
        dbflux_core::DbKind::MongoDB
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

    fn execute(
        &self,
        _request: &dbflux_core::QueryRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported("stub".to_string()))
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
        Ok(dbflux_core::SchemaSnapshot::default())
    }

    fn document_features(&self) -> DocumentFeatures {
        self.features
    }

    fn estimate_collection_count(
        &self,
        _request: &dbflux_core::CollectionCountRequest,
    ) -> Result<dbflux_core::CollectionCountEstimate, dbflux_core::DbError> {
        Ok(dbflux_core::CollectionCountEstimate {
            count: 800,
            exact: false,
        })
    }

    #[expect(
        clippy::unwrap_in_result,
        reason = "test fixture: the existing panic-on-poison policy is retained for this mutex"
    )]
    fn browse_collection(
        &self,
        request: &dbflux_core::CollectionBrowseRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        self.browse_offsets
            .lock()
            .unwrap()
            .push(request.pagination.offset());

        Ok(dbflux_core::QueryResult::json(
            Vec::new(),
            Vec::new(),
            std::time::Duration::from_millis(1),
        ))
    }

    fn document_query_codec(&self) -> Option<&dyn DocumentQueryCodec> {
        self.codec
            .as_ref()
            .map(|codec| codec as &dyn DocumentQueryCodec)
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        Some(&StubGenerator)
    }

    fn aggregate_collection(
        &self,
        _request: &dbflux_core::CollectionAggregateRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Ok(dbflux_core::QueryResult::json(
            vec![dbflux_core::ColumnMeta {
                name: "_id".to_string(),
                type_name: String::new(),
                kind: dbflux_core::ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            }],
            vec![vec![dbflux_core::Value::Text("team".to_string())]],
            std::time::Duration::from_millis(1),
        ))
    }
}

/// Native text of a pipeline, so it classifies as a read and runs
/// without asking.
struct StubGenerator;

impl dbflux_core::QueryGenerator for StubGenerator {
    fn supported_categories(&self) -> &'static [dbflux_core::MutationCategory] {
        &[dbflux_core::MutationCategory::Document]
    }

    fn generate_mutation(
        &self,
        _mutation: &dbflux_core::MutationRequest,
    ) -> Option<dbflux_core::GeneratedQuery> {
        None
    }

    fn aggregate_query(
        &self,
        request: &dbflux_core::CollectionAggregateRequest,
    ) -> Option<dbflux_core::GeneratedQuery> {
        Some(dbflux_core::GeneratedQuery {
            language: dbflux_core::QueryLanguage::MongoQuery,
            text: format!(
                "db.{}.aggregate({})",
                request.collection.name,
                serde_json::Value::Array(request.pipeline.clone())
            ),
        })
    }
}

fn builder_features() -> DocumentFeatures {
    DocumentFeatures::QUERY_SLOTS | DocumentFeatures::VISUAL_BUILDER
}

fn aggregate_features() -> DocumentFeatures {
    builder_features() | DocumentFeatures::AGGREGATE
}

fn collection_panel(
    cx: &mut TestAppContext,
    connection: StubDocumentConnection,
) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
    let (app_state, profile_id) = register_stub_connection(cx, Arc::new(connection));
    let window = cx.add_empty_window();

    let panel = window.update(|window, cx| {
        cx.new(|cx| {
            let source = DataSource::Collection {
                profile_id,
                collection: CollectionRef::new("shop", "orders"),
                pagination: Pagination::default(),
                total_docs: None,
            };
            DataGridPanel::new_internal(source, app_state, vec![], window, cx)
        })
    });
    window.run_until_parked();

    (panel, window)
}

/// Counts the rail events the grid emits from now on.
fn rail_events(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Rc<RefCell<Vec<&'static str>>> {
    let events = Rc::new(RefCell::new(Vec::new()));
    let sink = events.clone();

    window.update(|_, cx| {
        cx.subscribe(panel, move |_, event: &DataGridEvent, _| match event {
            DataGridEvent::OpenInspector { .. } => sink.borrow_mut().push("open"),
            DataGridEvent::CloseInspector => sink.borrow_mut().push("close"),
            _ => {}
        })
        .detach();
    });

    events
}

fn set_slot_texts(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
    filter: &str,
    projection: &str,
) {
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| {
            grid.filter_bar
                .filter_input
                .update(cx, |input, cx| input.set_value(filter, window, cx));
            grid.collection
                .projection_input
                .update(cx, |input, cx| input.set_value(projection, window, cx));
        });
    });
}

fn slot_texts(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> (String, String) {
    window.update(|_, cx| {
        let grid = panel.read(cx);
        (
            grid.filter_bar.filter_input.read(cx).value().to_string(),
            grid.collection
                .projection_input
                .read(cx)
                .value()
                .to_string(),
        )
    })
}

fn open_builder(panel: &gpui::Entity<DataGridPanel>, window: &mut VisualTestContext) {
    window.update(|window, cx| {
        panel.update(cx, |grid, cx| grid.open_document_builder(window, cx));
    });
    window.run_until_parked();
}

fn builder(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> gpui::Entity<crate::document_builder::DocumentBuilderPanel> {
    window.update(|_, cx| {
        panel
            .read(cx)
            .collection
            .builder
            .panel
            .clone()
            .expect("builder panel")
    })
}

fn first_condition(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> (u64, String, DocumentOperator) {
    let builder = builder(panel, window);
    window.update(|_, cx| {
        let draft = builder.read(cx).draft();
        let condition = draft.conditions()[0];
        (condition.id, condition.path.clone(), condition.operator)
    })
}

fn set_query_name(
    rail: &gpui::Entity<crate::document_builder::DocumentBuilderPanel>,
    window: &mut VisualTestContext,
    name: &str,
) {
    window.update(|window, cx| {
        rail.update(cx, |rail, cx| rail.set_query_name(name, window, cx));
    });
}

fn stored_queries(
    panel: &gpui::Entity<DataGridPanel>,
    window: &mut VisualTestContext,
) -> Vec<dbflux_storage::SavedDocumentQuerySummary> {
    window.update(|_, cx| {
        let grid = panel.read(cx);
        let scope = grid.document_query_scope().expect("a collection tab");
        grid.app_state.clone().update(cx, |app, _| {
            app.saved_document_queries.list(&scope).expect("list")
        })
    })
}

fn sync_slots(panel: &gpui::Entity<DataGridPanel>, window: &mut VisualTestContext) {
    window.update(|_, cx| {
        panel.update(cx, |grid, cx| grid.sync_slots_into_document_builder(cx));
    });
}
