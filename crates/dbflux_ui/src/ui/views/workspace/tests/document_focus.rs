// Explicit imports, not `use super::*`: the parent glob together with
// `#[gpui::test]` sends the macro expansion into unbounded recursion.
use crate::ui::document::{DataDocument, Tab};
use crate::ui::views::workspace::{FocusTarget, Workspace};
use dbflux_app::keymap::ContextId;
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::sync::Arc;

/// A document connection that answers no query. It exists so a collection
/// tab renders its query bar in the workspace without a database.
struct StubDocumentConnection {
    metadata: dbflux_core::DriverMetadata,
    features: dbflux_core::DocumentFeatures,
}

impl StubDocumentConnection {
    fn new() -> Self {
        Self {
            metadata: dbflux_core::DriverMetadata {
                id: "stub-documents".to_string(),
                display_name: "Stub".to_string(),
                description: "test stub".to_string(),
                category: dbflux_core::DatabaseCategory::Document,
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
            features: dbflux_core::DocumentFeatures::QUERY_SLOTS,
        }
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

    fn document_features(&self) -> dbflux_core::DocumentFeatures {
        self.features
    }

    fn browse_collection(
        &self,
        _request: &dbflux_core::CollectionBrowseRequest,
    ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
        Ok(dbflux_core::QueryResult::json(
            Vec::new(),
            Vec::new(),
            std::time::Duration::from_millis(1),
        ))
    }
}

/// Registers the stub as the profile behind a collection tab.
fn register_stub_document_connection(
    cx: &mut TestAppContext,
) -> (Entity<AppStateEntity>, uuid::Uuid) {
    let profile_id = uuid::Uuid::new_v4();

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    cx.update(|cx| {
        app_state.update(cx, |app, _cx| {
            let profile = dbflux_core::ConnectionProfile::new(
                "test",
                dbflux_core::DbConfig::SQLite {
                    path: std::path::PathBuf::from(":memory:"),
                    connection_id: None,
                },
            );
            let connected = dbflux_core::ConnectedProfile {
                profile,
                connection: Arc::new(StubDocumentConnection::new()),
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

fn new_document_workspace(
    cx: &mut TestAppContext,
) -> (
    Entity<Workspace>,
    Entity<DataDocument>,
    &mut VisualTestContext,
) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(dbflux_ui_base::keymap::init_keymap);

    let (app_state, profile_id) = register_stub_document_connection(cx);

    let workspace_holder: std::rc::Rc<std::cell::RefCell<Option<Entity<Workspace>>>> =
        Default::default();
    let document_holder: std::rc::Rc<std::cell::RefCell<Option<Entity<DataDocument>>>> =
        Default::default();
    let workspace_ref = workspace_holder.clone();
    let document_ref = document_holder.clone();

    let (_, window) = cx.add_window_view(|window, cx| {
        let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
        workspace_ref.replace(Some(workspace.clone()));

        let document = cx.new(|cx| {
            DataDocument::new_for_collection(
                profile_id,
                dbflux_core::CollectionRef::new("shop", "orders"),
                app_state.clone(),
                window,
                cx,
            )
        });
        document_ref.replace(Some(document.clone()));

        workspace.update(cx, |workspace, cx| {
            let pane = DataDocument::into_pane(document, cx);
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
        });

        gpui_component::Root::new(workspace, window, cx)
    });

    let workspace = workspace_holder
        .borrow()
        .clone()
        .expect("workspace should be created");
    let document = document_holder
        .borrow()
        .clone()
        .expect("document should be created");

    window.run_until_parked();
    (workspace, document, window)
}

/// The first click into the document area after the sidebar held the
/// keyboard must leave the keyboard on the query-bar field the click landed
/// on. The workspace used to refocus the active document on that click,
/// which took the keyboard away from the very editor it had just focused:
/// every typed letter then ran a grid shortcut and no filter could be
/// entered on a document collection (#1234).
#[gpui::test]
fn first_click_into_the_document_area_keeps_the_query_bar_field(cx: &mut TestAppContext) {
    let (workspace, document, window) = new_document_workspace(cx);

    // The sidebar owns the keyboard, as after the user picked the collection.
    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.set_focus(FocusTarget::Sidebar, window, cx)
        })
    });
    window.run_until_parked();
    window.update(|window, _| window.activate_window());

    let slot = window
        .debug_bounds("collection-slot-filter")
        .expect("the document query bar renders its filter slot");
    // The editor fills the slot behind the keyword; click inside it.
    let inside_editor = gpui::point(slot.right() - px(20.0), slot.center().y);
    window.simulate_click(inside_editor, gpui::Modifiers::none());
    window.run_until_parked();

    let context = window.update(|_, cx| {
        document
            .read(cx)
            .data_grid_for_tests()
            .read(cx)
            .active_context(cx)
    });
    assert_eq!(
        context,
        ContextId::TextInput,
        "the first click into the document area must leave the filter field holding the keyboard"
    );
}

use gpui::px;
