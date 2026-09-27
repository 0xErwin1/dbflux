//! The Builder toggle of a document collection and the rail it opens.
//!
//! The grid owns the query slots; the builder panel owns its draft. While
//! the rail is open every slot change is read into the builder, and every
//! builder edit comes back as a [`DocumentBuilderEvent::WriteSlots`]. Find
//! runs the slots through the ordinary collection browse, so results keep
//! their `_id` editability, count and history. The toggle is offered only
//! when the driver reports `DocumentFeatures::VISUAL_BUILDER` and returns a
//! codec; other document drivers show it disabled.

use std::sync::Arc;

use dbflux_components::controls::{Button, InputEvent};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone};
use dbflux_core::{Connection, DatabaseCategory, DocumentFeatures, DocumentFindSlots, Pagination};
use gpui::*;

use super::CollectionTab;
use crate::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use crate::document_builder::{DocumentBuilderEvent, DocumentBuilderPanel, SlotWrite};

/// Whether a collection offers the visual builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data_grid_panel) enum BuilderSupport {
    /// Not a document collection: no toggle.
    Hidden,
    /// A document driver without a builder: the toggle shows, disabled.
    Unsupported,
    Available,
}

/// The builder rail of one collection tab.
#[derive(Default)]
pub(in crate::data_grid_panel) struct DocumentBuilderState {
    /// Kept across closes so the draft survives reopening.
    pub panel: Option<Entity<DocumentBuilderPanel>>,
    /// Whether the builder owns the inspector rail.
    pub open: bool,
    _subscriptions: Vec<Subscription>,
}

impl DataGridPanel {
    fn collection_connection(&self, cx: &App) -> Option<Arc<dyn Connection>> {
        let DataSource::Collection { profile_id, .. } = &self.source else {
            return None;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .map(|connected| connected.connection.clone())
    }

    pub(in crate::data_grid_panel) fn document_builder_support(&self, cx: &App) -> BuilderSupport {
        let Some((category, features)) = self.collection_capabilities(cx) else {
            return BuilderSupport::Hidden;
        };

        if category != DatabaseCategory::Document {
            return BuilderSupport::Hidden;
        }

        let has_codec = self
            .collection_connection(cx)
            .is_some_and(|connection| connection.document_query_codec().is_some());
        let features_offered =
            features.contains(DocumentFeatures::VISUAL_BUILDER | DocumentFeatures::QUERY_SLOTS);

        if features_offered && has_codec {
            BuilderSupport::Available
        } else {
            BuilderSupport::Unsupported
        }
    }

    pub fn document_builder_is_open(&self) -> bool {
        self.collection.builder.open
    }

    /// The Builder toggle: opens the rail, or closes it when open.
    pub(in crate::data_grid_panel) fn toggle_document_builder(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.collection.builder.open {
            self.close_document_builder(cx);
        } else {
            self.open_document_builder(window, cx);
        }
    }

    /// Opens the builder in the inspector rail, reading the slots into it.
    pub(in crate::data_grid_panel) fn open_document_builder(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.document_builder_support(cx) != BuilderSupport::Available {
            return;
        }

        let Some(connection) = self.collection_connection(cx) else {
            return;
        };
        let Some(collection) = self.source.collection_ref().cloned() else {
            return;
        };

        // The builder and the document panel share one rail.
        self.clear_inspector_state(cx);

        let panel = match self.collection.builder.panel.clone() {
            Some(panel) => panel,
            None => self.create_document_builder(connection, collection, window, cx),
        };

        match (&self.collection.schema, &self.collection.schema_load) {
            (Some(sample), _) => panel.update(cx, |builder, cx| builder.set_schema(sample, cx)),
            (None, super::SchemaLoad::Loading) => {
                panel.update(cx, |builder, cx| builder.set_sampling(true, cx))
            }
            (None, _) => self.load_collection_schema(cx),
        }

        self.collection.builder.open = true;
        self.sync_slots_into_document_builder(cx);
        self.mount_document_builder(&panel, cx);
        cx.notify();
    }

    fn create_document_builder(
        &mut self,
        connection: Arc<dyn Connection>,
        collection: dbflux_core::CollectionRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<DocumentBuilderPanel> {
        let panel = cx.new(|cx| DocumentBuilderPanel::new(connection, collection, window, cx));

        let mut subscriptions = vec![cx.subscribe_in(
            &panel,
            window,
            |this, _, event: &DocumentBuilderEvent, window, cx| {
                this.handle_document_builder_event(event, window, cx);
            },
        )];

        let on_slot_change = |this: &mut Self, event: &InputEvent, cx: &mut Context<Self>| {
            if matches!(event, InputEvent::Change) {
                this.sync_slots_into_document_builder(cx);
            }
        };

        subscriptions.push(cx.subscribe(
            &self.filter_bar.filter_input,
            move |this, _, event: &InputEvent, cx| on_slot_change(this, event, cx),
        ));
        subscriptions.push(cx.subscribe(
            &self.collection.projection_input,
            move |this, _, event: &InputEvent, cx| on_slot_change(this, event, cx),
        ));
        subscriptions.push(cx.subscribe(
            &self.collection.sort_input,
            move |this, _, event: &InputEvent, cx| on_slot_change(this, event, cx),
        ));
        subscriptions.push(cx.subscribe(
            &self.filter_bar.limit_input,
            move |this, _, event: &InputEvent, cx| on_slot_change(this, event, cx),
        ));

        self.collection.builder.panel = Some(panel.clone());
        self.collection.builder._subscriptions = subscriptions;

        panel
    }

    /// Shows `panel` in the workspace inspector rail.
    fn mount_document_builder(&self, panel: &Entity<DocumentBuilderPanel>, cx: &mut Context<Self>) {
        cx.emit(DataGridEvent::OpenInspector {
            title: SharedString::from(dbflux_i18n::t!("document.collection.builder.title")),
            content: AnyView::from(panel.clone()),
            content_has_header: true,
        });
    }

    /// Re-mounts the builder when its tab becomes active again. Returns
    /// whether it took the rail.
    pub(in crate::data_grid_panel) fn remount_document_builder(
        &self,
        cx: &mut Context<Self>,
    ) -> bool {
        match self
            .collection
            .builder
            .panel
            .clone()
            .filter(|_| self.collection.builder.open)
        {
            Some(panel) => {
                self.mount_document_builder(&panel, cx);
                true
            }
            None => false,
        }
    }

    fn close_document_builder(&mut self, cx: &mut Context<Self>) {
        self.collection.builder.open = false;
        cx.emit(DataGridEvent::CloseInspector);
        cx.notify();
    }

    /// Records that something else took the rail.
    pub(in crate::data_grid_panel) fn mark_document_builder_closed(&mut self) {
        self.collection.builder.open = false;
    }

    /// The query slots as the codec reads them. The slots carry no skip.
    fn document_slots(&self, cx: &App) -> DocumentFindSlots {
        let limit_text = self.filter_bar.limit_input.read(cx).value().to_string();

        DocumentFindSlots {
            filter: self.filter_bar.filter_input.read(cx).value().to_string(),
            projection: self
                .collection
                .projection_input
                .read(cx)
                .value()
                .to_string(),
            sort: self.collection.sort_input.read(cx).value().to_string(),
            limit: limit_text.trim().parse::<u64>().ok(),
            skip: None,
        }
    }

    /// Reads the slots into the open builder.
    pub(in crate::data_grid_panel) fn sync_slots_into_document_builder(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        if !self.collection.builder.open {
            return;
        }
        let Some(panel) = self.collection.builder.panel.clone() else {
            return;
        };

        let slots = self.document_slots(cx);
        panel.update(cx, |builder, cx| builder.read_slots(slots, cx));
    }

    pub(in crate::data_grid_panel) fn handle_document_builder_event(
        &mut self,
        event: &DocumentBuilderEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            DocumentBuilderEvent::WriteSlots(write) => {
                self.write_document_slots(write, window, cx);
            }
            DocumentBuilderEvent::FindRequested => {
                self.collection.tab = CollectionTab::Documents;
                self.find_documents(window, cx);
            }
            DocumentBuilderEvent::OpenInEditorRequested(text) => {
                if let DataSource::Collection { profile_id, .. } = &self.source {
                    cx.emit(DataGridEvent::OpenEditorWithContent {
                        profile_id: *profile_id,
                        sql: text.clone(),
                    });
                }
            }
            DocumentBuilderEvent::CloseRequested => self.close_document_builder(cx),
        }
    }

    fn write_document_slots(
        &mut self,
        write: &SlotWrite,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(text) = &write.filter {
            self.filter_bar
                .filter_input
                .update(cx, |input, cx| input.set_value(text, window, cx));
        }
        if let Some(text) = &write.projection {
            self.collection
                .projection_input
                .update(cx, |input, cx| input.set_value(text, window, cx));
        }
        if let Some(text) = &write.sort {
            self.collection
                .sort_input
                .update(cx, |input, cx| input.set_value(text, window, cx));
        }
        if let Some(text) = &write.limit {
            self.filter_bar
                .limit_input
                .update(cx, |input, cx| input.set_value(text, window, cx));
        }

        cx.notify();
    }

    /// Where a Find starts reading: the builder's skip while it is open.
    pub(in crate::data_grid_panel) fn document_find_pagination(&self, cx: &App) -> Pagination {
        let current = match &self.source {
            DataSource::Collection { pagination, .. } => pagination.clone(),
            _ => Pagination::default(),
        };

        let limit = self
            .filter_bar
            .limit_input
            .read(cx)
            .value()
            .trim()
            .parse::<u32>()
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or_else(|| current.limit());

        let skip = self
            .collection
            .builder
            .panel
            .as_ref()
            .filter(|_| self.collection.builder.open)
            .and_then(|panel| panel.read(cx).skip())
            .unwrap_or(0);

        Pagination::Offset {
            limit,
            offset: skip,
        }
    }

    /// Footer note while the builder drives a find: its rows stay editable.
    pub(in crate::data_grid_panel) fn document_builder_footer(&self, cx: &App) -> Option<String> {
        (self.collection.builder.open && self.commits_document_patches(cx))
            .then(|| dbflux_i18n::t!("document.collection.builder.editable_note"))
    }

    /// The Builder toggle in the collection header.
    pub(in crate::data_grid_panel) fn render_document_builder_toggle(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let support = self.document_builder_support(cx);
        if support == BuilderSupport::Hidden {
            return None;
        }

        let button = Button::new(
            "collection-builder-toggle",
            dbflux_i18n::t!("document.collection.builder.toggle"),
        )
        .secondary()
        .icon(AppIcon::ListFilter)
        .selected(self.collection.builder.open)
        .tab_stop(false);

        let button = match support {
            BuilderSupport::Available => button
                .tooltip(dbflux_i18n::t!(
                    "document.collection.builder.toggle_tooltip"
                ))
                .on_click(cx.listener(|this, _, window, cx| {
                    this.toggle_document_builder(window, cx);
                })),
            BuilderSupport::Unsupported | BuilderSupport::Hidden => button
                .disabled(true)
                .tooltip(dbflux_i18n::t!("document.collection.builder.unsupported")),
        };

        Some(button.into_any_element())
    }

    /// Chip after the slots: whether they are in sync with the open builder.
    pub(in crate::data_grid_panel) fn render_document_builder_sync_chip(
        &self,
        cx: &App,
    ) -> Option<AnyElement> {
        if !self.collection.builder.open {
            return None;
        }

        let conflicted = self
            .collection
            .builder
            .panel
            .as_ref()
            .is_some_and(|panel| panel.read(cx).is_conflicted());

        let (id, badge) = if conflicted {
            (
                "collection-builder-out-of-sync",
                Badge::new(
                    dbflux_i18n::t!("document.collection.builder.out_of_sync"),
                    BadgeTone::Warning,
                )
                .icon(AppIcon::TriangleAlert),
            )
        } else {
            (
                "collection-builder-synced",
                Badge::new(
                    dbflux_i18n::t!("document.collection.builder.synced"),
                    BadgeTone::Success,
                )
                .icon(AppIcon::Link2),
            )
        };

        Some(div().id(id).flex_shrink_0().child(badge).into_any_element())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    use dbflux_core::{
        CollectionRef, DatabaseCategory, DocumentFeatures, DocumentOperator, DocumentQueryCodec,
        Pagination,
    };
    use dbflux_driver_mongodb::MongoDocumentCodec;
    use gpui::{AppContext, TestAppContext, VisualTestContext};

    use super::BuilderSupport;
    use crate::data_grid_panel::tests::register_stub_connection;
    use crate::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
    use crate::document_builder::DocumentBuilderEvent;

    /// A document connection whose builder support is set per test. It
    /// answers no query.
    struct StubDocumentConnection {
        metadata: dbflux_core::DriverMetadata,
        features: DocumentFeatures,
        codec: Option<MongoDocumentCodec>,
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
            }
        }
    }

    impl dbflux_core::Connection for StubDocumentConnection {
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

        fn document_query_codec(&self) -> Option<&dyn DocumentQueryCodec> {
            self.codec
                .as_ref()
                .map(|codec| codec as &dyn DocumentQueryCodec)
        }
    }

    fn builder_features() -> DocumentFeatures {
        DocumentFeatures::QUERY_SLOTS | DocumentFeatures::VISUAL_BUILDER
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

    #[gpui::test]
    fn the_builder_is_offered_only_with_the_feature_and_a_codec(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        let support = window.update(|_, cx| panel.read(cx).document_builder_support(cx));
        assert_eq!(support, BuilderSupport::Available);

        let mut cx = TestAppContext::single();
        let (panel, window) = collection_panel(
            &mut cx,
            StubDocumentConnection::new(
                DatabaseCategory::Document,
                DocumentFeatures::QUERY_SLOTS,
                false,
            ),
        );
        let support = window.update(|_, cx| panel.read(cx).document_builder_support(cx));
        assert_eq!(support, BuilderSupport::Unsupported);

        let mut cx = TestAppContext::single();
        let (panel, window) = collection_panel(
            &mut cx,
            StubDocumentConnection::new(DatabaseCategory::KeyValue, builder_features(), true),
        );
        let support = window.update(|_, cx| panel.read(cx).document_builder_support(cx));
        assert_eq!(support, BuilderSupport::Hidden);
    }

    #[gpui::test]
    fn an_unsupported_driver_never_opens_an_empty_rail(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(
                DatabaseCategory::Document,
                DocumentFeatures::QUERY_SLOTS | DocumentFeatures::VISUAL_BUILDER,
                false,
            ),
        );
        let events = rail_events(&panel, window);

        open_builder(&panel, window);

        window.update(|_, cx| {
            let grid = panel.read(cx);
            assert!(!grid.document_builder_is_open());
            assert!(grid.collection.builder.panel.is_none());
        });
        assert!(events.borrow().is_empty());
    }

    #[gpui::test]
    fn opening_reads_the_slots_and_mounts_the_rail(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
        let events = rail_events(&panel, window);

        open_builder(&panel, window);

        assert_eq!(*events.borrow(), vec!["open"]);
        assert!(window.update(|_, cx| panel.read(cx).document_builder_is_open()));
        assert_eq!(
            first_condition(&panel, window),
            (
                first_condition(&panel, window).0,
                "age".to_string(),
                DocumentOperator::Gt
            )
        );
    }

    #[gpui::test]
    fn a_builder_edit_rewrites_only_the_slot_it_changed(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "{ name: 1 }");
        open_builder(&panel, window);

        let (id, _, _) = first_condition(&panel, window);
        let builder = builder(&panel, window);
        window.update(|_, cx| {
            builder.update(cx, |builder, cx| {
                builder.set_operator(id, DocumentOperator::Gte, cx)
            });
        });
        window.run_until_parked();

        assert_eq!(
            slot_texts(&panel, window),
            (
                r#"{"age": {"$gte": 30}}"#.to_string(),
                "{ name: 1 }".to_string()
            )
        );
    }

    #[gpui::test]
    fn a_slot_edit_reads_back_into_the_builder(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        open_builder(&panel, window);

        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        window.update(|_, cx| {
            panel.update(cx, |grid, cx| grid.sync_slots_into_document_builder(cx));
        });

        let (_, path, operator) = first_condition(&panel, window);
        assert_eq!(path, "status");
        assert_eq!(operator, DocumentOperator::Eq);
    }

    #[gpui::test]
    fn a_slot_the_builder_cannot_read_waits_for_a_rewrite(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        let original = r#"{"$expr": {"$gt": ["$total", "$limit"]}, "c": 1}"#;
        set_slot_texts(&panel, window, original, "");
        open_builder(&panel, window);

        let builder = builder(&panel, window);
        assert!(window.update(|_, cx| builder.read(cx).is_conflicted()));

        let (id, path, _) = first_condition(&panel, window);
        assert_eq!(path, "c");
        window.update(|_, cx| {
            builder.update(cx, |builder, cx| {
                builder.set_operator(id, DocumentOperator::Ne, cx)
            });
        });
        window.run_until_parked();

        assert_eq!(slot_texts(&panel, window).0, original);
        assert!(!window.update(|_, cx| builder.read(cx).can_find()));

        window.update(|_, cx| {
            builder.update(cx, |builder, cx| builder.rewrite_from_builder(cx));
        });
        window.run_until_parked();

        assert_eq!(slot_texts(&panel, window).0, r#"{"c": {"$ne": 1}}"#);
        window.update(|_, cx| {
            assert!(!builder.read(cx).is_conflicted());
            assert!(builder.read(cx).can_find());
        });
    }

    #[gpui::test]
    fn find_starts_at_the_builder_skip(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        open_builder(&panel, window);

        let builder = builder(&panel, window);
        window.update(|window, cx| {
            builder.update(cx, |builder, cx| builder.set_skip(Some(40), cx));
            panel.update(cx, |grid, cx| {
                grid.filter_bar
                    .limit_input
                    .update(cx, |input, cx| input.set_value("20", window, cx));
            });
        });

        let start = window.update(|_, cx| panel.read(cx).document_find_pagination(cx));
        assert_eq!(
            start,
            Pagination::Offset {
                limit: 20,
                offset: 40,
            }
        );

        window.update(|_, cx| {
            panel.update(cx, |grid, _| grid.mark_document_builder_closed());
        });
        let start = window.update(|_, cx| panel.read(cx).document_find_pagination(cx));
        assert_eq!(start.offset(), 0, "a closed builder does not skip");
    }

    #[gpui::test]
    fn closing_the_rail_keeps_the_draft(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{"a": 1}"#, "");
        open_builder(&panel, window);
        let events = rail_events(&panel, window);

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| {
                grid.handle_document_builder_event(
                    &DocumentBuilderEvent::CloseRequested,
                    window,
                    cx,
                );
            });
        });

        assert_eq!(*events.borrow(), vec!["close"]);
        window.update(|_, cx| {
            let grid = panel.read(cx);
            assert!(!grid.document_builder_is_open());
            assert!(grid.collection.builder.panel.is_some());
        });

        open_builder(&panel, window);
        window.update(|_, cx| {
            panel.update(cx, |grid, cx| grid.clear_inspector_state(cx));
        });
        assert!(!window.update(|_, cx| panel.read(cx).document_builder_is_open()));
    }
}
