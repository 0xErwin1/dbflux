//! The Builder toggle of a document collection and the rail it opens.
//!
//! The grid owns the query slots; the builder panel owns its draft. While
//! the rail is open every slot change is read into the builder, and every
//! builder edit comes back as a [`DocumentBuilderEvent::WriteSlots`]. Find
//! runs the slots through the ordinary collection browse, so results keep
//! their `_id` editability, count and history. The toggle is offered only
//! when the driver reports `DocumentFeatures::VISUAL_BUILDER` and returns a
//! codec; other document drivers show it disabled.
//!
//! In Aggregate mode the slots are not written: the query bar shows a
//! read-only summary of the pipeline instead, and Run pipeline writes the
//! pipeline text into the Aggregate view and runs it there, through the
//! ordinary aggregate path.
//!
//! Queries saved from the rail belong to the collection: its connection
//! profile, database and collection name. The rail lists them, and opening
//! one loads it in the mode it was saved in.

use std::sync::Arc;

use dbflux_components::controls::{Button, InputEvent};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone};
use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    AuditAction, EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_core::{
    Connection, DatabaseCategory, DocumentFeatures, DocumentFindSlots, DocumentQueryMode,
    DocumentQuerySpec, Pagination,
};
use dbflux_storage::{DocumentQueryScope, SavedDocumentQuerySummary};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::CollectionTab;
use crate::data_grid_panel::{DataGridEvent, DataGridPanel, DataSource};
use crate::document_builder::{
    DocumentBuilderEvent, DocumentBuilderPanel, SavedQueryEntry, SlotWrite,
};

/// Audit object type of a saved document query.
const SAVED_QUERY_OBJECT_TYPE: &str = "saved_document_query";

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

    /// The slots were replaced from outside the builder, as by a history
    /// entry. Their query has no skip, so the builder's skip resets instead
    /// of applying to a query it was not set for, and an open builder reads
    /// the new slots. A closed one reads them when it reopens.
    pub(in crate::data_grid_panel) fn document_slots_replaced(&mut self, cx: &mut Context<Self>) {
        if let Some(panel) = self.collection.builder.panel.clone() {
            panel.update(cx, |builder, cx| builder.reset_skip(cx));
        }

        self.sync_slots_into_document_builder(cx);
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
                // Find focuses the table; a Find pressed from the keyboard
                // inside the rail keeps the keyboard there.
                let keep_rail = self.keyboard_in_document_builder(cx);
                self.collection.tab = CollectionTab::Documents;
                self.find_documents(window, cx);
                if keep_rail {
                    self.enter_side_island(window, cx);
                }
            }
            DocumentBuilderEvent::RunPipelineRequested(pipeline) => {
                self.run_builder_pipeline(pipeline, window, cx);
            }
            DocumentBuilderEvent::ModeChanged => cx.notify(),
            DocumentBuilderEvent::OpenInEditorRequested(text) => {
                if let DataSource::Collection { profile_id, .. } = &self.source {
                    cx.emit(DataGridEvent::OpenEditorWithContent {
                        profile_id: *profile_id,
                        sql: text.clone(),
                    });
                }
            }
            DocumentBuilderEvent::CloseRequested => self.close_document_builder(cx),
            DocumentBuilderEvent::SaveRequested { name, spec } => {
                self.save_document_query(name, spec, cx);
            }
            DocumentBuilderEvent::SavedQueriesRequested => self.send_saved_document_queries(cx),
            DocumentBuilderEvent::OpenSavedRequested { id } => {
                self.open_saved_document_query(id, cx);
            }
            DocumentBuilderEvent::DeleteSavedRequested { id } => {
                self.delete_saved_document_query(id, cx);
            }
        }
    }

    /// The collection saved document queries of this tab belong to.
    fn document_query_scope(&self) -> Option<DocumentQueryScope> {
        let DataSource::Collection {
            profile_id,
            collection,
            ..
        } = &self.source
        else {
            return None;
        };

        Some(DocumentQueryScope::new(
            profile_id.to_string(),
            collection.database.clone(),
            collection.name.clone(),
        ))
    }

    fn save_document_query(
        &mut self,
        name: &str,
        spec: &DocumentQuerySpec,
        cx: &mut Context<Self>,
    ) {
        let Some(scope) = self.document_query_scope() else {
            return;
        };

        let result = self.app_state.update(cx, |app, _| {
            let existed = app
                .saved_document_queries
                .list(&scope)?
                .iter()
                .any(|saved| saved.name == name.trim());
            let summary = app.saved_document_queries.save(&scope, name, spec)?;
            Ok::<_, dbflux_storage::error::StorageError>((summary, existed))
        });

        match result {
            Ok((summary, existed)) => {
                if let Some(panel) = self.collection.builder.panel.clone() {
                    let id = summary.id.clone();
                    panel.update(cx, |builder, cx| builder.mark_saved(id, cx));
                }
                self.send_saved_document_queries(cx);

                let action = if existed {
                    audit_actions::CONFIG_UPDATE
                } else {
                    audit_actions::CONFIG_CREATE
                };
                self.record_saved_query_audit(
                    saved_query_audit_event(
                        action,
                        &format!("Saved document query \"{}\"", summary.name),
                        &summary.id,
                        &scope,
                        self.collection_driver_id(cx).as_deref(),
                    ),
                    cx,
                );

                dbflux_ui_base::toast::Toast::success(dbflux_i18n::t!(
                    "document.collection.builder.saved.toast.saved",
                    name = summary.name
                ))
                .meta_right(dbflux_ui_base::toast::now_hms())
                .push(cx);
            }
            Err(error) => report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.collection.builder.saved.error.save_failed"),
                )
                .with_cause(error.to_string()),
                cx,
            ),
        }
    }

    /// Sends the saved queries of this collection to the rail.
    fn send_saved_document_queries(&mut self, cx: &mut Context<Self>) {
        let Some(scope) = self.document_query_scope() else {
            return;
        };
        let Some(panel) = self.collection.builder.panel.clone() else {
            return;
        };

        let result = self
            .app_state
            .update(cx, |app, _| app.saved_document_queries.list(&scope));

        match result {
            Ok(saved) => {
                let entries = saved.into_iter().map(saved_query_entry).collect();
                panel.update(cx, |builder, cx| builder.set_saved_queries(entries, cx));
            }
            Err(error) => report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.collection.builder.saved.error.list_failed"),
                )
                .with_cause(error.to_string()),
                cx,
            ),
        }
    }

    fn open_saved_document_query(&mut self, id: &str, cx: &mut Context<Self>) {
        let Some(panel) = self.collection.builder.panel.clone() else {
            return;
        };
        let Some(scope) = self.document_query_scope() else {
            return;
        };

        let loaded = self
            .app_state
            .read(cx)
            .saved_document_queries
            .load(id, &scope);

        let saved = match loaded {
            Ok(Some(saved)) => saved,
            Ok(None) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        dbflux_i18n::t!("document.collection.builder.saved.error.not_found"),
                    ),
                    cx,
                );
                self.send_saved_document_queries(cx);
                return;
            }
            Err(error) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        dbflux_i18n::t!("document.collection.builder.saved.error.open_failed"),
                    )
                    .with_cause(error.to_string()),
                    cx,
                );
                return;
            }
        };

        let opened = panel.update(cx, |builder, cx| {
            builder.open_saved(saved.summary.id, &saved.summary.name, &saved.spec, cx)
        });

        if !opened {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!(
                        "document.collection.builder.saved.error.aggregate_unavailable"
                    ),
                ),
                cx,
            );
        }
    }

    fn delete_saved_document_query(&mut self, id: &str, cx: &mut Context<Self>) {
        let Some(scope) = self.document_query_scope() else {
            return;
        };

        let result = self
            .app_state
            .update(cx, |app, _| app.saved_document_queries.delete(id, &scope));

        match result {
            Ok(deleted) => {
                self.send_saved_document_queries(cx);
                if deleted {
                    self.record_saved_query_audit(
                        saved_query_audit_event(
                            audit_actions::CONFIG_DELETE,
                            "Deleted a saved document query",
                            id,
                            &scope,
                            self.collection_driver_id(cx).as_deref(),
                        ),
                        cx,
                    );
                }
            }
            Err(error) => report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.collection.builder.saved.error.delete_failed"),
                )
                .with_cause(error.to_string()),
                cx,
            ),
        }
    }

    /// Driver of the collection's connection, for audit events.
    fn collection_driver_id(&self, cx: &App) -> Option<String> {
        self.collection_connection(cx)
            .map(|connection| connection.metadata().id.clone())
    }

    fn record_saved_query_audit(&self, event: EventRecord, cx: &App) {
        if let Err(error) = self.app_state.read(cx).audit_service().record(event) {
            log::error!("saved document query audit event failed to record: {error}");
        }
    }

    /// Writes the builder's pipeline into the Aggregate view and runs it
    /// there, so it goes through the same checks as a typed pipeline. While
    /// that view runs a pipeline or waits for its confirmation, the editor
    /// keeps the pipeline it holds and nothing runs.
    fn run_builder_pipeline(
        &mut self,
        pipeline: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let aggregate = &self.collection.aggregate;
        if aggregate.running || aggregate.pending_run.is_some() {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.collection.builder.run_pipeline_busy"),
                ),
                cx,
            );
            return;
        }

        self.set_collection_tab(CollectionTab::Aggregate, cx);
        if self.collection.tab != CollectionTab::Aggregate {
            return;
        }

        let pipeline = pipeline.to_string();
        self.collection
            .aggregate
            .pipeline_editor
            .update(cx, |editor, cx| editor.set_value(pipeline, window, cx));

        self.collection.aggregate.next_run_from_builder = true;
        self.run_aggregate(window, cx);
    }

    /// The open builder, when it composes an aggregation.
    pub(in crate::data_grid_panel) fn document_builder_in_aggregate(
        &self,
        cx: &App,
    ) -> Option<Entity<DocumentBuilderPanel>> {
        self.collection.builder.panel.clone().filter(|panel| {
            self.collection.builder.open && panel.read(cx).mode() == DocumentQueryMode::Aggregate
        })
    }

    /// Stage names of the builder's pipeline while it composes an
    /// aggregation; `None` while the slots are the query.
    pub(in crate::data_grid_panel) fn document_builder_pipeline_stages(
        &self,
        cx: &App,
    ) -> Option<Vec<String>> {
        self.document_builder_in_aggregate(cx)
            .and_then(|panel| panel.read(cx).pipeline_stages())
    }

    /// The row above the grid while the builder composes an aggregation:
    /// `pipeline [ $match, $group, ... ]`, read-only, with the synced chip.
    pub(in crate::data_grid_panel) fn render_builder_pipeline_row(
        &self,
        stages: Vec<String>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        use dbflux_components::primitives::Chamfer;
        use dbflux_components::tokens::{ChamferCut, ChromeColors, CollectionMetrics};
        use dbflux_components::typography::AppFonts;
        use gpui_component::ActiveTheme;

        let theme = cx.theme().clone();
        let summary = format!("[ {} ]", stages.join(", "));

        div()
            .id("collection-builder-pipeline-row")
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(CollectionMetrics::QUERY_ROW_GAP)
            .min_h(CollectionMetrics::QUERY_ROW_HEIGHT)
            .px(CollectionMetrics::QUERY_ROW_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .id("collection-slot-pipeline")
                    .debug_selector(|| "collection-slot-pipeline".to_string())
                    .relative()
                    .flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .gap(CollectionMetrics::SLOT_GAP)
                    .h(CollectionMetrics::SLOT_HEIGHT)
                    .px(CollectionMetrics::SLOT_PADDING_X)
                    .font_family(AppFonts::MONO)
                    .text_size(CollectionMetrics::SLOT_FONT)
                    .child(
                        Chamfer::new(ChamferCut::CONTROL)
                            .fill(theme.background)
                            .border(theme.input),
                    )
                    .child(
                        div()
                            .flex_shrink_0()
                            .font_weight(FontWeight::BOLD)
                            .text_color(ChromeColors::tint(&theme))
                            .child(dbflux_i18n::t!(
                                "document.collection.builder.pipeline_label"
                            )),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .text_color(theme.muted_foreground)
                            .child(SharedString::from(summary)),
                    ),
            )
            .children(self.render_document_builder_sync_chip(cx))
            .into_any_element()
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

    /// Documents the open builder skips before the first page. Paging
    /// counts from there: the collection's pagination is relative to it, so
    /// Previous stops at the skip and a new page size starts over at it.
    pub(in crate::data_grid_panel) fn document_builder_skip(&self, cx: &App) -> u64 {
        self.collection
            .builder
            .panel
            .as_ref()
            .filter(|_| self.collection.builder.open)
            .and_then(|panel| panel.read(cx).skip())
            .unwrap_or(0)
    }

    /// The first page of a Find, counted from the builder's skip.
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

        Pagination::Offset { limit, offset: 0 }
    }

    /// Footer note while the builder drives a find: its rows stay editable.
    pub(in crate::data_grid_panel) fn document_builder_footer(&self, cx: &App) -> Option<String> {
        let finds = self.document_builder_in_aggregate(cx).is_none();
        (self.collection.builder.open && finds && self.commits_document_patches(cx))
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

        // The pipeline summary is rendered from the builder, so it is always
        // in sync; only the find slots can hold what the builder cannot read.
        let conflicted = self.collection.builder.panel.as_ref().is_some_and(|panel| {
            let panel = panel.read(cx);
            panel.mode() == DocumentQueryMode::Find && panel.is_conflicted()
        });

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

fn saved_query_entry(summary: SavedDocumentQuerySummary) -> SavedQueryEntry {
    SavedQueryEntry {
        id: summary.id,
        name: summary.name,
        mode: summary.mode,
    }
}

/// The `Config` audit event of a change to a saved document query. The
/// driver is left unset when the connection is not known.
fn saved_query_audit_event(
    action: AuditAction,
    summary: &str,
    id: &str,
    scope: &DocumentQueryScope,
    driver_id: Option<&str>,
) -> EventRecord {
    let now_ms = dbflux_core::chrono::Utc::now().timestamp_millis();
    let details = serde_json::json!({ "collection": scope.collection });

    let mut event = EventRecord::new(
        now_ms,
        EventSeverity::Info,
        EventCategory::Config,
        EventOutcome::Success,
    )
    .with_summary(summary)
    .with_typed_action(action)
    .with_origin(EventOrigin::local())
    .with_actor_id("local")
    .with_object_ref(SAVED_QUERY_OBJECT_TYPE, id)
    .with_details_json(details.to_string());

    event.connection_id = Some(scope.profile_id.clone());
    event.database_name = Some(scope.database.clone());
    event.driver_id = driver_id.map(str::to_string);

    event
}

#[cfg(test)]
mod tests {
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

        let (start, skip) = window.update(|_, cx| {
            let grid = panel.read(cx);
            (
                grid.document_find_pagination(cx),
                grid.document_builder_skip(cx),
            )
        });
        assert_eq!(
            start,
            Pagination::Offset {
                limit: 20,
                offset: 0,
            },
            "pages count from the skip"
        );
        assert_eq!(skip, 40);

        window.update(|_, cx| {
            panel.update(cx, |grid, _| grid.mark_document_builder_closed());
        });
        let skip = window.update(|_, cx| panel.read(cx).document_builder_skip(cx));
        assert_eq!(skip, 0, "a closed builder does not skip");
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

    #[gpui::test]
    fn aggregate_mode_needs_the_aggregate_feature(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                assert!(!rail.aggregate_available());
                rail.add_group_stage(cx);
                rail.set_mode(DocumentQueryMode::Aggregate, cx);
                assert_eq!(rail.mode(), DocumentQueryMode::Find);
                assert!(rail.draft().group.is_none());
            });
        });

        let mut cx = TestAppContext::single();
        let (panel, window) = collection_panel(
            &mut cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                assert!(rail.aggregate_available());
                rail.add_group_stage(cx);
                assert_eq!(rail.mode(), DocumentQueryMode::Aggregate);
            });
        });
    }

    #[gpui::test]
    fn aggregate_mode_leaves_the_slots_alone_until_find_returns(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "{ name: 1 }");
        open_builder(&panel, window);
        let rail = builder(&panel, window);
        let (id, _, _) = first_condition(&panel, window);

        assert_eq!(
            window.update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx)),
            None,
            "find mode shows the slots"
        );

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                rail.add_group_stage(cx);
                rail.set_operator(id, DocumentOperator::Gte, cx);
            });
        });
        window.run_until_parked();

        assert_eq!(
            slot_texts(&panel, window),
            (
                r#"{ age: { $gt: 30 } }"#.to_string(),
                "{ name: 1 }".to_string()
            ),
            "no slot is written in aggregate mode"
        );
        assert_eq!(
            window.update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx)),
            Some(vec![
                "$match".to_string(),
                "$group".to_string(),
                "$limit".to_string()
            ]),
            "the limit slot's page size carries over as the pipeline's limit"
        );

        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "{ name: 1 }");
        window.update(|_, cx| {
            panel.update(cx, |grid, cx| grid.sync_slots_into_document_builder(cx));
        });
        assert_eq!(
            first_condition(&panel, window).1,
            "age",
            "a slot edit does not replace the pipeline's filter"
        );

        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "{ name: 1 }");
        window.update(|_, cx| {
            rail.update(cx, |rail, cx| rail.set_mode(DocumentQueryMode::Find, cx));
        });
        window.run_until_parked();

        assert_eq!(
            slot_texts(&panel, window),
            (
                r#"{"age": {"$gte": 30}}"#.to_string(),
                "{ name: 1 }".to_string()
            ),
            "back in find mode the builder writes the part it changed"
        );
        assert_eq!(
            window.update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx)),
            None
        );
    }

    #[gpui::test]
    fn a_slot_typed_while_the_rail_was_away_stays_locked_when_find_returns(
        cx: &mut TestAppContext,
    ) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);
        let (id, _, _) = first_condition(&panel, window);

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                rail.add_group_stage(cx);
                rail.set_operator(id, DocumentOperator::Gte, cx);
            });
        });
        window.update(|_, cx| {
            panel.update(cx, |grid, _| grid.mark_document_builder_closed());
        });

        let unreadable = r#"{"$expr": {"$gt": ["$total", "$limit"]}}"#;
        set_slot_texts(&panel, window, unreadable, "");
        sync_slots(&panel, window);

        open_builder(&panel, window);
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.remove_group_stage(cx)));
        window.run_until_parked();

        assert_eq!(
            slot_texts(&panel, window).0,
            unreadable,
            "the builder never overwrites a slot it cannot read"
        );
        window.update(|_, cx| {
            let rail = rail.read(cx);
            assert_eq!(rail.mode(), DocumentQueryMode::Find);
            assert!(rail.is_conflicted());
            assert!(!rail.can_find(), "the edit is held until the user decides");
        });
    }

    #[gpui::test]
    fn a_slot_edited_during_an_aggregation_reaches_the_builder_when_find_returns(
        cx: &mut TestAppContext,
    ) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        sync_slots(&panel, window);

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| rail.set_mode(DocumentQueryMode::Find, cx));
        });
        window.run_until_parked();

        assert_eq!(first_condition(&panel, window).1, "status");
        assert_eq!(
            slot_texts(&panel, window).0,
            r#"{ "status": "failed" }"#,
            "a part the builder did not edit takes the slot's text"
        );
        assert!(!window.update(|_, cx| rail.read(cx).is_conflicted()));
    }

    #[gpui::test]
    fn a_history_entry_reloads_the_builder_and_drops_its_skip(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.set_skip(Some(40), cx)));

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| {
                grid.collection.history = vec![super::super::QueryHistoryEntry {
                    filter: r#"{ "status": "failed" }"#.to_string(),
                    projection: String::new(),
                    sort: String::new(),
                    limit: String::new(),
                }];
                grid.run_history_entry(0, window, cx);
            });
        });
        window.run_until_parked();

        assert_eq!(first_condition(&panel, window).1, "status");
        window.update(|_, cx| {
            let rail = rail.read(cx);
            assert!(!rail.is_conflicted());
            assert_eq!(rail.skip(), None, "a history entry has no skip");
        });
    }

    #[gpui::test]
    fn opening_a_saved_find_writes_every_slot_it_replaces(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        let saved_filter = r#"{"age": {"$gt": 30}}"#;
        set_slot_texts(&panel, window, saved_filter, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        set_query_name(&rail, window, "adults");
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
        window.run_until_parked();
        let saved_id = stored_queries(&panel, window)[0].id.clone();

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        sync_slots(&panel, window);

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&saved_id, cx)));
        window.run_until_parked();

        assert_eq!(slot_texts(&panel, window).0, saved_filter);
        assert_eq!(
            window.update(|_, cx| rail.read(cx).mode()),
            DocumentQueryMode::Find
        );
    }

    #[gpui::test]
    fn run_pipeline_hands_the_pipeline_to_the_aggregate_view(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        let expected = window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                rail.add_group_stage(cx);
                assert!(rail.can_run());
                let spec = rail.draft().to_spec().expect("a valid pipeline");
                MongoDocumentCodec
                    .render_pipeline(&spec)
                    .expect("renderable")
            })
        });

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_run(cx)));
        window.run_until_parked();

        window.update(|_, cx| {
            let grid = panel.read(cx);
            assert_eq!(grid.collection.tab, CollectionTab::Aggregate);

            let aggregate = &grid.collection.aggregate;
            assert_eq!(
                aggregate.pipeline_editor.read(cx).value().to_string(),
                expected
            );
            assert_eq!(aggregate.result_count(), Some(1));
            assert!(aggregate.results_from_builder);
        });
        assert_eq!(
            slot_texts(&panel, window).0,
            r#"{ "status": "failed" }"#,
            "running the pipeline writes no slot"
        );

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.run_aggregate(window, cx));
        });
        window.run_until_parked();
        assert!(
            !window.update(|_, cx| panel.read(cx).collection.aggregate.results_from_builder),
            "a pipeline run from the editor is not the builder's"
        );
    }

    #[gpui::test]
    fn run_pipeline_leaves_a_busy_aggregate_view_alone(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        open_builder(&panel, window);
        let rail = builder(&panel, window);
        let typed = r#"[{"$match": {"status": "failed"}}]"#;

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| {
                grid.collection
                    .aggregate
                    .pipeline_editor
                    .update(cx, |editor, cx| editor.set_value(typed, window, cx));
                grid.collection.aggregate.running = true;
            });
            rail.update(cx, |rail, cx| {
                rail.add_group_stage(cx);
                rail.request_run(cx);
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let aggregate = &panel.read(cx).collection.aggregate;
            assert_eq!(
                aggregate.pipeline_editor.read(cx).value().to_string(),
                typed,
                "the pipeline being run keeps its text"
            );
            assert!(!aggregate.next_run_from_builder);
            assert!(aggregate.results.is_none());
        });
    }

    #[gpui::test]
    fn paging_a_builder_find_never_goes_below_its_skip(cx: &mut TestAppContext) {
        let offsets = Arc::new(Mutex::new(Vec::new()));
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true)
                .logging_browse_offsets(offsets.clone()),
        );
        open_builder(&panel, window);
        let rail = builder(&panel, window);
        offsets.lock().unwrap().clear();

        window.update(|window, cx| {
            rail.update(cx, |rail, cx| rail.set_skip(Some(40), cx));
            panel.update(cx, |grid, cx| {
                grid.filter_bar
                    .limit_input
                    .update(cx, |input, cx| input.set_value("20", window, cx));
                grid.find_documents(window, cx);
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let grid = panel.read(cx);
            assert!(!grid.can_go_prev(), "the first page starts at the skip");
            assert_eq!(
                grid.source.pagination().map(|page| page.current_page()),
                Some(1)
            );
        });

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.go_to_next_page(window, cx));
        });
        window.run_until_parked();
        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.go_to_prev_page(window, cx));
        });
        window.run_until_parked();
        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.go_to_prev_page(window, cx));
        });
        window.run_until_parked();

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| {
                grid.go_to_next_page(window, cx);
            });
        });
        window.run_until_parked();
        window.update(|window, cx| {
            panel.update(cx, |grid, cx| {
                grid.filter_bar
                    .limit_input
                    .update(cx, |input, cx| input.set_value("10", window, cx));
                grid.refresh(window, cx);
            });
        });
        window.run_until_parked();

        assert_eq!(
            *offsets.lock().unwrap(),
            vec![40, 60, 40, 60, 40],
            "find, next, previous (twice, the second stays), next, then a new \
             page size starts over at the skip"
        );
    }

    #[gpui::test]
    fn the_page_count_and_match_count_leave_out_the_skipped_documents(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        window.update(|window, cx| {
            rail.update(cx, |rail, cx| rail.set_skip(Some(700), cx));
            panel.update(cx, |grid, cx| {
                grid.filter_bar
                    .limit_input
                    .update(cx, |input, cx| input.set_value("50", window, cx));
                grid.find_documents(window, cx);
            });
        });
        window.run_until_parked();
        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.process_pending_actions(window, cx));
        });

        window.update(|_, cx| {
            let grid = panel.read(cx);
            assert_eq!(grid.total_pages(), Some(2), "800 matches, 700 skipped");
            assert!(grid.can_go_next());
            assert_eq!(
                grid.document_count_footer(),
                crate::labels::collection_matching_estimated(0, 100)
            );
        });

        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.go_to_next_page(window, cx));
        });
        window.run_until_parked();

        assert!(
            !window.update(|_, cx| panel.read(cx).can_go_next()),
            "the second page is the last"
        );
    }

    #[gpui::test]
    fn an_invalid_group_stage_cannot_run(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                rail.add_group_stage(cx);
                let id = rail.draft().group.as_ref().unwrap().accumulators[0].id;
                rail.set_accumulator_op(id, AccumulatorOp::Sum, cx);
                assert!(!rail.can_run(), "a sum needs a field");
                rail.request_run(cx);
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let grid = panel.read(cx);
            assert_eq!(grid.collection.tab, CollectionTab::Documents);
            assert!(grid.collection.aggregate.results.is_none());
        });
    }

    /// Window content for the rendered tests: the builder rail, as the
    /// workspace hosts it, or the grid itself.
    struct RailHost {
        grid: gpui::Entity<DataGridPanel>,
        show_builder: bool,
    }

    impl gpui::Render for RailHost {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{IntoElement, ParentElement, Styled};

            let builder = self
                .grid
                .read(cx)
                .collection
                .builder
                .panel
                .clone()
                .filter(|_| self.show_builder);

            let content = match builder {
                Some(builder) => builder.into_any_element(),
                None => self.grid.clone().into_any_element(),
            };

            gpui::div().size_full().child(content)
        }
    }

    fn center(bounds: gpui::Bounds<gpui::Pixels>) -> gpui::Point<gpui::Pixels> {
        gpui::point(
            bounds.origin.x + bounds.size.width / 2.0,
            bounds.origin.y + bounds.size.height / 2.0,
        )
    }

    /// Opens the builder of a grid rendered, like the workspace rail, as
    /// the content of a window `width` wide. The filter slot holds `filter`.
    fn rendered_rail<'a>(
        cx: &'a mut TestAppContext,
        filter: &str,
    ) -> (
        gpui::Entity<DataGridPanel>,
        gpui::Entity<RailHost>,
        &'a mut VisualTestContext,
    ) {
        rendered_rail_with(cx, filter, builder_features())
    }

    fn rendered_rail_with<'a>(
        cx: &'a mut TestAppContext,
        filter: &str,
        features: DocumentFeatures,
    ) -> (
        gpui::Entity<DataGridPanel>,
        gpui::Entity<RailHost>,
        &'a mut VisualTestContext,
    ) {
        let (app_state, profile_id) = register_stub_connection(
            cx,
            Arc::new(StubDocumentConnection::new(
                DatabaseCategory::Document,
                features,
                true,
            )),
        );

        let grid_slot: Rc<RefCell<Option<gpui::Entity<DataGridPanel>>>> =
            Rc::new(RefCell::new(None));
        let grid_handle = grid_slot.clone();
        let (host, window) = cx.add_window_view(|window, cx| {
            let grid = cx.new(|cx| {
                let source = DataSource::Collection {
                    profile_id,
                    collection: CollectionRef::new("shop", "orders"),
                    pagination: Pagination::default(),
                    total_docs: None,
                };
                DataGridPanel::new_internal(source, app_state, vec![], window, cx)
            });
            grid_handle.replace(Some(grid.clone()));
            RailHost {
                grid,
                show_builder: false,
            }
        });
        let grid = grid_slot.borrow().clone().expect("grid");

        set_slot_texts(&grid, window, filter, "");
        open_builder(&grid, window);
        host.update(window, |host, cx| {
            host.show_builder = true;
            cx.notify();
        });
        window.run_until_parked();

        (grid, host, window)
    }

    fn bounds_of(
        window: &mut VisualTestContext,
        selector: String,
    ) -> Option<gpui::Bounds<gpui::Pixels>> {
        window.debug_bounds(selector.leak())
    }

    #[gpui::test]
    fn clicking_an_operator_option_changes_the_condition_and_its_slot(cx: &mut TestAppContext) {
        use gpui::Modifiers;

        let (grid, _host, window) = rendered_rail(cx, r#"{"age": {"$gt": 30}}"#);
        let (id, _, operator) = first_condition(&grid, window);
        assert_eq!(operator, DocumentOperator::Gt);

        let trigger = bounds_of(window, format!("doc-builder-operator-{id}-trigger"))
            .expect("the operator select has its own trigger id");
        window.simulate_click(center(trigger), Modifiers::none());
        window.run_until_parked();

        // With no sample the field offers every operator; the list holds
        // them all, the last one included.
        assert!(bounds_of(window, format!("doc-builder-operator-{id}-option-all")).is_some());

        let option = bounds_of(window, format!("doc-builder-operator-{id}-option-eq"))
            .expect("every operator is an addressable option");
        window.simulate_click(center(option), Modifiers::none());
        window.run_until_parked();

        let (_, _, operator) = first_condition(&grid, window);
        assert_eq!(operator, DocumentOperator::Eq);
        assert!(
            bounds_of(window, format!("doc-builder-operator-{id}-option-eq")).is_none(),
            "choosing an option closes the list"
        );
        assert_eq!(slot_texts(&grid, window).0, r#"{"age": 30}"#);
    }

    #[gpui::test]
    fn the_operator_list_answers_the_keyboard(cx: &mut TestAppContext) {
        use gpui::Modifiers;

        let (grid, _host, window) = rendered_rail(cx, r#"{"age": {"$gt": 30}}"#);
        let (id, _, _) = first_condition(&grid, window);

        let trigger = bounds_of(window, format!("doc-builder-operator-{id}-trigger"))
            .expect("operator trigger");
        window.simulate_click(center(trigger), Modifiers::none());
        window.run_until_parked();

        window.simulate_keystrokes("down enter");
        window.run_until_parked();

        let (_, _, operator) = first_condition(&grid, window);
        assert_eq!(operator, DocumentOperator::Gte, "down from $gt is $gte");
        assert_eq!(slot_texts(&grid, window).0, r#"{"age": {"$gte": 30}}"#);

        window.simulate_click(center(trigger), Modifiers::none());
        window.run_until_parked();
        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(bounds_of(window, format!("doc-builder-operator-{id}-option-eq")).is_none());
        assert_eq!(first_condition(&grid, window).2, DocumentOperator::Gte);
    }

    #[gpui::test]
    fn aggregate_mode_shows_the_pipeline_summary_and_the_read_only_banner(cx: &mut TestAppContext) {
        let (grid, host, window) =
            rendered_rail_with(cx, r#"{"status": "failed"}"#, aggregate_features());
        let rail = builder(&grid, window);

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        window.run_until_parked();

        assert!(bounds_of(window, "doc-builder-group-stage".to_string()).is_some());
        assert!(
            bounds_of(window, "doc-builder-match-summary".to_string()).is_some(),
            "the filter collapses to its $match summary"
        );

        host.update(window, |host, cx| {
            host.show_builder = false;
            cx.notify();
        });
        window.run_until_parked();

        assert!(bounds_of(window, "collection-slot-pipeline".to_string()).is_some());
        assert!(
            bounds_of(window, "collection-slot-filter".to_string()).is_none(),
            "the find slots give way to the pipeline summary"
        );

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_run(cx)));
        window.run_until_parked();

        assert!(bounds_of(window, "aggregate-builder-read-only".to_string()).is_some());
        assert!(
            bounds_of(window, "collection-slot-pipeline".to_string()).is_some(),
            "the Aggregate view shows the builder's pipeline in place of its editor"
        );

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.remove_group_stage(cx)));
        window.run_until_parked();

        assert!(bounds_of(window, "collection-slot-pipeline".to_string()).is_none());
        assert!(
            bounds_of(window, "aggregate-builder-read-only".to_string()).is_some(),
            "the banner stays with the results it explains"
        );
    }

    /// Asserts the preview sits between the scrolling cards and the footer,
    /// inside the window.
    fn assert_preview_is_pinned(window: &mut VisualTestContext) -> gpui::Bounds<gpui::Pixels> {
        let sections = bounds_of(window, "doc-builder-body".to_string()).expect("scrolling body");
        let preview = bounds_of(window, "doc-builder-preview".to_string()).expect("preview");
        let footer = bounds_of(window, "doc-builder-footer".to_string()).expect("footer");

        assert!(
            preview.origin.y >= sections.bottom(),
            "the preview ({preview:?}) is outside the scrolling cards ({sections:?})"
        );
        assert!(
            preview.bottom() <= footer.origin.y,
            "the preview ({preview:?}) sits above the footer ({footer:?})"
        );

        preview
    }

    #[gpui::test]
    fn the_preview_stays_pinned_above_the_footer_in_both_modes(cx: &mut TestAppContext) {
        let (grid, _host, window) =
            rendered_rail_with(cx, r#"{"status": "failed"}"#, aggregate_features());
        let rail = builder(&grid, window);

        let before = assert_preview_is_pinned(window);

        window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                let root = rail.draft().filter.id;
                for _ in 0..12 {
                    rail.add_condition(root, cx);
                }
            });
        });
        window.run_until_parked();

        // The empty conditions add the "not run yet" line under the text, so
        // the pane grows upwards; it stays anchored to the footer.
        let after = assert_preview_is_pinned(window);
        assert_eq!(
            before.bottom(),
            after.bottom(),
            "more cards scroll behind the preview instead of moving it"
        );

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        window.run_until_parked();
        assert_eq!(
            window.update(|_, cx| rail.read(cx).mode()),
            DocumentQueryMode::Aggregate
        );
        assert_preview_is_pinned(window);
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

    #[gpui::test]
    fn a_saved_find_reopens_from_the_list_and_rewrites_the_slots(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        assert!(
            !window.update(|_, cx| rail.read(cx).can_save(cx)),
            "a query needs a name before it can be saved"
        );
        set_query_name(&rail, window, "  adults ");
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
        window.run_until_parked();

        let saved = stored_queries(&panel, window);
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].name, "adults");
        assert_eq!(saved[0].mode, DocumentQueryMode::Find);
        assert_eq!(
            window.update(|_, cx| rail.read(cx).loaded_id().map(str::to_string)),
            Some(saved[0].id.clone())
        );

        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        sync_slots(&panel, window);
        set_query_name(&rail, window, "");
        assert_eq!(first_condition(&panel, window).1, "status");

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.toggle_saved_menu(cx)));
        window.run_until_parked();
        let listed = window.update(|_, cx| rail.read(cx).saved_queries().to_vec());
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "adults");

        window
            .update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&listed[0].id, cx)));
        window.run_until_parked();

        let (_, path, operator) = first_condition(&panel, window);
        assert_eq!((path.as_str(), operator), ("age", DocumentOperator::Gt));
        assert_eq!(slot_texts(&panel, window).0, r#"{"age": {"$gt": 30}}"#);
        window.update(|_, cx| {
            let rail = rail.read(cx);
            assert_eq!(rail.mode(), DocumentQueryMode::Find);
            assert_eq!(rail.query_name(cx), "adults");
            assert!(!rail.is_conflicted());
        });
    }

    #[gpui::test]
    fn a_saved_aggregation_reopens_in_aggregate_mode_and_leaves_the_slots(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, aggregate_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        set_query_name(&rail, window, "failures by status");
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
        window.run_until_parked();

        let saved = stored_queries(&panel, window);
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].mode, DocumentQueryMode::Aggregate);

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.remove_group_stage(cx)));
        window.run_until_parked();
        set_slot_texts(&panel, window, r#"{"other": 1}"#, "");
        sync_slots(&panel, window);

        window
            .update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&saved[0].id, cx)));
        window.run_until_parked();

        assert_eq!(
            window.update(|_, cx| rail.read(cx).mode()),
            DocumentQueryMode::Aggregate
        );
        assert_eq!(first_condition(&panel, window).1, "status");
        assert_eq!(
            slot_texts(&panel, window).0,
            r#"{"other": 1}"#,
            "opening an aggregation writes no slot"
        );
        let stages = window
            .update(|_, cx| panel.read(cx).document_builder_pipeline_stages(cx))
            .expect("the query bar shows the pipeline");
        assert!(stages.contains(&"$group".to_string()), "{stages:?}");
    }

    #[gpui::test]
    fn an_aggregation_does_not_open_where_aggregations_cannot_run(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ "status": "failed" }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        let id = window.update(|_, cx| {
            let grid = panel.read(cx);
            let scope = grid.document_query_scope().expect("scope");
            let spec = dbflux_core::DocumentQuerySpec {
                mode: DocumentQueryMode::Aggregate,
                group: Some(dbflux_core::DocumentGroupStage::default()),
                ..dbflux_core::DocumentQuerySpec::default()
            };
            grid.app_state.clone().update(cx, |app, _| {
                app.saved_document_queries
                    .save(&scope, "grouped", &spec)
                    .expect("save")
                    .id
            })
        });

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_open_saved(&id, cx)));
        window.run_until_parked();

        window.update(|_, cx| {
            let rail = rail.read(cx);
            assert_eq!(rail.mode(), DocumentQueryMode::Find);
            assert_eq!(rail.loaded_id(), None);
        });
        assert_eq!(first_condition(&panel, window).1, "status");
    }

    #[gpui::test]
    fn deleting_a_saved_query_removes_it_from_the_list(cx: &mut TestAppContext) {
        let (panel, window) = collection_panel(
            cx,
            StubDocumentConnection::new(DatabaseCategory::Document, builder_features(), true),
        );
        set_slot_texts(&panel, window, r#"{ age: { $gt: 30 } }"#, "");
        open_builder(&panel, window);
        let rail = builder(&panel, window);

        set_query_name(&rail, window, "adults");
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.toggle_saved_menu(cx)));
        window.run_until_parked();

        let id = window.update(|_, cx| rail.read(cx).saved_queries()[0].id.clone());
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_delete_saved(&id, cx)));
        window.run_until_parked();

        assert!(stored_queries(&panel, window).is_empty());
        window.update(|_, cx| {
            let rail = rail.read(cx);
            assert!(rail.saved_queries().is_empty());
            assert_eq!(
                rail.loaded_id(),
                None,
                "the deleted query is no longer the loaded one"
            );
        });
    }

    #[gpui::test]
    fn clicking_a_saved_query_in_the_list_opens_it(cx: &mut TestAppContext) {
        use gpui::Modifiers;

        let (grid, _host, window) = rendered_rail(cx, r#"{"age": {"$gt": 30}}"#);
        let rail = builder(&grid, window);

        set_query_name(&rail, window, "adults");
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.request_save(cx)));
        set_slot_texts(&grid, window, r#"{"status": "failed"}"#, "");
        sync_slots(&grid, window);
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.toggle_saved_menu(cx)));
        window.run_until_parked();

        assert!(bounds_of(window, "doc-builder-saved-menu".to_string()).is_some());
        let id = stored_queries(&grid, window)[0].id.clone();
        let row = bounds_of(window, format!("doc-builder-saved-{id}")).expect("a row per query");
        window.simulate_click(center(row), Modifiers::none());
        window.run_until_parked();

        assert!(
            bounds_of(window, "doc-builder-saved-menu".to_string()).is_none(),
            "opening a query closes the list"
        );
        assert_eq!(first_condition(&grid, window).1, "age");
        assert_eq!(slot_texts(&grid, window).0, r#"{"age": {"$gt": 30}}"#);
    }

    /// A schema sample with a string `tier` and a numeric `price`.
    fn give_rail_a_sample(
        rail: &gpui::Entity<crate::document_builder::DocumentBuilderPanel>,
        window: &mut VisualTestContext,
    ) {
        let field = |path: &str, type_name: &str| dbflux_core::FieldSchemaStats {
            path: path.to_string(),
            presence: 10,
            types: vec![dbflux_core::FieldTypeShare {
                type_name: type_name.to_string(),
                count: 10,
            }],
            summary: dbflux_core::FieldValueSummary::Empty,
        };
        let sample = dbflux_core::CollectionSchemaSample {
            sampled_documents: 10,
            total_documents: Some(10),
            fields: (0..20)
                .map(|index| field(&format!("attribute{index:02}"), "String"))
                .chain([field("tier", "String"), field("price", "Double")])
                .collect(),
        };

        window.update(|_, cx| rail.update(cx, |rail, cx| rail.set_schema(&sample, cx)));
        window.run_until_parked();
    }

    fn click(window: &mut VisualTestContext, selector: String) {
        let bounds =
            bounds_of(window, selector.clone()).unwrap_or_else(|| panic!("{selector} is rendered"));
        window.simulate_click(center(bounds), gpui::Modifiers::none());
        window.run_until_parked();
    }

    #[gpui::test]
    fn picking_a_field_adds_the_group_key(cx: &mut TestAppContext) {
        let (grid, _host, window) = rendered_rail_with(cx, "", aggregate_features());
        window.simulate_resize(gpui::size(gpui::px(540.0), gpui::px(760.0)));
        let rail = builder(&grid, window);
        give_rail_a_sample(&rail, window);
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        window.run_until_parked();

        click(window, "doc-builder-group-key-add".to_string());

        // Twenty fields sort before `tier`, so its row starts below the
        // list's visible height until the search narrows the list.
        let list = bounds_of(window, "doc-builder-picker-list".to_string()).expect("list");
        let row = bounds_of(window, "doc-builder-picker-field-tier".to_string()).expect("row");
        assert!(
            row.origin.y >= list.bottom(),
            "{row:?} is scrolled out of {list:?}"
        );

        window.simulate_input("tier");
        window.run_until_parked();
        click(window, "doc-builder-picker-field-tier".to_string());

        window.update(|_, cx| {
            let rail = rail.read(cx);
            assert_eq!(
                rail.draft().group.as_ref().map(|stage| stage.keys.clone()),
                Some(vec!["tier".to_string()])
            );
        });
        let pipeline = window.update(|_, cx| rail.read(cx).pipeline_text());
        assert!(
            pipeline
                .as_deref()
                .is_some_and(|text| text.contains(r#""_id": "$tier""#)),
            "{pipeline:?}"
        );
    }

    #[gpui::test]
    fn picking_a_field_sets_the_accumulator_field(cx: &mut TestAppContext) {
        let (grid, _host, window) = rendered_rail_with(cx, "", aggregate_features());
        let rail = builder(&grid, window);
        give_rail_a_sample(&rail, window);

        let id = window.update(|_, cx| {
            rail.update(cx, |rail, cx| {
                rail.add_group_stage(cx);
                let id = rail.draft().group.as_ref().unwrap().accumulators[0].id;
                rail.set_accumulator_op(id, AccumulatorOp::Sum, cx);
                id
            })
        });
        window.run_until_parked();

        click(window, format!("doc-builder-acc-field-{id}"));
        click(window, "doc-builder-picker-field-price".to_string());

        let path = window.update(|_, cx| {
            rail.read(cx)
                .draft()
                .accumulator(id)
                .map(|accumulator| accumulator.path.clone())
        });
        assert_eq!(path.as_deref(), Some("price"));
    }

    #[gpui::test]
    fn the_field_picker_opens_inside_the_rail(cx: &mut TestAppContext) {
        let (grid, _host, window) = rendered_rail_with(cx, "", aggregate_features());
        let rail_width = crate::document_builder::RAIL_WIDTH;
        window.simulate_resize(gpui::size(rail_width, gpui::px(760.0)));
        let rail = builder(&grid, window);
        give_rail_a_sample(&rail, window);
        window.update(|_, cx| rail.update(cx, |rail, cx| rail.add_group_stage(cx)));
        window.run_until_parked();

        // Group keys push "+ field" to the right, until a picker opened at
        // its left edge would no longer fit in the rail.
        let mut picker = None;
        for index in 0..10 {
            click(window, "doc-builder-group-key-add".to_string());
            let add = bounds_of(window, "doc-builder-group-key-add".to_string()).expect("add");
            let open = bounds_of(window, "doc-builder-picker".to_string()).expect("picker");
            if add.origin.x + open.size.width > rail_width {
                picker = Some(open);
                break;
            }

            window.update(|_, cx| {
                rail.update(cx, |rail, cx| {
                    rail.pick(&format!("attribute{index:02}"), cx)
                })
            });
            window.run_until_parked();
        }

        let picker = picker.expect("the group keys moved the add button far enough right");
        assert!(
            picker.origin.x >= gpui::px(0.0) && picker.origin.x + picker.size.width <= rail_width,
            "the picker ({picker:?}) stays inside the {rail_width:?} rail"
        );
    }

    #[test]
    fn saved_query_audit_events_pass_config_validation() {
        let scope = dbflux_storage::DocumentQueryScope::new("profile-1", "shop", "orders");

        for action in [
            dbflux_core::observability::actions::CONFIG_CREATE,
            dbflux_core::observability::actions::CONFIG_UPDATE,
            dbflux_core::observability::actions::CONFIG_DELETE,
        ] {
            let event = super::saved_query_audit_event(
                action,
                "Saved \"adults\"",
                "id-1",
                &scope,
                Some("mongodb"),
            );

            dbflux_audit::AuditService::validate_event(&event).expect("a valid config event");
            assert_eq!(event.object_type.as_deref(), Some("saved_document_query"));
            assert_eq!(event.object_id.as_deref(), Some("id-1"));
            assert_eq!(event.driver_id.as_deref(), Some("mongodb"));
        }

        let event = super::saved_query_audit_event(
            dbflux_core::observability::actions::CONFIG_DELETE,
            "Deleted a saved document query",
            "id-1",
            &scope,
            None,
        );
        assert_eq!(event.driver_id, None, "an unknown driver is left unset");
        assert_eq!(event.connection_id.as_deref(), Some("profile-1"));
        assert_eq!(event.database_name.as_deref(), Some("shop"));
    }

    #[gpui::test]
    fn the_filter_slot_keeps_a_usable_width_beside_the_rail(cx: &mut TestAppContext) {
        use gpui::{px, size};

        let (_grid, host, window) = rendered_rail(cx, "");
        host.update(window, |host, cx| {
            host.show_builder = false;
            cx.notify();
        });
        window.simulate_resize(size(px(520.0), px(700.0)));
        window.run_until_parked();

        let filter =
            bounds_of(window, "collection-slot-filter".to_string()).expect("filter slot rendered");
        assert!(
            filter.size.width >= px(240.0),
            "the filter slot is {:?} wide in a 520 px grid",
            filter.size.width
        );

        window.simulate_resize(size(px(1400.0), px(700.0)));
        window.run_until_parked();

        let filter = bounds_of(window, "collection-slot-filter".to_string()).expect("filter");
        let limit = bounds_of(window, "collection-slot-limit".to_string()).expect("limit");
        assert_eq!(
            filter.origin.y, limit.origin.y,
            "a wide grid keeps the slots on one line"
        );
    }

    /// A collection grid with its builder open beside it, hosted as the
    /// workspace hosts it, under the app keymap.
    fn keyboard_rail(
        cx: &mut TestAppContext,
    ) -> (
        gpui::Entity<DataGridPanel>,
        gpui::Entity<crate::document_builder::DocumentBuilderPanel>,
        &mut VisualTestContext,
    ) {
        use crate::data_grid_panel::tests::rail_keys::host_in_rail;

        let (app_state, profile_id) = register_stub_connection(
            cx,
            Arc::new(StubDocumentConnection::new(
                DatabaseCategory::Document,
                aggregate_features(),
                true,
            )),
        );
        cx.update(dbflux_ui_base::keymap::init_keymap);

        let (panel, window) = host_in_rail(cx, move |window, cx| {
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
        open_builder(&panel, window);
        let builder = builder(&panel, window);

        (panel, builder, window)
    }

    /// A collection with the Documents, Schema and Aggregate views, hosted
    /// as the workspace hosts it, with the keyboard on the grid.
    fn keyboard_collection(
        cx: &mut TestAppContext,
    ) -> (gpui::Entity<DataGridPanel>, &mut VisualTestContext) {
        use crate::data_grid_panel::tests::rail_keys::host_in_rail;

        let (app_state, profile_id) = register_stub_connection(
            cx,
            Arc::new(StubDocumentConnection::new(
                DatabaseCategory::Document,
                aggregate_features(),
                true,
            )),
        );
        cx.update(dbflux_ui_base::keymap::init_keymap);

        host_in_rail(cx, move |window, cx| {
            cx.new(|cx| {
                let source = DataSource::Collection {
                    profile_id,
                    collection: CollectionRef::new("shop", "orders"),
                    pagination: Pagination::default(),
                    total_docs: None,
                };
                DataGridPanel::new_internal(source, app_state, vec![], window, cx)
            })
        })
    }

    fn run_toolbar(
        panel: &gpui::Entity<DataGridPanel>,
        window: &mut VisualTestContext,
        action: crate::data_grid_panel::context_menu::toolbar::ToolbarAction,
    ) {
        window.update(|window, cx| {
            panel.update(cx, |grid, cx| grid.run_toolbar_action(action, window, cx))
        });
        window.run_until_parked();
    }

    /// The query bar's Find, history and clear filter and the view tabs are
    /// reachable from the keyboard: the Toolbar entries run Find and open
    /// the history menu, whose menu keys rerun a query; Shift+F empties the
    /// filter slot and finds; Alt+L / Alt+H step through the views.
    #[gpui::test]
    fn the_query_bar_and_views_are_driven_by_keys(cx: &mut TestAppContext) {
        use crate::data_grid_panel::context_menu::toolbar::ToolbarAction;
        use crate::data_grid_panel::tests::rail_keys::{context, keys};
        use dbflux_app::keymap::ContextId;

        let (panel, window) = keyboard_collection(cx);

        set_slot_texts(&panel, window, "{ status: 1 }", "");
        run_toolbar(&panel, window, ToolbarAction::Find);
        assert_eq!(
            window.update(|_, cx| panel.read(cx).collection.history.len()),
            1,
            "the Find entry runs the query and records it"
        );

        let actions = window.update(|_, cx| panel.read(cx).toolbar_actions(cx));
        for expected in [
            ToolbarAction::ClearFilter,
            ToolbarAction::Find,
            ToolbarAction::QueryHistory,
            ToolbarAction::CollectionView(CollectionTab::Schema),
            ToolbarAction::CollectionView(CollectionTab::Aggregate),
        ] {
            assert!(
                actions.contains(&expected),
                "the Toolbar lists {expected:?}: {actions:?}"
            );
        }

        keys(window, "shift-f");
        assert_eq!(
            slot_texts(&panel, window).0,
            "",
            "Shift+F empties the filter slot"
        );

        run_toolbar(&panel, window, ToolbarAction::QueryHistory);
        assert_eq!(context(&panel, window), ContextId::ContextMenu);

        // The cleared query ran last, so it leads the history.
        keys(window, "j enter");
        assert_eq!(
            slot_texts(&panel, window).0,
            "{ status: 1 }",
            "J then Enter reruns the older query"
        );
        assert!(!window.update(|_, cx| panel.read(cx).collection.history_open));

        keys(window, "alt-l");
        assert_eq!(
            window.update(|_, cx| panel.read(cx).collection.tab),
            CollectionTab::Schema,
            "Alt+L shows the next view"
        );

        keys(window, "alt-h");
        assert_eq!(
            window.update(|_, cx| panel.read(cx).collection.tab),
            CollectionTab::Documents,
            "Alt+H shows the previous view"
        );
    }

    /// Ctrl+L enters the document builder; A adds a condition, whose field
    /// is picked by typing its path, whose operator comes from the operator
    /// list driven by J and Enter, and whose value is typed; Ctrl+Enter
    /// finds; X removes the condition; Alt+L switches to Aggregate; M opens
    /// the rail's menu.
    #[gpui::test]
    fn the_document_builder_is_driven_by_keys(cx: &mut TestAppContext) {
        use crate::data_grid_panel::tests::rail_keys::{context, keys};
        use dbflux_app::keymap::ContextId;

        let (panel, builder, window) = keyboard_rail(cx);

        keys(window, "ctrl-l");
        assert_eq!(context(&panel, window), ContextId::DocumentBuilder);

        keys(window, "j a");
        let (id, path, operator) = first_condition(&panel, window);
        assert!(path.is_empty(), "A adds an empty condition");
        assert_eq!(
            window.update(|_, cx| builder.read(cx).rail_cursor_for_test()),
            Some(format!("condition-{id}")),
            "the cursor moves onto it"
        );

        keys(window, "enter");
        window.simulate_input("age");
        window.run_until_parked();
        keys(window, "enter");
        assert_eq!(
            first_condition(&panel, window).1,
            "age",
            "Enter picks the typed path"
        );
        assert_eq!(
            context(&panel, window),
            ContextId::DocumentBuilder,
            "the keyboard is back on the rail"
        );

        keys(window, "l enter j enter");
        assert_ne!(
            first_condition(&panel, window).2,
            operator,
            "J and Enter pick another operator"
        );
        assert_eq!(context(&panel, window), ContextId::DocumentBuilder);

        keys(window, "l enter");
        window.simulate_input("30");
        window.run_until_parked();
        keys(window, "escape");
        assert_eq!(context(&panel, window), ContextId::DocumentBuilder);
        assert!(
            slot_texts(&panel, window).0.contains("age"),
            "the condition reached the filter slot: {:?}",
            slot_texts(&panel, window)
        );

        keys(window, "ctrl-enter");
        assert_eq!(
            window.update(|_, cx| panel.read(cx).collection.tab),
            CollectionTab::Documents,
            "Ctrl+Enter finds, as the Find button does"
        );
        assert_eq!(
            context(&panel, window),
            ContextId::DocumentBuilder,
            "a Find from the rail keeps the keyboard there"
        );

        keys(window, "x");
        assert!(
            window.update(|_, cx| builder.read(cx).draft().conditions().is_empty()),
            "X removes the condition"
        );

        keys(window, "alt-l");
        assert_eq!(
            window.update(|_, cx| builder.read(cx).mode()),
            DocumentQueryMode::Aggregate,
            "Alt+L switches to Aggregate"
        );

        keys(window, "m");
        assert_eq!(context(&panel, window), ContextId::ContextMenu);
        keys(window, "escape escape");
        assert_eq!(context(&panel, window), ContextId::Results);
    }

    /// Shift+J and Shift+K move a sort key, the way dragging it does.
    #[gpui::test]
    fn shift_j_moves_a_sort_key_down(cx: &mut TestAppContext) {
        use crate::data_grid_panel::tests::rail_keys::keys;

        let (panel, builder, window) = keyboard_rail(cx);
        window.update(|window, cx| {
            panel.update(cx, |grid, cx| {
                grid.collection.sort_input.update(cx, |input, cx| {
                    input.set_value("{ a: 1, b: -1 }", window, cx)
                });
                grid.sync_slots_into_document_builder(cx);
            });
        });
        window.run_until_parked();

        let sort_paths = |window: &mut VisualTestContext| {
            window.update(|_, cx| {
                builder
                    .read(cx)
                    .draft()
                    .sort
                    .iter()
                    .map(|key| key.path.clone())
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(sort_paths(window), vec!["a", "b"]);

        keys(window, "ctrl-l");
        let first_sort = window.update(|_, cx| {
            builder
                .read(cx)
                .rail_rows_for_test(cx)
                .iter()
                .position(|id| id == "sort-0")
                .expect("the sort keys are rows")
        });
        for _ in 0..first_sort {
            keys(window, "j");
        }

        keys(window, "shift-j");
        assert_eq!(sort_paths(window), vec!["b", "a"]);
        assert_eq!(
            window.update(|_, cx| builder.read(cx).rail_cursor_for_test()),
            Some("sort-1".to_string()),
            "the cursor stays on the moved key"
        );
    }
}
