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
        let Some((documents, features)) = self.collection_capabilities(cx) else {
            return BuilderSupport::Hidden;
        };

        if !documents {
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
                    .font_family(dbflux_components::fonts::editor_family(cx))
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
mod tests;
