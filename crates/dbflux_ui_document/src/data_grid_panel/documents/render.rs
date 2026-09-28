//! Chrome of a document collection: the four-slot query bar, the view row
//! (Tree / Table / JSON, step breadcrumb, pending edits, Commit), the
//! Documents / Schema / Aggregate switch, the Schema view, the JSON view and the
//! server-change card (P1DocTable, P2DocNested, P1DocSchema).

use dbflux_components::composites::{
    Breadcrumb, BreadcrumbSegment, MenuItem, menu_frame, menu_row,
};
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Chamfer, Icon, Kbd, SegmentedControl, SegmentedItem};
use dbflux_components::tokens::{ChamferCut, ChromeColors, CollectionMetrics, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_components::vim::VimBinding;
use dbflux_core::{FieldSchemaStats, FieldValueSummary, NULL_TYPE_NAME, Value};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::{Editor, EditorState};
use gpui_component::tooltip::Tooltip;

use super::{CollectionTab, SchemaLoad};
use crate::data_grid_panel::DataGridPanel;
use crate::data_view::DataViewMode;

/// Mongo-style shortcut label shown on the Commit button.
#[cfg(target_os = "macos")]
const COMMIT_SHORTCUT: &str = "Cmd S";
#[cfg(not(target_os = "macos"))]
const COMMIT_SHORTCUT: &str = "Ctrl S";

/// Narrowest the filter slot gets; below it the query bar wraps instead, so
/// the slot stays readable beside the builder rail.
const FILTER_SLOT_MIN_WIDTH: Pixels = px(260.0);

/// Space above and below a line of slots, so a single line keeps the
/// board's row height and wrapped lines keep the same padding.
const QUERY_ROW_PADDING_Y: Pixels = px(9.0);

/// Color of a type in the schema bars and legends.
fn type_color(type_name: &str, theme: &gpui_component::Theme, cx: &App) -> Hsla {
    match type_name {
        "String" | "Symbol" => theme.success,
        "ObjectId" => theme.muted_foreground,
        NULL_TYPE_NAME => theme.input,
        "Array" | "Object" => theme.info,
        "Double" => theme.warning,
        "Int32" | "Int64" => ChromeColors::tint(theme),
        "Decimal128" | "Date" | "Timestamp" => {
            dbflux_components::tokens::SyntaxColors::for_current(cx).number
        }
        "Boolean" => theme.danger,
        _ => theme.muted_foreground,
    }
}

fn percent(count: u64, total: u64) -> u32 {
    if total == 0 {
        return 0;
    }
    ((count as f64 / total as f64) * 100.0).round() as u32
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => other.as_display_string_truncated(40),
    }
}

impl DataGridPanel {
    /// Keyword-prefixed slot of the query bar on a cut-6 ground field. The
    /// keyword carries a tooltip explaining what the slot holds.
    fn query_slot(
        &self,
        id: &'static str,
        keyword: &'static str,
        tooltip: String,
        content: AnyElement,
        width: Option<Pixels>,
        cx: &App,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let tooltip = SharedString::from(tooltip);

        div()
            .id(id)
            .debug_selector(|| id.to_string())
            .relative()
            .flex()
            .items_center()
            .gap(CollectionMetrics::SLOT_GAP)
            .h(CollectionMetrics::SLOT_HEIGHT)
            .px(CollectionMetrics::SLOT_PADDING_X)
            .font_family(AppFonts::MONO)
            .text_size(CollectionMetrics::SLOT_FONT)
            .map(|slot| match width {
                Some(width) => slot.w(width).flex_shrink_0(),
                None => slot.flex_1().min_w(FILTER_SLOT_MIN_WIDTH),
            })
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(theme.input),
            )
            .child(
                div()
                    .id(SharedString::from(format!("{id}-label")))
                    .flex_shrink_0()
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::tint(theme))
                    .tooltip(move |window, cx| Tooltip::new(tooltip.clone()).build(window, cx))
                    .child(keyword),
            )
            .child(div().flex_1().min_w_0().overflow_hidden().child(content))
    }

    /// Query bar of a document collection: filter, project, sort and limit
    /// slots, Find and the query history (P1DocTable). While the builder
    /// composes an aggregation the slots are not its query, so the bar shows
    /// the pipeline summary instead.
    pub(in crate::data_grid_panel) fn render_document_query_bar(
        &self,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if let Some(stages) = self.document_builder_pipeline_stages(cx) {
            return self.render_builder_pipeline_row(stages, cx);
        }

        let theme = cx.theme().clone();
        let slots = self.has_document_query_slots(cx);

        let editor = |state: &Entity<EditorState>| {
            crate::completion_support::frameless_single_line_completion_editor(state)
                .text_color(ChromeColors::strong(&theme))
                .into_any_element()
        };

        let filter = self.query_slot(
            "collection-slot-filter",
            "filter",
            dbflux_i18n::t!("document.collection.slot.tooltip.filter"),
            editor(&self.filter_bar.filter_input),
            None,
            cx,
        );

        let limit = self.query_slot(
            "collection-slot-limit",
            "limit",
            dbflux_i18n::t!("document.collection.slot.tooltip.limit"),
            Input::new(&self.filter_bar.limit_input)
                .small()
                .appearance(false)
                .into_any_element(),
            Some(CollectionMetrics::LIMIT_SLOT_WIDTH),
            cx,
        );

        let history_open = self.collection.history_open;
        let history_menu = if history_open {
            Some(self.render_query_history_menu(cx))
        } else {
            None
        };

        div()
            .key_context(dbflux_components::key_contexts::DOCUMENT_QUERY_BAR)
            .flex()
            .flex_wrap()
            .flex_shrink_0()
            .items_center()
            .gap(CollectionMetrics::QUERY_ROW_GAP)
            .min_h(CollectionMetrics::QUERY_ROW_HEIGHT)
            .py(QUERY_ROW_PADDING_Y)
            .px(CollectionMetrics::QUERY_ROW_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(filter)
            .when(slots, |row| {
                row.child(self.query_slot(
                    "collection-slot-project",
                    "project",
                    dbflux_i18n::t!("document.collection.slot.tooltip.project"),
                    editor(&self.collection.projection_input),
                    Some(CollectionMetrics::PROJECT_SLOT_WIDTH),
                    cx,
                ))
                .child(self.query_slot(
                    "collection-slot-sort",
                    "sort",
                    dbflux_i18n::t!("document.collection.slot.tooltip.sort"),
                    editor(&self.collection.sort_input),
                    Some(CollectionMetrics::SORT_SLOT_WIDTH),
                    cx,
                ))
            })
            .child(limit)
            .children(self.render_document_builder_sync_chip(cx))
            .child(
                Button::new(
                    "collection-find",
                    dbflux_i18n::t!("document.collection.find"),
                )
                .primary()
                .icon(AppIcon::Play)
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(
                        dbflux_app::keymap::ContextId::Input,
                        dbflux_app::keymap::Command::RunQuery,
                    ),
                    Button::kbd,
                )
                .tab_stop(false)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.find_documents(window, cx);
                })),
            )
            .child(
                div()
                    .relative()
                    .child(
                        Button::new(
                            "collection-history",
                            dbflux_i18n::t!("document.collection.history"),
                        )
                        .secondary()
                        .icon(AppIcon::History)
                        .icon_only()
                        .selected(history_open)
                        .disabled(self.collection.history.is_empty())
                        .tab_stop(false)
                        .on_click(cx.listener(|this, _, window, cx| {
                            if this.collection.history_open {
                                this.close_query_history(window, cx);
                            } else {
                                this.open_query_history(window, cx);
                            }
                        })),
                    )
                    .when_some(history_menu, |anchor, menu| anchor.child(menu)),
            )
            .into_any_element()
    }

    fn render_query_history_menu(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows: Vec<AnyElement> = self
            .collection
            .history
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let item = MenuItem::new(entry.summary()).icon(AppIcon::History);
                menu_row(
                    SharedString::from(format!("collection-history-{index}")),
                    &item,
                    index == self.collection.history_selected,
                    cx,
                )
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if this.collection.history_selected != index {
                        this.collection.history_selected = index;
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.run_history_entry(index, window, cx);
                }))
                .into_any_element()
            })
            .collect();

        deferred(
            menu_frame(cx)
                .absolute()
                .top_full()
                .right_0()
                .mt(Spacing::XS)
                .w(px(480.0)) // guardrail-allow: history menu width, fits a long query
                .occlude()
                .track_focus(&self.focus.history_menu_focus)
                // The grid reports the ContextMenu context while the menu is
                // open, so the menu keys arrive here first.
                .on_action(cx.listener(
                    |this, action: &dbflux_ui_base::keymap::RunCommand, window, cx| {
                        let handled =
                            dbflux_ui_base::keymap::run_command(action).is_some_and(|command| {
                                this.dispatch_history_menu_command(command, window, cx)
                            });

                        if !handled {
                            cx.propagate();
                        }
                    },
                ))
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.collection.history_open = false;
                    cx.notify();
                }))
                .children(rows),
        )
        .with_priority(2)
        .into_any_element()
    }

    /// Row under the query bar: the view switch, the step breadcrumb or the
    /// keyboard hint, and the pending edits with Revert and Commit.
    pub(in crate::data_grid_panel) fn render_document_view_row(
        &self,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let muted = theme.muted_foreground;
        let mode = self.view_config.mode;
        let panel = cx.entity().downgrade();

        let modes = self.available_view_modes(cx);
        let items: Vec<SegmentedItem> = modes
            .iter()
            .map(|mode| {
                let (id, icon) = match mode {
                    DataViewMode::Document => ("tree", AppIcon::Layers),
                    DataViewMode::Table => ("table", AppIcon::Table),
                    DataViewMode::Json => ("json", AppIcon::Braces),
                };
                SegmentedItem::new(id, mode.label()).icon(icon)
            })
            .collect();
        let active = match mode {
            DataViewMode::Document => "tree",
            DataViewMode::Table => "table",
            DataViewMode::Json => "json",
        };

        let switch = SegmentedControl::new(items, active, move |selected, _, cx| {
            let mode = match selected.as_ref() {
                "tree" => DataViewMode::Document,
                "json" => DataViewMode::Json,
                _ => DataViewMode::Table,
            };
            if let Err(error) = panel.update(cx, |this, cx| this.set_document_view_mode(mode, cx)) {
                log::debug!("data grid released before its view switch: {error}");
            }
        });

        let hint_key = |key: &'static str| Kbd::new(key);

        let hint = |parts: Vec<AnyElement>| {
            div()
                .flex()
                .items_center()
                .gap(CollectionMetrics::HINT_GAP)
                .ml(CollectionMetrics::HINT_MARGIN_LEFT)
                .text_size(CollectionMetrics::HINT_FONT)
                .text_color(muted)
                .children(parts)
        };

        let guide = if self.is_stepped_into() {
            let collection_name = self
                .source
                .collection_ref()
                .map(|collection| collection.name.clone())
                .unwrap_or_default();

            let mut segments = vec![BreadcrumbSegment::new(collection_name).on_click(
                "collection-step-root",
                cx.listener(|this, _, _, cx| {
                    this.step_to_depth(0, cx);
                }),
            )];

            let labels = self.document_step_labels();
            let last = labels.len().saturating_sub(1);
            for (depth, label) in labels.into_iter().enumerate() {
                let mut segment = BreadcrumbSegment::new(label);
                if depth < last {
                    segment = segment.on_click(
                        SharedString::from(format!("collection-step-{depth}")),
                        cx.listener(move |this, _, _, cx| {
                            this.step_to_depth(depth + 1, cx);
                        }),
                    );
                }
                segments.push(segment);
            }

            hint(vec![
                Breadcrumb::new(segments).mono().into_any_element(),
                hint_key("Backspace").into_any_element(),
                div()
                    .child(dbflux_i18n::t!("document.collection.hint.steps_out"))
                    .into_any_element(),
            ])
            .into_any_element()
        } else if mode == DataViewMode::Table {
            let expands = self
                .collection
                .flat
                .columns
                .iter()
                .any(|column| column.group.is_some());

            hint(if expands {
                vec![
                    hint_key("e").into_any_element(),
                    div()
                        .child(dbflux_i18n::t!("document.collection.hint.expands"))
                        .into_any_element(),
                    hint_key("Enter").into_any_element(),
                    div()
                        .child(dbflux_i18n::t!("document.collection.hint.steps_into_array"))
                        .into_any_element(),
                ]
            } else {
                vec![
                    hint_key("Enter").into_any_element(),
                    div()
                        .child(dbflux_i18n::t!("document.collection.hint.steps_in"))
                        .into_any_element(),
                    hint_key("Backspace").into_any_element(),
                    div()
                        .child(dbflux_i18n::t!("document.collection.hint.steps_out"))
                        .into_any_element(),
                ]
            })
            .into_any_element()
        } else {
            div().into_any_element()
        };

        let editable = self.commits_document_patches(cx);
        let pending = if editable {
            self.pending_document_edit_count(cx)
        } else {
            0
        };
        let json_error = (mode == DataViewMode::Json)
            .then(|| self.collection.json_draft.error.clone())
            .flatten();

        let pending_label = (pending > 0).then(|| {
            let (prefix, detail) = match self.single_pending_document_path(cx) {
                Some(path) => (
                    dbflux_i18n::t!("document.collection.pending.one_field"),
                    path,
                ),
                None => (
                    dbflux_i18n::t!("document.collection.pending.label"),
                    crate::labels::collection_pending_count(pending, mode == DataViewMode::Json),
                ),
            };

            div()
                .flex()
                .gap(Spacing::XS)
                .text_size(CollectionMetrics::PENDING_FONT)
                .text_color(muted)
                .child(prefix)
                .child(div().text_color(theme.warning).child(detail))
        });

        let committing = self.collection.committing;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(CollectionMetrics::VIEW_ROW_GAP)
            .h(CollectionMetrics::VIEW_ROW_HEIGHT)
            .px(CollectionMetrics::VIEW_ROW_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(switch)
            .child(guide)
            .child(div().flex_1())
            .when_some(json_error, |row, error| {
                row.child(
                    div()
                        .max_w(px(360.0)) // guardrail-allow: keeps a long parse error on one line
                        .truncate()
                        .text_size(CollectionMetrics::PENDING_FONT)
                        .text_color(theme.danger)
                        .child(error),
                )
            })
            .when_some(pending_label, |row, label| row.child(label))
            .when(editable, |row| {
                row.child(
                    Button::new(
                        "collection-revert",
                        dbflux_i18n::t!("document.data.grid.edit_bar.revert"),
                    )
                    .secondary()
                    .icon(AppIcon::RotateCcw)
                    .disabled(pending == 0 || committing)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.revert_document_edits(window, cx);
                    })),
                )
                .child(
                    Button::new(
                        "collection-commit",
                        dbflux_i18n::t!("document.collection.commit.button"),
                    )
                    .primary()
                    .icon(AppIcon::Check)
                    .kbd(COMMIT_SHORTCUT)
                    .disabled(pending == 0 || committing)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.commit_document_edits(cx);
                    })),
                )
            })
    }

    /// Documents / Schema / Aggregate switch at the right of the header;
    /// only the views the driver offers are shown.
    pub(in crate::data_grid_panel) fn render_collection_tabs(
        &self,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let panel = cx.entity().downgrade();
        let id = |tab: CollectionTab| match tab {
            CollectionTab::Documents => "documents",
            CollectionTab::Schema => "schema",
            CollectionTab::Aggregate => "aggregate",
        };

        let items = self
            .collection_tabs(cx)
            .into_iter()
            .map(|tab| match tab {
                CollectionTab::Documents => SegmentedItem::new(
                    id(tab),
                    dbflux_i18n::t!("document.collection.tab.documents"),
                )
                .icon(AppIcon::File),
                CollectionTab::Schema => {
                    SegmentedItem::new(id(tab), dbflux_i18n::t!("document.collection.tab.schema"))
                        .icon(AppIcon::Columns)
                }
                CollectionTab::Aggregate => SegmentedItem::new(
                    id(tab),
                    dbflux_i18n::t!("document.collection.tab.aggregate"),
                )
                .icon(AppIcon::ChartColumnBig),
            })
            .collect();

        SegmentedControl::new(items, id(self.collection.tab), move |selected, _, cx| {
            let tab = match selected.as_ref() {
                "schema" => CollectionTab::Schema,
                "aggregate" => CollectionTab::Aggregate,
                _ => CollectionTab::Documents,
            };
            if let Err(error) = panel.update(cx, |this, cx| this.set_collection_tab(tab, cx)) {
                log::debug!("data grid released before its tab switch: {error}");
            }
        })
    }

    /// Metadata chip after the collection breadcrumb: the document count.
    /// The Schema view names the count in its own toolbar, so the chip is
    /// hidden there (IslDocSchema).
    pub(in crate::data_grid_panel) fn collection_meta_label(&self) -> Option<String> {
        if self.collection.tab == CollectionTab::Schema {
            return None;
        }

        let schema_total = self
            .collection
            .schema
            .as_ref()
            .and_then(|schema| schema.total_documents);

        let total = self
            .collection
            .count
            .filter(|count| self.collection.counted_filter == Some(None) || !count.exact)
            .map(|count| count.count)
            .or(schema_total)?;

        Some(crate::labels::collection_document_count(total))
    }

    /// Footer text: page size against the (estimated) match count, without
    /// the documents the builder skipped.
    pub(in crate::data_grid_panel) fn document_count_footer(&self) -> String {
        let shown = self.collection.documents.len();
        let skipped = self.collection.applied_skip;

        match self.collection.count {
            Some(count) if count.exact => {
                crate::labels::collection_matching(shown, count.count.saturating_sub(skipped))
            }
            Some(count) => crate::labels::collection_matching_estimated(
                shown,
                count.count.saturating_sub(skipped),
            ),
            None => crate::labels::collection_documents(shown),
        }
    }

    /// Footer note naming the sample the header presence bars come from.
    pub(in crate::data_grid_panel) fn presence_footer(&self) -> Option<String> {
        let schema = self.collection.schema.as_ref()?;
        (self.view_config.mode == DataViewMode::Table && !self.is_stepped_into())
            .then(|| crate::labels::collection_presence_note(schema.sampled_documents))
    }

    /// JSON view: the page as an editable JSON array.
    pub(in crate::data_grid_panel) fn render_document_json_view(
        &self,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let editable = self.commits_document_patches(cx);
        let vim = &self.collection.json_vim;
        let input = vim.input_id();
        let container = VimBinding::capture_run_command(
            VimBinding::wire(vim.leader_scope(div(), cx), input, cx),
            input,
            cx,
        );
        let theme = cx.theme();

        container
            .id("collection-json-view")
            .size_full()
            .flex()
            .flex_col()
            .bg(theme.background)
            .font_family(AppFonts::MONO)
            .child(
                div().flex_1().min_h_0().child(
                    vim.editor(!editable)
                        .bordered(false)
                        .size_full()
                        .disabled(!editable),
                ),
            )
            .children(vim.render_indicator(cx))
    }

    /// Schema view (P1DocSchema): sample controls and one row per field.
    pub(in crate::data_grid_panel) fn render_schema_view(
        &self,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let muted = theme.muted_foreground;
        let loading = self.collection.schema_load == SchemaLoad::Loading;

        let summary = match &self.collection.schema {
            Some(schema) => {
                crate::labels::collection_sampled(schema.sampled_documents, schema.total_documents)
            }
            None => dbflux_i18n::t!("document.collection.schema.not_sampled"),
        };

        let toolbar = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(CollectionMetrics::QUERY_ROW_GAP)
            .h(CollectionMetrics::SCHEMA_TOOLBAR_HEIGHT)
            .px(CollectionMetrics::QUERY_ROW_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Activity)
                    .size(CollectionMetrics::NESTED_ICON)
                    .color(muted),
            )
            .child(
                div()
                    .text_size(CollectionMetrics::VALUE_FONT)
                    .text_color(theme.foreground)
                    .child(summary),
            )
            .child(
                div()
                    .w(CollectionMetrics::SCHEMA_SAMPLE_WIDTH)
                    .child(self.collection.sample_dropdown.clone()),
            )
            .child(
                Button::new(
                    "collection-resample",
                    dbflux_i18n::t!("document.collection.schema.resample"),
                )
                .secondary()
                .icon(AppIcon::RefreshCcw)
                .disabled(loading)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.load_collection_schema(cx);
                })),
            )
            .child(div().flex_1())
            .child(
                div()
                    .text_size(CollectionMetrics::PENDING_FONT)
                    .text_color(muted)
                    .child(dbflux_i18n::t!("document.collection.schema.click_hint")),
            );

        let header_cell = |label: String, width: Option<Pixels>| {
            div()
                .when_some(width, |cell, width| cell.w(width).flex_shrink_0())
                .when(width.is_none(), |cell| cell.flex_1())
                .child(label)
        };

        let header = div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(CollectionMetrics::SCHEMA_HEADER_HEIGHT)
            .px(CollectionMetrics::SCHEMA_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(CollectionMetrics::SCHEMA_HEADER_FONT)
            .text_color(muted)
            .child(header_cell(
                dbflux_i18n::t!("document.collection.schema.column.field"),
                Some(CollectionMetrics::SCHEMA_FIELD_WIDTH),
            ))
            .child(header_cell(
                dbflux_i18n::t!("document.collection.schema.column.types"),
                Some(CollectionMetrics::SCHEMA_TYPES_WIDTH),
            ))
            .child(header_cell(
                dbflux_i18n::t!("document.collection.schema.column.presence"),
                Some(CollectionMetrics::SCHEMA_PRESENCE_WIDTH),
            ))
            .child(header_cell(
                dbflux_i18n::t!("document.collection.schema.column.values"),
                None,
            ));

        let rows: Vec<AnyElement> = self
            .collection
            .schema
            .as_ref()
            .map(|schema| {
                schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        self.render_schema_row(index, field, schema.sampled_documents, &theme, cx)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let empty = rows.is_empty().then(|| {
            div()
                .p(CollectionMetrics::SCHEMA_PADDING_X)
                .text_size(CollectionMetrics::VALUE_FONT)
                .text_color(muted)
                .child(match &self.collection.schema_load {
                    SchemaLoad::Loading => dbflux_i18n::t!("document.collection.schema.loading"),
                    SchemaLoad::Failed(error) => crate::labels::collection_schema_failed(error),
                    SchemaLoad::Idle => dbflux_i18n::t!("document.collection.schema.empty"),
                })
        });

        div()
            .key_context(dbflux_components::key_contexts::DOCUMENT_SCHEMA)
            .flex()
            .flex_col()
            .size_full()
            .text_color(strong)
            .child(toolbar)
            .child(header)
            .child(
                div()
                    .id("collection-schema-rows")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .children(rows)
                    .when_some(empty, |list, empty| list.child(empty)),
            )
    }

    fn render_schema_row(
        &self,
        index: usize,
        field: &FieldSchemaStats,
        sampled: u64,
        theme: &gpui_component::Theme,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let muted = theme.muted_foreground;
        let total_values = field.value_count().max(1);
        let presence = percent(field.presence, sampled);
        let presence_color = if presence >= 100 {
            theme.success
        } else {
            theme.warning
        };

        let bar_width = f32::from(CollectionMetrics::SCHEMA_TYPES_WIDTH)
            - f32::from(CollectionMetrics::TYPE_BAR_CLEARANCE);
        let presence_ratio = field.presence_ratio(sampled).clamp(0.0, 1.0);

        let segments: Vec<AnyElement> = field
            .types
            .iter()
            .map(|share| {
                let width = bar_width * presence_ratio * (share.count as f32 / total_values as f32);
                div()
                    .h(CollectionMetrics::TYPE_BAR_HEIGHT)
                    .w(px(width))
                    .bg(type_color(&share.type_name, theme, cx))
                    .into_any_element()
            })
            .collect();

        let legend: Vec<AnyElement> = field
            .types
            .iter()
            .map(|share| {
                div()
                    .flex()
                    .items_center()
                    .gap(CollectionMetrics::LEGEND_GAP)
                    .mr(CollectionMetrics::LEGEND_SPACING)
                    .child(div().size(CollectionMetrics::LEGEND_SWATCH).bg(type_color(
                        &share.type_name,
                        theme,
                        cx,
                    )))
                    .child(format!(
                        "{} {}%",
                        share.type_name,
                        percent(share.count, sampled.max(1))
                    ))
                    .into_any_element()
            })
            .collect();

        let path = field.path.clone();

        let values: AnyElement = if field.has_mixed_types() {
            let secondary = field
                .types
                .iter()
                .filter(|share| share.type_name != NULL_TYPE_NAME)
                .nth(1);

            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .child(
                    Icon::new(AppIcon::TriangleAlert)
                        .size(CollectionMetrics::NESTED_ICON)
                        .color(theme.warning),
                )
                .child(match secondary {
                    Some(share) => crate::labels::collection_mixed_types(
                        percent(share.count, total_values),
                        &share.type_name,
                    ),
                    None => dbflux_i18n::t!("document.collection.schema.mixed"),
                })
                .into_any_element()
        } else {
            match &field.summary {
                FieldValueSummary::Unique => div()
                    .child(dbflux_i18n::t!("document.collection.schema.unique"))
                    .into_any_element(),
                FieldValueSummary::Distinct { count, capped } => div()
                    .child(crate::labels::collection_distinct(*count, *capped))
                    .into_any_element(),
                FieldValueSummary::Range { min, max } => div()
                    .child(format!("{} \u{2192} {}", value_text(min), value_text(max)))
                    .into_any_element(),
                FieldValueSummary::ArrayLength { min, max, median } => div()
                    .child(crate::labels::collection_array_lengths(*min, *max, *median))
                    .into_any_element(),
                FieldValueSummary::Nested { fields } => div()
                    .child(crate::labels::collection_nested_fields(*fields))
                    .into_any_element(),
                FieldValueSummary::Empty => div()
                    .text_color(muted)
                    .child(dbflux_i18n::t!("document.collection.schema.only_null"))
                    .into_any_element(),
                FieldValueSummary::TopValues(shares) => {
                    let total: u64 = shares.iter().map(|share| share.count).sum::<u64>().max(1);
                    let mut parts: Vec<AnyElement> = Vec::new();

                    for (share_index, share) in shares.iter().enumerate() {
                        if share_index > 0 {
                            parts
                                .push(div().text_color(muted).child("\u{00b7}").into_any_element());
                        }

                        let value = share.value.clone();
                        let path = path.clone();
                        parts.push(
                            div()
                                .id(SharedString::from(format!(
                                    "schema-value-{index}-{share_index}"
                                )))
                                .cursor_pointer()
                                .hover(|value| value.text_color(ChromeColors::tint(theme)))
                                .child(format!(
                                    "{} {}%",
                                    value_text(&share.value),
                                    percent(share.count, total)
                                ))
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.add_value_to_filter(&path, &value, window, cx);
                                }))
                                .into_any_element(),
                        );
                    }

                    div()
                        .flex()
                        .flex_wrap()
                        .items_center()
                        .gap(Spacing::XS)
                        .children(parts)
                        .into_any_element()
                }
            }
        };

        div()
            .id(SharedString::from(format!("schema-row-{index}")))
            .flex()
            .items_center()
            .min_h(CollectionMetrics::SCHEMA_ROW_MIN_HEIGHT)
            .px(CollectionMetrics::SCHEMA_PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .child(
                div()
                    .w(CollectionMetrics::SCHEMA_FIELD_WIDTH)
                    .flex_shrink_0()
                    .truncate()
                    .font_family(AppFonts::MONO)
                    .text_size(CollectionMetrics::FIELD_FONT)
                    .text_color(ChromeColors::strong(theme))
                    .child(field.path.clone()),
            )
            .child(
                div()
                    .w(CollectionMetrics::SCHEMA_TYPES_WIDTH)
                    .flex_shrink_0()
                    .flex()
                    .flex_col()
                    .gap(CollectionMetrics::TYPE_BAR_GAP)
                    .pr(CollectionMetrics::TYPE_BAR_CLEARANCE)
                    .child(div().flex().w_full().bg(theme.secondary).children(segments))
                    .child(
                        div()
                            .flex()
                            .flex_wrap()
                            .text_size(CollectionMetrics::LEGEND_FONT)
                            .text_color(muted)
                            .children(legend),
                    ),
            )
            .child(
                div()
                    .w(CollectionMetrics::SCHEMA_PRESENCE_WIDTH)
                    .flex_shrink_0()
                    .font_family(AppFonts::MONO)
                    .text_size(CollectionMetrics::VALUE_FONT)
                    .text_color(presence_color)
                    .child(format!("{presence}%")),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .text_size(CollectionMetrics::VALUE_FONT)
                    .text_color(theme.foreground)
                    .child(values),
            )
            .into_any_element()
    }

    /// Card shown when a document changed on the server after the page
    /// loaded (P2DocNested): what changed, the exact write, and the choice.
    pub(in crate::data_grid_panel) fn render_conflict_card(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement + use<>> {
        let conflict = self.collection.conflict.as_ref()?;
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);

        let changed = crate::labels::dotted_paths(&conflict.change.changed_paths);
        let edited = crate::labels::dotted_paths(&conflict.edit_paths);
        let overlapping = crate::labels::dotted_paths(&conflict.change.overlapping_paths);

        let body = if conflict.replaces_whole_document {
            crate::labels::collection_conflict_replace(&changed)
        } else if conflict.change.can_apply_on_top() {
            crate::labels::collection_conflict_on_top(&changed, &edited)
        } else {
            crate::labels::collection_conflict_overlap(&changed, &overlapping)
        };

        Some(
            div()
                .id("collection-conflict")
                .absolute()
                .right(CollectionMetrics::CONFLICT_RIGHT)
                .bottom(CollectionMetrics::CONFLICT_BOTTOM)
                .w(CollectionMetrics::CONFLICT_WIDTH)
                .occlude()
                .child(
                    div()
                        .relative()
                        .flex()
                        .flex_col()
                        .gap(CollectionMetrics::CONFLICT_GAP)
                        .p(CollectionMetrics::CONFLICT_PADDING)
                        .child(
                            Chamfer::new(ChamferCut::OVERLAY)
                                .fill(theme.secondary)
                                .border(theme.input),
                        )
                        .child(
                            div()
                                .absolute()
                                .left_0()
                                .top_0()
                                .bottom_0()
                                .w(CollectionMetrics::CONFLICT_EDGE)
                                .bg(theme.warning),
                        )
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap(Spacing::SM)
                                .child(
                                    Icon::new(AppIcon::TriangleAlert)
                                        .size(CollectionMetrics::NESTED_ICON)
                                        .color(theme.warning),
                                )
                                .child(
                                    div()
                                        .font_weight(FontWeight::BOLD)
                                        .text_color(strong)
                                        .child(crate::labels::collection_conflict_title(
                                            &conflict.label,
                                        )),
                                ),
                        )
                        .child(
                            div()
                                .text_size(CollectionMetrics::CONFLICT_BODY_FONT)
                                .text_color(theme.muted_foreground)
                                .child(body),
                        )
                        .when_some(conflict.preview.clone(), |card, preview| {
                            card.child(
                                div()
                                    .relative()
                                    .px(CollectionMetrics::CONFLICT_CODE_PADDING_X)
                                    .py(CollectionMetrics::CONFLICT_CODE_PADDING_Y)
                                    .font_family(AppFonts::MONO)
                                    .text_size(CollectionMetrics::CONFLICT_CODE_FONT)
                                    .text_color(theme.foreground)
                                    .child(Chamfer::new(ChamferCut::CONTROL).fill(theme.background))
                                    .child(div().relative().child(preview)),
                            )
                        })
                        .child(
                            div()
                                .flex()
                                .justify_end()
                                .gap(Spacing::SM)
                                .child(
                                    Button::new(
                                        "collection-conflict-reload",
                                        dbflux_i18n::t!("document.collection.conflict.reload"),
                                    )
                                    .secondary()
                                    .icon(AppIcon::RefreshCcw)
                                    .on_click(cx.listener(
                                        |this, _, window, cx| {
                                            this.reload_conflicting_document(window, cx);
                                        },
                                    )),
                                )
                                .child(
                                    Button::new(
                                        "collection-conflict-apply",
                                        dbflux_i18n::t!("document.collection.conflict.apply"),
                                    )
                                    .primary()
                                    .icon(AppIcon::Check)
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.apply_conflicting_commit(cx);
                                        },
                                    )),
                                ),
                        ),
                ),
        )
    }
}
