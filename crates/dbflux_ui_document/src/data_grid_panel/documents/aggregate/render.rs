//! Chrome of the Aggregate view: the pipeline editor with Run and the
//! history, the results view row, the read-only results and their footer,
//! and the confirmation shown before a pipeline that writes. While the query
//! builder composes an aggregation, its pipeline summary takes the place of
//! the editor, and its results carry a banner explaining why they are
//! read-only (IslDocBuilderAggregate).

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{EmptyState, MenuItem, menu_frame, menu_row};
use dbflux_components::controls::{Button, Checkbox};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::modal::{Modal, ModalVariant};
use dbflux_components::modals::{modal_code, modal_lead};
use dbflux_components::primitives::{
    BannerBlock, BannerVariant, Chamfer, Icon, SegmentedControl, SegmentedItem,
};
use dbflux_components::tokens::{ChamferCut, CollectionMetrics, ModalMetrics, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_components::vim::VimBinding;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::Editor;

use crate::chrome::{document_footer, footer_item};
use crate::data_grid_panel::DataGridPanel;
use crate::data_view::DataViewMode;

impl DataGridPanel {
    /// The Aggregate view: pipeline section, view row, results and footer.
    pub(in crate::data_grid_panel) fn render_aggregate_view(
        &self,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        div()
            .key_context(dbflux_components::key_contexts::DOCUMENT_AGGREGATE)
            .id("collection-aggregate-view")
            .flex()
            .flex_col()
            .size_full()
            .child(match self.document_builder_pipeline_stages(cx) {
                Some(stages) => self.render_builder_pipeline_row(stages, cx),
                None => self.render_pipeline_section(cx).into_any_element(),
            })
            .children(self.render_builder_read_only_banner(cx))
            .child(self.render_aggregate_view_row(cx))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(self.render_aggregate_results(cx)),
            )
            .child(self.render_aggregate_footer(cx))
            .into_any_element()
    }

    /// Why the query builder's grouped results cannot be edited.
    fn render_builder_read_only_banner(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let aggregate = &self.collection.aggregate;
        if !aggregate.results_from_builder || aggregate.results.is_none() {
            return None;
        }

        let lock = Icon::new(AppIcon::Lock)
            .size(CollectionMetrics::NESTED_ICON)
            .color(cx.theme().warning);

        Some(
            div()
                .id("aggregate-builder-read-only")
                .debug_selector(|| "aggregate-builder-read-only".to_string())
                .flex_shrink_0()
                .px(CollectionMetrics::QUERY_ROW_PADDING_X)
                .py(Spacing::SM)
                .child(
                    BannerBlock::new(
                        BannerVariant::Warning,
                        dbflux_i18n::t!("document.collection.aggregate.builder_read_only.title"),
                    )
                    .with_icon(lock)
                    .with_body(dbflux_i18n::t!(
                        "document.collection.aggregate.builder_read_only.body"
                    )),
                )
                .into_any_element(),
        )
    }

    fn render_pipeline_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let vim = &self.collection.aggregate.pipeline_vim;
        let input = vim.input_id();
        let editor_container = VimBinding::capture_run_command(
            VimBinding::wire(vim.leader_scope(div(), cx), input, cx),
            input,
            cx,
        );
        let theme = cx.theme().clone();
        let aggregate = &self.collection.aggregate;
        let error = aggregate.pipeline_error.clone();
        let history_open = aggregate.history_open;

        let editor_box = div()
            .id("aggregate-pipeline")
            .relative()
            .h(CollectionMetrics::PIPELINE_EDITOR_HEIGHT)
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(if error.is_some() {
                        theme.danger
                    } else {
                        theme.input
                    }),
            )
            .child(
                editor_container
                    .relative()
                    .size_full()
                    .py(CollectionMetrics::PIPELINE_EDITOR_PADDING_Y)
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .text_size(CollectionMetrics::SLOT_FONT)
                    .child(
                        aggregate
                            .pipeline_vim
                            .editor(false)
                            .bordered(false)
                            .size_full(),
                    ),
            );
        let indicator = aggregate.pipeline_vim.render_indicator(cx);

        let error_line = error.map(|message| {
            div()
                .id("aggregate-pipeline-error")
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .text_size(CollectionMetrics::PENDING_FONT)
                .text_color(theme.danger)
                .child(
                    Icon::new(AppIcon::TriangleAlert)
                        .size(CollectionMetrics::NESTED_ICON)
                        .color(theme.danger),
                )
                .child(div().min_w_0().truncate().child(message))
        });

        let history_menu = history_open.then(|| self.render_pipeline_history_menu(cx));

        div()
            .flex()
            .flex_shrink_0()
            .gap(CollectionMetrics::QUERY_ROW_GAP)
            .px(CollectionMetrics::QUERY_ROW_PADDING_X)
            .py(CollectionMetrics::PIPELINE_PADDING_Y)
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_w_0()
                    .gap(CollectionMetrics::PIPELINE_ERROR_GAP)
                    .child(editor_box)
                    .children(indicator)
                    .when_some(error_line, |column, line| column.child(line)),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .flex_shrink_0()
                    .gap(CollectionMetrics::QUERY_ROW_GAP)
                    .child(
                        Button::new(
                            "aggregate-run",
                            dbflux_i18n::t!("document.collection.aggregate.run"),
                        )
                        .primary()
                        .icon(AppIcon::Play)
                        .when_some(
                            dbflux_ui_base::keymap::shortcut_label(
                                ContextId::Input,
                                Command::RunQuery,
                            ),
                            Button::kbd,
                        )
                        .disabled(aggregate.running)
                        .tab_stop(false)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.run_aggregate(window, cx);
                        })),
                    )
                    .child(
                        div()
                            .relative()
                            .child(
                                Button::new(
                                    "aggregate-history",
                                    dbflux_i18n::t!("document.collection.aggregate.history"),
                                )
                                .secondary()
                                .icon(AppIcon::History)
                                .selected(history_open)
                                .disabled(aggregate.history.is_empty())
                                .tab_stop(false)
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        let aggregate = &mut this.collection.aggregate;
                                        aggregate.history_open = !aggregate.history_open;
                                        cx.notify();
                                    },
                                )),
                            )
                            .when_some(history_menu, |anchor, menu| anchor.child(menu)),
                    ),
            )
    }

    fn render_pipeline_history_menu(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows: Vec<AnyElement> = self
            .collection
            .aggregate
            .history
            .iter()
            .enumerate()
            .map(|(index, pipeline)| {
                let item = MenuItem::new(crate::data_grid_panel::utils::single_line(pipeline))
                    .icon(AppIcon::History);
                menu_row(
                    SharedString::from(format!("aggregate-history-{index}")),
                    &item,
                    false,
                    cx,
                )
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.load_aggregate_history_entry(index, window, cx);
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
                .w(px(480.0)) // guardrail-allow: history menu width, fits a long pipeline
                .occlude()
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.collection.aggregate.history_open = false;
                    cx.notify();
                }))
                .children(rows),
        )
        .with_priority(2)
        .into_any_element()
    }

    /// Tree / Table / JSON switch of the results, and the read-only note.
    fn render_aggregate_view_row(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let panel = cx.entity().downgrade();

        let active = match self.collection.aggregate.view_mode {
            DataViewMode::Document => "tree",
            DataViewMode::Table => "table",
            DataViewMode::Json => "json",
        };

        let switch = SegmentedControl::new(
            vec![
                SegmentedItem::new("tree", DataViewMode::Document.label()).icon(AppIcon::Layers),
                SegmentedItem::new("table", DataViewMode::Table.label()).icon(AppIcon::Table),
                SegmentedItem::new("json", DataViewMode::Json.label()).icon(AppIcon::Braces),
            ],
            active,
            move |selected, _, cx| {
                let mode = match selected.as_ref() {
                    "tree" => DataViewMode::Document,
                    "json" => DataViewMode::Json,
                    _ => DataViewMode::Table,
                };
                if let Err(error) =
                    panel.update(cx, |this, cx| this.set_aggregate_view_mode(mode, cx))
                {
                    log::debug!("data grid released before its aggregate view switch: {error}");
                }
            },
        );

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
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(CollectionMetrics::HINT_GAP)
                    .ml(CollectionMetrics::HINT_MARGIN_LEFT)
                    .text_size(CollectionMetrics::HINT_FONT)
                    .text_color(theme.muted_foreground)
                    .child(
                        Icon::new(AppIcon::Lock)
                            .size(CollectionMetrics::NESTED_ICON)
                            .color(theme.muted_foreground),
                    )
                    .child(dbflux_i18n::t!(
                        "document.collection.aggregate.read_only_hint"
                    )),
            )
    }

    fn render_aggregate_results(&self, cx: &mut Context<Self>) -> AnyElement {
        let aggregate = &self.collection.aggregate;

        let empty = |icon: AppIcon, message: String| {
            div()
                .id("aggregate-results-empty")
                .size_full()
                .flex()
                .items_center()
                .justify_center()
                .child(EmptyState::new(icon, message))
                .into_any_element()
        };

        let Some(results) = aggregate.results.as_ref() else {
            return if aggregate.running {
                empty(
                    AppIcon::Loader,
                    dbflux_i18n::t!("document.collection.aggregate.running"),
                )
            } else {
                empty(
                    AppIcon::ChartColumnBig,
                    dbflux_i18n::t!("document.collection.aggregate.empty"),
                )
            };
        };

        if results.documents.is_empty() {
            return empty(
                AppIcon::ChartColumnBig,
                dbflux_i18n::t!("document.collection.aggregate.no_results"),
            );
        }

        match aggregate.view_mode {
            DataViewMode::Table => div()
                .id("aggregate-results-table")
                .size_full()
                .when_some(aggregate.data_table.clone(), |body, table| {
                    body.child(table)
                })
                .into_any_element(),
            DataViewMode::Document => div()
                .id("aggregate-results-tree")
                .size_full()
                .when_some(aggregate.tree.clone(), |body, tree| body.child(tree))
                .into_any_element(),
            DataViewMode::Json => div()
                .id("aggregate-results-json")
                .size_full()
                .bg(cx.theme().background)
                .font_family(dbflux_components::fonts::editor_family(cx))
                .child(
                    Editor::new(&aggregate.json_viewer)
                        .bordered(false)
                        .size_full()
                        .disabled(true),
                )
                .into_any_element(),
        }
    }

    fn render_aggregate_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let aggregate = &self.collection.aggregate;
        let count = aggregate.result_count();
        let truncated = aggregate.truncated();
        let elapsed = aggregate
            .results
            .as_ref()
            .map(|results| format!("{}ms", results.raw.execution_time.as_millis()));

        document_footer(cx)
            .debug_selector(|| "aggregate-footer".to_string())
            .when_some(count, |footer, count| {
                footer.child(footer_item(
                    AppIcon::Rows3,
                    crate::labels::collection_documents(count),
                    cx,
                ))
            })
            .child(
                footer_item(
                    AppIcon::Lock,
                    dbflux_i18n::t!("document.data.grid.status.read_only"),
                    cx,
                )
                .debug_selector(|| "aggregate-footer-read-only".to_string()),
            )
            .when(truncated, |footer| {
                footer.child(div().min_w_0().truncate().child(dbflux_i18n::t!(
                    "document.collection.aggregate.capped",
                    count = super::AGGREGATE_RESULT_LIMIT
                )))
            })
            .child(div().flex_1())
            .when_some(elapsed, |footer, elapsed| {
                footer.child(
                    div()
                        .flex_shrink_0()
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .child(elapsed),
                )
            })
    }

    /// Confirmation before a pipeline that writes, or that could not be
    /// classified, runs.
    pub(in crate::data_grid_panel) fn render_aggregate_confirm(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<impl IntoElement + use<>> {
        let pending = self.collection.aggregate.pending_run.as_ref()?;
        let suppress = pending.suppress;

        let (title, message) = match pending.kind {
            Some(kind) => (
                crate::labels::dangerous_query_title(kind),
                crate::labels::dangerous_query_body(kind),
            ),
            None => (
                dbflux_i18n::t!("document.code.dangerous_query.fallback.title"),
                dbflux_i18n::t!("document.code.dangerous_query.fallback.body"),
            ),
        };

        let body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(message, cx))
            .when_some(pending.query_text.clone(), |body, query| {
                body.child(modal_code(query.trim().to_string(), cx))
            })
            .when(pending.kind.is_some(), |body| {
                body.child(
                    Checkbox::new("aggregate-dont-ask-again")
                        .checked(suppress)
                        .label(dbflux_i18n::t!(
                            "document.code.dangerous_query.dont_ask_again"
                        ))
                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                            if let Some(pending) = this.collection.aggregate.pending_run.as_mut() {
                                pending.suppress = *checked;
                                cx.notify();
                            }
                        })),
                )
            });

        let footer = div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(
                Button::new(
                    "aggregate-confirm-cancel",
                    dbflux_i18n::t!("document.code.dangerous_query.cancel"),
                )
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(
                        ContextId::ConfirmModal,
                        Command::Cancel,
                    ),
                    Button::kbd,
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.cancel_aggregate_run(cx);
                })),
            )
            .child(
                Button::new(
                    "aggregate-confirm-run",
                    dbflux_i18n::t!("document.code.dangerous_query.run_anyway"),
                )
                .danger()
                .icon(AppIcon::Play)
                .when_some(
                    dbflux_ui_base::keymap::shortcut_label(
                        ContextId::ConfirmModal,
                        Command::Execute,
                    ),
                    Button::kbd,
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.confirm_aggregate_run(cx);
                })),
            );

        let entity_close = cx.entity().clone();
        let entity_confirm = cx.entity().clone();

        Some(
            Modal::new(title)
                .body(body)
                .footer(footer)
                .icon(AppIcon::TriangleAlert)
                .width(ModalMetrics::WIDTH)
                .variant(ModalVariant::Danger)
                .focus_handle(self.collection.aggregate.confirm_focus.handle())
                .on_close(move |_, cx| {
                    entity_close.update(cx, |this, cx| this.cancel_aggregate_run(cx));
                })
                .on_confirm(move |_, cx| {
                    entity_confirm.update(cx, |this, cx| this.confirm_aggregate_run(cx));
                }),
        )
    }
}
