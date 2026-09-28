//! Rendering of the builder rail (IslDocBuilder, IslDocBuilderAggregate,
//! IslDocBuilderStates): a header with the query name, the saved queries and
//! Save, the Find / Aggregate switch, the
//! scrolling cards (sync conflict, Filter, Project, Sort / limit / skip,
//! Group), the Preview pinned under them and the fixed footer with Open in
//! editor and Find or Run pipeline.

use dbflux_components::composites::menu_frame;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, Chamfer, Icon, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{
    BuilderMetrics, ChamferCut, ChromeColors, FontSizes, Heights, Spacing, SyntaxColors,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::{
    DocumentCombinator, DocumentFieldType, DocumentProjectionMode, DocumentQueryMode,
    DocumentSortDirection, DocumentSpecProblem,
};
use gpui::Focusable;
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Bounds, Context, FontWeight, HighlightStyle, Hsla, IntoElement, KeyDownEvent,
    Pixels, SharedString, StyledText, Window, canvas, deferred, div, linear_color_stop,
    linear_gradient, px,
};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use gpui_component::theme::Theme;

use super::keyboard::row_id;
use super::model::{
    AccumulatorDraft, AccumulatorOp, ConditionDraft, GroupDraft, GroupStageDraft, NodeDraft,
    NodeId, Operand, ProblemKind,
};
use super::panel::{DocumentBuilderPanel, PickTarget, is_valid_typed_path};
use super::values::{ScalarKind, ValueEditor, ValueProblem, format_value, operator_ranges};
use dbflux_components::composites::{RailOwner, rail_scroll_area, render_rail_menu};

/// Keycap on the Find button: the rail runs on Cmd/Ctrl+Enter.
#[cfg(target_os = "macos")]
const FIND_SHORTCUT_HINT: &str = "Cmd \u{21b5}";
#[cfg(not(target_os = "macos"))]
const FIND_SHORTCUT_HINT: &str = "Ctrl \u{21b5}";

/// Field select of a condition row.
const FIELD_WIDTH: Pixels = px(150.0);
/// Operator select of a condition row.
const OPERATOR_WIDTH: Pixels = px(96.0);
/// Operator list: tall enough for the 13 operators of an unsampled field
/// before it scrolls.
const OPERATOR_LIST_HEIGHT: Pixels = px(340.0);
/// Limit and skip inputs.
const PAGING_WIDTH: Pixels = px(70.0);
/// Chip entry input.
const CHIP_ENTRY_WIDTH: Pixels = px(110.0);
/// Output name input of an accumulator.
const ACCUMULATOR_NAME_WIDTH: Pixels = px(96.0);
/// Field picker popover.
const PICKER_WIDTH: Pixels = px(320.0);
const PICKER_LIST_HEIGHT: Pixels = px(280.0);
/// Indent per nesting level in the field picker.
const PICKER_INDENT: Pixels = px(14.0);
/// Colored bar at the left of a filter group.
const GROUP_BAR_WIDTH: Pixels = px(2.0);
/// Height of the fade over the bottom of the scrolling cards.
const FADE_HEIGHT: Pixels = px(28.0);
/// Query name input in the header.
const NAME_MIN_WIDTH: Pixels = px(120.0);
/// Saved queries popover.
const SAVED_MENU_WIDTH: Pixels = px(300.0);
const SAVED_LIST_HEIGHT: Pixels = px(280.0);

pub(super) fn render_panel(
    panel: &mut DocumentBuilderPanel,
    window: &mut Window,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    panel.ensure_inputs(window, cx);

    let theme = cx.theme().clone();
    let focus_handle = panel.focus_handle(cx);
    let entity = cx.entity();

    let rows = panel.rail_rows(cx);
    panel.rail_mark = panel
        .rail
        .mark(&rows, focus_handle.contains_focused(window, cx), cx);

    div()
        .id("doc-builder")
        .relative()
        .flex()
        .flex_col()
        .size_full()
        .bg(theme.popover)
        .track_focus(&focus_handle)
        .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
            let keystroke = &event.keystroke;
            if keystroke.key == "enter" && keystroke.modifiers.secondary() {
                this.request_run(cx);
                cx.stop_propagation();
            }
        }))
        .child(render_header(panel, &theme, cx))
        .child(render_mode_switch(panel, &theme, cx))
        .child(render_body(panel, &theme, cx))
        .child(render_preview_pane(panel, &theme, cx))
        .child(render_footer(panel, &theme, cx))
        .child(
            canvas(
                move |bounds, _window, cx| {
                    cx.defer(move |cx| {
                        entity.update(cx, |panel, cx| panel.record_rail_bounds(bounds, cx));
                    });
                },
                |_, _, _, _| {},
            )
            .absolute()
            .top_0()
            .left_0()
            .size_full(),
        )
        .children(render_rail_menu(&panel.rail, "doc-builder-rail-menu", cx))
}

// ---------------------------------------------------------------------------
// Header and footer
// ---------------------------------------------------------------------------

/// The header: the query name, the collection it reads, the saved queries
/// of that collection, Save and Close.
fn render_header(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    let can_save = panel.can_save(cx);
    let save_tooltip = if panel.query_name(cx).is_empty() {
        dbflux_i18n::t!("document.collection.builder.saved.save_needs_name")
    } else if can_save {
        dbflux_i18n::t!("document.collection.builder.saved.save")
    } else {
        dbflux_i18n::t!("document.collection.builder.saved.save_invalid")
    };

    panel
        .rail_mark
        .fixed_row(&row_id::name(), div())
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(BuilderMetrics::HEADER_GAP)
        .px(BuilderMetrics::RAIL_PADDING_X)
        .h(BuilderMetrics::HEADER_HEIGHT)
        .border_b_1()
        .border_color(theme.border)
        .child(
            Icon::new(AppIcon::ListFilter)
                .size(BuilderMetrics::HEADER_ICON)
                .color(ChromeColors::tint(theme)),
        )
        .child(
            div().flex_1().min_w(NAME_MIN_WIDTH).child(
                Input::new(&panel.name_input)
                    .id("doc-builder-query-name")
                    .small()
                    .w_full(),
            ),
        )
        .child(
            div()
                .min_w_0()
                .truncate()
                .font_weight(FontWeight::BOLD)
                .text_color(ChromeColors::strong(theme))
                .child(SharedString::from(panel.collection.name.clone())),
        )
        .child(Badge::new(
            panel.collection.database.clone(),
            BadgeTone::Neutral,
        ))
        .child(
            div()
                .relative()
                .child(
                    Button::new(
                        "doc-builder-saved-toggle",
                        dbflux_i18n::t!("document.collection.builder.saved.list"),
                    )
                    .icon(AppIcon::Folder)
                    .icon_only()
                    .selected(panel.saved_menu_open)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| this.toggle_saved_menu(cx))),
                )
                .children(render_saved_menu_if_open(panel, theme, cx)),
        )
        .child(
            Button::new(
                "doc-builder-save",
                dbflux_i18n::t!("document.collection.builder.saved.save"),
            )
            .icon(AppIcon::Save)
            .icon_only()
            .disabled(!can_save)
            .tooltip(save_tooltip)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.request_save(cx))),
        )
        .child(
            Button::new(
                "doc-builder-close",
                dbflux_i18n::t!("document.collection.builder.close"),
            )
            .icon(AppIcon::CircleX)
            .icon_only()
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.request_close(cx))),
        )
}

/// Find | Aggregate under the title, with what the mode means for the
/// result. Aggregate is disabled, with the reason, when the connection
/// cannot run aggregations.
fn render_mode_switch(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    let mode = panel.mode();
    let aggregate_available = panel.aggregate_available();

    let hint = match mode {
        DocumentQueryMode::Find if !aggregate_available => {
            dbflux_i18n::t!("document.collection.builder.mode.aggregate_unavailable")
        }
        DocumentQueryMode::Find => dbflux_i18n::t!("document.collection.builder.mode.find_hint"),
        DocumentQueryMode::Aggregate => {
            dbflux_i18n::t!("document.collection.builder.mode.aggregate_hint")
        }
    };

    let aggregate = Button::new(
        "doc-builder-mode-aggregate",
        dbflux_i18n::t!("document.collection.builder.mode.aggregate"),
    )
    .secondary()
    .icon(AppIcon::ChartColumnBig)
    .selected(mode == DocumentQueryMode::Aggregate)
    .disabled(!aggregate_available)
    .tab_stop(false)
    .on_click(cx.listener(|this, _, _, cx| this.set_mode(DocumentQueryMode::Aggregate, cx)));
    let aggregate = if aggregate_available {
        aggregate
    } else {
        aggregate.tooltip(dbflux_i18n::t!(
            "document.collection.builder.mode.aggregate_unavailable"
        ))
    };

    div()
        .flex()
        .flex_shrink_0()
        .flex_wrap()
        .items_center()
        .gap(Spacing::SM)
        .px(BuilderMetrics::RAIL_PADDING_X)
        .py(Spacing::SM)
        .border_b_1()
        .border_color(theme.border)
        .child(
            Button::new(
                "doc-builder-mode-find",
                dbflux_i18n::t!("document.collection.builder.mode.find"),
            )
            .secondary()
            .icon(AppIcon::Search)
            .selected(mode == DocumentQueryMode::Find)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.set_mode(DocumentQueryMode::Find, cx))),
        )
        .child(aggregate)
        .child(
            div()
                .id("doc-builder-mode-hint")
                .min_w_0()
                .child(caption(hint, theme.muted_foreground)),
        )
}

fn render_footer(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    let aggregate = panel.mode() == DocumentQueryMode::Aggregate;
    let can_run = panel.can_run();
    let run_tooltip = if !aggregate && !panel.sync.held().is_empty() {
        Some(dbflux_i18n::t!("document.collection.builder.find_blocked"))
    } else if !can_run {
        Some(dbflux_i18n::t!("document.collection.builder.find_invalid"))
    } else {
        None
    };

    let (run_id, run_label) = if aggregate {
        (
            "doc-builder-run-pipeline",
            dbflux_i18n::t!("document.collection.builder.run_pipeline"),
        )
    } else {
        (
            "doc-builder-find",
            dbflux_i18n::t!("document.collection.builder.find"),
        )
    };

    let can_open = panel.problems.is_empty() && panel.render_error.is_none();

    div()
        .id("doc-builder-footer")
        .debug_selector(|| "doc-builder-footer".to_string())
        .flex()
        .flex_shrink_0()
        .items_center()
        .gap(BuilderMetrics::FOOTER_GAP)
        .px(BuilderMetrics::RAIL_PADDING_X)
        .py(BuilderMetrics::FOOTER_PADDING_Y)
        .border_t_1()
        .border_color(theme.border)
        .child(
            Button::new(
                "doc-builder-open-editor",
                dbflux_i18n::t!("document.collection.builder.open_in_editor"),
            )
            .secondary()
            .icon(AppIcon::ExternalLink)
            .disabled(!can_open)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.request_open_in_editor(cx))),
        )
        .child(div().flex_1())
        .child(
            Button::new(run_id, run_label)
                .primary()
                .icon(AppIcon::Play)
                .kbd(FIND_SHORTCUT_HINT)
                .disabled(!can_run)
                .when_some(run_tooltip, Button::tooltip)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.request_run(cx))),
        )
}

// ---------------------------------------------------------------------------
// Body
// ---------------------------------------------------------------------------

fn render_body(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    let aggregate = panel.mode() == DocumentQueryMode::Aggregate;

    let conflict = (!aggregate && panel.is_conflicted())
        .then(|| render_conflict(panel, theme, cx).into_any_element());

    let count = panel.draft.condition_count();
    let filter_badge = (count > 0).then(|| {
        Badge::new(
            crate::labels::document_builder_condition_count(count),
            BadgeTone::Accent,
        )
        .into_any_element()
    });

    let filter_card = if aggregate {
        render_match_card(panel, filter_badge, theme, cx)
    } else {
        let root = panel.draft.filter.clone();
        card(
            dbflux_i18n::t!("document.collection.builder.section.filter"),
            AppIcon::ListFilter,
            filter_badge,
            theme,
            render_group(panel, &root, true, theme, cx),
        )
    };

    let project_card = if aggregate {
        disabled_card(
            dbflux_i18n::t!("document.collection.builder.section.project"),
            AppIcon::Columns,
            theme,
            div()
                .id("doc-builder-project-disabled")
                .child(caption(
                    dbflux_i18n::t!("document.collection.builder.project.disabled"),
                    theme.muted_foreground,
                ))
                .into_any_element(),
        )
    } else {
        card(
            dbflux_i18n::t!("document.collection.builder.section.project"),
            AppIcon::Columns,
            None,
            theme,
            render_projection(panel, theme, cx),
        )
    };

    let sort_card = card(
        dbflux_i18n::t!("document.collection.builder.section.sort"),
        AppIcon::ArrowUpDown,
        None,
        theme,
        render_sort(panel, theme, cx),
    );

    let group_card = match panel.draft.group.clone().filter(|_| aggregate) {
        Some(stage) => card(
            dbflux_i18n::t!("document.collection.builder.section.group_stage"),
            AppIcon::ChartColumnBig,
            None,
            theme,
            render_group_stage(panel, &stage, theme, cx),
        ),
        None => card(
            dbflux_i18n::t!("document.collection.builder.section.group"),
            AppIcon::ChartColumnBig,
            None,
            theme,
            render_add_group_stage(panel, theme, cx),
        ),
    };

    // Find keeps the Group card last, collapsed; Aggregate puts the stage
    // right after its $match, where it runs.
    let cards: Vec<AnyElement> = if aggregate {
        vec![filter_card, group_card, project_card, sort_card]
    } else {
        vec![filter_card, project_card, sort_card, group_card]
    };

    let fade = linear_gradient(
        180.,
        linear_color_stop(theme.popover.opacity(0.), 0.),
        linear_color_stop(theme.popover, 1.),
    );

    div()
        .debug_selector(|| "doc-builder-body".to_string())
        .relative()
        .flex_1()
        .min_h(px(0.))
        .flex()
        .flex_col()
        .child(rail_scroll_area(
            &panel.rail,
            "doc-builder-sections",
            div()
                .flex()
                .flex_col()
                .gap(BuilderMetrics::SECTION_GAP)
                .px(BuilderMetrics::RAIL_PADDING_X)
                .py(BuilderMetrics::SECTION_GAP)
                .children(conflict)
                .children(cards),
        ))
        .child(
            div()
                .absolute()
                .left_0()
                .right_0()
                .bottom_0()
                .h(FADE_HEIGHT)
                .bg(fade),
        )
}

/// A section card: the input cut on the ground with a line border, headed by
/// a tint icon, the uppercase label and an optional trailing element.
fn card(
    title: String,
    icon: AppIcon,
    trailing: Option<AnyElement>,
    theme: &Theme,
    body: AnyElement,
) -> AnyElement {
    div()
        .relative()
        .flex()
        .flex_col()
        .flex_shrink_0()
        .gap(BuilderMetrics::CARD_GAP)
        .p(BuilderMetrics::CARD_PADDING)
        .child(
            Chamfer::new(ChamferCut::INPUT)
                .fill(theme.background)
                .border(theme.border),
        )
        .child(
            div()
                .flex()
                .items_center()
                .gap(BuilderMetrics::CARD_HEADER_GAP)
                .child(
                    Icon::new(icon)
                        .size(BuilderMetrics::CARD_ICON)
                        .color(ChromeColors::tint(theme)),
                )
                .child(Text::label(title).font_size(BuilderMetrics::CARD_LABEL_FONT))
                .child(div().flex_1())
                .children(trailing),
        )
        .child(body)
        .into_any_element()
}

/// A card for a section that does not apply in the current mode: dimmed,
/// with the reason as its body.
fn disabled_card(title: String, icon: AppIcon, theme: &Theme, body: AnyElement) -> AnyElement {
    div()
        .opacity(0.6)
        .child(card(title, icon, None, theme, body))
        .into_any_element()
}

fn caption(text: impl Into<SharedString>, color: Hsla) -> AnyElement {
    Text::caption(text.into()).color(color).into_any_element()
}

fn link_button(
    id: impl Into<SharedString>,
    label: String,
    icon: AppIcon,
    on_click: impl Fn(&gpui::ClickEvent, &mut Window, &mut App) + 'static,
) -> Button {
    Button::new(id.into(), label)
        .ghost()
        .inline()
        .icon(icon)
        .tab_stop(false)
        .on_click(on_click)
}

// ---------------------------------------------------------------------------
// Sync conflict
// ---------------------------------------------------------------------------

fn render_conflict(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    let mut slots = Vec::new();
    for clause in panel.sync.unrepresentable() {
        if !slots.contains(&clause.slot) {
            slots.push(clause.slot);
        }
    }

    let titles = slots.into_iter().map(|slot| {
        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Icon::new(AppIcon::TriangleAlert)
                    .small()
                    .color(theme.warning),
            )
            .child(
                Text::body_sm(crate::labels::document_builder_conflict_title(slot))
                    .color(theme.warning)
                    .font_weight(FontWeight::SEMIBOLD),
            )
    });

    let clauses = panel
        .sync
        .unrepresentable()
        .iter()
        .enumerate()
        .map(|(index, clause)| {
            div()
                .id(SharedString::from(format!(
                    "doc-builder-conflict-clause-{index}"
                )))
                .px(Spacing::SM)
                .py(Spacing::XS)
                .bg(theme.muted)
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(SharedString::from(clause.text.clone()))
        });

    let can_keep = !panel.sync.held().is_empty();
    let can_rewrite = panel.problems.is_empty() && panel.render_error.is_none();

    div()
        .id("doc-builder-conflict")
        .relative()
        .flex()
        .flex_col()
        .flex_shrink_0()
        .gap(Spacing::SM)
        .p(BuilderMetrics::CARD_PADDING)
        .child(
            Chamfer::new(ChamferCut::INPUT)
                .fill(theme.warning.opacity(0.08))
                .border(theme.warning.opacity(0.4)),
        )
        .children(titles)
        .children(clauses)
        .child(caption(
            dbflux_i18n::t!("document.collection.builder.conflict.note"),
            theme.muted_foreground,
        ))
        .child(
            panel
                .rail_mark
                .row(&row_id::conflict(), div())
                .flex()
                .gap(Spacing::SM)
                .child(
                    panel.rail_mark.ring_element(
                        &row_id::conflict(),
                        "keep",
                        Button::new(
                            "doc-builder-keep-text",
                            dbflux_i18n::t!("document.collection.builder.conflict.keep"),
                        )
                        .secondary()
                        .disabled(!can_keep)
                        .tab_stop(false)
                        .on_click(cx.listener(|this, _, _, cx| this.keep_text(cx))),
                    ),
                )
                .child(
                    panel.rail_mark.ring_element(
                        &row_id::conflict(),
                        "rewrite",
                        Button::new(
                            "doc-builder-rewrite",
                            dbflux_i18n::t!("document.collection.builder.conflict.rewrite"),
                        )
                        .secondary()
                        .icon(AppIcon::RotateCcw)
                        .disabled(!can_rewrite)
                        .tab_stop(false)
                        .on_click(cx.listener(|this, _, _, cx| this.rewrite_from_builder(cx))),
                    ),
                ),
        )
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

fn render_group(
    panel: &DocumentBuilderPanel,
    group: &GroupDraft,
    is_root: bool,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let group_id = group.id;
    let bar = if is_root {
        ChromeColors::tint(theme)
    } else {
        theme.info
    };

    let combinator_id = |combinator: DocumentCombinator| {
        SharedString::from(format!("{combinator:?}").to_lowercase())
    };
    let weak = cx.weak_entity();
    let switch = SegmentedControl::new(
        [DocumentCombinator::And, DocumentCombinator::Or]
            .into_iter()
            .map(|combinator| {
                let label = match combinator {
                    DocumentCombinator::And => "$and",
                    DocumentCombinator::Or => "$or",
                };
                SegmentedItem::new(combinator_id(combinator), label)
            })
            .collect(),
        combinator_id(group.combinator),
        move |selected, _, cx| {
            let combinator = if selected.as_ref() == "or" {
                DocumentCombinator::Or
            } else {
                DocumentCombinator::And
            };
            if let Some(panel) = weak.upgrade() {
                panel.update(cx, |this, cx| this.set_combinator(group_id, combinator, cx));
            }
        },
    )
    .group(format!("doc-builder-combinator-{group_id}"));

    let quantifier = match group.combinator {
        DocumentCombinator::And => dbflux_i18n::t!("document.collection.builder.filter.all_of"),
        DocumentCombinator::Or => dbflux_i18n::t!("document.collection.builder.filter.any_of"),
    };

    let children: Vec<AnyElement> = group
        .children
        .iter()
        .map(|child| match child {
            NodeDraft::Condition(condition) => render_condition(panel, condition, theme, cx),
            NodeDraft::Group(nested) => render_group(panel, nested, false, theme, cx),
        })
        .collect();

    let empty_group_problem = !is_root
        && panel
            .problems
            .iter()
            .any(|problem| problem.node == group_id && problem.kind == ProblemKind::EmptyGroup);

    let mark = &panel.rail_mark;
    let group_row = row_id::group(group_id);

    let head = mark
        .row(&group_row, div())
        .flex()
        .items_center()
        .gap(BuilderMetrics::ROW_GAP)
        .child(mark.ring_element(&group_row, "combinator", switch))
        .child(caption(quantifier, theme.muted_foreground))
        .child(div().flex_1())
        .when(!is_root, |head| {
            head.child(
                Button::new(
                    SharedString::from(format!("doc-builder-remove-{group_id}")),
                    dbflux_i18n::t!("document.collection.builder.filter.remove"),
                )
                .ghost()
                .inline()
                .icon(AppIcon::CircleX)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(move |this, _, _, cx| this.remove_node(group_id, cx))),
            )
        });

    let actions = div()
        .flex()
        .gap(Spacing::SM)
        .child(mark.ring_element(
            &group_row,
            "add-condition",
            link_button(
                format!("doc-builder-add-condition-{group_id}"),
                dbflux_i18n::t!("document.collection.builder.filter.add_condition"),
                AppIcon::Plus,
                cx.listener(move |this, _, _, cx| this.add_condition(group_id, cx)),
            ),
        ))
        .child(mark.ring_element(
            &group_row,
            "add-group",
            link_button(
                format!("doc-builder-add-group-{group_id}"),
                dbflux_i18n::t!("document.collection.builder.filter.add_group"),
                AppIcon::Plus,
                cx.listener(move |this, _, _, cx| this.add_group(group_id, cx)),
            ),
        ));

    div()
        .id(SharedString::from(format!("doc-builder-group-{group_id}")))
        .flex()
        .gap(Spacing::SM)
        .child(div().flex_shrink_0().w(GROUP_BAR_WIDTH).bg(bar))
        .child(
            div()
                .flex_1()
                .min_w_0()
                .flex()
                .flex_col()
                .gap(BuilderMetrics::ROW_GAP)
                .child(head)
                .children(children)
                .when(empty_group_problem, |body| {
                    body.child(caption(
                        dbflux_i18n::t!("document.collection.builder.problem.empty_group"),
                        theme.danger,
                    ))
                })
                .child(actions),
        )
        .into_any_element()
}

fn render_condition(
    panel: &DocumentBuilderPanel,
    condition: &ConditionDraft,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let id = condition.id;
    let types = panel.types_for(id, &condition.path);
    let in_elem_match = panel
        .draft
        .condition_scope(id)
        .is_some_and(|scope| !scope.is_empty());

    let field = render_field_button(panel, condition, &types, in_elem_match, theme, cx);

    let operator = Some(render_operator_select(panel, condition, theme, cx));

    let value = render_value(panel, condition, theme, cx);

    let problem = panel
        .problems
        .iter()
        .find(|problem| problem.node == id)
        .map(|problem| problem.kind.clone());
    let chip_problem = panel.chip_problems.get(&id).copied();

    let looks_like_object_id = matches!(
        problem,
        Some(ProblemKind::Value(ValueProblem::LooksLikeObjectId))
    ) || chip_problem == Some(ValueProblem::LooksLikeObjectId);

    let problem_text = problem
        .as_ref()
        .filter(|kind| **kind != ProblemKind::EmptyGroup)
        .map(crate::labels::document_builder_problem)
        .or_else(|| chip_problem.map(crate::labels::document_builder_value_problem));

    let nested = match &condition.operand {
        Operand::Nested(body) => Some(render_group(panel, body, false, theme, cx)),
        _ => None,
    };

    let mark = &panel.rail_mark;
    let condition_row = row_id::condition(id);

    let row = mark
        .row(&condition_row, div())
        .flex()
        .items_center()
        .gap(BuilderMetrics::ROW_GAP)
        .child(field)
        .children(operator)
        .child(
            mark.ring(&condition_row, "value", div())
                .flex_1()
                .min_w_0()
                .child(value),
        )
        .child(
            Button::new(
                SharedString::from(format!("doc-builder-remove-{id}")),
                dbflux_i18n::t!("document.collection.builder.filter.remove"),
            )
            .ghost()
            .inline()
            .icon(AppIcon::CircleX)
            .icon_only()
            .tab_stop(false)
            .on_click(cx.listener(move |this, _, _, cx| this.remove_node(id, cx))),
        );

    div()
        .id(SharedString::from(format!("doc-builder-condition-{id}")))
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .child(row)
        .when_some(problem_text, |column, text| {
            column.child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(caption(text, theme.danger))
                    .when(looks_like_object_id, |line| {
                        line.child(mark.ring_element(
                            &condition_row,
                            "use-object-id",
                            link_button(
                                format!("doc-builder-use-oid-{id}"),
                                dbflux_i18n::t!("document.collection.builder.filter.use_object_id"),
                                AppIcon::Hash,
                                cx.listener(move |this, _, _, cx| {
                                    this.set_kind(id, ScalarKind::ObjectId, cx)
                                }),
                            ),
                        ))
                    }),
            )
        })
        .when(types.len() > 1, |column| {
            column.child(caption(
                crate::labels::document_builder_mixed_types(&types),
                theme.warning,
            ))
        })
        .children(nested)
        .into_any_element()
}

fn render_field_button(
    panel: &DocumentBuilderPanel,
    condition: &ConditionDraft,
    types: &[DocumentFieldType],
    in_elem_match: bool,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let id = condition.id;
    let target = PickTarget::Condition(id);

    let (label, label_color) = if !condition.path.is_empty() {
        (condition.path.clone(), ChromeColors::strong(theme))
    } else if in_elem_match {
        (
            dbflux_i18n::t!("document.collection.builder.filter.element"),
            theme.muted_foreground,
        )
    } else {
        (
            dbflux_i18n::t!("document.collection.builder.filter.pick_field"),
            theme.muted_foreground,
        )
    };

    let tag = if condition.path.is_empty() {
        None
    } else if !types.is_empty() {
        Some(crate::labels::document_field_type_tags(types))
    } else if !panel.is_sampled(id, &condition.path) {
        Some(dbflux_i18n::t!(
            "document.collection.builder.filter.unsampled"
        ))
    } else {
        None
    };

    let trigger = div()
        .id(SharedString::from(format!("doc-builder-field-{id}")))
        .relative()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .w_full()
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .cursor_pointer()
        .child(
            Chamfer::new(ChamferCut::CONTROL)
                .fill(theme.background)
                .border(theme.input),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .text_color(label_color)
                .child(SharedString::from(label)),
        )
        .children(tag.map(|tag| type_tag(tag, theme)))
        .on_click(cx.listener(move |this, _, window, cx| this.open_picker(target, window, cx)));

    panel
        .rail_mark
        .ring(&row_id::condition(id), "field", div())
        .w(FIELD_WIDTH)
        .flex_shrink_0()
        .child(trigger)
        .children(render_picker_if_open(panel, target, theme, cx))
        .into_any_element()
}

/// The operator select of condition `id`: a trigger showing the operator and,
/// while open, the list of the operators its field offers. The list takes the
/// keyboard: Up and Down move, Enter picks, Escape closes.
fn render_operator_select(
    panel: &DocumentBuilderPanel,
    condition: &ConditionDraft,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let id = condition.id;
    let open = panel
        .operator_menu
        .as_ref()
        .filter(|menu| menu.condition == id);

    let trigger_selector = format!("doc-builder-operator-{id}-trigger");
    let trigger = div()
        .id(SharedString::from(trigger_selector.clone()))
        .debug_selector(move || trigger_selector)
        .relative()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .w_full()
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .cursor_pointer()
        .child(
            Chamfer::new(ChamferCut::CONTROL)
                .fill(theme.background)
                .border(theme.input),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .text_color(ChromeColors::strong(theme))
                .child(SharedString::from(panel.operator_label(condition.operator))),
        )
        .child(
            Icon::new(AppIcon::ChevronDown)
                .small()
                .color(theme.muted_foreground),
        )
        .on_click(
            cx.listener(move |this, _, window, cx| this.toggle_operator_menu(id, window, cx)),
        );

    let list = open.map(|menu| {
        let rows: Vec<AnyElement> = panel
            .operator_choices_for(id)
            .into_iter()
            .enumerate()
            .map(|(index, operator)| {
                let label = panel.operator_label(operator);
                let row_selector = format!(
                    "doc-builder-operator-{id}-option-{}",
                    operator.name().replace('_', "")
                );
                let highlighted = index == menu.highlighted;
                let current = operator == condition.operator;

                div()
                    .id(SharedString::from(row_selector.clone()))
                    .debug_selector(move || row_selector)
                    .flex()
                    .items_center()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::XS)
                    .cursor_pointer()
                    .when(highlighted, |row| row.bg(theme.accent.opacity(0.2)))
                    .when(current, |row| row.font_weight(FontWeight::BOLD))
                    .hover(|row| row.bg(theme.accent.opacity(0.15)))
                    .child(SharedString::from(label))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.choose_operator(id, operator, window, cx)
                    }))
                    .into_any_element()
            })
            .collect();

        let popover = menu_frame(cx)
            .id(SharedString::from(format!(
                "doc-builder-operator-{id}-menu"
            )))
            .absolute()
            .top_full()
            .left_0()
            .mt(Spacing::XS)
            .min_w(OPERATOR_WIDTH)
            .occlude()
            .track_focus(&menu.focus)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                match event.keystroke.key.as_str() {
                    "down" => this.move_operator_highlight(1, cx),
                    "up" => this.move_operator_highlight(-1, cx),
                    "enter" => this.choose_highlighted_operator(window, cx),
                    "escape" => this.close_operator_menu(window, cx),
                    _ => return,
                }
                cx.stop_propagation();
            }))
            .on_mouse_down_out(
                cx.listener(|this, _, window, cx| this.close_operator_menu(window, cx)),
            )
            .child(
                div()
                    .id(SharedString::from(format!(
                        "doc-builder-operator-{id}-list"
                    )))
                    .max_h(OPERATOR_LIST_HEIGHT)
                    .flex()
                    .flex_col()
                    .overflow_y_scrollbar()
                    .children(rows),
            );

        deferred(popover).with_priority(2).into_any_element()
    });

    panel
        .rail_mark
        .ring(&row_id::condition(id), "operator", div())
        .id(SharedString::from(format!("doc-builder-operator-{id}")))
        .relative()
        .w(OPERATOR_WIDTH)
        .flex_shrink_0()
        .child(trigger)
        .children(list)
        .into_any_element()
}

fn type_tag(tag: String, theme: &Theme) -> AnyElement {
    div()
        .flex_shrink_0()
        .font_family(AppFonts::MONO)
        .text_size(FontSizes::LABEL)
        .text_color(theme.muted_foreground)
        .child(SharedString::from(tag))
        .into_any_element()
}

fn render_value(
    panel: &DocumentBuilderPanel,
    condition: &ConditionDraft,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let id = condition.id;
    let input = panel.value_inputs.get(&id).map(|input| input.state.clone());
    let muted = theme.muted_foreground;

    match (condition.editor(), &condition.operand) {
        (ValueEditor::Toggle, Operand::Toggle(flag)) => toggle(id, *flag, cx).into_any_element(),
        (ValueEditor::Nested, _) => div().into_any_element(),
        (ValueEditor::Chips(kind), Operand::Chips(items)) => {
            let chips = items.iter().enumerate().map(|(index, item)| {
                panel
                    .rail_mark
                    .row(&row_id::chip(id, index), div())
                    .id(SharedString::from(format!("doc-builder-chip-{id}-{index}")))
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .px(Spacing::XXS)
                    .bg(theme.secondary)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::XS)
                    .child(SharedString::from(format_value(
                        item,
                        kind,
                        panel.draft.value_syntax(),
                    )))
                    .child(
                        Button::new(
                            SharedString::from(format!("doc-builder-chip-remove-{id}-{index}")),
                            dbflux_i18n::t!("document.collection.builder.filter.remove"),
                        )
                        .ghost()
                        .inline()
                        .icon(AppIcon::X)
                        .icon_only()
                        .tab_stop(false)
                        .on_click(
                            cx.listener(move |this, _, _, cx| this.remove_chip(id, index, cx)),
                        ),
                    )
            });

            div()
                .flex()
                .flex_wrap()
                .items_center()
                .gap(Spacing::XS)
                .children(chips)
                .when_some(input, |row, state| {
                    row.child(
                        div().w(CHIP_ENTRY_WIDTH).child(
                            Input::new(&state)
                                .id(SharedString::from(format!("doc-builder-value-{id}")))
                                .small()
                                .placeholder(dbflux_i18n::t!(
                                    "document.collection.builder.filter.chip_placeholder"
                                ))
                                .w_full(),
                        ),
                    )
                })
                .into_any_element()
        }
        (editor, _) => {
            let Some(state) = input else {
                return div().into_any_element();
            };

            let field = Input::new(&state)
                .id(SharedString::from(format!("doc-builder-value-{id}")))
                .small()
                .w_full();

            let object_id_affixes = panel.draft.value_syntax().object_id_affixes();
            let field = match (editor, object_id_affixes) {
                (ValueEditor::Scalar(ScalarKind::ObjectId), Some((prefix, suffix))) => field
                    .prefix(mono_affix(prefix, muted))
                    .suffix(mono_affix(suffix, muted)),
                (ValueEditor::Scalar(ScalarKind::Date), _) => field
                    .prefix(Icon::new(AppIcon::Clock).small().color(muted))
                    .suffix(mono_affix("UTC", muted)),
                (ValueEditor::Pattern, _) => field.placeholder("/pattern/i"),
                _ => field.placeholder(dbflux_i18n::t!(
                    "document.collection.builder.filter.value_placeholder"
                )),
            };

            field.into_any_element()
        }
    }
}

fn mono_affix(text: impl Into<SharedString>, color: Hsla) -> AnyElement {
    div()
        .font_family(AppFonts::MONO)
        .text_size(FontSizes::XS)
        .text_color(color)
        .child(text.into())
        .into_any_element()
}

fn toggle(id: NodeId, flag: bool, cx: &mut Context<DocumentBuilderPanel>) -> SegmentedControl {
    let weak = cx.weak_entity();
    let active = if flag { "true" } else { "false" };

    SegmentedControl::new(
        vec![
            SegmentedItem::new(
                "true",
                dbflux_i18n::t!("document.collection.builder.filter.bool_true"),
            ),
            SegmentedItem::new(
                "false",
                dbflux_i18n::t!("document.collection.builder.filter.bool_false"),
            ),
        ],
        active,
        move |selected, _, cx| {
            let flag = selected.as_ref() == "true";
            if let Some(panel) = weak.upgrade() {
                panel.update(cx, |this, cx| this.set_toggle(id, flag, cx));
            }
        },
    )
    .group(format!("doc-builder-bool-{id}"))
}

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------

fn render_projection(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let mode_id = |mode: DocumentProjectionMode| match mode {
        DocumentProjectionMode::Include => "include",
        DocumentProjectionMode::Exclude => "exclude",
    };
    let weak = cx.weak_entity();
    let switch = SegmentedControl::new(
        vec![
            SegmentedItem::new(
                "include",
                dbflux_i18n::t!("document.collection.builder.project.include"),
            ),
            SegmentedItem::new(
                "exclude",
                dbflux_i18n::t!("document.collection.builder.project.exclude"),
            ),
        ],
        mode_id(panel.draft.projection.mode),
        move |selected, _, cx| {
            let mode = if selected.as_ref() == "exclude" {
                DocumentProjectionMode::Exclude
            } else {
                DocumentProjectionMode::Include
            };
            if let Some(panel) = weak.upgrade() {
                panel.update(cx, |this, cx| this.set_projection_mode(mode, cx));
            }
        },
    )
    .group("doc-builder-projection-mode");

    let chips = panel
        .draft
        .projection
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            panel
                .rail_mark
                .row(&row_id::projection_field(index), div())
                .id(SharedString::from(format!(
                    "doc-builder-projection-{index}"
                )))
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .px(Spacing::XXS)
                .bg(theme.secondary)
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .child(SharedString::from(field.clone()))
                .child(
                    Button::new(
                        SharedString::from(format!("doc-builder-projection-remove-{index}")),
                        dbflux_i18n::t!("document.collection.builder.filter.remove"),
                    )
                    .ghost()
                    .inline()
                    .icon(AppIcon::X)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(
                        cx.listener(move |this, _, _, cx| this.remove_projection_field(index, cx)),
                    ),
                )
        })
        .collect::<Vec<_>>();

    let add = panel
        .rail_mark
        .row(&row_id::projection_add(), div())
        .relative()
        .child(link_button(
            "doc-builder-projection-add",
            dbflux_i18n::t!("document.collection.builder.project.add_field"),
            AppIcon::Plus,
            cx.listener(|this, _, window, cx| this.open_picker(PickTarget::Projection, window, cx)),
        ))
        .children(render_picker_if_open(
            panel,
            PickTarget::Projection,
            theme,
            cx,
        ));

    div()
        .flex()
        .flex_col()
        .gap(BuilderMetrics::ROW_GAP)
        .child(
            panel
                .rail_mark
                .row(&row_id::projection_mode(), div())
                .flex()
                .child(switch),
        )
        .child(
            div()
                .flex()
                .flex_wrap()
                .items_center()
                .gap(BuilderMetrics::CHIP_GAP)
                .children(chips)
                .child(add),
        )
        .child(caption(
            dbflux_i18n::t!("document.collection.builder.project.id_note"),
            theme.muted_foreground,
        ))
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Sort, limit and skip
// ---------------------------------------------------------------------------

/// A sort key being dragged to a new position.
#[derive(Clone)]
struct SortDrag {
    index: usize,
    label: SharedString,
}

struct SortDragPreview {
    label: SharedString,
}

impl Render for SortDragPreview {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        div()
            .px(Spacing::SM)
            .py(Spacing::XS)
            .bg(theme.popover)
            .border_1()
            .border_color(theme.border)
            .font_family(AppFonts::MONO)
            .text_size(FontSizes::XS)
            .child(self.label.clone())
    }
}

fn render_sort(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let rows = panel
        .draft
        .sort
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let types = panel.sort_types(&key.path);
            let tag = types
                .first()
                .map(|first| crate::labels::document_field_type_tag(*first));
            let unknown = panel
                .problems
                .iter()
                .find_map(|problem| match &problem.kind {
                    ProblemKind::Spec(
                        spec_problem @ DocumentSpecProblem::AggregateSortPathUnknown { path },
                    ) if *path == key.path => Some(crate::labels::document_builder_problem(
                        &ProblemKind::Spec(spec_problem.clone()),
                    )),
                    _ => None,
                });
            let drag = SortDrag {
                index,
                label: SharedString::from(key.path.clone()),
            };

            let row = panel
                .rail_mark
                .row(&row_id::sort(index), div())
                .id(SharedString::from(format!("doc-builder-sort-{index}")))
                .flex()
                .items_center()
                .gap(BuilderMetrics::ROW_GAP)
                .drag_over::<SortDrag>(|style, _, _, cx| style.bg(cx.theme().accent.opacity(0.2)))
                .on_drop(cx.listener(move |this, drag: &SortDrag, _, cx| {
                    this.move_sort_key(drag.index, index, cx)
                }))
                .child(
                    div()
                        .id(SharedString::from(format!(
                            "doc-builder-sort-handle-{index}"
                        )))
                        .cursor_grab()
                        .child(
                            Icon::new(AppIcon::ArrowUpDown)
                                .small()
                                .color(theme.muted_foreground),
                        )
                        .on_drag(drag, |drag, _, _, cx| {
                            cx.new(|_| SortDragPreview {
                                label: drag.label.clone(),
                            })
                        }),
                )
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .truncate()
                        .font_family(AppFonts::MONO)
                        .text_size(FontSizes::XS)
                        .child(SharedString::from(key.path.clone())),
                )
                .children(tag.map(|tag| type_tag(tag, theme)))
                .child(direction_switch(index, key.direction, cx))
                .child(
                    Button::new(
                        SharedString::from(format!("doc-builder-sort-remove-{index}")),
                        dbflux_i18n::t!("document.collection.builder.filter.remove"),
                    )
                    .ghost()
                    .inline()
                    .icon(AppIcon::CircleX)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(cx.listener(move |this, _, _, cx| this.remove_sort_key(index, cx))),
                );

            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .child(row)
                .when_some(unknown, |column, text| {
                    column.child(caption(text, theme.danger))
                })
        })
        .collect::<Vec<_>>();

    let paging_row = row_id::paging();
    let add = panel
        .rail_mark
        .ring(&paging_row, "add", div())
        .relative()
        .child(link_button(
            "doc-builder-sort-add",
            dbflux_i18n::t!("document.collection.builder.sort.add_key"),
            AppIcon::Plus,
            cx.listener(|this, _, window, cx| this.open_picker(PickTarget::Sort, window, cx)),
        ))
        .children(render_picker_if_open(panel, PickTarget::Sort, theme, cx));

    let paging_field = |id: &'static str, field: &str, label: String, state| {
        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(caption(label.clone(), theme.muted_foreground))
            .child(
                panel
                    .rail_mark
                    .ring(&paging_row, field, div())
                    .w(PAGING_WIDTH)
                    .child(Input::new(state).id(id).small().aria_label(label).w_full()),
            )
    };

    let paging = panel
        .rail_mark
        .row(&paging_row, div())
        .flex()
        .items_center()
        .gap(Spacing::MD)
        .child(add)
        .child(div().flex_1())
        .child(paging_field(
            "doc-builder-limit",
            "limit",
            dbflux_i18n::t!("document.collection.builder.sort.limit"),
            &panel.limit_input,
        ))
        .child(paging_field(
            "doc-builder-skip",
            "skip",
            dbflux_i18n::t!("document.collection.builder.sort.skip"),
            &panel.skip_input,
        ));

    div()
        .flex()
        .flex_col()
        .gap(BuilderMetrics::ROW_GAP)
        .children(rows)
        .child(paging)
        .when(panel.limit_problem, |column| {
            column.child(caption(
                dbflux_i18n::t!("document.collection.builder.sort.invalid_limit"),
                theme.danger,
            ))
        })
        .when(panel.skip_problem, |column| {
            column.child(caption(
                dbflux_i18n::t!("document.collection.builder.sort.invalid_skip"),
                theme.danger,
            ))
        })
        .when(panel.mode() == DocumentQueryMode::Aggregate, |column| {
            column.child(caption(
                dbflux_i18n::t!("document.collection.builder.sort.aggregate_note"),
                theme.muted_foreground,
            ))
        })
        .into_any_element()
}

fn direction_switch(
    index: usize,
    direction: DocumentSortDirection,
    cx: &mut Context<DocumentBuilderPanel>,
) -> SegmentedControl {
    let weak = cx.weak_entity();
    let active = match direction {
        DocumentSortDirection::Ascending => "asc",
        DocumentSortDirection::Descending => "desc",
    };

    SegmentedControl::new(
        vec![
            SegmentedItem::new(
                "asc",
                dbflux_i18n::t!("document.collection.builder.sort.ascending"),
            ),
            SegmentedItem::new(
                "desc",
                dbflux_i18n::t!("document.collection.builder.sort.descending"),
            ),
        ],
        active,
        move |selected, _, cx| {
            let direction = if selected.as_ref() == "desc" {
                DocumentSortDirection::Descending
            } else {
                DocumentSortDirection::Ascending
            };
            if let Some(panel) = weak.upgrade() {
                panel.update(cx, |this, cx| this.set_sort_direction(index, direction, cx));
            }
        },
    )
    .group(format!("doc-builder-sort-dir-{index}"))
}

// ---------------------------------------------------------------------------
// Aggregate: $match summary and group stage
// ---------------------------------------------------------------------------

/// The Filter card in Aggregate mode: a one-line `$match` summary with an
/// Edit link that opens the full filter again.
fn render_match_card(
    panel: &DocumentBuilderPanel,
    badge: Option<AnyElement>,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let expanded = panel.filter_expanded;
    let toggle_label = if expanded {
        dbflux_i18n::t!("document.collection.builder.match.done")
    } else {
        dbflux_i18n::t!("document.collection.builder.match.edit")
    };
    let toggle = link_button(
        "doc-builder-match-edit",
        toggle_label,
        if expanded {
            AppIcon::ChevronUp
        } else {
            AppIcon::Pencil
        },
        cx.listener(|this, _, _, cx| this.toggle_filter_expanded(cx)),
    )
    .into_any_element();

    let trailing = panel
        .rail_mark
        .row(&row_id::match_summary(), div())
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .children(badge)
        .child(toggle)
        .into_any_element();

    let conflict_note = panel.is_conflicted().then(|| {
        caption(
            dbflux_i18n::t!("document.collection.builder.match.conflict"),
            theme.warning,
        )
    });

    let body = if expanded {
        let root = panel.draft.filter.clone();
        div()
            .flex()
            .flex_col()
            .gap(BuilderMetrics::ROW_GAP)
            .child(render_group(panel, &root, true, theme, cx))
            .children(conflict_note)
            .into_any_element()
    } else {
        let summary = match_summary(panel);
        let has_problem = panel
            .problems
            .iter()
            .any(|problem| panel.draft.node_ids().contains(&problem.node));

        div()
            .id("doc-builder-match-summary")
            .debug_selector(|| "doc-builder-match-summary".to_string())
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::XS)
                    .child(
                        div()
                            .flex_shrink_0()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(SyntaxColors::for_current(cx).keyword)
                            .child("$match"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .text_color(ChromeColors::strong(theme))
                            .child(SharedString::from(summary)),
                    ),
            )
            .when(has_problem, |column| {
                column.child(caption(
                    dbflux_i18n::t!("document.collection.builder.find_invalid"),
                    theme.danger,
                ))
            })
            .children(conflict_note)
            .into_any_element()
    };

    card(
        dbflux_i18n::t!("document.collection.builder.section.filter"),
        AppIcon::ListFilter,
        Some(trailing),
        theme,
        body,
    )
}

/// The root conditions in one line, nested groups counted.
fn match_summary(panel: &DocumentBuilderPanel) -> String {
    let mut parts = Vec::new();
    let mut groups = 0;

    for child in &panel.draft.filter.children {
        match child {
            NodeDraft::Condition(condition) => parts.push(condition_summary(panel, condition)),
            NodeDraft::Group(_) => groups += 1,
        }
    }

    if groups > 0 {
        parts.push(crate::labels::document_builder_group_count(groups));
    }

    if parts.is_empty() {
        dbflux_i18n::t!("document.collection.builder.match.everything")
    } else {
        parts.join(", ")
    }
}

fn condition_summary(panel: &DocumentBuilderPanel, condition: &ConditionDraft) -> String {
    let operator = panel.operator_label(condition.operator);
    let value = match &condition.operand {
        Operand::Text { text, .. } => text.clone(),
        Operand::Toggle(flag) => flag.to_string(),
        Operand::Chips(items) => format!(
            "[{}]",
            items
                .iter()
                .map(|item| format_value(item, condition.kind, panel.draft.value_syntax()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Operand::Nested(_) => "{\u{2026}}".to_string(),
    };

    format!("{} {operator} {value}", condition.path)
}

/// The collapsed Group card of Find mode.
fn render_add_group_stage(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let available = panel.aggregate_available();

    let add = link_button(
        "doc-builder-group-add",
        dbflux_i18n::t!("document.collection.builder.group.add"),
        AppIcon::Plus,
        cx.listener(|this, _, _, cx| this.add_group_stage(cx)),
    )
    .disabled(!available);
    let add = if available {
        add
    } else {
        add.tooltip(dbflux_i18n::t!(
            "document.collection.builder.mode.aggregate_unavailable"
        ))
    };

    div()
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .child(
            panel
                .rail_mark
                .row(&row_id::group_stage_add(), div())
                .flex()
                .child(add),
        )
        .child(caption(
            dbflux_i18n::t!("document.collection.builder.group.add_hint"),
            theme.muted_foreground,
        ))
        .into_any_element()
}

fn render_group_stage(
    panel: &DocumentBuilderPanel,
    stage: &GroupStageDraft,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let keys = stage
        .keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let tag = panel
                .catalog
                .types(key)
                .first()
                .map(|first| crate::labels::document_field_type_tag(*first));

            panel
                .rail_mark
                .row(&row_id::group_key(index), div())
                .id(SharedString::from(format!("doc-builder-group-key-{index}")))
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .px(Spacing::XXS)
                .bg(theme.secondary)
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .child(SharedString::from(key.clone()))
                .children(tag.map(|tag| type_tag(tag, theme)))
                .child(
                    Button::new(
                        SharedString::from(format!("doc-builder-group-key-remove-{index}")),
                        dbflux_i18n::t!("document.collection.builder.filter.remove"),
                    )
                    .ghost()
                    .inline()
                    .icon(AppIcon::X)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(cx.listener(move |this, _, _, cx| this.remove_group_key(index, cx))),
                )
                .into_any_element()
        })
        .collect::<Vec<_>>();

    let add_key = panel
        .rail_mark
        .row(&row_id::group_key_add(), div())
        .debug_selector(|| "doc-builder-group-key-add".to_string())
        .relative()
        .child(link_button(
            "doc-builder-group-key-add",
            dbflux_i18n::t!("document.collection.builder.group.add_key"),
            AppIcon::Plus,
            cx.listener(|this, _, window, cx| this.open_picker(PickTarget::GroupKey, window, cx)),
        ))
        .children(render_picker_if_open(
            panel,
            PickTarget::GroupKey,
            theme,
            cx,
        ));

    let keys_row = div()
        .flex()
        .flex_wrap()
        .items_center()
        .gap(BuilderMetrics::CHIP_GAP)
        .child(caption(
            dbflux_i18n::t!("document.collection.builder.group.group_by"),
            theme.muted_foreground,
        ))
        .when(stage.keys.is_empty(), |row| {
            row.child(caption(
                dbflux_i18n::t!("document.collection.builder.group.all_documents"),
                theme.muted_foreground,
            ))
        })
        .children(keys)
        .child(add_key);

    let accumulators = stage
        .accumulators
        .iter()
        .map(|accumulator| render_accumulator(panel, accumulator, theme, cx))
        .collect::<Vec<_>>();

    let stage_problem = panel
        .problems
        .iter()
        .filter(|problem| problem.node == stage.id)
        .find(|problem| {
            !matches!(
                problem.kind,
                ProblemKind::Spec(DocumentSpecProblem::AggregateSortPathUnknown { .. })
            )
        })
        .map(|problem| crate::labels::document_builder_problem(&problem.kind));

    let actions_row = row_id::group_stage_actions();

    div()
        .id("doc-builder-group-stage")
        .debug_selector(|| "doc-builder-group-stage".to_string())
        .flex()
        .flex_col()
        .gap(BuilderMetrics::ROW_GAP)
        .child(keys_row)
        .child(caption(
            dbflux_i18n::t!("document.collection.builder.group.accumulators"),
            theme.muted_foreground,
        ))
        .children(accumulators)
        .when_some(stage_problem, |column, text| {
            column.child(caption(text, theme.danger))
        })
        .child(
            panel
                .rail_mark
                .row(&actions_row, div())
                .flex()
                .gap(Spacing::SM)
                .child(panel.rail_mark.ring_element(
                    &actions_row,
                    "add-accumulator",
                    link_button(
                        "doc-builder-acc-add",
                        dbflux_i18n::t!("document.collection.builder.group.add_accumulator"),
                        AppIcon::Plus,
                        cx.listener(|this, _, _, cx| this.add_accumulator(cx)),
                    ),
                ))
                .child(div().flex_1())
                .child(panel.rail_mark.ring_element(
                    &actions_row,
                    "remove-stage",
                    link_button(
                        "doc-builder-group-remove",
                        dbflux_i18n::t!("document.collection.builder.group.remove"),
                        AppIcon::CircleX,
                        cx.listener(|this, _, _, cx| this.remove_group_stage(cx)),
                    ),
                )),
        )
        .into_any_element()
}

/// One accumulator: its output name, `$count` / `$sum` / `$avg`, the field
/// it reads (none for `$count`) and its problem, if any.
fn render_accumulator(
    panel: &DocumentBuilderPanel,
    accumulator: &AccumulatorDraft,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let id = accumulator.id;
    let weak = cx.weak_entity();
    let op_id = |op: AccumulatorOp| match op {
        AccumulatorOp::Count => "count",
        AccumulatorOp::Sum => "sum",
        AccumulatorOp::Avg => "avg",
    };

    let op_switch = SegmentedControl::new(
        AccumulatorOp::ALL
            .into_iter()
            .map(|op| SegmentedItem::new(op_id(op), format!("${}", op_id(op))))
            .collect(),
        op_id(accumulator.op),
        move |selected, _, cx| {
            let op = match selected.as_ref() {
                "sum" => AccumulatorOp::Sum,
                "avg" => AccumulatorOp::Avg,
                _ => AccumulatorOp::Count,
            };
            if let Some(panel) = weak.upgrade() {
                panel.update(cx, |this, cx| this.set_accumulator_op(id, op, cx));
            }
        },
    )
    .group(format!("doc-builder-acc-op-{id}"));

    let name = panel
        .accumulator_inputs
        .get(&id)
        .map(|input| input.state.clone())
        .map(|state| {
            panel
                .rail_mark
                .ring(&row_id::accumulator(id), "name", div())
                .w(ACCUMULATOR_NAME_WIDTH)
                .flex_shrink_0()
                .child(
                    Input::new(&state)
                        .id(SharedString::from(format!("doc-builder-acc-name-{id}")))
                        .small()
                        .placeholder(dbflux_i18n::t!(
                            "document.collection.builder.group.name_placeholder"
                        ))
                        .w_full(),
                )
                .into_any_element()
        });

    let field = if accumulator.op.takes_field() {
        render_accumulator_field(panel, accumulator, theme, cx)
    } else {
        div()
            .flex_1()
            .min_w_0()
            .child(caption(
                dbflux_i18n::t!("document.collection.builder.group.no_field"),
                theme.muted_foreground,
            ))
            .into_any_element()
    };

    let problem = panel
        .problems
        .iter()
        .find(|problem| problem.node == id)
        .map(|problem| match problem.kind {
            ProblemKind::MissingField => {
                dbflux_i18n::t!("document.collection.builder.group.pick_number")
            }
            ref kind => crate::labels::document_builder_problem(kind),
        });

    div()
        .id(SharedString::from(format!("doc-builder-acc-{id}")))
        .flex()
        .flex_col()
        .gap(Spacing::XS)
        .child(
            panel
                .rail_mark
                .row(&row_id::accumulator(id), div())
                .flex()
                .items_center()
                .gap(BuilderMetrics::ROW_GAP)
                .children(name)
                .child(
                    panel
                        .rail_mark
                        .ring_element(&row_id::accumulator(id), "op", op_switch),
                )
                .child(field)
                .child(
                    Button::new(
                        SharedString::from(format!("doc-builder-acc-remove-{id}")),
                        dbflux_i18n::t!("document.collection.builder.filter.remove"),
                    )
                    .ghost()
                    .inline()
                    .icon(AppIcon::CircleX)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(cx.listener(move |this, _, _, cx| this.remove_accumulator(id, cx))),
                ),
        )
        .when_some(problem, |column, text| {
            column.child(caption(text, theme.danger))
        })
        .into_any_element()
}

fn render_accumulator_field(
    panel: &DocumentBuilderPanel,
    accumulator: &AccumulatorDraft,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> AnyElement {
    let id = accumulator.id;
    let target = PickTarget::Accumulator(id);

    let (label, color) = if accumulator.path.is_empty() {
        (
            dbflux_i18n::t!("document.collection.builder.filter.pick_field"),
            theme.muted_foreground,
        )
    } else {
        (accumulator.path.clone(), ChromeColors::strong(theme))
    };
    let tag = (!accumulator.path.is_empty())
        .then(|| panel.catalog.types(&accumulator.path))
        .and_then(|types| types.first().copied())
        .map(crate::labels::document_field_type_tag);

    let trigger = div()
        .id(SharedString::from(format!("doc-builder-acc-field-{id}")))
        .debug_selector(move || format!("doc-builder-acc-field-{id}"))
        .relative()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .w_full()
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .cursor_pointer()
        .child(
            Chamfer::new(ChamferCut::CONTROL)
                .fill(theme.background)
                .border(theme.input),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .text_color(color)
                .child(SharedString::from(label)),
        )
        .children(tag.map(|tag| type_tag(tag, theme)))
        .on_click(cx.listener(move |this, _, window, cx| this.open_picker(target, window, cx)));

    panel
        .rail_mark
        .ring(&row_id::accumulator(id), "field", div())
        .relative()
        .flex_1()
        .min_w_0()
        .child(trigger)
        .children(render_picker_if_open(panel, target, theme, cx))
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Preview
// ---------------------------------------------------------------------------

/// The Preview card, fixed between the scrolling cards and the footer so
/// the query stays in view however far the cards are scrolled.
fn render_preview_pane(panel: &DocumentBuilderPanel, theme: &Theme, cx: &App) -> AnyElement {
    let mode_label = match panel.mode() {
        DocumentQueryMode::Find => "find",
        DocumentQueryMode::Aggregate => "aggregate",
    };

    div()
        .flex_shrink_0()
        .px(BuilderMetrics::RAIL_PADDING_X)
        .pb(BuilderMetrics::SECTION_GAP)
        .child(card(
            dbflux_i18n::t!("document.collection.builder.section.preview"),
            AppIcon::Code,
            Some(Badge::new(mode_label, BadgeTone::Neutral).into_any_element()),
            theme,
            render_preview(panel, theme, cx),
        ))
        .into_any_element()
}

fn render_preview(panel: &DocumentBuilderPanel, theme: &Theme, cx: &App) -> AnyElement {
    let keyword = SyntaxColors::for_current(cx).keyword;
    let highlights: Vec<(std::ops::Range<usize>, HighlightStyle)> = operator_ranges(&panel.preview)
        .into_iter()
        .map(|range| {
            (
                range,
                HighlightStyle {
                    color: Some(keyword),
                    font_weight: Some(FontWeight::SEMIBOLD),
                    ..HighlightStyle::default()
                },
            )
        })
        .collect();

    let problem = panel
        .problems
        .first()
        .map(|problem| crate::labels::document_builder_problem(&problem.kind));

    div()
        .id("doc-builder-preview")
        .debug_selector(|| "doc-builder-preview".to_string())
        .flex()
        .flex_col()
        .gap(Spacing::SM)
        .child(
            div()
                .id("doc-builder-preview-text")
                .h(BuilderMetrics::PREVIEW_HEIGHT)
                .overflow_y_scrollbar()
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .text_color(ChromeColors::strong(theme))
                .child(StyledText::new(panel.preview.clone()).with_highlights(highlights)),
        )
        .when_some(problem, |column, problem| {
            column.child(caption(
                dbflux_i18n::t!(
                    "document.collection.builder.preview.invalid",
                    error = problem
                ),
                theme.warning,
            ))
        })
        .when_some(panel.render_error.clone(), |column, error| {
            column.child(caption(
                dbflux_i18n::t!(
                    "document.collection.builder.preview.render_failed",
                    error = error
                ),
                theme.danger,
            ))
        })
        .into_any_element()
}

// ---------------------------------------------------------------------------
// Saved queries
// ---------------------------------------------------------------------------

/// The saved queries of the collection, under their header button: open
/// one by its name, delete it with the trailing button.
fn render_saved_menu_if_open(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> Option<AnyElement> {
    if !panel.saved_menu_open {
        return None;
    }

    let muted = theme.muted_foreground;

    let rows: Vec<AnyElement> = panel
        .saved_queries()
        .iter()
        .map(|entry| {
            let open_id = entry.id.clone();
            let remove_id = entry.id.clone();
            let loaded = panel.loaded_id() == Some(entry.id.as_str());
            let mode_label = match entry.mode {
                DocumentQueryMode::Find => "find",
                DocumentQueryMode::Aggregate => "aggregate",
            };

            let row_selector = format!("doc-builder-saved-{}", entry.id);

            let saved_row = row_id::saved(&entry.id);

            div()
                .id(SharedString::from(row_selector.clone()))
                .debug_selector(move || row_selector)
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .cursor_pointer()
                .hover(|row| row.bg(theme.accent.opacity(0.15)))
                .when(loaded, |row| row.bg(theme.accent.opacity(0.1)))
                .map(|row| panel.rail_mark.fixed_row(&saved_row, row))
                .on_click(cx.listener(move |this, _, _, cx| this.request_open_saved(&open_id, cx)))
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .truncate()
                        .text_size(FontSizes::XS)
                        .child(SharedString::from(entry.name.clone())),
                )
                .child(type_tag(mode_label.to_string(), theme))
                .child(
                    panel.rail_mark.ring_element(
                        &saved_row,
                        "delete",
                        Button::new(
                            SharedString::from(format!("doc-builder-saved-remove-{}", entry.id)),
                            dbflux_i18n::t!("document.collection.builder.saved.remove"),
                        )
                        .icon(AppIcon::Delete)
                        .icon_only()
                        .tab_stop(false)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            cx.stop_propagation();
                            this.request_delete_saved(&remove_id, cx);
                        })),
                    ),
                )
                .into_any_element()
        })
        .collect();

    let body = if rows.is_empty() {
        div()
            .id("doc-builder-saved-empty")
            .px(Spacing::SM)
            .py(Spacing::XS)
            .child(caption(
                dbflux_i18n::t!("document.collection.builder.saved.empty"),
                muted,
            ))
            .into_any_element()
    } else {
        div()
            .id("doc-builder-saved-list")
            .max_h(SAVED_LIST_HEIGHT)
            .flex()
            .flex_col()
            .overflow_y_scrollbar()
            .children(rows)
            .into_any_element()
    };

    let popover = menu_frame(cx)
        .id("doc-builder-saved-menu")
        .debug_selector(|| "doc-builder-saved-menu".to_string())
        .absolute()
        .top_full()
        .right_0()
        .mt(Spacing::XS)
        .w(SAVED_MENU_WIDTH)
        .occlude()
        .on_mouse_down_out(cx.listener(|this, _, _, cx| {
            if this.saved_menu_open {
                this.toggle_saved_menu(cx);
            }
        }))
        .child(body);

    Some(deferred(popover).with_priority(2).into_any_element())
}

// ---------------------------------------------------------------------------
// Field picker
// ---------------------------------------------------------------------------

/// Left edge of a picker opened from `anchor`, relative to it: under the
/// anchor, moved left as far as it takes to end inside the rail.
fn picker_offset(anchor: Bounds<Pixels>, rail: Bounds<Pixels>) -> Pixels {
    let leftmost = rail.left() + BuilderMetrics::RAIL_PADDING_X;
    let rightmost = rail.right() - BuilderMetrics::RAIL_PADDING_X - PICKER_WIDTH;

    let left = if anchor.left() > rightmost {
        rightmost
    } else {
        anchor.left()
    };
    let left = if left < leftmost { leftmost } else { left };

    left - anchor.left()
}

/// The field picker of `target` while it is open, with the element that
/// measures where it opens from. It shows once that element and the rail
/// have been painted, so it never opens past the rail's right edge.
fn render_picker_if_open(
    panel: &DocumentBuilderPanel,
    target: PickTarget,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> Vec<AnyElement> {
    let Some(picker) = panel
        .picker
        .as_ref()
        .filter(|picker| picker.target == target)
    else {
        return Vec::new();
    };

    let entity = cx.entity();
    let anchor_probe = canvas(
        move |bounds, _window, cx| {
            // A notify while the window draws is dropped, so the bounds are
            // recorded once this frame is done and the change redraws.
            cx.defer(move |cx| {
                entity.update(cx, |panel, cx| {
                    panel.record_picker_anchor(target, bounds, cx)
                });
            });
        },
        |_, _, _, _| {},
    )
    .absolute()
    .top_0()
    .left_0()
    .size_full()
    .into_any_element();

    let Some(offset) = picker
        .anchor
        .zip(panel.rail_bounds)
        .map(|(anchor, rail)| picker_offset(anchor, rail))
    else {
        return vec![anchor_probe];
    };

    let search = picker.search.clone();
    let query = panel.picker_query(cx);
    let catalog = panel.picker_catalog(target);
    let muted = theme.muted_foreground;

    let mut rows: Vec<AnyElement> = Vec::new();

    if query.trim().is_empty() && panel.allows_element_itself(target) {
        rows.push(
            picker_row(
                "doc-builder-picker-element".into(),
                0,
                dbflux_i18n::t!("document.collection.builder.filter.element"),
                Vec::new(),
                None,
                theme,
            )
            .on_click(cx.listener(|this, _, _, cx| this.pick("", cx)))
            .into_any_element(),
        );
    }

    let searching = !query.trim().is_empty();
    for field in catalog.search(&query) {
        let path = field.path.clone();
        let (depth, label) = if searching {
            (0, field.path.clone())
        } else {
            (field.depth, field.name.clone())
        };

        rows.push(
            picker_row(
                SharedString::from(format!("doc-builder-picker-field-{}", field.path)),
                depth,
                label,
                field.types.clone(),
                Some(field.presence_percent),
                theme,
            )
            .on_click(cx.listener(move |this, _, _, cx| this.pick(&path, cx)))
            .into_any_element(),
        );
    }

    let typed = query.trim().to_string();
    if panel.accepts_typed_path(target)
        && is_valid_typed_path(&typed)
        && catalog.field(&typed).is_none()
    {
        rows.push(
            picker_row(
                "doc-builder-picker-custom".into(),
                0,
                crate::labels::document_builder_use_path(&typed),
                Vec::new(),
                None,
                theme,
            )
            .child(type_tag(
                dbflux_i18n::t!("document.collection.builder.filter.unsampled"),
                theme,
            ))
            .on_click(cx.listener(move |this, _, _, cx| this.pick(&typed, cx)))
            .into_any_element(),
        );
    }

    let note = if panel.sampling {
        dbflux_i18n::t!("document.collection.builder.picker.loading")
    } else if let Some(sampled) = panel
        .sampled_documents
        .filter(|_| !panel.catalog.is_empty())
    {
        crate::labels::document_builder_sample_note(sampled)
    } else {
        dbflux_i18n::t!("document.collection.builder.picker.no_sample")
    };

    let popover = menu_frame(cx)
        .id("doc-builder-picker")
        .debug_selector(|| "doc-builder-picker".to_string())
        .absolute()
        .top_full()
        .left(offset)
        .mt(Spacing::XS)
        .w(PICKER_WIDTH)
        .occlude()
        .on_mouse_down_out(cx.listener(|this, _, _, cx| this.close_picker(cx)))
        .child(
            div().px(Spacing::SM).pb(Spacing::XS).child(
                Input::new(&search)
                    .id("doc-builder-picker-search")
                    .small()
                    .prefix(Icon::new(AppIcon::Search).small().color(muted))
                    .w_full(),
            ),
        )
        .child(
            div()
                .id("doc-builder-picker-list")
                .debug_selector(|| "doc-builder-picker-list".to_string())
                .max_h(PICKER_LIST_HEIGHT)
                .flex()
                .flex_col()
                .overflow_y_scrollbar()
                .children(rows),
        )
        .child(
            div()
                .px(Spacing::SM)
                .pt(Spacing::XS)
                .child(caption(note, muted)),
        );

    vec![
        anchor_probe,
        deferred(popover).with_priority(2).into_any_element(),
    ]
}

fn picker_row(
    id: SharedString,
    depth: usize,
    label: String,
    types: Vec<DocumentFieldType>,
    presence: Option<u32>,
    theme: &Theme,
) -> gpui::Stateful<gpui::Div> {
    let tags = (!types.is_empty())
        .then(|| type_tag(crate::labels::document_field_type_tags(&types), theme));

    let selector = id.to_string();

    div()
        .id(id)
        .debug_selector(move || selector)
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .px(Spacing::SM)
        .py(Spacing::XS)
        .pl(Spacing::SM + PICKER_INDENT * depth as f32)
        .cursor_pointer()
        .hover(|row| row.bg(theme.accent.opacity(0.15)))
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(FontSizes::XS)
                .child(SharedString::from(label)),
        )
        .children(tags)
        .when_some(presence, |row, presence| {
            row.child(
                div()
                    .flex_shrink_0()
                    .text_size(FontSizes::LABEL)
                    .text_color(theme.muted_foreground)
                    .child(SharedString::from(format!("{presence}%"))),
            )
        })
}

impl Render for DocumentBuilderPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        render_panel(self, window, cx)
    }
}
