//! Rendering of the builder rail (IslDocBuilder, IslDocBuilderStates): a
//! header, the scrolling cards (sync conflict, Filter, Project, Sort / limit
//! / skip, Preview) and the fixed footer with Open in editor and Find.

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
    DocumentCombinator, DocumentFieldType, DocumentProjectionMode, DocumentSortDirection,
};
use gpui::Focusable;
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Context, FontWeight, HighlightStyle, Hsla, IntoElement, KeyDownEvent, Pixels,
    SharedString, StyledText, Window, deferred, div, linear_color_stop, linear_gradient, px,
};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;
use gpui_component::theme::Theme;

use super::model::{ConditionDraft, GroupDraft, NodeDraft, NodeId, Operand, ProblemKind};
use super::panel::{DocumentBuilderPanel, PickTarget, is_valid_typed_path};
use super::values::{ScalarKind, ValueEditor, ValueProblem, format_value, operator_ranges};

/// Keycap on the Find button: the rail runs on Cmd/Ctrl+Enter.
#[cfg(target_os = "macos")]
const FIND_SHORTCUT_HINT: &str = "Cmd \u{21b5}";
#[cfg(not(target_os = "macos"))]
const FIND_SHORTCUT_HINT: &str = "Ctrl \u{21b5}";

/// Field select of a condition row.
const FIELD_WIDTH: Pixels = px(150.0);
/// Operator select of a condition row.
const OPERATOR_WIDTH: Pixels = px(96.0);
/// Limit and skip inputs.
const PAGING_WIDTH: Pixels = px(70.0);
/// Chip entry input.
const CHIP_ENTRY_WIDTH: Pixels = px(110.0);
/// Field picker popover.
const PICKER_WIDTH: Pixels = px(320.0);
const PICKER_LIST_HEIGHT: Pixels = px(280.0);
/// Indent per nesting level in the field picker.
const PICKER_INDENT: Pixels = px(14.0);
/// Colored bar at the left of a filter group.
const GROUP_BAR_WIDTH: Pixels = px(2.0);
/// Height of the fade over the bottom of the scrolling cards.
const FADE_HEIGHT: Pixels = px(28.0);

pub(super) fn render_panel(
    panel: &mut DocumentBuilderPanel,
    window: &mut Window,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    panel.ensure_inputs(window, cx);

    let theme = cx.theme().clone();
    let focus_handle = panel.focus_handle(cx);

    div()
        .id("doc-builder")
        .flex()
        .flex_col()
        .size_full()
        .bg(theme.popover)
        .track_focus(&focus_handle)
        .on_key_down(cx.listener(|this, event: &KeyDownEvent, _window, cx| {
            let keystroke = &event.keystroke;
            if keystroke.key == "enter" && keystroke.modifiers.secondary() {
                this.request_find(cx);
                cx.stop_propagation();
            }
        }))
        .child(render_header(panel, &theme, cx))
        .child(render_body(panel, &theme, cx))
        .child(render_footer(panel, &theme, cx))
}

// ---------------------------------------------------------------------------
// Header and footer
// ---------------------------------------------------------------------------

fn render_header(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    div()
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
        .child(Badge::new(
            dbflux_i18n::t!("document.collection.builder.mode.find"),
            BadgeTone::Accent,
        ))
        .child(div().flex_1())
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

fn render_footer(
    panel: &DocumentBuilderPanel,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> impl IntoElement {
    let can_find = panel.can_find();
    let find_tooltip = if !panel.sync.held().is_empty() {
        Some(dbflux_i18n::t!("document.collection.builder.find_blocked"))
    } else if !can_find {
        Some(dbflux_i18n::t!("document.collection.builder.find_invalid"))
    } else {
        None
    };

    let can_open = panel.problems.is_empty() && panel.render_error.is_none();

    div()
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
            Button::new(
                "doc-builder-find",
                dbflux_i18n::t!("document.collection.builder.find"),
            )
            .primary()
            .icon(AppIcon::Play)
            .kbd(FIND_SHORTCUT_HINT)
            .disabled(!can_find)
            .when_some(find_tooltip, Button::tooltip)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.request_find(cx))),
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
    let conflict = panel
        .is_conflicted()
        .then(|| render_conflict(panel, theme, cx).into_any_element());

    let count = panel.draft.condition_count();
    let filter_badge = (count > 0).then(|| {
        Badge::new(
            crate::labels::document_builder_condition_count(count),
            BadgeTone::Accent,
        )
        .into_any_element()
    });

    let root = panel.draft.filter.clone();
    let filter_body = render_group(panel, &root, true, theme, cx);
    let project_body = render_projection(panel, theme, cx);
    let sort_body = render_sort(panel, theme, cx);
    let preview_body = render_preview(panel, theme, cx);

    let fade = linear_gradient(
        180.,
        linear_color_stop(theme.popover.opacity(0.), 0.),
        linear_color_stop(theme.popover, 1.),
    );

    div()
        .relative()
        .flex_1()
        .min_h(px(0.))
        .child(
            div()
                .id("doc-builder-sections")
                .size_full()
                .flex()
                .flex_col()
                .gap(BuilderMetrics::SECTION_GAP)
                .px(BuilderMetrics::RAIL_PADDING_X)
                .py(BuilderMetrics::SECTION_GAP)
                .overflow_y_scrollbar()
                .children(conflict)
                .child(card(
                    dbflux_i18n::t!("document.collection.builder.section.filter"),
                    AppIcon::ListFilter,
                    filter_badge,
                    theme,
                    filter_body,
                ))
                .child(card(
                    dbflux_i18n::t!("document.collection.builder.section.project"),
                    AppIcon::Columns,
                    None,
                    theme,
                    project_body,
                ))
                .child(card(
                    dbflux_i18n::t!("document.collection.builder.section.sort"),
                    AppIcon::ArrowUpDown,
                    None,
                    theme,
                    sort_body,
                ))
                .child(card(
                    dbflux_i18n::t!("document.collection.builder.section.preview"),
                    AppIcon::Code,
                    Some(
                        Badge::new(
                            dbflux_i18n::t!("document.collection.builder.mode.find"),
                            BadgeTone::Neutral,
                        )
                        .into_any_element(),
                    ),
                    theme,
                    preview_body,
                )),
        )
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
            div()
                .flex()
                .gap(Spacing::SM)
                .child(
                    Button::new(
                        "doc-builder-keep-text",
                        dbflux_i18n::t!("document.collection.builder.conflict.keep"),
                    )
                    .secondary()
                    .disabled(!can_keep)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| this.keep_text(cx))),
                )
                .child(
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

    let head = div()
        .flex()
        .items_center()
        .gap(BuilderMetrics::ROW_GAP)
        .child(switch)
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
        .child(link_button(
            format!("doc-builder-add-condition-{group_id}"),
            dbflux_i18n::t!("document.collection.builder.filter.add_condition"),
            AppIcon::Plus,
            cx.listener(move |this, _, _, cx| this.add_condition(group_id, cx)),
        ))
        .child(link_button(
            format!("doc-builder-add-group-{group_id}"),
            dbflux_i18n::t!("document.collection.builder.filter.add_group"),
            AppIcon::Plus,
            cx.listener(move |this, _, _, cx| this.add_group(group_id, cx)),
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

    let operator = panel.operator_menus.get(&id).map(|menu| {
        div()
            .w(OPERATOR_WIDTH)
            .flex_shrink_0()
            .child(menu.dropdown.clone())
            .into_any_element()
    });

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

    let row = div()
        .flex()
        .items_center()
        .gap(BuilderMetrics::ROW_GAP)
        .child(field)
        .children(operator)
        .child(div().flex_1().min_w_0().child(value))
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
                        line.child(link_button(
                            format!("doc-builder-use-oid-{id}"),
                            dbflux_i18n::t!("document.collection.builder.filter.use_object_id"),
                            AppIcon::Hash,
                            cx.listener(move |this, _, _, cx| {
                                this.set_kind(id, ScalarKind::ObjectId, cx)
                            }),
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
    } else if let Some(first) = types.first() {
        Some(crate::labels::document_field_type_tag(*first))
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

    div()
        .relative()
        .w(FIELD_WIDTH)
        .flex_shrink_0()
        .child(trigger)
        .children(render_picker_if_open(panel, target, theme, cx))
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
                div()
                    .id(SharedString::from(format!("doc-builder-chip-{id}-{index}")))
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .px(Spacing::XXS)
                    .bg(theme.secondary)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::XS)
                    .child(SharedString::from(format_value(item, kind)))
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

            let field = match editor {
                ValueEditor::Scalar(ScalarKind::ObjectId) => field
                    .prefix(mono_affix("ObjectId(\"", muted))
                    .suffix(mono_affix("\")", muted)),
                ValueEditor::Scalar(ScalarKind::Date) => field
                    .prefix(Icon::new(AppIcon::Clock).small().color(muted))
                    .suffix(mono_affix("UTC", muted)),
                ValueEditor::Pattern => field.placeholder("/pattern/i"),
                _ => field.placeholder(dbflux_i18n::t!(
                    "document.collection.builder.filter.value_placeholder"
                )),
            };

            field.into_any_element()
        }
    }
}

fn mono_affix(text: &'static str, color: Hsla) -> AnyElement {
    div()
        .font_family(AppFonts::MONO)
        .text_size(FontSizes::XS)
        .text_color(color)
        .child(text)
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
            div()
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

    let add = div()
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
        .child(div().flex().child(switch))
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
            let types = panel.catalog.types(&key.path);
            let tag = types
                .first()
                .map(|first| crate::labels::document_field_type_tag(*first));
            let drag = SortDrag {
                index,
                label: SharedString::from(key.path.clone()),
            };

            div()
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
                )
        })
        .collect::<Vec<_>>();

    let add = div()
        .relative()
        .child(link_button(
            "doc-builder-sort-add",
            dbflux_i18n::t!("document.collection.builder.sort.add_key"),
            AppIcon::Plus,
            cx.listener(|this, _, window, cx| this.open_picker(PickTarget::Sort, window, cx)),
        ))
        .children(render_picker_if_open(panel, PickTarget::Sort, theme, cx));

    let paging_field = |id: &'static str, label: String, state| {
        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(caption(label.clone(), theme.muted_foreground))
            .child(
                div()
                    .w(PAGING_WIDTH)
                    .child(Input::new(state).id(id).small().aria_label(label).w_full()),
            )
    };

    let paging = div()
        .flex()
        .items_center()
        .gap(Spacing::MD)
        .child(add)
        .child(div().flex_1())
        .child(paging_field(
            "doc-builder-limit",
            dbflux_i18n::t!("document.collection.builder.sort.limit"),
            &panel.limit_input,
        ))
        .child(paging_field(
            "doc-builder-skip",
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
// Preview
// ---------------------------------------------------------------------------

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
        .flex()
        .flex_col()
        .gap(Spacing::SM)
        .child(
            div()
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
// Field picker
// ---------------------------------------------------------------------------

fn render_picker_if_open(
    panel: &DocumentBuilderPanel,
    target: PickTarget,
    theme: &Theme,
    cx: &mut Context<DocumentBuilderPanel>,
) -> Option<AnyElement> {
    let picker = panel
        .picker
        .as_ref()
        .filter(|picker| picker.target == target)?;
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
    if is_valid_typed_path(&typed) && catalog.field(&typed).is_none() {
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
        .absolute()
        .top_full()
        .left_0()
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

    Some(deferred(popover).with_priority(2).into_any_element())
}

fn picker_row(
    id: SharedString,
    depth: usize,
    label: String,
    types: Vec<DocumentFieldType>,
    presence: Option<u32>,
    theme: &Theme,
) -> gpui::Stateful<gpui::Div> {
    let tags: Vec<AnyElement> = types
        .iter()
        .map(|field_type| type_tag(crate::labels::document_field_type_tag(*field_type), theme))
        .collect();

    div()
        .id(id)
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
