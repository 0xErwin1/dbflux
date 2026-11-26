//! The Diagram view: the plan as boxes on the shared diagram canvas.
//!
//! Each operation is a node card, read left to right from the first read to
//! the final result. An edge runs from a child to the operation that consumes
//! its rows, labelled with the child's row estimate and drawn thicker the more
//! rows it carries. The canvas, pan, zoom and fit are the schema diagram's
//! (`dbflux_components::diagram`); this module only says what a plan node
//! looks like.

pub(super) mod geometry;

pub(super) use geometry::{DiagramGeometry, fit_view};

use dbflux_components::diagram::{
    self as canvas, CanvasEdge, NodeCard, diagram_surface, dot_grid, edge_overlay, node_body,
    node_header,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, Icon};
use dbflux_components::tokens::{ChromeColors, Radii, SchemaMetrics, Spacing, SyntaxColors};
use gpui::prelude::*;
use gpui::{
    AnyElement, App, Div, ElementId, FontWeight, Hsla, MouseButton, MouseDownEvent, MouseMoveEvent,
    MouseUpEvent, Point, Role, ScrollWheelEvent, SharedString, Window, div, px, relative,
};
use gpui_component::ActiveTheme;
use gpui_component::theme::Theme;

use super::PlanView;
use super::ids::{tag, tagged};
use super::model::{NodeId, PlanNodeView, format_cost, format_rows, format_share};
use super::tooltip::tip;
use geometry::{CardRows, NODE_WIDTH, ROW_PX};

impl PlanView {
    pub(super) fn render_diagram(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let pan = canvas::pixel_aligned_pan(
            self.camera.pan,
            self.camera.viewport_origin,
            self.camera.zoom,
            window.scale_factor(),
        );

        let mut children: Vec<AnyElement> = vec![dot_grid(theme.border)];
        if let Some(edges) = self.edge_overlay(&theme) {
            children.push(edges.into_any_element());
        }
        children.extend(self.edge_labels(cx));
        children.extend(self.node_cards(&theme, cx));

        let entity = cx.entity();
        let measure = canvas::measure_bounds(move |bounds, cx| {
            entity.update(cx, |this, cx| {
                if this.camera.viewport_origin != bounds.origin {
                    this.camera.viewport_origin = bounds.origin;
                    cx.notify();
                }
                this.camera.viewport_size = bounds.size;
                this.apply_pending_fit(cx);
            });
        });

        tagged("plan-diagram")
            .size_full()
            .relative()
            .overflow_hidden()
            .child(measure)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, _| {
                    if event.click_count == 1 {
                        this.camera.pan_anchor = Some(event.position);
                    }
                }),
            )
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                let Some(anchor) = this.camera.pan_anchor else {
                    return;
                };

                // A release outside the viewport never reaches it, so the pan
                // ends here when the button is no longer down.
                if event.pressed_button != Some(MouseButton::Left) {
                    this.camera.pan_anchor = None;
                    return;
                }

                let moved = event.position - anchor;
                if moved.x.abs() > px(0.5) || moved.y.abs() > px(0.5) {
                    this.camera.pan =
                        Point::new(this.camera.pan.x + moved.x, this.camera.pan.y + moved.y);
                    this.camera.pan_anchor = Some(event.position);
                    cx.notify();
                }
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _: &MouseUpEvent, _, _| {
                    this.camera.pan_anchor = None;
                }),
            )
            .on_scroll_wheel(cx.listener(|this, event: &ScrollWheelEvent, window, cx| {
                let mouse =
                    canvas::viewport_local_position(event.position, this.camera.viewport_origin);
                let delta = event.delta.pixel_delta(px(1.0)).y;
                let pan = canvas::pixel_aligned_pan(
                    this.camera.pan,
                    this.camera.viewport_origin,
                    this.camera.zoom,
                    window.scale_factor(),
                );

                if let Some((zoom, pan)) = canvas::wheel_zoom(mouse, pan, this.camera.zoom, delta) {
                    this.camera.zoom = zoom;
                    this.camera.pan = pan;
                    cx.notify();
                }
            }))
            .child(diagram_surface(
                theme.background,
                pan,
                self.camera.zoom,
                children,
            ))
            .into_any_element()
    }

    /// Every edge on one canvas element. The edges that touch the selected
    /// node take the tint, the rest stay quiet.
    fn edge_overlay(&self, theme: &Theme) -> Option<Div> {
        let geometry = self.geometry.as_ref()?;
        let quiet = theme.muted_foreground.opacity(0.55);
        let tint = ChromeColors::tint(theme);

        let edges = geometry
            .edges
            .iter()
            .map(|edge| {
                let touches_selection =
                    self.selected == Some(edge.from) || self.selected == Some(edge.to);
                let color = if touches_selection { tint } else { quiet };

                CanvasEdge::new(edge.route.clone(), color).width(edge.width)
            })
            .collect();

        edge_overlay(edges)
    }

    /// The row estimate on each edge, where the edge runs between two
    /// columns.
    fn edge_labels(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let Some(geometry) = self.geometry.as_ref() else {
            return Vec::new();
        };
        let theme = cx.theme();
        let (background, border, strong) =
            (theme.background, theme.border, ChromeColors::strong(theme));
        let font = dbflux_components::fonts::editor_family(cx);

        geometry
            .edges
            .iter()
            .filter_map(|edge| {
                let rows = edge.rows?;
                let (x, y) = edge.label_at;

                Some(
                    div()
                        .absolute()
                        .left(px(x - EDGE_LABEL_SLOT / 2.0))
                        .top(px(y - ROW_PX / 2.0))
                        .w(px(EDGE_LABEL_SLOT))
                        .h(px(ROW_PX))
                        .flex()
                        .items_center()
                        .justify_center()
                        .child(
                            tagged(ElementId::Name(format!("plan-edge-{}", edge.from).into()))
                                .px(Spacing::XS)
                                .rounded(Radii::SM)
                                .border_1()
                                .border_color(border)
                                .bg(background)
                                .font_family(font.clone())
                                .text_size(SchemaMetrics::INDEX_LABEL_FONT)
                                .text_color(strong)
                                .tooltip(tip(
                                    dbflux_i18n::t!("document.query_plan.tooltip.rows_title"),
                                    Some(
                                        dbflux_i18n::t!("document.query_plan.tooltip.rows_body")
                                            .into(),
                                    ),
                                ))
                                .child(format_rows(rows)),
                        )
                        .into_any_element(),
                )
            })
            .collect()
    }

    fn node_cards(&self, theme: &Theme, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let (Some(model), Some(geometry)) = (self.model(), self.geometry.as_ref()) else {
            return Vec::new();
        };

        model
            .nodes
            .iter()
            .filter_map(|node| {
                let bounds = geometry.nodes.get(node.id)?;
                let selected = self.selected == Some(node.id);
                Some(
                    self.node_card(node, bounds.x, bounds.y, selected, theme, cx)
                        .into_any_element(),
                )
            })
            .collect()
    }

    fn node_card(
        &self,
        node: &PlanNodeView,
        x: f32,
        y: f32,
        selected: bool,
        theme: &Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let id: NodeId = node.id;
        let rows = CardRows::of(node);

        let mut header = tag(
            node_header(
                node_icon(node),
                node.operation.clone(),
                selected,
                geometry::HEADER_PX,
                theme,
                cx,
            ),
            ElementId::Name(format!("plan-node-{id}-operation").into()),
        );
        if let Some(description) = node.description.clone() {
            header = header.tooltip(tip(node.operation.clone(), Some(description.into())));
        }
        if node.is_costliest {
            header = header.child(costliest_badge(id));
        }

        let body_rows = card_rows(node, rows, theme, cx);

        let card = NodeCard::new(x, y, NODE_WIDTH)
            .selected(selected)
            .header(header)
            .body(node_body(body_rows))
            .build(theme);

        tag(card, ElementId::Name(format!("plan-node-{id}").into()))
            .role(Role::Button)
            .aria_label(SharedString::from(node.operation.clone()))
            .cursor_pointer()
            .on_click(cx.listener(move |this, _, window, cx| {
                window.focus(&this.focus_handle, cx);
                this.select(id, cx);
            }))
    }
}

/// Width of the slot an edge label is centered in, in graph pixels.
const EDGE_LABEL_SLOT: f32 = 72.0;

fn costliest_badge(id: NodeId) -> AnyElement {
    tagged(ElementId::Name(format!("plan-node-{id}-costliest").into()))
        .flex_shrink_0()
        .tooltip(tip(
            dbflux_i18n::t!("document.query_plan.tooltip.costliest_title"),
            Some(dbflux_i18n::t!("document.query_plan.tooltip.costliest_body").into()),
        ))
        .child(Badge::new(
            dbflux_i18n::t!("document.query_plan.node.costliest"),
            BadgeTone::Warning,
        ))
        .into_any_element()
}

/// The icon a node's header carries, chosen from what the node does with
/// data (reads through an index, reads a relation, combines inputs), never
/// from the engine's operation name.
pub(super) fn node_icon(node: &PlanNodeView) -> AppIcon {
    if node.index.is_some() {
        AppIcon::Search
    } else if node.relation.is_some() {
        AppIcon::Table
    } else if node.children.len() > 1 {
        AppIcon::ArrowLeftRight
    } else {
        AppIcon::Layers
    }
}

/// The body rows of a card, the ones the node has data for.
fn card_rows(node: &PlanNodeView, rows: CardRows, theme: &Theme, cx: &App) -> Vec<AnyElement> {
    let strong = ChromeColors::strong(theme);
    let muted = theme.muted_foreground;
    let emphasis = if node.is_costliest {
        theme.warning
    } else {
        strong
    };
    let font = dbflux_components::fonts::editor_family(cx);
    let index_color = SyntaxColors::for_current(cx).number;

    let row = |children: Vec<AnyElement>| -> AnyElement {
        div()
            .flex()
            .items_center()
            .h(px(ROW_PX))
            .gap(SchemaMetrics::ROW_GAP)
            .overflow_hidden()
            .font_family(font.clone())
            .text_size(SchemaMetrics::ROW_FONT)
            .children(children)
            .into_any_element()
    };
    let name = |text: String| -> AnyElement {
        div()
            .flex_1()
            .min_w_0()
            .truncate()
            .text_color(strong)
            .child(text)
            .into_any_element()
    };
    let kind = |text: String| -> AnyElement {
        div()
            .flex_shrink_0()
            .text_color(muted)
            .child(text)
            .into_any_element()
    };
    let icon = |icon: AppIcon, color: Hsla| -> AnyElement {
        Icon::new(icon)
            .size(SchemaMetrics::ROW_ICON)
            .color(color)
            .into_any_element()
    };

    let mut body = Vec::new();

    if let Some(relation) = node.relation.clone().filter(|_| rows.relation) {
        body.push(row(vec![
            icon(AppIcon::Table, muted),
            name(relation),
            kind(dbflux_i18n::t!("document.query_plan.node.table")),
        ]));
    }
    if let Some(index) = node.index.clone().filter(|_| rows.index) {
        body.push(row(vec![
            icon(AppIcon::Hash, index_color),
            name(index),
            kind(dbflux_i18n::t!("document.query_plan.node.index")),
        ]));
    }
    if let Some(condition) = node.key_condition.clone().filter(|_| rows.condition) {
        body.push(row(vec![
            div()
                .flex_shrink_0()
                .text_color(muted)
                .child(condition.label)
                .into_any_element(),
            name(condition.value),
        ]));
    }
    if rows.cost {
        body.push(cost_row(node, emphasis, strong, muted, &row));
    }
    if let Some(share) = node.self_share.filter(|_| rows.share) {
        body.push(share_row(node.id, share, node.is_costliest, theme, &row));
    }

    body
}

fn cost_row(
    node: &PlanNodeView,
    emphasis: Hsla,
    strong: Hsla,
    muted: Hsla,
    row: &dyn Fn(Vec<AnyElement>) -> AnyElement,
) -> AnyElement {
    let figure = |value: String, label: String, color: Hsla| -> Div {
        div()
            .flex()
            .gap(Spacing::XS)
            .flex_shrink_0()
            .child(div().text_color(color).child(value))
            .child(div().text_color(muted).child(label))
    };

    let mut cells: Vec<AnyElement> = Vec::new();
    if let Some(cost) = node.total_cost {
        cells.push(
            tag(
                figure(
                    format_cost(cost),
                    dbflux_i18n::t!("document.query_plan.node.cost"),
                    emphasis,
                ),
                ElementId::Name(format!("plan-node-{}-cost", node.id).into()),
            )
            .tooltip(tip(
                dbflux_i18n::t!("document.query_plan.tooltip.cost_title"),
                Some(dbflux_i18n::t!("document.query_plan.tooltip.cost_body").into()),
            ))
            .into_any_element(),
        );
    }
    cells.push(div().flex_1().into_any_element());
    if let Some(rows) = node.rows {
        cells.push(
            figure(
                format_rows(rows),
                dbflux_i18n::t!("document.query_plan.node.rows"),
                strong,
            )
            .into_any_element(),
        );
    }

    row(cells)
}

fn share_row(
    id: NodeId,
    share: f64,
    costliest: bool,
    theme: &Theme,
    row: &dyn Fn(Vec<AnyElement>) -> AnyElement,
) -> AnyElement {
    let color = if costliest {
        theme.warning
    } else {
        ChromeColors::tint(theme)
    };
    let bar = div()
        .flex_1()
        .h(Spacing::XS)
        .rounded(Radii::SM)
        .bg(theme.border)
        .child(
            div()
                .h_full()
                .w(relative(share as f32))
                .min_w(px(1.0))
                .rounded(Radii::SM)
                .bg(color),
        );

    tagged(ElementId::Name(format!("plan-node-{id}-share").into()))
        .tooltip(tip(
            dbflux_i18n::t!("document.query_plan.tooltip.self_cost_title"),
            Some(dbflux_i18n::t!("document.query_plan.tooltip.self_cost_node_body").into()),
        ))
        .child(row(vec![
            div()
                .flex_shrink_0()
                .text_color(color)
                .font_weight(FontWeight::MEDIUM)
                .child(format_share(share))
                .into_any_element(),
            bar.into_any_element(),
            div()
                .flex_shrink_0()
                .text_color(theme.muted_foreground)
                .child(dbflux_i18n::t!("document.query_plan.node.self"))
                .into_any_element(),
        ]))
        .into_any_element()
}
