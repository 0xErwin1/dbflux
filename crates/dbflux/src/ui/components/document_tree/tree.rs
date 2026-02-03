use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};

use super::events::{DocumentTreeEvent, TreeDirection};
use super::node::{NodeId, NodeValue, TreeNode};
use super::state::DocumentTreeState;

/// Height of each row in the tree.
pub const TREE_ROW_HEIGHT: Pixels = px(26.0);

/// Indentation per depth level.
const INDENT_WIDTH: Pixels = px(16.0);

actions!(
    document_tree,
    [
        MoveUp,
        MoveDown,
        MoveLeft,
        MoveRight,
        MoveToTop,
        MoveToBottom,
        PageUp,
        PageDown,
        ToggleExpand,
        StartEdit,
        OpenPreview,
        DeleteDocument,
    ]
);

/// Context string for keybindings.
const CONTEXT: &str = "DocumentTree";

/// Initialize keybindings for DocumentTree.
pub fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("up", MoveUp, Some(CONTEXT)),
        KeyBinding::new("k", MoveUp, Some(CONTEXT)),
        KeyBinding::new("down", MoveDown, Some(CONTEXT)),
        KeyBinding::new("j", MoveDown, Some(CONTEXT)),
        KeyBinding::new("left", MoveLeft, Some(CONTEXT)),
        KeyBinding::new("h", MoveLeft, Some(CONTEXT)),
        KeyBinding::new("right", MoveRight, Some(CONTEXT)),
        KeyBinding::new("l", MoveRight, Some(CONTEXT)),
        KeyBinding::new("home", MoveToTop, Some(CONTEXT)),
        KeyBinding::new("g", MoveToTop, Some(CONTEXT)),
        KeyBinding::new("end", MoveToBottom, Some(CONTEXT)),
        KeyBinding::new("shift-g", MoveToBottom, Some(CONTEXT)),
        KeyBinding::new("pageup", PageUp, Some(CONTEXT)),
        KeyBinding::new("ctrl-u", PageUp, Some(CONTEXT)),
        KeyBinding::new("pagedown", PageDown, Some(CONTEXT)),
        KeyBinding::new("ctrl-d", PageDown, Some(CONTEXT)),
        KeyBinding::new("space", ToggleExpand, Some(CONTEXT)),
        KeyBinding::new("enter", StartEdit, Some(CONTEXT)),
        KeyBinding::new("f2", StartEdit, Some(CONTEXT)),
        KeyBinding::new("e", OpenPreview, Some(CONTEXT)),
        KeyBinding::new("d d", DeleteDocument, Some(CONTEXT)),
        KeyBinding::new("delete", DeleteDocument, Some(CONTEXT)),
    ]);
}

/// Document tree component for displaying MongoDB documents.
pub struct DocumentTree {
    id: ElementId,
    state: Entity<DocumentTreeState>,
}

impl DocumentTree {
    pub fn new(
        id: impl Into<ElementId>,
        state: Entity<DocumentTreeState>,
        _cx: &mut Context<Self>,
    ) -> Self {
        Self {
            id: id.into(),
            state,
        }
    }
}

impl Render for DocumentTree {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.state.clone();

        // Read state to get visible nodes count (needs mutable access first)
        let node_count = self.state.update(cx, |s, _| s.visible_node_count());
        let focus_handle = self.state.read(cx).focus_handle(cx);
        let scroll_handle = self.state.read(cx).scroll_handle().clone();
        let theme = cx.theme();

        div()
            .id(self.id.clone())
            .key_context(CONTEXT)
            .track_focus(&focus_handle)
            .size_full()
            .bg(theme.background)
            .overflow_hidden()
            .on_action({
                let state = self.state.clone();
                move |_: &MoveUp, _window, cx| {
                    state.update(cx, |s, cx| s.move_cursor(TreeDirection::Up, cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &MoveDown, _window, cx| {
                    state.update(cx, |s, cx| s.move_cursor(TreeDirection::Down, cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &MoveLeft, _window, cx| {
                    state.update(cx, |s, cx| s.handle_left(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &MoveRight, _window, cx| {
                    state.update(cx, |s, cx| s.handle_right(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &MoveToTop, _window, cx| {
                    state.update(cx, |s, cx| s.move_to_first(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &MoveToBottom, _window, cx| {
                    state.update(cx, |s, cx| s.move_to_last(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &PageUp, _window, cx| {
                    state.update(cx, |s, cx| s.page_up(20, cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &PageDown, _window, cx| {
                    state.update(cx, |s, cx| s.page_down(20, cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &ToggleExpand, _window, cx| {
                    let cursor = state.read(cx).cursor().cloned();
                    if let Some(id) = cursor {
                        state.update(cx, |s, cx| s.toggle_expand(&id, cx));
                    }
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &StartEdit, _window, cx| {
                    state.update(cx, |s, cx| s.request_edit(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &OpenPreview, _window, cx| {
                    state.update(cx, |s, cx| s.request_document_preview(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &DeleteDocument, _window, cx| {
                    state.update(cx, |s, cx| s.request_delete(cx));
                }
            })
            .on_click(cx.listener(|this, _, window, cx| {
                this.state.update(cx, |s, _| s.focus(window));
                cx.emit(DocumentTreeEvent::Focused);
            }))
            .child(
                uniform_list("document-tree-list", node_count, {
                    let state = state.clone();
                    move |range, _window, cx| {
                        let visible_nodes: Vec<TreeNode> =
                            state.update(cx, |s, _| s.visible_nodes().to_vec());

                        let cursor = state.read(cx).cursor().cloned();
                        let theme = cx.theme().clone();

                        let expanded_set: std::collections::HashSet<NodeId> = visible_nodes
                            .iter()
                            .filter(|n| state.read(cx).is_expanded(&n.id))
                            .map(|n| n.id.clone())
                            .collect();

                        range
                            .filter_map(|ix| visible_nodes.get(ix).cloned())
                            .map(|node| {
                                let is_cursor = cursor.as_ref() == Some(&node.id);
                                let is_expanded = expanded_set.contains(&node.id);
                                let state_clone = state.clone();
                                let node_id = node.id.clone();

                                render_tree_row(
                                    node,
                                    is_cursor,
                                    is_expanded,
                                    theme.clone(),
                                    state_clone,
                                    node_id,
                                )
                            })
                            .collect()
                    }
                })
                .track_scroll(scroll_handle)
                .size_full()
                .with_sizing_behavior(ListSizingBehavior::Infer),
            )
    }
}

impl EventEmitter<DocumentTreeEvent> for DocumentTree {}

fn render_tree_row(
    node: TreeNode,
    is_cursor: bool,
    is_expanded: bool,
    theme: gpui_component::Theme,
    state: Entity<DocumentTreeState>,
    node_id: NodeId,
) -> Stateful<Div> {
    let indent = INDENT_WIDTH * node.depth as f32;
    let is_expandable = node.is_expandable();

    let chevron_state = state.clone();
    let chevron_node_id = node_id.clone();

    let row_state = state.clone();
    let row_node_id = node_id.clone();

    let selection_color = theme.selection;
    let secondary_color = theme.secondary;
    let primary_color = theme.primary;
    let muted_color = theme.muted_foreground;

    div()
        .id(ElementId::Name(
            format!("tree-row-{:?}", node.id.path).into(),
        ))
        .h(TREE_ROW_HEIGHT)
        .w_full()
        .flex()
        .items_center()
        .pl(indent)
        .pr(Spacing::SM)
        .when(is_cursor, |d| d.bg(selection_color))
        .hover(move |d| {
            d.bg(if is_cursor {
                selection_color
            } else {
                secondary_color.opacity(0.5)
            })
        })
        .cursor_pointer()
        .on_click({
            move |event, window, cx| {
                let click_count = event.click_count();
                row_state.update(cx, |s, cx| {
                    s.focus(window);

                    if click_count == 1 {
                        // Single click: set cursor to this node
                        s.set_cursor(&row_node_id, cx);
                    } else if click_count == 2 {
                        // Double click: execute action (expand, edit, or preview)
                        s.execute_node(&row_node_id, cx);
                    }
                });
            }
        })
        // Expand/collapse chevron
        .child(render_chevron(
            is_expandable,
            is_expanded,
            muted_color,
            chevron_state,
            chevron_node_id,
        ))
        // Key
        .child(
            div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(primary_color)
                        .font_weight(FontWeight::MEDIUM)
                        .child(node.key.to_string()),
                )
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(muted_color)
                        .child(":"),
                ),
        )
        // Value preview
        .child(
            div()
                .flex_1()
                .overflow_x_hidden()
                .ml(Spacing::XS)
                .child(render_value_preview(&node.value, &theme)),
        )
        // Type badge
        .child(
            div()
                .text_size(FontSizes::XS)
                .text_color(muted_color.opacity(0.7))
                .px(Spacing::XS)
                .rounded(Radii::SM)
                .bg(secondary_color.opacity(0.3))
                .child(node.value.type_label()),
        )
}

fn render_chevron(
    is_expandable: bool,
    is_expanded: bool,
    muted_color: Hsla,
    state: Entity<DocumentTreeState>,
    node_id: NodeId,
) -> Div {
    let chevron = div()
        .w(px(16.0))
        .h(px(16.0))
        .flex()
        .items_center()
        .justify_center();

    if is_expandable {
        let icon = if is_expanded {
            AppIcon::ChevronDown
        } else {
            AppIcon::ChevronRight
        };

        chevron
            .child(svg().path(icon.path()).size_3().text_color(muted_color))
            .cursor_pointer()
            .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                cx.stop_propagation();
                state.update(cx, |s, cx| s.toggle_expand(&node_id, cx));
            })
    } else {
        chevron
    }
}

fn render_value_preview(value: &NodeValue, theme: &gpui_component::Theme) -> impl IntoElement {
    let (text, color) = match value {
        NodeValue::Scalar(v) => {
            let color = match v {
                dbflux_core::Value::Null => theme.muted_foreground,
                dbflux_core::Value::Bool(_) => theme.warning,
                dbflux_core::Value::Int(_) | dbflux_core::Value::Float(_) => theme.success,
                dbflux_core::Value::Text(_) => theme.foreground,
                dbflux_core::Value::ObjectId(_) => theme.primary,
                _ => theme.foreground,
            };
            (value.preview().to_string(), color)
        }
        NodeValue::Document(_) | NodeValue::Array(_) => {
            (value.preview().to_string(), theme.muted_foreground)
        }
    };

    div()
        .text_size(FontSizes::SM)
        .text_color(color)
        .overflow_x_hidden()
        .text_ellipsis()
        .whitespace_nowrap()
        .child(text)
}
