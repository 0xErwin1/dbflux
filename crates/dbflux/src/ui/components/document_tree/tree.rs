use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::input::{Input, InputEvent, InputState};

use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};

use super::events::{DocumentTreeEvent, TreeDirection};
use super::node::{NodeId, NodeValue, TreeNode};
use super::state::{DocumentTreeState, DocumentViewMode};

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
        ToggleViewMode,
        OpenSearch,
        NextMatch,
        PrevMatch,
        CloseSearch,
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
        KeyBinding::new("r", ToggleViewMode, Some(CONTEXT)),
        KeyBinding::new("ctrl-f", OpenSearch, Some(CONTEXT)),
        KeyBinding::new("/", OpenSearch, Some(CONTEXT)),
        KeyBinding::new("n", NextMatch, Some(CONTEXT)),
        KeyBinding::new("shift-n", PrevMatch, Some(CONTEXT)),
        KeyBinding::new("escape", CloseSearch, Some(CONTEXT)),
    ]);
}

/// Document tree component for displaying MongoDB documents.
pub struct DocumentTree {
    id: ElementId,
    state: Entity<DocumentTreeState>,
    raw_json_input: Option<Entity<InputState>>,
    search_input: Option<Entity<InputState>>,
    _search_subscription: Option<Subscription>,
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
            raw_json_input: None,
            search_input: None,
            _search_subscription: None,
        }
    }

    fn ensure_raw_json_input(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        if let Some(input) = &self.raw_json_input {
            return input.clone();
        }

        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("json")
                .line_number(true)
                .soft_wrap(true)
        });

        self.raw_json_input = Some(input.clone());
        input
    }

    fn ensure_search_input(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        if let Some(input) = &self.search_input {
            return input.clone();
        }

        let input = cx.new(|cx| InputState::new(window, cx).placeholder("Search..."));

        let state = self.state.clone();
        let subscription = cx.subscribe(&input, move |_this, input, event, cx| {
            if let InputEvent::Change = event {
                let value = input.read(cx).value().to_string();
                state.update(cx, |s, cx| s.set_search(&value, cx));
            }
        });

        self.search_input = Some(input.clone());
        self._search_subscription = Some(subscription);
        input
    }
}

impl Render for DocumentTree {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.state.clone();
        let state_ref = self.state.read(cx);
        let view_mode = state_ref.view_mode();
        let is_tree_mode = view_mode == DocumentViewMode::Tree;
        let is_search_visible = state_ref.is_search_visible();
        let search_match_count = state_ref.search_match_count();
        let current_match_index = state_ref.current_match_index();

        // Lazily initialize and update raw JSON input when in Raw mode
        let raw_json_input = if !is_tree_mode {
            let input = self.ensure_raw_json_input(window, cx);
            let raw_json = self.state.update(cx, |s, _| s.raw_json().to_string());
            let current_value = input.read(cx).value().to_string();
            if current_value != raw_json {
                input.update(cx, |input_state, cx| {
                    input_state.set_value(&raw_json, window, cx);
                });
            }
            Some(input)
        } else {
            self.raw_json_input.clone()
        };

        // Lazily initialize search input when search is visible
        let search_input = if is_search_visible {
            let input = self.ensure_search_input(window, cx);
            // Focus the search input when search opens
            input.update(cx, |input_state, cx| {
                input_state.focus(window, cx);
            });
            Some(input)
        } else {
            self.search_input.clone()
        };

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
            .flex()
            .flex_col()
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
            .on_action({
                let state = self.state.clone();
                move |_: &ToggleViewMode, _window, cx| {
                    state.update(cx, |s, cx| s.toggle_view_mode(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &OpenSearch, _window, cx| {
                    state.update(cx, |s, cx| s.open_search(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &NextMatch, _window, cx| {
                    state.update(cx, |s, cx| s.next_match(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &PrevMatch, _window, cx| {
                    state.update(cx, |s, cx| s.prev_match(cx));
                }
            })
            .on_action({
                let state = self.state.clone();
                move |_: &CloseSearch, _window, cx| {
                    state.update(cx, |s, cx| s.close_search(cx));
                }
            })
            .on_click(cx.listener(|this, _, window, cx| {
                this.state.update(cx, |s, _| s.focus(window));
                cx.emit(DocumentTreeEvent::Focused);
            }))
            // Toolbar
            .child(render_toolbar(view_mode, theme.clone(), state.clone()))
            // Search bar
            .when_some(search_input.filter(|_| is_search_visible), |d, input| {
                d.child(render_search_bar(
                    input,
                    search_match_count,
                    current_match_index,
                    theme.clone(),
                    state.clone(),
                ))
            })
            // Content: Tree view or Raw JSON
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .when(is_tree_mode, |d| {
                        d.child(
                            uniform_list("document-tree-list", node_count, {
                                let state = state.clone();
                                move |range, _window, cx| {
                                    let visible_nodes: Vec<TreeNode> =
                                        state.update(cx, |s, _| s.visible_nodes().to_vec());

                                    let cursor = state.read(cx).cursor().cloned();
                                    let theme = cx.theme().clone();

                                    let state_ref = state.read(cx);

                                    let expanded_set: std::collections::HashSet<NodeId> =
                                        visible_nodes
                                            .iter()
                                            .filter(|n| state_ref.is_expanded(&n.id))
                                            .map(|n| n.id.clone())
                                            .collect();

                                    let expanded_values_set: std::collections::HashSet<NodeId> =
                                        visible_nodes
                                            .iter()
                                            .filter(|n| state_ref.is_value_expanded(&n.id))
                                            .map(|n| n.id.clone())
                                            .collect();

                                    let search_matches_set: std::collections::HashSet<NodeId> =
                                        visible_nodes
                                            .iter()
                                            .filter(|n| state_ref.is_search_match(&n.id))
                                            .map(|n| n.id.clone())
                                            .collect();

                                    let current_match: Option<NodeId> = visible_nodes
                                        .iter()
                                        .find(|n| state_ref.is_current_match(&n.id))
                                        .map(|n| n.id.clone());

                                    range
                                        .filter_map(|ix| visible_nodes.get(ix).cloned())
                                        .map(|node| {
                                            let is_cursor = cursor.as_ref() == Some(&node.id);
                                            let is_expanded = expanded_set.contains(&node.id);
                                            let is_value_expanded =
                                                expanded_values_set.contains(&node.id);
                                            let is_search_match =
                                                search_matches_set.contains(&node.id);
                                            let is_current_match =
                                                current_match.as_ref() == Some(&node.id);
                                            let state_clone = state.clone();
                                            let node_id = node.id.clone();

                                            render_tree_row(
                                                node,
                                                is_cursor,
                                                is_expanded,
                                                is_value_expanded,
                                                is_search_match,
                                                is_current_match,
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
                    })
                    .when_some(raw_json_input.filter(|_| !is_tree_mode), |d, input| {
                        d.child(
                            div()
                                .size_full()
                                .p(Spacing::SM)
                                .child(Input::new(&input).w_full().h_full()),
                        )
                    }),
            )
    }
}

impl EventEmitter<DocumentTreeEvent> for DocumentTree {}

fn render_toolbar(
    view_mode: DocumentViewMode,
    theme: gpui_component::Theme,
    state: Entity<DocumentTreeState>,
) -> Div {
    let is_tree_mode = view_mode == DocumentViewMode::Tree;

    div()
        .flex()
        .items_center()
        .justify_between()
        .px(Spacing::SM)
        .py(Spacing::XS)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary.opacity(0.3))
        .child(
            div()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(if is_tree_mode {
                    "Tree View"
                } else {
                    "Raw JSON"
                }),
        )
        .child(
            div()
                .id("view-mode-toggle")
                .flex()
                .items_center()
                .justify_center()
                .size(Heights::ICON_SM)
                .rounded(Radii::SM)
                .cursor_pointer()
                .bg(if is_tree_mode {
                    theme.transparent
                } else {
                    theme.selection
                })
                .hover(|d| d.bg(theme.secondary))
                .on_click({
                    move |_, _, cx| {
                        state.update(cx, |s, cx| s.toggle_view_mode(cx));
                    }
                })
                .child(
                    svg()
                        .path(if is_tree_mode {
                            AppIcon::Braces.path()
                        } else {
                            AppIcon::Rows3.path()
                        })
                        .size_4()
                        .text_color(theme.muted_foreground),
                ),
        )
}

fn render_search_bar(
    input: Entity<InputState>,
    match_count: usize,
    current_match: Option<usize>,
    theme: gpui_component::Theme,
    state: Entity<DocumentTreeState>,
) -> Div {
    let match_text = if match_count > 0 {
        format!(
            "{}/{}",
            current_match.map(|i| i + 1).unwrap_or(0),
            match_count
        )
    } else {
        "No matches".to_string()
    };

    div()
        .flex()
        .items_center()
        .gap(Spacing::SM)
        .px(Spacing::SM)
        .py(Spacing::XS)
        .border_b_1()
        .border_color(theme.border)
        .bg(theme.secondary.opacity(0.3))
        .child(
            svg()
                .path(AppIcon::Search.path())
                .size_4()
                .text_color(theme.muted_foreground),
        )
        .child(div().flex_1().child(Input::new(&input).small().w_full()))
        .child(
            div()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(match_text),
        )
        .child(
            div()
                .id("close-search")
                .flex()
                .items_center()
                .justify_center()
                .size(Heights::ICON_SM)
                .rounded(Radii::SM)
                .cursor_pointer()
                .hover(|d| d.bg(theme.secondary))
                .on_click({
                    move |_, _, cx| {
                        state.update(cx, |s, cx| s.close_search(cx));
                    }
                })
                .child(
                    svg()
                        .path(AppIcon::X.path())
                        .size_3()
                        .text_color(theme.muted_foreground),
                ),
        )
}

#[allow(clippy::too_many_arguments)]
fn render_tree_row(
    node: TreeNode,
    is_cursor: bool,
    is_expanded: bool,
    is_value_expanded: bool,
    is_search_match: bool,
    is_current_match: bool,
    theme: gpui_component::Theme,
    state: Entity<DocumentTreeState>,
    node_id: NodeId,
) -> Stateful<Div> {
    let indent = INDENT_WIDTH * node.depth as f32;
    let is_expandable = node.is_expandable();
    let is_truncated = node.value.is_truncated();

    let chevron_state = state.clone();
    let chevron_node_id = node_id.clone();

    let row_state = state.clone();
    let row_node_id = node_id.clone();

    let value_state = state.clone();
    let value_node_id = node_id.clone();

    let selection_color = theme.selection;
    let secondary_color = theme.secondary;
    let primary_color = theme.primary;
    let muted_color = theme.muted_foreground;
    let warning_color = theme.warning;

    // Determine background color based on state
    let bg_color = if is_cursor {
        selection_color
    } else if is_current_match {
        warning_color.opacity(0.4)
    } else if is_search_match {
        warning_color.opacity(0.2)
    } else {
        Hsla::transparent_black()
    };

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
        .bg(bg_color)
        .hover(move |d| {
            d.bg(if is_cursor {
                selection_color
            } else if is_current_match {
                warning_color.opacity(0.5)
            } else if is_search_match {
                warning_color.opacity(0.3)
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
        .child(render_value_preview_with_expand(
            &node.value,
            is_value_expanded,
            is_truncated,
            &theme,
            value_state,
            value_node_id,
        ))
        // Type badge with type-colored background
        .child({
            let type_color = get_type_color(&node.value, &theme);
            div()
                .text_size(FontSizes::XS)
                .text_color(type_color)
                .px(Spacing::XS)
                .rounded(Radii::SM)
                .bg(type_color.opacity(0.15))
                .child(node.value.type_label())
        })
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

fn get_type_color(value: &NodeValue, theme: &gpui_component::Theme) -> Hsla {
    match value {
        NodeValue::Scalar(v) => match v {
            dbflux_core::Value::Null => theme.muted_foreground,
            dbflux_core::Value::Bool(_) => hsla(280.0 / 360.0, 0.6, 0.6, 1.0),
            dbflux_core::Value::Int(_) => hsla(120.0 / 360.0, 0.5, 0.5, 1.0),
            dbflux_core::Value::Float(_) | dbflux_core::Value::Decimal(_) => {
                hsla(150.0 / 360.0, 0.5, 0.5, 1.0)
            }
            dbflux_core::Value::Text(_) => hsla(30.0 / 360.0, 0.7, 0.6, 1.0),
            dbflux_core::Value::ObjectId(_) => theme.primary,
            dbflux_core::Value::DateTime(_)
            | dbflux_core::Value::Date(_)
            | dbflux_core::Value::Time(_) => hsla(200.0 / 360.0, 0.6, 0.5, 1.0),
            dbflux_core::Value::Bytes(_) => theme.warning,
            dbflux_core::Value::Json(_) => theme.muted_foreground,
            _ => theme.foreground,
        },
        NodeValue::Document(_) | NodeValue::Array(_) => theme.muted_foreground,
    }
}

fn render_value_preview_with_expand(
    value: &NodeValue,
    is_expanded: bool,
    is_truncated: bool,
    theme: &gpui_component::Theme,
    state: Entity<DocumentTreeState>,
    node_id: NodeId,
) -> Stateful<Div> {
    let color = get_type_color(value, theme);

    let text = if is_expanded {
        value.full_preview().to_string()
    } else {
        value.preview().to_string()
    };

    let base = div()
        .id(ElementId::Name(format!("value-{:?}", node_id.path).into()))
        .flex_1()
        .overflow_x_hidden()
        .ml(Spacing::XS)
        .text_size(FontSizes::SM)
        .text_color(color);

    if is_expanded {
        base.max_h(px(120.0))
            .overflow_y_scroll()
            .child(text)
            .when(is_truncated, |d| {
                d.cursor_pointer()
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        cx.stop_propagation();
                        state.update(cx, |s, cx| s.toggle_value_expand(&node_id, cx));
                    })
            })
    } else {
        base.text_ellipsis()
            .whitespace_nowrap()
            .child(text)
            .when(is_truncated, |d| {
                d.cursor_pointer()
                    .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                        cx.stop_propagation();
                        state.update(cx, |s, cx| s.toggle_value_expand(&node_id, cx));
                    })
            })
    }
}
