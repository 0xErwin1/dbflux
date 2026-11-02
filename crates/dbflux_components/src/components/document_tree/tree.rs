use crate::controls::{GpuiInput as Input, InputEvent, InputState};
use crate::icons::AppIcon;
use crate::primitives::{Icon, Text};
use crate::tokens::{FontSizes, Heights, Radii, Spacing, ui};
use crate::vim::{VimBinding, VimHost};
use bolt::components::document_tree::{DocumentTreeRowHost, DocumentTreeRowView, render_row};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::input::EditorState;
use std::sync::Arc;

use super::events::{DocumentTreeEvent, TreeDirection};
use super::node::{NodeId, TreeNode};
use super::state::{DocumentTreeState, DocumentViewMode};

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
        CycleDataView,
        OpenSearch,
        NextMatch,
        PrevMatch,
        CloseSearch,
    ]
);

/// GPUI key context the tree installs on its root element.
///
/// Every single-chord binding in this context is generated from the app
/// keymap's `DocumentTree` layer by the UI layer, so this domain-free crate
/// never hard-codes those keys.
pub const CONTEXT: &str = "DocumentTree";

/// Document tree component for displaying MongoDB documents.
pub struct DocumentTree {
    id: ElementId,
    state: Entity<DocumentTreeState>,
    raw_json_input: Option<Entity<EditorState>>,
    /// Vim mode for the raw JSON editor, created with it.
    raw_json_vim: Option<VimBinding>,
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
            raw_json_vim: None,
            search_input: None,
            _search_subscription: None,
        }
    }

    fn ensure_raw_json_input(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<EditorState> {
        if let Some(input) = &self.raw_json_input {
            return input.clone();
        }

        // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
        });

        self.raw_json_input = Some(input.clone());
        self.raw_json_vim = Some(VimBinding::new(input.clone(), window, cx));
        VimBinding::follow_setting(self, input.entity_id(), cx);
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

        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document_tree.search_placeholder"))
        });

        let state = self.state.clone();
        let subscription =
            cx.subscribe_in(&input, window, move |_this, input, event, window, cx| {
                match event {
                    InputEvent::Change => {
                        let value = input.read(cx).value().to_string();
                        state.update(cx, |s, cx| s.set_search(&value, cx));
                    }
                    // Enter keeps the matches and hands the keyboard back to
                    // the tree, where n / Shift+N step through them.
                    InputEvent::PressEnter { .. } => {
                        state.update(cx, |s, cx| s.focus(window, cx));
                    }
                    _ => {}
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

            if self.state.update(cx, |s, _| s.take_search_focus_request()) {
                input.update(cx, |input_state, cx| {
                    input_state.focus(window, cx);
                });
            }

            Some(input)
        } else {
            self.search_input.clone()
        };

        // Vim's listeners on the raw JSON editor. In Insert mode the tree's
        // Escape (`CloseSearch`) leaves Insert mode instead of handing the
        // keyboard back to the tree.
        let raw_json_editor = self.raw_json_vim.as_ref().map(|vim| {
            let input = vim.input_id();
            let container = VimBinding::wire(vim.leader_scope(div(), cx), input, cx);
            let container = VimBinding::capture_action::<CloseSearch, _>(container, input, cx);

            container
                .size_full()
                .flex()
                .flex_col()
                .p(Spacing::SM)
                .child(
                    div().flex_1().min_h_0().child(
                        vim.editor(false)
                            .w_full()
                            .h_full()
                            .font_family(crate::fonts::editor_family(cx))
                            .font_weight(FontWeight::MEDIUM)
                            .text_size(FontSizes::BASE),
                    ),
                )
                .children(vim.render_indicator(cx))
        });

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
                move |_: &StartEdit, window, cx| {
                    state.update(cx, |s, cx| s.start_edit_at_cursor(window, cx));
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
                move |_: &CycleDataView, _window, cx| {
                    state.update(cx, |_, cx| {
                        cx.emit(DocumentTreeEvent::CycleDataViewRequested)
                    });
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
                move |_: &CloseSearch, window, cx| {
                    state.update(cx, |s, cx| {
                        s.close_search(cx);
                        s.focus(window, cx);
                    });
                }
            })
            .on_click(cx.listener(|this, _, window, cx| {
                this.state.update(cx, |s, cx| s.focus(window, cx));
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
                    .font_family(crate::fonts::grid_family(cx))
                    .when(is_tree_mode, |d| {
                        d.child(
                            uniform_list("document-tree-list", node_count, {
                                let state = state.clone();
                                move |range, _window, cx| {
                                    let visible_nodes: Vec<TreeNode> =
                                        state.update(cx, |s, _| s.visible_nodes().to_vec());

                                    let cursor = state.read(cx).cursor().cloned();

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

                                    let editing_node = state_ref.editing_node().cloned();
                                    let inline_edit_input = state_ref.inline_edit_input().cloned();

                                    let host = DocumentTreeRowHost {
                                        chevron_expanded: AppIcon::ChevronDown.into(),
                                        chevron_collapsed: AppIcon::ChevronRight.into(),
                                        on_row_click: {
                                            let state = state.clone();
                                            Arc::new(
                                                move |node_id: &NodeId,
                                                      event: &ClickEvent,
                                                      window: &mut Window,
                                                      cx: &mut App| {
                                                    let click_count = event.click_count();
                                                    state.update(cx, |s, cx| {
                                                        s.focus(window, cx);

                                                        if click_count == 1 {
                                                            // Single click: set cursor to this node
                                                            s.set_cursor(node_id, cx);
                                                        } else if click_count == 2 {
                                                            // Double click: expand/collapse or edit
                                                            s.execute_node(node_id, window, cx);
                                                        }
                                                    });
                                                },
                                            )
                                        },
                                        on_row_context: {
                                            let state = state.clone();
                                            Arc::new(
                                                move |node_id: &NodeId,
                                                      event: &MouseDownEvent,
                                                      window: &mut Window,
                                                      cx: &mut App| {
                                                    cx.stop_propagation();
                                                    let position = event.position;
                                                    state.update(cx, |s, cx| {
                                                        s.focus(window, cx);
                                                        s.set_cursor(node_id, cx);
                                                        s.request_context_menu(position, cx);
                                                    });
                                                },
                                            )
                                        },
                                        on_chevron_click: {
                                            let state = state.clone();
                                            Arc::new(
                                                move |node_id: &NodeId,
                                                      _event: &ClickEvent,
                                                      _window: &mut Window,
                                                      cx: &mut App| {
                                                    state.update(cx, |s, cx| s.toggle_expand(node_id, cx));
                                                },
                                            )
                                        },
                                        on_value_click: {
                                            let state = state.clone();
                                            Arc::new(
                                                move |node_id: &NodeId,
                                                      event: &ClickEvent,
                                                      window: &mut Window,
                                                      cx: &mut App| {
                                                    let click_count = event.click_count();
                                                    state.update(cx, |s, cx| {
                                                        if click_count == 1 {
                                                            s.set_cursor(node_id, cx);
                                                        } else if click_count == 2 {
                                                            s.handle_value_click(node_id, window, cx);
                                                        }
                                                    });
                                                },
                                            )
                                        },
                                    };

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
                                            let is_editing =
                                                editing_node.as_ref() == Some(&node.id);

                                            let value_text: SharedString =
                                                if let Some(summary) = node
                                                    .summary
                                                    .as_deref()
                                                    .filter(|_| !is_expanded)
                                                {
                                                    summary.to_owned().into()
                                                } else if is_expanded {
                                                    node.value.full_preview().to_string().into()
                                                } else {
                                                    node.value.preview().to_string().into()
                                                };

                                            render_row(
                                                DocumentTreeRowView {
                                                    id: node.id.clone(),
                                                    depth: node.depth,
                                                    key: node.key.to_string().into(),
                                                    value_text,
                                                    value_class: node.value.type_class(),
                                                    type_label: node.value.type_label(),
                                                    is_expandable: node.is_expandable(),
                                                    is_expanded,
                                                    is_value_expanded,
                                                    is_cursor,
                                                    is_current_match,
                                                    is_search_match,
                                                    is_editing,
                                                    inline_edit_input: inline_edit_input.clone(),
                                                },
                                                &host,
                                                cx,
                                            )
                                        })
                                        .collect()
                                }
                            })
                            .track_scroll(&scroll_handle)
                            .size_full()
                            .with_sizing_behavior(ListSizingBehavior::Infer),
                        )
                    })
                    .when_some(
                        raw_json_editor.filter(|_| raw_json_input.is_some() && !is_tree_mode),
                        |d, editor| d.child(editor),
                    ),
            )
    }
}

impl EventEmitter<DocumentTreeEvent> for DocumentTree {}

impl VimHost for DocumentTree {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.raw_json_vim
            .as_ref()
            .and_then(|vim| vim.for_input(input))
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.raw_json_vim
            .as_mut()
            .and_then(|vim| vim.for_input_mut(input))
    }
    /// Render reloads the raw view from the documents whenever its text
    /// differs, so an edit never sticks; Vim treats it as a viewer.
    fn vim_read_only(&self, _input: EntityId, _cx: &App) -> bool {
        true
    }
}

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
            Text::caption(if is_tree_mode {
                dbflux_i18n::t!("document_tree.view.tree")
            } else {
                dbflux_i18n::t!("document_tree.view.raw_json")
            })
            .font_size(FontSizes::XS),
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
                    Icon::new(if is_tree_mode {
                        AppIcon::Braces
                    } else {
                        AppIcon::Rows3
                    })
                    .size(Heights::ICON_SM)
                    .muted(),
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
        dbflux_i18n::t!("document_tree.no_matches")
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
        .child(Icon::new(AppIcon::Search).size(Heights::ICON_SM).muted())
        .child(div().flex_1().child(Input::new(&input).small().w_full()))
        .child(Text::caption(match_text).font_size(FontSizes::XS))
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
                .child(Icon::new(AppIcon::X).size(ui(12.0)).muted()),
        )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[test]
    fn document_tree_keys_resolve_in_both_locales() {
        let keys = [
            "document_tree.search_placeholder",
            "document_tree.view.tree",
            "document_tree.view.raw_json",
            "document_tree.no_matches",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn document_tree_raw_json_differs_between_locales() {
        let en = dbflux_i18n::t!("document_tree.view.raw_json", locale = "en");
        let es = dbflux_i18n::t!("document_tree.view.raw_json", locale = "es");
        assert_ne!(en, es);
    }
}

#[cfg(test)]
mod vim_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{CloseSearch, DocumentTree};
    use crate::components::document_tree::state::DocumentTreeState;
    use dbflux_core::Value;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::collections::BTreeMap;

    /// A tree in its Raw JSON view with the keyboard in the editor, the
    /// app's Escape binding for the tree, and Vim mode on.
    fn raw_tree(cx: &mut TestAppContext) -> (Entity<DocumentTree>, &mut VisualTestContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::actions::record_last_keystroke(cx);
            crate::vim::set_vim_enabled(cx, true);
            cx.bind_keys([gpui::KeyBinding::new(
                "escape",
                CloseSearch,
                Some(super::CONTEXT),
            )]);
        });

        let (tree, window) = cx.add_window_view(|_window, cx| {
            let state = cx.new(|cx| {
                let mut state = DocumentTreeState::new(cx);
                state.load_from_values(
                    vec![(
                        "doc".to_string(),
                        Value::Document(BTreeMap::from([(
                            "name".to_string(),
                            Value::Text("alice".to_string()),
                        )])),
                    )],
                    cx,
                );
                state.toggle_view_mode(cx);
                state
            });
            DocumentTree::new("tree", state, cx)
        });
        window.run_until_parked();

        window.update(|window, cx| {
            let input = tree.read(cx).raw_json_input.clone().expect("raw editor");
            input.update(cx, |state, cx| {
                state.set_selected_range(0..0, cx);
                state.focus(window, cx);
            });
        });
        window.run_until_parked();

        (tree, window)
    }

    fn cursor_and_focus(
        tree: &Entity<DocumentTree>,
        window: &mut VisualTestContext,
    ) -> (usize, bool) {
        window.update(|window, cx| {
            let input = tree.read(cx).raw_json_input.clone().expect("raw editor");
            let state = input.read(cx);
            (
                state.cursor(),
                gpui::Focusable::focus_handle(state, cx).is_focused(window),
            )
        })
    }

    /// `j` moves down the raw JSON, the first Escape leaves Insert mode in
    /// the editor, and Escape in Normal mode hands the keyboard to the tree.
    #[gpui::test]
    fn the_raw_json_view_takes_vim_keys(cx: &mut TestAppContext) {
        let (tree, window) = raw_tree(cx);

        window.simulate_keystrokes("j");
        window.run_until_parked();
        let (cursor, focused) = cursor_and_focus(&tree, window);
        assert!(cursor > 0 && focused, "j moved down: {cursor}");

        window.simulate_keystrokes("i escape");
        window.run_until_parked();
        assert!(
            cursor_and_focus(&tree, window).1,
            "the first Escape only leaves Insert mode"
        );

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!cursor_and_focus(&tree, window).1);
    }
}
