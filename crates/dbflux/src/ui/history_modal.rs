use crate::app::AppState;
use crate::keymap::ContextId;
use crate::ui::toast::ToastExt;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::{HistoryEntry, SavedQuery};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::input::{Input, InputEvent, InputState};
use uuid::Uuid;

#[derive(Clone)]
pub struct HistoryQuerySelected {
    pub sql: String,
    pub name: Option<String>,
    pub saved_query_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct QuerySaved {
    pub id: Uuid,
    pub name: String,
}

#[derive(Default, Clone, Copy, PartialEq)]
pub enum HistoryTab {
    #[default]
    Recent,
    Saved,
}

enum ModalMode {
    Browse,
    Save { sql: String },
}

pub struct HistoryModal {
    app_state: Entity<AppState>,
    visible: bool,
    active_tab: HistoryTab,
    selected_index: Option<usize>,
    search_query: String,
    search_input: Entity<InputState>,
    rename_input: Entity<InputState>,
    editing_id: Option<Uuid>,
    mode: ModalMode,
    save_name_input: Entity<InputState>,
}

impl HistoryModal {
    pub fn new(app_state: Entity<AppState>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input = cx.new(|cx| InputState::new(window, cx).placeholder("Search..."));
        let rename_input = cx.new(|cx| InputState::new(window, cx));
        let save_name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Query name"));

        cx.subscribe_in(
            &search_input,
            window,
            |this, entity, event: &InputEvent, _, cx| {
                if let InputEvent::Change = event {
                    this.search_query = entity.read(cx).value().to_string();
                    cx.notify();
                }
            },
        )
        .detach();

        cx.subscribe_in(
            &rename_input,
            window,
            |this, _entity, event: &InputEvent, window, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    this.finish_rename(window, cx);
                }
            },
        )
        .detach();

        cx.subscribe_in(
            &save_name_input,
            window,
            |this, _entity, event: &InputEvent, window, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    this.confirm_save(window, cx);
                }
            },
        )
        .detach();

        Self {
            app_state,
            visible: false,
            active_tab: HistoryTab::default(),
            selected_index: None,
            search_query: String::new(),
            search_input,
            rename_input,
            editing_id: None,
            mode: ModalMode::Browse,
            save_name_input,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Returns true if the modal is in a mode where text input is expected
    /// (save mode or renaming). In this case, navigation keys should not be processed.
    pub fn is_input_mode(&self) -> bool {
        matches!(self.mode, ModalMode::Save { .. }) || self.editing_id.is_some()
    }

    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.mode = ModalMode::Browse;
        self.active_tab = HistoryTab::Recent;
        self.selected_index = Some(0);
        self.search_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });
        self.search_query.clear();
        self.editing_id = None;
        cx.notify();
    }

    pub fn open_saved_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.mode = ModalMode::Browse;
        self.active_tab = HistoryTab::Saved;
        self.selected_index = Some(0);
        self.search_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });
        self.search_query.clear();
        self.editing_id = None;
        cx.notify();
    }

    pub fn open_save(&mut self, sql: String, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.mode = ModalMode::Save { sql };
        self.save_name_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.selected_index = None;
        self.editing_id = None;
        cx.notify();
    }

    pub fn select_next(&mut self, cx: &mut Context<Self>) {
        let count = self.current_list_count(cx);
        if count == 0 {
            return;
        }

        let next = match self.selected_index {
            Some(idx) => (idx + 1).min(count.saturating_sub(1)),
            None => 0,
        };
        self.selected_index = Some(next);
        cx.notify();
    }

    pub fn select_prev(&mut self, cx: &mut Context<Self>) {
        let count = self.current_list_count(cx);
        if count == 0 {
            return;
        }

        let prev = match self.selected_index {
            Some(idx) => idx.saturating_sub(1),
            None => count.saturating_sub(1),
        };
        self.selected_index = Some(prev);
        cx.notify();
    }

    pub fn execute_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.mode {
            ModalMode::Browse => {
                let Some(idx) = self.selected_index else {
                    return;
                };

                let (sql, name, saved_query_id) = match self.active_tab {
                    HistoryTab::Recent => {
                        let entries = self.filtered_history_entries(cx);
                        entries
                            .get(idx)
                            .map(|e| (e.sql.clone(), None, None))
                            .unwrap_or_default()
                    }
                    HistoryTab::Saved => {
                        let entries = self.filtered_saved_queries(cx);
                        if let Some(entry) = entries.get(idx) {
                            self.app_state.update(cx, |state, _| {
                                state.update_saved_query_last_used(entry.id);
                            });
                            (entry.sql.clone(), Some(entry.name.clone()), Some(entry.id))
                        } else {
                            (String::new(), None, None)
                        }
                    }
                };

                if !sql.is_empty() {
                    cx.emit(HistoryQuerySelected {
                        sql,
                        name,
                        saved_query_id,
                    });
                    self.close(cx);
                }
            }
            ModalMode::Save { .. } => {
                self.confirm_save(window, cx);
            }
        }
    }

    pub fn delete_selected(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.mode, ModalMode::Browse) || self.active_tab != HistoryTab::Saved {
            return;
        }

        let entries = self.filtered_saved_queries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            let entry_id = entry.id;
            self.app_state.update(cx, |state, _| {
                state.remove_saved_query(entry_id);
            });

            let new_count = self.filtered_saved_queries(cx).len();
            self.selected_index = if new_count == 0 {
                None
            } else {
                Some(idx.min(new_count.saturating_sub(1)))
            };
            cx.notify();
        }
    }

    pub fn toggle_favorite_selected(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.mode, ModalMode::Browse) || self.active_tab != HistoryTab::Saved {
            return;
        }

        let entries = self.filtered_saved_queries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            self.app_state.update(cx, |state, _| {
                state.toggle_saved_query_favorite(entry.id);
            });
            cx.notify();
        }
    }

    pub fn start_rename_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.mode, ModalMode::Browse) || self.active_tab != HistoryTab::Saved {
            return;
        }

        let entries = self.filtered_saved_queries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            self.editing_id = Some(entry.id);
            self.rename_input.update(cx, |state, cx| {
                state.set_value(&entry.name, window, cx);
                state.focus(window, cx);
            });
            cx.notify();
        }
    }

    pub fn focus_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.mode, ModalMode::Browse) {
            return;
        }

        self.search_input
            .update(cx, |state, cx| state.focus(window, cx));
    }

    pub fn save_selected_history(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.mode, ModalMode::Browse) || self.active_tab != HistoryTab::Recent {
            return;
        }

        let entries = self.filtered_history_entries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            let sql = entry.sql.clone();
            self.mode = ModalMode::Save { sql };
            self.save_name_input.update(cx, |state, cx| {
                state.set_value("", window, cx);
                state.focus(window, cx);
            });
            cx.notify();
        }
    }

    fn finish_rename(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(id) = self.editing_id else {
            return;
        };

        let new_name = self.rename_input.read(cx).value();
        if new_name.trim().is_empty() {
            self.editing_id = None;
            return;
        }

        let sql = self
            .app_state
            .read(cx)
            .saved_queries()
            .iter()
            .find(|q| q.id == id)
            .map(|q| q.sql.clone());

        if let Some(sql) = sql {
            self.app_state.update(cx, |state, _| {
                state.update_saved_query(id, new_name.to_string(), sql);
            });
        }

        self.editing_id = None;
        self.search_input
            .update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    fn confirm_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let ModalMode::Save { ref sql } = self.mode else {
            return;
        };

        let name = self.save_name_input.read(cx).value();
        if name.trim().is_empty() {
            cx.toast_warning("Enter a name for the query", window);
            return;
        }

        let name = name.to_string();
        let query = SavedQuery::new(name.clone(), sql.clone(), None);
        let id = query.id;
        self.app_state.update(cx, |state, _| {
            state.add_saved_query(query);
        });
        cx.emit(QuerySaved { id, name });
        cx.toast_success("Saved query", window);
        self.close(cx);
    }

    fn current_list_count(&self, cx: &Context<Self>) -> usize {
        match self.active_tab {
            HistoryTab::Recent => self.filtered_history_entries(cx).len(),
            HistoryTab::Saved => self.filtered_saved_queries(cx).len(),
        }
    }

    fn filtered_history_entries(&self, cx: &Context<Self>) -> Vec<HistoryEntry> {
        filter_history_entries(
            self.app_state.read(cx).history_entries(),
            &self.search_query,
            50,
        )
    }

    fn filtered_saved_queries(&self, cx: &Context<Self>) -> Vec<SavedQuery> {
        filter_saved_queries(self.app_state.read(cx).saved_queries(), &self.search_query)
    }

    fn render_browse(&self, _window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let search_input = self.search_input.clone();
        let rename_input = self.rename_input.clone();
        let selected = self.selected_index.unwrap_or(0);

        div()
            .id("history-modal")
            .key_context(ContextId::HistoryModal.as_gpui_context())
            .absolute()
            .inset_0()
            .bg(gpui::black().opacity(0.5))
            .flex()
            .justify_center()
            .items_start()
            .pt(px(80.0))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.close(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &crate::keymap::SelectNext, _, cx| {
                this.select_next(cx);
            }))
            .on_action(cx.listener(|this, _: &crate::keymap::SelectPrev, _, cx| {
                this.select_prev(cx);
            }))
            .on_action(cx.listener(|this, _: &crate::keymap::Execute, window, cx| {
                this.execute_selected(window, cx);
            }))
            .on_action(cx.listener(|this, _: &crate::keymap::Delete, _, cx| {
                this.delete_selected(cx);
            }))
            .on_action(
                cx.listener(|this, _: &crate::keymap::ToggleFavorite, _, cx| {
                    this.toggle_favorite_selected(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &crate::keymap::Rename, window, cx| {
                this.start_rename_selected(window, cx);
            }))
            .on_action(
                cx.listener(|this, _: &crate::keymap::FocusSearch, window, cx| {
                    this.focus_search(window, cx);
                }),
            )
            .on_action(
                cx.listener(|this, _: &crate::keymap::SaveQuery, window, cx| {
                    this.save_selected_history(window, cx);
                }),
            )
            .on_action(cx.listener(|this, _: &crate::keymap::Cancel, _, cx| {
                this.close(cx);
            }))
            .child(
                div()
                    .w(px(620.0))
                    .max_h(px(520.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::LG)
                    .shadow_lg()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap(Spacing::SM)
                            .p(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .child(self.render_tabs(cx))
                            .child(Input::new(&search_input).small().cleanable(true)),
                    )
                    .child(self.render_list(&rename_input, selected, cx))
                    .child(self.render_footer(cx)),
            )
            .into_any_element()
    }

    fn render_tabs(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .gap(Spacing::XS)
            .child(
                div()
                    .id("tab-recent")
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .rounded(Radii::MD)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .when(self.active_tab == HistoryTab::Recent, |this| {
                        this.bg(theme.secondary).text_color(theme.foreground)
                    })
                    .when(self.active_tab != HistoryTab::Recent, |this| {
                        this.text_color(theme.muted_foreground)
                            .hover(|d| d.bg(theme.secondary))
                    })
                    .child("Recent")
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.active_tab = HistoryTab::Recent;
                            this.selected_index = Some(0);
                            cx.notify();
                        }),
                    ),
            )
            .child(
                div()
                    .id("tab-saved")
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .rounded(Radii::MD)
                    .cursor_pointer()
                    .text_size(FontSizes::SM)
                    .when(self.active_tab == HistoryTab::Saved, |this| {
                        this.bg(theme.secondary).text_color(theme.foreground)
                    })
                    .when(self.active_tab != HistoryTab::Saved, |this| {
                        this.text_color(theme.muted_foreground)
                            .hover(|d| d.bg(theme.secondary))
                    })
                    .child("Saved")
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.active_tab = HistoryTab::Saved;
                            this.selected_index = Some(0);
                            cx.notify();
                        }),
                    ),
            )
    }

    fn render_list(
        &self,
        rename_input: &Entity<InputState>,
        selected: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        match self.active_tab {
            HistoryTab::Recent => {
                let entries = self.filtered_history_entries(cx);
                div()
                    .flex_1()
                    .overflow_y_hidden()
                    .children(entries.iter().enumerate().map(|(idx, entry)| {
                        let is_selected = idx == selected;
                        let sql = entry.sql.clone();

                        div()
                            .id(("history-entry", idx))
                            .flex()
                            .flex_col()
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .border_b_1()
                            .border_color(theme.border)
                            .cursor_pointer()
                            .when(is_selected, |d| d.bg(theme.secondary))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                cx.emit(HistoryQuerySelected {
                                    sql: sql.clone(),
                                    name: None,
                                    saved_query_id: None,
                                });
                                this.close(cx);
                            }))
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(entry.sql_preview(60)),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(entry.formatted_timestamp())
                                    .when_some(entry.row_count, |d, count| {
                                        d.child(format!("{} rows", count))
                                    })
                                    .child(format!("{}ms", entry.execution_time_ms)),
                            )
                    }))
                    .when(entries.is_empty(), |d| {
                        d.child(
                            div()
                                .px(Spacing::SM)
                                .py(Spacing::LG)
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .text_center()
                                .child("No history yet"),
                        )
                    })
                    .into_any_element()
            }
            HistoryTab::Saved => {
                let entries = self.filtered_saved_queries(cx);
                let rename_input = rename_input.clone();

                div()
                    .flex_1()
                    .overflow_y_hidden()
                    .children(entries.iter().enumerate().map(|(idx, entry)| {
                        let is_selected = idx == selected;
                        let id = entry.id;
                        let sql = entry.sql.clone();
                        let entry_name = entry.name.clone();
                        let is_favorite = entry.is_favorite;
                        let is_editing = self.editing_id == Some(id);
                        let rename_input = rename_input.clone();

                        div()
                            .id(("saved-query", idx))
                            .flex()
                            .flex_col()
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .border_b_1()
                            .border_color(theme.border)
                            .cursor_pointer()
                            .when(is_selected, |d| d.bg(theme.secondary))
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, _window, cx| {
                                cx.emit(HistoryQuerySelected {
                                    sql: sql.clone(),
                                    name: Some(entry_name.clone()),
                                    saved_query_id: Some(id),
                                });
                                this.app_state.update(cx, |state, _| {
                                    state.update_saved_query_last_used(id);
                                });
                                this.close(cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .child(
                                        div()
                                            .flex_1()
                                            .when(is_editing, |d| {
                                                d.child(
                                                    div()
                                                        .w(px(200.0))
                                                        .child(Input::new(&rename_input).small()),
                                                )
                                            })
                                            .when(!is_editing, |d| {
                                                d.child(
                                                    div()
                                                        .text_size(FontSizes::SM)
                                                        .text_color(theme.foreground)
                                                        .child(entry.name.clone()),
                                                )
                                            }),
                                    )
                                    .child(
                                        div().flex().items_center().gap(Spacing::XS).child(
                                            div()
                                                .id(SharedString::from(format!("favorite-{}", id)))
                                                .w(Heights::ICON_SM)
                                                .h(Heights::ICON_SM)
                                                .flex()
                                                .items_center()
                                                .justify_center()
                                                .rounded(Radii::SM)
                                                .text_size(FontSizes::SM)
                                                .when(is_favorite, |d| {
                                                    d.text_color(gpui::rgb(0xF59E0B))
                                                })
                                                .when(!is_favorite, |d| {
                                                    d.text_color(theme.muted_foreground)
                                                })
                                                .hover(|d| d.bg(theme.secondary))
                                                .on_click(cx.listener(move |this, _, _, cx| {
                                                    this.app_state.update(cx, |state, _| {
                                                        state.toggle_saved_query_favorite(id);
                                                    });
                                                    cx.notify();
                                                }))
                                                .child(if is_favorite { "★" } else { "☆" }),
                                        ),
                                    ),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(entry.sql_preview(80)),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(entry.formatted_last_used_at()),
                            )
                    }))
                    .when(entries.is_empty(), |d| {
                        d.child(
                            div()
                                .px(Spacing::SM)
                                .py(Spacing::LG)
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .text_center()
                                .child("No saved queries"),
                        )
                    })
                    .into_any_element()
            }
        }
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let count = self.current_list_count(cx);

        let shortcuts = match self.active_tab {
            HistoryTab::Recent => "C-j/k Navigate  Enter Load  C-s Save  Esc Close",
            HistoryTab::Saved => {
                "C-j/k Navigate  Enter Load  C-d Delete  C-f Favorite  C-r Rename  Esc Close"
            }
        };

        div()
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .flex()
            .items_center()
            .justify_between()
            .text_size(FontSizes::XS)
            .text_color(theme.muted_foreground)
            .child(shortcuts)
            .child(format!("{} items", count))
    }

    fn render_save(&self, _window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let input = self.save_name_input.clone();

        div()
            .id("save-query-modal")
            .key_context(ContextId::HistoryModal.as_gpui_context())
            .absolute()
            .inset_0()
            .bg(gpui::black().opacity(0.5))
            .flex()
            .justify_center()
            .items_start()
            .pt(px(120.0))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.close(cx);
                }),
            )
            .on_action(cx.listener(|this, _: &crate::keymap::Execute, window, cx| {
                this.confirm_save(window, cx);
            }))
            .on_action(cx.listener(|this, _: &crate::keymap::Cancel, _, cx| {
                this.close(cx);
            }))
            .child(
                div()
                    .w(px(420.0))
                    .bg(theme.background)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::LG)
                    .shadow_lg()
                    .overflow_hidden()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_b_1()
                            .border_color(theme.border)
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .child("Save Query"),
                    )
                    .child(div().p(Spacing::MD).child(Input::new(&input).w_full()))
                    .child(
                        div()
                            .px(Spacing::MD)
                            .py(Spacing::SM)
                            .border_t_1()
                            .border_color(theme.border)
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child("Enter to save, Esc to cancel"),
                    ),
            )
            .into_any_element()
    }
}

impl Render for HistoryModal {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        match self.mode {
            ModalMode::Browse => self.render_browse(window, cx),
            ModalMode::Save { .. } => self.render_save(window, cx),
        }
    }
}

impl EventEmitter<HistoryQuerySelected> for HistoryModal {}
impl EventEmitter<QuerySaved> for HistoryModal {}

fn filter_history_entries(
    entries: &[HistoryEntry],
    query: &str,
    max_entries: usize,
) -> Vec<HistoryEntry> {
    if query.trim().is_empty() {
        return entries.iter().take(max_entries).cloned().collect();
    }

    let query_lower = query.to_lowercase();
    entries
        .iter()
        .filter(|entry| entry.sql.to_lowercase().contains(&query_lower))
        .take(max_entries)
        .cloned()
        .collect()
}

fn filter_saved_queries(entries: &[SavedQuery], query: &str) -> Vec<SavedQuery> {
    let mut filtered: Vec<SavedQuery> = if query.trim().is_empty() {
        entries.to_vec()
    } else {
        let query_lower = query.to_lowercase();
        entries
            .iter()
            .filter(|entry| {
                entry.name.to_lowercase().contains(&query_lower)
                    || entry.sql.to_lowercase().contains(&query_lower)
            })
            .cloned()
            .collect()
    };

    filtered.sort_by(|a, b| {
        b.is_favorite
            .cmp(&a.is_favorite)
            .then_with(|| b.last_used_at.cmp(&a.last_used_at))
    });

    filtered
}

#[cfg(test)]
mod tests {
    use super::{filter_history_entries, filter_saved_queries};
    use dbflux_core::{HistoryEntry, SavedQuery};
    use std::time::Duration;

    #[test]
    fn filters_history_entries_by_query() {
        let entries = vec![
            HistoryEntry::new(
                "SELECT 1".to_string(),
                None,
                None,
                Duration::from_millis(10),
                None,
            ),
            HistoryEntry::new(
                "SELECT 2".to_string(),
                None,
                None,
                Duration::from_millis(10),
                None,
            ),
        ];

        let filtered = filter_history_entries(&entries, "2", 10);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].sql, "SELECT 2");
    }

    #[test]
    fn filters_saved_queries_by_query() {
        let entries = vec![
            SavedQuery::new("Users".to_string(), "SELECT * FROM users".to_string(), None),
            SavedQuery::new(
                "Orders".to_string(),
                "SELECT * FROM orders".to_string(),
                None,
            ),
        ];

        let filtered = filter_saved_queries(&entries, "orders");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "Orders");
    }
}
