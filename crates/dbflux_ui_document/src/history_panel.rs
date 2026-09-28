//! The query history island of a code document (IslEditor).
//!
//! A 300 px panel the document hands to the workspace as a side island. It
//! lists recent queries and saved queries, filters them, loads one into the
//! editor, saves a recent one, and renames, favorites or deletes a saved one.
//! Closed by default; the editor toolbar's History button and its shortcut
//! toggle it. It owns the keyboard only while focus is inside it, so the
//! editor keeps working next to an open history.

use dbflux_app::keymap::ContextId;
use dbflux_components::actions::{
    Cancel, Delete, Execute, FocusSearch, Rename, SaveQuery, SelectNext, SelectPrev, ToggleFavorite,
};
use dbflux_components::controls::{Button, GpuiInput as Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{ChromeColors, Heights, HistoryPanelMetrics, Radii};
use dbflux_components::typography::AppFonts;
use dbflux_core::{HistoryEntry, SavedQuery};
use dbflux_ui_base::toast::{Toast, now_hms};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use uuid::Uuid;

#[derive(Clone)]
pub struct HistoryQuerySelected {
    pub sql: String,
    pub name: Option<String>,
    pub saved_query_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct HistoryPanelClosed;

#[allow(dead_code)]
#[derive(Clone)]
pub struct QuerySaved {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum HistoryTab {
    #[default]
    Recent,
    Saved,
}

enum PanelMode {
    Browse,
    Save { sql: String },
}

// Type aliases for the callback closure signatures used in HistoryPanelCallbacks.
// These keep the struct field types short enough to satisfy the type_complexity lint.
type HistoryProviderFn = Box<dyn Fn(&App) -> Vec<HistoryEntry>>;
type SavedProviderFn = Box<dyn Fn(&App) -> Vec<SavedQuery>>;
type OnSaveFn = Box<dyn Fn(SavedQuery, &mut App)>;
type OnRenameFn = Box<dyn Fn(Uuid, String, String, &mut App)>;
type OnDeleteFn = Box<dyn Fn(Uuid, &mut App)>;
type OnToggleFavoriteFn = Box<dyn Fn(Uuid, &mut App)>;
type OnMarkUsedFn = Box<dyn Fn(Uuid, &mut App)>;

/// Injected callbacks that give `HistoryPanel` read and write access to the
/// owner's `AppStateEntity` without holding a direct entity reference.
pub struct HistoryPanelCallbacks {
    /// Returns a snapshot of the current query history.
    pub history_provider: HistoryProviderFn,
    /// Returns a snapshot of the current saved queries.
    pub saved_provider: SavedProviderFn,
    /// Persists a new saved query.
    pub on_save: OnSaveFn,
    /// Renames a saved query (id, new_name, sql).
    pub on_rename: OnRenameFn,
    /// Deletes a saved query by id.
    pub on_delete: OnDeleteFn,
    /// Toggles the favorite flag on a saved query by id.
    pub on_toggle_favorite: OnToggleFavoriteFn,
    /// Records that a saved query was used (updates last-used timestamp).
    pub on_mark_used: OnMarkUsedFn,
}

pub struct HistoryPanel {
    callbacks: HistoryPanelCallbacks,
    visible: bool,
    /// Focus is on the panel or inside it; only then does it own the
    /// keyboard.
    focus_within: bool,
    /// The search field shows under the header (the header's search button,
    /// `FocusSearch`, or a filter still applied).
    search_open: bool,
    focus_handle: FocusHandle,
    active_tab: HistoryTab,
    selected_index: Option<usize>,
    search_query: String,
    search_input: Entity<InputState>,
    rename_input: Entity<InputState>,
    editing_id: Option<Uuid>,
    mode: PanelMode,
    save_name_input: Entity<InputState>,
    _focus_subscriptions: [Subscription; 2],
}

impl HistoryPanel {
    pub fn new(
        callbacks: HistoryPanelCallbacks,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let search_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.history_modal.search_placeholder"
            ))
        });
        let rename_input = cx.new(|cx| InputState::new(window, cx));
        let save_name_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.history_modal.save.name_placeholder"
            ))
        });

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

        let focus_handle = cx.focus_handle();
        let focus_subscriptions = [
            cx.on_focus_in(&focus_handle, window, |this, _, cx| {
                this.focus_within = true;
                cx.notify();
            }),
            cx.on_focus_out(&focus_handle, window, |this, _, _, cx| {
                this.focus_within = false;
                cx.notify();
            }),
        ];

        Self {
            callbacks,
            visible: false,
            focus_within: false,
            search_open: false,
            focus_handle,
            active_tab: HistoryTab::default(),
            selected_index: None,
            search_query: String::new(),
            search_input,
            rename_input,
            editing_id: None,
            mode: PanelMode::Browse,
            save_name_input,
            _focus_subscriptions: focus_subscriptions,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// The panel is open and focus is on it or inside it, so keys go to the
    /// history rather than the editor.
    pub fn owns_keyboard(&self) -> bool {
        self.visible && self.focus_within
    }

    /// The list shown: recent queries or saved ones.
    pub fn active_tab(&self) -> HistoryTab {
        self.active_tab
    }

    /// Returns true if the modal is in a mode where text input is expected
    /// (save mode or renaming). In this case, navigation keys should not be processed.
    #[allow(dead_code)]
    pub fn is_input_mode(&self) -> bool {
        matches!(self.mode, PanelMode::Save { .. }) || self.editing_id.is_some()
    }

    /// Opens the panel on the recent queries and gives it the keyboard.
    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_on(HistoryTab::Recent, window, cx);
    }

    /// Opens the panel on the saved queries and gives it the keyboard.
    pub fn open_saved_tab(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_on(HistoryTab::Saved, window, cx);
    }

    fn open_on(&mut self, tab: HistoryTab, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.mode = PanelMode::Browse;
        self.active_tab = tab;
        self.selected_index = Some(0);
        self.search_open = false;
        self.search_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.search_query.clear();
        self.editing_id = None;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    fn set_active_tab(&mut self, tab: HistoryTab, cx: &mut Context<Self>) {
        self.active_tab = tab;
        self.selected_index = Some(0);
        self.editing_id = None;
        cx.notify();
    }

    /// Shows the other list (Recent or Saved), like its segmented control.
    /// The save form has no tabs, so it ignores the key.
    pub fn step_tab(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.mode, PanelMode::Browse) {
            return;
        }

        let next = match self.active_tab {
            HistoryTab::Recent => HistoryTab::Saved,
            HistoryTab::Saved => HistoryTab::Recent,
        };
        self.set_active_tab(next, cx);
    }

    /// Shows the search field and focuses it, or hides it and clears the
    /// filter when it is already showing.
    fn toggle_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.search_open {
            self.search_open = false;
            self.search_query.clear();
            self.search_input.update(cx, |state, cx| {
                state.set_value("", window, cx);
            });
            self.focus_handle.focus(window, cx);
            cx.notify();
        } else {
            self.focus_search(window, cx);
        }
    }

    #[allow(dead_code)]
    pub fn open_save(&mut self, sql: String, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.mode = PanelMode::Save { sql };
        self.save_name_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
            state.focus(window, cx);
        });
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        let was_visible = self.visible;
        self.visible = false;
        self.selected_index = None;
        self.editing_id = None;
        if was_visible {
            cx.emit(HistoryPanelClosed);
        }
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
            PanelMode::Browse => {
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
                            let id = entry.id;
                            let result = (entry.sql.clone(), Some(entry.name.clone()), Some(id));
                            (self.callbacks.on_mark_used)(id, cx);
                            result
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
                }
            }
            PanelMode::Save { .. } => {
                self.confirm_save(window, cx);
            }
        }
    }

    pub fn delete_selected(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.mode, PanelMode::Browse) || self.active_tab != HistoryTab::Saved {
            return;
        }

        let entries = self.filtered_saved_queries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            let entry_id = entry.id;
            (self.callbacks.on_delete)(entry_id, cx);

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
        if !matches!(self.mode, PanelMode::Browse) || self.active_tab != HistoryTab::Saved {
            return;
        }

        let entries = self.filtered_saved_queries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            (self.callbacks.on_toggle_favorite)(entry.id, cx);
            cx.notify();
        }
    }

    pub fn start_rename_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.mode, PanelMode::Browse) || self.active_tab != HistoryTab::Saved {
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
        if !matches!(self.mode, PanelMode::Browse) {
            return;
        }

        self.search_open = true;
        self.search_input
            .update(cx, |state, cx| state.focus(window, cx));
        cx.notify();
    }

    pub fn save_selected_history(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.mode, PanelMode::Browse) || self.active_tab != HistoryTab::Recent {
            return;
        }

        let entries = self.filtered_history_entries(cx);
        let Some(idx) = self.selected_index else {
            return;
        };

        if let Some(entry) = entries.get(idx) {
            let sql = entry.sql.clone();
            self.mode = PanelMode::Save { sql };
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

        let sql = (self.callbacks.saved_provider)(cx)
            .into_iter()
            .find(|q| q.id == id)
            .map(|q| q.sql);

        if let Some(sql) = sql {
            (self.callbacks.on_rename)(id, new_name.to_string(), sql, cx);
        }

        self.editing_id = None;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    fn confirm_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let PanelMode::Save { ref sql } = self.mode else {
            return;
        };

        let name = self.save_name_input.read(cx).value();
        if name.trim().is_empty() {
            Toast::warning(dbflux_i18n::t!(
                "document.key_value.history_modal.save.name_required"
            ))
            .meta_right(now_hms())
            .push(cx);
            return;
        }

        let name = name.to_string();
        let query = SavedQuery::new(name.clone(), sql.clone(), None);
        let id = query.id;
        (self.callbacks.on_save)(query, cx);
        cx.emit(QuerySaved { id, name });
        Toast::success(dbflux_i18n::t!(
            "document.key_value.history_modal.save.success_toast"
        ))
        .meta_right(now_hms())
        .push(cx);
        self.mode = PanelMode::Browse;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    /// Leaves the save form for the list without saving.
    fn cancel_save(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.mode = PanelMode::Browse;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    fn current_list_count(&self, cx: &Context<Self>) -> usize {
        match self.active_tab {
            HistoryTab::Recent => self.filtered_history_entries(cx).len(),
            HistoryTab::Saved => self.filtered_saved_queries(cx).len(),
        }
    }

    fn filtered_history_entries(&self, cx: &Context<Self>) -> Vec<HistoryEntry> {
        let entries = (self.callbacks.history_provider)(cx);
        filter_history_entries(entries.as_slice(), &self.search_query, 50)
    }

    fn filtered_saved_queries(&self, cx: &Context<Self>) -> Vec<SavedQuery> {
        let queries = (self.callbacks.saved_provider)(cx);
        filter_saved_queries(queries.as_slice(), &self.search_query)
    }

    /// Esc: leaves the save form, or closes the panel from the list.
    pub fn cancel(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.mode, PanelMode::Save { .. }) {
            self.cancel_save(window, cx);
        } else {
            self.close(cx);
        }
    }

    fn render_header(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let browsing = matches!(self.mode, PanelMode::Browse);
        let title = if browsing {
            dbflux_i18n::t!("document.code.history.title")
        } else {
            dbflux_i18n::t!("document.key_value.history_modal.save.title")
        };

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(HistoryPanelMetrics::ENTRY_GAP)
            .h(HistoryPanelMetrics::HEADER_HEIGHT)
            .px(HistoryPanelMetrics::PADDING_X)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(Text::label(title).color(theme.muted_foreground)),
            )
            .when(browsing, |header| {
                header.child(
                    Button::new(
                        "history-search-toggle",
                        dbflux_i18n::t!("document.code.history.search"),
                    )
                    .icon(AppIcon::Search)
                    .icon_only()
                    .selected(self.search_open)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.toggle_search(window, cx);
                    })),
                )
            })
    }

    fn render_tab_switch(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let entity = cx.entity().downgrade();
        let active = match self.active_tab {
            HistoryTab::Recent => "recent",
            HistoryTab::Saved => "saved",
        };

        div()
            .flex()
            .flex_none()
            .px(HistoryPanelMetrics::PADDING_X)
            .pb(HistoryPanelMetrics::SECTION_GAP)
            .child(SegmentedControl::new(
                vec![
                    SegmentedItem::new(
                        "recent",
                        crate::labels::history_tab_label(HistoryTab::Recent),
                    )
                    .icon(AppIcon::Clock),
                    SegmentedItem::new(
                        "saved",
                        crate::labels::history_tab_label(HistoryTab::Saved),
                    )
                    .icon(AppIcon::Star),
                ],
                active,
                move |id, _, cx| {
                    let tab = if id.as_ref() == "saved" {
                        HistoryTab::Saved
                    } else {
                        HistoryTab::Recent
                    };

                    if let Some(entity) = entity.upgrade() {
                        entity.update(cx, |panel, cx| panel.set_active_tab(tab, cx));
                    }
                },
            ))
    }

    fn render_browse(&self, cx: &mut Context<Self>) -> AnyElement {
        let show_search = self.search_open || !self.search_query.is_empty();
        let search_input = self.search_input.clone();
        let rename_input = self.rename_input.clone();
        let selected = self.selected_index.unwrap_or(0);

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(self.render_tab_switch(cx))
            .when(show_search, |body| {
                body.child(
                    div()
                        .flex_none()
                        .px(HistoryPanelMetrics::PADDING_X)
                        .pb(HistoryPanelMetrics::SECTION_GAP)
                        .child(Input::new(&search_input).small().cleanable(true)),
                )
            })
            .child(self.render_list(&rename_input, selected, cx))
            .into_any_element()
    }

    /// One entry: the query on a mono line, then its meta line; the current
    /// entry is filled with the tint.
    fn entry_frame(
        id: ElementId,
        is_selected: bool,
        theme: &gpui_component::Theme,
    ) -> Stateful<Div> {
        let tint = ChromeColors::tint(theme);

        div()
            .id(id)
            .flex()
            .flex_col()
            .flex_none()
            .gap(HistoryPanelMetrics::ENTRY_GAP)
            .px(HistoryPanelMetrics::PADDING_X)
            .py(HistoryPanelMetrics::ENTRY_PADDING_Y)
            .cursor_pointer()
            .when(is_selected, |entry| {
                entry.bg(tint.opacity(HistoryPanelMetrics::SELECTED_FILL_ALPHA))
            })
            .when(!is_selected, |entry| {
                entry.hover(|entry| entry.bg(theme.secondary))
            })
    }

    fn query_line(sql: String, color: Hsla) -> impl IntoElement {
        div()
            .min_w_0()
            .whitespace_nowrap()
            .overflow_hidden()
            .text_ellipsis()
            .font_family(AppFonts::MONO)
            .text_size(HistoryPanelMetrics::QUERY_FONT)
            .text_color(color)
            .child(sql)
    }

    fn meta_line(text: String, color: Hsla) -> impl IntoElement {
        div()
            .min_w_0()
            .whitespace_nowrap()
            .overflow_hidden()
            .text_ellipsis()
            .text_size(HistoryPanelMetrics::META_FONT)
            .text_color(color)
            .child(text)
    }

    fn render_list(
        &self,
        rename_input: &Entity<InputState>,
        selected: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let muted = theme.muted_foreground;
        let now = chrono::Utc::now().timestamp();

        let list = div()
            .id("history-entries")
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .overflow_y_scroll();

        match self.active_tab {
            HistoryTab::Recent => {
                let entries = self.filtered_history_entries(cx);
                let is_empty = entries.is_empty();

                let rows = entries.into_iter().enumerate().map(|(index, entry)| {
                    let is_selected = index == selected;
                    let sql = entry.sql.clone();

                    Self::entry_frame(("history-entry", index).into(), is_selected, &theme)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.selected_index = Some(index);
                            cx.emit(HistoryQuerySelected {
                                sql: sql.clone(),
                                name: None,
                                saved_query_id: None,
                            });
                            cx.notify();
                        }))
                        .child(Self::query_line(
                            entry.sql_preview(200),
                            if is_selected {
                                strong
                            } else {
                                theme.foreground
                            },
                        ))
                        .child(Self::meta_line(history_entry_meta(&entry, now), muted))
                });

                list.children(rows)
                    .when(is_empty, |list| {
                        list.child(Self::empty_note(
                            dbflux_i18n::t!("document.key_value.history_modal.empty.recent"),
                            muted,
                        ))
                    })
                    .into_any_element()
            }
            HistoryTab::Saved => {
                let entries = self.filtered_saved_queries(cx);
                let is_empty = entries.is_empty();

                let rows = entries.into_iter().enumerate().map(|(index, entry)| {
                    let is_selected = index == selected;
                    let id = entry.id;
                    let sql = entry.sql.clone();
                    let entry_name = entry.name.clone();
                    let is_favorite = entry.is_favorite;
                    let is_editing = self.editing_id == Some(id);

                    let name = if is_editing {
                        div()
                            .flex_1()
                            .min_w_0()
                            .child(Input::new(rename_input).small())
                            .into_any_element()
                    } else {
                        div()
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .text_color(strong)
                            .child(entry.name.clone())
                            .into_any_element()
                    };

                    let favorite = div()
                        .id(SharedString::from(format!("history-favorite-{id}")))
                        .flex()
                        .flex_none()
                        .items_center()
                        .justify_center()
                        .size(Heights::ICON_SM)
                        .rounded(Radii::SM)
                        .hover(|star| star.bg(theme.secondary))
                        .on_click(cx.listener(move |this, _, _, cx| {
                            cx.stop_propagation();
                            (this.callbacks.on_toggle_favorite)(id, cx);
                            cx.notify();
                        }))
                        .child(
                            Icon::new(AppIcon::Star)
                                .size(HistoryPanelMetrics::QUERY_FONT)
                                .color(if is_favorite { theme.warning } else { muted }),
                        );

                    Self::entry_frame(("saved-query", index).into(), is_selected, &theme)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.selected_index = Some(index);
                            cx.emit(HistoryQuerySelected {
                                sql: sql.clone(),
                                name: Some(entry_name.clone()),
                                saved_query_id: Some(id),
                            });
                            (this.callbacks.on_mark_used)(id, cx);
                            cx.notify();
                        }))
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap(HistoryPanelMetrics::ENTRY_GAP)
                                .text_size(HistoryPanelMetrics::QUERY_FONT)
                                .child(name)
                                .child(favorite),
                        )
                        .child(Self::query_line(entry.sql_preview(200), theme.foreground))
                        .child(Self::meta_line(entry.formatted_last_used_at(), muted))
                });

                list.children(rows)
                    .when(is_empty, |list| {
                        list.child(Self::empty_note(
                            dbflux_i18n::t!("document.key_value.history_modal.empty.saved"),
                            muted,
                        ))
                    })
                    .into_any_element()
            }
        }
    }

    fn empty_note(text: String, color: Hsla) -> impl IntoElement {
        div()
            .px(HistoryPanelMetrics::PADDING_X)
            .py(HistoryPanelMetrics::ENTRY_PADDING_Y)
            .text_size(HistoryPanelMetrics::META_FONT)
            .text_color(color)
            .child(text)
    }

    fn render_save(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let preview = match &self.mode {
            PanelMode::Save { sql } => sql.clone(),
            PanelMode::Browse => String::new(),
        };

        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .gap(HistoryPanelMetrics::SECTION_GAP)
            .px(HistoryPanelMetrics::PADDING_X)
            .child(Self::query_line(preview, theme.foreground))
            .child(Input::new(&self.save_name_input).small().w_full())
            .child(Self::meta_line(
                dbflux_i18n::t!("document.shared.hint.enter_save_esc_cancel"),
                theme.muted_foreground,
            ))
            .into_any_element()
    }
}

/// Meta line of a recent query (IslEditor): how long ago it ran, the rows it
/// returned when known, and how long it took, joined by a middle dot.
fn history_entry_meta(entry: &HistoryEntry, now: i64) -> String {
    let mut parts = vec![history_age_label(now - entry.timestamp)];

    if let Some(rows) = entry.row_count {
        parts.push(crate::labels::row_count_label(rows));
    }

    parts.push(dbflux_i18n::t!(
        "document.code.history.duration_ms",
        count = entry.execution_time_ms
    ));

    parts.join(" · ")
}

/// How long ago a query ran, in the coarsest whole unit: "now" under a
/// minute, then minutes, hours and days.
fn history_age_label(elapsed_seconds: i64) -> String {
    const MINUTE: i64 = 60;
    const HOUR: i64 = 60 * MINUTE;
    const DAY: i64 = 24 * HOUR;

    let elapsed = elapsed_seconds.max(0);

    if elapsed < MINUTE {
        dbflux_i18n::t!("document.code.history.age.now")
    } else if elapsed < HOUR {
        dbflux_i18n::t!(
            "document.code.history.age.minutes",
            count = elapsed / MINUTE
        )
    } else if elapsed < DAY {
        dbflux_i18n::t!("document.code.history.age.hours", count = elapsed / HOUR)
    } else {
        dbflux_i18n::t!("document.code.history.age.days", count = elapsed / DAY)
    }
}

impl Focusable for HistoryPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for HistoryPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let body = match self.mode {
            PanelMode::Browse => self.render_browse(cx),
            PanelMode::Save { .. } => self.render_save(cx),
        };

        div()
            .id("history-panel")
            .key_context(ContextId::HistoryModal.as_gpui_context())
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    if !this.focus_handle.contains_focused(window, cx) {
                        this.focus_handle.focus(window, cx);
                    }
                }),
            )
            .on_action(cx.listener(|this, _: &SelectNext, _, cx| {
                this.select_next(cx);
            }))
            .on_action(cx.listener(|this, _: &SelectPrev, _, cx| {
                this.select_prev(cx);
            }))
            .on_action(cx.listener(|this, _: &Execute, window, cx| {
                this.execute_selected(window, cx);
            }))
            .on_action(cx.listener(|this, _: &Delete, _, cx| {
                this.delete_selected(cx);
            }))
            .on_action(cx.listener(|this, _: &ToggleFavorite, _, cx| {
                this.toggle_favorite_selected(cx);
            }))
            .on_action(cx.listener(|this, _: &Rename, window, cx| {
                this.start_rename_selected(window, cx);
            }))
            .on_action(cx.listener(|this, _: &FocusSearch, window, cx| {
                this.focus_search(window, cx);
            }))
            .on_action(cx.listener(|this, _: &SaveQuery, window, cx| {
                this.save_selected_history(window, cx);
            }))
            .on_action(cx.listener(|this, _: &Cancel, window, cx| {
                this.cancel(window, cx);
            }))
            .child(self.render_header(cx))
            .child(body)
    }
}

impl EventEmitter<HistoryQuerySelected> for HistoryPanel {}
impl EventEmitter<HistoryPanelClosed> for HistoryPanel {}
impl EventEmitter<QuerySaved> for HistoryPanel {}

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

#[cfg(test)]
mod panel_tests {
    use super::{
        HistoryPanel, HistoryPanelCallbacks, HistoryPanelClosed, HistoryQuerySelected, PanelMode,
        history_age_label, history_entry_meta,
    };
    use dbflux_components::theme;
    use dbflux_core::HistoryEntry;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, Focusable as _, TestAppContext, VisualTestContext};
    use gpui_component::Root;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    fn entry(sql: &str, rows: Option<usize>, millis: u64) -> HistoryEntry {
        HistoryEntry::new(
            sql.to_string(),
            None,
            None,
            Duration::from_millis(millis),
            rows,
        )
    }

    fn callbacks(entries: Vec<HistoryEntry>) -> HistoryPanelCallbacks {
        HistoryPanelCallbacks {
            history_provider: Box::new(move |_| entries.clone()),
            saved_provider: Box::new(|_| Vec::new()),
            on_save: Box::new(|_, _| {}),
            on_rename: Box::new(|_, _, _, _| {}),
            on_delete: Box::new(|_, _| {}),
            on_toggle_favorite: Box::new(|_, _| {}),
            on_mark_used: Box::new(|_, _| {}),
        }
    }

    struct Harness<'a> {
        window: &'a mut VisualTestContext,
        panel: Entity<HistoryPanel>,
        selected: Rc<RefCell<Vec<String>>>,
        closed: Rc<RefCell<usize>>,
    }

    /// A window whose root is the history panel, so its focus handle is in
    /// the rendered tree the way the workspace island puts it there.
    fn harness(cx: &mut TestAppContext) -> Harness<'_> {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_| ToastHost::new());
            cx.set_global(ToastGlobal { host });
        });

        let panel_holder: Rc<RefCell<Option<Entity<HistoryPanel>>>> = Rc::new(RefCell::new(None));
        let holder = panel_holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let panel = cx.new(|cx| {
                HistoryPanel::new(
                    callbacks(vec![
                        entry("SELECT 1", Some(50), 38),
                        entry("SELECT 2", None, 12),
                    ]),
                    window,
                    cx,
                )
            });
            holder.replace(Some(panel.clone()));
            Root::new(panel, window, cx)
        });

        let panel = panel_holder.borrow().clone().expect("panel is created");
        let selected: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let closed = Rc::new(RefCell::new(0));

        window.update(|_, cx| {
            let sink = selected.clone();
            cx.subscribe(&panel, move |_, event: &HistoryQuerySelected, _| {
                sink.borrow_mut().push(event.sql.clone());
            })
            .detach();

            let sink = closed.clone();
            cx.subscribe(&panel, move |_, _: &HistoryPanelClosed, _| {
                *sink.borrow_mut() += 1;
            })
            .detach();
        });

        Harness {
            window,
            panel,
            selected,
            closed,
        }
    }

    /// Loading an entry keeps the island open next to the editor; the modal
    /// used to close on every pick.
    #[gpui::test]
    fn loading_an_entry_keeps_the_panel_open(cx: &mut TestAppContext) {
        let harness = harness(cx);
        let panel = harness.panel.clone();

        harness.window.update(|window, cx| {
            panel.update(cx, |panel, cx| {
                panel.open(window, cx);
                panel.select_next(cx);
                panel.execute_selected(window, cx);
            });
        });
        harness.window.run_until_parked();

        assert_eq!(harness.selected.borrow().as_slice(), ["SELECT 2"]);
        assert_eq!(*harness.closed.borrow(), 0);
        assert!(harness.window.update(|_, cx| panel.read(cx).is_visible()));
    }

    /// Esc leaves the save form for the list first, and closes the panel
    /// only from the list.
    #[gpui::test]
    fn escape_leaves_the_save_form_before_closing(cx: &mut TestAppContext) {
        let harness = harness(cx);
        let panel = harness.panel.clone();

        harness.window.update(|window, cx| {
            panel.update(cx, |panel, cx| {
                panel.open(window, cx);
                panel.save_selected_history(window, cx);
            });
        });

        let saving = harness.window.update(
            |_, cx| matches!(panel.read(cx).mode, PanelMode::Save { ref sql } if sql == "SELECT 1"),
        );
        assert!(saving, "Ctrl+S on a recent entry opens the save form");

        harness.window.update(|window, cx| {
            panel.update(cx, |panel, cx| panel.cancel(window, cx));
        });
        let (browsing, visible) = harness.window.update(|_, cx| {
            let panel = panel.read(cx);
            (matches!(panel.mode, PanelMode::Browse), panel.is_visible())
        });
        assert!(browsing && visible, "the first Esc returns to the list");

        harness.window.update(|window, cx| {
            panel.update(cx, |panel, cx| panel.cancel(window, cx));
        });
        harness.window.run_until_parked();

        assert!(!harness.window.update(|_, cx| panel.read(cx).is_visible()));
        assert_eq!(*harness.closed.borrow(), 1);
    }

    /// Draws the window, which is where focus changes reach their listeners.
    fn redraw(window: &mut VisualTestContext) {
        window.update(|window, _| window.activate_window());
        window.run_until_parked();
        window.update(|window, _| window.refresh());
        window.run_until_parked();
    }

    /// The panel owns the keyboard only while focus is inside it, so an open
    /// history never takes keys from the editor beside it.
    #[gpui::test]
    fn the_panel_owns_the_keyboard_only_while_focused(cx: &mut TestAppContext) {
        let harness = harness(cx);
        let panel = harness.panel.clone();

        harness.window.update(|window, cx| {
            panel.update(cx, |panel, cx| panel.open(window, cx));
        });
        redraw(harness.window);

        assert!(
            harness
                .window
                .update(|_, cx| panel.read(cx).owns_keyboard())
        );

        let elsewhere = harness.window.update(|_, cx| cx.focus_handle());
        harness.window.update(|window, cx| {
            elsewhere.focus(window, cx);
        });
        redraw(harness.window);

        let (visible, owns_keyboard) = harness.window.update(|_, cx| {
            let panel = panel.read(cx);
            (panel.is_visible(), panel.owns_keyboard())
        });
        assert!(visible && !owns_keyboard);

        harness.window.update(|window, cx| {
            panel.read(cx).focus_handle(cx).focus(window, cx);
        });
        redraw(harness.window);

        assert!(
            harness
                .window
                .update(|_, cx| panel.read(cx).owns_keyboard())
        );
    }

    #[test]
    fn ages_use_the_coarsest_whole_unit() {
        assert_eq!(history_age_label(0), "now");
        assert_eq!(history_age_label(59), "now");
        assert_eq!(history_age_label(-5), "now");
        assert_eq!(history_age_label(60 * 5), "5 min ago");
        assert_eq!(history_age_label(60 * 60 * 3 + 10), "3 h ago");
        assert_eq!(history_age_label(60 * 60 * 24 * 2), "2 d ago");
    }

    #[test]
    fn the_meta_line_reads_age_rows_and_duration() {
        let with_rows = entry("SELECT 1", Some(50), 38);
        let without_rows = entry("SELECT 2", None, 12);

        assert_eq!(
            history_entry_meta(&with_rows, with_rows.timestamp),
            "now · 50 rows · 38 ms"
        );
        assert_eq!(
            history_entry_meta(&without_rows, without_rows.timestamp + 120),
            "2 min ago · 12 ms"
        );
    }
}
