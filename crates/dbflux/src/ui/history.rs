use crate::app::AppState;
use crate::ui::editor::EditorPane;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use dbflux_core::HistoryEntry;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use uuid::Uuid;

pub struct HistoryPanel {
    app_state: Entity<AppState>,
    editor: Entity<EditorPane>,
    search_input: Entity<InputState>,
    search_query: String,
    is_collapsed: bool,
    pending_load_sql: Option<String>,
}

impl HistoryPanel {
    pub fn new(
        app_state: Entity<AppState>,
        editor: Entity<EditorPane>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search history..."));

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

        Self {
            app_state,
            editor,
            search_input,
            search_query: String::new(),
            is_collapsed: true,
            pending_load_sql: None,
        }
    }

    fn toggle_collapsed(&mut self, cx: &mut Context<Self>) {
        self.is_collapsed = !self.is_collapsed;
        cx.notify();
    }

    fn load_query(&mut self, sql: String, cx: &mut Context<Self>) {
        self.pending_load_sql = Some(sql);
        cx.notify();
    }

    fn toggle_favorite(&mut self, id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, _cx| {
            state.toggle_history_favorite(id);
        });
        cx.notify();
    }

    fn remove_entry(&mut self, id: Uuid, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, _cx| {
            state.remove_history_entry(id);
        });
        cx.notify();
    }

    fn filtered_entries(&self, entries: &[HistoryEntry]) -> Vec<HistoryEntry> {
        const MAX_VISIBLE: usize = 50;

        if self.search_query.is_empty() {
            entries.iter().take(MAX_VISIBLE).cloned().collect()
        } else {
            let query = self.search_query.to_lowercase();
            entries
                .iter()
                .filter(|e| e.sql.to_lowercase().contains(&query))
                .take(MAX_VISIBLE)
                .cloned()
                .collect()
        }
    }
}

impl Render for HistoryPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(sql) = self.pending_load_sql.take() {
            self.editor.update(cx, |editor, cx| {
                editor.set_query(&sql, window, cx);
            });
        }

        let theme = cx.theme();
        let is_collapsed = self.is_collapsed;
        let search_input = self.search_input.clone();

        let (entry_count, filtered): (usize, Vec<HistoryEntry>) = if is_collapsed {
            (self.app_state.read(cx).history_entries().len(), Vec::new())
        } else {
            let entries = self.app_state.read(cx).history_entries();
            let filtered = self.filtered_entries(entries);
            (filtered.len(), filtered)
        };

        div()
            .flex()
            .flex_col()
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .id("history-header")
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::SM)
                    .h(Heights::ROW)
                    .cursor_pointer()
                    .hover(|d| d.bg(theme.secondary))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle_collapsed(cx);
                    }))
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.muted_foreground)
                                    .child(if is_collapsed { "▸" } else { "▾" }),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::SM)
                                    .font_weight(FontWeight::MEDIUM)
                                    .text_color(theme.muted_foreground)
                                    .child(format!("HISTORY ({})", entry_count)),
                            ),
                    ),
            )
            .when(!is_collapsed, |d| {
                d.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .child(Input::new(&search_input).small().w_full()),
                )
                .child(div().flex_1().overflow_hidden().max_h(px(300.0)).children(
                    filtered.into_iter().map(|entry| {
                        let entry_id = entry.id;
                        let sql = entry.sql.clone();
                        let is_favorite = entry.is_favorite;

                        div()
                            .id(ElementId::Name(format!("history-{}", entry_id).into()))
                            .flex()
                            .flex_col()
                            .px(Spacing::SM)
                            .py(Spacing::XS)
                            .border_b_1()
                            .border_color(theme.border)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.load_query(sql.clone(), cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .child(
                                        div()
                                            .flex_1()
                                            .text_size(FontSizes::SM)
                                            .text_color(theme.foreground)
                                            .overflow_hidden()
                                            .text_ellipsis()
                                            .child(entry.sql_preview(50)),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap(Spacing::XS)
                                            .child(
                                                div()
                                                    .id(ElementId::Name(
                                                        format!("fav-{}", entry_id).into(),
                                                    ))
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
                                                        this.toggle_favorite(entry_id, cx);
                                                    }))
                                                    .child(if is_favorite { "★" } else { "☆" }),
                                            )
                                            .child(
                                                div()
                                                    .id(ElementId::Name(
                                                        format!("del-{}", entry_id).into(),
                                                    ))
                                                    .w(Heights::ICON_SM)
                                                    .h(Heights::ICON_SM)
                                                    .flex()
                                                    .items_center()
                                                    .justify_center()
                                                    .rounded(Radii::SM)
                                                    .text_size(FontSizes::SM)
                                                    .text_color(theme.muted_foreground)
                                                    .hover(|d: StyleRefinement| {
                                                        d.bg(theme.secondary)
                                                            .text_color(gpui::rgb(0xEF4444))
                                                    })
                                                    .on_click(cx.listener(move |this, _, _, cx| {
                                                        this.remove_entry(entry_id, cx);
                                                    }))
                                                    .child("×"),
                                            ),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::SM)
                                    .mt(Spacing::XS)
                                    .child(
                                        div()
                                            .text_color(theme.muted_foreground)
                                            .text_size(FontSizes::XS)
                                            .child(entry.formatted_timestamp()),
                                    )
                                    .when_some(entry.row_count, |d, count| {
                                        d.child(
                                            div()
                                                .text_color(theme.muted_foreground)
                                                .text_size(FontSizes::XS)
                                                .child(format!("{} rows", count)),
                                        )
                                    })
                                    .child(
                                        div()
                                            .text_color(theme.muted_foreground)
                                            .text_size(FontSizes::XS)
                                            .child(format!("{}ms", entry.execution_time_ms)),
                                    ),
                            )
                    }),
                ))
                .when(entry_count == 0, |d| {
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
            })
    }
}
