#![allow(dead_code)]

use super::tab_manager::TabManager;
use super::types::{DocumentId, DocumentMetaSnapshot, DocumentState};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

pub struct TabBar {
    tab_manager: Entity<TabManager>,
    focus_handle: FocusHandle,

    // Drag state (for future drag & drop support)
    dragging_tab: Option<DocumentId>,
    drop_target_index: Option<usize>,
}

impl TabBar {
    pub fn new(tab_manager: Entity<TabManager>, cx: &mut Context<Self>) -> Self {
        Self {
            tab_manager,
            focus_handle: cx.focus_handle(),
            dragging_tab: None,
            drop_target_index: None,
        }
    }
}

impl Render for TabBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let manager = self.tab_manager.read(cx);
        let active_id = manager.active_id();
        let drop_target_index = self.drop_target_index;

        // Pre-calculate snapshots to avoid multiple reads
        let tab_snapshots: Vec<_> = manager
            .documents()
            .iter()
            .map(|doc| doc.meta_snapshot(cx))
            .collect();

        // Pre-render all tabs using for loop to avoid closure borrow issues
        let mut tabs: Vec<AnyElement> = Vec::with_capacity(tab_snapshots.len());
        for (idx, meta) in tab_snapshots.into_iter().enumerate() {
            tabs.push(
                self.render_tab(meta, idx, active_id, drop_target_index, cx)
                    .into_any_element(),
            );
        }

        // Cache theme colors before building the div
        let tab_bar_bg = cx.theme().tab_bar;
        let border_color = cx.theme().border;

        // Build new tab button
        let new_tab_btn = self.render_new_tab_button(cx).into_any_element();

        div()
            .id("tab-bar")
            .h(px(36.0))
            .w_full()
            .flex()
            .items_center()
            .bg(tab_bar_bg)
            .border_b_1()
            .border_color(border_color)
            // Scrollable tabs container
            .child(
                div()
                    .flex()
                    .flex_1()
                    .overflow_x_hidden()
                    .gap_px()
                    .children(tabs),
            )
            // New tab button
            .child(new_tab_btn)
    }
}

impl TabBar {
    fn render_tab(
        &self,
        meta: DocumentMetaSnapshot,
        idx: usize,
        active_id: Option<DocumentId>,
        drop_target_index: Option<usize>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let id = meta.id;
        let is_active = active_id == Some(id);
        let is_modified = meta.state == DocumentState::Modified;
        let is_executing = meta.state == DocumentState::Executing;
        let is_drop_target = drop_target_index == Some(idx);

        let title = if is_modified {
            format!("{}*", meta.title)
        } else {
            meta.title.clone()
        };

        let tab_manager = self.tab_manager.clone();

        let icon_path = match meta.icon {
            super::types::DocumentIcon::Sql => AppIcon::Code.path(),
            super::types::DocumentIcon::Table => AppIcon::Table.path(),
            super::types::DocumentIcon::Redis => AppIcon::Database.path(),
            super::types::DocumentIcon::RedisKey => AppIcon::Hash.path(),
            super::types::DocumentIcon::Terminal => AppIcon::SquareTerminal.path(),
            super::types::DocumentIcon::Mongo => AppIcon::Database.path(),
            super::types::DocumentIcon::Collection => AppIcon::Folder.path(),
        };

        div()
            .id(ElementId::Name(format!("tab-{}", id.0).into()))
            .h_full()
            .min_w(px(100.0))
            .max_w(px(200.0))
            .px(Spacing::MD)
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .cursor_pointer()
            // Styles based on state
            .when(is_active, |el| {
                el.bg(cx.theme().tab_bar)
                    .border_b_2()
                    .border_color(cx.theme().accent)
            })
            .when(!is_active, |el| {
                el.bg(cx.theme().background)
                    .hover(|el| el.bg(cx.theme().secondary))
            })
            .when(is_drop_target, |el| {
                el.border_l_2().border_color(cx.theme().accent)
            })
            // Click to activate
            .on_click({
                let tab_manager = tab_manager.clone();
                cx.listener(move |_this, _event, _window, cx| {
                    tab_manager.update(cx, |mgr, cx| {
                        mgr.activate(id, cx);
                    });
                })
            })
            // Middle-click to close
            .on_mouse_down(MouseButton::Middle, {
                let tab_manager = tab_manager.clone();
                cx.listener(move |_this, _event, _window, cx| {
                    tab_manager.update(cx, |mgr, cx| {
                        mgr.close(id, cx);
                    });
                })
            })
            // Icon
            .child(svg().path(icon_path).size_3().text_color(if is_active {
                cx.theme().accent
            } else {
                cx.theme().muted_foreground
            }))
            // Title
            .child(
                div()
                    .text_sm()
                    .flex_1()
                    .truncate()
                    .text_color(if is_active {
                        cx.theme().foreground
                    } else {
                        cx.theme().muted_foreground
                    })
                    .child(title),
            )
            // Spinner or close button
            .child(self.render_tab_action(id, is_executing, is_modified, cx))
    }

    fn render_tab_action(
        &self,
        id: DocumentId,
        is_executing: bool,
        is_modified: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        // Cache theme colors before creating closures
        let accent = cx.theme().accent;
        let warning = cx.theme().warning;
        let secondary = cx.theme().secondary;
        let muted_fg = cx.theme().muted_foreground;

        div()
            .w(px(16.0))
            .h(px(16.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(Radii::SM)
            .child(if is_executing {
                // Loader icon while executing
                svg()
                    .path(AppIcon::Loader.path())
                    .size(px(12.0))
                    .text_color(accent)
                    .into_any_element()
            } else if is_modified {
                // Dot to indicate modified
                div()
                    .w(px(8.0))
                    .h(px(8.0))
                    .rounded_full()
                    .bg(warning)
                    .into_any_element()
            } else {
                // Close button
                div()
                    .id(ElementId::Name(format!("tab-close-{}", id.0).into()))
                    .w(px(16.0))
                    .h(px(16.0))
                    .rounded(Radii::SM)
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(move |el| el.bg(secondary))
                    .child(
                        svg()
                            .path(AppIcon::X.path())
                            .size(px(12.0))
                            .text_color(muted_fg),
                    )
                    .on_click(cx.listener(move |this, _event: &ClickEvent, _window, cx| {
                        this.tab_manager.update(cx, |mgr, cx| {
                            mgr.close(id, cx);
                        });
                    }))
                    .into_any_element()
            })
    }

    fn render_new_tab_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("new-tab-btn")
            .w(px(32.0))
            .h_full()
            .flex()
            .items_center()
            .justify_center()
            .cursor_pointer()
            .hover(|el| el.bg(cx.theme().secondary))
            .child(
                svg()
                    .path(AppIcon::Plus.path())
                    .size(px(14.0))
                    .text_color(cx.theme().muted_foreground),
            )
            .on_click(cx.listener(|_this, _event, _window, cx| {
                cx.emit(TabBarEvent::NewTabRequested);
            }))
    }
}

impl EventEmitter<TabBarEvent> for TabBar {}

#[derive(Clone, Debug)]
pub enum TabBarEvent {
    NewTabRequested,
}
