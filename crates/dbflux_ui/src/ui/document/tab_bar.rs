use std::cell::Cell;
use std::rc::Rc;

use super::tab_manager::TabManager;
use super::types::{DocumentId, DocumentMetaSnapshot, DocumentState};
use crate::ui::components::context_menu::MenuItem;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{Radii, Spacing};
use dbflux_components::primitives::{Icon, Text};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

const TAB_BAR_HEIGHT: Pixels = px(36.0);

#[allow(dead_code)]
pub struct TabBar {
    tab_manager: Entity<TabManager>,
    focus_handle: FocusHandle,

    context_menu: Option<TabContextMenu>,

    /// Center X of the active tab, updated each render via canvas measurement.
    active_tab_center_x: Rc<Cell<Pixels>>,

    // Drag state (for future drag & drop support)
    dragging_tab: Option<DocumentId>,
    drop_target_index: Option<usize>,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct TabContextMenu {
    pub tab_id: DocumentId,
    pub tab_index: usize,
    /// X position from the mouse click (window-absolute).
    pub position_x: Pixels,
    pub selected_index: usize,
}

pub const TAB_MENU_CLOSE: usize = 0;
pub const TAB_MENU_CLOSE_OTHERS: usize = 1;
pub const TAB_MENU_CLOSE_ALL: usize = 2;
#[allow(dead_code)]
pub const TAB_MENU_SEPARATOR: usize = 3;
pub const TAB_MENU_CLOSE_LEFT: usize = 4;
pub const TAB_MENU_CLOSE_RIGHT: usize = 5;

impl TabBar {
    pub fn new(tab_manager: Entity<TabManager>, cx: &mut Context<Self>) -> Self {
        Self {
            tab_manager,
            focus_handle: cx.focus_handle(),
            context_menu: None,
            active_tab_center_x: Rc::new(Cell::new(px(0.0))),
            dragging_tab: None,
            drop_target_index: None,
        }
    }

    pub fn context_menu_state(&self) -> Option<&TabContextMenu> {
        self.context_menu.as_ref()
    }

    pub fn close_context_menu(&mut self, cx: &mut Context<Self>) {
        self.context_menu = None;
        cx.notify();
    }

    pub fn context_menu_hover_at(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(ref mut menu) = self.context_menu
            && menu.selected_index != index
        {
            menu.selected_index = index;
            cx.notify();
        }
    }

    pub fn context_menu_execute_at(&mut self, action_index: usize, cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.take() else {
            return;
        };

        let tab_id = menu.tab_id;

        match action_index {
            TAB_MENU_CLOSE => cx.emit(TabBarEvent::CloseTab(tab_id)),
            TAB_MENU_CLOSE_OTHERS => cx.emit(TabBarEvent::CloseOtherTabs(tab_id)),
            TAB_MENU_CLOSE_ALL => cx.emit(TabBarEvent::CloseAllTabs),
            TAB_MENU_CLOSE_LEFT => cx.emit(TabBarEvent::CloseTabsToLeft(tab_id)),
            TAB_MENU_CLOSE_RIGHT => cx.emit(TabBarEvent::CloseTabsToRight(tab_id)),
            _ => {}
        }

        cx.notify();
    }

    pub fn build_tab_menu_items() -> Vec<MenuItem> {
        vec![
            MenuItem::new("Close").icon(AppIcon::X),
            MenuItem::new("Close Others").icon(AppIcon::X),
            MenuItem::new("Close All").icon(AppIcon::X),
            MenuItem::separator(),
            MenuItem::new("Close to the Left").icon(AppIcon::ChevronLeft),
            MenuItem::new("Close to the Right").icon(AppIcon::ChevronRight),
        ]
    }

    pub fn has_context_menu_open(&self) -> bool {
        self.context_menu.is_some()
    }

    pub fn open_context_menu_for_active(&mut self, cx: &mut Context<Self>) {
        let manager = self.tab_manager.read(cx);
        let Some(active_id) = manager.active_id() else {
            return;
        };

        let active_index = manager
            .documents()
            .iter()
            .position(|d| d.id() == active_id)
            .unwrap_or(0);

        self.context_menu = Some(TabContextMenu {
            tab_id: active_id,
            tab_index: active_index,
            position_x: self.active_tab_center_x.get(),
            selected_index: 0,
        });
        cx.notify();
    }

    pub fn context_menu_select_next(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let items = Self::build_tab_menu_items();
        menu.selected_index = next_actionable_index(menu.selected_index, &items);
        cx.notify();
    }

    pub fn context_menu_select_prev(&mut self, cx: &mut Context<Self>) {
        let Some(ref mut menu) = self.context_menu else {
            return;
        };

        let items = Self::build_tab_menu_items();
        menu.selected_index = prev_actionable_index(menu.selected_index, &items);
        cx.notify();
    }

    pub fn context_menu_execute(&mut self, cx: &mut Context<Self>) {
        let Some(menu) = &self.context_menu else {
            return;
        };

        self.context_menu_execute_at(menu.selected_index, cx);
    }
}

/// Returns the next non-separator index after `current`, or `current` if at the end.
pub fn next_actionable_index(current: usize, items: &[MenuItem]) -> usize {
    let mut idx = current + 1;
    while idx < items.len() {
        if !items[idx].is_separator {
            return idx;
        }
        idx += 1;
    }
    current
}

/// Returns the previous non-separator index before `current`, or `current` if at the start.
pub fn prev_actionable_index(current: usize, items: &[MenuItem]) -> usize {
    if current == 0 {
        return current;
    }

    let mut idx = current - 1;
    loop {
        if !items[idx].is_separator {
            return idx;
        }
        if idx == 0 {
            return current;
        }
        idx -= 1;
    }
}

impl Render for TabBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let manager = self.tab_manager.read(cx);
        let active_id = manager.active_id();
        let drop_target_index = self.drop_target_index;

        let tab_snapshots: Vec<_> = manager
            .documents()
            .iter()
            .map(|doc| doc.meta_snapshot(cx))
            .collect();

        let mut tabs: Vec<AnyElement> = Vec::with_capacity(tab_snapshots.len());
        for (idx, meta) in tab_snapshots.into_iter().enumerate() {
            tabs.push(
                self.render_tab(meta, idx, active_id, drop_target_index, cx)
                    .into_any_element(),
            );
        }

        let tab_bar_bg = cx.theme().tab_bar;
        let border_color = cx.theme().border;
        let new_tab_btn = self.render_new_tab_button(cx).into_any_element();

        div()
            .id("tab-bar")
            .h(TAB_BAR_HEIGHT)
            .w_full()
            .flex()
            .items_center()
            .bg(tab_bar_bg)
            .border_b_1()
            .border_color(border_color)
            .child(
                div()
                    .flex()
                    .items_center()
                    .overflow_x_hidden()
                    .gap_px()
                    .children(tabs)
                    .child(new_tab_btn),
            )
            .child(div().flex_1())
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
        let is_executing = meta.state == DocumentState::Executing;
        let is_drop_target = drop_target_index == Some(idx);

        let title = meta.title.clone();

        let tab_manager = self.tab_manager.clone();

        let icon = match meta.icon {
            super::types::DocumentIcon::Sql => AppIcon::Code,
            super::types::DocumentIcon::Table => AppIcon::Table,
            super::types::DocumentIcon::Redis => AppIcon::Database,
            super::types::DocumentIcon::RedisKey => AppIcon::Hash,
            super::types::DocumentIcon::Terminal => AppIcon::SquareTerminal,
            super::types::DocumentIcon::Mongo => AppIcon::Database,
            super::types::DocumentIcon::Collection => AppIcon::Folder,
            super::types::DocumentIcon::Script => AppIcon::ScrollText,
            super::types::DocumentIcon::Audit => AppIcon::ScrollText,
        };

        let center_x = self.active_tab_center_x.clone();

        div()
            .id(ElementId::Name(format!("tab-{}", id.0).into()))
            .relative()
            .h_full()
            .min_w(px(100.0))
            .max_w(px(200.0))
            .px(Spacing::MD)
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .cursor_pointer()
            .when(is_active, |el| {
                el.bg(cx.theme().tab_bar)
                    .border_b_2()
                    .border_color(cx.theme().accent)
                    .child(
                        canvas(
                            move |bounds: Bounds<Pixels>, _, _| {
                                center_x.set(bounds.center().x);
                            },
                            |_, _, _, _| {},
                        )
                        .absolute()
                        .size_full(),
                    )
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
            // Right-click for context menu
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, _window, cx| {
                    this.context_menu = Some(TabContextMenu {
                        tab_id: id,
                        tab_index: idx,
                        position_x: event.position.x,
                        selected_index: 0,
                    });
                    cx.notify();
                }),
            )
            // Icon
            .child(Icon::new(icon).size(px(16.0)).color(if is_active {
                cx.theme().foreground
            } else {
                cx.theme().muted_foreground
            }))
            // Title
            .child(div().flex_1().truncate().child(if is_active {
                Text::caption(title).color(cx.theme().foreground)
            } else {
                Text::caption(title)
            }))
            // Spinner or close button
            .child(self.render_tab_action(id, is_executing, cx))
    }

    fn render_tab_action(
        &self,
        id: DocumentId,
        is_executing: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let accent = cx.theme().accent;
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
                Icon::new(AppIcon::Loader)
                    .size(px(12.0))
                    .color(accent)
                    .into_any_element()
            } else {
                div()
                    .id(ElementId::Name(format!("tab-close-{}", id.0).into()))
                    .w(px(16.0))
                    .h(px(16.0))
                    .rounded(Radii::SM)
                    .flex()
                    .items_center()
                    .justify_center()
                    .hover(move |el| el.bg(secondary))
                    .child(Icon::new(AppIcon::X).size(px(12.0)).color(muted_fg))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _event, _window, cx| {
                            cx.stop_propagation();
                            this.tab_manager.update(cx, |mgr, cx| {
                                mgr.close(id, cx);
                            });
                        }),
                    )
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
            .child(Icon::new(AppIcon::Plus).size(px(14.0)).muted())
            .on_click(cx.listener(|_this, _event, _window, cx| {
                cx.emit(TabBarEvent::NewTabRequested);
            }))
    }
}

impl EventEmitter<TabBarEvent> for TabBar {}

#[derive(Clone, Debug)]
pub enum TabBarEvent {
    NewTabRequested,
    CloseTab(DocumentId),
    CloseOtherTabs(DocumentId),
    CloseAllTabs,
    CloseTabsToLeft(DocumentId),
    CloseTabsToRight(DocumentId),
}

#[cfg(test)]
mod tests {
    use super::{
        TAB_MENU_CLOSE, TAB_MENU_CLOSE_ALL, TAB_MENU_CLOSE_LEFT, TAB_MENU_CLOSE_OTHERS,
        TAB_MENU_CLOSE_RIGHT, TAB_MENU_SEPARATOR, TabBar, next_actionable_index,
        prev_actionable_index,
    };

    #[test]
    fn build_tab_menu_items_returns_correct_structure() {
        let items = TabBar::build_tab_menu_items();

        assert_eq!(items.len(), 6);
        assert_eq!(items[TAB_MENU_CLOSE].label.as_ref(), "Close");
        assert_eq!(items[TAB_MENU_CLOSE_OTHERS].label.as_ref(), "Close Others");
        assert_eq!(items[TAB_MENU_CLOSE_ALL].label.as_ref(), "Close All");
        assert!(items[TAB_MENU_SEPARATOR].is_separator);
        assert_eq!(
            items[TAB_MENU_CLOSE_LEFT].label.as_ref(),
            "Close to the Left"
        );
        assert_eq!(
            items[TAB_MENU_CLOSE_RIGHT].label.as_ref(),
            "Close to the Right"
        );
    }

    #[test]
    fn build_tab_menu_items_have_icons() {
        let items = TabBar::build_tab_menu_items();

        for (idx, item) in items.iter().enumerate() {
            if item.is_separator {
                assert!(item.icon.is_none(), "separator should have no icon");
            } else {
                assert!(item.icon.is_some(), "item {} should have an icon", idx);
            }
        }
    }

    #[test]
    fn no_tab_menu_items_are_danger_or_submenu() {
        let items = TabBar::build_tab_menu_items();

        for item in &items {
            assert!(!item.is_danger);
            assert!(!item.has_submenu);
        }
    }

    #[test]
    fn next_actionable_skips_separator() {
        let items = TabBar::build_tab_menu_items();

        // 0 -> 1 -> 2 -> 4 (skip separator at 3) -> 5
        assert_eq!(next_actionable_index(0, &items), 1);
        assert_eq!(next_actionable_index(1, &items), 2);
        assert_eq!(next_actionable_index(2, &items), 4);
        assert_eq!(next_actionable_index(4, &items), 5);
    }

    #[test]
    fn next_actionable_stays_at_end() {
        let items = TabBar::build_tab_menu_items();
        assert_eq!(next_actionable_index(5, &items), 5);
    }

    #[test]
    fn prev_actionable_skips_separator() {
        let items = TabBar::build_tab_menu_items();

        // 5 -> 4 -> 2 (skip separator at 3) -> 1 -> 0
        assert_eq!(prev_actionable_index(5, &items), 4);
        assert_eq!(prev_actionable_index(4, &items), 2);
        assert_eq!(prev_actionable_index(2, &items), 1);
        assert_eq!(prev_actionable_index(1, &items), 0);
    }

    #[test]
    fn prev_actionable_stays_at_start() {
        let items = TabBar::build_tab_menu_items();
        assert_eq!(prev_actionable_index(0, &items), 0);
    }
}
