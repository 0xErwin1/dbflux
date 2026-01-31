use crate::ui::icons::AppIcon;
use crate::ui::tasks_panel::TasksPanel;
use crate::ui::tokens::{Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

const MIN_HEIGHT: Pixels = px(100.0);
const MAX_HEIGHT: Pixels = px(400.0);
const DEFAULT_HEIGHT: Pixels = px(200.0);
const GRIP_HEIGHT: Pixels = px(4.0);

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BottomDockTab {
    Tasks,
    Output,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum BottomDockState {
    #[default]
    Collapsed,
    Expanded,
}

pub struct BottomDock {
    tasks_panel: Entity<TasksPanel>,
    active_tab: BottomDockTab,
    state: BottomDockState,
    height: Pixels,
    last_height: Pixels,

    is_resizing: bool,
    resize_start_y: Option<Pixels>,
    resize_start_height: Option<Pixels>,
}

impl BottomDock {
    pub fn new(tasks_panel: Entity<TasksPanel>, _cx: &mut Context<Self>) -> Self {
        Self {
            tasks_panel,
            active_tab: BottomDockTab::Tasks,
            state: BottomDockState::Collapsed,
            height: DEFAULT_HEIGHT,
            last_height: DEFAULT_HEIGHT,
            is_resizing: false,
            resize_start_y: None,
            resize_start_height: None,
        }
    }

    pub fn toggle(&mut self, cx: &mut Context<Self>) {
        match self.state {
            BottomDockState::Expanded => {
                self.last_height = self.height;
                self.state = BottomDockState::Collapsed;
            }
            BottomDockState::Collapsed => {
                self.state = BottomDockState::Expanded;
                self.height = self.last_height;
            }
        }
        cx.notify();
    }

    pub fn show(&mut self, tab: BottomDockTab, cx: &mut Context<Self>) {
        self.active_tab = tab;
        if self.state == BottomDockState::Collapsed {
            self.state = BottomDockState::Expanded;
        }
        cx.notify();
    }

    pub fn is_visible(&self) -> bool {
        self.state == BottomDockState::Expanded
    }

    pub fn tasks_panel(&self) -> &Entity<TasksPanel> {
        &self.tasks_panel
    }
}

impl Render for BottomDock {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.is_visible() {
            return div().into_any_element();
        }

        div()
            .id("bottom-dock")
            .w_full()
            .h(self.height)
            .flex()
            .flex_col()
            .border_t_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().tab_bar)
            .child(self.render_grip(window, cx))
            .child(self.render_tab_bar(window, cx))
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .child(match self.active_tab {
                        BottomDockTab::Tasks => self.tasks_panel.clone().into_any_element(),
                        BottomDockTab::Output => self.render_output(cx).into_any_element(),
                    }),
            )
            .into_any_element()
    }
}

impl BottomDock {
    fn render_grip(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("bottom-dock-grip")
            .w_full()
            .h(GRIP_HEIGHT)
            .cursor_row_resize()
            .hover(|el| el.bg(cx.theme().accent.opacity(0.3)))
            .when(self.is_resizing, |el| el.bg(cx.theme().primary))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.is_resizing = true;
                    this.resize_start_y = Some(event.position.y);
                    this.resize_start_height = Some(this.height);
                    cx.notify();
                }),
            )
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                if !this.is_resizing {
                    return;
                }

                let Some(start_y) = this.resize_start_y else {
                    return;
                };
                let Some(start_height) = this.resize_start_height else {
                    return;
                };

                // Drag up = increase height
                let delta = start_y - event.position.y;
                let new_height = (start_height + delta).clamp(MIN_HEIGHT, MAX_HEIGHT);
                this.height = new_height;
                cx.notify();
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.is_resizing = false;
                    this.resize_start_y = None;
                    this.resize_start_height = None;
                    cx.notify();
                }),
            )
    }

    fn render_tab_bar(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .w_full()
            .h(px(28.0))
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(cx.theme().border)
            .child(self.render_tab_button(BottomDockTab::Tasks, "Tasks", cx))
            .child(self.render_tab_button(BottomDockTab::Output, "Output", cx))
            .child(div().flex_1())
            .child(
                div()
                    .id("bottom-dock-close")
                    .w(px(20.0))
                    .h(px(20.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .cursor_pointer()
                    .rounded(Radii::SM)
                    .hover(|el| el.bg(cx.theme().secondary_hover))
                    .child(
                        svg()
                            .path(AppIcon::X.path())
                            .size(px(12.0))
                            .text_color(cx.theme().muted_foreground),
                    )
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle(cx);
                    })),
            )
    }

    fn render_tab_button(
        &self,
        tab: BottomDockTab,
        label: &'static str,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let is_active = self.active_tab == tab;
        let id = match tab {
            BottomDockTab::Tasks => "bottom-tab-tasks",
            BottomDockTab::Output => "bottom-tab-output",
        };

        div()
            .id(id)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .cursor_pointer()
            .rounded(Radii::SM)
            .text_xs()
            .when(is_active, |el| {
                el.bg(cx.theme().secondary_hover)
                    .text_color(cx.theme().foreground)
            })
            .when(!is_active, |el| {
                el.text_color(cx.theme().muted_foreground)
                    .hover(|el| el.bg(cx.theme().secondary_hover))
            })
            .child(label)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.active_tab = tab;
                cx.notify();
            }))
    }

    fn render_output(&self, cx: &Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .text_color(cx.theme().muted_foreground)
            .child("Output panel (placeholder)")
    }
}
