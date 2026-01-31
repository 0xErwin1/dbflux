use crate::ui::icons::AppIcon;
use crate::ui::sidebar::Sidebar;
use crate::ui::tokens::{Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

const COLLAPSED_WIDTH: Pixels = px(48.0);
const MIN_WIDTH: Pixels = px(180.0);
const MAX_WIDTH: Pixels = px(500.0);
const DEFAULT_WIDTH: Pixels = px(260.0);
const GRIP_WIDTH: Pixels = px(4.0);

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SidebarState {
    #[default]
    Expanded,
    Collapsed,
}

pub struct SidebarDock {
    sidebar: Entity<Sidebar>,
    state: SidebarState,
    width: Pixels,
    last_expanded_width: Pixels,

    is_resizing: bool,
    resize_start_x: Option<Pixels>,
    resize_start_width: Option<Pixels>,
}

impl SidebarDock {
    pub fn new(sidebar: Entity<Sidebar>, _cx: &mut Context<Self>) -> Self {
        Self {
            sidebar,
            state: SidebarState::Expanded,
            width: DEFAULT_WIDTH,
            last_expanded_width: DEFAULT_WIDTH,
            is_resizing: false,
            resize_start_x: None,
            resize_start_width: None,
        }
    }

    pub fn toggle(&mut self, cx: &mut Context<Self>) {
        match self.state {
            SidebarState::Expanded => {
                self.last_expanded_width = self.width;
                self.state = SidebarState::Collapsed;
            }
            SidebarState::Collapsed => {
                self.state = SidebarState::Expanded;
                self.width = self.last_expanded_width;
            }
        }
        cx.notify();
    }

    pub fn is_collapsed(&self) -> bool {
        self.state == SidebarState::Collapsed
    }

    pub fn current_width(&self) -> Pixels {
        match self.state {
            SidebarState::Collapsed => COLLAPSED_WIDTH,
            SidebarState::Expanded => self.width,
        }
    }

    pub fn sidebar(&self) -> &Entity<Sidebar> {
        &self.sidebar
    }
}

impl Render for SidebarDock {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let content_width = if self.is_collapsed() {
            COLLAPSED_WIDTH
        } else {
            self.width - GRIP_WIDTH
        };

        div()
            .id("sidebar-dock")
            .h_full()
            .w(self.current_width())
            .flex()
            .flex_row()
            .bg(cx.theme().tab_bar)
            .border_r_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .h_full()
                    .w(content_width)
                    .flex()
                    .flex_col()
                    .overflow_hidden()
                    .child(if self.is_collapsed() {
                        self.render_collapsed(window, cx).into_any_element()
                    } else {
                        self.render_expanded(window, cx).into_any_element()
                    }),
            )
            .when(!self.is_collapsed(), |el| {
                el.child(self.render_grip(window, cx))
            })
    }
}

impl SidebarDock {
    fn render_collapsed(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .pt(Spacing::MD)
            .gap(Spacing::SM)
            .child(
                self.icon_button("sidebar-expand", AppIcon::ChevronRight, cx)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle(cx);
                    })),
            )
            .child(
                self.icon_button("sidebar-database", AppIcon::Database, cx)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.toggle(cx);
                    })),
            )
    }

    fn render_expanded(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .child(
                div()
                    .w_full()
                    .h(px(36.0))
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::MD)
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(cx.theme().foreground)
                            .child("Connections"),
                    )
                    .child(
                        self.icon_button("sidebar-collapse", AppIcon::ChevronLeft, cx)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle(cx);
                            })),
                    ),
            )
            .child(div().flex_1().overflow_hidden().child(self.sidebar.clone()))
    }

    fn render_grip(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("sidebar-grip")
            .h_full()
            .w(GRIP_WIDTH)
            .cursor_col_resize()
            .hover(|el| el.bg(cx.theme().accent.opacity(0.3)))
            .when(self.is_resizing, |el| el.bg(cx.theme().primary))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.is_resizing = true;
                    this.resize_start_x = Some(event.position.x);
                    this.resize_start_width = Some(this.width);
                    cx.notify();
                }),
            )
            .on_mouse_move(cx.listener(|this, event: &MouseMoveEvent, _, cx| {
                if !this.is_resizing {
                    return;
                }

                let Some(start_x) = this.resize_start_x else {
                    return;
                };
                let Some(start_width) = this.resize_start_width else {
                    return;
                };

                let delta = event.position.x - start_x;
                let new_width = (start_width + delta).clamp(MIN_WIDTH, MAX_WIDTH);
                this.width = new_width;
                cx.notify();
            }))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.is_resizing = false;
                    this.resize_start_x = None;
                    this.resize_start_width = None;
                    cx.notify();
                }),
            )
    }

    fn icon_button(&self, id: &'static str, icon: AppIcon, cx: &Context<Self>) -> Stateful<Div> {
        div()
            .id(id)
            .w(px(32.0))
            .h(px(32.0))
            .flex()
            .items_center()
            .justify_center()
            .rounded(Radii::MD)
            .cursor_pointer()
            .hover(|el| el.bg(cx.theme().secondary_hover))
            .child(
                svg()
                    .path(icon.path())
                    .size_4()
                    .text_color(cx.theme().muted_foreground),
            )
    }
}
