use crate::ui::icons::AppIcon;
use crate::ui::sidebar::Sidebar;
use crate::ui::tokens::Radii;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;

pub enum SidebarDockEvent {
    OpenSettings,
    Collapsed,
    Expanded,
}

const COLLAPSED_WIDTH: Pixels = px(48.0);
const DEFAULT_EXPANDED_WIDTH: Pixels = px(280.0);
const MIN_WIDTH: Pixels = px(200.0);
const MAX_WIDTH: Pixels = px(500.0);
const HEADER_HEIGHT: Pixels = px(36.0);
const HEADER_PADDING: Pixels = px(8.0);
const BUTTON_SIZE: Pixels = px(32.0);
const ICON_SIZE: Pixels = px(18.0);
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
            width: DEFAULT_EXPANDED_WIDTH,
            last_expanded_width: DEFAULT_EXPANDED_WIDTH,
            is_resizing: false,
            resize_start_x: None,
            resize_start_width: None,
        }
    }

    pub fn toggle(&mut self, cx: &mut Context<Self>) {
        self.finish_resize(cx);

        match self.state {
            SidebarState::Expanded => {
                self.last_expanded_width = self.width;
                self.state = SidebarState::Collapsed;
                cx.emit(SidebarDockEvent::Collapsed);
            }
            SidebarState::Collapsed => {
                self.state = SidebarState::Expanded;
                self.width = self.last_expanded_width;
                cx.emit(SidebarDockEvent::Expanded);
            }
        }
        cx.notify();
    }

    pub fn is_collapsed(&self) -> bool {
        self.state == SidebarState::Collapsed
    }

    pub fn is_resizing(&self) -> bool {
        self.is_resizing
    }

    pub fn finish_resize(&mut self, cx: &mut Context<Self>) {
        if self.is_resizing {
            self.is_resizing = false;
            self.resize_start_x = None;
            self.resize_start_width = None;
            cx.notify();
        }
    }

    pub fn handle_resize_move(&mut self, position_x: Pixels, cx: &mut Context<Self>) {
        if !self.is_resizing {
            return;
        }

        let Some(start_x) = self.resize_start_x else {
            return;
        };
        let Some(start_width) = self.resize_start_width else {
            return;
        };

        let delta = position_x - start_x;
        let new_width = (start_width + delta).clamp(MIN_WIDTH, MAX_WIDTH);
        self.width = new_width;
        cx.notify();
    }

    fn current_width(&self) -> Pixels {
        if self.is_collapsed() {
            COLLAPSED_WIDTH
        } else {
            self.width
        }
    }
}

impl Render for SidebarDock {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_collapsed = self.is_collapsed();
        let content_width = if is_collapsed {
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
                    .child(self.render_header(window, cx))
                    .child(div().flex_1().overflow_hidden().child(if is_collapsed {
                        self.render_collapsed_content(window, cx).into_any_element()
                    } else {
                        self.sidebar.clone().into_any_element()
                    })),
            )
            .when(!is_collapsed, |el| el.child(self.render_grip(window, cx)))
    }
}

impl EventEmitter<SidebarDockEvent> for SidebarDock {}

impl SidebarDock {
    fn render_header(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_collapsed = self.is_collapsed();
        let toggle_icon = if is_collapsed {
            AppIcon::ChevronRight
        } else {
            AppIcon::ChevronLeft
        };

        let toggle_button = self
            .header_button("sidebar-toggle", toggle_icon, cx)
            .on_click(cx.listener(|this, _, _, cx| {
                this.toggle(cx);
            }));

        let base = div()
            .w_full()
            .h(HEADER_HEIGHT)
            .flex()
            .flex_row()
            .items_center()
            .border_b_1()
            .border_color(cx.theme().border);

        if is_collapsed {
            base.justify_center().child(toggle_button)
        } else {
            base.px(HEADER_PADDING)
                .child(toggle_button)
                .child(div().flex_1())
                .child(div().w(BUTTON_SIZE))
        }
    }

    fn render_collapsed_content(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .child(
                div()
                    .w_full()
                    .flex()
                    .justify_center()
                    .py(HEADER_PADDING)
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(
                        self.header_button("sidebar-database", AppIcon::Database, cx)
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.toggle(cx);
                            })),
                    ),
            )
            .child(div().flex_1())
            .child(
                div()
                    .w_full()
                    .flex()
                    .justify_center()
                    .py(HEADER_PADDING)
                    .border_t_1()
                    .border_color(cx.theme().border)
                    .child(
                        self.header_button("sidebar-settings", AppIcon::Settings, cx)
                            .on_click(cx.listener(|_this, _, _, cx| {
                                cx.emit(SidebarDockEvent::OpenSettings);
                            })),
                    ),
            )
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

    fn header_button(&self, id: &'static str, icon: AppIcon, cx: &Context<Self>) -> Stateful<Div> {
        div()
            .id(id)
            .w(BUTTON_SIZE)
            .h(BUTTON_SIZE)
            .flex()
            .items_center()
            .justify_center()
            .rounded(Radii::MD)
            .cursor_pointer()
            .hover(|el| el.bg(cx.theme().secondary_hover))
            .child(
                svg()
                    .path(icon.path())
                    .size(ICON_SIZE)
                    .text_color(cx.theme().muted_foreground),
            )
    }
}
