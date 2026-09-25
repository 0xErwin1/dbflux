use dbflux_components::tokens::{ChromeColors, ShellMetrics};
use dbflux_ui_sidebar::Sidebar;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use std::time::{Duration, Instant};

pub enum SidebarDockEvent {
    Collapsed,
    Expanded,
}

/// A collapsed sidebar leaves no column behind: the activity rail stays on
/// screen and reopens it.
const COLLAPSED_WIDTH: Pixels = px(0.0);
const DEFAULT_EXPANDED_WIDTH: Pixels = ShellMetrics::SIDEBAR_WIDTH;
const MIN_WIDTH: Pixels = px(200.0);
const MAX_WIDTH: Pixels = px(800.0);
const GRIP_WIDTH: Pixels = px(7.0);

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum SidebarState {
    #[default]
    Expanded,
    Collapsed,
}

pub struct SidebarDock {
    sidebar: Entity<Sidebar>,
    _sidebar_subscription: Subscription,
    state: SidebarState,
    transient_reveal: bool,
    pointer_inside: bool,
    sidebar_focused: bool,
    hover_deadline: Option<Instant>,
    hover_generation: u64,
    width: Pixels,
    last_expanded_width: Pixels,

    is_resizing: bool,
    resize_start_x: Option<Pixels>,
    resize_start_width: Option<Pixels>,
}

impl SidebarDock {
    pub fn new(sidebar: Entity<Sidebar>, cx: &mut Context<Self>) -> Self {
        let subscription = cx.observe(&sidebar, |dock, _, cx| dock.dismiss_if_idle(cx));
        Self {
            sidebar,
            _sidebar_subscription: subscription,
            state: SidebarState::Expanded,
            transient_reveal: false,
            pointer_inside: false,
            sidebar_focused: false,
            hover_deadline: None,
            hover_generation: 0,
            width: DEFAULT_EXPANDED_WIDTH,
            last_expanded_width: DEFAULT_EXPANDED_WIDTH,
            is_resizing: false,
            resize_start_x: None,
            resize_start_width: None,
        }
    }

    pub fn toggle(&mut self, cx: &mut Context<Self>) {
        let closing_transient = self.state == SidebarState::Collapsed && self.transient_reveal;
        self.finish_resize(cx);
        self.cancel_hover();
        if closing_transient {
            self.transient_reveal = false;
            cx.emit(SidebarDockEvent::Collapsed);
            cx.notify();
            return;
        }

        match self.state {
            SidebarState::Expanded => {
                self.last_expanded_width = self.width;
                self.state = SidebarState::Collapsed;
                self.transient_reveal = false;
                cx.emit(SidebarDockEvent::Collapsed);
            }
            SidebarState::Collapsed => {
                self.state = SidebarState::Expanded;
                self.transient_reveal = false;
                self.width = self.last_expanded_width;
                cx.emit(SidebarDockEvent::Expanded);
            }
        }
        cx.notify();
    }

    pub fn expand(&mut self, cx: &mut Context<Self>) {
        if self.state == SidebarState::Collapsed {
            self.finish_resize(cx);
            self.state = SidebarState::Expanded;
            self.width = self.last_expanded_width;
            cx.notify();
        }
    }

    fn cancel_hover(&mut self) {
        self.hover_deadline = None;
        self.hover_generation = self.hover_generation.wrapping_add(1);
    }

    pub fn pointer_enter(&mut self, now: Instant, cx: &mut Context<Self>) {
        if self.pointer_inside {
            return;
        }
        self.pointer_inside = true;
        if self.state == SidebarState::Collapsed && !self.transient_reveal {
            self.hover_deadline = Some(now + Duration::from_millis(250));
            self.hover_generation = self.hover_generation.wrapping_add(1);
            let generation = self.hover_generation;
            let entity = cx.entity().clone();
            cx.spawn(async move |_, cx| {
                cx.background_executor()
                    .timer(Duration::from_millis(250))
                    .await;
                entity.update(cx, |dock, cx| {
                    if dock.hover_generation == generation {
                        dock.process_hover_deadline(Instant::now(), cx);
                    }
                })
            })
            .detach();
        }
    }

    pub fn pointer_leave(&mut self, cx: &mut Context<Self>) {
        self.pointer_inside = false;
        self.cancel_hover();
        self.dismiss_if_idle(cx);
    }

    pub fn set_sidebar_focused(&mut self, focused: bool, cx: &mut Context<Self>) {
        self.sidebar_focused = focused;
        if focused {
            self.cancel_hover();
            self.reveal_transiently(cx);
        } else {
            self.dismiss_if_idle(cx);
        }
    }

    pub fn process_hover_deadline(&mut self, now: Instant, cx: &mut Context<Self>) {
        if self.pointer_inside && self.hover_deadline.is_some_and(|deadline| now >= deadline) {
            self.cancel_hover();
            self.reveal_transiently(cx);
        }
    }

    pub fn dismiss_if_idle(&mut self, cx: &mut Context<Self>) {
        if !self.pointer_inside
            && !self.sidebar_focused
            && !self.is_resizing
            && !self.sidebar.read(cx).has_transient_interaction()
        {
            self.dismiss_transient(cx);
        }
    }

    pub fn reveal_transiently(&mut self, cx: &mut Context<Self>) {
        if self.state == SidebarState::Collapsed && !self.transient_reveal {
            self.transient_reveal = true;
            self.width = self.last_expanded_width;
            cx.notify();
        }
    }

    pub fn dismiss_transient(&mut self, cx: &mut Context<Self>) {
        if self.transient_reveal {
            self.transient_reveal = false;
            cx.notify();
        }
    }

    pub fn is_collapsed(&self) -> bool {
        self.state == SidebarState::Collapsed && !self.transient_reveal
    }

    pub fn is_resizing(&self) -> bool {
        self.is_resizing
    }

    pub(crate) fn begin_resize(&mut self, position_x: Pixels, cx: &mut Context<Self>) {
        self.is_resizing = true;
        self.resize_start_x = Some(position_x);
        self.resize_start_width = Some(self.width);
        cx.notify();
    }

    pub fn finish_resize(&mut self, cx: &mut Context<Self>) {
        if self.is_resizing {
            self.is_resizing = false;
            self.resize_start_x = None;
            self.resize_start_width = None;
            self.last_expanded_width = self.width;
            self.dismiss_if_idle(cx);
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

    /// Width of the sidebar when shown: the live width while expanded, the
    /// width it reopens at while collapsed. The title bar's left block
    /// follows it.
    pub fn expanded_width(&self) -> Pixels {
        if self.state == SidebarState::Collapsed && !self.transient_reveal {
            self.last_expanded_width
        } else {
            self.width
        }
    }

    pub(crate) fn current_width(&self) -> Pixels {
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

        let pointer_entity = cx.entity().clone();
        let pointer_listeners = canvas(
            |_, _, _| {},
            move |bounds, _, window, _| {
                window.on_mouse_event({
                    let entity = pointer_entity.clone();
                    move |event: &MouseMoveEvent, phase, _, cx| {
                        if phase.bubble() {
                            entity.update(cx, |dock, cx| {
                                if bounds.contains(&event.position) {
                                    dock.pointer_enter(Instant::now(), cx);
                                } else if dock.pointer_inside {
                                    dock.pointer_leave(cx);
                                }
                            });
                        }
                    }
                });
                window.on_mouse_event({
                    let entity = pointer_entity.clone();
                    move |_: &MouseExitEvent, phase, _, cx| {
                        if phase.bubble() {
                            entity.update(cx, |dock, cx| dock.pointer_leave(cx));
                        }
                    }
                });
            },
        )
        .absolute()
        .size_full();

        let resize_listeners = self.is_resizing.then(|| {
            let entity = cx.entity().clone();

            // Element-level mouse listeners stop seeing the drag as soon as
            // the cursor leaves the grip, and children that swallow mouse
            // moves break the workspace-level fallback. Window-level
            // listeners must be registered during paint, hence the canvas.
            canvas(
                |_, _, _| {},
                move |_, _, window, _| {
                    window.on_mouse_event({
                        let entity = entity.clone();
                        move |event: &MouseMoveEvent, phase, _, cx| {
                            if phase.bubble() {
                                entity.update(cx, |dock, cx| {
                                    dock.handle_resize_move(event.position.x, cx);
                                });
                            }
                        }
                    });

                    window.on_mouse_event({
                        let entity = entity.clone();
                        move |_: &MouseUpEvent, phase, _, cx| {
                            if phase.bubble() {
                                entity.update(cx, |dock, cx| dock.finish_resize(cx));
                            }
                        }
                    });
                },
            )
            .absolute()
            .size_full()
        });

        div()
            .id("sidebar-dock")
            .relative()
            .h_full()
            .w(self.current_width())
            .flex()
            .flex_row()
            .bg(cx.theme().tab_bar)
            .when(!is_collapsed, |el| {
                el.border_r_1().border_color(cx.theme().border)
            })
            .child(pointer_listeners)
            .when_some(resize_listeners, |el, listeners| el.child(listeners))
            .when(!is_collapsed, |el| {
                el.child(
                    div()
                        .h_full()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .child(self.sidebar.clone()),
                )
                .child(self.render_grip(window, cx))
            })
    }
}

impl EventEmitter<SidebarDockEvent> for SidebarDock {}

impl SidebarDock {
    fn render_grip(&self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("sidebar-grip")
            .absolute()
            .top_0()
            .bottom_0()
            .right_0()
            .w(GRIP_WIDTH)
            .cursor_col_resize()
            .hover(|el| el.bg(cx.theme().accent.opacity(0.3)))
            .when(self.is_resizing, |el| el.bg(ChromeColors::tint(cx.theme())))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.begin_resize(event.position.x, cx);
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
                cx.listener(|this, _, _, cx| this.finish_resize(cx)),
            )
    }
}
