use std::time::Duration;

use gpui::prelude::*;
use gpui::{px, rems, App, Context, Entity, Global, Hsla, MouseButton, Window};
use gpui_component::ActiveTheme;

use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Success,
    Info,
    Warning,
    Error,
}

impl ToastKind {
    fn icon(self) -> AppIcon {
        match self {
            Self::Success => AppIcon::CircleCheck,
            Self::Info => AppIcon::Info,
            Self::Warning => AppIcon::TriangleAlert,
            Self::Error => AppIcon::CircleAlert,
        }
    }

    fn color(self, cx: &App) -> Hsla {
        let theme = cx.theme();
        match self {
            Self::Success => theme.success,
            Self::Info => theme.info,
            Self::Warning => theme.warning,
            Self::Error => theme.danger,
        }
    }

    fn auto_dismiss(self) -> bool {
        !matches!(self, Self::Error)
    }
}

struct Toast {
    id: u64,
    kind: ToastKind,
    message: String,
}

pub struct ToastGlobal {
    pub host: Entity<ToastHost>,
}

impl Global for ToastGlobal {}

pub struct ToastHost {
    toasts: Vec<Toast>,
    next_id: u64,
}

impl ToastHost {
    pub fn new() -> Self {
        Self {
            toasts: Vec::new(),
            next_id: 1,
        }
    }

    pub fn push(&mut self, kind: ToastKind, message: impl Into<String>, cx: &mut Context<Self>) {
        let id = self.next_id;
        self.next_id += 1;

        let toast = Toast {
            id,
            kind,
            message: message.into(),
        };

        self.toasts.push(toast);
        cx.notify();

        if kind.auto_dismiss() {
            self.schedule_dismiss(id, cx);
        }
    }

    fn dismiss(&mut self, id: u64, cx: &mut Context<Self>) {
        self.toasts.retain(|t| t.id != id);
        cx.notify();
    }

    fn schedule_dismiss(&self, id: u64, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(Duration::from_secs(4))
                .await;

            cx.update(|cx| {
                if let Some(entity) = this.upgrade() {
                    entity.update(cx, |host, cx| {
                        host.dismiss(id, cx);
                    });
                }
            })
            .ok();
        })
        .detach();
    }
}

fn mix_color(base: Hsla, accent: Hsla, ratio: f32) -> Hsla {
    Hsla {
        h: base.h * (1.0 - ratio) + accent.h * ratio,
        s: base.s * (1.0 - ratio) + accent.s * ratio,
        l: base.l * (1.0 - ratio) + accent.l * ratio,
        a: base.a,
    }
}

fn with_alpha(color: Hsla, alpha: f32) -> Hsla {
    Hsla { a: alpha, ..color }
}

impl Render for ToastHost {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.toasts.is_empty() {
            return gpui::div().into_any_element();
        }

        let theme = cx.theme();

        let items = self
            .toasts
            .iter()
            .map(|toast| {
                let toast_id = toast.id;
                let accent = toast.kind.color(cx);
                let icon_path = toast.kind.icon().path();

                let background = mix_color(theme.popover, accent, 0.15);
                let border_color = with_alpha(accent, 0.5);

                let close_button = gpui::div()
                    .id(("toast-close", toast_id))
                    .flex()
                    .items_center()
                    .justify_center()
                    .size(rems(1.5))
                    .rounded(Radii::SM)
                    .cursor_pointer()
                    .hover(|s| s.bg(with_alpha(accent, 0.2)))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |host, _, _, cx| {
                            host.dismiss(toast_id, cx);
                        }),
                    )
                    .child(
                        gpui::svg()
                            .path(AppIcon::X.path())
                            .size(px(14.0))
                            .text_color(theme.muted_foreground),
                    );

                gpui::div()
                    .id(("toast", toast_id))
                    .flex()
                    .items_center()
                    .gap(Spacing::MD)
                    .pl(Spacing::MD)
                    .pr(Spacing::SM)
                    .py(Spacing::SM)
                    .min_w(rems(20.0))
                    .max_w(rems(28.0))
                    .border_1()
                    .border_color(border_color)
                    .bg(background)
                    .rounded(Radii::LG)
                    .shadow_lg()
                    .child(
                        gpui::svg()
                            .path(icon_path)
                            .size(px(18.0))
                            .flex_shrink_0()
                            .text_color(accent),
                    )
                    .child(
                        gpui::div()
                            .flex_1()
                            .overflow_hidden()
                            .text_ellipsis()
                            .text_size(FontSizes::SM)
                            .text_color(theme.foreground)
                            .child(toast.message.clone()),
                    )
                    .child(close_button)
                    .into_any_element()
            })
            .collect::<Vec<_>>();

        gpui::div()
            .id("toast-host")
            .absolute()
            .top(Spacing::LG)
            .right(Spacing::LG)
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .children(items)
            .into_any_element()
    }
}

pub trait ToastExt {
    fn toast_success(&mut self, message: impl Into<String>, window: &mut Window);
    fn toast_info(&mut self, message: impl Into<String>, window: &mut Window);
    fn toast_warning(&mut self, message: impl Into<String>, window: &mut Window);
    fn toast_error(&mut self, message: impl Into<String>, window: &mut Window);
}

impl<T> ToastExt for Context<'_, T> {
    fn toast_success(&mut self, message: impl Into<String>, _window: &mut Window) {
        let host = self.global::<ToastGlobal>().host.clone();
        host.update(self, |host, cx| {
            host.push(ToastKind::Success, message, cx);
        });
    }

    fn toast_info(&mut self, message: impl Into<String>, _window: &mut Window) {
        let host = self.global::<ToastGlobal>().host.clone();
        host.update(self, |host, cx| {
            host.push(ToastKind::Info, message, cx);
        });
    }

    fn toast_warning(&mut self, message: impl Into<String>, _window: &mut Window) {
        let host = self.global::<ToastGlobal>().host.clone();
        host.update(self, |host, cx| {
            host.push(ToastKind::Warning, message, cx);
        });
    }

    fn toast_error(&mut self, message: impl Into<String>, _window: &mut Window) {
        let host = self.global::<ToastGlobal>().host.clone();
        host.update(self, |host, cx| {
            host.push(ToastKind::Error, message, cx);
        });
    }
}
