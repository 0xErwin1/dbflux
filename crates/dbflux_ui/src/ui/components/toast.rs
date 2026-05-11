use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dbflux_components::controls::Button;
use dbflux_components::icon::IconSource;
use dbflux_components::primitives::{Icon, IconButton, Text};
use dbflux_components::tokens::BannerColors;
use dbflux_components::typography::AppFonts;
use gpui::prelude::*;
use gpui::{App, Context, Entity, Global, Hsla, SharedString, Window, px, rems};
use gpui_component::ActiveTheme;

use crate::ui::AsyncUpdateResultExt;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Radii, Spacing};

/// Wall-clock snapshot used as the default `meta_right` timestamp on toasts.
/// Captured once at build time — no tick/loop logic.
pub fn now_hms() -> String {
    dbflux_core::chrono::Local::now()
        .format("%H:%M:%S")
        .to_string()
}

/// Builds the standard "Copy" action attached to error toasts. The payload
/// argument is captured by the click handler so the clipboard text matches
/// what the user saw, even if the toast is later dismissed.
pub fn copy_action(payload: impl Into<String>) -> ToastAction {
    let payload = payload.into();
    ToastAction::new("copy-error", "Copy").on_click(move |cx: &mut App| {
        cx.write_to_clipboard(gpui::ClipboardItem::new_string(payload.clone()));
    })
}

/// Toast visual variant. Drives icon, accent stripe, banner colors, and the
/// default auto-dismiss policy.
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

    fn icon_source(self) -> IconSource {
        IconSource::Svg(self.icon().path().into())
    }

    /// Foreground / accent fill color (icon, stripe, progress fill where appropriate).
    fn accent(self, cx: &App) -> Hsla {
        let theme = cx.theme();
        match self {
            Self::Success => BannerColors::success_fg(theme),
            Self::Info => BannerColors::info_fg(theme),
            Self::Warning => BannerColors::warning_fg(theme),
            Self::Error => BannerColors::danger_fg(theme),
        }
    }

    /// Background tint applied to the toast card. Kept subtle so the accent
    /// stripe and theme chrome carry the variant signal.
    fn background(self, cx: &App) -> Hsla {
        let theme = cx.theme();
        match self {
            Self::Success => BannerColors::success_bg(theme),
            Self::Info => BannerColors::info_bg(theme),
            Self::Warning => BannerColors::warning_bg(theme),
            Self::Error => BannerColors::danger_bg(theme),
        }
    }
}

/// Callback fired when a toast action is clicked.
pub type ToastActionCallback = Arc<dyn Fn(&mut App) + Send + Sync + 'static>;

/// Action button attached to a toast. `callback` may be `None`, in which case
/// the button is rendered disabled — we never inject a placeholder handler.
pub struct ToastAction {
    pub id: SharedString,
    pub label: SharedString,
    pub primary: bool,
    pub callback: Option<ToastActionCallback>,
}

impl ToastAction {
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            primary: false,
            callback: None,
        }
    }

    pub fn primary(mut self) -> Self {
        self.primary = true;
        self
    }

    pub fn on_click<F>(mut self, callback: F) -> Self
    where
        F: Fn(&mut App) + Send + Sync + 'static,
    {
        self.callback = Some(Arc::new(callback));
        self
    }
}

/// Maximum number of action buttons rendered per toast — beyond this the
/// extras are silently dropped to keep the action row scannable.
const MAX_ACTIONS: usize = 3;

/// Default auto-dismiss delay for variants that auto-dismiss.
const AUTO_DISMISS: Duration = Duration::from_secs(4);

/// Toast model + fluent builder.
///
/// Constructed via [`Toast::success`] / [`Toast::info`] / [`Toast::warning`] /
/// [`Toast::error`]. The fluent setters consume `self`; [`Toast::push`]
/// resolves [`ToastGlobal`] and forwards the toast to the [`ToastHost`].
pub struct Toast {
    kind: ToastKind,
    title: SharedString,
    subtitle: Option<SharedString>,
    meta_right: Option<SharedString>,
    body: Option<SharedString>,
    details: Option<SharedString>,
    code_block: Option<SharedString>,
    progress: Option<f32>,
    actions: Vec<ToastAction>,
    details_collapsible: bool,
    auto_dismiss_after: Option<Duration>,
}

impl Toast {
    fn with_kind(kind: ToastKind, title: impl Into<SharedString>) -> Self {
        // Errors collapse details by default — they typically carry a code
        // block and a long body that would otherwise dominate the stack.
        let details_collapsible = matches!(kind, ToastKind::Error);
        Self {
            kind,
            title: title.into(),
            subtitle: None,
            meta_right: None,
            body: None,
            details: None,
            code_block: None,
            progress: None,
            actions: Vec::new(),
            details_collapsible,
            auto_dismiss_after: None,
        }
    }

    pub fn success(title: impl Into<SharedString>) -> Self {
        Self::with_kind(ToastKind::Success, title)
    }

    pub fn info(title: impl Into<SharedString>) -> Self {
        Self::with_kind(ToastKind::Info, title)
    }

    pub fn warning(title: impl Into<SharedString>) -> Self {
        Self::with_kind(ToastKind::Warning, title)
    }

    pub fn error(title: impl Into<SharedString>) -> Self {
        Self::with_kind(ToastKind::Error, title)
    }

    pub fn subtitle(mut self, value: impl Into<SharedString>) -> Self {
        self.subtitle = Some(value.into());
        self
    }

    pub fn meta_right(mut self, value: impl Into<SharedString>) -> Self {
        self.meta_right = Some(value.into());
        self
    }

    pub fn body(mut self, value: impl Into<SharedString>) -> Self {
        self.body = Some(value.into());
        self
    }

    pub fn details(mut self, value: impl Into<SharedString>) -> Self {
        self.details = Some(value.into());
        self
    }

    pub fn code_block(mut self, value: impl Into<SharedString>) -> Self {
        self.code_block = Some(value.into());
        self
    }

    /// Sets the progress bar fraction (clamped to 0.0..=1.0).
    pub fn progress(mut self, value: f32) -> Self {
        self.progress = Some(value.clamp(0.0, 1.0));
        self
    }

    pub fn action(mut self, action: ToastAction) -> Self {
        self.actions.push(action);
        self
    }

    pub fn collapsible(mut self) -> Self {
        self.details_collapsible = true;
        self
    }

    pub fn not_collapsible(mut self) -> Self {
        self.details_collapsible = false;
        self
    }

    pub fn auto_dismiss_after(mut self, duration: Duration) -> Self {
        self.auto_dismiss_after = Some(duration);
        self
    }

    /// Resolve the effective auto-dismiss delay based on variant + content.
    fn effective_auto_dismiss(&self) -> Option<Duration> {
        if let Some(explicit) = self.auto_dismiss_after {
            return Some(explicit);
        }
        match self.kind {
            ToastKind::Success => Some(AUTO_DISMISS),
            // Info auto-dismisses only when there's no follow-up interaction.
            ToastKind::Info if self.progress.is_none() && self.actions.is_empty() => {
                Some(AUTO_DISMISS)
            }
            _ => None,
        }
    }

    /// Append this toast to the global [`ToastHost`].
    pub fn push(self, cx: &mut App) {
        let host = cx.global::<ToastGlobal>().host.clone();
        host.update(cx, |host, cx| host.push_rich(self, cx));
    }
}

/// Stored toast inside the host. Mirrors [`Toast`] plus an id and the resolved
/// auto-dismiss policy.
struct StoredToast {
    id: u64,
    kind: ToastKind,
    title: SharedString,
    subtitle: Option<SharedString>,
    meta_right: Option<SharedString>,
    body: Option<SharedString>,
    details: Option<SharedString>,
    code_block: Option<SharedString>,
    progress: Option<f32>,
    actions: Vec<ToastAction>,
    details_collapsible: bool,
}

impl StoredToast {
    fn has_collapsible_content(&self) -> bool {
        self.body.is_some() || self.details.is_some() || self.code_block.is_some()
    }
}

pub struct ToastGlobal {
    pub host: Entity<ToastHost>,
}

impl Global for ToastGlobal {}

pub struct ToastHost {
    toasts: Vec<StoredToast>,
    /// Set of toast ids whose collapsible block is currently collapsed.
    collapsed: HashSet<u64>,
    next_id: u64,
}

impl ToastHost {
    pub fn new() -> Self {
        Self {
            toasts: Vec::new(),
            collapsed: HashSet::new(),
            next_id: 1,
        }
    }

    pub fn push_rich(&mut self, toast: Toast, cx: &mut Context<Self>) {
        let id = self.next_id;
        self.next_id += 1;

        let auto_dismiss = toast.effective_auto_dismiss();

        // Initially-collapsed when the toast opts in AND there's something to hide.
        let stored = StoredToast {
            id,
            kind: toast.kind,
            title: toast.title,
            subtitle: toast.subtitle,
            meta_right: toast.meta_right,
            body: toast.body,
            details: toast.details,
            code_block: toast.code_block,
            progress: toast.progress,
            actions: toast.actions,
            details_collapsible: toast.details_collapsible,
        };

        if stored.details_collapsible && stored.has_collapsible_content() {
            self.collapsed.insert(id);
        }

        self.toasts.push(stored);
        cx.notify();

        if let Some(delay) = auto_dismiss {
            self.schedule_dismiss(id, delay, cx);
        }
    }

    fn dismiss(&mut self, id: u64, cx: &mut Context<Self>) {
        self.toasts.retain(|t| t.id != id);
        self.collapsed.remove(&id);
        cx.notify();
    }

    fn toggle_collapsed(&mut self, id: u64, cx: &mut Context<Self>) {
        if !self.collapsed.insert(id) {
            self.collapsed.remove(&id);
        }
        cx.notify();
    }

    fn schedule_dismiss(&self, id: u64, delay: Duration, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            cx.background_executor().timer(delay).await;

            cx.update(|cx| {
                if let Some(entity) = this.upgrade() {
                    entity.update(cx, |host, cx| {
                        host.dismiss(id, cx);
                    });
                }
            })
            .log_if_dropped();
        })
        .detach();
    }
}

impl Default for ToastHost {
    fn default() -> Self {
        Self::new()
    }
}

impl Render for ToastHost {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.toasts.is_empty() {
            return gpui::div().into_any_element();
        }

        let theme = cx.theme();
        let card_bg = theme.background;
        let border_color = theme.border;
        let muted = theme.muted_foreground;

        let items = self
            .toasts
            .iter()
            .map(|toast| self.render_toast(toast, card_bg, border_color, muted, cx))
            .collect::<Vec<_>>();

        // Anchored bottom-left above the status bar; the workspace wraps us in
        // a full-screen absolute layer so `bottom`/`left` align to the window.
        gpui::div()
            .id("toast-host")
            .absolute()
            .bottom(Spacing::LG)
            .left(Spacing::LG)
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .children(items)
            .into_any_element()
    }
}

impl ToastHost {
    fn render_toast(
        &self,
        toast: &StoredToast,
        card_bg: Hsla,
        border_color: Hsla,
        muted: Hsla,
        cx: &Context<Self>,
    ) -> gpui::AnyElement {
        let toast_id = toast.id;
        let accent = toast.kind.accent(cx);
        let stripe_bg = toast.kind.background(cx);
        let icon_source = toast.kind.icon_source();

        let is_collapsed = self.collapsed.contains(&toast_id);
        let can_collapse = toast.details_collapsible && toast.has_collapsible_content();
        let show_details = !(can_collapse && is_collapsed);

        // Title row: icon · title · subtitle · spacer · meta_right · close
        let icon_element = match icon_source {
            IconSource::Svg(path) => Icon::new(IconSource::Svg(path))
                .size(px(16.0))
                .color(accent)
                .into_any_element(),
            IconSource::Named(name) => Icon::new(name)
                .size(px(16.0))
                .color(accent)
                .into_any_element(),
        };

        let mut title_row = gpui::div()
            .flex()
            .flex_row()
            .items_center()
            .gap(Spacing::SM)
            .child(gpui::div().flex_shrink_0().child(icon_element))
            .child(Text::body(toast.title.clone()).font_weight(gpui::FontWeight::BOLD));

        if let Some(subtitle) = &toast.subtitle {
            title_row = title_row
                .child(Text::caption("·").color(muted.opacity(0.6)))
                .child(Text::caption(subtitle.clone()));
        }

        // Spacer pushes meta + close to the right.
        title_row = title_row.child(gpui::div().flex_1());

        if let Some(meta) = &toast.meta_right {
            title_row = title_row.child(Text::caption_xs(meta.clone()));
        }

        let close_button = IconButton::new(
            ("toast-close", toast_id),
            IconSource::Svg(AppIcon::X.path().into()),
        )
        .icon_size(px(12.0))
        .on_click(cx.listener(move |host, _, _, cx| {
            host.dismiss(toast_id, cx);
        }));

        title_row = title_row.child(close_button);

        let mut card = gpui::div()
            .id(("toast", toast_id))
            .flex()
            .flex_row()
            .min_w(rems(22.0))
            .max_w(rems(26.0))
            .border_1()
            .border_color(border_color)
            .bg(card_bg)
            .rounded(Radii::MD)
            .shadow_lg();

        // Left accent stripe — fills the toast height.
        let stripe = gpui::div().w(px(4.0)).flex_shrink_0().bg(stripe_bg);

        let mut content = gpui::div()
            .flex_1()
            .min_w_0()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .child(title_row);

        if show_details {
            if let Some(body) = &toast.body {
                content = content.child(Text::body_sm(body.clone()));
            }

            if let Some(details) = &toast.details {
                content =
                    content.child(Text::caption_xs(details.clone()).color(muted.opacity(0.7)));
            }

            if let Some(code) = &toast.code_block {
                let code_block = gpui::div()
                    .mt(Spacing::XS)
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .border_1()
                    .border_color(border_color.opacity(0.6))
                    .bg(cx.theme().secondary)
                    .rounded(Radii::SM)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::XS)
                    .text_color(cx.theme().foreground)
                    .child(code.clone());
                content = content.child(code_block);
            }
        }

        if let Some(progress) = toast.progress {
            let percent = (progress * 100.0).round() as u32;
            let percent_label: SharedString = format!("{}%", percent).into();
            let progress_row = gpui::div()
                .mt(Spacing::XS)
                .flex()
                .flex_row()
                .items_center()
                .gap(Spacing::SM)
                .child(
                    gpui::div()
                        .flex_1()
                        .h(px(2.0))
                        .bg(border_color.opacity(0.6))
                        .rounded(Radii::FULL)
                        .child(
                            gpui::div()
                                .h_full()
                                .w(gpui::relative(progress))
                                .bg(cx.theme().primary)
                                .rounded(Radii::FULL),
                        ),
                )
                .child(Text::caption_xs(percent_label));
            content = content.child(progress_row);
        }

        if can_collapse {
            let label: SharedString = if is_collapsed {
                "Show details".into()
            } else {
                "Hide details".into()
            };
            let toggle = gpui::div()
                .id(("toast-toggle", toast_id))
                .mt(Spacing::XS)
                .cursor_pointer()
                .child(Text::caption_xs(label).color(accent))
                .on_click(cx.listener(move |host, _, _, cx| {
                    host.toggle_collapsed(toast_id, cx);
                }));
            content = content.child(toggle);
        }

        if !toast.actions.is_empty() {
            let mut action_row = gpui::div()
                .mt(Spacing::SM)
                .flex()
                .flex_row()
                .justify_end()
                .gap(Spacing::SM);

            for (idx, action) in toast.actions.iter().take(MAX_ACTIONS).enumerate() {
                let button_id: SharedString = format!("toast-action-{}-{}", toast_id, idx).into();
                let mut button = Button::new(button_id, action.label.clone()).small();
                if action.primary {
                    button = button.primary();
                }
                match action.callback.clone() {
                    Some(callback) => {
                        button = button.on_click(move |_, _, app| callback(app));
                    }
                    None => {
                        // No callback: render disabled — never fake an action.
                        button = button.disabled(true);
                    }
                }
                action_row = action_row.child(button);
            }

            content = content.child(action_row);
        }

        card = card.child(stripe).child(content);
        card.into_any_element()
    }
}

pub struct PendingToast {
    pub message: String,
    pub is_error: bool,
}

pub fn flush_pending_toast<T>(
    toast: Option<PendingToast>,
    _window: &mut Window,
    cx: &mut Context<T>,
) {
    let Some(toast) = toast else {
        return;
    };

    if toast.is_error {
        let payload = toast.message.clone();
        Toast::error(toast.message)
            .meta_right(now_hms())
            .action(
                ToastAction::new("copy-error", "Copy").on_click(move |cx: &mut App| {
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(payload.clone()));
                }),
            )
            .push(cx);
    } else {
        Toast::success(toast.message).meta_right(now_hms()).push(cx);
    }
}
