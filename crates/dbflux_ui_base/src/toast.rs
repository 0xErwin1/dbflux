use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::user_error::throttle::TokenBucket;

use dbflux_components::controls::Button;
use dbflux_components::primitives::{Chamfer, Icon};
use dbflux_components::semantic::BannerColors as SemBannerColors;
use dbflux_components::typography::AppFonts;
use gpui::prelude::*;
use gpui::{App, Context, Entity, FontWeight, Global, Hsla, SharedString, Window};
use gpui_component::ActiveTheme;

use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::{ChamferCut, ChromeColors, Feedback, FontSizes, Spacing};

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
    ToastAction::new("copy-error", dbflux_i18n::t!("toast.action.copy")).on_click(
        move |cx: &mut App| {
            cx.write_to_clipboard(gpui::ClipboardItem::new_string(payload.clone()));
        },
    )
}

/// Toast visual variant. Drives the icon, the edge stripe color, and the
/// default auto-dismiss policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Success,
    Info,
    Warning,
    Error,
}

impl ToastKind {
    /// Kind icon. A toast that reports progress shows the loader instead.
    fn icon(self, has_progress: bool) -> AppIcon {
        match self {
            _ if has_progress => AppIcon::Loader,
            Self::Success => AppIcon::CircleCheck,
            Self::Info => AppIcon::Info,
            Self::Warning => AppIcon::TriangleAlert,
            Self::Error => AppIcon::CircleAlert,
        }
    }

    /// Edge stripe and icon color. An Info toast that reports progress is
    /// work in flight, so it carries the tint like a busy status.
    fn accent(self, has_progress: bool, cx: &App) -> Hsla {
        let banners = SemBannerColors::for_current(cx);
        match self {
            Self::Info if has_progress => ChromeColors::tint(cx.theme()),
            Self::Success => banners.success_fg,
            Self::Info => banners.info_fg,
            Self::Warning => banners.warning_fg,
            Self::Error => banners.error_fg,
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
    /// Close the toast after the callback runs.
    pub dismisses: bool,
}

impl ToastAction {
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            primary: false,
            callback: None,
            dismisses: false,
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

    /// Close the toast once the action has run, for actions that settle what
    /// the toast was about (for example "Skip").
    pub fn dismisses(mut self) -> Self {
        self.dismisses = true;
        self
    }
}

/// One action button of a toast, as [`ToastHost::toast_controls`] lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToastControlAction {
    pub id: SharedString,
    pub label: SharedString,
    /// The button has a handler; without one it is drawn disabled.
    pub enabled: bool,
}

/// The controls of one toast besides its close button.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToastControls {
    pub actions: Vec<ToastControlAction>,
    /// `Some(collapsed)` when the toast can show or hide its details.
    pub details_toggle: Option<bool>,
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
    /// Token bucket for Warning/Info toasts. Error toasts bypass this entirely.
    warn_info_bucket: TokenBucket,
}

impl ToastHost {
    pub fn new() -> Self {
        Self {
            toasts: Vec::new(),
            collapsed: HashSet::new(),
            next_id: 1,
            warn_info_bucket: TokenBucket::new(),
        }
    }

    pub fn push_rich(&mut self, toast: Toast, cx: &mut Context<Self>) {
        if matches!(toast.kind, ToastKind::Warning | ToastKind::Info)
            && !self.warn_info_bucket.try_consume()
        {
            log::debug!(
                target: "dbflux_ui::toast",
                "toast dropped by throttle: kind={:?} title={}",
                toast.kind, toast.title
            );
            return;
        }

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

    /// Runs the action at `index` of toast `toast_id`, then closes the toast
    /// when the action asks for it. The callback runs outside any host
    /// update, so it may push toasts of its own.
    pub fn run_action(host: &Entity<Self>, toast_id: u64, index: usize, cx: &mut App) {
        let action = host
            .read(cx)
            .toasts
            .iter()
            .find(|toast| toast.id == toast_id)
            .and_then(|toast| {
                toast
                    .actions
                    .get(index)
                    .map(|action| (action.callback.clone(), action.dismisses))
            });

        let Some((callback, dismisses)) = action else {
            return;
        };

        if let Some(callback) = callback {
            callback(cx);
        }

        if dismisses {
            host.update(cx, |host, cx| host.dismiss(toast_id, cx));
        }
    }

    /// Closes toast `id`, like its close button.
    pub fn dismiss(&mut self, id: u64, cx: &mut Context<Self>) {
        self.toasts.retain(|t| t.id != id);
        self.collapsed.remove(&id);
        cx.notify();
    }

    /// The newest toast on screen, which the keyboard acts on.
    pub fn latest_toast_id(&self) -> Option<u64> {
        self.toasts.last().map(|toast| toast.id)
    }

    /// What a toast offers besides its close button, for a keyboard menu:
    /// its action buttons (the ones drawn), and its details toggle.
    pub fn toast_controls(&self, id: u64) -> Option<ToastControls> {
        let toast = self.toasts.iter().find(|toast| toast.id == id)?;
        let can_collapse = toast.details_collapsible && toast.has_collapsible_content();

        Some(ToastControls {
            actions: toast
                .actions
                .iter()
                .take(MAX_ACTIONS)
                .map(|action| ToastControlAction {
                    id: action.id.clone(),
                    label: action.label.clone(),
                    enabled: action.callback.is_some(),
                })
                .collect(),
            details_toggle: can_collapse.then(|| self.collapsed.contains(&id)),
        })
    }

    /// Shows or hides the details of toast `id`, like its toggle link.
    pub fn toggle_collapsed(&mut self, id: u64, cx: &mut Context<Self>) {
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
            });
        })
        .detach();
    }
}

impl Default for ToastHost {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod i18n_tests {
    const TOAST_ACTION_KEYS: &[&str] = &[
        "toast.action.copy",
        "toast.action.show_details",
        "toast.action.hide_details",
        "toast.action.dismiss",
    ];

    #[test]
    fn toast_catalog_keys_resolve() {
        for key in TOAST_ACTION_KEYS {
            let english = dbflux_i18n::t!(key);
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert_ne!(english, *key, "missing English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(spanish, *key, "missing Spanish translation for {key}");
        }
    }

    #[test]
    fn toast_copy_label_differs_between_locales() {
        let english = dbflux_i18n::t!("toast.action.copy", locale = "en");
        let spanish = dbflux_i18n::t!("toast.action.copy", locale = "es");

        assert_eq!(english, "Copy");
        assert_eq!(spanish, "Copiar");
        assert_ne!(english, spanish);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ToastHost {
    /// Returns the number of currently-visible toasts. For use in tests only.
    pub fn toast_count(&self) -> usize {
        self.toasts.len()
    }

    /// Returns the title of the most-recently-pushed toast, if any. For use in tests only.
    pub fn last_toast_title(&self) -> Option<String> {
        self.toasts.last().map(|t| t.title.to_string())
    }

    /// Returns the `ToastKind` of the most-recently-pushed toast, if any. For use in tests only.
    pub fn last_toast_kind(&self) -> Option<ToastKind> {
        self.toasts.last().map(|t| t.kind)
    }
}

impl Render for ToastHost {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.toasts.is_empty() {
            return gpui::div().into_any_element();
        }

        let items = self
            .toasts
            .iter()
            .map(|toast| self.render_toast(toast, cx))
            .collect::<Vec<_>>();

        // Stacks from the top-right corner of whatever region the workspace
        // mounts the host in (the document area), newest last.
        gpui::div()
            .id("toast-host")
            .absolute()
            .top(Feedback::TOAST_STACK_INSET)
            .right(Feedback::TOAST_STACK_INSET)
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .children(items)
            .into_any_element()
    }
}

impl ToastHost {
    fn render_toast(&self, toast: &StoredToast, cx: &Context<Self>) -> gpui::AnyElement {
        let toast_id = toast.id;
        let has_progress = toast.progress.is_some();
        let accent = toast.kind.accent(has_progress, cx);
        let icon = toast.kind.icon(has_progress);

        let theme = cx.theme();
        let card_fill = theme.secondary;
        let well = theme.background;
        let strong = ChromeColors::strong(theme);
        let body_color = theme.foreground;
        let muted = theme.muted_foreground;
        let progress_fill = theme.primary;

        let is_collapsed = self.collapsed.contains(&toast_id);
        let can_collapse = toast.details_collapsible && toast.has_collapsible_content();
        let show_details = !(can_collapse && is_collapsed);

        // Title block: title, optional subtitle below. `flex_1 min_w_0` lets a
        // long subtitle wrap within the toast width.
        let title_block = gpui::div()
            .flex()
            .flex_col()
            .flex_1()
            .min_w_0()
            .gap(Feedback::TOAST_TITLE_LINE_GAP)
            .child(
                gpui::div()
                    .text_size(FontSizes::BASE)
                    .font_weight(FontWeight::BOLD)
                    .text_color(strong)
                    .child(toast.title.clone()),
            )
            .when_some(toast.subtitle.clone(), |el, subtitle| {
                el.child(
                    gpui::div()
                        .text_size(Feedback::TOAST_BODY_FONT)
                        .text_color(muted)
                        .child(subtitle),
                )
            });

        let close_button = Button::new(("toast-close", toast_id), "Dismiss")
            .ghost()
            .inline()
            .icon(AppIcon::CircleX)
            .icon_only()
            .icon_size(Feedback::TOAST_CLOSE_ICON)
            .on_click(cx.listener(move |host, _, _, cx| {
                host.dismiss(toast_id, cx);
            }));

        let title_row = gpui::div()
            .flex()
            .flex_row()
            .items_center()
            .gap(Feedback::TOAST_TITLE_GAP)
            .child(
                gpui::div()
                    .flex_shrink_0()
                    .child(Icon::new(icon).size(Feedback::TOAST_ICON).color(accent)),
            )
            .child(title_block)
            .when_some(toast.meta_right.clone(), |el, meta| {
                el.child(
                    gpui::div()
                        .flex_shrink_0()
                        .font_family(AppFonts::MONO)
                        .text_size(Feedback::TOAST_META_FONT)
                        .text_color(muted)
                        .child(meta),
                )
            })
            .child(gpui::div().flex_shrink_0().child(close_button));

        // `.occlude()` makes the toast card opaque to hit-testing so clicks on
        // empty toast area do not leak through to the workspace underneath.
        let mut card = gpui::div()
            .id(("toast", toast_id))
            .occlude()
            .relative()
            .flex()
            .flex_col()
            .w(Feedback::TOAST_WIDTH)
            .gap(Feedback::TOAST_ROW_GAP)
            .py(Feedback::TOAST_PADDING_Y)
            .px(Feedback::TOAST_PADDING_X)
            .child(
                Chamfer::new(ChamferCut::OVERLAY)
                    .fill(card_fill)
                    .left_edge(accent, Feedback::TOAST_STRIPE),
            )
            .child(title_row);

        if show_details {
            if let Some(body) = &toast.body {
                card = card.child(
                    gpui::div()
                        .text_size(Feedback::TOAST_BODY_FONT)
                        .text_color(body_color)
                        .child(body.clone()),
                );
            }

            if let Some(details) = &toast.details {
                card = card.child(
                    gpui::div()
                        .text_size(Feedback::TOAST_META_FONT)
                        .text_color(muted)
                        .child(details.clone()),
                );
            }

            if let Some(code) = &toast.code_block {
                card = card.child(
                    gpui::div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .bg(well)
                        .font_family(AppFonts::MONO)
                        .text_size(FontSizes::XS)
                        .text_color(body_color)
                        .child(code.clone()),
                );
            }
        }

        if let Some(progress) = toast.progress {
            let percent = (progress * 100.0).round() as u32;
            let percent_label: SharedString = format!("{}%", percent).into();

            card = card.child(
                gpui::div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap(Feedback::TOAST_TITLE_GAP)
                    .child(
                        gpui::div()
                            .flex_1()
                            .h(Feedback::TOAST_PROGRESS_HEIGHT)
                            .bg(well)
                            .child(
                                gpui::div()
                                    .h_full()
                                    .w(gpui::relative(progress))
                                    .bg(progress_fill),
                            ),
                    )
                    .child(
                        gpui::div()
                            .font_family(AppFonts::MONO)
                            .text_size(Feedback::TOAST_META_FONT)
                            .text_color(muted)
                            .child(percent_label),
                    ),
            );
        }

        if can_collapse {
            let label: String = if is_collapsed {
                dbflux_i18n::t!("toast.action.show_details")
            } else {
                dbflux_i18n::t!("toast.action.hide_details")
            };
            card = card.child(
                gpui::div()
                    .id(("toast-toggle", toast_id))
                    .cursor_pointer()
                    .text_size(Feedback::TOAST_META_FONT)
                    .text_color(accent)
                    .child(label)
                    .on_click(cx.listener(move |host, _, _, cx| {
                        host.toggle_collapsed(toast_id, cx);
                    })),
            );
        }

        if !toast.actions.is_empty() {
            let mut action_row = gpui::div().flex().flex_row().gap(Spacing::SM);

            for (idx, action) in toast.actions.iter().take(MAX_ACTIONS).enumerate() {
                let button_id: SharedString = format!("toast-action-{}-{}", toast_id, idx).into();
                let mut button = Button::new(button_id, action.label.clone()).inline();
                if action.primary {
                    button = button.primary();
                }
                match action.callback.as_ref() {
                    Some(_) => {
                        let host = cx.entity();
                        button = button.on_click(move |_, _, app| {
                            Self::run_action(&host, toast_id, idx, app);
                        });
                    }
                    None => {
                        // No callback: render disabled — never fake an action.
                        button = button.disabled(true);
                    }
                }
                action_row = action_row.child(button);
            }

            card = card.child(action_row);
        }

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
                ToastAction::new("copy-error", dbflux_i18n::t!("toast.action.copy")).on_click(
                    move |cx: &mut App| {
                        cx.write_to_clipboard(gpui::ClipboardItem::new_string(payload.clone()));
                    },
                ),
            )
            .push(cx);
    } else {
        Toast::success(toast.message).meta_right(now_hms()).push(cx);
    }
}

#[cfg(test)]
mod action_tests {
    use super::{Toast, ToastAction, ToastHost};
    use gpui::{AppContext as _, TestAppContext};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[gpui::test]
    fn only_dismissing_actions_close_their_toast(cx: &mut TestAppContext) {
        let runs = Arc::new(AtomicUsize::new(0));
        let host = cx.new(|_| ToastHost::new());

        let keep_runs = runs.clone();
        let skip_runs = runs.clone();
        let toast = Toast::error("Update available")
            .action(ToastAction::new("keep", "Keep").on_click(move |_| {
                keep_runs.fetch_add(1, Ordering::SeqCst);
            }))
            .action(
                ToastAction::new("skip", "Skip")
                    .dismisses()
                    .on_click(move |_| {
                        skip_runs.fetch_add(1, Ordering::SeqCst);
                    }),
            );
        host.update(cx, |host, cx| host.push_rich(toast, cx));

        cx.update(|cx| ToastHost::run_action(&host, 1, 0, cx));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(host.read_with(cx, |host, _| host.toast_count()), 1);

        cx.update(|cx| ToastHost::run_action(&host, 1, 1, cx));
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(host.read_with(cx, |host, _| host.toast_count()), 0);
    }
}
