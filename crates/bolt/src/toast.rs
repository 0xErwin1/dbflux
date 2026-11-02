//! The toast stack: a severity-toned notification model, its bounded stack
//! with auto-dismiss and hover-pause timers, and the presentation shell that
//! draws it.
//!
//! Everything here is mechanism: the token bucket that throttles Warning and
//! Info toasts, the stack bound, the dismiss timings and the shell's metrics.
//! A product supplies the content (which events produce a toast, their text
//! and actions), its icon glyphs through [`ToastIcons`], and its words
//! through `crate::strings`. The toast throttle and its tests moved here
//! verbatim from the product's `user_error::throttle`; the token bucket is
//! now generic in capacity and refill interval, with the toast policy as the
//! caller that fixes the values.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gpui::prelude::*;
use gpui::{App, Context, Entity, FontWeight, Global, Hsla, SharedString, Window};
use gpui_component::ActiveTheme;
use gpui_component::IconName;

use crate::controls::Button;
use crate::fonts;
use crate::icon::IconSource;
use crate::primitives::{Chamfer, Icon};
use crate::semantic::BannerColors;
use crate::strings;
use crate::tokens::{ChamferCut, ChromeColors, Feedback, FontSizes, Spacing};

// ---------------------------------------------------------------------------
// Throttle
// ---------------------------------------------------------------------------

/// Toast throttle capacity: the Warning/Info toasts a full bucket lets
/// through before the next refill.
pub const BUCKET_CAPACITY: u8 = 5;

/// Toast throttle refill: one token back per interval, capped at the
/// capacity.
pub const REFILL_INTERVAL: Duration = Duration::from_secs(2);

/// Token-bucket rate limiter for `Warning`/`Info` toasts.
///
/// The bucket starts full. Each `try_consume` call attempts to take one token.
/// Tokens refill based on elapsed time since the last refill, up to the
/// capacity cap. The algorithm is O(1) per call.
///
/// `Error` and `Fatal` toasts bypass this entirely — callers skip
/// `try_consume` for those severities.
pub struct TokenBucket {
    capacity: u8,
    refill_interval: Duration,
    tokens: u8,
    last_refill: Instant,
    now_fn: fn() -> Instant,
}

impl TokenBucket {
    pub fn new(capacity: u8, refill_interval: Duration) -> Self {
        Self::with_clock(capacity, refill_interval, Instant::now)
    }

    /// Constructs a bucket with an injectable clock — use this in tests for
    /// deterministic time control.
    pub fn with_clock(capacity: u8, refill_interval: Duration, now_fn: fn() -> Instant) -> Self {
        Self {
            tokens: capacity,
            last_refill: (now_fn)(),
            capacity,
            refill_interval,
            now_fn,
        }
    }

    /// Returns `true` if the caller may proceed (a token was consumed).
    /// Returns `false` when the bucket was empty after attempting a refill.
    pub fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    fn refill(&mut self) {
        let now = (self.now_fn)();
        let elapsed = now.duration_since(self.last_refill);
        let refill_count =
            (elapsed.as_millis() / self.refill_interval.as_millis()).min(u8::MAX as u128) as u8;
        if refill_count > 0 {
            self.tokens = self.tokens.saturating_add(refill_count).min(self.capacity);
            self.last_refill = now;
        }
    }
}

// ---------------------------------------------------------------------------
// Icons seam
// ---------------------------------------------------------------------------

/// The icons a product's toasts draw. Install it once at startup; without it
/// the toasts use gpui-component's severity glyphs.
#[derive(Clone, Debug)]
pub struct ToastIcons {
    pub success: IconSource,
    pub info: IconSource,
    pub warning: IconSource,
    pub error: IconSource,
    /// Shown instead of the kind icon while a toast reports progress.
    pub loader: IconSource,
    pub close: IconSource,
}

impl Global for ToastIcons {}

impl Default for ToastIcons {
    fn default() -> Self {
        Self {
            success: IconName::CircleCheck.into(),
            info: IconName::Info.into(),
            warning: IconName::TriangleAlert.into(),
            error: IconName::CircleAlert.into(),
            loader: IconName::Loader.into(),
            close: IconName::CircleX.into(),
        }
    }
}

impl ToastIcons {
    /// The installed icons, or the defaults when the product installed none.
    pub fn current(cx: &App) -> Self {
        cx.try_global::<Self>().cloned().unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

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
    fn icon(self, has_progress: bool, icons: &ToastIcons) -> IconSource {
        let kind_icon = match self {
            Self::Success => &icons.success,
            Self::Info => &icons.info,
            Self::Warning => &icons.warning,
            Self::Error => &icons.error,
        };
        if has_progress {
            icons.loader.clone()
        } else {
            kind_icon.clone()
        }
    }

    /// Edge stripe and icon color. An Info toast that reports progress is
    /// work in flight, so it carries the tint like a busy status.
    fn accent(self, has_progress: bool, cx: &App) -> Hsla {
        let banners = BannerColors::for_current(cx);
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

/// Toasts shown at once. Older ones fold into a single "N more" entry that
/// expands or dismisses them together.
pub const VISIBLE_TOAST_LIMIT: usize = 4;

/// How long a toast stays before the stack closes it on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastAutoDismiss {
    /// Success and Info toasts close after this long, and Warning toasts
    /// after twice it. A toast with actions or progress stays.
    After(Duration),
    /// Every toast stays until the user dismisses it.
    Never,
}

impl ToastAutoDismiss {
    /// Converts the persisted delay, in seconds. `0` turns auto-dismiss off.
    pub fn from_secs(secs: u32) -> Self {
        if secs == 0 {
            Self::Never
        } else {
            Self::After(Duration::from_secs(u64::from(secs)))
        }
    }
}

impl Default for ToastAutoDismiss {
    fn default() -> Self {
        // The design-system default; a product pins its own by pushing its
        // persisted setting through `ToastHost::set_auto_dismiss`.
        Self::After(Duration::from_secs(8))
    }
}

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

    /// Resolve the effective auto-dismiss delay for `policy`. `None` keeps
    /// the toast until the user dismisses it.
    fn effective_auto_dismiss(&self, policy: ToastAutoDismiss) -> Option<Duration> {
        if let Some(explicit) = self.auto_dismiss_after {
            return Some(explicit);
        }

        let base = match policy {
            ToastAutoDismiss::Never => return None,
            ToastAutoDismiss::After(base) => base,
        };

        // A toast with a follow-up to settle stays until it is resolved.
        let settles_on_its_own = self.progress.is_none() && self.actions.is_empty();

        match self.kind {
            ToastKind::Success if settles_on_its_own => Some(base),
            ToastKind::Info if settles_on_its_own => Some(base),
            // A warning reports something worth reading but needs no reply,
            // so it outlives Success and Info.
            ToastKind::Warning if settles_on_its_own => Some(base * 2),
            _ => None,
        }
    }

    /// Append this toast to the global [`ToastHost`].
    pub fn push(self, cx: &mut App) {
        let host = cx.global::<ToastGlobal>().host.clone();
        host.update(cx, |host, cx| host.push_rich(self, cx));
    }
}

// ---------------------------------------------------------------------------
// Host
// ---------------------------------------------------------------------------

/// Stored toast inside the host. Mirrors [`Toast`] plus an id, the delay the
/// stack will close it after, and the state of that timer.
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
    /// Auto-dismiss delay this toast was stored with; `None` keeps it until
    /// the user dismisses it.
    delay: Option<Duration>,
    /// Time left on the timer while the pointer holds it, or `None` while the
    /// timer runs or the toast is persistent.
    paused_remaining: Option<Duration>,
    /// When the running timer was armed.
    timer_armed_at: Option<Instant>,
    /// Bumped every time the timer is armed, so a timer that was replaced
    /// while the pointer hovered the toast does nothing when it fires.
    timer_generation: u64,
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
    /// Delay the stack closes a toast after, from the product's settings.
    auto_dismiss: ToastAutoDismiss,
    /// Whether the user expanded the stack past [`VISIBLE_TOAST_LIMIT`].
    stack_expanded: bool,
}

impl ToastHost {
    pub fn new() -> Self {
        Self {
            toasts: Vec::new(),
            collapsed: HashSet::new(),
            next_id: 1,
            warn_info_bucket: TokenBucket::new(BUCKET_CAPACITY, REFILL_INTERVAL),
            auto_dismiss: ToastAutoDismiss::default(),
            stack_expanded: false,
        }
    }

    /// Applies the auto-dismiss delay from the product's settings to the
    /// toasts pushed from now on. Toasts already on screen keep the delay
    /// they were pushed with.
    pub fn set_auto_dismiss(&mut self, policy: ToastAutoDismiss) {
        self.auto_dismiss = policy;
    }

    pub fn push_rich(&mut self, toast: Toast, cx: &mut Context<Self>) {
        if matches!(toast.kind, ToastKind::Warning | ToastKind::Info)
            && !self.warn_info_bucket.try_consume()
        {
            log::debug!(
                target: "bolt::toast",
                "toast dropped by throttle: kind={:?} title={}",
                toast.kind, toast.title
            );
            return;
        }

        let id = self.next_id;
        self.next_id += 1;

        // A new toast folds the stack again, so an expanded stack cannot grow
        // past the visible limit for the rest of the session.
        self.stack_expanded = false;

        let delay = toast.effective_auto_dismiss(self.auto_dismiss);

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
            delay,
            paused_remaining: None,
            timer_armed_at: None,
            timer_generation: 0,
        };

        if stored.details_collapsible && stored.has_collapsible_content() {
            self.collapsed.insert(id);
        }

        self.toasts.push(stored);
        cx.notify();

        if let Some(delay) = delay {
            self.arm_timer(id, delay, cx);
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

    /// Number of toasts the stack is hiding behind its "N more" entry.
    pub fn hidden_count(&self) -> usize {
        if self.stack_expanded {
            0
        } else {
            self.toasts.len().saturating_sub(VISIBLE_TOAST_LIMIT)
        }
    }

    /// Shows every toast, past [`VISIBLE_TOAST_LIMIT`], until the stack
    /// empties or the user dismisses them.
    pub fn expand_stack(&mut self, cx: &mut Context<Self>) {
        if self.stack_expanded {
            return;
        }
        self.stack_expanded = true;
        cx.notify();
    }

    /// Closes every toast the stack is hiding behind its "N more" entry.
    pub fn dismiss_hidden(&mut self, cx: &mut Context<Self>) {
        let hidden = self.hidden_count();
        if hidden == 0 {
            return;
        }

        let removed: Vec<u64> = self.toasts.drain(..hidden).map(|toast| toast.id).collect();
        for id in removed {
            self.collapsed.remove(&id);
        }
        cx.notify();
    }

    /// Arms the auto-dismiss timer of toast `id`, replacing one already
    /// running. The previous timer keeps sleeping but does nothing when it
    /// fires, because its generation no longer matches.
    fn arm_timer(&mut self, id: u64, delay: Duration, cx: &mut Context<Self>) {
        let Some(toast) = self.toasts.iter_mut().find(|toast| toast.id == id) else {
            return;
        };

        toast.timer_generation = toast.timer_generation.wrapping_add(1);
        let generation = toast.timer_generation;
        toast.timer_armed_at = Some(Instant::now());
        toast.paused_remaining = None;

        cx.spawn(async move |this, cx| {
            cx.background_executor().timer(delay).await;

            cx.update(|cx| {
                if let Some(entity) = this.upgrade() {
                    entity.update(cx, |host, cx| host.fire_timer(id, generation, cx));
                }
            });
        })
        .detach();
    }

    /// Closes toast `id` when `generation` is still the armed timer, which it
    /// is not after the timer was replaced or paused.
    fn fire_timer(&mut self, id: u64, generation: u64, cx: &mut Context<Self>) {
        let current = self
            .toasts
            .iter()
            .find(|toast| toast.id == id)
            .is_some_and(|toast| toast.timer_generation == generation);

        if current {
            self.dismiss(id, cx);
        }
    }

    /// Holds the timer of toast `id` while the pointer rests on it, keeping
    /// the time it had left.
    fn pause_timer(&mut self, id: u64, cx: &mut Context<Self>) {
        let Some(toast) = self.toasts.iter_mut().find(|toast| toast.id == id) else {
            return;
        };

        let (Some(delay), Some(armed_at)) = (toast.delay, toast.timer_armed_at) else {
            return;
        };

        toast.paused_remaining = Some(delay.saturating_sub(armed_at.elapsed()));
        toast.timer_generation = toast.timer_generation.wrapping_add(1);
        toast.timer_armed_at = None;
        cx.notify();
    }

    /// Restarts the timer of toast `id` with the time it had left when the
    /// pointer left it.
    fn resume_timer(&mut self, id: u64, cx: &mut Context<Self>) {
        let Some(toast) = self.toasts.iter().find(|toast| toast.id == id) else {
            return;
        };

        let Some(remaining) = toast.paused_remaining else {
            return;
        };

        self.arm_timer(id, remaining, cx);
    }
}

impl Default for ToastHost {
    fn default() -> Self {
        Self::new()
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

// ---------------------------------------------------------------------------
// Presentation
// ---------------------------------------------------------------------------

impl Render for ToastHost {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.toasts.is_empty() {
            return gpui::div().into_any_element();
        }

        let hidden = self.hidden_count();
        let first_visible = if hidden > 0 { hidden } else { 0 };

        let mut items = Vec::new();
        if hidden > 0 {
            items.push(self.render_hidden_summary(hidden, cx));
        }
        #[expect(
            clippy::indexing_slicing,
            reason = "`hidden_count` returns either 0 or `toasts.len() - VISIBLE_TOAST_LIMIT` (saturating), so `first_visible <= self.toasts.len()` and the tail slice is in bounds"
        )]
        items.extend(
            self.toasts[first_visible..]
                .iter()
                .map(|toast| self.render_toast(toast, cx)),
        );

        // Stacks from the top-right corner of whatever region the host mounts
        // in, newest last.
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
        let icons = ToastIcons::current(cx);
        let accent = toast.kind.accent(has_progress, cx);
        let icon = toast.kind.icon(has_progress, &icons);

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

        let close_button = Button::new(
            ("toast-close", toast_id),
            strings::text(cx, "toast.action.dismiss"),
        )
        .ghost()
        .inline()
        .icon(icons.close.clone())
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
                        .font_family(fonts::editor_family(cx))
                        .text_size(Feedback::TOAST_META_FONT)
                        .text_color(muted)
                        .child(meta),
                )
            })
            .child(gpui::div().flex_shrink_0().child(close_button));

        // `.occlude()` makes the toast card opaque to hit-testing so clicks on
        // empty toast area do not leak through to the surface underneath.
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
            .on_hover(cx.listener(move |host, hovered: &bool, _, cx| {
                if *hovered {
                    host.pause_timer(toast_id, cx);
                } else {
                    host.resume_timer(toast_id, cx);
                }
            }))
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
                        .font_family(fonts::editor_family(cx))
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
                            .font_family(fonts::editor_family(cx))
                            .text_size(Feedback::TOAST_META_FONT)
                            .text_color(muted)
                            .child(percent_label),
                    ),
            );
        }

        if can_collapse {
            let label = if is_collapsed {
                strings::text(cx, "toast.action.show_details")
            } else {
                strings::text(cx, "toast.action.hide_details")
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

    /// The compact entry that stands in for the toasts the stack is hiding.
    /// Its label expands the stack; its close button dismisses them together.
    fn render_hidden_summary(&self, hidden: usize, cx: &Context<Self>) -> gpui::AnyElement {
        let theme = cx.theme();
        let card_fill = theme.secondary;
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let icons = ToastIcons::current(cx);

        let label = strings::text_with(
            cx,
            "toast.stack.hidden_more",
            &[("count", &hidden.to_string())],
        );

        let dismiss_button = Button::new(
            "toast-hidden-dismiss",
            strings::text(cx, "toast.action.dismiss"),
        )
        .ghost()
        .inline()
        .icon(icons.close.clone())
        .icon_only()
        .icon_size(Feedback::TOAST_CLOSE_ICON)
        .on_click(cx.listener(|host, _, _, cx| host.dismiss_hidden(cx)));

        gpui::div()
            .id("toast-hidden-summary")
            .occlude()
            .relative()
            .flex()
            .flex_row()
            .items_center()
            .w(Feedback::TOAST_WIDTH)
            .gap(Spacing::SM)
            .py(Feedback::TOAST_PADDING_Y)
            .px(Feedback::TOAST_PADDING_X)
            .child(
                Chamfer::new(ChamferCut::OVERLAY)
                    .fill(card_fill)
                    .left_edge(muted, Feedback::TOAST_STRIPE),
            )
            .child(
                gpui::div()
                    .id("toast-hidden-expand")
                    .flex_1()
                    .min_w_0()
                    .cursor_pointer()
                    .text_size(FontSizes::BASE)
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(strong)
                    .child(label)
                    .on_click(cx.listener(|host, _, _, cx| host.expand_stack(cx))),
            )
            .child(gpui::div().flex_shrink_0().child(dismiss_button))
            .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod throttle_tests {
    use std::cell::Cell;
    use std::time::{Duration, Instant};

    use super::{BUCKET_CAPACITY, REFILL_INTERVAL, TokenBucket};

    thread_local! {
        static FAKE_NOW: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    fn fake_clock() -> Instant {
        FAKE_NOW.with(|c| c.get().expect("fake clock not initialised"))
    }

    fn set_fake_now(t: Instant) {
        FAKE_NOW.with(|c| c.set(Some(t)));
    }

    fn bucket() -> TokenBucket {
        TokenBucket::with_clock(BUCKET_CAPACITY, REFILL_INTERVAL, fake_clock)
    }

    #[test]
    fn five_consecutive_consumes_succeed() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = bucket();

        for i in 1..=5 {
            assert!(bucket.try_consume(), "consume {i} should succeed");
        }
    }

    #[test]
    fn sixth_consume_fails_immediately() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = bucket();

        for _ in 0..5 {
            bucket.try_consume();
        }

        assert!(
            !bucket.try_consume(),
            "sixth consume must fail when bucket is empty"
        );
    }

    #[test]
    fn one_token_refills_after_refill_interval() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = bucket();

        // Drain the bucket.
        for _ in 0..5 {
            bucket.try_consume();
        }
        assert!(!bucket.try_consume());

        // Advance past one refill interval.
        set_fake_now(base + REFILL_INTERVAL + Duration::from_millis(1));
        assert!(
            bucket.try_consume(),
            "one refilled token should allow one more consume"
        );
    }

    #[test]
    fn bucket_caps_at_capacity_after_long_idle() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = bucket();

        // Drain completely.
        for _ in 0..5 {
            bucket.try_consume();
        }

        // Advance by 60 s — far beyond 5 * REFILL_INTERVAL.
        set_fake_now(base + Duration::from_secs(60));

        // Must succeed exactly BUCKET_CAPACITY times, then fail.
        for i in 1..=BUCKET_CAPACITY {
            assert!(
                bucket.try_consume(),
                "consume {i} after refill must succeed"
            );
        }
        assert!(
            !bucket.try_consume(),
            "capacity cap: no more than BUCKET_CAPACITY tokens"
        );
    }

    /// The bucket is generic: a smaller capacity and a faster interval behave
    /// the same way at their own scale.
    #[test]
    fn a_custom_capacity_and_interval_throttle_the_same_way() {
        let base = Instant::now();
        set_fake_now(base);
        let mut bucket = TokenBucket::with_clock(2, Duration::from_millis(100), fake_clock);

        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume(), "custom capacity respected");

        set_fake_now(base + Duration::from_millis(150));
        assert!(
            bucket.try_consume(),
            "one token refilled after the custom interval"
        );
        assert!(
            !bucket.try_consume(),
            "only one token per interval at the custom scale"
        );
    }
}

#[cfg(test)]
mod icons_tests {
    use gpui::TestAppContext;

    use super::{IconName, ToastIcons};
    use crate::icon::IconSource;

    #[gpui::test]
    fn an_app_without_installed_icons_resolves_the_defaults(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let icons = ToastIcons::current(cx);
            assert_eq!(icons.success, IconSource::from(IconName::CircleCheck));
            assert_eq!(icons.loader, IconSource::from(IconName::Loader));
        });
    }

    #[gpui::test]
    fn installing_replaces_the_icons_and_a_copy_reads_back(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let custom = ToastIcons {
                success: IconSource::from(IconName::Bell),
                ..ToastIcons::default()
            };
            cx.set_global(custom);
        });
        cx.update(|cx| {
            let icons = ToastIcons::current(cx);
            assert_eq!(icons.success, IconSource::from(IconName::Bell));
            assert_eq!(icons.error, IconSource::from(IconName::CircleAlert));
        });
    }

    /// A toast that reports progress shows the loader whatever its kind.
    #[test]
    fn progress_shows_the_loader_instead_of_the_kind_icon() {
        use super::ToastKind;

        let icons = ToastIcons::default();
        let busy = ToastKind::Success.icon(true, &icons);
        let idle = ToastKind::Success.icon(false, &icons);
        assert_eq!(busy, icons.loader);
        assert_eq!(idle, icons.success);
    }
}

#[cfg(test)]
mod action_tests {
    use gpui::{AppContext as _, TestAppContext};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{Toast, ToastAction, ToastHost};

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

#[cfg(test)]
mod auto_dismiss_tests {
    use gpui::{AppContext as _, Entity, TestAppContext};
    use std::time::Duration;

    use super::{
        BUCKET_CAPACITY, Toast, ToastAction, ToastAutoDismiss, ToastHost, VISIBLE_TOAST_LIMIT,
    };

    fn new_host(cx: &mut TestAppContext) -> Entity<ToastHost> {
        cx.new(|_| ToastHost::new())
    }

    fn shown(host: &Entity<ToastHost>, cx: &TestAppContext) -> usize {
        host.read_with(cx, |host, _| host.toast_count())
    }

    fn hidden(host: &Entity<ToastHost>, cx: &TestAppContext) -> usize {
        host.read_with(cx, |host, _| host.hidden_count())
    }

    fn tick(cx: &TestAppContext, duration: Duration) {
        cx.executor().advance_clock(duration);
        cx.run_until_parked();
    }

    /// The stored delay follows the kind, the content and the policy: a
    /// Warning outlives a Success, an action or an Error keeps the toast, and
    /// the policy can turn auto-dismiss off entirely.
    #[gpui::test]
    fn delays_follow_kind_content_and_policy(cx: &mut TestAppContext) {
        let host = new_host(cx);

        host.update(cx, |host, cx| {
            host.push_rich(Toast::success("ok"), cx);
            host.push_rich(Toast::warning("careful"), cx);
            host.push_rich(
                Toast::warning("act").action(ToastAction::new("go", "Go")),
                cx,
            );
            host.push_rich(Toast::error("boom"), cx);
            host.push_rich(
                Toast::success("done").action(ToastAction::new("undo", "Undo")),
                cx,
            );
        });

        host.read_with(cx, |host, _| {
            assert_eq!(host.toasts[0].delay, Some(Duration::from_secs(8)));
            assert_eq!(
                host.toasts[1].delay,
                Some(Duration::from_secs(16)),
                "a warning gets twice the base delay"
            );
            assert_eq!(
                host.toasts[2].delay, None,
                "a warning with an action stays until it is resolved"
            );
            assert_eq!(host.toasts[3].delay, None, "an error stays");
            assert_eq!(
                host.toasts[4].delay, None,
                "a success with an action stays until it is resolved"
            );
        });

        host.update(cx, |host, _| host.set_auto_dismiss(ToastAutoDismiss::Never));
        host.update(cx, |host, cx| host.push_rich(Toast::success("ok"), cx));
        assert_eq!(
            host.read_with(cx, |host, _| host.toasts.last().unwrap().delay),
            None,
            "the policy can turn auto-dismiss off"
        );
    }

    #[gpui::test]
    fn a_success_toast_closes_after_the_configured_delay(cx: &mut TestAppContext) {
        let host = new_host(cx);
        host.update(cx, |host, cx| host.push_rich(Toast::success("ok"), cx));

        tick(cx, Duration::from_secs(7));
        assert_eq!(shown(&host, cx), 1, "still shown before its delay");

        tick(cx, Duration::from_secs(2));
        assert_eq!(shown(&host, cx), 0, "closed after its delay");
    }

    #[gpui::test]
    fn a_warning_toast_closes_after_twice_the_delay(cx: &mut TestAppContext) {
        let host = new_host(cx);
        host.update(cx, |host, cx| host.push_rich(Toast::warning("careful"), cx));

        tick(cx, Duration::from_secs(8));
        assert_eq!(shown(&host, cx), 1, "a warning outlives a success");

        tick(cx, Duration::from_secs(8));
        assert_eq!(shown(&host, cx), 0, "closed once its longer delay passes");
    }

    #[gpui::test]
    fn an_error_toast_stays_until_dismissed(cx: &mut TestAppContext) {
        let host = new_host(cx);
        host.update(cx, |host, cx| host.push_rich(Toast::error("boom"), cx));

        tick(cx, Duration::from_secs(600));
        assert_eq!(shown(&host, cx), 1);

        host.update(cx, |host, cx| host.dismiss(1, cx));
        assert_eq!(shown(&host, cx), 0);
    }

    #[gpui::test]
    fn hovering_holds_the_timer_and_leaving_it_restarts_it(cx: &mut TestAppContext) {
        let host = new_host(cx);
        host.update(cx, |host, cx| host.push_rich(Toast::success("ok"), cx));

        host.update(cx, |host, cx| host.pause_timer(1, cx));
        tick(cx, Duration::from_secs(60));
        assert_eq!(shown(&host, cx), 1, "a held toast does not close");

        host.update(cx, |host, cx| host.resume_timer(1, cx));
        tick(cx, Duration::from_secs(9));
        assert_eq!(shown(&host, cx), 0, "leaving restarts the timer");
    }

    /// Past the visible limit the older toasts fold into one entry, which can
    /// expand them or dismiss them together.
    #[gpui::test]
    fn the_stack_folds_the_toasts_past_the_visible_limit(cx: &mut TestAppContext) {
        let host = new_host(cx);
        for index in 0..(VISIBLE_TOAST_LIMIT + 2) {
            host.update(cx, |host, cx| {
                host.push_rich(Toast::success(format!("toast {index}")), cx)
            });
        }

        assert_eq!(shown(&host, cx), VISIBLE_TOAST_LIMIT + 2);
        assert_eq!(hidden(&host, cx), 2);

        host.update(cx, |host, cx| host.expand_stack(cx));
        assert_eq!(hidden(&host, cx), 0, "expanding shows every toast");

        host.update(cx, |host, cx| {
            host.push_rich(Toast::success("one more"), cx)
        });
        assert_eq!(hidden(&host, cx), 3, "a new toast folds the stack again");
    }

    #[gpui::test]
    fn dismissing_the_folded_toasts_keeps_the_newest(cx: &mut TestAppContext) {
        let host = new_host(cx);
        for index in 0..(VISIBLE_TOAST_LIMIT + 3) {
            host.update(cx, |host, cx| {
                host.push_rich(Toast::success(format!("toast {index}")), cx)
            });
        }

        host.update(cx, |host, cx| host.dismiss_hidden(cx));

        assert_eq!(shown(&host, cx), VISIBLE_TOAST_LIMIT);
        assert_eq!(hidden(&host, cx), 0);
        assert_eq!(
            host.read_with(cx, |host, _| host.toasts.first().unwrap().title.to_string()),
            format!("toast 3"),
            "the oldest toasts are the ones dismissed"
        );
    }

    /// A toast the stack is hiding still closes on its own timer.
    #[gpui::test]
    fn a_folded_toast_still_closes_on_its_timer(cx: &mut TestAppContext) {
        let host = new_host(cx);
        for index in 0..(VISIBLE_TOAST_LIMIT + 1) {
            host.update(cx, |host, cx| {
                host.push_rich(Toast::success(format!("toast {index}")), cx)
            });
        }
        assert_eq!(hidden(&host, cx), 1);

        tick(cx, Duration::from_secs(9));
        assert_eq!(shown(&host, cx), 0, "every toast closed on its own timer");
    }

    /// Warning and Info toasts share the throttle bucket; Error toasts bypass
    /// it entirely.
    #[gpui::test]
    fn the_throttle_drops_warning_and_info_past_the_capacity(cx: &mut TestAppContext) {
        let host = new_host(cx);

        host.update(cx, |host, cx| {
            for index in 0..BUCKET_CAPACITY {
                host.push_rich(Toast::warning(format!("warn {index}")), cx);
            }
            // Errors bypass the bucket and are never dropped.
            host.push_rich(Toast::error("boom"), cx);
        });
        assert_eq!(shown(&host, cx), BUCKET_CAPACITY as usize + 1);

        host.update(cx, |host, cx| {
            let before = host.toast_count();
            host.push_rich(Toast::warning("dropped"), cx);
            assert_eq!(
                host.toast_count(),
                before,
                "an empty bucket drops the toast"
            );
        });
    }
}
