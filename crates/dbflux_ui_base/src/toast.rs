//! DBFlux's toast content and wiring.
//!
//! The toast *mechanism* — the throttle bucket, the severity model, the
//! bounded stack with its auto-dismiss and hover-pause timers, and the
//! presentation shell — lives in `bolt::toast` and is re-exported here at the
//! old paths. This module keeps the product half: which clock formats a
//! toast's timestamp, the copy action, the persisted auto-dismiss setting
//! reaching the running stack, the pending-toast flush, and the product icon
//! catalogue installed into Bolt's [`ToastIcons`] seam.

use gpui::{App, Context, Entity, Window};

pub use bolt::toast::{
    Toast, ToastAction, ToastActionCallback, ToastAutoDismiss, ToastControlAction, ToastControls,
    ToastGlobal, ToastHost, ToastIcons, ToastKind, VISIBLE_TOAST_LIMIT,
};

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

/// The product's toast glyphs: the same `AppIcon` catalogue the toast drew
/// before the shell moved to Bolt, widened to `IconSource` at the seam.
fn product_toast_icons() -> ToastIcons {
    use dbflux_components::icons::AppIcon;

    ToastIcons {
        success: AppIcon::CircleCheck.into(),
        info: AppIcon::Info.into(),
        warning: AppIcon::TriangleAlert.into(),
        error: AppIcon::CircleAlert.into(),
        loader: AppIcon::Loader.into(),
        close: AppIcon::CircleX.into(),
    }
}

/// Installs the product's toast glyphs into Bolt's icon seam. Call once at
/// startup, before the first toast renders; `publish_auto_dismiss_setting`
/// calls it from the workspace's toast-host setup.
pub fn install_toast_icons(cx: &mut App) {
    cx.set_global(product_toast_icons());
}

pub struct PendingToast {
    pub message: String,
    pub is_error: bool,
}

/// Applies the persisted toast auto-dismiss delay now and after every
/// app-state change, so a change in Settings > General reaches the running
/// stack without a restart. Also installs the product's toast glyphs: this is
/// the ui_base startup path that runs before any toast can render.
pub fn publish_auto_dismiss_setting(
    app_state: &Entity<crate::AppStateEntity>,
    host: &Entity<ToastHost>,
    cx: &mut App,
) {
    install_toast_icons(cx);
    apply_auto_dismiss_setting(app_state, host, cx);

    let host = host.clone();
    cx.subscribe(
        app_state,
        move |app_state, _: &crate::AppStateChanged, cx| {
            apply_auto_dismiss_setting(&app_state, &host, cx);
        },
    )
    .detach();
}

fn apply_auto_dismiss_setting(
    app_state: &Entity<crate::AppStateEntity>,
    host: &Entity<ToastHost>,
    cx: &mut App,
) {
    let secs = app_state
        .read(cx)
        .general_settings()
        .toast_auto_dismiss_secs;
    let policy = ToastAutoDismiss::from_secs(secs);
    host.update(cx, |host, _| host.set_auto_dismiss(policy));
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
mod i18n_tests {
    const TOAST_ACTION_KEYS: &[&str] = &[
        "toast.action.copy",
        "toast.action.show_details",
        "toast.action.hide_details",
        "toast.action.dismiss",
        "toast.stack.hidden_more",
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

    /// The hidden-stack label the shell renders through `bolt::strings`
    /// interpolates the count exactly like the `t!` macro did: translate in
    /// the locale, then replace `%{count}`.
    #[test]
    fn hidden_more_label_interpolates_the_count() {
        let interpolate = |locale: &str| {
            dbflux_i18n::translate_in(locale, "toast.stack.hidden_more").replace("%{count}", "3")
        };
        assert_eq!(interpolate("en"), "3 more");
        assert_eq!(interpolate("es"), "3 más");
    }
}

#[cfg(test)]
mod parity_tests {
    use super::install_toast_icons;
    use crate::toast::{ToastAutoDismiss, ToastIcons};
    use bolt::icon::IconSource;
    use dbflux_core::GeneralSettings;
    use gpui::TestAppContext;

    /// Bolt's `ToastAutoDismiss` carries the design-system default; the
    /// product pins it to its own settings default, so a drift on either side
    /// fails here instead of changing the toast behaviour silently.
    #[test]
    fn bolt_default_auto_dismiss_matches_the_product_settings_default() {
        assert_eq!(
            ToastAutoDismiss::default(),
            ToastAutoDismiss::from_secs(GeneralSettings::DEFAULT_TOAST_AUTO_DISMISS_SECS),
            "bolt's default auto-dismiss drifted from GeneralSettings' constant"
        );
    }

    /// The installed glyphs are the exact SVG paths the toast drew before the
    /// shell moved: the move changes no pixels.
    #[gpui::test]
    fn installed_icons_draw_the_same_glyphs_the_product_drew(cx: &mut TestAppContext) {
        cx.update(install_toast_icons);
        cx.update(|cx| {
            let icons = ToastIcons::current(cx);
            let svg = |path: &'static str| IconSource::Svg(path.into());
            assert_eq!(icons.success, svg("icons/ui/circle-check.svg"));
            assert_eq!(icons.info, svg("icons/ui/info.svg"));
            assert_eq!(icons.warning, svg("icons/ui/triangle-alert.svg"));
            assert_eq!(icons.error, svg("icons/ui/circle-alert.svg"));
            assert_eq!(icons.loader, svg("icons/ui/loader.svg"));
            assert_eq!(icons.close, svg("icons/ui/circle-x.svg"));
        });
    }
}
