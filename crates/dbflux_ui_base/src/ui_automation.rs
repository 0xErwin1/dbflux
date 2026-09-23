//! Optional UI automation bridge, compiled only with the `ui-automation` feature.
//!
//! With the feature enabled, [`install`] attaches a `gpui-mcp` bridge to a window
//! so an automation client running as the same user can discover it, read its
//! rendered element tree and inject input. Without the feature, [`install`] does
//! nothing and `gpui-mcp` is not compiled, so every window can call it
//! unconditionally and the feature gate stays in this module.

use gpui::{App, Window};

/// Installs the automation bridge into `window`.
///
/// The bridge lives until the window closes or the application quits, at which
/// point its listener stops and its discovery descriptor is removed. A failure
/// is logged and leaves the window usable without automation.
#[cfg(feature = "ui-automation")]
pub fn install(window: &mut Window, cx: &mut App) {
    bridge::install(window, cx);
}

/// No-op: the `ui-automation` feature is disabled.
#[cfg(not(feature = "ui-automation"))]
pub fn install(_window: &mut Window, _cx: &mut App) {}

#[cfg(feature = "ui-automation")]
mod bridge {
    use std::collections::HashMap;

    use dbflux_core::{LogErr, ReleaseChannel};
    use gpui::{App, Global, Window, WindowId};
    use gpui_mcp::{AppId, BridgeConfig, BridgeHandle};

    /// Bridge handles keyed by the window they serve.
    ///
    /// `BridgeHandle` is not `Send` and stops its listener when dropped, so the
    /// handles are owned on the foreground thread for as long as their window exists.
    #[derive(Default)]
    struct Bridges(HashMap<WindowId, BridgeHandle>);

    impl Global for Bridges {}

    pub(super) fn install(window: &mut Window, cx: &mut App) {
        let channel = ReleaseChannel::current();

        let Some(app_id) = AppId::new(channel.app_id()).log_err() else {
            return;
        };
        let config = BridgeConfig::new(app_id, channel.display_name());

        let Some(handle) = BridgeHandle::install(window, cx, config).log_err() else {
            return;
        };

        log::info!(
            "UI automation bridge listening, descriptor {}",
            handle.endpoint_path().display()
        );

        let window_id = window.window_handle().window_id();
        registry(cx).0.insert(window_id, handle);
    }

    fn registry(cx: &mut App) -> &mut Bridges {
        if !cx.has_global::<Bridges>() {
            cx.on_window_closed(|cx, window_id| {
                if let Some(handle) = cx.default_global::<Bridges>().0.remove(&window_id) {
                    handle.shutdown();
                }
            })
            .detach();

            // The platform can exit without closing windows one by one, so the
            // descriptors would outlive the process without this.
            cx.on_app_quit(|cx| {
                for (_, handle) in cx.default_global::<Bridges>().0.drain() {
                    handle.shutdown();
                }
                async {}
            })
            .detach();
        }

        cx.default_global::<Bridges>()
    }
}

#[cfg(all(test, feature = "ui-automation"))]
mod tests {
    use dbflux_components::controls::{Input, InputState};
    use gpui::{
        AppContext as _, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _,
        Render, Styled as _, TestAppContext, VisualTestContext, Window, div,
    };
    use gpui_mcp::{Automation, UiTree};

    const SECRET: &str = "hunter2-automation-secret";

    struct PasswordField {
        state: Entity<InputState>,
        secret: bool,
    }

    impl Render for PasswordField {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .id("root")
                .size_full()
                .child(Input::new(&self.state).secret(self.secret))
        }
    }

    /// Renders a masked password input holding [`SECRET`] and returns the
    /// observed trees while masked and after a show-password toggle unmasks it.
    fn observe_masked_then_unmasked(secret: bool, cx: &mut TestAppContext) -> (UiTree, UiTree) {
        cx.update(gpui_component::init);

        let automation = Automation::isolated();
        let automation_for_window = automation.clone();
        let (view, visual) = cx.add_window_view(move |window, cx| {
            automation_for_window.attach(window);
            let state = cx.new(|cx| InputState::new(window, cx).masked(true));
            PasswordField { state, secret }
        });

        set_input(&view, visual, |state, window, cx| {
            state.set_value(SECRET, window, cx)
        });
        let masked = automation.snapshot();

        set_input(&view, visual, |state, window, cx| {
            state.set_masked(false, window, cx)
        });
        let unmasked = automation.snapshot();

        (masked, unmasked)
    }

    fn set_input(
        view: &Entity<PasswordField>,
        visual: &mut VisualTestContext,
        update: impl FnOnce(&mut InputState, &mut Window, &mut Context<InputState>),
    ) {
        visual.update(|window, cx| {
            let state = view.read(cx).state.clone();
            state.update(cx, |state, cx| update(state, window, cx));
        });
        visual.run_until_parked();
    }

    fn exposes_secret(tree: &UiTree) -> bool {
        tree.nodes.values().any(|node| {
            let text = node.text.as_ref().map(|text| text.text.as_str());
            let value = node.value.as_ref().map(|value| value.value.as_str());
            let label = node.label.as_deref();

            [text, value, label]
                .into_iter()
                .flatten()
                .any(|observed| observed.contains(SECRET))
        })
    }

    #[gpui::test]
    fn secret_input_value_never_reaches_automation(cx: &mut TestAppContext) {
        let (masked, unmasked) = observe_masked_then_unmasked(true, cx);

        assert!(
            !exposes_secret(&masked),
            "masked secret input exposed its value"
        );
        assert!(
            !exposes_secret(&unmasked),
            "unmasked secret input exposed its value"
        );
    }

    /// Control case: without `secret`, the observer does read an unmasked
    /// input's value, so the assertion above is not passing vacuously.
    #[gpui::test]
    fn plain_input_value_is_observed_once_unmasked(cx: &mut TestAppContext) {
        let (masked, unmasked) = observe_masked_then_unmasked(false, cx);

        assert!(!exposes_secret(&masked));
        assert!(exposes_secret(&unmasked));
    }
}
