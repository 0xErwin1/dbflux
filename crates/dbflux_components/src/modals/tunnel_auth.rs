use crate::controls::{Button, Checkbox, Input, InputState};
use crate::icons::AppIcon;
use crate::modals::modal::{Modal, ModalFocus, ModalVariant};
use crate::primitives::{BannerBlock, BannerVariant, Icon};
use crate::tokens::{ChromeColors, ModalMetrics, Spacing};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::{Context, EventEmitter, Focusable, Pixels, Subscription, Window, div, px};
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Width of the SSH passphrase dialog (P1Modals).
const TUNNEL_AUTH_WIDTH: Pixels = px(460.0);

/// Outcome emitted when the user resolves the modal.
#[derive(Clone, Debug)]
pub enum TunnelAuthOutcome {
    /// User supplied a passphrase and clicked Connect.
    Provided { passphrase: String, remember: bool },
    /// User cancelled — the connection attempt should be abandoned.
    Cancelled,
}

/// Request payload used to open `ModalTunnelAuth`.
#[derive(Clone, Debug)]
pub struct TunnelAuthRequest {
    /// The SSH tunnel profile UUID (used as the vault key).
    pub tunnel_id: Uuid,
    /// Friendly display name for the tunnel profile (shown in the sub-line).
    pub tunnel_name: String,
    /// SSH server hostname.
    pub host: String,
    /// SSH server port.
    pub port: u16,
    /// SSH username.
    pub user: String,
    /// When true, shows an inline "Incorrect passphrase. Try again." banner.
    pub last_attempt_failed: bool,
}

impl TunnelAuthRequest {
    /// Validate a passphrase value.
    ///
    /// Returns `Err` with a human-readable message when the passphrase is empty,
    /// which is used to disable the Connect button.
    pub fn validate_passphrase(passphrase: &str) -> Result<(), &'static str> {
        if passphrase.is_empty() {
            Err("Passphrase cannot be empty")
        } else {
            Ok(())
        }
    }
}

/// `user@host:port` of the tunnel's SSH server.
fn tunnel_address(user: &str, host: &str, port: u16) -> String {
    format!("{user}@{host}:{port}")
}

/// Modal entity for SSH passphrase prompt.
///
/// Uses `Modal` (`ModalVariant::Default`) (460 px). The parent opens it via
/// `pending_tunnel_auth_open: Option<TunnelAuthRequest>` and subscribes to
/// `TunnelAuthOutcome` events.
pub struct ModalTunnelAuth {
    request: Option<TunnelAuthRequest>,
    visible: bool,
    passphrase_input: gpui::Entity<InputState>,
    remember: bool,
    show_passphrase: bool,
    focus: ModalFocus,
    _input_observation: Subscription,
}

impl ModalTunnelAuth {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let passphrase_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("modals.tunnel_auth.placeholder"))
                .masked(true)
        });

        // The Connect button and Enter both follow the typed passphrase, so
        // every edit re-renders the modal.
        let input_observation = cx.observe(&passphrase_input, |_, _, cx| cx.notify());

        Self {
            request: None,
            visible: false,
            passphrase_input,
            remember: true,
            show_passphrase: false,
            focus: ModalFocus::new(cx),
            _input_observation: input_observation,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal with the given request.
    ///
    /// Resets the passphrase field and sets the remember checkbox to checked.
    pub fn open(
        &mut self,
        request: TunnelAuthRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.passphrase_input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });
        self.remember = true;
        self.set_passphrase_visible(false, window, cx);
        self.request = Some(request);
        self.visible = true;

        let input_focus = self.passphrase_input.read(cx).focus_handle(cx);
        self.focus.focus(Some(&input_focus), window, cx);

        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Connect with the typed passphrase, as the Connect button does. Does
    /// nothing while the passphrase is empty.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        let passphrase = self.passphrase_input.read(cx).value().to_string();
        if TunnelAuthRequest::validate_passphrase(&passphrase).is_err() {
            return;
        }

        cx.emit(TunnelAuthOutcome::Provided {
            passphrase,
            remember: self.remember,
        });
        self.close(cx);
    }

    /// Shows or hides the typed passphrase, as the eye in the field does.
    fn toggle_passphrase_visibility(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.set_passphrase_visible(!self.show_passphrase, window, cx);
        cx.notify();
    }

    fn set_passphrase_visible(
        &mut self,
        visible: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.show_passphrase = visible;
        self.passphrase_input.update(cx, |state, cx| {
            state.set_masked(!visible, window, cx);
        });
    }

    /// Abandon the connection attempt.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(TunnelAuthOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<TunnelAuthOutcome> for ModalTunnelAuth {}

impl Render for ModalTunnelAuth {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();

        let passphrase_value = self.passphrase_input.read(cx).value().to_string();
        let connect_enabled = TunnelAuthRequest::validate_passphrase(&passphrase_value).is_ok();

        let remember = self.remember;
        let show_passphrase = self.show_passphrase;

        let error_banner = request.last_attempt_failed.then(|| {
            BannerBlock::new(
                BannerVariant::Danger,
                dbflux_i18n::t!("modals.tunnel_auth.incorrect"),
            )
        });

        let tunnel_line = div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap(Spacing::XS)
            .text_color(theme.foreground)
            .child(dbflux_i18n::t!("modals.tunnel_auth.tunnel_label"))
            .child(
                div()
                    .text_color(ChromeColors::strong(theme))
                    .child(request.tunnel_name.clone()),
            )
            .child("\u{00B7}")
            .child(
                div()
                    .font_family(crate::fonts::editor_family(cx))
                    .child(tunnel_address(&request.user, &request.host, request.port)),
            );

        let (toggle_icon, toggle_label) = if show_passphrase {
            (
                AppIcon::EyeOff,
                dbflux_i18n::t!("modals.tunnel_auth.hide_passphrase"),
            )
        } else {
            (
                AppIcon::Eye,
                dbflux_i18n::t!("modals.tunnel_auth.show_passphrase"),
            )
        };

        let passphrase_field = Input::new(&self.passphrase_input)
            .secret(true)
            .w_full()
            .aria_label(dbflux_i18n::t!("modals.tunnel_auth.placeholder"))
            .prefix(
                Icon::new(AppIcon::Lock)
                    .size(ModalMetrics::LIST_ICON)
                    .color(theme.muted_foreground),
            )
            .suffix(
                Button::new("tunnel-auth-reveal", toggle_label)
                    .ghost()
                    .inline()
                    .icon(toggle_icon)
                    .icon_only()
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.toggle_passphrase_visibility(window, cx);
                    })),
            );

        let body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .when_some(error_banner, |body, banner| body.child(banner))
            .child(tunnel_line)
            .child(passphrase_field)
            .child(
                Checkbox::new("tunnel-auth-remember")
                    .checked(remember)
                    .label(dbflux_i18n::t!("modals.tunnel_auth.remember"))
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.remember = *checked;
                        cx.notify();
                    })),
            );

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.cancel(cx);
        });

        let on_connect = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.confirm(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(
                Button::new(
                    "tunnel-auth-cancel",
                    dbflux_i18n::t!("modals.tunnel_auth.cancel"),
                )
                .on_click(on_cancel),
            )
            .child(
                Button::new(
                    "tunnel-auth-connect",
                    dbflux_i18n::t!("modals.tunnel_auth.connect"),
                )
                .primary()
                .icon(AppIcon::Plug)
                .when_some(
                    crate::actions::shortcut_label(
                        cx,
                        dbflux_core::keymap_types::ContextId::Modal.id(),
                        dbflux_core::keymap_types::Command::Execute.id(),
                        "\u{21B5}",
                    ),
                    Button::kbd,
                )
                .disabled(!connect_enabled)
                .on_click(on_connect),
            );

        Modal::new(dbflux_i18n::t!("modals.tunnel_auth.title"))
            .body(body)
            .footer(footer)
            .icon(AppIcon::KeyRound)
            .variant(ModalVariant::Default)
            .width(TUNNEL_AUTH_WIDTH)
            .focus_handle(self.focus.handle())
            .on_close({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.cancel(cx)).log_err();
                }
            })
            .on_confirm({
                let entity = cx.entity().downgrade();
                move |_, cx| {
                    entity.update(cx, |this, cx| this.confirm(cx)).log_err();
                }
            })
            .confirm_enabled(connect_enabled)
            .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests — pure validation logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_passphrase_fails_validation() {
        assert!(TunnelAuthRequest::validate_passphrase("").is_err());
    }

    #[test]
    fn non_empty_passphrase_passes_validation() {
        assert!(TunnelAuthRequest::validate_passphrase("hunter2").is_ok());
        assert!(TunnelAuthRequest::validate_passphrase(" ").is_ok());
    }

    #[test]
    fn tunnel_auth_keys_resolve_in_both_locales() {
        let keys = [
            "modals.tunnel_auth.title",
            "modals.tunnel_auth.tunnel_label",
            "modals.tunnel_auth.show_passphrase",
            "modals.tunnel_auth.hide_passphrase",
            "modals.tunnel_auth.placeholder",
            "modals.tunnel_auth.incorrect",
            "modals.tunnel_auth.empty_error",
            "modals.tunnel_auth.remember",
            "modals.tunnel_auth.cancel",
            "modals.tunnel_auth.connect",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn tunnel_auth_connect_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.tunnel_auth.connect", locale = "en");
        let es = dbflux_i18n::t!("modals.tunnel_auth.connect", locale = "es");
        assert_ne!(en, es);
    }

    #[test]
    fn tunnel_address_joins_user_host_and_port() {
        assert_eq!(
            tunnel_address("ec2-user", "bastion.example.com", 22),
            "ec2-user@bastion.example.com:22"
        );
    }
}
