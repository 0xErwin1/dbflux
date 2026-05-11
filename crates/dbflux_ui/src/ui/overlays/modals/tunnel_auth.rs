use crate::ui::tokens::{FontSizes, Spacing};
use dbflux_components::controls::{Checkbox, Input, InputState};
use dbflux_components::primitives::{BannerBlock, BannerVariant, surface_raised};
use gpui::prelude::*;
use gpui::{Context, EventEmitter, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::button::{Button, ButtonVariants};
use uuid::Uuid;

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

/// Modal entity for SSH passphrase prompt.
///
/// Uses `ModalShell::Default` (480 px). The parent opens it via
/// `pending_tunnel_auth_open: Option<TunnelAuthRequest>` and subscribes to
/// `TunnelAuthOutcome` events.
pub struct ModalTunnelAuth {
    request: Option<TunnelAuthRequest>,
    visible: bool,
    passphrase_input: gpui::Entity<InputState>,
    remember: bool,
}

impl ModalTunnelAuth {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let passphrase_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Enter passphrase")
                .masked(true)
        });
        Self {
            request: None,
            visible: false,
            passphrase_input,
            remember: true,
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
        self.request = Some(request);
        self.visible = true;
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        cx.notify();
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

        let tunnel_name = request.tunnel_name.clone();
        let host = request.host.clone();
        let port = request.port;
        let user = request.user.clone();
        let last_attempt_failed = request.last_attempt_failed;

        let passphrase_value = self.passphrase_input.read(cx).value().to_string();
        let connect_enabled = TunnelAuthRequest::validate_passphrase(&passphrase_value).is_ok();

        let remember = self.remember;

        // Error banner shown when a previous attempt with the same modal was rejected.
        let error_banner = if last_attempt_failed {
            Some(
                BannerBlock::new(BannerVariant::Danger, "Incorrect passphrase. Try again.")
                    .into_any_element(),
            )
        } else {
            None
        };

        // Connection details pre-block: "Host: host:port · User: user"
        let connection_detail = format!("Host: {}:{} · User: {}", host, port, user);

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .when_some(error_banner, |el, banner| el.child(banner))
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .child(format!(
                        "Enter the passphrase for tunnel \"{}\"",
                        tunnel_name
                    )),
            )
            .child(
                surface_raised(cx)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .font_family(dbflux_components::typography::AppFonts::MONO)
                            .text_color(theme.muted_foreground)
                            .child(connection_detail),
                    ),
            )
            .child(
                div().w_full().child(
                    Input::new(&self.passphrase_input)
                        .w_full()
                        .placeholder("Enter passphrase"),
                ),
            )
            .child(
                Checkbox::new("tunnel-auth-remember")
                    .checked(remember)
                    .label("Remember for this session")
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.remember = *checked;
                        cx.notify();
                    })),
            );

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(TunnelAuthOutcome::Cancelled);
            this.close(cx);
        });

        let on_connect = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            let passphrase = this.passphrase_input.read(cx).value().to_string();
            if TunnelAuthRequest::validate_passphrase(&passphrase).is_err() {
                return;
            }
            let remember = this.remember;
            cx.emit(TunnelAuthOutcome::Provided {
                passphrase,
                remember,
            });
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("tunnel-auth-cancel")
                    .label("Cancel")
                    .on_click(on_cancel),
            )
            .child(
                Button::new("tunnel-auth-connect")
                    .label("Connect")
                    .primary()
                    .disabled(!connect_enabled)
                    .on_click(on_connect),
            );

        use super::shell::{ModalShell, ModalVariant};

        ModalShell::new(
            "SSH passphrase required",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(480.0))
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
}
