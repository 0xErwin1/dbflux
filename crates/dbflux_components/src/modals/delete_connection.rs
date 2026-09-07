use crate::icons::AppIcon;
use crate::modals::shell::{ModalShell, ModalVariant};
use crate::primitives::{Icon, Text, surface_raised};
use crate::tokens::{FontSizes, Heights, Spacing};
use crate::typography::AppFonts;
use gpui::prelude::*;
use gpui::{Context, EventEmitter, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::button::{Button, ButtonVariants};

/// Outcome emitted when the user resolves the modal.
#[derive(Clone, Debug)]
pub enum DeleteConnectionOutcome {
    Confirmed,
    Cancelled,
}

/// Request payload used via `pending_modal_open` on the sidebar/workspace.
#[derive(Clone, Debug)]
pub struct DeleteConnectionRequest {
    /// Display name of the connection to delete.
    pub connection_name: String,
    /// Whether there are open documents for this connection.
    pub has_open_documents: bool,
}

/// Modal entity for confirming connection deletion.
///
/// Uses `ModalShell::Danger` (460 px, 2 px red top-border).
/// The parent opens via `pending_modal_open: Option<DeleteConnectionRequest>` and
/// subscribes to `DeleteConnectionOutcome` events.
pub struct ModalDeleteConnection {
    request: Option<DeleteConnectionRequest>,
    visible: bool,
}

impl ModalDeleteConnection {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: DeleteConnectionRequest, cx: &mut Context<Self>) {
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

impl EventEmitter<DeleteConnectionOutcome> for ModalDeleteConnection {}

impl Render for ModalDeleteConnection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let connection_name = request.connection_name.clone();
        let has_open_documents = request.has_open_documents;

        // Body: warning icon + description + connection name badge + optional sub-line.
        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(Spacing::SM)
                    .child(
                        Icon::new(AppIcon::TriangleAlert)
                            .size(Heights::ICON_SM)
                            .color(theme.danger),
                    )
                    .child(
                        // flex_1 + min_w_0 lets the description wrap to the
                        // modal's width instead of overflowing past the
                        // card edge (same pattern as the toast/banner fix).
                        div().flex_1().min_w_0().child(
                            Text::body(dbflux_i18n::t!("modals.delete_connection.warning"))
                                .into_any_element(),
                        ),
                    ),
            )
            .child(
                surface_raised(cx)
                    .w_full()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::SM)
                            .font_family(AppFonts::MONO)
                            .text_color(theme.foreground)
                            .child(connection_name),
                    ),
            )
            .when(has_open_documents, |el| {
                el.child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child(dbflux_i18n::t!("modals.delete_connection.documents_closed")),
                )
            });

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DeleteConnectionOutcome::Cancelled);
            this.close(cx);
        });

        let on_confirm = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(DeleteConnectionOutcome::Confirmed);
            this.close(cx);
        });

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("delete-conn-cancel")
                    .label(dbflux_i18n::t!("modals.delete_connection.cancel"))
                    .on_click(on_cancel),
            )
            .child(
                Button::new("delete-conn-confirm")
                    .label(dbflux_i18n::t!("modals.delete_connection.confirm"))
                    .danger()
                    .on_click(on_confirm),
            );

        ModalShell::new(
            dbflux_i18n::t!("modals.delete_connection.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(460.0))
        .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests — translation key resolution
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #[test]
    fn delete_connection_keys_resolve_in_both_locales() {
        let keys = [
            "modals.delete_connection.title",
            "modals.delete_connection.warning",
            "modals.delete_connection.documents_closed",
            "modals.delete_connection.cancel",
            "modals.delete_connection.confirm",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn delete_connection_confirm_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.delete_connection.confirm", locale = "en");
        let es = dbflux_i18n::t!("modals.delete_connection.confirm", locale = "es");
        assert_ne!(en, es);
    }
}
