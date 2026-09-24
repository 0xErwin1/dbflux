use crate::icons::AppIcon;
use crate::modals::shell::{ModalFocus, ModalShell, ModalVariant};
use crate::primitives::{Icon, Text, surface_raised};
use crate::tokens::{FontSizes, Heights, Spacing};
use crate::typography::AppFonts;
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::{Context, EventEmitter, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::button::{Button, ButtonVariants};

/// Debug selector of the box that shows the connection name, for layout tests.
pub const DELETE_CONNECTION_NAME_SELECTOR: &str = "delete-connection-name";

/// Outcome emitted when the user resolves the modal.
#[derive(Clone, Debug, PartialEq)]
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
    focus: ModalFocus,
}

impl ModalDeleteConnection {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
            focus: ModalFocus::new(cx),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: DeleteConnectionRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        self.focus.focus_on_next_render();
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Resolve the modal as if the primary button was clicked: emit the
    /// outcome and close. The keyboard path (ConfirmModal keymap) uses this
    /// so Enter resolves the modal through the same outcome handler as a
    /// mouse click.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        cx.emit(DeleteConnectionOutcome::Confirmed);
        self.close(cx);
    }

    /// Resolve the modal as if the cancel button was clicked.
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        cx.emit(DeleteConnectionOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<DeleteConnectionOutcome> for ModalDeleteConnection {}

impl Render for ModalDeleteConnection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

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
                    .debug_selector(|| DELETE_CONNECTION_NAME_SELECTOR.to_string())
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
            this.cancel(cx);
        });

        let on_confirm = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.confirm(cx);
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
