use crate::controls::Button;
use crate::icons::AppIcon;
use crate::modals::modal::{Modal, ModalFocus, ModalVariant};
use crate::primitives::Text;
use crate::tokens::{ModalMetrics, Spacing};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::{App, Context, EventEmitter, Window, div};

/// Element id of the confirmation's Cancel button, for tests.
pub const CONFIRM_CANCEL_ID: &str = "confirm-cancel";

/// Element id of the confirmation's primary button, for tests.
pub const CONFIRM_CONFIRM_ID: &str = "confirm-confirm";

/// Outcome emitted when the user resolves the confirmation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfirmOutcome {
    /// The user accepted the primary action.
    Confirmed,
    /// The user cancelled, dismissed or pressed Escape.
    Cancelled,
}

/// Request payload describing one confirmation.
///
/// Title, body and primary label stay in the caller's i18n namespace; the
/// shared `modals.confirm.*` keys only cover the Cancel button.
#[derive(Clone, Debug)]
pub struct ConfirmRequest {
    /// Title shown in the modal header.
    pub title: String,
    /// Body text describing what is about to happen.
    pub body: String,
    /// Tone of the card: `Danger` for destructive actions, `Default` otherwise.
    pub variant: ModalVariant,
    /// Label of the primary (confirm) button, in the caller's namespace.
    pub confirm_label: String,
}

/// Reusable confirmation modal (P1Modals) over the app's own [`Modal`].
///
/// The house hosting pattern (see `ModalDeleteConnection`): an [`Entity`]
/// owning the request and visibility plus a [`ModalFocus`], opened with
/// [`ModalConfirm::open`], resolved through the [`ConfirmOutcome`] event, with
/// `confirm`/`cancel` callable directly for keyboard paths and tests.
pub struct ModalConfirm {
    request: Option<ConfirmRequest>,
    visible: bool,
    focus: ModalFocus,
}

impl ModalConfirm {
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

    /// Opens the confirmation for `request`. Focus moves into the modal on its
    /// next render, so Escape reaches it without the caller touching focus.
    pub fn open(&mut self, request: ConfirmRequest, cx: &mut Context<Self>) {
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

    /// Whether the confirmation (or anything inside it) holds focus.
    pub fn contains_focus(&self, window: &Window, cx: &App) -> bool {
        self.focus.handle().contains_focused(window, cx)
    }

    /// Body of the request the confirmation is currently showing, so a host
    /// that re-renders with the request still pending re-opens only when the
    /// request changed.
    pub fn current_body(&self) -> Option<&str> {
        self.request.as_ref().map(|request| request.body.as_str())
    }

    /// Resolve the modal as if the primary button was clicked: emit
    /// [`ConfirmOutcome::Confirmed`] and close. The keyboard path (Enter, via
    /// `Modal::on_confirm`) uses this so it resolves through the same outcome
    /// handler as a mouse click. Resolving an already-closed confirmation is a
    /// no-op, so Enter and a click cannot emit twice.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        if !self.visible {
            return;
        }
        cx.emit(ConfirmOutcome::Confirmed);
        self.close(cx);
    }

    /// Resolve the modal as if the cancel button was clicked: emit
    /// [`ConfirmOutcome::Cancelled`] and close. Escape and a backdrop click
    /// land here through `Modal::on_close`. Resolving an already-closed
    /// confirmation is a no-op, like [`Self::confirm`].
    pub fn cancel(&mut self, cx: &mut Context<Self>) {
        if !self.visible {
            return;
        }
        cx.emit(ConfirmOutcome::Cancelled);
        self.close(cx);
    }
}

impl EventEmitter<ConfirmOutcome> for ModalConfirm {}

impl Render for ModalConfirm {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let is_danger = request.variant == ModalVariant::Danger;

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::body(request.body.clone()));

        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.cancel(cx);
        });

        let on_confirm = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.confirm(cx);
        });

        let confirm_button = if is_danger {
            Button::new(CONFIRM_CONFIRM_ID, request.confirm_label.clone())
                .danger()
                .on_click(on_confirm)
        } else {
            Button::new(CONFIRM_CONFIRM_ID, request.confirm_label.clone())
                .primary()
                .on_click(on_confirm)
        };

        let footer = div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(
                Button::new(CONFIRM_CANCEL_ID, dbflux_i18n::t!("modals.confirm.cancel"))
                    .on_click(on_cancel),
            )
            .child(confirm_button);

        let icon = if is_danger {
            AppIcon::TriangleAlert
        } else {
            AppIcon::CircleAlert
        };

        Modal::new(request.title.clone())
            .body(body)
            .footer(footer)
            .icon(icon)
            .variant(request.variant)
            .width(ModalMetrics::WIDTH)
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
// Tests — rendering, outcomes, focus
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
