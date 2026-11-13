//! The bar at the bottom of the details pane: the writes of the selected
//! secret as buttons — Edit value and Delete, and the ones the store offers
//! beyond them. A button the store cannot do is not drawn; one the secret's
//! own state refuses (another service manages it, or its deletion is
//! scheduled) is drawn disabled, with the reason in its tooltip.

use crate::secret_store::ValueState;
use crate::secret_store::view::{SecretView, SecretViewEvent};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, SecretStoreMetrics};
use dbflux_core::SecretStoreCapabilities;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

impl SecretView {
    /// Whether the store takes writes, so Edit value is something to offer.
    pub(crate) fn supports_value_edit(&self) -> bool {
        self.store_allows(SecretStoreCapabilities::WRITE)
    }

    /// Whether Edit value may be pressed: the secret takes writes, no editor
    /// or dialog is open, and the value is either loaded as text or can be
    /// retrieved first.
    pub(crate) fn can_request_edit_value(&self) -> bool {
        if self.value_edit.is_some()
            || !self.connection_present
            || self.dialog_is_open()
            || !self.store_allows(SecretStoreCapabilities::WRITE)
            || self.write_refusal_reason().is_some()
        {
            return false;
        }

        match &self.value {
            ValueState::Loaded(value) => !value.content.is_binary(),
            ValueState::Loading => false,
            ValueState::NotLoaded | ValueState::Failed(_) => self.can_retrieve_value(),
        }
    }

    /// Edit value: opens the editor on the loaded value. A value that is not
    /// loaded is retrieved first — the retrieval is audited like any other —
    /// and the editor opens when it lands.
    pub(crate) fn request_edit_value(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_request_edit_value() {
            return;
        }

        // The side pane never hosts the editor: the secret's tab does, and it
        // retrieves the value first when it is not loaded.
        if self.edit_in_tab
            && let Some(name) = self.name.clone()
        {
            cx.emit(SecretViewEvent::OpenInTab { name, edit: true });
            return;
        }

        if self.can_edit_value() {
            self.start_value_edit(window, cx);
            return;
        }

        self.retrieve_selected_value(cx);
        self.edit_after_retrieve = true;
    }

    /// Opens the editor the bar asked for once its retrieval landed, and
    /// forgets the request when the retrieval failed. Runs on the render
    /// pass, which has the window the editor is built with.
    pub(in crate::secret_store) fn open_pending_edit(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.edit_after_retrieve {
            return;
        }

        match &self.value {
            ValueState::Loaded(_) => {
                self.edit_after_retrieve = false;
                self.start_value_edit(window, cx);
            }
            ValueState::Failed(_) | ValueState::NotLoaded => self.edit_after_retrieve = false,
            ValueState::Loading => {}
        }
    }

    /// The buttons of the bar, left to right, with the reason a refused one
    /// is disabled.
    pub(in crate::secret_store) fn render_action_bar(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<Stateful<Div>> {
        if !self.store_allows(SecretStoreCapabilities::WRITE) {
            return None;
        }

        let refusal = self.write_refusal_reason();
        let refusal = refusal.as_deref();

        let mut buttons: Vec<AnyElement> = vec![self.edit_value_button(refusal, cx)];

        if self.supports_description_edit() && self.description_edit_rewrites_value() {
            buttons.push(self.edit_description_button(refusal, cx));
        }

        if self.supports_rotation() {
            buttons.push(self.rotate_now_button(cx));
        }

        if self.supports_delete() {
            buttons.push(self.delete_button(refusal, cx));
        }

        let theme = cx.theme();

        Some(
            div()
                .id("secret-action-bar")
                .flex()
                .flex_wrap()
                .items_center()
                .gap(DocumentMetrics::GAP)
                .px(SecretStoreMetrics::DETAILS_PADDING_X)
                .py(SecretStoreMetrics::DETAILS_PADDING_Y)
                .border_t_1()
                .border_color(theme.border)
                .text_color(ChromeColors::strong(theme))
                .children(buttons),
        )
    }

    fn edit_value_button(&self, refusal: Option<&str>, cx: &mut Context<Self>) -> AnyElement {
        blocked(
            Button::new(
                "secret-action-edit-value",
                dbflux_i18n::t!("document.secret_store.edit.open"),
            )
            .icon(AppIcon::Pencil)
            .tab_stop(false)
            .disabled(!self.can_request_edit_value())
            .on_click(cx.listener(|this, _, window, cx| this.request_edit_value(window, cx))),
            refusal,
        )
        .into_any_element()
    }

    fn edit_description_button(&self, refusal: Option<&str>, cx: &mut Context<Self>) -> AnyElement {
        blocked(
            Button::new(
                "secret-action-edit-description",
                dbflux_i18n::t!("document.secret_store.description_edit.button"),
            )
            .icon(AppIcon::Pencil)
            .tab_stop(false)
            .disabled(!self.can_edit_description())
            .on_click(cx.listener(|this, _, window, cx| this.open_edit_description(window, cx))),
            refusal,
        )
        .into_any_element()
    }

    fn rotate_now_button(&self, cx: &mut Context<Self>) -> AnyElement {
        let button = Button::new(
            "secret-action-rotate-now",
            dbflux_i18n::t!("document.secret_store.rotate.button"),
        )
        .icon(AppIcon::Zap)
        .tab_stop(false)
        .disabled(!self.can_rotate_now())
        .on_click(cx.listener(|this, _, window, cx| this.rotate_selected_now(window, cx)));

        match self.rotate_now_block_reason() {
            Some(reason) => crate::secret_store::view::details::rotate_with_reason(button, reason),
            None => button,
        }
        .into_any_element()
    }

    fn delete_button(&self, refusal: Option<&str>, cx: &mut Context<Self>) -> AnyElement {
        blocked(
            Button::new(
                "secret-action-delete",
                dbflux_i18n::t!("document.secret_store.delete.button"),
            )
            .danger()
            .icon(AppIcon::Delete)
            .tab_stop(false)
            .disabled(!self.can_delete_selected())
            .on_click(cx.listener(|this, _, window, cx| this.open_delete(window, cx))),
            refusal,
        )
        .into_any_element()
    }
}

/// A button the secret's own state refuses, disabled with the reason as its
/// tooltip.
fn blocked(button: Button, refusal: Option<&str>) -> Button {
    match refusal {
        Some(reason) => button.disabled(true).tooltip(reason.to_string()),
        None => button,
    }
}
