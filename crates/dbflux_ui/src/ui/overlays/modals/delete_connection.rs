use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Spacing};
use dbflux_components::primitives::{Icon, Text, surface_raised};
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
                    .child(Icon::new(AppIcon::TriangleAlert).size(px(16.0)).color(theme.danger))
                    .child(
                        Text::body(
                            "You're about to delete the following connection. This can't be undone.",
                        )
                        .into_any_element(),
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
                            .font_family(dbflux_components::typography::AppFonts::MONO)
                            .text_color(theme.foreground)
                            .child(connection_name),
                    ),
            )
            .when(has_open_documents, |el| {
                el.child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child("Any open documents using this connection will be closed."),
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
                    .label("Cancel")
                    .on_click(on_cancel),
            )
            .child(
                Button::new("delete-conn-confirm")
                    .label("Delete")
                    .danger()
                    .on_click(on_confirm),
            );

        use super::shell::{ModalShell, ModalVariant};

        ModalShell::new(
            "Delete connection",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Danger)
        .width(px(460.0))
        .into_any_element()
    }
}
