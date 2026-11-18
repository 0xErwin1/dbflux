//! Rendering of `SecretDocument`: the breadcrumb, the secret laid out by its
//! view, the states in which the tab leaves the store alone, and the footer.

use super::{SecretDocument, SecretDocumentBlock};
use crate::chrome::{connection_segment, document_bar, document_footer, footer_key_hint};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{Breadcrumb, BreadcrumbSegment};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{BannerBlock, BannerVariant, Text};
use dbflux_components::tokens::{DocumentMetrics, SecretStoreMetrics};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

impl SecretDocument {
    /// The connection, the store and the secret.
    fn breadcrumb(&self, cx: &App) -> Breadcrumb {
        let mut segments: Vec<BreadcrumbSegment> =
            connection_segment(&self.app_state, self.profile_id, cx)
                .into_iter()
                .collect();

        segments.push(BreadcrumbSegment::new(self.store.label.clone()).icon(AppIcon::Lock, None));
        segments.push(BreadcrumbSegment::new(self.name.clone()).icon(AppIcon::KeyRound, None));

        Breadcrumb::new(segments).mono()
    }

    /// Where the secret lives: the store, the connection and the region.
    fn subtitle(&self) -> String {
        match self.region.as_deref() {
            Some(region) => dbflux_i18n::t!(
                "document.secret.subtitle",
                store = self.store.label.as_str(),
                connection = self.connection_name.as_str(),
                region = region
            ),
            None => dbflux_i18n::t!(
                "document.secret.subtitle_no_region",
                store = self.store.label.as_str(),
                connection = self.connection_name.as_str()
            ),
        }
    }

    /// The tab says why it leaves the store alone, inline: no toast, no
    /// action that would reach the store.
    fn render_blocked(&self, block: &SecretDocumentBlock, cx: &mut Context<Self>) -> AnyElement {
        let (id, title, body) = match block {
            SecretDocumentBlock::Removed => (
                "secret-document-removed",
                dbflux_i18n::t!("document.secret.removed.title"),
                dbflux_i18n::t!("document.secret.removed.body", name = self.name.as_str()),
            ),
            SecretDocumentBlock::RegionMismatch { live } => (
                "secret-document-region-mismatch",
                dbflux_i18n::t!("document.secret.region_mismatch.title"),
                dbflux_i18n::t!(
                    "document.secret.region_mismatch.body",
                    name = self.name.as_str(),
                    region = self.region.as_deref().unwrap_or_default(),
                    live = live.as_deref().unwrap_or_default()
                ),
            ),
        };
        let removed = matches!(block, SecretDocumentBlock::Removed);

        div()
            .id(id)
            .flex_1()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(DocumentMetrics::GAP)
            .p(SecretStoreMetrics::DETAILS_PADDING_X)
            .child(BannerBlock::new(BannerVariant::Warning, title).with_body(body))
            .when(removed, |panel| {
                panel.child(
                    Button::new(
                        "secret-document-close",
                        dbflux_i18n::t!("document.secret.close"),
                    )
                    .icon(AppIcon::X)
                    .tab_stop(false)
                    .on_click(cx.listener(|_, _, _, cx| {
                        cx.emit(crate::handle::DocumentEvent::RequestClose)
                    })),
                )
            })
            .into_any_element()
    }

    fn render_footer(&self, cx: &App) -> impl IntoElement {
        let keymap = dbflux_ui_base::effective_keymap();
        let hint = |command: Command, key: &'static str| {
            keymap
                .shortcut_for_command(ContextId::Results, command)
                .map(|shortcut| footer_key_hint(shortcut, dbflux_i18n::t!(key)))
        };
        let view = self.view.read(cx);
        let writable = view.supports_value_edit();
        let rolls_back = view.supports_roll_back();

        document_footer(cx)
            .child(
                div().flex_1().min_w_0().truncate().child(
                    Text::caption(dbflux_i18n::t!("document.secret.footer.value_scope"))
                        .muted_foreground(),
                ),
            )
            .children(hint(
                Command::ExpandCollapse,
                "document.secret.footer.hint.reveal",
            ))
            .children(hint(
                Command::ResultsCopyCell,
                "document.secret_store.footer.hint.copy_arn",
            ))
            .children(
                hint(Command::EditValue, "document.secret.footer.hint.edit").filter(|_| writable),
            )
            .children(
                hint(
                    Command::RollBackVersion,
                    "document.secret.footer.hint.roll_back",
                )
                .filter(|_| rolls_back),
            )
    }
}

impl Render for SecretDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.open_requested_edit(window, cx);
        self.view
            .update(cx, |view, cx| view.prepare_frame(window, cx));

        let context = self.active_context(cx);
        let subtitle = self.subtitle();

        let draft_kept = self.view.read(cx).is_editing_value();
        let body = match self.block(cx) {
            // A deleted secret under unsaved text keeps the text on screen,
            // read-only, under the notice that it was deleted.
            Some(SecretDocumentBlock::Removed) if draft_kept => {
                let name = self.name.clone();
                self.view
                    .update(cx, |view, cx| view.render_tab_editor(&name, subtitle, cx))
            }
            Some(block) => self.render_blocked(&block, cx),
            None => self.view.update(cx, |view, cx| {
                view.render_document_body(context, subtitle, cx)
            }),
        };

        let mut root = div()
            .id("secret-document")
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().popover)
            .child(document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx).child(self.breadcrumb(cx)))
            .child(body)
            .child(self.render_footer(cx));

        if let Some(dialog) = self
            .view
            .update(cx, |view, cx| view.render_write_dialog(cx))
        {
            root = root.child(dialog);
        }

        root
    }
}
