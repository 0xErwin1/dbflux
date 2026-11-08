//! The secret laid out as a tab: the header of artboard 02, then its sections
//! in the artboard's order (value, versions, rotation, details, tags), and the
//! editor alone while a value is being edited (artboard 06). Every section is
//! the one the details pane draws.

use super::SecretView;
use crate::secret_store::list::rotation_chip;
use dbflux_app::keymap::ContextId;
use dbflux_components::controls::Button;
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, Icon};
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, SecretStoreMetrics};
use dbflux_core::SecretStoreCapabilities;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

impl SecretView {
    /// The body of a document of the shown secret. `subtitle` names where it
    /// lives (store, connection, region), which only the host knows.
    pub(crate) fn render_document_body(
        &mut self,
        host_context: ContextId,
        subtitle: String,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        self.host_context = host_context;

        let Some(summary) = self.selected_summary().cloned() else {
            return div().flex_1().into_any_element();
        };

        let sections = self.detail_sections(&summary, cx);
        let header = self.render_document_header(&summary, subtitle, cx);
        let editing = self.is_editing_value();

        // The artboard stacks the sections in one column, in this order.
        let column: Vec<AnyElement> = if editing {
            vec![sections.value]
        } else {
            [
                Some(sections.value),
                sections.managed,
                sections.status,
                sections.versions,
                sections.rotation,
                sections.metadata,
                sections.tags,
            ]
            .into_iter()
            .flatten()
            .collect()
        };
        let columns = div()
            .id("secret-document-sections")
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::SECTION_GAP)
            .children(column);

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(header)
            .child(
                div()
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .child(
                        div()
                            .id("secret-document-body")
                            .size_full()
                            .track_scroll(&self.details_scroll)
                            .overflow_y_scroll()
                            .px(SecretStoreMetrics::DETAILS_PADDING_X)
                            .py(SecretStoreMetrics::DETAILS_PADDING_Y)
                            .child(columns),
                    )
                    .child(Scrollbar::vertical(&self.details_scroll)),
            )
            .into_any_element()
    }

    /// The name, where it lives, the rotation chip, and Copy ARN, Refresh and
    /// Delete.
    fn render_document_header(
        &self,
        summary: &dbflux_core::SecretSummary,
        subtitle: String,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let border = theme.border;
        let warning = theme.warning;
        let danger = theme.danger;
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let chip = self
            .store
            .supports(SecretStoreCapabilities::ROTATION)
            .then(|| rotation_chip(summary.rotation.as_ref()));
        let has_resource_id = !self.hierarchical();

        div()
            .id("secret-document-header")
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .px(SecretStoreMetrics::DETAILS_PADDING_X)
            .py(SecretStoreMetrics::DETAILS_PADDING_Y)
            .border_b_1()
            .border_color(border)
            .child(
                Icon::new(AppIcon::KeyRound)
                    .size(DocumentMetrics::TITLE_ICON)
                    .color(warning),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .child(
                        div()
                            .id("secret-document-name")
                            .truncate()
                            .font_family(fonts::editor_family(cx))
                            .font_weight(FontWeight::BOLD)
                            .text_size(SecretStoreMetrics::NAME_FONT)
                            .text_color(strong)
                            .child(summary.name.clone()),
                    )
                    .child(
                        div()
                            .id("secret-document-subtitle")
                            .truncate()
                            .text_size(DocumentMetrics::TABLE_META_FONT)
                            .text_color(muted)
                            .child(subtitle),
                    ),
            )
            .children(
                chip.map(|chip| Badge::new(chip.label, chip.tone).id("secret-document-rotation")),
            )
            .when(has_resource_id, |header| {
                header.child(
                    Button::new(
                        "secret-document-copy-arn",
                        dbflux_i18n::t!("document.secret_store.action.copy_resource_id"),
                    )
                    .icon(AppIcon::Copy)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, _, cx| this.copy_resource_id(cx))),
                )
            })
            .child(
                Button::new(
                    "secret-document-refresh",
                    dbflux_i18n::t!("document.secret_store.toolbar.refresh"),
                )
                .icon(AppIcon::RefreshCcw)
                .tab_stop(false)
                .disabled(!self.connection_present)
                .on_click(cx.listener(|this, _, _, cx| this.refresh_shown(cx))),
            )
            .when(self.supports_delete(), |header| {
                header.child(
                    Button::new(
                        "secret-document-delete",
                        dbflux_i18n::t!("document.secret_store.delete.button"),
                    )
                    .icon(AppIcon::Delete)
                    .text_color(danger)
                    .tab_stop(false)
                    .disabled(!self.can_delete_selected())
                    .on_click(cx.listener(|this, _, window, cx| this.open_delete(window, cx))),
                )
            })
    }
}
