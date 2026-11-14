//! The secret laid out as a tab: the header of artboard 02, then its sections
//! in the artboard's order (value, versions, rotation, details, tags), and the
//! editor alone while a value is being edited (artboard 06). Every section is
//! the one the details pane draws.

use super::{DetailsState, SecretView};
use dbflux_app::keymap::ContextId;
use dbflux_components::controls::Button;
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, BannerBlock, BannerVariant, Icon, Text};
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, SecretStoreMetrics};
use dbflux_core::SecretStoreCapabilities;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

/// Width of the tab's right-hand column of cards.
/// The least the main column keeps before the side column wraps below it:
/// room for the versions table with its staging labels.
const MAIN_COLUMN_MIN_WIDTH: Pixels = px(520.0); // guardrail-allow: width the versions table needs, no token carries it
const SIDE_COLUMN_WIDTH: Pixels = px(470.0); // guardrail-allow: artboard 02 right column, no token carries 470 px

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
            return self.render_document_pending(subtitle, cx);
        };

        // While a value is edited the editor takes the whole tab.
        if self.is_editing_value() {
            return self.render_tab_editor(&summary.name, subtitle, cx);
        }

        let sections = self.detail_sections(&summary, cx);
        let header = self.render_document_header(&summary, subtitle, cx);

        // The artboard's two columns: the value and its versions on the left,
        // the rotation, details and tags in a fixed column on the right.
        let left: Vec<AnyElement> = [
            Some(sections.value),
            sections.managed,
            sections.status,
            sections.versions,
        ]
        .into_iter()
        .flatten()
        .collect();
        let right: Vec<AnyElement> = [sections.rotation, sections.metadata, sections.tags]
            .into_iter()
            .flatten()
            .collect();

        let column = |id: &'static str, sections: Vec<AnyElement>| {
            div()
                .id(id)
                .flex()
                .flex_col()
                .min_w_0()
                .gap(SecretStoreMetrics::SECTION_GAP)
                .children(sections)
        };
        // The columns wrap: the side column drops below the main one when the
        // tab cannot hold both, so nothing is squeezed out of the versions.
        let columns = div()
            .id("secret-document-sections")
            .flex()
            .flex_wrap()
            .items_start()
            .gap(SecretStoreMetrics::SECTION_GAP)
            .child(
                column("secret-document-main", left)
                    .flex_grow(1.0)
                    .flex_basis(MAIN_COLUMN_MIN_WIDTH)
                    .min_w(MAIN_COLUMN_MIN_WIDTH),
            )
            .when(!right.is_empty(), |columns| {
                columns.child(
                    column("secret-document-side", right)
                        .w(SIDE_COLUMN_WIDTH)
                        .max_w_full()
                        .flex_shrink_0(),
                )
            });

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
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let rotating = summary
            .rotation
            .as_ref()
            .is_some_and(|rotation| rotation.enabled);
        let chip = self
            .store
            .supports(SecretStoreCapabilities::ROTATION)
            .then(|| {
                if rotating {
                    (
                        dbflux_i18n::t!("document.secret.rotation_badge.on"),
                        BadgeTone::Success,
                    )
                } else {
                    (
                        dbflux_i18n::t!("document.secret.rotation_badge.off"),
                        BadgeTone::Neutral,
                    )
                }
            });

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
                chip.map(|(label, tone)| Badge::new(label, tone).id("secret-document-rotation")),
            )
            .children(self.document_header_actions(cx))
    }

    /// Copy ARN (when the store has one), Refresh and Delete.
    fn document_header_actions(&self, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let danger = cx.theme().danger;
        let mut actions = Vec::new();

        if !self.hierarchical() {
            actions.push(
                Button::new(
                    "secret-document-copy-arn",
                    dbflux_i18n::t!("document.secret_store.action.copy_resource_id"),
                )
                .icon(AppIcon::Copy)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.copy_resource_id(cx)))
                .into_any_element(),
            );
        }

        actions.push(
            Button::new(
                "secret-document-refresh",
                dbflux_i18n::t!("document.secret_store.toolbar.refresh"),
            )
            .icon(AppIcon::RefreshCcw)
            .tab_stop(false)
            .disabled(!self.connection_present)
            .on_click(cx.listener(|this, _, _, cx| this.refresh_shown(cx)))
            .into_any_element(),
        );

        if self.supports_delete() {
            actions.push(
                Button::new(
                    "secret-document-delete",
                    dbflux_i18n::t!("document.secret_store.delete.button"),
                )
                .icon(AppIcon::Delete)
                .text_color(danger)
                .tab_stop(false)
                .disabled(!self.can_delete_selected())
                .on_click(cx.listener(|this, _, window, cx| this.open_delete(window, cx)))
                .into_any_element(),
            );
        }

        actions
    }

    /// A tab that has no summary yet: the secret is still being described, or
    /// the describe failed. It says which, keeps Refresh, and offers a retry,
    /// so a new tab is never blank.
    fn render_document_pending(&mut self, subtitle: String, cx: &mut Context<Self>) -> AnyElement {
        let Some(name) = self.name.clone() else {
            return div().flex_1().into_any_element();
        };

        let theme = cx.theme();
        let border = theme.border;
        let warning = theme.warning;
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;

        let header = div()
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
                            .child(name),
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
            .child(
                Button::new(
                    "secret-document-refresh",
                    dbflux_i18n::t!("document.secret_store.toolbar.refresh"),
                )
                .icon(AppIcon::RefreshCcw)
                .tab_stop(false)
                .disabled(!self.connection_present)
                .on_click(cx.listener(|this, _, _, cx| this.refresh_shown(cx))),
            );

        let body: AnyElement = match &self.details {
            DetailsState::Failed(error) => div()
                .id("secret-document-details-failed")
                .flex()
                .flex_col()
                .gap(DocumentMetrics::GAP)
                .child(
                    BannerBlock::new(
                        BannerVariant::Danger,
                        dbflux_i18n::t!("document.secret_store.details.failed"),
                    )
                    .with_body(error.clone()),
                )
                .child(
                    div().flex().child(
                        Button::new(
                            "secret-document-retry",
                            dbflux_i18n::t!("document.secret.retry"),
                        )
                        .icon(AppIcon::RefreshCcw)
                        .tab_stop(false)
                        .on_click(cx.listener(|this, _, _, cx| this.refresh_shown(cx))),
                    ),
                )
                .into_any_element(),
            _ => div()
                .id("secret-document-loading")
                .child(
                    Text::caption(dbflux_i18n::t!("document.secret_store.details.loading"))
                        .muted_foreground(),
                )
                .into_any_element(),
        };

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(header)
            .child(
                div()
                    .px(SecretStoreMetrics::DETAILS_PADDING_X)
                    .py(SecretStoreMetrics::DETAILS_PADDING_Y)
                    .child(body),
            )
            .into_any_element()
    }
}
