//! The Versions table and the History list of the details pane.

use super::{SecretView, ValueState, VersionsState};
use super::{section_title, short_version_id};
use crate::secret_store::render::{UNKNOWN, format_optional_time};
use dbflux_components::composites::ListRow;
use dbflux_components::controls::Button;
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Badge, BadgeTone, BannerBlock, BannerVariant, Text};
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, SecretStoreMetrics, ui};
use dbflux_core::{SecretStoreCapabilities, SecretVersionInfo};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Columns of the staged versions table. The artboard's 150 px version and
/// created columns are narrower here: the details pane is one column wide.
const VERSION_ID_WIDTH: Rems = ui(80.0);
const VERSION_CREATED_WIDTH: Rems = ui(118.0);
/// The Roll back button's column: an icon-only button, named by its tooltip,
/// so the column does not take the room the staging labels need in the
/// default 380 px pane.
const VERSION_ACTION_WIDTH: Rems = ui(28.0);
/// The tab has the room for a labelled Roll back button.
const VERSION_ACTION_LABELLED_WIDTH: Rems = ui(104.0);
const HISTORY_VERSION_WIDTH: Rems = ui(44.0);
const VERSION_HEADER_HEIGHT: Rems = ui(30.0);
const VERSION_ROW_HEIGHT: Rems = ui(40.0);
const HISTORY_ROW_HEIGHT: Rems = ui(30.0);
const VERSION_HEADER_FONT: Rems = ui(11.5);
const VERSION_ROW_FONT: Rems = ui(12.5);

/// Gap between the label chips of one version, as the artboard sets it.
pub(super) const LABEL_CHIP_GAP: Pixels = px(6.0); // guardrail-allow: artboard 02 spacing between staging label chips, no token carries 6 px here

impl SecretView {
    /// The version history: a table of versions with their staging labels
    /// and creation date on a store with labels, a list of versions with the
    /// principal and date on one without. `None` while there is nothing to
    /// show: before the load starts, and on a store without versions.
    pub(super) fn render_versions_section(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let staged = self.store.supports(SecretStoreCapabilities::VERSION_LABELS);

        let title = if staged {
            dbflux_i18n::t!("document.secret_store.versions.title")
        } else {
            dbflux_i18n::t!("document.secret_store.versions.history_title")
        };

        let mut header = div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .child(section_title(title, cx))
            .child(div().flex_1());

        let body: AnyElement = match &self.versions {
            VersionsState::Idle | VersionsState::NotSupported => return None,
            VersionsState::Loading => {
                Text::caption(dbflux_i18n::t!("document.secret_store.versions.loading"))
                    .muted_foreground()
                    .into_any_element()
            }
            VersionsState::ScheduledForDeletion => Text::caption(dbflux_i18n::t!(
                "document.secret_store.versions.scheduled_for_deletion"
            ))
            .muted_foreground()
            .into_any_element(),
            VersionsState::NotConnected => Text::caption(dbflux_i18n::t!(
                "document.secret_store.versions.not_connected"
            ))
            .muted_foreground()
            .into_any_element(),
            VersionsState::Failed(reason) => BannerBlock::new(
                BannerVariant::Danger,
                dbflux_i18n::t!("document.secret_store.versions.failed"),
            )
            .with_body(reason.clone())
            .into_any_element(),
            VersionsState::Loaded(versions) => {
                header = header.child(Badge::new(
                    versions_count_label(versions.len()),
                    BadgeTone::Neutral,
                ));

                self.render_versions_rows(versions, staged, cx)
                    .into_any_element()
            }
        };

        if self.in_tab() {
            return Some(
                super::card::card(
                    "secret-document-versions-card",
                    header,
                    vec![div().id("secret-versions").child(body).into_any_element()],
                    cx,
                )
                .into_any_element(),
            );
        }

        Some(
            div()
                .id("secret-versions")
                .flex()
                .flex_col()
                .gap(SecretStoreMetrics::ROW_GAP)
                .child(header)
                .child(body)
                .into_any_element(),
        )
    }

    pub(super) fn render_versions_rows(
        &self,
        versions: &[SecretVersionInfo],
        staged: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let shown = self.highlighted_version();

        let rows: Vec<AnyElement> = versions
            .iter()
            .enumerate()
            .map(|(index, version)| {
                let selected = shown == Some(version.id.as_str());
                self.render_version_row(index, version, staged, selected, cx)
            })
            .collect();

        let rollback = staged && self.supports_roll_back();

        div()
            .flex()
            .flex_col()
            .when(staged, |table| {
                table.child(versions_table_header(rollback, self.in_tab(), cx))
            })
            .children(rows)
            .when(rollback, |table| {
                table.child(
                    div()
                        .id("secret-versions-rollback-note")
                        .pt(SecretStoreMetrics::ROW_GAP)
                        .child(
                            Text::caption(dbflux_i18n::t!(
                                "document.secret_store.rollback.footnote"
                            ))
                            .muted_foreground(),
                        ),
                )
            })
    }

    /// One version: clicking it retrieves that version's value. The row
    /// shows metadata only, never a value.
    pub(super) fn render_version_row(
        &self,
        index: usize,
        version: &SecretVersionInfo,
        staged: bool,
        selected: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let strong = ChromeColors::strong(theme);
        let family = fonts::editor_family(cx);
        let created = format_optional_time(version.created);

        let row = ListRow::new(SharedString::from(format!("secret-version-row-{index}")))
            .selected(selected)
            .build(cx)
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .text_size(VERSION_ROW_FONT)
            .border_b_1()
            .border_color(theme.table_row_border)
            .on_click(cx.listener(move |this, _, _, cx| this.activate_version(index, cx)));

        let cell =
            |text: String, width: Rems, color: Hsla| version_cell(&family, text, width, color);

        if !staged {
            return history_row(row, version, created, &family, muted, strong);
        }

        let current = self.is_current_version(&version.id);
        let labels = version_labels(index, version, current);

        // Every row keeps the action column when the store can roll back,
        // so the created dates stay aligned; the current version has no
        // button, since there is nothing to roll back to.
        let action = self
            .supports_roll_back()
            .then(|| self.render_rollback_cell(index, version, current, cx));

        row.h(VERSION_ROW_HEIGHT)
            .child(
                cell(short_version_id(&version.id), VERSION_ID_WIDTH, strong)
                    .id(SharedString::from(format!("secret-version-{index}-id"))),
            )
            .child(
                div()
                    .id(SharedString::from(format!("secret-version-{index}-labels")))
                    .flex_1()
                    .min_w_0()
                    .overflow_hidden()
                    .child(labels),
            )
            .child(
                cell(created, VERSION_CREATED_WIDTH, muted)
                    .id(SharedString::from(format!(
                        "secret-version-{index}-created"
                    )))
                    .text_size(DocumentMetrics::TABLE_META_FONT),
            )
            .children(action)
            .into_any_element()
    }

    /// The Roll back column of one staged row: empty for the current version.
    fn render_rollback_cell(
        &self,
        index: usize,
        version: &SecretVersionInfo,
        current: bool,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let cell = div()
            .id(SharedString::from(format!("secret-version-{index}-action")))
            .w(action_width(self.in_tab()))
            .flex_shrink_0()
            .flex()
            .justify_end();

        if current {
            return cell;
        }

        cell.child(
            Button::new(
                SharedString::from(format!("secret-version-rollback-{index}")),
                if self.in_tab() {
                    dbflux_i18n::t!("document.secret_store.rollback.button")
                } else {
                    dbflux_i18n::t!(
                        "document.secret_store.rollback.action",
                        version = short_version_id(&version.id)
                    )
                },
            )
            .ghost()
            .icon(AppIcon::RotateCcw)
            .when(!self.in_tab(), |button| button.icon_only())
            .tab_stop(false)
            .when(self.in_tab(), |button| {
                button.aria_label(dbflux_i18n::t!(
                    "document.secret_store.rollback.action",
                    version = short_version_id(&version.id)
                ))
            })
            .when(self.version_is_pending(index), |button| {
                let reason = dbflux_i18n::t!("document.secret_store.rollback.pending_reason");

                button
                    .aria_label(format!(
                        "{}: {reason}",
                        dbflux_i18n::t!(
                            "document.secret_store.rollback.action",
                            version = short_version_id(&version.id)
                        )
                    ))
                    .tooltip(reason)
            })
            .disabled(!self.can_roll_back_version(index))
            .on_click(cx.listener(move |this, _, window, cx| {
                // The button sits on the row that retrieves the
                // version; rolling back must not also retrieve it.
                cx.stop_propagation();
                this.open_roll_back(index, window, cx);
            })),
        )
    }
}

/// The staging labels of one version as chips, green while it is current.
fn version_labels(index: usize, version: &SecretVersionInfo, current: bool) -> Div {
    if version.labels.is_empty() {
        return div().child(
            Text::caption(dbflux_i18n::t!("document.secret_store.versions.no_labels"))
                .muted_foreground(),
        );
    }

    div()
        .flex()
        .flex_wrap()
        .gap(LABEL_CHIP_GAP)
        .children(version.labels.iter().enumerate().map(|(chip, label)| {
            Badge::new(
                label.clone(),
                if current {
                    BadgeTone::Success
                } else {
                    BadgeTone::Neutral
                },
            )
            .id(SharedString::from(format!(
                "secret-version-{index}-label-{chip}"
            )))
        }))
}

/// "1 version" or "N versions".
pub(super) fn versions_count_label(count: usize) -> String {
    let key = if count == 1 {
        "document.secret_store.versions.count.one"
    } else {
        "document.secret_store.versions.count.many"
    };

    dbflux_i18n::t!(key, count = count)
}

/// The column titles of the staged table: Version, Staging labels, Created.
pub(super) fn versions_table_header(with_actions: bool, labelled: bool, cx: &App) -> Div {
    let theme = cx.theme();

    div()
        .flex()
        .items_center()
        .gap(DocumentMetrics::GAP)
        .h(VERSION_HEADER_HEIGHT)
        .border_b_1()
        .border_color(theme.border)
        .text_size(VERSION_HEADER_FONT)
        .text_color(theme.muted_foreground)
        .child(
            div()
                .id("secret-versions-col-version")
                .w(VERSION_ID_WIDTH)
                .flex_shrink_0()
                .whitespace_nowrap()
                .child(dbflux_i18n::t!(
                    "document.secret_store.versions.column.version"
                )),
        )
        .child(
            div()
                .id("secret-versions-col-labels")
                .flex_1()
                .min_w_0()
                .whitespace_nowrap()
                .truncate()
                .child(dbflux_i18n::t!(
                    "document.secret_store.versions.column.labels"
                )),
        )
        .child(
            div()
                .id("secret-versions-col-created")
                .w(VERSION_CREATED_WIDTH)
                .flex_shrink_0()
                .whitespace_nowrap()
                .child(dbflux_i18n::t!(
                    "document.secret_store.versions.column.created"
                )),
        )
        .when(with_actions, |header| {
            header.child(div().w(action_width(labelled)).flex_shrink_0())
        })
}

/// The width of the Roll back column: wide enough for the label in the tab.
fn action_width(labelled: bool) -> Rems {
    if labelled {
        VERSION_ACTION_LABELLED_WIDTH
    } else {
        VERSION_ACTION_WIDTH
    }
}

/// One fixed-width cell of a version row, in the editor face.
fn version_cell(family: &SharedString, text: String, width: Rems, color: Hsla) -> Div {
    div()
        .w(width)
        .flex_shrink_0()
        .truncate()
        .font_family(family.clone())
        .text_color(color)
        .child(text)
}

/// A row of the History list (a store without staging labels): the version
/// number, who modified it and when.
fn history_row(
    row: Stateful<Div>,
    version: &SecretVersionInfo,
    created: String,
    family: &SharedString,
    muted: Hsla,
    strong: Hsla,
) -> AnyElement {
    let author = version
        .modified_by
        .clone()
        .unwrap_or_else(|| UNKNOWN.to_string());

    row.h(HISTORY_ROW_HEIGHT)
        .child(version_cell(
            family,
            dbflux_i18n::t!(
                "document.secret_store.versions.history_version",
                id = version.id.as_str()
            ),
            HISTORY_VERSION_WIDTH,
            strong,
        ))
        .child(
            div()
                .flex_1()
                .min_w_0()
                .truncate()
                .font_family(family.clone())
                .text_size(DocumentMetrics::TABLE_META_FONT)
                .text_color(muted)
                .child(author),
        )
        .child(
            version_cell(family, created, VERSION_CREATED_WIDTH, muted)
                .text_size(DocumentMetrics::TABLE_META_FONT),
        )
        .into_any_element()
}
