//! The details pane of `SecretStoreDocument`: the selected secret's value
//! section, its details, its rotation and its tags.

use super::render::{UNKNOWN, format_optional_time, listing_shortcut, version_label};
use super::value::{MASK, RetrievedValue, ValueContent, ValueFailure, ValueView};
use super::{DetailsState, SecretRowKey, SecretStoreDocument, ValueState, VersionsState};
use crate::buckets_table::format_bytes;
use dbflux_app::keymap::Command;
use dbflux_components::composites::ListRow;
use dbflux_components::controls::{Button, Input};
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, Icon, SegmentedControl, SegmentedItem,
    Text,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, DocumentMetrics, SecretStoreMetrics, ui,
};
use dbflux_core::{
    SecretDetails, SecretRotation, SecretStoreCapabilities, SecretSummary, SecretVersionInfo,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::{ScrollableElement, Scrollbar};

/// Lines the multi-line viewer keeps visible before it scrolls, so the
/// sections below the value keep their place.
pub(super) const VIEWER_VISIBLE_LINES: f32 = 14.0;

/// Width of the viewer's line-number gutter: three digits at the editor size.
pub(super) const VIEWER_GUTTER_WIDTH: Rems = ui(28.0);

/// Columns of the staged versions table. The artboard's 150 px version and
/// created columns are narrower here: the details pane is one column wide.
const VERSION_ID_WIDTH: Rems = ui(80.0);
const VERSION_CREATED_WIDTH: Rems = ui(118.0);
/// The Roll back button's column: the label, its icon and the button's
/// padding.
const VERSION_ACTION_WIDTH: Rems = ui(92.0);
const HISTORY_VERSION_WIDTH: Rems = ui(44.0);
const VERSION_HEADER_HEIGHT: Rems = ui(30.0);
const VERSION_ROW_HEIGHT: Rems = ui(40.0);
const HISTORY_ROW_HEIGHT: Rems = ui(30.0);
const VERSION_HEADER_FONT: Rems = ui(11.5);
const VERSION_ROW_FONT: Rems = ui(12.5);

/// Gap between the label chips of one version, as the artboard sets it.
const LABEL_CHIP_GAP: Pixels = px(6.0); // guardrail-allow: artboard 02 spacing between staging label chips, no token carries 6 px here

/// Vertical padding of a tag chip, as the artboard sets it.
const TAG_CHIP_PADDING_Y: Pixels = px(3.0); // guardrail-allow: artboard 01 tag chip padding, no token carries 3 px here

/// Characters of a version id the table shows before the ellipsis.
const VERSION_ID_CHARS: usize = 8;

/// `a41c09e2-…` as `a41c09e2…`: the first characters of an id longer than the
/// column, so ids that differ late stay distinguishable by the row, not the
/// text.
pub(super) fn short_version_id(id: &str) -> String {
    let mut chars = id.chars();
    let head: String = chars.by_ref().take(VERSION_ID_CHARS).collect();

    if chars.next().is_some() {
        format!("{head}…")
    } else {
        head
    }
}

fn section_title(title: String, cx: &App) -> Div {
    div()
        .text_size(DocumentMetrics::DETAIL_LABEL_FONT)
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(cx.theme().muted_foreground)
        .child(title.to_uppercase())
}

fn section(title: String, rows: Vec<AnyElement>, cx: &App) -> Div {
    div()
        .flex()
        .flex_col()
        .gap(SecretStoreMetrics::ROW_GAP)
        .child(section_title(title, cx))
        .children(rows)
}

/// One `label  value` row of a details section, the value in mono.
fn detail_row(label: String, value: String, cx: &App) -> AnyElement {
    let theme = cx.theme();

    div()
        .flex()
        .items_start()
        .gap(DocumentMetrics::GAP)
        .text_size(DocumentMetrics::TABLE_META_FONT)
        .child(
            div()
                .w(SecretStoreMetrics::LABEL_WIDTH)
                .flex_shrink_0()
                .text_color(theme.muted_foreground)
                .child(label),
        )
        .child(
            div()
                .flex_1()
                .min_w_0()
                .font_family(dbflux_components::fonts::editor_family(cx))
                .text_color(ChromeColors::strong(theme))
                .child(value),
        )
        .into_any_element()
}

/// The last retrieval as a date. AWS reports it rounded down to the day, as
/// midnight UTC, so a local time of day would be one that never happened and
/// could read earlier than the creation date.
pub(super) fn last_retrieved_label(
    time: Option<dbflux_core::chrono::DateTime<dbflux_core::chrono::Utc>>,
) -> String {
    time.map_or_else(
        || UNKNOWN.to_string(),
        |time| time.date_naive().format("%Y-%m-%d").to_string(),
    )
}

fn or_unknown(value: Option<&str>) -> String {
    value.map_or_else(|| UNKNOWN.to_string(), str::to_string)
}

/// The last change, with the principal that made it when the store reports
/// one.
fn last_changed_label(details: &SecretDetails) -> String {
    let when = format_optional_time(details.summary.last_changed);

    match details.last_changed_by.as_deref() {
        Some(by) => format!("{when} · {by}"),
        None => when,
    }
}

fn rotation_schedule_label(rotation: &SecretRotation) -> String {
    match (rotation.schedule.as_deref(), rotation.window.as_deref()) {
        (Some(schedule), Some(window)) => dbflux_i18n::t!(
            "document.secret_store.rotation.schedule_with_window",
            schedule = super::render::rotation_label(Some(rotation)),
            window = window,
            expression = schedule
        ),
        (Some(_), None) => super::render::rotation_label(Some(rotation)),
        (None, _) => UNKNOWN.to_string(),
    }
}

impl SecretStoreDocument {
    pub(super) fn render_details_pane(&self, cx: &mut Context<Self>) -> AnyElement {
        let border = cx.theme().border;
        let background = cx.theme().background;

        let body: AnyElement = match (&self.selected, self.selected_summary().cloned()) {
            (Some(SecretRowKey::Secret(_)), Some(summary)) => {
                self.render_secret_details(&summary, cx).into_any_element()
            }
            _ => div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .p(SecretStoreMetrics::DETAILS_PADDING_X)
                .child(
                    Text::body(dbflux_i18n::t!("document.secret_store.details.none"))
                        .muted_foreground(),
                )
                .into_any_element(),
        };

        div()
            .id("secret-details")
            .w(SecretStoreMetrics::DETAILS_WIDTH)
            .flex_shrink_1()
            .min_w(SecretStoreMetrics::DETAILS_MIN_WIDTH)
            .h_full()
            .flex()
            .flex_col()
            .border_l_1()
            .border_color(border)
            .bg(background)
            .child(body)
            .into_any_element()
    }

    fn render_secret_details(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mut sections: Vec<AnyElement> =
            vec![self.render_value_section(summary, cx).into_any_element()];

        if let Some(service) = summary.owning_service.as_deref() {
            sections.push(managed_card(&summary.name, service).into_any_element());
        }

        // The history loads apart from the describe, so it shows whatever
        // state the describe is in; it sits after the metadata when there is
        // one.
        let mut versions = self.render_versions_section(cx);

        match &self.details {
            DetailsState::Loaded(details) => {
                sections.push(self.render_metadata_section(details, cx).into_any_element());
                sections.extend(versions.take());

                if self.store.supports(SecretStoreCapabilities::ROTATION) {
                    sections.push(render_rotation_section(details, cx).into_any_element());
                }

                sections.push(render_tags_section(details, cx).into_any_element());
            }
            DetailsState::Loading => sections.push(
                Text::caption(dbflux_i18n::t!("document.secret_store.details.loading"))
                    .muted_foreground()
                    .into_any_element(),
            ),
            DetailsState::Failed(error) => sections.push(
                BannerBlock::new(
                    BannerVariant::Danger,
                    dbflux_i18n::t!("document.secret_store.details.failed"),
                )
                .with_body(error.clone())
                .into_any_element(),
            ),
            DetailsState::Idle => {}
        }

        sections.extend(versions);

        let header = self.render_details_header(summary, cx);

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(header)
            .child(
                div()
                    .id("secret-details-body")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap(SecretStoreMetrics::SECTION_GAP)
                    .px(SecretStoreMetrics::DETAILS_PADDING_X)
                    .py(SecretStoreMetrics::DETAILS_PADDING_Y)
                    .children(sections),
            )
    }

    /// The secret's name with the pane's two actions: Copy name and Close.
    /// The artboard's third button, open in a tab, waits for the document per
    /// secret.
    fn render_details_header(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let border = theme.border;
        let warning = theme.warning;
        let strong = ChromeColors::strong(theme);

        div()
            .id("secret-details-header")
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
                    .id("secret-details-name")
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .font_family(fonts::editor_family(cx))
                    .font_weight(FontWeight::BOLD)
                    .text_size(SecretStoreMetrics::NAME_FONT)
                    .text_color(strong)
                    .child(summary.name.clone()),
            )
            .child(
                Button::new(
                    "secret-details-copy-name",
                    dbflux_i18n::t!("document.secret_store.context_menu.copy_name"),
                )
                .ghost()
                .icon(AppIcon::Copy)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.copy_selected_name(cx))),
            )
            .child(
                Button::new(
                    "secret-details-close",
                    dbflux_i18n::t!("document.secret_store.details.close"),
                )
                .ghost()
                .icon(AppIcon::X)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|this, _, window, cx| this.close_details(window, cx))),
            )
    }

    /// The details the describe reported. A typed store shows its type, tier
    /// and data type; every store shows the identifier, description,
    /// encryption key, versions and the change dates it reports.
    fn render_metadata_section(&self, details: &SecretDetails, cx: &App) -> Div {
        let summary = &details.summary;
        let typed = self.store.supports(SecretStoreCapabilities::TYPED_VALUES);
        let label = |key: &str| dbflux_i18n::t!(key);

        let mut rows = vec![detail_row(
            label("document.secret_store.details.resource_id"),
            or_unknown(details.resource_id.as_deref()),
            cx,
        )];

        if typed {
            rows.push(detail_row(
                label("document.secret_store.details.type"),
                or_unknown(summary.value_type.as_deref()),
                cx,
            ));
            rows.push(detail_row(
                label("document.secret_store.details.tier"),
                or_unknown(summary.tier.as_deref()),
                cx,
            ));
            rows.push(detail_row(
                label("document.secret_store.details.data_type"),
                or_unknown(details.data_type.as_deref()),
                cx,
            ));
        }

        rows.push(detail_row(
            label("document.secret_store.details.encryption_key"),
            or_unknown(details.encryption_key.as_deref()),
            cx,
        ));
        rows.push(detail_row(
            label("document.secret_store.details.description"),
            or_unknown(summary.description.as_deref()),
            cx,
        ));

        if typed {
            rows.push(detail_row(
                label("document.secret_store.details.version"),
                version_label(summary.version.as_deref()),
                cx,
            ));
            rows.push(detail_row(
                label("document.secret_store.details.last_modified"),
                last_changed_label(details),
                cx,
            ));
        } else {
            rows.push(detail_row(
                label("document.secret_store.details.created"),
                format_optional_time(details.created),
                cx,
            ));
            rows.push(detail_row(
                label("document.secret_store.details.last_changed"),
                last_changed_label(details),
                cx,
            ));
            rows.push(detail_row(
                label("document.secret_store.details.last_retrieved"),
                last_retrieved_label(details.last_accessed),
                cx,
            ));
        }

        section(
            dbflux_i18n::t!("document.secret_store.details.title"),
            rows,
            cx,
        )
    }

    /// The version history: a table of versions with their staging labels
    /// and creation date on a store with labels, a list of versions with the
    /// principal and date on one without. `None` while there is nothing to
    /// show: before the load starts, and on a store without versions.
    fn render_versions_section(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
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

    fn render_versions_rows(
        &self,
        versions: &[SecretVersionInfo],
        staged: bool,
        cx: &mut Context<Self>,
    ) -> Div {
        let shown = match self.value {
            ValueState::Loading | ValueState::Loaded(_) => self
                .value_version
                .as_deref()
                .or_else(|| self.current_version_id()),
            _ => None,
        };

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
                table.child(versions_table_header(rollback, cx))
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
    fn render_version_row(
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

        let cell = |text: String, width: Rems, color: Hsla| {
            div()
                .w(width)
                .flex_shrink_0()
                .truncate()
                .font_family(family.clone())
                .text_color(color)
                .child(text)
        };

        if !staged {
            let author = version
                .modified_by
                .clone()
                .unwrap_or_else(|| UNKNOWN.to_string());

            return row
                .h(HISTORY_ROW_HEIGHT)
                .child(cell(
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
                    cell(created, VERSION_CREATED_WIDTH, muted)
                        .text_size(DocumentMetrics::TABLE_META_FONT),
                )
                .into_any_element();
        }

        let current = self.is_current_version(&version.id);
        let labels = if version.labels.is_empty() {
            div().child(
                Text::caption(dbflux_i18n::t!("document.secret_store.versions.no_labels"))
                    .muted_foreground(),
            )
        } else {
            div()
                .flex()
                .flex_wrap()
                .gap(LABEL_CHIP_GAP)
                .children(version.labels.iter().map(|label| {
                    Badge::new(
                        label.clone(),
                        if current {
                            BadgeTone::Success
                        } else {
                            BadgeTone::Neutral
                        },
                    )
                }))
        };

        // Every row keeps the action column when the store can roll back,
        // so the created dates stay aligned; the current version has no
        // button, since there is nothing to roll back to.
        let action = self.supports_roll_back().then(|| {
            let cell = div()
                .w(VERSION_ACTION_WIDTH)
                .flex_shrink_0()
                .flex()
                .justify_end();

            if current {
                return cell;
            }

            cell.child(
                Button::new(
                    SharedString::from(format!("secret-version-rollback-{index}")),
                    dbflux_i18n::t!("document.secret_store.rollback.button"),
                )
                .inline()
                .icon(AppIcon::RotateCcw)
                .tab_stop(false)
                .disabled(!self.can_roll_back_version(index))
                .on_click(cx.listener(move |this, _, window, cx| {
                    // The button sits on the row that retrieves the
                    // version; rolling back must not also retrieve it.
                    cx.stop_propagation();
                    this.open_roll_back(index, window, cx);
                })),
            )
        });

        row.h(VERSION_ROW_HEIGHT)
            .child(cell(
                short_version_id(&version.id),
                VERSION_ID_WIDTH,
                strong,
            ))
            .child(div().flex_1().min_w_0().child(labels))
            .child(
                cell(created, VERSION_CREATED_WIDTH, muted)
                    .text_size(DocumentMetrics::TABLE_META_FONT),
            )
            .children(action)
            .into_any_element()
    }

    fn render_value_section(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let header = div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .child(section_title(
                dbflux_i18n::t!("document.secret_store.value.title"),
                cx,
            ))
            .when_some(self.value_kind_label(), |header, label| {
                header.child(Badge::new(label, BadgeTone::Accent).id("secret-value-kind"))
            })
            .child(div().flex_1())
            .when_some(self.value_header_meta(), |header, meta| {
                header.child(Text::caption(meta).muted_foreground())
            });

        let body: AnyElement = if let Some(date) = summary.deletion_scheduled_for {
            pending_deletion_card(date, cx).into_any_element()
        } else {
            match &self.value {
                ValueState::NotLoaded => self.render_value_not_loaded(cx).into_any_element(),
                ValueState::Loading => {
                    Text::caption(dbflux_i18n::t!("document.secret_store.value.retrieving"))
                        .muted_foreground()
                        .into_any_element()
                }
                ValueState::Loaded(value) => self.render_value_loaded(value, cx).into_any_element(),
                ValueState::Failed(failure) => render_value_failed(failure, cx).into_any_element(),
            }
        };

        div()
            .id("secret-value-section")
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .child(header)
            .when_some(
                self.shown_older_version().map(str::to_string),
                |section, version| section.child(self.render_version_banner(version, cx)),
            )
            .child(body)
    }

    /// Says which version the value section shows when it is not the
    /// current one, with the one action that goes back.
    fn render_version_banner(&self, version: String, cx: &mut Context<Self>) -> BannerBlock {
        let back = Button::new(
            "secret-version-back-to-current",
            dbflux_i18n::t!("document.secret_store.versions.back_to_current"),
        )
        .icon(AppIcon::RotateCcw)
        .tab_stop(false)
        .disabled(!self.can_return_to_current())
        .on_click(cx.listener(|this, _, _, cx| this.back_to_current_version(cx)));

        BannerBlock::new(
            BannerVariant::Info,
            dbflux_i18n::t!("document.secret_store.versions.shown", version = version),
        )
        .with_body(if self.current_version_id().is_some() {
            dbflux_i18n::t!("document.secret_store.versions.not_current")
        } else {
            dbflux_i18n::t!("document.secret_store.versions.current_unknown")
        })
        .with_actions(back)
    }

    /// "dotenv · 9 lines" once a value is shown: counts only, no text.
    fn value_kind_label(&self) -> Option<String> {
        match &self.value {
            ValueState::Loaded(value) => Some(value.kind_label()),
            _ => None,
        }
    }

    /// "Retrieved 14:02 · audited" once a value is shown.
    fn value_header_meta(&self) -> Option<String> {
        let ValueState::Loaded(value) = &self.value else {
            return None;
        };

        Some(dbflux_i18n::t!(
            "document.secret_store.value.retrieved_at",
            time = value.retrieved_at.format("%H:%M").to_string()
        ))
    }

    fn retrieve_button(&self, cx: &mut Context<Self>) -> Button {
        let mut button = Button::new(
            "secret-retrieve-value",
            dbflux_i18n::t!("document.secret_store.value.retrieve"),
        )
        .primary()
        .icon(AppIcon::Eye)
        .tab_stop(false)
        .disabled(!self.can_retrieve_value())
        .on_click(cx.listener(|this, _, _, cx| this.retrieve_selected_value(cx)));

        if let Some(key) = listing_shortcut(Command::ToggleValuePanel) {
            button = button.kbd(key);
        }

        button
    }

    fn render_value_not_loaded(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        let encryption_key = match &self.details {
            DetailsState::Loaded(details)
                if self.store.supports(SecretStoreCapabilities::TYPED_VALUES) =>
            {
                details.encryption_key.clone()
            }
            _ => None,
        };

        let explanation = match &encryption_key {
            Some(_) => dbflux_i18n::t!("document.secret_store.value.encrypted_hint"),
            None => dbflux_i18n::t!("document.secret_store.value.not_loaded_hint"),
        };

        let theme = cx.theme();
        let fill = theme.popover;
        let border = theme.border;
        let muted = theme.muted_foreground;

        div()
            .id("secret-value-card")
            .relative()
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
            .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
            .child(Chamfer::new(ChamferCut::CARD).fill(fill).border(border))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(
                        div().id("secret-value-card-lock").child(
                            Icon::new(AppIcon::Lock)
                                .size(DocumentMetrics::TITLE_ICON)
                                .color(muted),
                        ),
                    )
                    .child(div().font_weight(FontWeight::SEMIBOLD).child(Text::body(
                        dbflux_i18n::t!("document.secret_store.value.not_loaded"),
                    ))),
            )
            .child(Text::caption(explanation).muted_foreground())
            .when_some(encryption_key, |body, key| {
                body.child(
                    Text::caption(dbflux_i18n::t!(
                        "document.secret_store.value.needs_decrypt",
                        key = key
                    ))
                    .muted_foreground(),
                )
            })
            .child(div().flex().child(self.retrieve_button(cx)))
    }

    fn render_value_view_switch(
        &self,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let entity = cx.entity().downgrade();
        let active = match value.view {
            ValueView::KeyValue => "key-value",
            ValueView::Plaintext => "plaintext",
        };

        let switch = SegmentedControl::new(
            value_view_items(value.content.has_key_value_view()),
            active,
            move |id, _window, cx| {
                let view = if id.as_ref() == "plaintext" {
                    ValueView::Plaintext
                } else {
                    ValueView::KeyValue
                };

                if let Some(doc) = entity.upgrade() {
                    doc.update(cx, |doc, cx| doc.set_value_view(view, cx));
                }
            },
        )
        .group("secret-value-view");

        div().child(switch).into_any_element()
    }

    fn render_value_loaded(&self, value: &RetrievedValue, cx: &mut Context<Self>) -> Div {
        let mut body = div().flex().flex_col().gap(SecretStoreMetrics::ROW_GAP);

        // The open editor replaces the value's presentation: the stored text
        // in an editable surface, with Save and Discard, drawn inside the
        // redaction boundary the multi-line viewer uses.
        if self.is_editing_value() {
            return self.render_value_editor(cx);
        }

        body = match &value.content {
            ValueContent::KeyValue { .. } | ValueContent::Plaintext(_) => {
                body.child(self.render_value_view_switch(value, cx))
            }
            ValueContent::Binary { size, .. } => body.child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(Text::body(dbflux_i18n::t!(
                        "document.secret_store.value.binary_size",
                        size = format_bytes(*size as u64)
                    )))
                    .child(Badge::new(
                        dbflux_i18n::t!("document.secret_store.value.read_only"),
                        BadgeTone::Neutral,
                    )),
            ),
        };

        let fields: Vec<AnyElement> = value
            .fields()
            .iter()
            .enumerate()
            .map(|(index, (key, _))| {
                self.render_value_field(
                    index,
                    key.map(str::to_string),
                    value.is_revealed(index),
                    value,
                    cx,
                )
            })
            .collect();

        body = body.children(fields);

        if value.content.is_binary() {
            body = body.child(
                Text::caption(dbflux_i18n::t!("document.secret_store.value.binary_note"))
                    .muted_foreground(),
            );
        }

        body.child(self.render_value_actions(value, cx)).child(
            Text::caption(dbflux_i18n::t!("document.secret_store.value.tab_only"))
                .muted_foreground(),
        )
    }

    /// One value field: its key on a key/value secret, the value in a
    /// secret-marked read-only input, and its reveal and copy buttons. A value
    /// with more than one line is drawn in the multi-line viewer instead.
    fn render_value_field(
        &self,
        index: usize,
        key: Option<String>,
        revealed: bool,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if value.is_multiline() {
            return self
                .render_multiline_value_field(index, key, revealed, value, cx)
                .into_any_element();
        }

        let theme = cx.theme();
        let aria = key
            .clone()
            .unwrap_or_else(|| dbflux_i18n::t!("document.secret_store.value.title"));

        let field: AnyElement = match self.value_inputs.get(index) {
            Some(input) => Input::new(input)
                .id(SharedString::from(format!("secret-value-field-{index}")))
                .aria_label(aria)
                .secret(true)
                .appearance(false)
                .disabled(true)
                .into_any_element(),
            None => div()
                .text_color(theme.muted_foreground)
                .child(MASK)
                .into_any_element(),
        };

        div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .h(SecretStoreMetrics::VALUE_ROW_HEIGHT)
            .font_family(fonts::editor_family(cx))
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .when_some(key, |row, key| {
                row.child(
                    div()
                        .w(SecretStoreMetrics::VALUE_KEY_WIDTH)
                        .flex_shrink_0()
                        .truncate()
                        .text_color(theme.muted_foreground)
                        .child(key),
                )
            })
            .child(div().flex_1().min_w_0().child(field))
            .children(value_field_buttons(index, revealed, cx))
            .into_any_element()
    }

    /// A value the viewer draws: its key and actions on one row, then the
    /// bounded viewer box.
    fn render_multiline_value_field(
        &self,
        index: usize,
        key: Option<String>,
        revealed: bool,
        value: &RetrievedValue,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let muted = cx.theme().muted_foreground;
        let family = fonts::editor_family(cx);
        let lines = value.viewer_lines(revealed);

        let header = div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .text_size(DocumentMetrics::TABLE_META_FONT)
            .when_some(key, |header, key| {
                header.child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(family)
                        .text_color(muted)
                        .child(key),
                )
            })
            .child(div().flex_1())
            .children(value_field_buttons(index, revealed, cx));

        div()
            .id(SharedString::from(format!("secret-value-field-{index}")))
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .child(header)
            .child(render_value_viewer(&lines, &self.value_viewer_scroll, cx))
    }

    fn render_value_actions(&self, value: &RetrievedValue, cx: &mut Context<Self>) -> Div {
        let copy_label = if value.content.has_key_value_view() {
            dbflux_i18n::t!("document.secret_store.value.copy_json")
        } else {
            dbflux_i18n::t!("document.secret_store.value.copy_all")
        };

        let copy_all = Button::new("secret-value-copy-all", copy_label)
            .inline()
            .icon(AppIcon::Copy)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.copy_full_value(cx)));

        let (reveal_key, reveal_icon) = if value.all_revealed() {
            ("document.secret_store.value.hide_all", AppIcon::EyeOff)
        } else {
            ("document.secret_store.value.show_all", AppIcon::Eye)
        };

        let reveal_all = Button::new("secret-value-hide-all", dbflux_i18n::t!(reveal_key))
            .inline()
            .icon(reveal_icon)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.toggle_reveal_all(cx)));

        div()
            .flex()
            .flex_wrap()
            .gap(DocumentMetrics::GAP)
            .child(reveal_all)
            .child(copy_all)
            .when(self.can_edit_value(), |actions| {
                actions.child(
                    Button::new(
                        "secret-value-edit",
                        dbflux_i18n::t!("document.secret_store.edit.open"),
                    )
                    .inline()
                    .icon(AppIcon::Pencil)
                    .tab_stop(false)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.start_value_edit(window, cx);
                    })),
                )
            })
    }
}

/// The two tabs of the value view. Key/value is muted, with the reason in
/// its tooltip, on a value that does not open as a one-level JSON object.
pub(super) fn value_view_items(key_value_available: bool) -> Vec<SegmentedItem> {
    let key_value = SegmentedItem::new(
        "key-value",
        dbflux_i18n::t!("document.secret_store.value.view.key_value"),
    );

    let key_value = if key_value_available {
        key_value
    } else {
        key_value.disabled_reason(dbflux_i18n::t!(
            "document.secret_store.value.view.key_value_unavailable"
        ))
    };

    vec![
        key_value,
        SegmentedItem::new(
            "plaintext",
            dbflux_i18n::t!("document.secret_store.value.view.plaintext"),
        ),
    ]
}

/// The reveal and copy buttons of one value field, shared by the single-line
/// field and the multi-line viewer.
fn value_field_buttons(
    index: usize,
    revealed: bool,
    cx: &mut Context<SecretStoreDocument>,
) -> [AnyElement; 2] {
    [
        Button::new(
            SharedString::from(format!("secret-value-reveal-{index}")),
            if revealed {
                dbflux_i18n::t!("document.secret_store.value.hide")
            } else {
                dbflux_i18n::t!("document.secret_store.value.reveal")
            },
        )
        .ghost()
        .icon(if revealed {
            AppIcon::EyeOff
        } else {
            AppIcon::Eye
        })
        .icon_only()
        .tab_stop(false)
        .on_click(cx.listener(move |this, _, _, cx| this.toggle_reveal(index, cx)))
        .into_any_element(),
        Button::new(
            SharedString::from(format!("secret-value-copy-{index}")),
            dbflux_i18n::t!("document.secret_store.value.copy"),
        )
        .ghost()
        .icon(AppIcon::Copy)
        .icon_only()
        .tab_stop(false)
        .on_click(cx.listener(move |this, _, _, cx| this.copy_value_field(index, cx)))
        .into_any_element(),
    ]
}

/// The bounded viewer of a multi-line value: a monospace box with a
/// line-number gutter that keeps every line break and wraps long lines at the
/// pane width. The box grows with its lines up to [`VIEWER_VISIBLE_LINES`]
/// and scrolls past that, so a short value does not push the sections below
/// it down and a long one does not run away.
///
/// The lines it draws are hidden from the accessibility tree and redacted in
/// the rendered frame UI automation reads, so neither a masked nor a revealed
/// value leaves the value behind.
fn render_value_viewer(lines: &[String], scroll_handle: &ScrollHandle, cx: &App) -> Div {
    let theme = cx.theme();
    let muted = theme.muted_foreground;
    let foreground = theme.foreground;
    let fill = theme.popover;
    let border = theme.border;

    let line_height = fonts::ui_px(cx, DocumentMetrics::DETAIL_BLOCK_FONT)
        * DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT;

    let rows = lines.iter().enumerate().map(|(index, line)| {
        div()
            .id(SharedString::from(format!(
                "secret-value-viewer-line-{index}"
            )))
            .relative()
            .flex_shrink_0()
            .w_full()
            .min_h(line_height)
            .pl(VIEWER_GUTTER_WIDTH)
            .child(
                div()
                    .absolute()
                    .left_0()
                    .top_0()
                    .w(VIEWER_GUTTER_WIDTH)
                    .text_color(muted)
                    .child((index + 1).to_string()),
            )
            .child(div().w_full().child(line.clone()))
    });

    div()
        .relative()
        .px(DocumentMetrics::DETAIL_BLOCK_PADDING_X)
        .py(DocumentMetrics::DETAIL_BLOCK_PADDING_Y)
        .font_family(fonts::editor_family(cx))
        .text_size(DocumentMetrics::DETAIL_BLOCK_FONT)
        .line_height(relative(DocumentMetrics::DETAIL_BLOCK_LINE_HEIGHT))
        .text_color(foreground)
        .child(Chamfer::new(ChamferCut::INPUT).fill(fill).border(border))
        .child(
            div()
                .id("secret-value-viewer")
                .flex()
                .flex_col()
                .max_h(line_height * VIEWER_VISIBLE_LINES)
                .aria_hidden(true)
                .frame_redacted(true)
                .track_scroll(scroll_handle)
                .children(rows)
                .overflow_y_scroll(),
        )
        .child(Scrollbar::vertical(scroll_handle))
}

fn render_value_failed(failure: &ValueFailure, cx: &mut Context<SecretStoreDocument>) -> Div {
    let title = if failure.decrypt_denied {
        dbflux_i18n::t!("document.secret_store.value.failed_decrypt")
    } else {
        dbflux_i18n::t!("document.secret_store.value.failed")
    };

    let actions = div()
        .flex()
        .gap(DocumentMetrics::GAP)
        .child(
            Button::new(
                "secret-value-copy-error",
                dbflux_i18n::t!("document.secret_store.value.copy_error"),
            )
            .icon(AppIcon::Copy)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.copy_value_error(cx))),
        )
        .child(
            Button::new(
                "secret-value-view-in-audit",
                dbflux_i18n::t!("document.secret_store.value.view_in_audit"),
            )
            .icon(AppIcon::ScrollText)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.view_value_error_in_audit(cx))),
        );

    div()
        .flex()
        .flex_col()
        .gap(SecretStoreMetrics::ROW_GAP)
        .child(
            BannerBlock::new(BannerVariant::Danger, title)
                .with_pre(failure.message.clone())
                .with_actions(actions),
        )
        .child(
            Text::caption(dbflux_i18n::t!(
                "document.secret_store.value.metadata_loaded"
            ))
            .muted_foreground(),
        )
}

/// "1 version" or "N versions".
fn versions_count_label(count: usize) -> String {
    let key = if count == 1 {
        "document.secret_store.versions.count.one"
    } else {
        "document.secret_store.versions.count.many"
    };

    dbflux_i18n::t!(key, count = count)
}

/// The column titles of the staged table: Version, Staging labels, Created.
fn versions_table_header(with_actions: bool, cx: &App) -> Div {
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
                .w(VERSION_ID_WIDTH)
                .flex_shrink_0()
                .child(dbflux_i18n::t!(
                    "document.secret_store.versions.column.version"
                )),
        )
        .child(div().flex_1().min_w_0().child(dbflux_i18n::t!(
            "document.secret_store.versions.column.labels"
        )))
        .child(
            div()
                .w(VERSION_CREATED_WIDTH)
                .flex_shrink_0()
                .child(dbflux_i18n::t!(
                    "document.secret_store.versions.column.created"
                )),
        )
        .when(with_actions, |header| {
            header.child(div().w(VERSION_ACTION_WIDTH).flex_shrink_0())
        })
}

fn pending_deletion_card(
    date: dbflux_core::chrono::DateTime<dbflux_core::chrono::Utc>,
    _cx: &App,
) -> BannerBlock {
    BannerBlock::new(
        BannerVariant::Warning,
        dbflux_i18n::t!(
            "document.secret_store.value.deletion_scheduled",
            date = super::render::format_time(date)
        ),
    )
    .with_body(dbflux_i18n::t!(
        "document.secret_store.value.deletion_scheduled_body"
    ))
}

fn managed_card(name: &str, service: &str) -> BannerBlock {
    BannerBlock::new(
        BannerVariant::Info,
        dbflux_i18n::t!("document.secret_store.managed.title", service = service),
    )
    .with_body(dbflux_i18n::t!(
        "document.secret_store.managed.body",
        name = name,
        service = service
    ))
}

fn render_rotation_section(details: &SecretDetails, cx: &App) -> Div {
    let label = |key: &str| dbflux_i18n::t!(key);

    let rows = match &details.summary.rotation {
        Some(rotation) => vec![
            detail_row(
                label("document.secret_store.rotation.status"),
                if rotation.enabled {
                    label("document.secret_store.rotation.enabled")
                } else {
                    label("document.secret_store.rotation.disabled")
                },
                cx,
            ),
            detail_row(
                label("document.secret_store.rotation.schedule"),
                rotation_schedule_label(rotation),
                cx,
            ),
            detail_row(
                label("document.secret_store.rotation.next"),
                format_optional_time(rotation.next_rotation),
                cx,
            ),
            detail_row(
                label("document.secret_store.rotation.function"),
                or_unknown(rotation.function.as_deref()),
                cx,
            ),
            detail_row(
                label("document.secret_store.rotation.last"),
                format_optional_time(rotation.last_rotated),
                cx,
            ),
        ],
        None => vec![detail_row(
            label("document.secret_store.rotation.status"),
            label("document.secret_store.rotation.disabled"),
            cx,
        )],
    };

    section(
        dbflux_i18n::t!("document.secret_store.rotation.title"),
        rows,
        cx,
    )
}

/// Tags as `key=value` chips in a row that wraps. The artboard's Edit
/// button is left out until a tags-edit action exists.
fn render_tags_section(details: &SecretDetails, cx: &App) -> Div {
    let theme = cx.theme();
    let fill = theme.secondary;
    let border = theme.border;
    let muted = theme.muted_foreground;
    let family = fonts::editor_family(cx);

    let body: AnyElement = if details.tags.is_empty() {
        Text::caption(dbflux_i18n::t!("document.secret_store.tags.none"))
            .muted_foreground()
            .into_any_element()
    } else {
        div()
            .flex()
            .flex_wrap()
            .gap(LABEL_CHIP_GAP)
            .children(details.tags.iter().enumerate().map(|(index, tag)| {
                div()
                    .id(SharedString::from(format!("secret-tag-{index}")))
                    .relative()
                    .px(DocumentMetrics::GAP)
                    .py(TAG_CHIP_PADDING_Y)
                    .font_family(family.clone())
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(muted)
                    .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill).border(border))
                    .child(format!("{}={}", tag.key, tag.value))
            }))
            .into_any_element()
    };

    section(
        dbflux_i18n::t!("document.secret_store.tags.title"),
        vec![body],
        cx,
    )
}
