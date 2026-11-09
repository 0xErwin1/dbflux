//! The details pane of a `SecretView`: the selected secret's value
//! section, its details, its rotation and its tags.

mod card;
mod document;
pub(super) mod resize;
mod sections;
mod value;
mod versions;

#[cfg(test)]
pub(in crate::secret_store) use value::value_view_items;

use self::sections::managed_card;
use super::{DetailsState, SecretView, SecretViewEvent, ValueState, VersionsState};
use crate::secret_store::render::{UNKNOWN, format_optional_time, version_label};
use dbflux_app::keymap::ContextId;
use dbflux_components::controls::Button;
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{BannerBlock, BannerVariant, Icon, Text};
use dbflux_components::tokens::{
    ChromeColors, DocumentMetrics, PreviewRailMetrics, SecretStoreMetrics, SplitterMetrics, ui,
};
use dbflux_core::{SecretDetails, SecretStoreCapabilities, SecretSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::Scrollbar;

/// Lines the multi-line viewer keeps visible before it scrolls, so the
/// sections below the value keep their place.
pub(in crate::secret_store) const VIEWER_VISIBLE_LINES: f32 = 14.0;

/// Width of the viewer's line-number gutter: three digits at the editor size.
pub(in crate::secret_store) const VIEWER_GUTTER_WIDTH: Rems = ui(28.0);

/// Characters of a version id the table shows before the ellipsis.
const VERSION_ID_CHARS: usize = 8;

pub(super) fn section_title(title: String, cx: &App) -> Div {
    div()
        .text_size(DocumentMetrics::DETAIL_LABEL_FONT)
        .font_weight(FontWeight::SEMIBOLD)
        .text_color(cx.theme().muted_foreground)
        .child(title.to_uppercase())
}

/// A disabled Rotate now button that says why: the reason is its tooltip and
/// part of its accessible name.
pub(in crate::secret_store) fn rotate_with_reason(button: Button, reason: String) -> Button {
    button
        .aria_label(format!(
            "{}: {reason}",
            dbflux_i18n::t!("document.secret_store.rotate.button")
        ))
        .tooltip(reason)
}

/// One `label  value` row of a details section, the value in mono.
pub(super) fn detail_row(label: String, value: String, cx: &App) -> AnyElement {
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
pub(in crate::secret_store) fn last_retrieved_label(
    time: Option<dbflux_core::chrono::DateTime<dbflux_core::chrono::Utc>>,
) -> String {
    time.map_or_else(
        || UNKNOWN.to_string(),
        |time| time.date_naive().format("%Y-%m-%d").to_string(),
    )
}

pub(super) fn or_unknown(value: Option<&str>) -> String {
    value.map_or_else(|| UNKNOWN.to_string(), str::to_string)
}

/// The last change, with the principal that made it when the store reports
/// one.
pub(super) fn last_changed_label(details: &SecretDetails) -> String {
    let when = format_optional_time(details.summary.last_changed);

    match details.last_changed_by.as_deref() {
        Some(by) => format!("{when} · {by}"),
        None => when,
    }
}

/// `a41c09e2-…` as `a41c09e2…`: the first characters of an id longer than the
/// column, so ids that differ late stay distinguishable by the row, not the
/// text.
pub(in crate::secret_store) fn short_version_id(id: &str) -> String {
    let mut chars = id.chars();
    let head: String = chars.by_ref().take(VERSION_ID_CHARS).collect();

    if chars.next().is_some() {
        format!("{head}…")
    } else {
        head
    }
}

/// The sections of a secret's details, in the slots a host places them.
pub(in crate::secret_store) struct DetailSections {
    pub value: AnyElement,
    pub managed: Option<AnyElement>,
    pub metadata: Option<AnyElement>,
    pub versions: Option<AnyElement>,
    pub rotation: Option<AnyElement>,
    pub tags: Option<AnyElement>,
    /// The describe's loading line or failure banner.
    pub status: Option<AnyElement>,
}

impl DetailSections {
    /// The pane's single column: the value, the managed card, then the
    /// metadata, history, rotation and tags once the describe landed (the
    /// status and then the history before it did).
    fn into_column(self) -> Vec<AnyElement> {
        let described = self.metadata.is_some();
        let mut column = vec![self.value];
        column.extend(self.managed);
        column.extend(self.metadata);

        if described {
            column.extend(self.versions);
            column.extend(self.rotation);
            column.extend(self.tags);
            column.extend(self.status);
        } else {
            column.extend(self.status);
            column.extend(self.versions);
        }

        column
    }
}

impl SecretView {
    pub(in crate::secret_store) fn render_pane(
        &mut self,
        host_context: ContextId,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        self.host_context = host_context;

        let border = cx.theme().border;
        let background = cx.theme().background;

        let body: AnyElement = match (&self.name, self.selected_summary().cloned()) {
            (Some(_), Some(summary)) => self.render_secret_details(&summary, cx).into_any_element(),
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

        // The gutter sits left of the pane, between it and the listing; the
        // pane keeps its minimum width however narrow the row gets.
        div()
            .relative()
            .w(self.details_width() + SplitterMetrics::GUTTER)
            .flex_shrink_1()
            .min_w(SecretStoreMetrics::DETAILS_MIN_WIDTH + SplitterMetrics::GUTTER)
            .h_full()
            .flex()
            .child(self.measure_details_edge())
            .child(self.render_details_grip(cx))
            .child(
                div()
                    .id("secret-details")
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .flex()
                    .flex_col()
                    .border_l_1()
                    .border_color(border)
                    .bg(background)
                    .child(body),
            )
            .into_any_element()
    }

    /// The sections of the shown secret, each built once. The pane stacks
    /// them in one column; a document lays them out as it sees fit.
    pub(super) fn detail_sections(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> DetailSections {
        let mut sections = DetailSections {
            value: self.render_value_section(summary, cx).into_any_element(),
            managed: summary
                .owning_service
                .as_deref()
                .map(|service| managed_card(&summary.name, service).into_any_element()),
            metadata: None,
            versions: self.render_versions_section(cx),
            rotation: None,
            tags: None,
            status: None,
        };

        // The history loads apart from the describe, so it shows whatever
        // state the describe is in; it sits after the metadata when there is
        // one.
        match &self.details {
            DetailsState::Loaded(details) => {
                sections.metadata =
                    Some(self.render_metadata_section(details, cx).into_any_element());

                if self.store.supports(SecretStoreCapabilities::ROTATION) {
                    sections.rotation =
                        Some(self.render_rotation_section(details, cx).into_any_element());
                }

                sections.tags = Some(self.render_tags_section(details, cx).into_any_element());
            }
            DetailsState::Loading => {
                sections.status = Some(
                    Text::caption(dbflux_i18n::t!("document.secret_store.details.loading"))
                        .muted_foreground()
                        .into_any_element(),
                )
            }
            DetailsState::Failed(error) => {
                sections.status = Some(
                    BannerBlock::new(
                        BannerVariant::Danger,
                        dbflux_i18n::t!("document.secret_store.details.failed"),
                    )
                    .with_body(error.clone())
                    .into_any_element(),
                )
            }
            DetailsState::Idle => {}
        }

        sections
    }

    pub(super) fn render_secret_details(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let sections = self.detail_sections(summary, cx).into_column();

        let header = self.render_details_header(summary, cx);

        let bar = self.render_action_bar(cx);

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(header)
            .child(
                // The viewport ends where the action bar begins, so the bar
                // stays pinned and never covers content: the sections scroll
                // inside it on their own handle.
                div()
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .child(
                        div()
                            .id("secret-details-body")
                            .size_full()
                            .track_scroll(&self.details_scroll)
                            .overflow_y_scroll()
                            .flex()
                            .flex_col()
                            .gap(SecretStoreMetrics::SECTION_GAP)
                            .px(SecretStoreMetrics::DETAILS_PADDING_X)
                            .py(SecretStoreMetrics::DETAILS_PADDING_Y)
                            .children(sections),
                    )
                    .child(Scrollbar::vertical(&self.details_scroll)),
            )
            .children(bar)
    }

    /// The secret's name with the pane's controls: Copy name, open in a tab
    /// and Close. The header holds no write, so closing the pane can never
    /// end in a deletion; the writes live in the bar at the bottom. It is
    /// laid out like the object browser's preview header.
    pub(super) fn render_details_header(
        &self,
        summary: &SecretSummary,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let theme = cx.theme();
        let border = theme.border;
        let warning = theme.warning;
        let strong = ChromeColors::strong(theme);
        let name = summary.name.clone();

        let icon_button = |id: &'static str, label: String, icon: AppIcon| {
            Button::new(id, label.clone())
                .icon(icon)
                .icon_only()
                .tooltip(label)
                .tab_stop(false)
        };

        div()
            .id("secret-details-header")
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(PreviewRailMetrics::HEADER_GAP)
            .h(DocumentMetrics::HEADER_HEIGHT)
            .px(DocumentMetrics::PADDING_X)
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
                    .text_color(strong)
                    .child(name.clone()),
            )
            .child(
                icon_button(
                    "secret-details-copy-name",
                    dbflux_i18n::t!("document.secret_store.context_menu.copy_name").to_string(),
                    AppIcon::Copy,
                )
                .on_click(cx.listener(|this, _, _, cx| this.copy_selected_name(cx))),
            )
            .child(
                icon_button(
                    "secret-details-expand",
                    dbflux_i18n::t!("document.secret_store.details.open_in_tab").to_string(),
                    AppIcon::Maximize2,
                )
                .on_click(cx.listener(move |_, _, _, cx| {
                    cx.emit(SecretViewEvent::OpenInTab {
                        name: name.clone(),
                        edit: false,
                    })
                })),
            )
            .child(
                icon_button(
                    "secret-details-close",
                    dbflux_i18n::t!("document.secret_store.details.close").to_string(),
                    AppIcon::X,
                )
                .on_click(cx.listener(|_, _, _, cx| cx.emit(SecretViewEvent::CloseRequested))),
            )
    }

    /// The details the describe reported. A typed store shows its type, tier
    /// and data type; every store shows the identifier, description,
    /// encryption key, versions and the change dates it reports.
    pub(super) fn render_metadata_section(
        &self,
        details: &SecretDetails,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let rows = self.metadata_rows(details, cx);
        let edit = self.edit_description_action(cx);

        self.framed_section(
            "secret-document-details-card",
            dbflux_i18n::t!("document.secret_store.details.title"),
            None,
            edit.into_iter().collect(),
            rows,
            cx,
        )
    }

    /// One row per fact the describe reported: the deletion state first when
    /// one is scheduled, then the identifier, the typed-store fields and the
    /// change dates.
    fn metadata_rows(&self, details: &SecretDetails, cx: &mut Context<Self>) -> Vec<AnyElement> {
        let summary = &details.summary;
        let typed = self.store.supports(SecretStoreCapabilities::TYPED_VALUES);
        let label = |key: &str| dbflux_i18n::t!(key);

        let mut rows = Vec::new();

        // A scheduled deletion leads the details, as its state does. The
        // moment it was requested is shown only when the store reports it.
        if let Some(date) = self.deletion_of(summary) {
            if let Some(requested) = details.deleted_at {
                rows.push(detail_row(
                    label("document.secret_store.delete.row_deleted"),
                    format_optional_time(Some(requested)),
                    cx,
                ));
            }
            if date.is_some() {
                rows.push(detail_row(
                    label("document.secret_store.delete.row_deletes_on"),
                    format_optional_time(date),
                    cx,
                ));
            }
        }

        let resource_id = detail_row(
            label("document.secret_store.details.resource_id"),
            or_unknown(details.resource_id.as_deref()),
            cx,
        );
        rows.push(self.resource_id_row(resource_id, details.resource_id.is_some(), cx));

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

        let encryption_key = detail_row(
            label("document.secret_store.details.encryption_key"),
            or_unknown(details.encryption_key.as_deref()),
            cx,
        );
        let description = detail_row(
            label("document.secret_store.details.description"),
            or_unknown(summary.description.as_deref()),
            cx,
        );

        // The tab lists the description before the key, as its artboard does.
        if self.in_tab() {
            rows.extend([description, encryption_key]);
        } else {
            rows.extend([encryption_key, description]);
        }

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

        rows
    }

    /// The ARN row, with a copy button beside it in the tab.
    fn resource_id_row(
        &self,
        row: AnyElement,
        has_resource_id: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if !self.in_tab() || !has_resource_id {
            return row;
        }

        div()
            .flex()
            .items_center()
            .gap(DocumentMetrics::GAP)
            .child(div().flex_1().min_w_0().child(row))
            .child(
                Button::new(
                    "secret-document-details-copy-arn",
                    dbflux_i18n::t!("document.secret_store.action.copy_resource_id"),
                )
                .ghost()
                .icon(AppIcon::Copy)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| this.copy_resource_id(cx))),
            )
            .into_any_element()
    }

    /// The Edit button of the Details section, for a store that changes the
    /// description alone.
    fn edit_description_action(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        // A store that changes the description alone has the button here in
        // both hosts; one that rewrites the value for it has it in the pane's
        // bottom bar, and here too in the tab, which has no bar.
        (self.supports_description_edit()
            && (self.in_tab() || !self.description_edit_rewrites_value()))
        .then(|| {
            let button = Button::new(
                "secret-details-edit",
                dbflux_i18n::t!("document.secret_store.description_edit.button"),
            )
            .icon(AppIcon::Pencil)
            .tab_stop(false)
            .disabled(!self.can_edit_description())
            .on_click(cx.listener(|this, _, window, cx| this.open_edit_description(window, cx)));

            match self.write_refusal_reason() {
                Some(reason) => button.tooltip(reason),
                None => button,
            }
            .into_any_element()
        })
    }
}
