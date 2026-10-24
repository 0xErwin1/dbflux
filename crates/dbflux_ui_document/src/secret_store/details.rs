//! The details pane of `SecretStoreDocument`: the selected secret's value
//! section, its details, its rotation and its tags.

use super::render::{UNKNOWN, format_optional_time, listing_shortcut};
use super::value::{RetrievedValue, ValueContent, ValueFailure, ValueView};
use super::{DetailsState, SecretRowKey, SecretStoreDocument, ValueState};
use crate::buckets_table::format_bytes;
use dbflux_app::keymap::Command;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Icon, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, SecretStoreMetrics};
use dbflux_core::{SecretDetails, SecretRotation, SecretStoreCapabilities, SecretSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

/// The masked text a value field shows until it is revealed.
const MASK: &str = "••••••••••••";

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

fn or_unknown(value: Option<&str>) -> String {
    value.map_or_else(|| UNKNOWN.to_string(), str::to_string)
}

/// "4 · AWSCURRENT, AWSPREVIOUS": the version count and the labels.
fn versions_label(details: &SecretDetails) -> String {
    let count = details.version_count.map(|count| count.to_string());
    let labels = (!details.version_labels.is_empty()).then(|| details.version_labels.join(", "));

    match (count, labels) {
        (Some(count), Some(labels)) => format!("{count} · {labels}"),
        (Some(count), None) => count,
        (None, Some(labels)) => labels,
        (None, None) => UNKNOWN.to_string(),
    }
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

        match &self.details {
            DetailsState::Loaded(details) => {
                sections.push(self.render_metadata_section(details, cx).into_any_element());

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

        let theme = cx.theme();

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
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .child(
                        Icon::new(AppIcon::KeyRound)
                            .size(DocumentMetrics::TITLE_ICON)
                            .color(theme.warning),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .truncate()
                            .font_family(dbflux_components::fonts::editor_family(cx))
                            .font_weight(FontWeight::BOLD)
                            .text_size(SecretStoreMetrics::NAME_FONT)
                            .text_color(ChromeColors::strong(theme))
                            .child(summary.name.clone()),
                    ),
            )
            .children(sections)
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
                or_unknown(summary.version.as_deref()),
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
                format_optional_time(details.last_accessed),
                cx,
            ));
        }

        if self.store.supports(SecretStoreCapabilities::VERSIONS) && !typed {
            rows.push(detail_row(
                label("document.secret_store.details.versions"),
                versions_label(details),
                cx,
            ));
        }

        section(
            dbflux_i18n::t!("document.secret_store.details.title"),
            rows,
            cx,
        )
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
            .child(body)
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

    fn render_value_not_loaded(&self, cx: &mut Context<Self>) -> Div {
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

        div()
            .flex()
            .flex_col()
            .gap(SecretStoreMetrics::ROW_GAP)
            .child(Text::body(dbflux_i18n::t!(
                "document.secret_store.value.not_loaded"
            )))
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
            .child(div().child(self.retrieve_button(cx)))
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
            vec![
                SegmentedItem::new(
                    "key-value",
                    dbflux_i18n::t!("document.secret_store.value.view.key_value"),
                ),
                SegmentedItem::new(
                    "plaintext",
                    dbflux_i18n::t!("document.secret_store.value.view.plaintext"),
                ),
            ],
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

        body = match &value.content {
            ValueContent::KeyValue { .. } => body.child(self.render_value_view_switch(value, cx)),
            ValueContent::Plaintext(_) => body.child(
                div()
                    .flex()
                    .flex_col()
                    .gap(SecretStoreMetrics::ROW_GAP)
                    .child(
                        Button::new(
                            "secret-value-view-key-value-unavailable",
                            dbflux_i18n::t!("document.secret_store.value.view.key_value"),
                        )
                        .disabled(true)
                        .tab_stop(false),
                    )
                    .child(
                        Text::caption(dbflux_i18n::t!(
                            "document.secret_store.value.plaintext_only"
                        ))
                        .muted_foreground(),
                    ),
            ),
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
    /// secret-marked read-only input, and its reveal and copy buttons.
    fn render_value_field(
        &self,
        index: usize,
        key: Option<String>,
        revealed: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
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
            .font_family(dbflux_components::fonts::editor_family(cx))
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
            .child(
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
                .on_click(cx.listener(move |this, _, _, cx| this.toggle_reveal(index, cx))),
            )
            .child(
                Button::new(
                    SharedString::from(format!("secret-value-copy-{index}")),
                    dbflux_i18n::t!("document.secret_store.value.copy"),
                )
                .ghost()
                .icon(AppIcon::Copy)
                .icon_only()
                .tab_stop(false)
                .on_click(cx.listener(move |this, _, _, cx| this.copy_value_field(index, cx))),
            )
            .into_any_element()
    }

    fn render_value_actions(&self, value: &RetrievedValue, cx: &mut Context<Self>) -> Div {
        let copy_label = if value.content.has_key_value_view() {
            dbflux_i18n::t!("document.secret_store.value.copy_json")
        } else {
            dbflux_i18n::t!("document.secret_store.value.copy_all")
        };

        let mut copy_all = Button::new("secret-value-copy-all", copy_label)
            .icon(AppIcon::Copy)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.copy_full_value(cx)));

        if let Some(key) = listing_shortcut(Command::ResultsCopyRow) {
            copy_all = copy_all.kbd(key);
        }

        let (reveal_key, reveal_icon) = if value.all_revealed() {
            ("document.secret_store.value.hide_all", AppIcon::EyeOff)
        } else {
            ("document.secret_store.value.show_all", AppIcon::Eye)
        };

        let mut reveal_all = Button::new("secret-value-hide-all", dbflux_i18n::t!(reveal_key))
            .icon(reveal_icon)
            .tab_stop(false)
            .on_click(cx.listener(|this, _, _, cx| this.toggle_reveal_all(cx)));

        if let Some(key) = listing_shortcut(Command::ExpandCollapse) {
            reveal_all = reveal_all.kbd(key);
        }

        div()
            .flex()
            .flex_wrap()
            .gap(DocumentMetrics::GAP)
            .child(reveal_all)
            .child(copy_all)
    }
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

fn render_tags_section(details: &SecretDetails, cx: &App) -> Div {
    let rows: Vec<AnyElement> = if details.tags.is_empty() {
        vec![
            Text::caption(dbflux_i18n::t!("document.secret_store.tags.none"))
                .muted_foreground()
                .into_any_element(),
        ]
    } else {
        details
            .tags
            .iter()
            .map(|tag| detail_row(tag.key.clone(), tag.value.clone(), cx))
            .collect()
    };

    section(
        dbflux_i18n::t!("document.secret_store.tags.title"),
        rows,
        cx,
    )
}
