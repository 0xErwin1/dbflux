//! The rotation, tags and managed-by sections of the details pane.

use super::versions::LABEL_CHIP_GAP;
use super::{DetailsState, SecretView};
use super::{detail_row, or_unknown, rotate_with_reason};
use crate::secret_store::render::{UNKNOWN, format_optional_time};
use dbflux_components::controls::Button;
use dbflux_components::fonts;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{BannerBlock, BannerVariant, Chamfer, Text};
use dbflux_components::tokens::{ChamferCut, DocumentMetrics};
use dbflux_core::{SecretDetails, SecretRotation, SecretSummary};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Vertical padding of a tag chip, as the artboard sets it.
const TAG_CHIP_PADDING_Y: Pixels = px(3.0); // guardrail-allow: artboard 01 tag chip padding, no token carries 3 px here

impl SecretView {
    /// The Tags section: the tags as chips, and Edit when the store edits
    /// them. The button says Edit and is named Edit tags, as the narrow pane
    /// of the design draws it.
    pub(super) fn render_tags_section(
        &self,
        details: &SecretDetails,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let edit = self.supports_tags().then(|| {
            let button = Button::new(
                "secret-tags-edit",
                dbflux_i18n::t!("document.secret_store.tags_edit.button"),
            )
            .icon(AppIcon::Tag)
            .tab_stop(false)
            .disabled(!self.can_edit_tags())
            .on_click(cx.listener(|this, _, window, cx| this.open_edit_tags(window, cx)));

            match self.write_refusal_reason() {
                Some(reason) => button.tooltip(reason),
                None => button,
            }
            .into_any_element()
        });

        self.framed_section(
            "secret-document-tags-card",
            dbflux_i18n::t!("document.secret_store.tags.title"),
            None,
            edit.into_iter().collect(),
            vec![tag_chips(details, cx)],
            cx,
        )
    }

    /// The Rotation section: the settings the store reports, the warning a
    /// cancelled rotation left, and the buttons that start or edit one.
    pub(super) fn render_rotation_section(
        &self,
        details: &SecretDetails,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut rows = rotation_rows(details, self.in_tab(), cx);

        if let Some(notice) = self.rotation_notice_for_selection() {
            rows.push(
                div()
                    .id("secret-rotation-notice")
                    .child(
                        BannerBlock::new(
                            BannerVariant::Warning,
                            dbflux_i18n::t!(
                                "document.secret_store.rotation_edit.unfinished_title",
                                version = notice.version.as_str()
                            ),
                        )
                        .with_body(dbflux_i18n::t!(
                            "document.secret_store.rotation_edit.unfinished_body",
                            version = notice.version.as_str()
                        )),
                    )
                    .into_any_element(),
            );
        }

        if self.supports_rotation() {
            let refusal = self.write_refusal_reason();
            let edit = Button::new(
                "secret-rotation-edit",
                dbflux_i18n::t!("document.secret_store.rotation_edit.button"),
            )
            .icon(AppIcon::Pencil)
            .tab_stop(false)
            .disabled(!self.can_edit_rotation())
            .on_click(cx.listener(|this, _, window, cx| this.open_edit_rotation(window, cx)));
            let rotate = Button::new(
                "secret-rotation-rotate-now",
                dbflux_i18n::t!("document.secret_store.rotate.immediately"),
            )
            .icon(AppIcon::Zap)
            .tab_stop(false)
            .disabled(!self.can_rotate_now())
            .on_click(cx.listener(|this, _, window, cx| this.rotate_selected_now(window, cx)));

            let edit = match refusal {
                Some(reason) => edit.tooltip(reason),
                None => edit,
            };
            let rotate = match self.rotate_now_block_reason() {
                Some(reason) => rotate_with_reason(rotate, reason),
                None => rotate,
            };

            rows.push(
                div()
                    .flex()
                    .flex_wrap()
                    .gap(DocumentMetrics::GAP)
                    .child(edit)
                    // The pane's bottom bar owns Rotate now; the tab has no bar.
                    .when(self.in_tab(), |row| row.child(rotate))
                    .into_any_element(),
            );
        }

        self.framed_section(
            "secret-document-rotation-card",
            dbflux_i18n::t!("document.secret_store.rotation.title"),
            None,
            Vec::new(),
            rows,
            cx,
        )
    }

    /// The scheduled deletion of the secret: the date it takes effect, when
    /// it was requested and with which window, and Restore.
    pub(super) fn render_pending_deletion(
        &self,
        summary: &SecretSummary,
        date: Option<dbflux_core::chrono::DateTime<dbflux_core::chrono::Utc>>,
        cx: &mut Context<Self>,
    ) -> Div {
        let deleted_at = match &self.details {
            DetailsState::Loaded(details) => details.deleted_at,
            _ => None,
        };

        let body = match deleted_at.zip(date) {
            Some((deleted_at, date)) => dbflux_i18n::t!(
                "document.secret_store.delete.scheduled_detail",
                name = summary.name.as_str(),
                deleted = format_optional_time(Some(deleted_at)),
                days = (date - deleted_at).num_days().max(0)
            ),
            None => dbflux_i18n::t!("document.secret_store.value.deletion_scheduled_body"),
        };

        let mut banner = BannerBlock::new(
            BannerVariant::Warning,
            match date {
                Some(date) => dbflux_i18n::t!(
                    "document.secret_store.value.deletion_scheduled",
                    date = crate::secret_store::render::format_time(date)
                ),
                None => dbflux_i18n::t!("document.secret_store.value.deletion_scheduled_unknown"),
            },
        )
        .with_body(body);

        if self.supports_restore() {
            banner = banner.with_actions(
                Button::new(
                    "secret-restore",
                    dbflux_i18n::t!("document.secret_store.restore.button"),
                )
                .primary()
                .icon(AppIcon::RotateCcw)
                .tab_stop(false)
                .disabled(!self.can_restore_selected())
                .on_click(cx.listener(|this, _, _, cx| this.restore_selected(cx))),
            );
        }

        div().child(banner)
    }
}

pub(super) fn managed_card(name: &str, service: &str) -> BannerBlock {
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

/// The rows of the Rotation section: the settings the store reports.
pub(super) fn rotation_rows(details: &SecretDetails, in_tab: bool, cx: &App) -> Vec<AnyElement> {
    let label = |key: &str| dbflux_i18n::t!(key);

    // The tab lists what the store knows, in the artboard's order: status,
    // schedule, window, function, last and next rotation.
    if in_tab && let Some(rotation) = &details.summary.rotation {
        let mut rows = vec![detail_row(
            label("document.secret_store.rotation.status"),
            if rotation.enabled {
                label("document.secret_store.rotation.enabled")
            } else {
                label("document.secret_store.rotation.disabled")
            },
            cx,
        )];

        if let Some(schedule) = &rotation.schedule {
            rows.push(detail_row(
                label("document.secret_store.rotation.schedule"),
                schedule.clone(),
                cx,
            ));
        }
        if let Some(window) = &rotation.window {
            rows.push(detail_row(
                label("document.secret_store.rotation.window"),
                window.clone(),
                cx,
            ));
        }
        if let Some(function) = &rotation.function {
            rows.push(detail_row(
                label("document.secret_store.rotation.function"),
                function.clone(),
                cx,
            ));
        }
        if let Some(last) = rotation.last_rotated {
            rows.push(detail_row(
                label("document.secret_store.rotation.last"),
                format_optional_time(Some(last)),
                cx,
            ));
        }
        if let Some(next) = rotation.next_rotation {
            rows.push(detail_row(
                label("document.secret_store.rotation.next"),
                format_optional_time(Some(next)),
                cx,
            ));
        }

        return rows;
    }

    match &details.summary.rotation {
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
    }
}

/// Tags as `key=value` chips in a row that wraps.
pub(super) fn tag_chips(details: &SecretDetails, cx: &App) -> AnyElement {
    let theme = cx.theme();
    let fill = theme.secondary;
    let border = theme.border;
    let muted = theme.muted_foreground;
    let family = fonts::editor_family(cx);

    if details.tags.is_empty() {
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
    }
}

pub(super) fn rotation_schedule_label(rotation: &SecretRotation) -> String {
    match (rotation.schedule.as_deref(), rotation.window.as_deref()) {
        (Some(schedule), Some(window)) => dbflux_i18n::t!(
            "document.secret_store.rotation.schedule_with_window",
            schedule = crate::secret_store::render::rotation_label(Some(rotation)),
            window = window,
            expression = schedule
        ),
        (Some(_), None) => crate::secret_store::render::rotation_label(Some(rotation)),
        (None, _) => UNKNOWN.to_string(),
    }
}
