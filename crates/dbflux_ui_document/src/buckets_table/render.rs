//! Rendering for `BucketsTableDocument`.
//!
//! Layout, top to bottom: toolbar (search / refresh / new bucket), column
//! header, bucket rows, optional details strip for the selected bucket, and a
//! footer summary + keyboard hint bar. Every row carries a single row-level
//! mouse handler; cells are pure presentation.

use super::data::{BucketDetailsState, BucketRow, BucketSizeEstimateState};
use super::{BucketsFocusMode, BucketsTableDocument};
use crate::chrome::{
    connection_segment, document_bar, document_footer, footer_item, footer_key_hint, search_field,
};
use crate::handle::DocumentEvent;
use crate::types::DocumentState;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::composites::{
    Breadcrumb, BreadcrumbSegment, EmptyState, EmptyStateAction, ListRow,
};
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::{Icon, Status, StatusIndicator, Text};
use dbflux_components::tokens::{ChromeColors, DocumentMetrics, ObjectStoreMetrics, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_core::VersioningStatus;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

/// Placeholder for a value that has not been fetched (and never is fetched
/// automatically — see DEC-14).
const UNKNOWN: &str = "—";

/// Formats a byte count with a binary-prefix unit, one decimal place above
/// the kibibyte boundary.
pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    format!("{value:.1} {}", UNITS[unit])
}

/// Keystroke that refreshes the bucket list while the table has focus, read
/// from the keymap so the empty-state hint always names the live binding.
/// `None` when nothing in the table context refreshes the document.
pub(super) fn refresh_shortcut() -> Option<String> {
    dbflux_ui_base::effective_keymap()
        .shortcut_for_command(ContextId::Results, Command::RefreshSchema)
}

/// Footer summary line: how many buckets are listed and how many distinct
/// regions they span (regions are only counted once their lazy details land).
pub(super) fn summary_line(rows: &[&BucketRow]) -> String {
    let mut regions: Vec<&str> = rows
        .iter()
        .filter_map(|row| match &row.details {
            BucketDetailsState::Loaded(details) => Some(details.region.as_str()),
            _ => None,
        })
        .collect();
    regions.sort_unstable();
    regions.dedup();

    crate::labels::buckets_table_summary_line(rows.len(), regions.len())
}

/// What a bucket cell shows: a fetched value, a value still being fetched,
/// or the single "—" placeholder for anything unknown (not fetched yet, not
/// requested, failed, or not provided by the driver).
#[derive(Clone, Debug, PartialEq, Eq)]
enum CellValue {
    Known(String),
    Loading,
    Missing,
}

impl CellValue {
    /// Renders the value in `color`; loading and missing values stay muted
    /// so they never read as data.
    fn render(self, color: Hsla, muted: Hsla) -> AnyElement {
        match self {
            CellValue::Known(value) => div().text_color(color).child(value).into_any_element(),
            CellValue::Loading => Icon::new(AppIcon::Loader)
                .size(ObjectStoreMetrics::LOADING_ICON)
                .color(muted)
                .into_any_element(),
            CellValue::Missing => div().text_color(muted).child(UNKNOWN).into_any_element(),
        }
    }
}

fn region_value(row: &BucketRow) -> CellValue {
    match &row.details {
        BucketDetailsState::Loaded(details) => CellValue::Known(details.region.clone()),
        BucketDetailsState::Loading => CellValue::Loading,
        _ => CellValue::Missing,
    }
}

/// Versioning as the details strip spells it: `Off` for a bucket that never
/// had versioning, once its details resolved.
fn versioning_value(row: &BucketRow) -> CellValue {
    match &row.details {
        BucketDetailsState::Loaded(details) => CellValue::Known(
            crate::labels::versioning_status_label(details.versioning)
                .unwrap_or_else(crate::labels::versioning_off_label),
        ),
        BucketDetailsState::Loading => CellValue::Loading,
        _ => CellValue::Missing,
    }
}

fn object_count_value(row: &BucketRow) -> CellValue {
    match &row.size_estimate {
        BucketSizeEstimateState::Loaded(estimate) if estimate.truncated => {
            CellValue::Known(format!("{}+", estimate.object_count))
        }
        BucketSizeEstimateState::Loaded(estimate) => {
            CellValue::Known(estimate.object_count.to_string())
        }
        BucketSizeEstimateState::Loading => CellValue::Loading,
        _ => CellValue::Missing,
    }
}

fn size_value(row: &BucketRow) -> CellValue {
    match &row.size_estimate {
        BucketSizeEstimateState::Loaded(estimate) if estimate.truncated => {
            CellValue::Known(format!("{}+", format_bytes(estimate.total_bytes)))
        }
        BucketSizeEstimateState::Loaded(estimate) => {
            CellValue::Known(format_bytes(estimate.total_bytes))
        }
        BucketSizeEstimateState::Loading => CellValue::Loading,
        _ => CellValue::Missing,
    }
}

/// Default encryption once the loaded details report it; `None` hides the
/// field, since not every store (or caller) can read it.
fn encryption_value(row: &BucketRow) -> Option<CellValue> {
    match &row.details {
        BucketDetailsState::Loaded(details) => details
            .encryption
            .as_ref()
            .map(|encryption| CellValue::Known(crate::labels::bucket_encryption_label(encryption))),
        _ => None,
    }
}

/// Public-access blocking once the loaded details report it; `None` hides
/// the field.
fn public_access_value(row: &BucketRow) -> Option<CellValue> {
    match &row.details {
        BucketDetailsState::Loaded(details) => details
            .public_access
            .map(|status| CellValue::Known(crate::labels::public_access_status_label(status))),
        _ => None,
    }
}

/// Label and value of every field the details strip shows, in board order.
/// Only fields the object-store API reports are listed: region and
/// versioning from the bucket details, objects and size from the on-demand
/// estimate, and encryption and public access only when the details carry
/// them.
fn details_fields(row: &BucketRow) -> Vec<(String, CellValue)> {
    let mut fields = vec![
        (
            dbflux_i18n::t!("document.buckets_table.columns.region"),
            region_value(row),
        ),
        (
            dbflux_i18n::t!("document.buckets_table.columns.versioning"),
            versioning_value(row),
        ),
    ];

    if let Some(encryption) = encryption_value(row) {
        fields.push((
            dbflux_i18n::t!("document.buckets_table.columns.encryption"),
            encryption,
        ));
    }

    fields.push((
        dbflux_i18n::t!("document.buckets_table.columns.objects"),
        object_count_value(row),
    ));
    fields.push((
        dbflux_i18n::t!("document.buckets_table.columns.size"),
        size_value(row),
    ));

    if let Some(public_access) = public_access_value(row) {
        fields.push((
            dbflux_i18n::t!("document.buckets_table.columns.public_access"),
            public_access,
        ));
    }

    fields
}

/// Status diamond and label of a bucket's versioning, or `None` until its
/// details load.
fn versioning_status(row: &BucketRow) -> Option<(Status, String)> {
    let BucketDetailsState::Loaded(details) = &row.details else {
        return None;
    };

    let status = match details.versioning {
        VersioningStatus::Enabled => Status::Connected,
        VersioningStatus::Suspended => Status::Warning,
        VersioningStatus::Disabled => Status::Idle,
    };

    let label = crate::labels::versioning_status_label(details.versioning)
        .unwrap_or_else(crate::labels::versioning_off_label);

    Some((status, label))
}

/// Creation date of a bucket as the table shows it.
fn created_date_label(row: &BucketRow) -> String {
    row.info
        .created_at
        .map(|created| created.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| UNKNOWN.to_string())
}

/// Keystroke bound to `command` in the table, from the live keymap.
fn table_shortcut(command: Command) -> Option<String> {
    dbflux_ui_base::effective_keymap().shortcut_for_command(ContextId::Results, command)
}

impl BucketsTableDocument {
    /// Header row: the connection and "Buckets" breadcrumb, the bucket
    /// search, New bucket and the primary Refresh.
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let is_loading = self.state == DocumentState::Loading;

        let mut segments: Vec<BreadcrumbSegment> =
            connection_segment(&self.app_state, self.profile_id, cx)
                .into_iter()
                .collect();
        segments.push(BreadcrumbSegment::new(dbflux_i18n::t!(
            "document.buckets_table.breadcrumb"
        )));

        let search = div()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = BucketsFocusMode::Search;
                    cx.stop_propagation();
                    cx.notify();
                }),
            )
            .child(search_field(
                &self.search_input,
                Some(ObjectStoreMetrics::SEARCH_WIDTH),
                self.focus_mode == BucketsFocusMode::Search,
                cx,
            ));

        let mut refresh = Button::new(
            "buckets-refresh",
            dbflux_i18n::t!("document.buckets_table.toolbar.refresh"),
        )
        .primary()
        .icon(if is_loading {
            AppIcon::Loader
        } else {
            AppIcon::RefreshCcw
        })
        .tab_stop(false)
        .on_click(cx.listener(|this, _, _, cx| {
            this.load_buckets(cx);
        }));

        if let Some(key) = refresh_shortcut() {
            refresh = refresh.kbd(key);
        }

        document_bar(DocumentMetrics::HEADER_HEIGHT_TALL, cx)
            .child(Breadcrumb::new(segments))
            .child(div().flex_1())
            .child(search)
            .child(
                Button::new(
                    "buckets-new",
                    dbflux_i18n::t!("document.buckets_table.toolbar.new_bucket"),
                )
                .icon(AppIcon::Plus)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.request_new_bucket(cx);
                })),
            )
            .child(refresh)
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let column = |width: Option<Rems>, key: &str| {
            let label = dbflux_i18n::t!(key);
            match width {
                Some(width) => div().w(width).flex_shrink_0().child(label),
                None => div().flex_1().min_w_0().child(label),
            }
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(DocumentMetrics::TABLE_ROW_HEIGHT)
            .px(ObjectStoreMetrics::TABLE_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(DocumentMetrics::TABLE_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(column(None, "document.buckets_table.columns.name"))
            .child(column(
                Some(ObjectStoreMetrics::REGION_WIDTH),
                "document.buckets_table.columns.region",
            ))
            .child(column(
                Some(ObjectStoreMetrics::OBJECTS_WIDTH),
                "document.buckets_table.columns.objects",
            ))
            .child(column(
                Some(ObjectStoreMetrics::SIZE_WIDTH),
                "document.buckets_table.columns.size",
            ))
            .child(column(
                Some(ObjectStoreMetrics::VERSIONING_WIDTH),
                "document.buckets_table.columns.versioning",
            ))
            .child(column(
                Some(ObjectStoreMetrics::CREATED_WIDTH),
                "document.buckets_table.columns.created",
            ))
    }

    fn render_row(&self, row: &BucketRow, selected: bool, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let name = row.info.name.clone();
        let row_id = SharedString::from(format!("bucket-row-{name}"));
        let select_name = name.clone();
        let bucket_color = theme.warning;

        let mono_cell = |width: Rems, value: CellValue, color: Hsla| {
            div()
                .w(width)
                .flex_shrink_0()
                .flex()
                .items_center()
                .pr(DocumentMetrics::GAP)
                .truncate()
                .font_family(AppFonts::MONO)
                .text_size(DocumentMetrics::TABLE_META_FONT)
                .child(value.render(color, muted))
        };

        ListRow::new(row_id)
            .selected(selected)
            .selection_bar(true)
            .build(cx)
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(ObjectStoreMetrics::BUCKET_ROW_HEIGHT)
            .px(ObjectStoreMetrics::TABLE_PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .text_size(ObjectStoreMetrics::NAME_FONT)
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select_bucket(select_name.clone(), cx);
                cx.emit(DocumentEvent::RequestFocus);
            }))
            .child(
                div()
                    .flex()
                    .flex_1()
                    .min_w_0()
                    .items_center()
                    .gap(ObjectStoreMetrics::NAME_GAP)
                    .overflow_hidden()
                    .child(
                        Icon::new(AppIcon::Box)
                            .size(ObjectStoreMetrics::NAME_ICON)
                            .color(bucket_color),
                    )
                    .child(
                        div()
                            .truncate()
                            .font_family(AppFonts::MONO)
                            .text_color(ChromeColors::strong(theme))
                            .child(name),
                    ),
            )
            .child(mono_cell(
                ObjectStoreMetrics::REGION_WIDTH,
                region_value(row),
                muted,
            ))
            .child(mono_cell(
                ObjectStoreMetrics::OBJECTS_WIDTH,
                object_count_value(row),
                ChromeColors::strong(theme),
            ))
            .child(mono_cell(
                ObjectStoreMetrics::SIZE_WIDTH,
                size_value(row),
                theme.foreground,
            ))
            .child(
                div()
                    .w(ObjectStoreMetrics::VERSIONING_WIDTH)
                    .flex_shrink_0()
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .child(match versioning_status(row) {
                        Some((status, label)) => {
                            StatusIndicator::new(status).label(label).into_any_element()
                        }
                        None => versioning_value(row).render(muted, muted),
                    }),
            )
            .child(
                div()
                    .w(ObjectStoreMetrics::CREATED_WIDTH)
                    .flex_shrink_0()
                    .text_size(DocumentMetrics::TABLE_META_FONT)
                    .text_color(muted)
                    .child(created_date_label(row)),
            )
            .into_any_element()
    }

    /// Details strip for the selected bucket, toggled with Space. It also
    /// hosts the on-demand "Calculate size" action so the billed
    /// `estimate_bucket_size` walk stays an explicit, deliberate click.
    fn render_details(&self, row: &BucketRow, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let estimate_pending = matches!(row.size_estimate, BucketSizeEstimateState::Loading);

        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;

        let detail_pair = |label: String, value: CellValue| {
            div()
                .flex()
                .flex_col()
                .flex_shrink_0()
                .gap(DocumentMetrics::DETAIL_LABEL_GAP)
                .child(
                    div()
                        .text_size(DocumentMetrics::DETAIL_LABEL_FONT)
                        .text_color(theme.muted_foreground)
                        .child(label),
                )
                .child(
                    div()
                        .flex()
                        .items_center()
                        .font_family(AppFonts::MONO)
                        .text_size(ObjectStoreMetrics::DETAILS_VALUE_FONT)
                        .child(value.render(strong, muted)),
                )
        };

        let mut browse = Button::new(
            "buckets-browse",
            dbflux_i18n::t!("document.buckets_table.details.browse"),
        )
        .primary()
        .icon(AppIcon::ChevronRight)
        .tab_stop(false)
        .on_click(cx.listener(|this, _, _, cx| {
            this.open_selected_bucket(cx);
        }));

        if let Some(key) = table_shortcut(Command::Execute) {
            browse = browse.kbd(key);
        }

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ObjectStoreMetrics::DETAILS_GAP)
            .px(ObjectStoreMetrics::TABLE_PADDING_X)
            .py(ObjectStoreMetrics::DETAILS_PADDING_Y)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(DocumentMetrics::GAP)
                    .font_weight(FontWeight::BOLD)
                    .text_color(ChromeColors::strong(theme))
                    .child(
                        Icon::new(AppIcon::Box)
                            .size(ObjectStoreMetrics::DETAILS_ICON)
                            .color(theme.warning),
                    )
                    .child(row.info.name.clone()),
            )
            .children(
                details_fields(row)
                    .into_iter()
                    .map(|(label, value)| detail_pair(label, value)),
            )
            .child(div().flex_1())
            .child(
                Button::new(
                    "buckets-calculate-size",
                    if estimate_pending {
                        dbflux_i18n::t!("document.buckets_table.details.calculating")
                    } else {
                        dbflux_i18n::t!("document.buckets_table.details.calculate_size")
                    },
                )
                .icon(if estimate_pending {
                    AppIcon::Loader
                } else {
                    AppIcon::Sigma
                })
                .disabled(estimate_pending)
                .tab_stop(false)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.estimate_selected_bucket_size(cx);
                })),
            )
            .child(browse)
    }

    fn render_footer(&self, rows: &[&BucketRow], cx: &Context<Self>) -> impl IntoElement {
        let hints = [
            (Command::Execute, "document.buckets_table.footer.hint.open"),
            (
                Command::ExpandCollapse,
                "document.buckets_table.footer.hint.properties",
            ),
            (
                Command::ResultsAddRow,
                "document.buckets_table.footer.hint.new",
            ),
            (Command::Delete, "document.buckets_table.footer.hint.delete"),
        ]
        .into_iter()
        .filter_map(|(command, key)| {
            table_shortcut(command).map(|shortcut| footer_key_hint(shortcut, dbflux_i18n::t!(key)))
        });

        document_footer(cx)
            .h(ObjectStoreMetrics::FOOTER_HEIGHT)
            .child(footer_item(AppIcon::Box, summary_line(rows), cx))
            .child(div().flex_1())
            .children(hints)
    }

    fn render_empty_state(&self) -> AnyElement {
        let message = match (self.state, &self.last_error) {
            (DocumentState::Loading, _) => dbflux_i18n::t!("document.buckets_table.empty.loading"),
            (DocumentState::Error, Some(err)) => {
                dbflux_i18n::t!(
                    "document.buckets_table.empty.error_detail",
                    error = err.as_str()
                )
            }
            (DocumentState::Error, None) => dbflux_i18n::t!("document.buckets_table.empty.error"),
            _ if !self.search_query.trim().is_empty() => dbflux_i18n::t!(
                "document.buckets_table.empty.no_match",
                query = self.search_query.trim()
            ),
            _ => dbflux_i18n::t!("document.buckets_table.empty.no_buckets"),
        };

        let is_error = self.state == DocumentState::Error;

        let mut empty = EmptyState::new(
            if is_error {
                AppIcon::TriangleAlert
            } else {
                AppIcon::Box
            },
            message,
        );

        if is_error {
            empty = empty.danger();
        }

        if let Some(key) = refresh_shortcut() {
            empty = empty.action(EmptyStateAction::new(
                AppIcon::RefreshCcw,
                dbflux_i18n::t!("document.buckets_table.toolbar.refresh"),
                [key],
            ));
        }

        div()
            .flex_1()
            .flex()
            .items_center()
            .justify_center()
            .p(DocumentMetrics::PADDING_X)
            .child(empty)
            .into_any_element()
    }

    fn render_delete_confirm(&self, bucket: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let footer = div()
            .flex()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "buckets-delete-cancel",
                    dbflux_i18n::t!("document.buckets_table.delete_confirm.cancel"),
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.cancel_delete_bucket(cx);
                })),
            )
            .child(
                Button::new(
                    "buckets-delete-confirm",
                    dbflux_i18n::t!("document.buckets_table.delete_confirm.confirm"),
                )
                .danger()
                .icon(AppIcon::Delete)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.confirm_delete_bucket(cx);
                })),
            );

        Modal::new(dbflux_i18n::t!(
            "document.buckets_table.delete_confirm.title"
        ))
        .id("buckets-delete-overlay")
        .danger()
        .icon(AppIcon::TriangleAlert)
        .width(px(420.0))
        .body(Text::body(dbflux_i18n::t!(
            "document.buckets_table.delete_confirm.body",
            bucket = bucket
        )))
        .footer(footer)
    }
}

impl Render for BucketsTableDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // The New Bucket modal's inputs need a `Window`, which the toolbar
        // click that raised the intent never had.
        self.drain_pending_new_bucket(window, cx);

        let rows: Vec<BucketRow> = self
            .filtered_buckets(&self.search_query)
            .into_iter()
            .cloned()
            .collect();
        let row_refs: Vec<&BucketRow> = rows.iter().collect();

        let selected = self.selected_bucket().map(str::to_string);
        let details_row = self
            .show_details
            .then(|| {
                selected
                    .as_ref()
                    .and_then(|name| rows.iter().find(|row| &row.info.name == name))
                    .cloned()
            })
            .flatten();

        let pending_delete = self.pending_delete.clone();

        let body = if rows.is_empty() {
            self.render_empty_state()
        } else {
            div()
                .flex_1()
                .min_h_0()
                .flex()
                .flex_col()
                .child(
                    div()
                        .id("buckets-table-rows")
                        .min_h_0()
                        .overflow_y_scrollbar()
                        .children(rows.iter().map(|row| {
                            let is_selected = selected.as_deref() == Some(row.info.name.as_str());
                            self.render_row(row, is_selected, cx)
                        })),
                )
                .when_some(details_row, |this, row| {
                    this.child(self.render_details(&row, cx))
                })
                .into_any_element()
        };

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().popover)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = BucketsFocusMode::Table;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .child(self.render_toolbar(cx))
            .child(self.render_header(cx))
            .child(body)
            .child(self.render_footer(&row_refs, cx))
            .when_some(pending_delete, |this, bucket| {
                this.child(self.render_delete_confirm(&bucket, cx))
            })
            .when(self.new_bucket().is_some(), |this| {
                this.child(self.render_new_bucket_modal(cx))
            })
    }
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the standard
    // `#[test]` attribute below.
    use super::{BucketDetailsState, BucketRow, BucketSizeEstimateState};
    use super::{
        CellValue, details_fields, format_bytes, object_count_value, region_value, size_value,
        summary_line, versioning_value,
    };
    use dbflux_core::{
        BucketDetails, BucketEncryption, BucketInfo, BucketSizeEstimate, PublicAccessStatus,
        VersioningStatus,
    };

    fn row(name: &str, region: Option<&str>) -> BucketRow {
        BucketRow {
            info: BucketInfo {
                name: name.to_string(),
                created_at: None,
            },
            details: match region {
                Some(region) => BucketDetailsState::Loaded(BucketDetails {
                    region: region.to_string(),
                    versioning: VersioningStatus::Enabled,
                    encryption: None,
                    public_access: None,
                }),
                None => BucketDetailsState::NotLoaded,
            },
            size_estimate: BucketSizeEstimateState::NotRequested,
        }
    }

    /// T20: byte formatting steps through binary units and keeps whole bytes
    /// below the first boundary.
    #[test]
    fn format_bytes_uses_binary_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1024 * 1024 * 3), "3.0 MiB");
    }

    /// T20: the footer summary counts visible buckets and the distinct
    /// regions among the rows whose details already resolved.
    #[test]
    fn summary_line_counts_buckets_and_distinct_regions() {
        let rows = [
            row("a", Some("us-east-1")),
            row("b", Some("us-east-1")),
            row("c", Some("eu-west-1")),
            row("d", None),
        ];
        let refs: Vec<&BucketRow> = rows.iter().collect();

        assert_eq!(summary_line(&refs), "4 buckets · 2 regions");
    }

    /// T20: singular wording for a single bucket in a single region.
    #[test]
    fn summary_line_uses_singular_wording() {
        let rows = [row("only", Some("us-east-1"))];
        let refs: Vec<&BucketRow> = rows.iter().collect();

        assert_eq!(summary_line(&refs), "1 bucket · 1 region");
    }

    /// T20: an unfetched estimate renders as the em-dash placeholder — the
    /// table never implies a count it did not pay for.
    #[test]
    fn object_and_size_values_stay_unknown_until_estimated() {
        let row = row("a", Some("us-east-1"));

        assert_eq!(object_count_value(&row), CellValue::Missing);
        assert_eq!(size_value(&row), CellValue::Missing);
    }

    /// T20: a truncated estimate is marked so the user can tell the walk hit
    /// the object cap.
    #[test]
    fn truncated_estimate_is_marked() {
        let mut row = row("a", Some("us-east-1"));
        row.size_estimate = BucketSizeEstimateState::Loaded(BucketSizeEstimate {
            object_count: 10_000,
            total_bytes: 2048,
            truncated: true,
        });

        assert_eq!(
            object_count_value(&row),
            CellValue::Known("10000+".to_string())
        );
        assert_eq!(size_value(&row), CellValue::Known("2.0 KiB+".to_string()));
    }

    /// Values still being fetched share one loading state, distinct from the
    /// placeholder of a value that is not known.
    #[test]
    fn fetches_in_flight_render_as_loading_not_as_a_dash() {
        let mut row = row("a", None);
        row.details = BucketDetailsState::Loading;
        row.size_estimate = BucketSizeEstimateState::Loading;

        assert_eq!(region_value(&row), CellValue::Loading);
        assert_eq!(versioning_value(&row), CellValue::Loading);
        assert_eq!(object_count_value(&row), CellValue::Loading);
        assert_eq!(size_value(&row), CellValue::Loading);
    }

    /// Failed lookups fall back to the same placeholder as unfetched ones.
    #[test]
    fn failed_lookups_render_as_missing() {
        let mut row = row("a", None);
        row.details = BucketDetailsState::Error("denied".to_string());
        row.size_estimate = BucketSizeEstimateState::Error("denied".to_string());

        assert_eq!(region_value(&row), CellValue::Missing);
        assert_eq!(versioning_value(&row), CellValue::Missing);
        assert_eq!(size_value(&row), CellValue::Missing);
    }

    /// Versioning reads `On` once enabled, `Off` once resolved as disabled,
    /// and stays unknown until the details land.
    #[test]
    fn versioning_value_reflects_details_state() {
        assert_eq!(
            versioning_value(&row("a", Some("us-east-1"))),
            CellValue::Known(dbflux_i18n::t!("document.buckets_table.versioning.on"))
        );
        assert_eq!(versioning_value(&row("b", None)), CellValue::Missing);

        let mut disabled = row("c", Some("us-east-1"));
        disabled.details = BucketDetailsState::Loaded(BucketDetails {
            region: "us-east-1".to_string(),
            versioning: VersioningStatus::Disabled,
            encryption: None,
            public_access: None,
        });
        assert_eq!(
            versioning_value(&disabled),
            CellValue::Known(crate::labels::versioning_off_label())
        );
    }

    /// The details strip lists region, versioning, objects and size, in that
    /// order, with the row's current values.
    #[test]
    fn details_strip_lists_the_fields_the_driver_reports() {
        let fields = details_fields(&row("a", Some("eu-west-1")));

        let labels: Vec<&str> = fields.iter().map(|(label, _)| label.as_str()).collect();
        assert_eq!(
            labels,
            vec![
                dbflux_i18n::t!("document.buckets_table.columns.region"),
                dbflux_i18n::t!("document.buckets_table.columns.versioning"),
                dbflux_i18n::t!("document.buckets_table.columns.objects"),
                dbflux_i18n::t!("document.buckets_table.columns.size"),
            ]
        );
        assert_eq!(fields[0].1, CellValue::Known("eu-west-1".to_string()));
        assert_eq!(fields[2].1, CellValue::Missing);
    }

    /// Encryption and public access join the strip in board order once the
    /// details report them.
    #[test]
    fn details_strip_adds_encryption_and_public_access_when_known() {
        let mut bucket = row("a", Some("eu-west-1"));
        bucket.details = BucketDetailsState::Loaded(BucketDetails {
            region: "eu-west-1".to_string(),
            versioning: VersioningStatus::Enabled,
            encryption: Some(BucketEncryption::SseKms { key_id: None }),
            public_access: Some(PublicAccessStatus::Blocked),
        });

        let fields = details_fields(&bucket);

        let labels: Vec<&str> = fields.iter().map(|(label, _)| label.as_str()).collect();
        assert_eq!(
            labels,
            vec![
                dbflux_i18n::t!("document.buckets_table.columns.region"),
                dbflux_i18n::t!("document.buckets_table.columns.versioning"),
                dbflux_i18n::t!("document.buckets_table.columns.encryption"),
                dbflux_i18n::t!("document.buckets_table.columns.objects"),
                dbflux_i18n::t!("document.buckets_table.columns.size"),
                dbflux_i18n::t!("document.buckets_table.columns.public_access"),
            ]
        );
        assert_eq!(fields[2].1, CellValue::Known("SSE-KMS".to_string()));
        assert_eq!(
            fields[5].1,
            CellValue::Known(dbflux_i18n::t!(
                "document.buckets_table.public_access.blocked"
            ))
        );
    }
}
