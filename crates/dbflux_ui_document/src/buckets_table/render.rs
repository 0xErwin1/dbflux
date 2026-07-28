//! Rendering for `BucketsTableDocument`.
//!
//! Layout, top to bottom: toolbar (search / refresh / new bucket), column
//! header, bucket rows, optional details strip for the selected bucket, and a
//! footer summary + keyboard hint bar. Every row carries a single row-level
//! mouse handler; cells are pure presentation.

use super::data::{BucketDetailsState, BucketRow, BucketSizeEstimateState};
use super::{BucketsFocusMode, BucketsTableDocument};
use crate::handle::DocumentEvent;
use crate::types::DocumentState;
use dbflux_components::controls::Input;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, overlay_bg, surface_panel};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::VersioningStatus;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Column widths. `Name` takes the remaining space; the rest are fixed so the
/// numeric columns stay right-aligned against a stable edge.
const REGION_WIDTH: Pixels = px(120.0);
const OBJECTS_WIDTH: Pixels = px(96.0);
const SIZE_WIDTH: Pixels = px(112.0);
const VERSIONING_WIDTH: Pixels = px(96.0);
const CREATED_WIDTH: Pixels = px(160.0);

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

    let bucket_word = if rows.len() == 1 { "bucket" } else { "buckets" };
    let region_word = if regions.len() == 1 {
        "region"
    } else {
        "regions"
    };

    format!(
        "{} {bucket_word} · {} {region_word}",
        rows.len(),
        regions.len()
    )
}

fn region_label(row: &BucketRow) -> String {
    match &row.details {
        BucketDetailsState::Loaded(details) => details.region.clone(),
        BucketDetailsState::Loading => "…".to_string(),
        _ => UNKNOWN.to_string(),
    }
}

fn versioning_label(row: &BucketRow) -> Option<&'static str> {
    match &row.details {
        BucketDetailsState::Loaded(details) => match details.versioning {
            VersioningStatus::Enabled => Some("on"),
            VersioningStatus::Suspended => Some("suspended"),
            VersioningStatus::Disabled => None,
        },
        _ => None,
    }
}

fn object_count_label(row: &BucketRow) -> String {
    match &row.size_estimate {
        BucketSizeEstimateState::Loaded(estimate) if estimate.truncated => {
            format!("{}+", estimate.object_count)
        }
        BucketSizeEstimateState::Loaded(estimate) => estimate.object_count.to_string(),
        BucketSizeEstimateState::Loading => "…".to_string(),
        _ => UNKNOWN.to_string(),
    }
}

fn size_label(row: &BucketRow) -> String {
    match &row.size_estimate {
        BucketSizeEstimateState::Loaded(estimate) if estimate.truncated => {
            format!("{}+", format_bytes(estimate.total_bytes))
        }
        BucketSizeEstimateState::Loaded(estimate) => format_bytes(estimate.total_bytes),
        BucketSizeEstimateState::Loading => "…".to_string(),
        _ => UNKNOWN.to_string(),
    }
}

fn created_label(row: &BucketRow) -> String {
    row.info
        .created_at
        .map(|created| created.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| UNKNOWN.to_string())
}

impl BucketsTableDocument {
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_loading = self.state == DocumentState::Loading;

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .flex_1()
                    .items_center()
                    .gap(Spacing::SM)
                    .max_w(px(360.0))
                    .child(Icon::new(AppIcon::Search).small().muted())
                    .child(
                        div()
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.focus_mode = BucketsFocusMode::Search;
                                    cx.stop_propagation();
                                    cx.notify();
                                }),
                            )
                            .child(
                                Input::new(&self.search_input)
                                    .small()
                                    .cleanable(true)
                                    .w_full(),
                            ),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .id("buckets-refresh")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .hover(|d| d.bg(theme.secondary))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.load_buckets(cx);
                            }))
                            .child(
                                Icon::new(if is_loading {
                                    AppIcon::Loader
                                } else {
                                    AppIcon::RefreshCcw
                                })
                                .small()
                                .muted(),
                            )
                            .child(Text::caption("Refresh")),
                    )
                    .child(
                        div()
                            .id("buckets-new")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .bg(theme.primary)
                            .hover(|d| d.opacity(0.9))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.request_new_bucket(cx);
                            }))
                            .child(
                                Icon::new(AppIcon::Plus)
                                    .size(Heights::ICON_SM)
                                    .color(theme.primary_foreground),
                            )
                            .child(Text::caption("New bucket").color(theme.primary_foreground)),
                    ),
            )
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(div().flex_1().child(Text::caption("Name")))
            .child(div().w(REGION_WIDTH).child(Text::caption("Region")))
            .child(
                div()
                    .w(OBJECTS_WIDTH)
                    .flex()
                    .justify_end()
                    .child(Text::caption("Objects")),
            )
            .child(
                div()
                    .w(SIZE_WIDTH)
                    .flex()
                    .justify_end()
                    .child(Text::caption("Size")),
            )
            .child(div().w(VERSIONING_WIDTH).child(Text::caption("Versioning")))
            .child(div().w(CREATED_WIDTH).child(Text::caption("Created")))
    }

    fn render_row(&self, row: &BucketRow, selected: bool, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let name = row.info.name.clone();
        let row_id = SharedString::from(format!("bucket-row-{name}"));
        let select_name = name.clone();

        div()
            .id(row_id)
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .h(Heights::ROW)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .cursor_pointer()
            .when(selected, |d| d.bg(theme.list_active))
            .when(!selected, |d| d.hover(|d| d.bg(theme.list_active)))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    this.select_bucket(select_name.clone(), cx);
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .child(
                div()
                    .flex()
                    .flex_1()
                    .items_center()
                    .gap(Spacing::SM)
                    .overflow_hidden()
                    .child(Icon::new(AppIcon::Box).small().muted())
                    .child(
                        div()
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(Text::code(name)),
                    ),
            )
            .child(
                div()
                    .w(REGION_WIDTH)
                    .child(Text::code(region_label(row)).muted_foreground()),
            )
            .child(
                div()
                    .w(OBJECTS_WIDTH)
                    .flex()
                    .justify_end()
                    .child(Text::code(object_count_label(row))),
            )
            .child(
                div()
                    .w(SIZE_WIDTH)
                    .flex()
                    .justify_end()
                    .child(Text::code(size_label(row))),
            )
            .child(
                div()
                    .w(VERSIONING_WIDTH)
                    .child(match versioning_label(row) {
                        Some(label) => Text::code(label).success(),
                        None => Text::code(UNKNOWN).muted_foreground(),
                    }),
            )
            .child(
                div()
                    .w(CREATED_WIDTH)
                    .child(Text::code(created_label(row)).muted_foreground()),
            )
            .into_any_element()
    }

    /// Details strip for the selected bucket, toggled with Space. It also
    /// hosts the on-demand "Calculate size" action so the billed
    /// `estimate_bucket_size` walk stays an explicit, deliberate click.
    fn render_details(&self, row: &BucketRow, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let estimate_pending = matches!(row.size_estimate, BucketSizeEstimateState::Loading);

        let versioning = versioning_label(row).unwrap_or("off");

        let detail_pair = |label: &'static str, value: String| {
            div()
                .flex()
                .flex_col()
                .gap(Spacing::XXS)
                .child(Text::caption(label))
                .child(Text::code(value))
        };

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::LG)
            .px(Spacing::SM)
            .py(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XL)
                    .child(detail_pair("Region", region_label(row)))
                    .child(detail_pair("Versioning", versioning.to_string()))
                    .child(detail_pair("Created", created_label(row)))
                    .child(detail_pair("Objects", object_count_label(row)))
                    .child(detail_pair("Size", size_label(row))),
            )
            .child(
                div()
                    .id("buckets-calculate-size")
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .h(Heights::CONTROL)
                    .px(Spacing::SM)
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.border)
                    .when(!estimate_pending, |d| {
                        d.cursor_pointer()
                            .hover(|d| d.bg(theme.muted))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.estimate_selected_bucket_size(cx);
                            }))
                    })
                    .when(estimate_pending, |d| d.opacity(0.6))
                    .child(Icon::new(AppIcon::Sigma).small().muted())
                    .child(Text::caption(if estimate_pending {
                        "Calculating…"
                    } else {
                        "Calculate size"
                    })),
            )
    }

    fn render_footer(&self, rows: &[&BucketRow], cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex()
            .items_center()
            .justify_between()
            .gap(Spacing::MD)
            .h(Heights::ROW_COMPACT)
            .px(Spacing::SM)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.tab_bar)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(Icon::new(AppIcon::Box).small().muted())
                    .child(Text::caption(summary_line(rows))),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::MD)
                    .child(Text::key_hint("Enter browse"))
                    .child(Text::key_hint("Space properties"))
                    .child(Text::key_hint("x delete (empty only)")),
            )
    }

    fn render_empty_state(&self) -> AnyElement {
        let message = match (self.state, &self.last_error) {
            (DocumentState::Loading, _) => "Loading buckets…".to_string(),
            (DocumentState::Error, Some(err)) => format!("Failed to list buckets: {err}"),
            (DocumentState::Error, None) => "Failed to list buckets".to_string(),
            _ if !self.search_query.trim().is_empty() => {
                format!("No buckets match \"{}\"", self.search_query.trim())
            }
            _ => "This connection has no buckets".to_string(),
        };

        let is_error = self.state == DocumentState::Error;

        div()
            .flex_1()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .child(
                Icon::new(if is_error {
                    AppIcon::TriangleAlert
                } else {
                    AppIcon::Box
                })
                .size(Heights::ICON_LG)
                .muted(),
            )
            .child(if is_error {
                Text::body(message).danger()
            } else {
                Text::muted(message)
            })
            .child(Text::key_hint("r refresh"))
            .into_any_element()
    }

    fn render_delete_confirm(&self, bucket: &str, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .id("buckets-delete-overlay")
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                surface_panel(cx)
                    .rounded(Radii::MD)
                    .min_w(px(340.0))
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .p(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                Icon::new(AppIcon::TriangleAlert)
                                    .size(Heights::ICON_MD)
                                    .warning(),
                            )
                            .child(Text::heading("Delete bucket?")),
                    )
                    .child(Text::muted(format!(
                        "\"{bucket}\" is empty and will be removed. This cannot be undone."
                    )))
                    .child(
                        div()
                            .flex()
                            .justify_end()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("buckets-delete-cancel")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(theme.muted))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_delete_bucket(cx);
                                    }))
                                    .child(Text::caption("Cancel")),
                            )
                            .child(
                                div()
                                    .id("buckets-delete-confirm")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .px(Spacing::SM)
                                    .py(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.danger)
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_delete_bucket(cx);
                                    }))
                                    .child(
                                        Icon::new(AppIcon::Delete)
                                            .size(Heights::ICON_SM)
                                            .color(theme.background),
                                    )
                                    .child(Text::caption("Delete").color(theme.background)),
                            ),
                    ),
            )
    }
}

impl Render for BucketsTableDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
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
                .overflow_hidden()
                .children(rows.iter().map(|row| {
                    let is_selected = selected.as_deref() == Some(row.info.name.as_str());
                    self.render_row(row, is_selected, cx)
                }))
                .into_any_element()
        };

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .bg(cx.theme().background)
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
            .when_some(details_row, |this, row| {
                this.child(self.render_details(&row, cx))
            })
            .child(body)
            .child(self.render_footer(&row_refs, cx))
            .when_some(pending_delete, |this, bucket| {
                this.child(self.render_delete_confirm(&bucket, cx))
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
        UNKNOWN, format_bytes, object_count_label, size_label, summary_line, versioning_label,
    };
    use dbflux_core::{BucketDetails, BucketInfo, BucketSizeEstimate, VersioningStatus};

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
    fn object_and_size_labels_stay_unknown_until_estimated() {
        let row = row("a", Some("us-east-1"));

        assert_eq!(object_count_label(&row), UNKNOWN);
        assert_eq!(size_label(&row), UNKNOWN);
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

        assert_eq!(object_count_label(&row), "10000+");
        assert_eq!(size_label(&row), "2.0 KiB+");
    }

    /// T20: versioning renders only for buckets whose details resolved, and
    /// `Disabled` collapses to the muted placeholder rather than a label.
    #[test]
    fn versioning_label_reflects_details_state() {
        assert_eq!(versioning_label(&row("a", Some("us-east-1"))), Some("on"));
        assert_eq!(versioning_label(&row("b", None)), None);
    }
}
