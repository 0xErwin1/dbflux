use super::*;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::{ObjectFacet, VisualQuerySpec};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Boxed click handler used by filter-bar render helpers.
pub(crate) type FilterBarClickHandler = Box<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;

/// Tracks the current relational-filter state for the filter-bar chip and
/// inline error affordance.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) enum RelationalFilterState {
    /// Raw filter (or empty input) — chip hidden, no inline error.
    #[default]
    Inactive,
    /// FK cache is loading; a subtle spinner is shown in the chip area.
    Resolving,
    /// Relational lowering succeeded; chip visible with join count.
    Active {
        join_count: usize,
        predicate_count: usize,
    },
    /// Resolve or depth error; inline diagnostic + "Open in builder" link.
    Error {
        message: String,
        partial_spec: Box<VisualQuerySpec>,
    },
}

/// Output of the cheap pre-check before invoking the full parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterMode {
    /// No unquoted dot — take the raw-filter path without parsing.
    Raw,
    /// At least one unquoted dot found — attempt relational lowering.
    Relational,
}

/// Scan `text` for an unquoted `.` in a single pass.
///
/// String literals (single- and double-quoted) are skipped atomically so that
/// `email = 'a.b@x.com'` returns `Raw` while `user.email = 'x'` returns
/// `Relational`. Does not perform full parsing; this is intentionally cheap.
// The five bytes[i] reads below each sit in a loop or guard conditioned on
// i < bytes.len(); the bound is not re-checked per access.
#[expect(
    clippy::indexing_slicing,
    reason = "five bytes[i] reads: the outer match under while i < bytes.len(), the closing-quote check at the top of each inner while i < bytes.len() loop, and the doubled-quote check behind i < bytes.len() && in each quote arm"
)]
pub(crate) fn classify_filter_input(text: &str) -> FilterMode {
    if text.trim().is_empty() {
        return FilterMode::Raw;
    }

    let bytes = text.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'\'' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'"' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'.' => {
                return FilterMode::Relational;
            }
            _ => {
                i += 1;
            }
        }
    }

    FilterMode::Raw
}

/// Render the relational filter chip (T24 / FR-UPGRADE-1 to FR-UPGRADE-3).
///
/// Returns `None` when the state is not `Active` — the caller skips the child
/// in that case. When `Active`, returns a clickable pill element. The click
/// handler opens the query builder pre-seeded with the resolved spec.
pub(crate) fn render_relational_chip(
    state: &RelationalFilterState,
    cx: &App,
    on_click: FilterBarClickHandler,
) -> Option<AnyElement> {
    let RelationalFilterState::Active {
        join_count,
        predicate_count: _,
    } = state
    else {
        return None;
    };

    let theme = cx.theme().clone();
    let label = format!(
        "relational \u{00b7} {} join{}",
        join_count,
        if *join_count == 1 { "" } else { "s" }
    );

    let theme_hover = theme.clone();
    let chip = div()
        .id("relational-filter-chip")
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .rounded_full()
        .bg(theme.secondary)
        .cursor_pointer()
        .hover(move |d| d.bg(theme_hover.secondary.opacity(0.7)))
        .on_click(on_click)
        .child(
            Icon::new(AppIcon::Link2)
                .small()
                .color(theme.muted_foreground),
        )
        .child(Text::caption(label).color(theme.muted_foreground));

    Some(chip.into_any_element())
}

/// Render the Resolving state indicator (T27 / FR-GATE-4).
///
/// A subtle inline spinner shown while FK metadata is still loading. Returns
/// `None` for all states except `Resolving`.
pub(crate) fn render_resolving_indicator(
    state: &RelationalFilterState,
    cx: &App,
) -> Option<AnyElement> {
    if !matches!(state, RelationalFilterState::Resolving) {
        return None;
    }

    let theme = cx.theme().clone();

    let indicator = div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .h(Heights::ROW_COMPACT)
        .px(Spacing::XS)
        .child(
            Icon::new(AppIcon::Loader)
                .small()
                .color(theme.muted_foreground),
        );

    Some(indicator.into_any_element())
}

/// Render the inline error affordance (T26 / FR-ERR-1 to FR-ERR-2).
///
/// Returns `None` for non-Error states. When `Error`, returns a one-line div
/// with the error message and an "Open in builder" link.
///
/// `on_open_builder` is called when the user clicks "Open in builder".
pub(crate) fn render_relational_error(
    state: &RelationalFilterState,
    cx: &App,
    on_open_builder: FilterBarClickHandler,
) -> Option<AnyElement> {
    let RelationalFilterState::Error { message, .. } = state else {
        return None;
    };

    let theme = cx.theme().clone();
    let message = message.clone();

    let theme_hover = theme.clone();
    let theme_ring = theme.ring;
    let row = div()
        .flex()
        .items_center()
        .gap(Spacing::XS)
        .h(Heights::ROW_COMPACT)
        .px(Spacing::SM)
        .child(Icon::new(AppIcon::CircleAlert).small().color(theme.danger))
        .child(Text::caption(message).color(theme.danger))
        .child(
            div()
                .id("open-builder-from-error")
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .cursor_pointer()
                .rounded(Radii::SM)
                .px(Spacing::XS)
                .hover(move |d| d.bg(theme_hover.secondary))
                .on_click(on_open_builder)
                .child(
                    Text::caption(dbflux_i18n::t!("document.data.grid.filter.open_in_builder"))
                        .color(theme_ring),
                )
                .child(Icon::new(AppIcon::ExternalLink).small().color(theme_ring)),
        );

    Some(row.into_any_element())
}

/// Whether the filter input border should show an error ring (T26).
///
/// Used by the caller to apply a red border to the filter input container
/// when a resolve error is active.
pub(crate) fn filter_input_has_error(state: &RelationalFilterState) -> bool {
    matches!(state, RelationalFilterState::Error { .. })
}

impl DataGridPanel {
    // === Filter bar presentation helpers ===

    /// Filter verb and filter keyword ("SELECT * FROM" / "find" / "FROM") shown
    /// in the toolbar to the left of the source name and to the left of the filter
    /// input, respectively.
    ///
    /// Derived from the source's facets: a collection its driver declares as
    /// documents is read with `find`; every other source is read with SQL.
    pub(crate) fn filter_labels_for_source(
        source: &DataSource,
        app_state: &Entity<AppStateEntity>,
        cx: &App,
    ) -> (&'static str, &'static str) {
        if !source.is_table()
            && Self::source_facets(source, app_state, cx).contains(&ObjectFacet::Document)
        {
            return ("find", "WHERE");
        }

        ("SELECT * FROM", "WHERE")
    }

    /// Caption and label that name the source left of the filter input.
    ///
    /// A collection whose driver describes its browse query shows that query,
    /// in the connection's own language, with no caption. Every other source
    /// shows the category-derived verb and the qualified source name.
    pub(crate) fn source_query_labels(&self, cx: &App) -> (&'static str, String) {
        if let (DataSource::Collection { .. }, Some(label)) =
            (&self.source, &self.filter_bar.browse_query_label)
        {
            return ("", label.clone());
        }

        let (prefix, _) = Self::filter_labels_for_source(&self.source, &self.app_state, cx);

        let source_name = match &self.source {
            DataSource::Table { table, .. } => table.qualified_name(),
            DataSource::Collection { collection, .. } => collection.qualified_name(),
            DataSource::QueryResult { .. } => String::new(),
        };

        (prefix, source_name)
    }

    /// Filter input placeholder text, derived from the source's facets.
    ///
    /// Returns an empty string for a time-series collection because its
    /// `browse_collection` ignores the filter field (InfluxDB) — showing a
    /// misleading placeholder would lie to the user.
    pub(super) fn filter_placeholder_for_source(
        source: &DataSource,
        app_state: &Entity<AppStateEntity>,
        cx: &App,
    ) -> &'static str {
        const SQL_EXAMPLE: &str = "e.g. id > 10 AND name LIKE '%test%'";

        if source.is_table() {
            return SQL_EXAMPLE;
        }

        let facets = Self::source_facets(source, app_state, cx);
        if facets.contains(&ObjectFacet::Document) {
            r#"e.g. {"name": {"$regex": "test"}}"#
        } else if super::facets::time_series_facet(&facets).is_some() {
            ""
        } else {
            SQL_EXAMPLE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FilterMode, classify_filter_input};

    // T21: classify_filter_input

    #[test]
    fn classify_empty_input_is_raw() {
        assert_eq!(classify_filter_input(""), FilterMode::Raw);
        assert_eq!(classify_filter_input("   "), FilterMode::Raw);
    }

    #[test]
    fn classify_dot_only_inside_string_literal_is_raw() {
        assert_eq!(
            classify_filter_input("email = 'a.b@x.com'"),
            FilterMode::Raw
        );
        assert_eq!(
            classify_filter_input("email = \"a.b@x.com\""),
            FilterMode::Raw
        );
    }

    #[test]
    fn classify_unquoted_dot_is_relational() {
        assert_eq!(
            classify_filter_input("user.email = 'x'"),
            FilterMode::Relational
        );
        assert_eq!(
            classify_filter_input("created_by.organization.name = 'Acme'"),
            FilterMode::Relational
        );
    }

    #[test]
    fn classify_bare_column_filter_is_raw() {
        assert_eq!(classify_filter_input("status = 'active'"), FilterMode::Raw);
        assert_eq!(classify_filter_input("age > 30"), FilterMode::Raw);
    }

    #[test]
    fn classify_mixed_literal_and_dotted_path_is_relational() {
        // The literal dot is inside quotes, but the path dot is outside
        assert_eq!(
            classify_filter_input("email = 'a.b@x.com' AND user.role = 'admin'"),
            FilterMode::Relational
        );
    }

    #[test]
    fn classify_single_quote_escape_does_not_terminate_early() {
        // 'it''s fine' should be treated as a single string literal
        assert_eq!(classify_filter_input("col = 'it''s fine'"), FilterMode::Raw);
    }

    #[test]
    fn classify_double_quote_escape_does_not_terminate_early() {
        assert_eq!(classify_filter_input("col = \"a\"\"b\""), FilterMode::Raw);
    }
}
