//! The paradigm facets of the source a grid shows, as its connection declares
//! them. Views and gates read these instead of the connection's category.

use super::{AppStateEntity, DataGridPanel, DataSource};
use dbflux_core::{ObjectFacet, TimeSeriesFacet};
use gpui::{App, Entity};

impl DataGridPanel {
    /// Facets of `source`: a table's from its cached details (tabular until
    /// they load), a collection's as its driver declares them, tabular for a
    /// query result.
    pub(crate) fn source_facets(
        source: &DataSource,
        app_state: &Entity<AppStateEntity>,
        cx: &App,
    ) -> Vec<ObjectFacet> {
        let tabular = || vec![ObjectFacet::Tabular];

        match source {
            DataSource::Table {
                profile_id,
                database,
                table,
                ..
            } => {
                let state = app_state.read(cx);
                let Some(connected) = state.connections().get(profile_id) else {
                    return tabular();
                };
                let cache_key = (
                    Self::table_details_database(connected, database.as_deref()),
                    table.schema.clone(),
                    table.name.clone(),
                );

                connected
                    .table_details
                    .get(&cache_key)
                    .map(|details| connected.connection.object_facets(details))
                    .unwrap_or_else(tabular)
            }
            DataSource::Collection {
                profile_id,
                collection,
                ..
            } => app_state
                .read(cx)
                .connections()
                .get(profile_id)
                .map(|connected| connected.connection.collection_facets(collection))
                .unwrap_or_else(tabular),
            DataSource::QueryResult { .. } => tabular(),
        }
    }

    /// Facets of the source this grid shows.
    pub(crate) fn facets(&self, cx: &App) -> Vec<ObjectFacet> {
        Self::source_facets(&self.source, &self.app_state, cx)
    }

    /// Picks up facets that arrived after the panel opened (a table's come
    /// with its details): a time series opens its chart on the rows already
    /// shown.
    pub(super) fn sync_source_facets(&mut self, cx: &mut gpui::Context<Self>) {
        let facets = self.facets(cx);
        let time_series = time_series_facet(&facets).cloned();
        let was_time_series = self.chart.time_series_source;

        self.chart.time_series_source = time_series.is_some();
        self.chart.time_series_column = time_series.and_then(|facet| facet.time_column);

        if !was_time_series && self.chart.time_series_source && !self.result.rows.is_empty() {
            let result = self.result.clone();
            self.apply_chart_for_result(&result, cx);
        }
    }

    /// Whether the source this grid shows holds documents.
    pub(crate) fn has_document_facet(&self, cx: &App) -> bool {
        self.facets(cx).contains(&ObjectFacet::Document)
    }
}

/// The time axis among `facets`, when the source is a time series.
pub(crate) fn time_series_facet(facets: &[ObjectFacet]) -> Option<&TimeSeriesFacet> {
    facets.iter().find_map(|facet| match facet {
        ObjectFacet::TimeSeries(time_series) => Some(time_series),
        _ => None,
    })
}
