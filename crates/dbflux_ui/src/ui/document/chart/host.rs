//! `ChartHost` trait and `HostAdapter` enum.
//!
//! `ChartHost` is the seam between `ChartShell` and any surface that can
//! mount a chart. Implementors provide the query text, connection identifier,
//! time-range panel entity, refresh-dropdown entity, and a re-execution
//! callback.
//!
//! `HostAdapter` sidesteps GPUI's lack of ergonomic `Entity<dyn Trait>`
//! support by wrapping concrete entity types as enum variants. Adding a new
//! host requires one new variant and a delegation arm — both local changes
//! to this file.

use crate::ui::common::time_range::view::TimeRangePanel;
use crate::ui::document::data_grid_panel::DataGridPanel;
use dbflux_components::controls::Dropdown;
use dbflux_core::QueryResult;
use gpui::prelude::*;
use gpui::{App, Entity, Window};
use std::sync::Arc;
use uuid::Uuid;

/// The behavioral seam between `ChartShell` and its surrounding host.
///
/// Implementors expose the query text, active connection, time-range panel,
/// refresh dropdown, and a re-execution path. The shell calls these to
/// drive toolbar rendering and to request re-runs. All methods receive
/// a `&App` read context so they can be called from within shell update
/// closures without requiring a borrow of the host entity at the call site.
pub trait ChartHost {
    /// The current query text. Returns `None` when the host has nothing
    /// executable (e.g. a collection-ref-only browse without a user query).
    fn current_query(&self, cx: &App) -> Option<String>;

    /// The connection profile ID associated with this host, if any.
    fn connection_id(&self, cx: &App) -> Option<Uuid>;

    /// The time-range panel owned by the host, if applicable.
    ///
    /// Returns `None` for relational sources that do not expose a time-range
    /// picker, or before the panel has been wired in by the parent document.
    fn time_range_panel(&self, cx: &App) -> Option<Entity<TimeRangePanel>>;

    /// The refresh-policy dropdown owned by the host.
    fn refresh_dropdown(&self, cx: &App) -> Entity<Dropdown>;

    /// The most recent query result held by the host, if any.
    fn current_result(&self, cx: &App) -> Option<Arc<QueryResult>>;

    /// Request a fresh query execution.
    ///
    /// The host is responsible for wiring this into whatever execution
    /// path it controls (e.g. emitting an event to the parent CodeDocument,
    /// calling a `DocumentTaskRunner`, or re-paging a table source).
    fn request_reexecute(&mut self, window: &mut Window, cx: &mut App);
}

/// Concrete adapter enum that implements `ChartHost` by delegating to an
/// inner entity.
///
/// GPUI does not currently make `Entity<dyn Trait>` ergonomic, so this enum
/// is the single place that knows about concrete host types. Adding a new
/// host is a local change: one variant + one `impl ChartHost for HostAdapter`
/// arm. The enum does NOT branch on driver IDs.
#[derive(Clone)]
pub enum HostAdapter {
    /// Chart hosted by a `DataGridPanel` (CodeDocument result tab).
    DataGrid(Entity<DataGridPanel>),
}

impl ChartHost for HostAdapter {
    fn current_query(&self, cx: &App) -> Option<String> {
        match self {
            HostAdapter::DataGrid(entity) => entity.read(cx).chart_host_current_query(cx),
        }
    }

    fn connection_id(&self, cx: &App) -> Option<Uuid> {
        match self {
            HostAdapter::DataGrid(entity) => entity.read(cx).chart_host_connection_id(cx),
        }
    }

    fn time_range_panel(&self, cx: &App) -> Option<Entity<TimeRangePanel>> {
        match self {
            HostAdapter::DataGrid(entity) => entity.read(cx).chart_host_time_range_panel(cx),
        }
    }

    fn refresh_dropdown(&self, cx: &App) -> Entity<Dropdown> {
        match self {
            HostAdapter::DataGrid(entity) => entity.read(cx).chart_host_refresh_dropdown(cx),
        }
    }

    fn current_result(&self, cx: &App) -> Option<Arc<QueryResult>> {
        match self {
            HostAdapter::DataGrid(entity) => entity.read(cx).chart_host_current_result(cx),
        }
    }

    fn request_reexecute(&mut self, _window: &mut Window, cx: &mut App) {
        match self {
            HostAdapter::DataGrid(entity) => {
                entity.update(cx, |panel, cx| {
                    panel.chart_host_request_reexecute(cx);
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time test: verify that `HostAdapter` is `Clone` and that the
    /// `ChartHost` trait object can be invoked via the enum. This is a
    /// structural check that the trait and adapter compile correctly.
    #[test]
    fn host_adapter_is_clone() {
        // HostAdapter must be Clone so ChartShell can hold it across updates.
        // Verified at the type level by requiring Clone in the trait bound.
        fn assert_clone<T: Clone>() {}
        assert_clone::<HostAdapter>();
    }
}
