//! "Explain this query" in the cell menu of a query result.
//!
//! The grid does not know how a plan is made or shown. The document that
//! hosts the result hands it an [`ExplainHost`]; choosing the row passes the
//! query that produced the result to that host. The row is offered only when
//! the host can show a plan and the connection's driver declares query plans,
//! read from its capabilities, so no driver is named here.

use super::{DataGridPanel, DataSource};
use gpui::{App, Context, Window};
use std::rc::Rc;

/// Id of the menu row; also its keyboard-activation id.
pub(super) const EXPLAIN_THIS_QUERY_ID: &str = "explain-this-query";

/// What the hosting document runs when "Explain this query" is chosen,
/// given the query that produced the result.
pub(crate) type ExplainHost = Rc<dyn Fn(String, &mut App)>;

impl DataGridPanel {
    /// Lets the hosting document explain this grid's query, or withdraws it.
    pub(crate) fn set_explain_host(&mut self, host: Option<ExplainHost>) {
        self.explain_host = host;
    }

    /// Whether the cell menu offers "Explain this query".
    pub(super) fn can_explain_from_context_menu(&self, cx: &App) -> bool {
        if self.explain_host.is_none() {
            return false;
        }

        let DataSource::QueryResult {
            original_query,
            profile_id,
            ..
        } = &self.source
        else {
            return false;
        };

        if original_query.trim().is_empty() {
            return false;
        }

        let declares_no_plans = profile_id.is_some_and(|profile_id| {
            self.app_state
                .read(cx)
                .connections()
                .get(&profile_id)
                .is_some_and(crate::query_plan::declares_no_plans)
        });

        !declares_no_plans
    }

    /// Closes the menu and hands the result's query to the host.
    pub(super) fn explain_this_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(menu) = self.context_menu.take() else {
            return;
        };

        if let (Some(host), Some(query)) =
            (self.explain_host.clone(), self.chart_host_current_query(cx))
        {
            // The host opens a tab of the document that owns this grid; run
            // it once this update has ended so it may touch the grid.
            cx.defer(move |cx| host(query, cx));
        }

        self.restore_focus_after_context_menu(menu.is_document_view, window, cx);
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn open_context_menu_on_selection_for_test(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_context_menu_at_selection(window, cx);
    }

    #[cfg(test)]
    pub(crate) fn activate_menu_row_for_test(
        &mut self,
        id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dispatch_menu_activation(id, window, cx);
    }
}
