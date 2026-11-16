//! The "Set value" submenu of the cell menu: the column default, the
//! expressions the dialect offers for the column's type, NULL, and an
//! expression typed in the inline editor.
//!
//! Everything here reads generic seams (`Connection::metadata`,
//! `Connection::dialect`, the cached column metadata); no driver id is
//! consulted.

use super::{ContextMenuItem, DataGridPanel, DataSource, TableContextMenu};
use dbflux_components::components::data_table::ContextMenuAction;
use dbflux_components::components::data_table::model::CellValue;
use dbflux_components::components::data_table::selection::CellCoord;
use dbflux_components::icons::AppIcon;
use gpui::{App, Context, SharedString, Window};

/// One selectable row of the "Set value" flyout.
pub(in crate::data_grid_panel) struct SetValueEntry {
    pub(in crate::data_grid_panel) label: SharedString,
    /// The expression a row stages, or the reason a row is disabled.
    pub(in crate::data_grid_panel) detail: Option<SharedString>,
    pub(in crate::data_grid_panel) action: ContextMenuAction,
    pub(in crate::data_grid_panel) disabled: bool,
    /// A section header drawn above this row.
    pub(in crate::data_grid_panel) header: Option<SharedString>,
    pub(in crate::data_grid_panel) is_expression: bool,
}

impl DataGridPanel {
    /// Whether the cell menu offers "Set value": a table of a connection
    /// that speaks SQL, where an expression has a meaning.
    pub(in crate::data_grid_panel) fn offers_set_value_menu(&self, cx: &App) -> bool {
        let DataSource::Table { profile_id, .. } = &self.source else {
            return false;
        };

        self.app_state
            .read(cx)
            .connections()
            .get(profile_id)
            .is_some_and(|connected| {
                connected.connection.metadata().query_language == dbflux_core::QueryLanguage::Sql
            })
    }

    /// Inserts the "Set value" trigger after "Edit in modal", when the menu
    /// has one and the connection takes expressions.
    pub(in crate::data_grid_panel) fn with_set_value_menu(
        &self,
        mut items: Vec<ContextMenuItem>,
        menu: &TableContextMenu,
        cx: &App,
    ) -> Vec<ContextMenuItem> {
        if menu.is_document_view || menu.is_column_header || !self.offers_set_value_menu(cx) {
            return items;
        }

        let Some(position) = items
            .iter()
            .position(|item| item.action == Some(ContextMenuAction::EditInModal))
        else {
            return items;
        };

        items.insert(
            position + 1,
            ContextMenuItem {
                label: dbflux_i18n::t!("document.data.context_menu.item.set_value").into(),
                action: Some(ContextMenuAction::SetValueMenu),
                icon: Some(AppIcon::Code),
                is_separator: false,
                is_danger: false,
                detail: None,
                disabled: false,
            },
        );
        items
    }

    /// The expressions the dialect offers for column `col`'s declared type.
    pub(in crate::data_grid_panel) fn type_value_expressions(
        &self,
        col: usize,
        cx: &App,
    ) -> Vec<String> {
        let DataSource::Table { profile_id, .. } = &self.source else {
            return Vec::new();
        };
        let Some(column) = self.result.columns.get(col) else {
            return Vec::new();
        };
        let Some(connected) = self.app_state.read(cx).connections().get(profile_id) else {
            return Vec::new();
        };

        if connected.connection.metadata().query_language != dbflux_core::QueryLanguage::Sql {
            return Vec::new();
        }

        connected
            .connection
            .dialect()
            .type_value_expressions(&column.type_name)
            .into_iter()
            .map(str::to_owned)
            .collect()
    }

    /// What the table asks when an editor opens in expression mode: the
    /// dialect's values for the column's declared type. `None` for a source
    /// that takes no expressions.
    pub(in crate::data_grid_panel) fn expression_suggestion_source(
        &self,
    ) -> Option<dbflux_components::components::data_table::SuggestionSource> {
        let DataSource::Table { profile_id, .. } = &self.source else {
            return None;
        };

        let profile_id = *profile_id;
        let app_state = self.app_state.clone();
        let type_names: Vec<String> = self
            .result
            .columns
            .iter()
            .map(|column| column.type_name.clone())
            .collect();

        Some(std::rc::Rc::new(move |col: usize, cx: &App| {
            let Some(type_name) = type_names.get(col) else {
                return Vec::new();
            };
            let state = app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                return Vec::new();
            };
            if connected.connection.metadata().query_language != dbflux_core::QueryLanguage::Sql {
                return Vec::new();
            }

            connected
                .connection
                .dialect()
                .type_value_expressions(type_name)
                .into_iter()
                .map(str::to_owned)
                .collect()
        }))
    }

    /// The rows of the flyout, in the order they are drawn and navigated.
    pub(in crate::data_grid_panel) fn set_value_entries(
        &self,
        col: usize,
        cx: &App,
    ) -> Vec<SetValueEntry> {
        let column_name = self
            .result
            .columns
            .get(col)
            .map(|column| column.name.clone())
            .unwrap_or_default();
        let type_name = self
            .result
            .columns
            .get(col)
            .map(|column| column.type_name.clone())
            .unwrap_or_default();
        let default = self.get_column_default(col, cx);

        let mut entries = vec![SetValueEntry {
            label: dbflux_i18n::t!("document.data.context_menu.item.set_value_column_default")
                .into(),
            detail: Some(match &default {
                Some(expression) => expression.clone().into(),
                None => dbflux_i18n::t!(
                    "document.data.context_menu.item.set_value_no_default",
                    column = column_name
                )
                .into(),
            }),
            action: ContextMenuAction::SetDefault,
            disabled: default.is_none(),
            header: None,
            is_expression: default.is_some(),
        }];

        for (index, expression) in self.type_value_expressions(col, cx).into_iter().enumerate() {
            let header = (index == 0).then(|| {
                SharedString::from(dbflux_i18n::t!(
                    "document.data.context_menu.item.set_value_type_values",
                    type = type_name
                ))
            });

            entries.push(SetValueEntry {
                label: expression.into(),
                detail: None,
                action: ContextMenuAction::SetTypeValue(index),
                disabled: false,
                header,
                is_expression: true,
            });
        }

        entries.push(SetValueEntry {
            label: dbflux_i18n::t!("document.data.context_menu.item.set_value_null").into(),
            detail: None,
            action: ContextMenuAction::SetNull,
            disabled: false,
            header: None,
            is_expression: false,
        });
        entries.push(SetValueEntry {
            label: dbflux_i18n::t!("document.data.context_menu.item.set_value_expression").into(),
            detail: None,
            action: ContextMenuAction::SetExpression,
            disabled: false,
            header: None,
            is_expression: false,
        });

        entries
    }

    /// Stages the `index`th expression the dialect offers for the column.
    pub(in crate::data_grid_panel) fn handle_set_type_value(
        &mut self,
        row: usize,
        col: usize,
        index: usize,
        cx: &mut Context<Self>,
    ) {
        let Some(expression) = self.type_value_expressions(col, cx).into_iter().nth(index) else {
            return;
        };
        let Some(table_state) = &self.grid_table.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            state.stage_cell_value(row, col, CellValue::expression(&expression));
            cx.notify();
        });
    }

    /// Opens the inline editor on the cell in expression mode.
    pub(in crate::data_grid_panel) fn handle_set_expression(
        &mut self,
        row: usize,
        col: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(table_state) = &self.grid_table.table_state else {
            return;
        };

        table_state.update(cx, |state, cx| {
            let coord = CellCoord::new(row, col);
            state.select_cell(coord, cx);
            state.start_editing_as_expression(coord, window, cx);
        });
    }
}
