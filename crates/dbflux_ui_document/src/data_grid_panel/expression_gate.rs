//! The confirmation a save needs when it writes SQL expressions.
//!
//! A cell holding an expression is written into the statement verbatim
//! instead of being bound, so the save is classified with
//! [`DangerousQueryKind::RawExpressionInSet`], as the visual builder's
//! mutations are, and waits for the user before it runs. A batch save pauses
//! here the way it does for [`super::PendingDeleteConfirm`].

use super::{DataGridPanel, DataSource};
use dbflux_components::components::data_table::model::CellValue;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::DangerousQueryKind;
use gpui::prelude::FluentBuilder;
use gpui::*;

/// The write a confirmation is holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HeldSave {
    /// A pending insert, by its index in the edit buffer.
    Insert(usize),
    /// The staged edits of a base row.
    Row(usize),
}

pub(super) struct PendingExpressionConfirm {
    pub(super) save: HeldSave,
    pub(super) batch_owned: bool,
    /// `column = expression`, one per expression the save writes.
    pub(super) expressions: Vec<String>,
    pub(super) kind: DangerousQueryKind,
}

impl DataGridPanel {
    /// Holds `save` for confirmation when it carries an expression. Returns
    /// `true` when the save was held, in which case the caller must not run
    /// it: [`Self::confirm_expressions`] runs it after the user agrees.
    pub(super) fn hold_save_for_expression_confirm(
        &mut self,
        save: HeldSave,
        batch_owned: bool,
        cx: &mut Context<Self>,
    ) -> bool {
        if std::mem::take(&mut self.expression_confirmed) {
            return false;
        }
        if !matches!(self.source, DataSource::Table { .. }) {
            return false;
        }

        let Some(table_state) = &self.grid_table.table_state else {
            return false;
        };

        let state = table_state.read(cx);
        let cells: Vec<(usize, CellValue)> = match save {
            HeldSave::Insert(insert_idx) => state
                .edit_buffer()
                .get_pending_insert_by_idx(insert_idx)
                .map(|cells| cells.iter().cloned().enumerate().collect())
                .unwrap_or_default(),
            HeldSave::Row(row_idx) => state
                .edit_buffer()
                .row_changes(row_idx)
                .into_iter()
                .map(|(col, cell)| (col, cell.clone()))
                .collect(),
        };

        let expressions: Vec<String> = cells
            .iter()
            .filter_map(|(col, cell)| {
                let expression = cell.as_expression()?;
                let name = state.model().columns.get(*col)?.title.to_string();
                Some(format!("{name} = {expression}"))
            })
            .collect();

        if expressions.is_empty() {
            return false;
        }

        self.pending_expression_confirm = Some(PendingExpressionConfirm {
            save,
            batch_owned,
            expressions,
            kind: DangerousQueryKind::RawExpressionInSet,
        });
        cx.notify();
        true
    }

    pub fn has_expression_confirm(&self) -> bool {
        self.pending_expression_confirm.is_some()
    }

    /// The user agreed: runs the held save.
    pub fn confirm_expressions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(held) = self.pending_expression_confirm.take() else {
            return;
        };

        self.expression_confirmed = true;
        match held.save {
            HeldSave::Insert(insert_idx) => {
                self.handle_commit_insert(insert_idx, held.batch_owned, cx)
            }
            HeldSave::Row(row_idx) => self.handle_save_row(row_idx, held.batch_owned, cx),
        }
        // The held save may have returned without reaching the gate.
        self.expression_confirmed = false;

        self.focus_active_view(window, cx);
        cx.notify();
    }

    /// The user declined: nothing is written, and a batch or a close that was
    /// waiting on this save stops with the edits still staged.
    pub fn cancel_expressions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(held) = self.pending_expression_confirm.take() else {
            return;
        };

        if held.batch_owned {
            self.abandon_close_apply(cx);
        }
        self.focus_active_view(window, cx);
        cx.notify();
    }

    pub(super) fn render_expression_confirm_modal(&self, cx: &mut Context<Self>) -> Modal {
        let (kind, expressions) = self
            .pending_expression_confirm
            .as_ref()
            .map(|held| (held.kind, held.expressions.clone()))
            .unwrap_or((DangerousQueryKind::RawExpressionInSet, Vec::new()));

        let footer = div()
            .flex()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "expression-cancel-btn",
                    dbflux_i18n::t!("document.data.context_menu.expression_confirm.cancel"),
                )
                .icon(AppIcon::X)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.cancel_expressions(window, cx);
                })),
            )
            .child(
                Button::new(
                    "expression-confirm-btn",
                    dbflux_i18n::t!("document.data.context_menu.expression_confirm.confirm"),
                )
                .danger()
                .icon(AppIcon::Code)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.confirm_expressions(window, cx);
                })),
            );

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(Text::body(crate::labels::dangerous_query_body(kind)))
            .child(Text::body(dbflux_i18n::t!(
                "document.data.context_menu.expression_confirm.list"
            )))
            .children(expressions.into_iter().map(Text::body));

        Modal::new(crate::labels::dangerous_query_title(kind))
            .id("expression-modal-overlay")
            .danger()
            .icon(AppIcon::TriangleAlert)
            .width(px(480.0))
            .body(body)
            .footer(footer)
    }
}
