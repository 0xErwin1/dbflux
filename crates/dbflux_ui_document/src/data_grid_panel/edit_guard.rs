//! The check every write by primary key makes before it is sent.
//!
//! A row is saved or deleted by the key the grid learned when it opened the
//! table. If the table's key changed since (or the table is gone), the
//! identity captured for each staged row no longer names that row, and the
//! statement would hit the wrong rows or none. So before a save, a delete, a
//! Save All or a chunked mutation, the table's details are read again, off the
//! UI thread, once for the whole batch, and compared with the key in use.
//!
//! A matching key refreshes the cache and lets the write go on. Anything else
//! sends nothing and opens the drift modal in its edit mode, which offers
//! "Reload table" (the staged edits are discarded) and "Cancel" (they stay
//! staged and unsaved).

use super::*;
use dbflux_components::modals::{ModalSchemaDrift, SchemaDriftDismissed, SchemaDriftRefresh};
use dbflux_core::{
    DbError, FetchTableDetailsResult, PrimaryKeyCheck, QueryTableRef, check_primary_key,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

/// What to release when the guard stops a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OnStop {
    /// Nothing else waits on the write.
    Nothing,
    /// A batch (Save All, bulk delete, a close applying edits) is parked on
    /// this write and must not wait for it.
    AbandonBatch,
    /// A close is waiting on a visual mutation that will not run.
    ReleaseClose,
}

type AfterGuard = Box<dyn FnOnce(&mut DataGridPanel, &mut Context<DataGridPanel>)>;

struct BlockedEdit {
    /// The fresh details to write on reload; `None` when the table is gone.
    fetched: Option<Box<FetchTableDetailsResult>>,
}

/// A request that arrived while another request's check was running.
struct QueuedRequest {
    on_stop: OnStop,
    then: AfterGuard,
}

pub(super) struct EditGuard {
    pub(super) modal: Entity<ModalSchemaDrift>,
    check_in_flight: bool,
    /// Run one at a time after the check in flight, each through its own check.
    queued: Vec<QueuedRequest>,
    blocked: Option<BlockedEdit>,
}

impl EditGuard {
    pub(super) fn new(window: &mut Window, cx: &mut Context<DataGridPanel>) -> Self {
        let modal = cx.new(ModalSchemaDrift::new);

        cx.subscribe_in(
            &modal,
            window,
            |this, _, _: &SchemaDriftRefresh, window, cx| {
                this.reload_blocked_table(window, cx);
            },
        )
        .detach();

        cx.subscribe_in(
            &modal,
            window,
            |this, _, _: &SchemaDriftDismissed, window, cx| {
                this.edit_guard.blocked = None;
                this.focus_active_view(window, cx);
            },
        )
        .detach();

        Self {
            modal,
            check_in_flight: false,
            queued: Vec::new(),
            blocked: None,
        }
    }
}

impl DataGridPanel {
    /// The primary key the grid writes by. A builder result's binding names
    /// the same real source columns: `compute_editable_binding` refuses an
    /// aliased key and `apply_pk_details` copies the binding's key here.
    fn key_in_use(&self) -> Vec<String> {
        self.pk_columns.clone()
    }

    /// The key of each of `rows`, for a batch that must find them again after
    /// a reload. Empty when the source is not a table, whose rows are addressed
    /// by position.
    pub(super) fn dirty_row_keys(
        &self,
        rows: &[usize],
        cx: &App,
    ) -> Vec<Option<Vec<dbflux_core::Value>>> {
        if !matches!(self.source, DataSource::Table { .. }) {
            return Vec::new();
        }
        let Some(table_state) = self.grid_table.table_state.as_ref() else {
            return Vec::new();
        };
        let state = table_state.read(cx);
        let model = state.model();

        rows.iter()
            .map(|&row| {
                state
                    .pk_columns()
                    .iter()
                    .map(|&col| model.cell(row, col).map(|cell| cell.to_value()))
                    .collect::<Option<Vec<_>>>()
                    .filter(|key| !key.is_empty())
            })
            .collect()
    }

    /// The row that holds `key` in the grid now, if any. Read after a check,
    /// when a reload may have moved the rows since the request was made.
    pub(super) fn row_index_of_key(&self, key: &[dbflux_core::Value], cx: &App) -> Option<usize> {
        let state = self.grid_table.table_state.as_ref()?.read(cx);
        let pk_indices = state.pk_columns();
        let model = state.model();
        let first = *pk_indices.first()?;

        (0..)
            .map_while(|row| model.cell(row, first).map(|_| row))
            .find(|&row| {
                pk_indices.len() == key.len()
                    && pk_indices.iter().zip(key).all(|(&col, value)| {
                        model.cell(row, col).map(|cell| cell.to_value()).as_ref() == Some(value)
                    })
            })
    }

    /// Runs `then` once the table's key is confirmed unchanged. Sources other
    /// than a table have no key to confirm and run `then` at once, as does a
    /// connection the driver cannot read table details from.
    pub(super) fn guard_edit(
        &mut self,
        on_stop: OnStop,
        then: impl FnOnce(&mut DataGridPanel, &mut Context<DataGridPanel>) + 'static,
        cx: &mut Context<Self>,
    ) {
        let DataSource::Table {
            profile_id,
            database,
            table,
            ..
        } = &self.source
        else {
            then(self, cx);
            return;
        };
        let (profile_id, source_database, table) = (*profile_id, database.clone(), table.clone());

        if self.edit_guard.check_in_flight {
            self.edit_guard.queued.push(QueuedRequest {
                on_stop,
                then: Box::new(then),
            });
            return;
        }

        let database = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                then(self, cx);
                return;
            };
            Self::table_details_database(connected, source_database.as_deref())
        };

        let params = match self.app_state.read(cx).prepare_refetch_table_details(
            profile_id,
            &database,
            table.schema.as_deref(),
            &table.name,
        ) {
            Ok(params) => params,
            Err(error) => {
                self.report_guard_failure(&error.to_string(), on_stop, cx);
                return;
            }
        };

        self.edit_guard.check_in_flight = true;
        let grid_key = self.key_in_use();
        let entity = cx.entity().clone();
        let then: AfterGuard = Box::new(then);

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { params.execute() })
                .await;

            cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    panel.finish_edit_guard(result, &grid_key, on_stop, then, cx);
                });
            });
        })
        .detach();
    }

    fn finish_edit_guard(
        &mut self,
        result: Result<FetchTableDetailsResult, DbError>,
        grid_key: &[String],
        on_stop: OnStop,
        then: AfterGuard,
        cx: &mut Context<Self>,
    ) {
        self.edit_guard.check_in_flight = false;

        let proceeded = match result {
            Ok(fetched) => match check_primary_key(grid_key, &fetched.details) {
                PrimaryKeyCheck::Same => {
                    self.write_fresh_details(fetched, cx);
                    if self.key_in_use() == grid_key {
                        then(self, cx);
                    } else {
                        // The grid took another key while the check ran, so
                        // the key it was checked with is not the one it writes by.
                        self.guard_edit(on_stop, then, cx);
                    }
                    true
                }
                check => {
                    self.block_edit(check, Some(Box::new(fetched)), on_stop, cx);
                    false
                }
            },
            Err(DbError::ObjectNotFound(_)) => {
                self.block_edit(PrimaryKeyCheck::TableGone, None, on_stop, cx);
                false
            }
            // A driver that cannot read table details has no key to compare.
            Err(DbError::NotSupported(_)) => {
                then(self, cx);
                true
            }
            Err(error) => {
                self.report_guard_failure(&error.to_string(), on_stop, cx);
                false
            }
        };

        self.drain_queued_requests(proceeded, cx);
    }

    /// Requests that arrived during the check each get their own check when
    /// it passed, and are released unsent when it did not: the answer on screen
    /// (the drift modal or the error) already tells the user why.
    fn drain_queued_requests(&mut self, rerun: bool, cx: &mut Context<Self>) {
        let queued = std::mem::take(&mut self.edit_guard.queued);
        for request in queued {
            if rerun {
                self.guard_edit(request.on_stop, request.then, cx);
            } else {
                self.release_on_stop(request.on_stop, cx);
            }
        }
    }

    fn write_fresh_details(&mut self, fetched: FetchTableDetailsResult, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, _| {
            state.set_table_details(
                fetched.profile_id,
                fetched.database.clone(),
                fetched.schema.clone(),
                fetched.table.clone(),
                fetched.details,
            );
            state.set_dependents(
                fetched.profile_id,
                fetched.database,
                fetched.schema,
                fetched.table,
                fetched.dependents,
            );
        });
    }

    fn report_guard_failure(&mut self, error: &str, on_stop: OnStop, cx: &mut Context<Self>) {
        self.release_on_stop(on_stop, cx);
        report_error(
            UserFacingError::new(
                ErrorKind::Driver,
                crate::labels::edit_guard_check_failed_error(error),
            ),
            cx,
        );
    }

    fn release_on_stop(&mut self, on_stop: OnStop, cx: &mut Context<Self>) {
        match on_stop {
            OnStop::Nothing => {}
            OnStop::AbandonBatch => self.abandon_close_apply(cx),
            OnStop::ReleaseClose => cx.emit(DataGridEvent::MutationFinished { landed: false }),
        }
    }

    fn block_edit(
        &mut self,
        check: PrimaryKeyCheck,
        fetched: Option<Box<FetchTableDetailsResult>>,
        on_stop: OnStop,
        cx: &mut Context<Self>,
    ) {
        self.release_on_stop(on_stop, cx);

        let DataSource::Table {
            database, table, ..
        } = &self.source
        else {
            return;
        };
        let table_ref = QueryTableRef {
            database: database.clone(),
            schema: table.schema.clone(),
            table: table.name.clone(),
        };

        self.edit_guard.blocked = Some(BlockedEdit { fetched });
        self.edit_guard
            .modal
            .update(cx, |modal, cx| modal.open_for_edit(table_ref, check, cx));
        cx.notify();
    }

    /// "Reload table": the fresh details replace the cached ones, the grid
    /// takes the table's key as it is now, and the rows are read again. The
    /// staged edits are discarded: their identities were taken under the old
    /// key.
    fn reload_blocked_table(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(blocked) = self.edit_guard.blocked.take() else {
            return;
        };
        self.edit_guard
            .modal
            .update(cx, |modal, cx| modal.close(cx));

        if let Some(table_state) = self.grid_table.table_state.clone() {
            table_state.update(cx, |state, cx| state.revert_all(cx));
        }

        match blocked.fetched {
            Some(fetched) => {
                let columns = fetched.details.columns.clone().unwrap_or_default();
                let pk_names = Self::primary_key_names(&columns);
                self.write_fresh_details(*fetched, cx);
                self.drop_sorts_on_missing_columns(&columns);
                if pk_names.is_empty() {
                    self.pk_columns.clear();
                }
                self.apply_pk_details(pk_names, cx);
            }
            None => self.forget_table_details(cx),
        }

        if self.pending.requery.is_none() {
            self.pending.refresh = true;
        }

        self.focus_active_view(window, cx);
    }

    fn forget_table_details(&mut self, cx: &mut Context<Self>) {
        let DataSource::Table {
            profile_id,
            database,
            table,
            ..
        } = &self.source
        else {
            return;
        };
        let (profile_id, source_database, table) = (*profile_id, database.clone(), table.clone());

        let database = {
            let state = self.app_state.read(cx);
            let Some(connected) = state.connections().get(&profile_id) else {
                return;
            };
            Self::table_details_database(connected, source_database.as_deref())
        };

        self.app_state.update(cx, |state, _| {
            state.invalidate_table_details(
                profile_id,
                &database,
                table.schema.as_deref(),
                &table.name,
            );
        });
    }

    /// A sort on a column the table no longer has would fail the reload.
    fn drop_sorts_on_missing_columns(&mut self, columns: &[dbflux_core::ColumnInfo]) {
        if let DataSource::Table { order_by, .. } = &mut self.source {
            order_by.retain(|sort| columns.iter().any(|column| column.name == sort.column.name));
        }
    }

    pub fn has_edit_guard_modal(&self, cx: &App) -> bool {
        self.edit_guard.modal.read(cx).is_visible()
    }

    /// Escape cancels and Enter reloads while the guard's modal is open.
    pub(super) fn dispatch_edit_guard_command(
        &mut self,
        cmd: dbflux_app::keymap::Command,
        cx: &mut Context<Self>,
    ) -> bool {
        use dbflux_app::keymap::Command;

        if !self.has_edit_guard_modal(cx) {
            return false;
        }

        match cmd {
            Command::Cancel => self
                .edit_guard
                .modal
                .update(cx, |modal, cx| modal.dismiss(cx)),
            Command::Execute => self
                .edit_guard
                .modal
                .update(cx, |modal, cx| modal.refresh(cx)),
            _ => {}
        }
        true
    }
}
