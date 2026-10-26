//! "Export all rows": pages through the whole read behind a grid (a table
//! or collection browse, or a builder SELECT, which a relational filter
//! lowers to) with the grid's filter, sort and column projection, and
//! streams it into one file.
//!
//! The rows go through the same read the grid ran, one page at a time, so
//! memory stays bounded by the page size. A query result is never offered:
//! it would have to be run again, and an arbitrary query may not be safe to
//! repeat.

mod document_shape;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dbflux_core::{CancelToken, Connection, TaskId, TaskKind, TaskTarget};
use dbflux_transfer::{
    AlignByName, AutoColumnMap, BrowseSource, FileFormat, FileSink, PageShape, ReplayableRead,
    RowSource, TableMappingMode, TransferError, TransferOutcome, TransferReport, run_transfer,
};
use dbflux_ui_base::app_state_entity::AppStateChanged;
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use dbflux_ui_base::{SaveTargetOutcome, SaveTargetRequest};
use gpui::*;

use super::documents::CollectionTab;
use super::{DataGridPanel, DataSource};
use crate::progress_forwarding::spawn_task_progress_forwarder;
use document_shape::DocumentPageShape;

/// Rows read per page. Large enough to keep the round trips few, small
/// enough that one page never weighs much in memory.
pub(super) const EXPORT_ALL_PAGE_SIZE: u32 = 1_000;

/// What the background run needs, resolved on the foreground.
struct ExportAllRun {
    connection: Arc<dyn Connection>,
    read: ReplayableRead,
    shape: Box<dyn PageShape>,
    estimated_total: Option<u64>,
    format: FileFormat,
    target_path: PathBuf,
    cancel_token: CancelToken,
}

/// The source the export reads from, resolved from the grid.
struct ExportAllSource {
    profile_id: uuid::Uuid,
    database: Option<String>,
    object_name: String,
    qualified_name: String,
    estimated_total: Option<u64>,
}

impl DataGridPanel {
    /// Whether the rows on screen came from a read the grid can run again
    /// page by page: a table or collection browse, or a builder SELECT
    /// (which an active relational filter also runs).
    pub(super) fn can_export_all_rows(&self) -> bool {
        // A nearest-neighbor search is a top-k read with no offset: the rows
        // loaded are already its whole result, so there is nothing to page.
        if self.nearest_neighbors.is_some() {
            return false;
        }

        match (&self.source, &self.last_replayable_read) {
            (
                DataSource::Table { .. },
                Some(ReplayableRead::Table(_) | ReplayableRead::VisualQuery { .. }),
            ) => true,
            // A nested value stepped into, or the Aggregate tab, shows rows
            // that are not documents of the collection browse.
            (DataSource::Collection { .. }, Some(ReplayableRead::Collection { .. })) => {
                self.collection.steps.is_empty() && self.collection.tab != CollectionTab::Aggregate
            }
            _ => false,
        }
    }

    /// Asks where to save, then streams every row of the read into that
    /// file as a task in the Tasks panel, which can cancel it.
    pub fn export_all_rows(
        &mut self,
        format: FileFormat,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.chrome.export_menu_open {
            self.close_export_menu(window, cx);
        }

        if !self.can_export_all_rows() {
            return;
        }
        let (Some(read), Some(source)) =
            (self.last_replayable_read.clone(), self.export_all_source())
        else {
            return;
        };

        let Some(connection) =
            self.export_all_connection(source.profile_id, source.database.as_deref(), cx)
        else {
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.data.grid.error.connection_not_found"),
                ),
                cx,
            );
            return;
        };

        let shape = self.export_all_shape(format, cx);
        // The table's count does not bound a builder query's rows.
        let estimated_total = match &read {
            ReplayableRead::VisualQuery { .. } => None,
            _ => source.estimated_total,
        };
        let suggested_name = format!("{}.{}", source.object_name, format.extension());
        let save_target_override = self.app_state.read(cx).save_target_override();
        let entity = cx.entity().downgrade();

        cx.spawn(async move |_this, cx| {
            let Some(target_path) =
                choose_target_path(save_target_override, &suggested_name, format, cx).await
            else {
                return;
            };

            let qualified_name = source.qualified_name;
            let target = TaskTarget {
                profile_id: source.profile_id,
                database: source.database,
            };
            let started = cx.update(|cx| {
                entity.update(cx, |panel, cx| {
                    panel.start_export_all_task(&qualified_name, target, cx, |cancel_token| {
                        ExportAllRun {
                            connection,
                            read,
                            shape,
                            estimated_total,
                            format,
                            target_path,
                            cancel_token,
                        }
                    })
                })
            });

            if started.is_err() {
                log::info!("The grid closed before the export of {qualified_name} started");
            }
        })
        .detach();
    }

    fn export_all_source(&self) -> Option<ExportAllSource> {
        match &self.source {
            DataSource::Table {
                profile_id,
                database,
                table,
                total_rows,
                ..
            } => Some(ExportAllSource {
                profile_id: *profile_id,
                database: database.clone(),
                object_name: table.name.clone(),
                qualified_name: table.qualified_name(),
                estimated_total: *total_rows,
            }),
            DataSource::Collection {
                profile_id,
                collection,
                total_docs,
                ..
            } => Some(ExportAllSource {
                profile_id: *profile_id,
                database: Some(collection.database.clone()),
                object_name: collection.name.clone(),
                qualified_name: collection.qualified_name(),
                estimated_total: *total_docs,
            }),
            DataSource::QueryResult { .. } => None,
        }
    }

    /// A document collection's pages are flattened as the grid flattens its
    /// page, so a CSV file has the loaded-rows export's columns. JSON has no
    /// header to fix, so each document is written whole.
    fn export_all_shape(&self, format: FileFormat, cx: &App) -> Box<dyn PageShape> {
        if self.source.is_collection() && self.is_document_collection(cx) {
            let shape =
                DocumentPageShape::new(self.collection.expanded.clone(), self.identity_columns());
            match format {
                FileFormat::Json => Box::new(shape.whole_documents()),
                FileFormat::Csv => Box::new(shape),
            }
        } else {
            Box::new(AlignByName::default())
        }
    }

    fn export_all_connection(
        &self,
        profile_id: uuid::Uuid,
        database: Option<&str>,
        cx: &App,
    ) -> Option<Arc<dyn Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)?
            .resolve_connection_for_execution(database)
            .ok()
    }

    /// Registers the run in the Tasks panel and starts it in the background.
    fn start_export_all_task(
        &self,
        table_name: &str,
        target: TaskTarget,
        cx: &mut Context<Self>,
        build_run: impl FnOnce(CancelToken) -> ExportAllRun,
    ) {
        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Export,
                crate::labels::export_all_rows_task_label(table_name),
                Some(target),
            );
            cx.emit(AppStateChanged);
            pair
        });

        self.spawn_export_all_run(task_id, build_run(cancel_token), cx);
    }

    fn spawn_export_all_run(&self, task_id: TaskId, run: ExportAllRun, cx: &mut Context<Self>) {
        let progress = Arc::new(Mutex::new((0_u64, run.estimated_total)));
        spawn_task_progress_forwarder(
            self.app_state.clone(),
            task_id,
            {
                let progress = Arc::clone(&progress);
                move || {
                    *progress
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                }
            },
            cx,
        );

        let app_state = self.app_state.clone();
        let target_path = run.target_path.clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn(async move { stream_all_rows(run, &progress) })
                .await;

            cx.update(|cx| {
                finish_export_all(&app_state, task_id, &target_path, result, cx);
            });
        })
        .detach();
    }
}

/// Resolves where the file goes: the native save dialog, or the fallback
/// directory when there is none. `None` when the user cancelled or no target
/// could be found, which has already been reported.
async fn choose_target_path(
    save_target_override: Option<dbflux_ui_base::SaveTargetProvider>,
    suggested_name: &str,
    format: FileFormat,
    cx: &mut AsyncApp,
) -> Option<PathBuf> {
    let outcome = dbflux_ui_base::file_dialog::resolve_save_target(
        save_target_override,
        SaveTargetRequest {
            suggested_name,
            language_name: format.label(),
            default_extension: format.extension(),
        },
        async {
            rfd::AsyncFileDialog::new()
                .set_title(crate::labels::context_menu_export_dialog_title(
                    format.label(),
                ))
                .set_file_name(suggested_name)
                .add_filter(format.label(), &[format.extension()])
                .save_file()
                .await
                .map(|handle| handle.path().to_path_buf())
        },
    )
    .await;

    match outcome {
        SaveTargetOutcome::Selected { path, .. } => Some(path),
        SaveTargetOutcome::Cancelled => None,
        SaveTargetOutcome::Failed(error) => {
            report_error_async(
                UserFacingError::new(
                    ErrorKind::Storage,
                    crate::labels::context_menu_export_dialog_fallback_failed_error(&error),
                ),
                cx,
            );
            None
        }
    }
}

/// Reads every page of the read and writes it to the target file, keeping
/// `progress` at `(rows written, total)` for the Tasks panel.
fn stream_all_rows(
    run: ExportAllRun,
    progress: &Mutex<(u64, Option<u64>)>,
) -> Result<TransferReport, TransferError> {
    let ExportAllRun {
        connection,
        read,
        shape,
        estimated_total,
        format,
        target_path,
        cancel_token,
    } = run;

    let mut source = BrowseSource::open_with_shape(
        connection,
        read,
        EXPORT_ALL_PAGE_SIZE,
        estimated_total,
        shape,
    )?;
    let columns = source.columns().to_vec();
    // The file mirrors what the grid reads, so no column is remapped.
    let column_map = AutoColumnMap::new(&columns, &columns);
    let mut sink = FileSink::to_file(target_path, format);
    if source.writes_whole_documents() {
        sink = sink.writing_whole_documents();
    }

    let mut report = run_transfer(
        &mut source,
        &column_map,
        &mut sink,
        TableMappingMode::Existing,
        &cancel_token,
        &mut |rows_done, total| {
            if let Ok(mut guard) = progress.lock() {
                *guard = (rows_done, total);
            }
        },
    )?;

    let dropped = source.dropped_columns();
    if !dropped.is_empty() {
        report
            .warnings
            .push(crate::labels::export_all_rows_dropped_columns_warning(
                &dropped.join(", "),
            ));
    }

    Ok(report)
}

/// Moves the task to its terminal state and tells the user how it ended.
fn finish_export_all(
    app_state: &Entity<dbflux_ui_base::AppStateEntity>,
    task_id: TaskId,
    target_path: &std::path::Path,
    result: Result<TransferReport, TransferError>,
    cx: &mut App,
) {
    let path_text = target_path.display().to_string();

    match result {
        Ok(report) if report.outcome == TransferOutcome::Cancelled => {
            app_state.update(cx, |state, cx| {
                state.tasks_mut().cancel(task_id);
                cx.emit(AppStateChanged);
            });
            Toast::warning(crate::labels::export_all_rows_cancelled_toast(&path_text)).push(cx);
        }
        Ok(report) => {
            app_state.update(cx, |state, cx| {
                state.complete_task(task_id);
                cx.emit(AppStateChanged);
            });
            Toast::success(crate::labels::export_all_rows_exported_toast(
                report.rows_transferred,
                &path_text,
            ))
            .push(cx);
            for warning in report.warnings {
                Toast::warning(warning).push(cx);
            }
        }
        Err(error) => {
            let kind = match error {
                TransferError::Source(_) => ErrorKind::Driver,
                TransferError::Sink(_) => ErrorKind::Storage,
            };
            let message = error.to_string();
            app_state.update(cx, |state, cx| {
                state.fail_task(task_id, message.clone());
                cx.emit(AppStateChanged);
            });
            report_error(
                UserFacingError::new(kind, crate::labels::export_all_rows_failed_error(&message)),
                cx,
            );
        }
    }
}

#[cfg(test)]
mod tests;
