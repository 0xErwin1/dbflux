//! T23 — Import wizard: pick a folder bundle written by Export, review/adjust
//! each table's target-table handling and column mapping (T22), confirm any
//! destructive plan, then run the import via `dbflux_transfer::import::run_import`.
//!
//! Targets the connection the wizard was opened from (T24 wires
//! `SidebarEvent::RequestImportWizard` from the sidebar's connected-profile
//! Import action) — this slice does not offer a separate target-connection
//! picker for Import (that is Migration's job in a later slice); the wizard
//! always imports into the profile/database it was invoked against, which is
//! by definition already connected.

mod column_mapping;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dbflux_components::controls::{Button, Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{Connection, DriverCapabilities, TaskId, TaskKind, TaskStatus, TaskTarget};
use dbflux_transfer::import::{ImportOptions, ImportOutcome, ImportTablePlan, run_import};
use dbflux_transfer::manifest::read_manifest;
use dbflux_ui_base::app_state_entity::{AppStateChanged, AppStateEntity};
use dbflux_ui_base::modal_frame::ModalFrame;
use dbflux_ui_base::toast::Toast;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;

pub use column_mapping::TableImportConfig;
use column_mapping::mapping_mode_options;

#[derive(Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    PickFolder,
    Configure,
    Confirm,
    Running,
    Done,
}

/// One table row's live controls, wrapping the pure [`TableImportConfig`]
/// with the `Dropdown` entities the user adjusts it through. Item lists are
/// static (source/target column names don't change once the folder is
/// picked); only selections and `config` mutate.
struct TableImportRow {
    config: TableImportConfig,
    mapping_mode_dropdown: Entity<Dropdown>,
    rebind_target_dropdown: Entity<Dropdown>,
    rebind_source_dropdown: Entity<Dropdown>,
}

pub struct ImportWizard {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    visible: bool,
    profile_id: Option<Uuid>,
    database: Option<String>,
    step: WizardStep,
    manifest_dir: Option<PathBuf>,
    manifest_error: Option<String>,
    rows: Vec<TableImportRow>,
    supports_truncate: bool,
    loading: bool,
    running: bool,
    progress: Arc<Mutex<(u64, Option<u64>)>>,
    active_task_id: Option<TaskId>,
    result_summary: Option<String>,
    result_warnings: Vec<String>,
    _row_subscriptions: Vec<Subscription>,
}

impl ImportWizard {
    pub fn new(app_state: Entity<AppStateEntity>, cx: &mut Context<Self>) -> Self {
        Self {
            app_state,
            focus_handle: cx.focus_handle(),
            visible: false,
            profile_id: None,
            database: None,
            step: WizardStep::PickFolder,
            manifest_dir: None,
            manifest_error: None,
            rows: Vec::new(),
            supports_truncate: false,
            loading: false,
            running: false,
            progress: Arc::new(Mutex::new((0, None))),
            active_task_id: None,
            result_summary: None,
            result_warnings: Vec::new(),
            _row_subscriptions: Vec::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.visible = true;
        self.profile_id = Some(profile_id);
        self.database = database;
        self.step = WizardStep::PickFolder;
        self.manifest_dir = None;
        self.manifest_error = None;
        self.rows.clear();
        self._row_subscriptions.clear();
        self.loading = false;
        self.running = false;
        self.active_task_id = None;
        self.result_summary = None;
        self.result_warnings.clear();
        self.focus_handle.focus(window);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        cx.notify();
    }

    fn resolve_connection(&self, cx: &App) -> Option<Arc<dyn Connection>> {
        let profile_id = self.profile_id?;
        let connected = self.app_state.read(cx).connections().get(&profile_id)?;

        Some(match &self.database {
            Some(db) => connected.connection_for_database(db),
            None => connected.connection.clone(),
        })
    }

    fn target_database(&self, connection: &Arc<dyn Connection>) -> String {
        self.database
            .clone()
            .or_else(|| connection.active_database())
            .unwrap_or_default()
    }

    fn choose_folder(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            self.manifest_error = Some("No active connection for this profile".to_string());
            cx.notify();
            return;
        };
        let target_database = self.target_database(&connection);
        let supports_truncate = connection.supports(DriverCapabilities::TRUNCATE_TABLE);
        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        self.loading = true;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let dir = if dialog_available {
                match rfd::AsyncFileDialog::new()
                    .set_title("Choose Import Folder")
                    .pick_folder()
                    .await
                {
                    Some(handle) => handle.path().to_path_buf(),
                    None => {
                        this.update(cx, |this, cx| {
                            this.loading = false;
                            cx.notify();
                        })
                        .ok();
                        return;
                    }
                }
            } else {
                this.update(cx, |this, cx| {
                    this.loading = false;
                    this.manifest_error =
                        Some("No folder picker available on this platform".to_string());
                    cx.notify();
                })
                .ok();
                return;
            };

            let manifest_path = dir.join("manifest.json");
            let manifest_result = cx
                .background_executor()
                .spawn(async move { read_manifest(&manifest_path) })
                .await;

            let manifest = match manifest_result {
                Ok(manifest) => manifest,
                Err(e) => {
                    this.update(cx, |this, cx| {
                        this.loading = false;
                        this.manifest_error = Some(format!("Invalid import bundle: {e}"));
                        cx.notify();
                    })
                    .ok();
                    return;
                }
            };

            let mut configs = Vec::with_capacity(manifest.tables.len());
            for manifest_table in &manifest.tables {
                let target_exists_and_columns = connection
                    .table_details(
                        &target_database,
                        manifest_table.schema.as_deref(),
                        &manifest_table.name,
                    )
                    .ok()
                    .map(|info| {
                        info.columns
                            .unwrap_or_default()
                            .into_iter()
                            .map(|c| dbflux_core::TransferColumn {
                                name: c.name,
                                type_name: Some(c.type_name),
                                nullable: c.nullable,
                                is_primary_key: c.is_primary_key,
                            })
                            .collect::<Vec<_>>()
                    });
                let target_exists = target_exists_and_columns.is_some();
                let target_columns = target_exists_and_columns.unwrap_or_default();

                configs.push(TableImportConfig::new(
                    manifest_table,
                    target_exists,
                    target_columns,
                ));
            }

            this.update(cx, |this, cx| {
                this.loading = false;
                this.manifest_dir = Some(dir);
                this.supports_truncate = supports_truncate;
                this.build_rows(configs, cx);
                this.step = WizardStep::Configure;
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn build_rows(&mut self, configs: Vec<TableImportConfig>, cx: &mut Context<Self>) {
        self.rows.clear();
        self._row_subscriptions.clear();
        let supports_truncate = self.supports_truncate;

        for (table_index, config) in configs.into_iter().enumerate() {
            let mode_options = mapping_mode_options(supports_truncate);
            let selected_mode_index = mode_options
                .iter()
                .position(|(_, mode)| *mode == config.mapping_mode);
            let mode_items: Vec<DropdownItem> = mode_options
                .iter()
                .map(|(label, _)| DropdownItem::new(*label))
                .collect();

            let mapping_mode_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("import-mode-{table_index}")))
                    .items(mode_items)
                    .selected_index(selected_mode_index)
                    .placeholder("Mode")
            });

            let target_items: Vec<DropdownItem> = config
                .target_columns
                .iter()
                .map(|c| DropdownItem::new(c.name.clone()))
                .collect();
            let rebind_target_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("import-target-{table_index}")))
                    .items(target_items)
                    .placeholder("Target column")
            });

            let mut source_items = vec![DropdownItem::new("(unset)")];
            source_items.extend(
                config
                    .source_columns
                    .iter()
                    .map(|c| DropdownItem::new(c.name.clone())),
            );
            let rebind_source_dropdown = cx.new(|_cx| {
                Dropdown::new(SharedString::from(format!("import-source-{table_index}")))
                    .items(source_items)
                    .placeholder("Source column")
            });

            let mode_sub = cx.subscribe(
                &mapping_mode_dropdown,
                move |this, _entity, event: &DropdownSelectionChanged, cx| {
                    let mode_options = mapping_mode_options(this.supports_truncate);
                    if let Some((_, mode)) = mode_options.get(event.index)
                        && let Some(row) = this.rows.get_mut(table_index)
                    {
                        row.config.mapping_mode = *mode;
                        cx.notify();
                    }
                },
            );

            self.rows.push(TableImportRow {
                config,
                mapping_mode_dropdown,
                rebind_target_dropdown,
                rebind_source_dropdown,
            });
            self._row_subscriptions.push(mode_sub);
        }
    }

    fn apply_rebind(&mut self, table_index: usize, cx: &mut Context<Self>) {
        let Some(row) = self.rows.get(table_index) else {
            return;
        };

        let Some(target_index) = row
            .rebind_target_dropdown
            .read(cx)
            .selected_value()
            .and_then(|value| {
                row.config
                    .target_columns
                    .iter()
                    .position(|c| c.name.as_str() == value.as_ref())
            })
        else {
            return;
        };

        let source_index = row
            .rebind_source_dropdown
            .read(cx)
            .selected_value()
            .and_then(|value| {
                row.config
                    .source_columns
                    .iter()
                    .position(|c| c.name.as_str() == value.as_ref())
            });

        if let Some(row) = self.rows.get_mut(table_index) {
            row.config.set_binding(target_index, source_index);
        }
        cx.notify();
    }

    fn continue_from_configure(&mut self, cx: &mut Context<Self>) {
        let has_destructive = self.rows.iter().any(|row| row.config.is_destructive());
        self.step = if has_destructive {
            WizardStep::Confirm
        } else {
            self.start_import(cx);
            WizardStep::Running
        };
        cx.notify();
    }

    fn confirm_destructive_and_run(&mut self, cx: &mut Context<Self>) {
        self.start_import(cx);
        self.step = WizardStep::Running;
        cx.notify();
    }

    fn start_import(&mut self, cx: &mut Context<Self>) {
        let Some(connection) = self.resolve_connection(cx) else {
            report_error(
                UserFacingError::new(ErrorKind::Storage, "No active connection for this import"),
                cx,
            );
            return;
        };
        let Some(manifest_dir) = self.manifest_dir.clone() else {
            return;
        };
        let target_database = self.target_database(&connection);
        let profile_id = self.profile_id;
        let database = self.database.clone();

        let plans: Vec<ImportTablePlan> = self
            .rows
            .iter()
            .map(|row| ImportTablePlan {
                source_table: row.config.source_table.clone(),
                target_schema: row.config.target_schema.clone(),
                target_table: row.config.target_table.clone(),
                mapping_mode: row.config.mapping_mode,
                column_overrides: Some(row.config.to_overrides()),
            })
            .collect();
        let destructive_confirmed = self.rows.iter().any(|row| row.config.is_destructive());

        self.running = true;
        self.result_summary = None;
        self.result_warnings.clear();
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = (0, None);

        let description = format!("Import {} table(s)", plans.len());
        let (task_id, cancel_token) = self.app_state.update(cx, |state, cx| {
            let pair = state.start_task_for_target(
                TaskKind::Import,
                description,
                profile_id.map(|profile_id| TaskTarget {
                    profile_id,
                    database: database.clone(),
                }),
            );
            cx.emit(AppStateChanged);
            pair
        });
        self.active_task_id = Some(task_id);

        let app_state = self.app_state.clone();
        let progress = Arc::clone(&self.progress);
        let ticker_progress = Arc::clone(&self.progress);
        let ticker_app_state = app_state.clone();

        cx.spawn(async move |_this, cx| {
            loop {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(150))
                    .await;

                let still_running = cx
                    .update(|cx| {
                        ticker_app_state.update(cx, |state, cx| {
                            let Some(snapshot) = state.tasks().get(task_id) else {
                                return false;
                            };
                            if snapshot.status != TaskStatus::Running {
                                return false;
                            }

                            let (rows_done, estimated_total) = *ticker_progress
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if let Some(total) = estimated_total
                                && total > 0
                            {
                                let fraction = (rows_done as f32 / total as f32).clamp(0.0, 1.0);
                                state.tasks_mut().update_progress(task_id, fraction);
                                cx.notify();
                            }

                            true
                        })
                    })
                    .unwrap_or(false);

                if !still_running {
                    break;
                }
            }
        })
        .detach();

        cx.spawn(async move |this, cx| {
            let import_result = cx
                .background_executor()
                .spawn(async move {
                    let options = ImportOptions {
                        segment_size: 500,
                        target_database,
                        destructive_confirmed,
                    };

                    run_import(
                        &connection,
                        &manifest_dir,
                        &plans,
                        &options,
                        &cancel_token,
                        move |_index, rows_done, estimated_total| {
                            if let Ok(mut guard) = progress.lock() {
                                *guard = (rows_done, estimated_total);
                            }
                        },
                    )
                })
                .await;

            this.update(cx, |this, cx| {
                this.running = false;
                this.step = WizardStep::Done;

                match import_result {
                    Ok(outcome) if outcome.cancelled => {
                        app_state.update(cx, |state, cx| {
                            state.tasks_mut().cancel(task_id);
                            cx.emit(AppStateChanged);
                        });
                        this.result_summary = Some("Import cancelled".to_string());
                    }
                    Ok(outcome) => {
                        app_state.update(cx, |state, cx| {
                            state.complete_task(task_id);
                            cx.emit(AppStateChanged);
                        });
                        this.result_summary = Some(Self::summarize(&outcome));
                        this.result_warnings = outcome.warnings;
                        Toast::success("Import completed").push(cx);
                    }
                    Err(e) => {
                        app_state.update(cx, |state, cx| {
                            state.fail_task(task_id, e.to_string());
                            cx.emit(AppStateChanged);
                        });
                        report_error(
                            UserFacingError::new(ErrorKind::Driver, format!("Import failed: {e}")),
                            cx,
                        );
                        this.result_summary = Some(format!("Import failed: {e}"));
                    }
                }

                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn summarize(outcome: &ImportOutcome) -> String {
        let imported = outcome.tables.iter().filter(|t| !t.skipped).count();
        let skipped = outcome.tables.iter().filter(|t| t.skipped).count();
        let rows: u64 = outcome.tables.iter().map(|t| t.rows_imported).sum();

        format!("Imported {imported} table(s), {rows} row(s) total ({skipped} skipped)")
    }
}

impl Render for ImportWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            let _ = close_entity.update(cx, |this, cx| this.close(cx));
        };

        let mut frame = ModalFrame::new("import-wizard", &self.focus_handle, close)
            .title("Import Data")
            .icon(AppIcon::Download)
            .width(px(720.0))
            .max_height(px(640.0));

        let body = match self.step {
            WizardStep::PickFolder => self.render_pick_folder(cx),
            WizardStep::Configure => self.render_configure(cx),
            WizardStep::Confirm => self.render_confirm(cx),
            WizardStep::Running => self.render_running(),
            WizardStep::Done => self.render_done(cx),
        };

        frame = frame.child(body);
        frame.render(cx).into_any_element()
    }
}

impl ImportWizard {
    fn render_pick_folder(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(
                "Choose the folder that contains manifest.json and the exported table files.",
            ))
            .when_some(self.manifest_error.clone(), |d, error| {
                d.child(Text::caption(error))
            })
            .child(
                Button::new(
                    "import-wizard-choose-folder",
                    if self.loading {
                        "Reading manifest..."
                    } else {
                        "Choose Folder..."
                    },
                )
                .disabled(self.loading)
                .on_click(cx.listener(|this, _, _, cx| this.choose_folder(cx))),
            )
            .into_any_element()
    }

    fn render_configure(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows = self.rows.iter().enumerate().map(|(table_index, row)| {
            let unmatched = row.config.unmatched_source_names();

            div()
                .flex()
                .flex_col()
                .gap(Spacing::XS)
                .p(Spacing::SM)
                .border_1()
                .child(Text::body(format!(
                    "{} → {}",
                    row.config.source_table, row.config.target_table
                )))
                .child(
                    div()
                        .flex()
                        .gap(Spacing::SM)
                        .child(row.mapping_mode_dropdown.clone())
                        .child(row.rebind_target_dropdown.clone())
                        .child(row.rebind_source_dropdown.clone())
                        .child(
                            Button::new(
                                SharedString::from(format!("import-apply-mapping-{table_index}")),
                                "Apply Mapping",
                            )
                            .ghost()
                            .on_click(cx.listener(
                                move |this, _, _, cx| {
                                    this.apply_rebind(table_index, cx);
                                },
                            )),
                        ),
                )
                .when(!unmatched.is_empty(), |d| {
                    d.child(Text::caption(format!(
                        "Unmatched source column(s), will be skipped unless remapped: {}",
                        unmatched.join(", ")
                    )))
                })
                .into_any_element()
        });

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .children(rows)
            .child(
                Button::new("import-wizard-continue", "Continue")
                    .on_click(cx.listener(|this, _, _, cx| this.continue_from_configure(cx))),
            )
            .into_any_element()
    }

    fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let destructive_tables: Vec<String> = self
            .rows
            .iter()
            .filter(|row| row.config.is_destructive())
            .map(|row| row.config.target_table.clone())
            .collect();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body(format!(
                "This will drop-and-recreate or truncate the following table(s) before loading data: {}",
                destructive_tables.join(", ")
            )))
            .child(Text::caption("This cannot be undone. Confirm to proceed."))
            .child(
                div()
                    .flex()
                    .gap(Spacing::SM)
                    .child(
                        Button::new("import-wizard-cancel-confirm", "Back")
                            .ghost()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.step = WizardStep::Configure;
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("import-wizard-confirm-destructive", "Yes, proceed")
                            .on_click(cx.listener(|this, _, _, cx| this.confirm_destructive_and_run(cx))),
                    ),
            )
            .into_any_element()
    }

    fn render_running(&self) -> AnyElement {
        let (rows_done, estimated_total) = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let label = match estimated_total {
            Some(total) if total > 0 => format!("{rows_done} / {total} rows"),
            _ => format!("{rows_done} rows"),
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(Text::body("Importing..."))
            .child(Text::caption(label))
            .into_any_element()
    }

    fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .when_some(self.result_summary.clone(), |d, summary| {
                d.child(Text::body(summary))
            })
            .when(!self.result_warnings.is_empty(), |d| {
                d.child(Text::caption(self.result_warnings.join("; ")))
            })
            .child(
                Button::new("import-wizard-close", "Close")
                    .on_click(cx.listener(|this, _, _, cx| this.close(cx))),
            )
            .into_any_element()
    }
}
