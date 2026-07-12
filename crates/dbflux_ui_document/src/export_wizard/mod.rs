//! Export wizard: a four-phase flow (Tables → Format & Options → Confirm →
//! Run) rendered inside a modal with the shared phase rail, wearing the
//! migrate/import wizard chrome. Unlike migrate, no phase needs live
//! cross-connection metadata — the sidebar already resolved the table
//! selection before the wizard opens — so this is a single flat entity
//! holding its own format/folder/segment-size state, not a set of child
//! phase entities.
//!
//! Reached from the sidebar's "Export Table…" action, which pre-populates
//! `profile_id` / `database` / `tables`. The folder picker and format choice
//! now live inside the modal (Format & Options phase) instead of firing an
//! immediate OS dialog from the context menu. The run itself (`start_export`)
//! reuses `dbflux_transfer::export::{run_export, ExportOptions}` unchanged —
//! see [`run`].

pub mod phases;
mod run;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dbflux_components::composites::{RailItem, render_wizard_rail};
use dbflux_components::controls::{
    Button, Dropdown, DropdownItem, DropdownSelectionChanged, GpuiInput as Input, InputEvent,
    InputState,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
use dbflux_core::{Connection, TableRef};
use dbflux_transfer::FileFormat;
use dbflux_ui_base::app_state_entity::AppStateEntity;
use dbflux_ui_base::modal_frame::ModalFrame;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use uuid::Uuid;

use phases::{ExportPhase, RailEntry, RunState, next_phase, prev_phase, rail_entries};
use run::RunProgress;

const SEGMENT_SIZE_INPUT_WIDTH: Pixels = px(160.0);

fn format_items() -> Vec<DropdownItem> {
    FileFormat::ALL
        .iter()
        .map(|format| DropdownItem::new(format.label()))
        .collect()
}

/// Maps the wizard's [`RailEntry`]s to the shared rail composite's
/// domain-free [`RailItem`]s.
fn to_rail_items(current: ExportPhase) -> Vec<RailItem> {
    rail_entries(current)
        .into_iter()
        .map(|entry: RailEntry| RailItem {
            label: entry.phase.label().into(),
            completed: entry.completed,
            current: entry.current,
        })
        .collect()
}

pub struct ExportWizard {
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    visible: bool,

    profile_id: Option<Uuid>,
    database: Option<String>,
    tables: Vec<TableRef>,

    phase: ExportPhase,

    format_dropdown: Entity<Dropdown>,
    _format_dropdown_sub: Subscription,
    selected_format_index: usize,

    output_dir: Option<PathBuf>,
    choosing_folder: bool,
    folder_error: Option<String>,

    segment_size_input: Entity<InputState>,
    _segment_size_sub: Subscription,
    segment_size: u32,
    segment_size_invalid: bool,

    run_state: RunState,
    progress: Arc<Mutex<RunProgress>>,
    cancel_token: Option<dbflux_core::CancelToken>,
    result_summary: Option<String>,
    result_warnings: Vec<String>,
}

impl ExportWizard {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let format_dropdown = cx.new(|_cx| {
            Dropdown::new("export-wizard-format")
                .items(format_items())
                .selected_index(Some(0))
                .placeholder("Format")
        });
        let format_dropdown_sub = cx.subscribe(
            &format_dropdown,
            |this, _entity, event: &DropdownSelectionChanged, cx| {
                this.selected_format_index = event.index;
                cx.notify();
            },
        );

        let segment_size_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(phases::DEFAULT_SEGMENT_SIZE.to_string())
                .placeholder("Segment / chunk size")
        });
        let segment_size_sub = cx.subscribe_in(
            &segment_size_input,
            window,
            |this, _entity, event: &InputEvent, window, cx| {
                if let InputEvent::Change = event {
                    this.on_segment_size_changed(window, cx);
                }
            },
        );

        Self {
            app_state,
            focus_handle: cx.focus_handle(),
            visible: false,
            profile_id: None,
            database: None,
            tables: Vec::new(),
            phase: ExportPhase::Tables,
            format_dropdown,
            _format_dropdown_sub: format_dropdown_sub,
            selected_format_index: 0,
            output_dir: None,
            choosing_folder: false,
            folder_error: None,
            segment_size_input,
            _segment_size_sub: segment_size_sub,
            segment_size: phases::DEFAULT_SEGMENT_SIZE,
            segment_size_invalid: false,
            run_state: RunState::Idle,
            progress: Arc::new(Mutex::new(RunProgress::default())),
            cancel_token: None,
            result_summary: None,
            result_warnings: Vec::new(),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn is_running(&self) -> bool {
        self.run_state == RunState::Running
    }

    pub fn open(
        &mut self,
        profile_id: Uuid,
        database: Option<String>,
        tables: Vec<TableRef>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // A run already in flight owns the wizard until it terminates —
        // mirrors `MigrateWizard::open`'s re-entry guard: resurface the
        // running wizard instead of resetting its state and orphaning the
        // task.
        if self.is_running() {
            self.visible = true;
            self.phase = ExportPhase::Run;
            self.focus_handle.focus(window);
            cx.notify();
            return;
        }

        self.visible = true;
        self.profile_id = Some(profile_id);
        self.database = database;
        self.tables = tables;
        self.phase = ExportPhase::Tables;

        self.selected_format_index = 0;
        self.format_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(0), cx);
        });

        self.output_dir = None;
        self.choosing_folder = false;
        self.folder_error = None;

        self.segment_size = phases::DEFAULT_SEGMENT_SIZE;
        self.segment_size_invalid = false;
        self.segment_size_input.update(cx, |input, cx| {
            input.set_value(phases::DEFAULT_SEGMENT_SIZE.to_string(), window, cx);
        });

        self.run_state = RunState::Idle;
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = RunProgress::default();
        self.cancel_token = None;
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

    /// A human-readable profile name for the run's task description.
    fn profile_label(&self, cx: &App) -> String {
        let Some(profile_id) = self.profile_id else {
            return String::new();
        };
        self.app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default()
    }

    fn selected_format(&self) -> FileFormat {
        FileFormat::ALL[self.selected_format_index.min(FileFormat::ALL.len() - 1)]
    }

    fn on_segment_size_changed(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let typed = self.segment_size_input.read(cx).value().to_string();

        match phases::parse_segment_size(&typed) {
            Some(value) => {
                self.segment_size = value;
                self.segment_size_invalid = false;
            }
            None => {
                self.segment_size_invalid = true;
            }
        }

        cx.notify();
    }

    /// Picks the export destination folder in-modal, replacing the
    /// pre-redesign flow's immediate OS dialog on menu click. Same
    /// dialog-availability probe and fallback directory as the original
    /// sidebar action.
    fn choose_folder(&mut self, cx: &mut Context<Self>) {
        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        self.choosing_folder = true;
        self.folder_error = None;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let picked = if dialog_available {
                rfd::AsyncFileDialog::new()
                    .set_title("Choose Export Folder")
                    .pick_folder()
                    .await
                    .map(|handle| handle.path().to_path_buf())
            } else {
                match dbflux_ui_base::file_dialog::fallback_export_dir() {
                    Ok(dir) => Some(dir),
                    Err(err) => {
                        this.update(cx, |this, cx| {
                            this.choosing_folder = false;
                            this.folder_error = Some(format!(
                                "No folder picker available and the fallback export \
                                 directory could not be created: {err}"
                            ));
                            cx.notify();
                        })
                        .ok();
                        return;
                    }
                }
            };

            this.update(cx, |this, cx| {
                this.choosing_folder = false;
                if let Some(dir) = picked {
                    this.output_dir = Some(dir);
                    this.folder_error = None;
                }
                cx.notify();
            })
            .ok();
        })
        .detach();
    }

    fn go_back(&mut self, cx: &mut Context<Self>) {
        if let Some(previous) = prev_phase(self.phase) {
            self.go_to_phase(previous, cx);
        }
    }

    /// Back-navigation from the rail: only ever returns to an already-passed
    /// phase, and is inert once a run is live (mirrors `MigrateWizard`).
    fn go_to_phase(&mut self, phase: ExportPhase, cx: &mut Context<Self>) {
        if self.phase == ExportPhase::Run && self.is_running() {
            return;
        }
        if phase < self.phase {
            self.phase = phase;
            cx.notify();
        }
    }

    /// Whether the footer's Continue button is enabled for the current
    /// phase: `Tables` requires a non-empty selection, `Format & Options`
    /// requires a chosen folder and a valid segment size.
    fn continue_enabled(&self) -> bool {
        match self.phase {
            ExportPhase::Tables => !self.tables.is_empty(),
            ExportPhase::FormatOptions => self.output_dir.is_some() && !self.segment_size_invalid,
            ExportPhase::Confirm | ExportPhase::Run => false,
        }
    }

    fn advance(&mut self, cx: &mut Context<Self>) {
        if !self.continue_enabled() {
            return;
        }
        if let Some(next) = next_phase(self.phase) {
            self.phase = next;
            cx.notify();
        }
    }
}

impl Render for ExportWizard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let close_entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            close_entity.update(cx, |this, cx| this.close(cx)).ok();
        };

        let frame = ModalFrame::new("export-wizard", &self.focus_handle, close)
            .title("Export Data")
            .icon(AppIcon::ArrowUp)
            .width(px(720.0))
            .height_fraction(0.7)
            .center_vertically()
            .child(self.render_body(cx));

        frame.render(cx).into_any_element()
    }
}

impl ExportWizard {
    fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        // `flex_1` (not `size_full`): the modal container is a fixed-height
        // flex column whose first child is the header, so the body must grow
        // into the *remaining* height (see `MigrateWizard::render_body`).
        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h(px(0.0))
            .w_full()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(render_wizard_rail(
                        &to_rail_items(self.phase),
                        None::<fn(usize, &mut Window, &mut App)>,
                        cx,
                    ))
                    .child(self.render_phase_area(cx)),
            )
            .child(self.render_footer(cx))
            .into_any_element()
    }

    fn render_phase_area(&self, cx: &mut Context<Self>) -> AnyElement {
        let content = match self.phase {
            ExportPhase::Tables => self.render_tables(),
            ExportPhase::FormatOptions => self.render_format_options(cx),
            ExportPhase::Confirm => self.render_confirm(cx),
            ExportPhase::Run => self.render_run(cx),
        };

        div()
            .flex_1()
            .min_w(px(0.0))
            .flex()
            .flex_col()
            .p(Spacing::MD)
            .child(content)
            .into_any_element()
    }

    fn render_tables(&self) -> AnyElement {
        let rows = self
            .tables
            .iter()
            .map(|table| Text::body(table.qualified_name()).into_any_element());

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .size_full()
            .child(Text::label(format!(
                "{} table(s) selected for export",
                self.tables.len()
            )))
            .child(
                div()
                    .id("export-wizard-tables")
                    .flex_1()
                    .min_h(px(0.0))
                    .overflow_y_scroll()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .children(rows),
            )
            .into_any_element()
    }

    fn render_format_options(&self, cx: &mut Context<Self>) -> AnyElement {
        let folder_label = self
            .output_dir
            .as_ref()
            .map(|dir| dir.display().to_string())
            .unwrap_or_else(|| "No folder chosen".to_string());

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::label("Format"))
                    .child(self.format_dropdown.clone()),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::label("Output folder"))
                    .child(Text::caption(folder_label))
                    .child(
                        Button::new(
                            "export-wizard-choose-folder",
                            if self.choosing_folder {
                                "Choosing…"
                            } else {
                                "Choose Folder…"
                            },
                        )
                        .small()
                        .disabled(self.choosing_folder)
                        .on_click(cx.listener(|this, _event, _window, cx| this.choose_folder(cx))),
                    )
                    .when_some(self.folder_error.clone(), |parent, error| {
                        parent.child(Text::caption(error).danger())
                    }),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::label("Segment / chunk size (advanced)"))
                    .child(
                        div()
                            .w(SEGMENT_SIZE_INPUT_WIDTH)
                            .child(Input::new(&self.segment_size_input).small().w_full()),
                    )
                    .when(self.segment_size_invalid, |parent| {
                        parent.child(
                            Text::caption(
                                "Must be a whole number of 1 or more; kept the previous value.",
                            )
                            .danger(),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_confirm(&self, cx: &mut Context<Self>) -> AnyElement {
        let folder_label = self
            .output_dir
            .as_ref()
            .map(|dir| dir.display().to_string())
            .unwrap_or_default();

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::label("Review export plan"))
            .child(Text::caption(format!(
                "{} table(s) as {} to {folder_label}",
                self.tables.len(),
                self.selected_format().label(),
            )))
            .child(Text::caption(format!(
                "Segment / chunk size: {}",
                self.segment_size
            )))
            .child(
                div().flex().justify_end().child(
                    Button::new("export-wizard-start", "Start Export")
                        .small()
                        .primary()
                        .on_click(cx.listener(|this, _event, _window, cx| this.start_export(cx))),
                ),
            )
            .into_any_element()
    }

    fn render_run(&self, cx: &mut Context<Self>) -> AnyElement {
        match self.run_state {
            RunState::Idle => div().into_any_element(),
            RunState::Running => self.render_running(cx),
            RunState::Done => self.render_done(),
        }
    }

    fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let progress = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let names: Vec<String> = self.tables.iter().map(|t| t.qualified_name()).collect();
        let total_tables = names.len();
        let current_index = progress.table_index.min(total_tables.saturating_sub(1));
        let current_table = names.get(current_index).cloned().unwrap_or_default();

        let rows_label = match progress.estimated_total {
            Some(total) if total > 0 => format!("{} / {} rows", progress.rows_done, total),
            _ => format!("{} rows", progress.rows_done),
        };
        let position_label = if total_tables > 0 {
            format!("Table {} of {}", current_index + 1, total_tables)
        } else {
            "Preparing".to_string()
        };

        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::label("Exporting…"))
            .child(Text::caption(format!("{position_label}: {current_table}")))
            .child(Text::caption(rows_label).muted_foreground())
            .child(
                div().flex().justify_end().child(
                    Button::new("export-wizard-cancel", "Cancel")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _event, _window, cx| this.cancel_run(cx))),
                ),
            )
            .into_any_element()
    }

    fn render_done(&self) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .when_some(self.result_summary.clone(), |el, summary| {
                el.child(Text::body(summary))
            })
            .when(!self.result_warnings.is_empty(), |el| {
                el.child(Text::caption(self.result_warnings.join("; ")))
            })
            .into_any_element()
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let border = theme.border;

        let running = self.run_state == RunState::Running;
        let done = self.run_state == RunState::Done;

        let shows_back = prev_phase(self.phase).is_some() && !running && !done;
        let shows_continue = next_phase(self.phase).is_some();
        let continue_enabled = self.continue_enabled();

        let actions = div()
            .flex()
            .flex_row()
            .items_center()
            .gap(Spacing::SM)
            .when(shows_back, |parent| {
                parent.child(
                    Button::new("export-wizard-back", "Back")
                        .small()
                        .ghost()
                        .on_click(cx.listener(|this, _event, _window, cx| this.go_back(cx))),
                )
            })
            .when(shows_continue, |parent| {
                parent.child(
                    Button::new("export-wizard-continue", "Continue")
                        .small()
                        .primary()
                        .disabled(!continue_enabled)
                        .on_click(cx.listener(|this, _event, _window, cx| this.advance(cx))),
                )
            })
            .when(done, |parent| {
                parent.child(
                    Button::new("export-wizard-close", "Close")
                        .small()
                        .primary()
                        .on_click(cx.listener(|this, _event, _window, cx| this.close(cx))),
                )
            });

        div()
            .flex()
            .flex_row()
            .items_center()
            .justify_end()
            .gap(Spacing::SM)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_t_1()
            .border_color(border)
            .child(actions)
            .into_any_element()
    }
}
