//! Export wizard: a four-phase flow (Tables → Format → Confirm → Run)
//! rendered inside a modal with the shared horizontal stepper and footer
//! (P1Flows). Unlike migrate, no phase needs live
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

use dbflux_core::keymap_types::ContextId;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dbflux_components::composites::{
    RailItem, render_wizard_progress_bar, render_wizard_stepper, wizard_progress_fraction,
};
use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{
    Modal, modal_form_row, modal_frame, modal_hint, modal_lead, modal_value_field,
};
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{ChromeColors, ModalMetrics, ui};
use dbflux_components::typography::AppFonts;
use dbflux_core::{Connection, TableRef};
use dbflux_transfer::FileFormat;
use dbflux_ui_base::app_state_entity::AppStateEntity;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

use phases::{ExportPhase, RailEntry, RunState, next_phase, prev_phase, rail_entries};
use run::RunProgress;

/// Width of the export dialog (P1Flows).
const EXPORT_WIZARD_WIDTH: Pixels = px(620.0);

/// Width of the chunk-size field. (120 px)
const SEGMENT_SIZE_INPUT_WIDTH: Pixels = px(120.0);

/// Height of a selected-table row on the Tables step. (36 px)
const TABLE_ROW_HEIGHT: Rems = ui(36.0);

/// Tallest the selected-table list grows before it scrolls.
const TABLE_LIST_MAX_HEIGHT: Pixels = px(240.0);

/// Segment id of a file format in the format picker.
fn format_segment_id(format: FileFormat) -> &'static str {
    format.extension()
}

/// Icon of a file format in the format picker.
fn format_icon(format: FileFormat) -> AppIcon {
    match format {
        FileFormat::Csv => AppIcon::FileSpreadsheet,
        FileFormat::Json => AppIcon::Braces,
    }
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

    selected_format: FileFormat,

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
        let segment_size_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(phases::DEFAULT_SEGMENT_SIZE.to_string())
                .placeholder(dbflux_i18n::t!(
                    "document.export_wizard.format_options.segment_size_placeholder"
                ))
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
            selected_format: FileFormat::ALL[0],
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
            self.focus_handle.focus(window, cx);
            cx.notify();
            return;
        }

        self.visible = true;
        self.profile_id = Some(profile_id);
        self.database = database;
        self.tables = tables;
        self.phase = ExportPhase::Tables;

        self.selected_format = FileFormat::ALL[0];

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

        self.focus_handle.focus(window, cx);
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
        self.selected_format
    }

    fn select_format(&mut self, segment_id: &str, cx: &mut Context<Self>) {
        if let Some(format) = FileFormat::ALL
            .into_iter()
            .find(|format| format_segment_id(*format) == segment_id)
        {
            self.selected_format = format;
            cx.notify();
        }
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
                    .set_title(dbflux_i18n::t!("document.export_wizard.format_options.dialog_title"))
                    .pick_folder()
                    .await
                    .map(|handle| handle.path().to_path_buf())
            } else {
                match dbflux_ui_base::file_dialog::fallback_export_dir() {
                    Ok(dir) => Some(dir),
                    Err(err) => {
                        this.update(cx, |this, cx| {
                            this.choosing_folder = false;
                            this.folder_error = Some(dbflux_i18n::t!(
                                "document.export_wizard.format_options.error.no_dialog_fallback_failed",
                                error = err
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
    /// phase: `Tables` requires a non-empty selection, `Format`
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

        let body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(render_wizard_stepper(&to_rail_items(self.phase), cx))
            .child(match self.phase {
                ExportPhase::Tables => self.render_tables(cx),
                ExportPhase::FormatOptions => self.render_format_options(cx),
                ExportPhase::Confirm => self.render_confirm(cx),
                ExportPhase::Run => self.render_run(cx),
            });

        Modal::new(crate::labels::export_wizard_title(self.tables.len()))
            .id("export-wizard")
            .focus_handle(&self.focus_handle)
            .on_close(close)
            .key_context(ContextId::SqlPreviewModal.as_gpui_context())
            .icon(AppIcon::Download)
            .width(EXPORT_WIZARD_WIDTH)
            .body(body)
            .footer(self.render_footer(cx))
            .into_any_element()
    }
}

impl ExportWizard {
    fn render_tables(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let last_row = self.tables.len().saturating_sub(1);

        let rows = self.tables.iter().enumerate().map(|(index, table)| {
            div()
                .flex()
                .flex_shrink_0()
                .items_center()
                .gap(ModalMetrics::LIST_ROW_GAP)
                .h(TABLE_ROW_HEIGHT)
                .px(ModalMetrics::LIST_ROW_PADDING_X)
                .when(index < last_row, |row| {
                    row.border_b_1().border_color(theme.table_row_border)
                })
                .child(
                    Icon::new(AppIcon::Table)
                        .size(ModalMetrics::LIST_ICON)
                        .color(theme.muted_foreground),
                )
                .child(
                    div()
                        .min_w_0()
                        .truncate()
                        .font_family(dbflux_components::fonts::editor_family(cx))
                        .text_size(ModalMetrics::CODE_FONT)
                        .text_color(ChromeColors::strong(theme))
                        .child(table.qualified_name()),
                )
        });

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                dbflux_i18n::t!(
                    "document.export_wizard.tables.selected_count",
                    count = self.tables.len()
                ),
                cx,
            ))
            .child(
                modal_frame(cx).child(
                    div()
                        .id("export-wizard-tables")
                        .flex()
                        .flex_col()
                        .max_h(TABLE_LIST_MAX_HEIGHT)
                        .overflow_y_scroll()
                        .children(rows),
                ),
            )
            .into_any_element()
    }

    fn render_format_options(&self, cx: &mut Context<Self>) -> AnyElement {
        let format_items: Vec<SegmentedItem> = FileFormat::ALL
            .into_iter()
            .map(|format| {
                SegmentedItem::new(format_segment_id(format), format.label())
                    .icon(format_icon(format))
            })
            .collect();

        let entity = cx.entity().downgrade();
        let format_picker = div().flex().child(SegmentedControl::new(
            format_items,
            format_segment_id(self.selected_format),
            move |segment_id, _window, cx| {
                entity
                    .update(cx, |this, cx| this.select_format(segment_id, cx))
                    .ok();
            },
        ));

        let folder_value = self
            .output_dir
            .as_ref()
            .map(|dir| SharedString::from(dir.display().to_string()));

        let folder_control = div()
            .flex()
            .gap(ModalMetrics::FOOTER_GAP)
            .child(div().flex_1().min_w_0().child(modal_value_field(
                folder_value,
                dbflux_i18n::t!("document.export_wizard.format_options.no_folder_chosen"),
                None,
                cx,
            )))
            .child(
                Button::new(
                    "export-wizard-choose-folder",
                    if self.choosing_folder {
                        dbflux_i18n::t!("document.export_wizard.format_options.choosing")
                    } else {
                        dbflux_i18n::t!("document.export_wizard.format_options.choose_folder")
                    },
                )
                .icon(AppIcon::Folder)
                .disabled(self.choosing_folder)
                .on_click(cx.listener(|this, _event, _window, cx| this.choose_folder(cx))),
            );

        let folder_hint = self
            .folder_error
            .clone()
            .map(|error| Text::caption(error).danger().into_any_element());

        let segment_hint = if self.segment_size_invalid {
            Text::caption(dbflux_i18n::t!(
                "document.export_wizard.format_options.segment_size_invalid"
            ))
            .danger()
            .into_any_element()
        } else {
            modal_hint(
                dbflux_i18n::t!("document.export_wizard.format_options.segment_size_hint"),
                cx,
            )
        };

        div()
            .flex()
            .flex_col()
            .child(modal_form_row(
                dbflux_i18n::t!("document.export_wizard.format_options.format_label"),
                format_picker,
                None,
                cx,
            ))
            .child(modal_form_row(
                dbflux_i18n::t!("document.export_wizard.format_options.output_folder_label"),
                folder_control,
                folder_hint,
                cx,
            ))
            .child(modal_form_row(
                dbflux_i18n::t!("document.export_wizard.format_options.segment_size_label"),
                div()
                    .w(SEGMENT_SIZE_INPUT_WIDTH)
                    .font_family(dbflux_components::fonts::editor_family(cx))
                    .child(Input::new(&self.segment_size_input).w_full().aria_label(
                        dbflux_i18n::t!("document.export_wizard.format_options.segment_size_label"),
                    )),
                Some(segment_hint),
                cx,
            ))
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
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(
                dbflux_i18n::t!(
                    "document.export_wizard.confirm.summary",
                    count = self.tables.len(),
                    format = self.selected_format().label(),
                    folder = folder_label
                ),
                cx,
            ))
            .child(modal_hint(
                dbflux_i18n::t!(
                    "document.export_wizard.confirm.segment_size",
                    size = self.segment_size
                ),
                cx,
            ))
            .into_any_element()
    }

    fn render_run(&self, cx: &mut Context<Self>) -> AnyElement {
        match self.run_state {
            RunState::Idle => div().into_any_element(),
            RunState::Running => self.render_running(cx),
            RunState::Done => self.render_done(cx),
        }
    }

    fn render_running(&self, cx: &mut Context<Self>) -> AnyElement {
        let progress = *self.progress.lock().unwrap_or_else(|p| p.into_inner());
        let names: Vec<String> = self.tables.iter().map(|t| t.qualified_name()).collect();
        let total_tables = names.len();
        let current_index = progress.table_index.min(total_tables.saturating_sub(1));
        let current_table = names.get(current_index).cloned().unwrap_or_default();

        let rows_label =
            crate::labels::export_running_rows_label(progress.rows_done, progress.estimated_total);
        let position_label =
            crate::labels::export_running_position_label(current_index, total_tables);
        let fraction = wizard_progress_fraction(progress.rows_done, progress.estimated_total);

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(format!("{position_label}: {current_table}"), cx))
            .when_some(fraction, |el, fraction| {
                el.child(render_wizard_progress_bar(fraction, cx))
            })
            .child(modal_hint(rows_label, cx))
            .into_any_element()
    }

    fn render_done(&self, cx: &mut Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .when_some(self.result_summary.clone(), |el, summary| {
                el.child(modal_lead(summary, cx))
            })
            .when(!self.result_warnings.is_empty(), |el| {
                el.child(modal_hint(self.result_warnings.join("; "), cx))
            })
            .into_any_element()
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> AnyElement {
        let running = self.run_state == RunState::Running;
        let done = self.run_state == RunState::Done;

        let shows_back = prev_phase(self.phase).is_some() && !running && !done;
        let shows_continue = next_phase(self.phase).is_some();
        let shows_start = self.phase == ExportPhase::Confirm;
        let continue_enabled = self.continue_enabled();

        div()
            .flex()
            .items_center()
            .gap(ModalMetrics::FOOTER_GAP)
            .when(shows_back, |footer| {
                footer.child(
                    Button::new(
                        "export-wizard-back",
                        dbflux_i18n::t!("document.export_wizard.footer.back"),
                    )
                    .icon(AppIcon::ChevronLeft)
                    .on_click(cx.listener(|this, _event, _window, cx| this.go_back(cx))),
                )
            })
            .when(shows_continue, |footer| {
                footer.child(
                    Button::new(
                        "export-wizard-continue",
                        dbflux_i18n::t!("document.export_wizard.footer.continue"),
                    )
                    .primary()
                    .icon(AppIcon::ChevronRight)
                    .disabled(!continue_enabled)
                    .on_click(cx.listener(|this, _event, _window, cx| this.advance(cx))),
                )
            })
            .when(shows_start, |footer| {
                footer.child(
                    Button::new(
                        "export-wizard-start",
                        dbflux_i18n::t!("document.export_wizard.confirm.start_export"),
                    )
                    .primary()
                    .icon(AppIcon::Play)
                    .on_click(cx.listener(|this, _event, _window, cx| this.start_export(cx))),
                )
            })
            .when(running, |footer| {
                footer.child(
                    Button::new(
                        "export-wizard-cancel",
                        dbflux_i18n::t!("document.export_wizard.running.cancel"),
                    )
                    .danger()
                    .icon(AppIcon::CircleX)
                    .on_click(cx.listener(|this, _event, _window, cx| this.cancel_run(cx))),
                )
            })
            .when(done, |footer| {
                footer.child(
                    Button::new(
                        "export-wizard-close",
                        dbflux_i18n::t!("document.export_wizard.footer.close"),
                    )
                    .primary()
                    .on_click(cx.listener(|this, _event, _window, cx| this.close(cx))),
                )
            })
            .into_any_element()
    }
}
