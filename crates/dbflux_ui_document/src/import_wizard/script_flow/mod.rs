//! The wizard's "Run SQL script" flow: a `.sql` file picked with "Choose
//! File…" on a SQL connection is read and split at once, reviewed on
//! Configure (what it holds, on error, one transaction), confirmed on every
//! run, executed statement by statement by
//! [`dbflux_transfer::sql_script::run_script`], and reported on Done.
//!
//! The steps are the wizard's own (`WizardStep`); this module owns what is
//! specific to a script. Nothing here names a driver: transactions come from
//! `DriverCapabilities::TRANSACTIONS` and the driver's own vocabulary, the
//! classification and the dangerous patterns from the connection's language
//! service.

mod audit;
mod interrupt;
mod messages;
mod options;
mod render;
mod run;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dbflux_core::{Connection, ReadOnlyReason};
use dbflux_transfer::sql_script::{OnError, ScriptError, ScriptSummary, SqlScript, load_script};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::*;

use super::errors::ErrorBlock;
use super::{ImportWizard, WizardStep};
use messages::{DoneView, load_error_block};
use options::{ConnectionRules, ScriptOptions, read_only_conflict};

/// Statements the Configure preview lists.
const PREVIEW_STATEMENTS: usize = 6;

/// Destructive statements the confirmation lists.
const DESTRUCTIVE_SHOWN: usize = 3;

/// A script read and split, with the choices made for it.
pub(super) struct ScriptFlow {
    path: PathBuf,
    script: Arc<SqlScript>,
    summary: ScriptSummary,
    options: ScriptOptions,
    rules: ConnectionRules,
    /// The statement the run is on, written by the run's worker.
    snippet: Arc<Mutex<String>>,
    /// Set when the run ends.
    done: Option<DoneView>,
    /// The connection of the run in progress, which Cancel Run interrupts.
    interrupt: Option<Arc<interrupt::ScopedConnection>>,
}

impl ScriptFlow {
    /// The statements that make a read-only profile refuse the script.
    pub(super) fn read_only_conflict(&self) -> Option<usize> {
        read_only_conflict(&self.summary, &self.rules)
    }

    /// Whether the user can turn "Run in one transaction" on or off.
    pub(super) fn transaction_available(&self) -> bool {
        self.options.availability(&self.rules) == options::TransactionAvailability::Available
    }

    fn file_name(&self) -> String {
        self.path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    }
}

/// Reads and splits the script at `path` and classifies its statements, off
/// the UI thread.
fn read_script(
    connection: &Arc<dyn Connection>,
    path: &Path,
    read_only_profile: bool,
) -> Result<ScriptFlow, ScriptError> {
    let script = load_script(path, &connection.metadata().query_language)?;
    let summary = script.summarize(connection.as_ref(), PREVIEW_STATEMENTS);
    let rules = ConnectionRules::of(
        connection.as_ref(),
        read_only_profile,
        summary.transaction_control.as_ref(),
    );

    Ok(ScriptFlow {
        path: path.to_path_buf(),
        script: Arc::new(script),
        summary,
        options: ScriptOptions::default(),
        rules,
        snippet: Arc::default(),
        done: None,
        interrupt: None,
    })
}

impl ImportWizard {
    /// Whether the step shown is one of a script run: the steps after the
    /// file was picked.
    pub(super) fn script_step_active(&self) -> bool {
        self.script.is_some() && self.step != WizardStep::PickFolder
    }

    /// The dialog's title: the script flow names itself once a script is
    /// picked.
    pub(super) fn dialog_title(&self) -> String {
        if self.script_step_active() {
            dbflux_i18n::t!("document.import_wizard.script.title")
        } else {
            dbflux_i18n::t!("document.import_wizard.title")
        }
    }

    /// Whether the connection runs SQL scripts, so the file dialog offers
    /// `.sql`. Decided by its query language alone.
    pub(super) fn sql_scripts_offered(connection: &dyn Connection) -> bool {
        matches!(
            connection.metadata().query_language,
            dbflux_core::QueryLanguage::Sql
        )
    }

    /// Whether the wizard's connection runs SQL scripts, which decides what
    /// the Pick Source text says about `.sql` files.
    pub(super) fn connection_runs_scripts(&self, cx: &App) -> bool {
        self.resolve_connection(cx)
            .is_some_and(|connection| Self::sql_scripts_offered(connection.as_ref()))
    }

    fn profile_is_read_only(&self, cx: &App) -> bool {
        self.profile_id
            .and_then(|profile_id| self.app_state.read(cx).connections().get(&profile_id))
            .is_some_and(|connected| {
                connected.read_only_reason == Some(ReadOnlyReason::ProfileSetting)
            })
    }

    /// Reads the picked script and, when it is fit to run, opens Configure.
    /// A script that cannot be read keeps the user on Pick Source with the
    /// reason and what to do next.
    pub(super) fn open_script(
        &mut self,
        connection: Arc<dyn Connection>,
        path: PathBuf,
        cx: &mut Context<Self>,
    ) {
        let read_only_profile = self.profile_is_read_only(cx);
        let generation = self.next_file_read_request();

        self.loading = true;
        self.loading_folder = false;
        self.manifest_error = None;
        cx.notify();

        cx.spawn(async move |this, cx| {
            let loaded = cx
                .background_executor()
                .spawn(async move { read_script(&connection, &path, read_only_profile) })
                .await;

            this.update(cx, |this, cx| {
                this.finish_script_read(generation, loaded, cx)
            })
            .ok();
        })
        .detach();
    }

    fn finish_script_read(
        &mut self,
        generation: u64,
        loaded: Result<ScriptFlow, ScriptError>,
        cx: &mut Context<Self>,
    ) {
        if generation != self.file_read_request {
            return;
        }
        self.loading = false;

        match loaded {
            Ok(flow) => {
                self.file_import = None;
                self.manifest_dir = None;
                self.rows.clear();
                self._row_subscriptions.clear();
                self.field_cursor = 0;
                self.script = Some(flow);
                self.step = WizardStep::Configure;
            }
            Err(error) => {
                let block = load_error_block(&error);
                report_error(
                    UserFacingError::new(ErrorKind::User, block.message.clone()),
                    cx,
                );
                self.script = None;
                self.manifest_error = Some(block);
            }
        }
        cx.notify();
    }

    pub(super) fn set_script_on_error(&mut self, on_error: OnError, cx: &mut Context<Self>) {
        if let Some(flow) = self.script.as_mut() {
            flow.options.on_error = on_error;
            cx.notify();
        }
    }

    pub(super) fn set_script_transaction(&mut self, checked: bool, cx: &mut Context<Self>) {
        if let Some(flow) = self.script.as_mut() {
            flow.options.transaction = checked;
            cx.notify();
        }
    }

    /// The keyboard's way to the "On error" segments: the other choice.
    pub(super) fn toggle_script_on_error(&mut self, cx: &mut Context<Self>) {
        let Some(flow) = self.script.as_ref() else {
            return;
        };
        let next = match flow.options.on_error {
            OnError::Stop => OnError::Continue,
            OnError::Continue => OnError::Stop,
        };

        self.set_script_on_error(next, cx);
    }

    /// The keyboard's way to the transaction checkbox, when it is on offer.
    pub(super) fn toggle_script_transaction(&mut self, cx: &mut Context<Self>) {
        let Some(flow) = self
            .script
            .as_ref()
            .filter(|flow| flow.transaction_available())
        else {
            return;
        };
        let next = !flow.options.transaction;

        self.set_script_transaction(next, cx);
    }

    /// Continue on Configure: every script goes through Confirm. A read-only
    /// profile that the script would change stays on Configure, which says why.
    pub(super) fn continue_script(&mut self, cx: &mut Context<Self>) {
        let Some(flow) = self.script.as_ref() else {
            return;
        };

        if flow.read_only_conflict().is_none() {
            self.step = WizardStep::Confirm;
        }
        cx.notify();
    }

    /// The error block of a script that could not run on this profile, shown
    /// on Configure.
    pub(super) fn script_read_only_block(&self) -> Option<ErrorBlock> {
        let changing = self.script.as_ref()?.read_only_conflict()?;

        Some(ErrorBlock {
            message: options::counted(
                "document.import_wizard.script.error.read_only",
                changing,
                &[],
            ),
            next_step: dbflux_i18n::t!("document.import_wizard.script.error.read_only_next"),
            footer_reason: None,
        })
    }
}
