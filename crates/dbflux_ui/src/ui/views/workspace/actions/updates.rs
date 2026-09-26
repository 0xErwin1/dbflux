use super::*;
use dbflux_app::updates::{self, StartupDialog};
use dbflux_ui_base::updates::{UpdateDialogRequest, run_update_check};

impl Workspace {
    /// Runs once per app start, for the main window only: opens the welcome
    /// or "What is new" dialog when due, records this start, and checks for
    /// a newer release when the user allows it.
    pub fn start_update_flow(&mut self, cx: &mut Context<Self>) {
        let settings = self.app_state.read(cx).update_settings().clone();
        let current_version = updates::current_version();
        let dialog = updates::startup_dialog(&settings, current_version);

        let recorded = updates::record_startup(&settings, current_version, &dialog);
        if recorded != settings {
            let result = self
                .app_state
                .update(cx, |state, _| state.set_update_settings(recorded));
            if let Err(error) = result {
                log::warn!("Failed to record this start in the update settings: {error}");
            }
        }

        match dialog {
            StartupDialog::Welcome => self.open_update_dialog(UpdateDialogRequest::Welcome, cx),
            StartupDialog::WhatsNew { since } => self.open_update_dialog(
                UpdateDialogRequest::WhatsNew {
                    since: since.map(|version| version.to_string()),
                },
                cx,
            ),
            StartupDialog::None => {}
        }

        if settings.check_for_updates_on_startup {
            run_update_check(&self.app_state, cx);
        }
    }

    /// Opens the dialog another window asked for, if no other workspace took
    /// it first.
    pub(in crate::ui::views::workspace) fn open_requested_update_dialog(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let request = self
            .app_state
            .update(cx, |state, _| state.pending_update_dialog.take());

        if let Some(request) = request {
            self.open_update_dialog(request, cx);
        }
    }

    fn open_update_dialog(&mut self, request: UpdateDialogRequest, cx: &mut Context<Self>) {
        match request {
            UpdateDialogRequest::Welcome => {
                self.whats_new_dialog
                    .update(cx, |dialog, cx| dialog.close(cx));
                self.welcome_dialog.update(cx, |dialog, cx| dialog.open(cx));
            }
            UpdateDialogRequest::WhatsNew { since } => {
                self.welcome_dialog
                    .update(cx, |dialog, cx| dialog.close(cx));
                self.whats_new_dialog
                    .update(cx, |dialog, cx| dialog.open(since, cx));
            }
        }

        cx.notify();
    }
}
