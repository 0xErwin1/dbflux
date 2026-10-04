//! The unsaved-changes part of a quit.
//!
//! A quit asks about the documents whose pending changes it cannot save
//! safely on its own, in the unsaved-changes prompt tab closes use. It is
//! asked after the active-query prompt, never at the same time: "Quit
//! anyway" there leads here. Documents the shutdown flush saves without
//! asking are not listed.
//!
//! The prompt's answers mean what they mean for a tab close: Save saves the
//! checked documents, an unchecked one keeps its changes, "Don't save" drops
//! the listed documents' changes, and Cancel keeps everything. Every answer
//! that does not cancel checks every document again before the quit goes on,
//! because the application stays usable while saves run: a document that
//! needs asking then (an unchecked one, one changed meanwhile, or a file
//! changed on disk) opens the prompt again instead of being lost.

use super::*;
use crate::ui::document::DocumentId;
use crate::ui::document::pane::QuitDisposition;
use crate::ui::overlays::modals::{
    DirtySummaryEntry, UnsavedChangesOutcome, UnsavedChangesRequest,
};

impl Workspace {
    /// Opens the unsaved-changes prompt for the documents a quit cannot save
    /// on its own, leaving out `discard`, the documents whose changes the
    /// user already chose to drop in this quit, and takes the keyboard.
    ///
    /// Returns `false`, without opening anything, when no document needs
    /// asking, so the caller can go ahead with the quit.
    pub(in crate::ui::views::workspace) fn prompt_unsaved_before_quit(
        &mut self,
        discard: Vec<DocumentId>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        self.commit_pending_inputs_before_quit(&discard, cx);

        let entries = self.documents_requiring_quit_confirmation(&discard, cx);

        if entries.is_empty() {
            self.pending_quit = None;
            return false;
        }

        self.pending_quit = Some(PendingQuit::Asking { discard });

        self.modal_unsaved_changes.update(cx, |modal, cx| {
            modal.open(UnsavedChangesRequest { entries }, cx);
        });

        // Take the keyboard so Enter and Escape resolve the prompt through
        // the ConfirmModal keymap instead of reaching the document behind it.
        self.focus_handle.focus(window, cx);
        cx.notify();

        true
    }

    /// Commits the input each document still holds in an open editor, such
    /// as a value typed into a cell before Enter, leaving out `discard`, so
    /// the quit check counts it as a pending change. As in the shutdown
    /// flush, input that cannot be committed does not hold the quit up.
    fn commit_pending_inputs_before_quit(
        &mut self,
        discard: &[DocumentId],
        cx: &mut Context<Self>,
    ) {
        self.tab_manager.update(cx, |manager, cx| {
            for tab in manager.documents() {
                if !discard.contains(&tab.id()) {
                    tab.as_pane().commit_pending_input(cx);
                }
            }
        });
    }

    /// The documents whose pending changes need the user's decision before a
    /// quit, in tab order, leaving out `discard`.
    fn documents_requiring_quit_confirmation(
        &self,
        discard: &[DocumentId],
        cx: &App,
    ) -> Vec<DirtySummaryEntry> {
        self.tab_manager
            .read(cx)
            .documents()
            .iter()
            .filter(|tab| !discard.contains(&tab.id()))
            .filter(|tab| tab.as_pane().quit_disposition(cx) == QuitDisposition::NeedsDecision)
            .map(|tab| DirtySummaryEntry {
                id: tab.id(),
                name: tab.tab_title(cx),
                summary: tab.change_summary(cx).unwrap_or_default(),
                action: tab.as_pane().close_action(),
            })
            .collect()
    }

    /// Applies the user's answer to the unsaved-changes prompt a quit opened.
    /// No tab is closed in any case: closing rewrites the session.
    pub(in crate::ui::views::workspace) fn resolve_quit_prompt(
        &mut self,
        outcome: &UnsavedChangesOutcome,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(PendingQuit::Asking { mut discard }) = self.pending_quit.take() else {
            return;
        };

        match outcome {
            UnsavedChangesOutcome::Cancelled => {
                self.set_focus(self.focus_target, window, cx);
            }

            UnsavedChangesOutcome::DiscardAll(ids) => {
                discard.extend(ids.iter().copied());
                self.continue_quit(discard, window, cx);
            }

            UnsavedChangesOutcome::SaveSelected(selected) => {
                self.save_before_quit(selected, discard, window, cx);
                self.set_focus(self.focus_target, window, cx);
            }
        }
    }

    /// Starts the saves the user chose in the quit's prompt. A document that
    /// cannot start one keeps the application open, with a warning: the
    /// quit would otherwise lose its changes.
    fn save_before_quit(
        &mut self,
        selected: &[DocumentId],
        discard: Vec<DocumentId>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // In place before the saves start: a save that ends at once reports
        // before this returns.
        self.pending_quit = Some(PendingQuit::Saving {
            waiting: selected.to_vec(),
            failed: false,
            discard,
        });

        let mut unsaveable = 0;

        for id in selected {
            let started = self.tab_manager.update(cx, |manager, cx| {
                manager
                    .document(*id)
                    .is_some_and(|tab| tab.as_pane().save_for_quit(window, cx))
            });

            if !started {
                unsaveable += 1;
            }
        }

        if unsaveable > 0 {
            self.pending_quit = None;

            Toast::warning(crate::ui::labels::unsaved_changes_cannot_save_message(
                unsaveable,
            ))
            .meta_right(now_hms())
            .push(cx);
        }
    }

    /// Takes `id` out of the saves the quit waits for: its save reported
    /// whether it `succeeded`, or its tab closed. Once nothing is waited for,
    /// the quit is dropped when a save failed (each failure was already
    /// reported, and its changes stay in the open application) and goes on
    /// through [`Self::continue_quit`] otherwise.
    pub(in crate::ui::views::workspace) fn leave_quit_wait(
        &mut self,
        id: DocumentId,
        succeeded: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(PendingQuit::Saving {
            waiting, failed, ..
        }) = self.pending_quit.as_mut()
        else {
            return;
        };

        let Some(position) = waiting.iter().position(|waited| *waited == id) else {
            return;
        };

        waiting.remove(position);
        *failed |= !succeeded;

        if !waiting.is_empty() {
            return;
        }

        let Some(PendingQuit::Saving {
            failed, discard, ..
        }) = self.pending_quit.take()
        else {
            return;
        };

        if failed {
            return;
        }

        self.continue_quit(discard, window, cx);
    }

    /// Checks every document again and quits when nothing needs asking:
    /// the changes of `discard` are dropped and [`QuitConfirmed`] is emitted.
    /// Otherwise the prompt opens again, and the quit waits for its answer.
    fn continue_quit(
        &mut self,
        discard: Vec<DocumentId>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.prompt_unsaved_before_quit(discard.clone(), window, cx) {
            return;
        }

        self.tab_manager.update(cx, |manager, cx| {
            for id in &discard {
                if let Some(tab) = manager.document(*id) {
                    tab.as_pane().discard_for_quit(cx);
                }
            }
        });

        cx.emit(QuitConfirmed);
    }
}
