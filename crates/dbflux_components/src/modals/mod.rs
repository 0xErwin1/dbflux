pub mod active_query;
pub mod cell_editor;
pub mod delete_connection;
pub mod document_preview;
pub mod drop_table;
pub mod import_dashboard;
pub mod mutation_confirm;
pub mod schema_drift;
pub mod shell;
pub mod tunnel_auth;
pub mod unsaved_changes;

pub use active_query::{
    ActiveQueryOutcome, ActiveQueryRequest, ActiveQueryTrigger, ModalActiveQuery,
};
pub use cell_editor::{CellEditorClosedEvent, CellEditorModal, CellEditorSaveEvent};
pub use delete_connection::{
    DeleteConnectionOutcome, DeleteConnectionRequest, ModalDeleteConnection,
};
pub use document_preview::{
    DOC_INDEX_NEW, DocumentPreviewClosedEvent, DocumentPreviewModal, DocumentPreviewSaveEvent,
};
pub use drop_table::{DropTableOutcome, DropTableRequest, ModalDropTable};
pub use import_dashboard::{
    ImportDashboardCancelled, ImportDashboardConfirmed, ModalImportDashboard,
};
pub use mutation_confirm::{
    ModalMutationConfirm, ModalMutationConfirmHard, MutationConfirmHardRequest,
    MutationConfirmOutcome, MutationConfirmRequest,
};
pub use schema_drift::{
    ModalSchemaDrift, SchemaDriftContinue, SchemaDriftDismissed, SchemaDriftRefresh,
};
pub use shell::{ModalShell, ModalVariant};
pub use tunnel_auth::{ModalTunnelAuth, TunnelAuthOutcome, TunnelAuthRequest};
pub use unsaved_changes::{
    CloseAction, DirtySummaryEntry, ModalUnsavedChanges, UnsavedChangesOutcome,
    UnsavedChangesRequest,
};

/// Binds Escape to [`crate::actions::Cancel`] inside the cell editor and the
/// document preview. Both render in a `ModalFrame`, which closes on `Cancel`;
/// a focused editor only lets Escape through when it has nothing of its own
/// to cancel.
pub fn register_modal_keybindings(cx: &mut gpui::App) {
    cx.bind_keys(modal_keybindings());
}

fn modal_keybindings() -> Vec<gpui::KeyBinding> {
    use dbflux_core::keymap_types::ContextId;

    [ContextId::CellEditorModal, ContextId::DocumentPreviewModal]
        .iter()
        .map(|context| {
            gpui::KeyBinding::new(
                "escape",
                crate::actions::Cancel,
                Some(context.as_gpui_context()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::modal_keybindings;
    use dbflux_core::keymap_types::ContextId;
    use gpui::{KeyContext, Keymap, Keystroke};

    fn escape_action_in(context: ContextId) -> bool {
        let mut keymap = Keymap::default();
        keymap.add_bindings(modal_keybindings());

        let escape = [Keystroke::parse("escape").expect("valid keystroke")];
        let stack = [KeyContext::parse(context.as_gpui_context()).expect("valid context")];
        let (matches, _pending) = keymap.bindings_for_input(&escape, &stack);

        matches
            .first()
            .is_some_and(|binding| binding.action().partial_eq(&crate::actions::Cancel))
    }

    #[test]
    fn cell_editor_and_document_preview_have_their_own_contexts() {
        assert_ne!(
            ContextId::CellEditorModal.as_gpui_context(),
            ContextId::SqlPreviewModal.as_gpui_context()
        );
        assert_ne!(
            ContextId::DocumentPreviewModal.as_gpui_context(),
            ContextId::SqlPreviewModal.as_gpui_context()
        );
        assert_ne!(
            ContextId::CellEditorModal.as_gpui_context(),
            ContextId::DocumentPreviewModal.as_gpui_context()
        );
    }

    #[test]
    fn escape_cancels_in_both_modal_contexts_only() {
        assert!(escape_action_in(ContextId::CellEditorModal));
        assert!(escape_action_in(ContextId::DocumentPreviewModal));
        assert!(
            !escape_action_in(ContextId::SqlPreviewModal),
            "the SQL preview keeps its own bindings"
        );
    }
}
