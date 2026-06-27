use super::*;

impl Workspace {
    /// Opens a table in a new DataDocument tab, or focuses the existing one.
    pub(in crate::ui::views::workspace) fn open_table_document(
        &mut self,
        profile_id: uuid::Uuid,
        table: dbflux_core::TableRef,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager.read(cx).find_by_key(
                &crate::ui::document::DocumentKey::Table {
                    profile_id,
                    database: database.clone(),
                    table: table.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this table")
                    .meta_right(now_hms())
                    .action(copy_action("No active connection for this table"))
                    .push(cx);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });
                log::info!(
                    "Focused existing table document: {:?}.{:?}",
                    table.schema,
                    table.name
                );
                return;
            }
            OpenDocumentDecision::OpenNew => {}
        }

        // Create a DataDocument for the table
        let doc = cx.new(|cx| {
            DataDocument::new_for_table(
                profile_id,
                table.clone(),
                database.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let pane = DataDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        log::info!("Opened table document: {:?}.{:?}", table.schema, table.name);
    }

    pub(in crate::ui::views::workspace) fn open_collection_document(
        &mut self,
        profile_id: uuid::Uuid,
        collection: dbflux_core::CollectionRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let presentation = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| {
                collection_document_presentation_for_connection(connected, &collection)
            })
            .unwrap_or(CollectionDocumentPresentation::DataGrid);

        let existing_id = if has_connection {
            match presentation {
                CollectionDocumentPresentation::DataGrid => self.tab_manager.read(cx).find_by_key(
                    &crate::ui::document::DocumentKey::Collection {
                        profile_id,
                        collection: collection.clone(),
                    },
                    cx,
                ),
                CollectionDocumentPresentation::AuditLike => {
                    use crate::ui::document::DocumentKey;
                    let target = dbflux_core::EventStreamTarget {
                        collection: collection.clone(),
                        child_id: None,
                    };
                    self.tab_manager
                        .read(cx)
                        .find_by_key(&DocumentKey::EventStream { profile_id, target }, cx)
                }
            }
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this collection")
                    .meta_right(now_hms())
                    .action(copy_action("No active connection for this collection"))
                    .push(cx);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });
                log::info!(
                    "Focused existing collection document: {}.{}",
                    collection.database,
                    collection.name
                );
                return;
            }
            OpenDocumentDecision::OpenNew => {}
        }

        match presentation {
            CollectionDocumentPresentation::DataGrid => {
                let doc = cx.new(|cx| {
                    DataDocument::new_for_collection(
                        profile_id,
                        collection.clone(),
                        self.app_state.clone(),
                        window,
                        cx,
                    )
                });
                let pane = DataDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
            CollectionDocumentPresentation::AuditLike => {
                let doc = cx.new(|cx| {
                    crate::ui::document::AuditDocument::new_for_event_stream(
                        profile_id,
                        dbflux_core::EventStreamTarget {
                            collection: collection.clone(),
                            child_id: None,
                        },
                        collection.name.clone(),
                        self.app_state.clone(),
                        window,
                        cx,
                    )
                });
                let pane = crate::ui::document::AuditDocument::into_pane(doc, cx);
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });
            }
        }

        log::info!(
            "Opened collection document: {}.{}",
            collection.database,
            collection.name
        );
    }

    pub(in crate::ui::views::workspace) fn open_event_stream_document(
        &mut self,
        profile_id: uuid::Uuid,
        target: dbflux_core::EventStreamTarget,
        title: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            use crate::ui::document::DocumentKey;
            self.tab_manager.read(cx).find_by_key(
                &DocumentKey::EventStream {
                    profile_id,
                    target: target.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this event source")
                    .meta_right(now_hms())
                    .action(copy_action("No active connection for this event source"))
                    .push(cx);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });
                return;
            }
            OpenDocumentDecision::OpenNew => {}
        }

        let doc = cx.new(|cx| {
            crate::ui::document::AuditDocument::new_for_event_stream(
                profile_id,
                target.clone(),
                title.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });

        let pane = crate::ui::document::AuditDocument::into_pane(doc, cx);
        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
    }

    pub(in crate::ui::views::workspace) fn open_key_value_document(
        &mut self,
        profile_id: uuid::Uuid,
        database: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let has_connection = self
            .app_state
            .read(cx)
            .connections()
            .contains_key(&profile_id);

        let existing_id = if has_connection {
            self.tab_manager.read(cx).find_by_key(
                &crate::ui::document::DocumentKey::KeyValueDb {
                    profile_id,
                    database: database.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                Toast::error("No active connection for this key-value database")
                    .meta_right(now_hms())
                    .action(copy_action(
                        "No active connection for this key-value database",
                    ))
                    .push(cx);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(id, cx);
                });
                return;
            }
            OpenDocumentDecision::OpenNew => {}
        }

        let doc = cx.new(|cx| {
            crate::ui::document::KeyValueDocument::new(
                profile_id,
                database.clone(),
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let pane = crate::ui::document::KeyValueDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    pub(in crate::ui::views::workspace) fn close_tabs_batch(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        selector: impl FnOnce(
            &[crate::ui::document::Tab],
            crate::ui::document::DocumentId,
        ) -> Vec<crate::ui::document::DocumentId>,
        reference_id: crate::ui::document::DocumentId,
    ) {
        let ids = selector(self.tab_manager.read(cx).documents(), reference_id);

        for doc_id in ids {
            self.close_tab(doc_id, window, cx);
        }
    }

    pub(in crate::ui::views::workspace) fn close_tab(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.cleanup_empty_script(doc_id, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.close(doc_id, cx);
        });
    }

    /// Closes the active tab.
    ///
    /// If the tab has unsaved changes, opens `ModalUnsavedChanges` instead of
    /// closing immediately. The modal's subscription in `Workspace::new` handles
    /// the final close/save after the user decides.
    pub(in crate::ui::views::workspace) fn close_active_tab(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(doc_id) = self.tab_manager.read(cx).active_id() else {
            return;
        };

        let dirty_summaries = self.tab_manager.read(cx).dirty_summaries(cx);
        let this_doc_dirty = dirty_summaries
            .iter()
            .find(|(id, _)| *id == doc_id)
            .cloned();

        if let Some((id, summary)) = this_doc_dirty {
            let doc_name = self
                .tab_manager
                .read(cx)
                .document(doc_id)
                .map(|d| d.tab_title(cx))
                .unwrap_or_else(|| "Untitled".to_string());

            use crate::ui::overlays::modals::{DirtySummaryEntry, UnsavedChangesRequest};
            let req = UnsavedChangesRequest {
                entries: vec![DirtySummaryEntry {
                    id,
                    name: doc_name,
                    summary,
                }],
            };
            self.modal_unsaved_changes.update(cx, |modal, cx| {
                modal.open(req, cx);
            });
        } else {
            self.close_tab(doc_id, window, cx);
        }
    }

    /// Deletes the backing file for empty file-backed scripts about to be closed.
    fn cleanup_empty_script(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        cx: &mut Context<Self>,
    ) {
        let empty_script_path = self
            .tab_manager
            .read(cx)
            .document(doc_id)
            .and_then(|tab| tab.is_file_backed_empty(cx));

        if let Some(path) = empty_script_path {
            self.app_state.update(cx, |state, cx| {
                if let Some(dir) = state.scripts_directory_mut()
                    && dir.delete(&path).is_ok()
                {
                    cx.emit(AppStateChanged);
                }
            });
        }
    }
}
