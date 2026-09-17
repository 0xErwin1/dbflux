use super::*;
use crate::ui::document::pane::CloseDisposition;
use crate::ui::labels::{
    NoActiveConnectionKind, documents_default_title, documents_no_active_connection_message,
};

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
                let message = documents_no_active_connection_message(NoActiveConnectionKind::Table);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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
                let message =
                    documents_no_active_connection_message(NoActiveConnectionKind::Collection);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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
                let message =
                    documents_no_active_connection_message(NoActiveConnectionKind::EventSource);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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
                let message =
                    documents_no_active_connection_message(NoActiveConnectionKind::KeyValueDb);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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

    /// Opens the searchable buckets table for an object-storage connection
    /// root, or focuses the existing one.
    pub(in crate::ui::views::workspace) fn open_object_store_buckets_document(
        &mut self,
        profile_id: uuid::Uuid,
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
                &crate::ui::document::DocumentKey::ObjectStoreBucketsRoot { profile_id },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                let message = documents_no_active_connection_message(
                    NoActiveConnectionKind::ObjectStorageAccount,
                );
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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
            crate::ui::document::BucketsTableDocument::new(
                profile_id,
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let pane = crate::ui::document::BucketsTableDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Opens the prefix/object tree browser for a single bucket, or focuses
    /// the existing one for that `(profile_id, bucket)` pair. Reached from
    /// the buckets table's Enter-on-row action and the sidebar's
    /// `OpenObjectStoreBucket` event.
    pub(in crate::ui::views::workspace) fn open_object_browser(
        &mut self,
        profile_id: uuid::Uuid,
        bucket: String,
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
                &crate::ui::document::DocumentKey::ObjectBrowser {
                    profile_id,
                    bucket: bucket.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                let message =
                    documents_no_active_connection_message(NoActiveConnectionKind::Bucket);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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
            crate::ui::document::ObjectBrowserDocument::new(
                profile_id,
                bucket,
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let pane = crate::ui::document::ObjectBrowserDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Opens one object-store text object in its own editor tab, or focuses
    /// the existing tab for that `(profile_id, bucket, key)` triple. Reached
    /// from the object browser's "Open in editor" header button and its row
    /// context menu, both drained generically in `render.rs`.
    pub(in crate::ui::views::workspace) fn open_object_editor(
        &mut self,
        profile_id: uuid::Uuid,
        request: crate::ui::document::ObjectEditorRequest,
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
                &crate::ui::document::DocumentKey::ObjectEditor {
                    profile_id,
                    bucket: request.bucket.clone(),
                    key: request.key.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                let message =
                    documents_no_active_connection_message(NoActiveConnectionKind::Object);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
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
            crate::ui::document::ObjectEditorDocument::new(
                profile_id,
                request.bucket,
                request.key,
                request.on_saved,
                self.app_state.clone(),
                cx,
            )
        });
        let pane = crate::ui::document::ObjectEditorDocument::into_pane(doc, cx);

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

    /// Routes one close through the document's own close policy.
    ///
    /// Every tab-close route funnels here — the close button, a middle-click,
    /// the context menu, batch closes, and the active-tab command. A document
    /// that persists its pending edits as part of closing reports `Deferred`,
    /// keeps its tab open, and asks the workspace to close only once the write
    /// lands; nothing is ever removed over unlanded edits. A document without a
    /// close policy reports `CloseNow` and keeps its existing behaviour.
    ///
    /// Returns `true` when the close was accepted (the tab is gone, or its flush
    /// is in flight and will close it), `false` when the tab stayed open.
    pub(in crate::ui::views::workspace) fn close_tab(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let disposition = self.tab_manager.update(cx, |manager, cx| {
            manager
                .document(doc_id)
                .map(|tab| tab.as_pane().resolve_close(window, cx))
        });

        match disposition {
            Some(CloseDisposition::CloseNow) => {
                self.close_tab_now(doc_id, window, cx);
                true
            }
            Some(CloseDisposition::Deferred) => true,
            Some(CloseDisposition::KeepOpen) | None => false,
        }
    }

    /// Removes a tab whose own close policy agreed it may go now.
    fn close_tab_now(
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
    /// A document that decides its own close policy (a code document) flushes
    /// its pending edits and closes without the confirmation dialog: closing
    /// persists first, so there is nothing to discard. A document that requires
    /// an explicit user save still gets `ModalUnsavedChanges`, and this returns
    /// `false` so the caller does not refocus the document over the
    /// confirmation. Returns `true` when the close was accepted.
    pub(in crate::ui::views::workspace) fn close_active_tab(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(doc_id) = self.tab_manager.read(cx).active_id() else {
            return true;
        };

        // The unsaved-changes dialog guards documents whose pending edits need
        // an explicit save. A document with its own close policy persists them
        // itself, so the dialog would only get in the way.
        let decides_own_close = self
            .tab_manager
            .read(cx)
            .document(doc_id)
            .is_some_and(|tab| tab.as_pane().has_close_policy());

        let dirty_summaries = self.tab_manager.read(cx).dirty_summaries(cx);
        let this_doc_dirty = dirty_summaries
            .iter()
            .find(|(id, _)| *id == doc_id)
            .cloned();

        if let Some((id, summary)) = this_doc_dirty
            && !decides_own_close
        {
            let doc_name = self
                .tab_manager
                .read(cx)
                .document(doc_id)
                .map(|d| d.tab_title(cx))
                .unwrap_or_else(documents_default_title);

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
            // Take focus off any editor input so Enter/Escape resolve through
            // the ConfirmModal keymap instead of editing the buffer behind the
            // modal.
            self.focus_handle.focus(window, cx);
            false
        } else {
            self.close_tab(doc_id, window, cx)
        }
    }

    /// Deletes the backing file of an empty file-backed script about to be closed,
    /// but only while the file still holds exactly what dbflux last loaded or
    /// wrote there.
    ///
    /// The pane reports a path only when the empty buffer and the document's own
    /// recorded baseline for that path agree with the current on-disk bytes, so a
    /// file another process changed into, a file with no trustworthy baseline,
    /// a baseline belonging to another path, and an unreadable file are all
    /// kept. Deleting on buffer emptiness alone could destroy foreign content; a
    /// leftover empty script is the safer failure.
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

#[cfg(test)]
mod tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::ui::document::{CodeDocument, DocumentId, Tab};
    use crate::ui::views::workspace::Workspace;
    use dbflux_core::QueryLanguage;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn new_workspace(
        cx: &mut TestAppContext,
    ) -> (
        Entity<Workspace>,
        Entity<AppStateEntity>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
        let workspace_ref = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
            workspace_ref.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        });

        let workspace = holder
            .borrow()
            .clone()
            .expect("workspace should be created");
        (workspace, app_state, window)
    }

    /// The scripts root the workspace cleanup deletes from.
    fn scripts_root(
        window: &mut VisualTestContext,
        app_state: &Entity<AppStateEntity>,
    ) -> std::path::PathBuf {
        window.update(|_, cx| {
            app_state
                .read(cx)
                .scripts_directory()
                .expect("the test environment resolves a scripts directory")
                .root_path()
                .to_path_buf()
        })
    }

    /// A uniquely named script inside the real scripts root, so the cleanup's
    /// delete (which refuses paths outside the root) can run without racing a
    /// concurrently running test.
    fn unique_root_script(root: &std::path::Path, label: &str) -> std::path::PathBuf {
        root.join(format!("dbflux-t2c-{label}-{}.sql", uuid::Uuid::new_v4()))
    }

    /// Opens a real file-backed code tab whose buffer is empty and whose on-disk
    /// bytes are `on_disk`. `baseline` records the bytes the document owns for a
    /// path the way opening, creating, or a landed write would; `None` leaves the
    /// document without a trustworthy baseline.
    fn open_empty_file_backed_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
        path: &std::path::Path,
        on_disk: &str,
        baseline: Option<(&std::path::Path, &str)>,
    ) -> DocumentId {
        std::fs::write(path, on_disk).expect("seed the backing file");
        let path = path.to_path_buf();
        let baseline =
            baseline.map(|(baseline_path, bytes)| (baseline_path.to_path_buf(), bytes.to_string()));

        let document = window.update(|window, cx| {
            cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                )
                .with_path(path.clone());
                document.set_content("", window, cx);
                if let Some((baseline_path, bytes)) = baseline {
                    document.seed_file_baseline(baseline_path, bytes);
                }
                document
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = CodeDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
            });
        });

        document_id
    }

    fn open_untitled_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
    ) -> DocumentId {
        let document = window.update(|window, cx| {
            cx.new(|cx| {
                let mut document = CodeDocument::new_with_language(
                    app_state.clone(),
                    None,
                    QueryLanguage::Sql,
                    window,
                    cx,
                );
                document.set_content("", window, cx);
                document
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = CodeDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
            });
        });

        document_id
    }

    fn close_tab(window: &mut VisualTestContext, workspace: &Entity<Workspace>, id: DocumentId) {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.close_tab(id, window, cx);
            });
        });
        window.run_until_parked();
    }

    /// The accepted cleanup: an empty script whose file still holds exactly the
    /// bytes dbflux last wrote is deleted when its tab closes.
    #[gpui::test]
    fn an_empty_script_still_holding_dbflux_own_bytes_is_deleted_on_close(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let root = scripts_root(window, &app_state);
        let path = unique_root_script(&root, "owned");

        let id = open_empty_file_backed_tab(
            window,
            &workspace,
            &app_state,
            &path,
            "",
            Some((&path, "")),
        );
        close_tab(window, &workspace, id);

        let deleted = !path.exists();
        if path.exists() {
            std::fs::remove_file(&path).ok();
        }

        assert!(
            deleted,
            "an empty script dbflux still owns must be cleaned up on close"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "the closed tab is removed"
            );
        });
    }

    /// The defect: the buffer is empty, but another process wrote content into
    /// the file dbflux had last written empty. Deleting on buffer emptiness alone
    /// destroys those foreign bytes; cleanup must keep the file.
    #[gpui::test]
    fn an_empty_buffer_whose_file_changed_outside_dbflux_is_not_deleted(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let root = scripts_root(window, &app_state);
        let path = unique_root_script(&root, "foreign");

        let id = open_empty_file_backed_tab(
            window,
            &workspace,
            &app_state,
            &path,
            "",
            Some((&path, "")),
        );
        // Another process writes into the file after dbflux's own empty write.
        std::fs::write(&path, "FOREIGN;").expect("the external write must succeed");

        close_tab(window, &workspace, id);

        let survived = std::fs::read_to_string(&path).ok();
        if path.exists() {
            std::fs::remove_file(&path).ok();
        }

        assert_eq!(
            survived.as_deref(),
            Some("FOREIGN;"),
            "closing an empty buffer must not delete a file that changed outside dbflux"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "the tab still closes; only the file is kept"
            );
        });
    }

    /// With no trustworthy baseline there is nothing that authorizes deletion, so
    /// cleanup fails closed and keeps the file.
    #[gpui::test]
    fn an_empty_buffer_without_a_trustworthy_baseline_is_not_deleted(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let root = scripts_root(window, &app_state);
        let path = unique_root_script(&root, "no-baseline");

        let id = open_empty_file_backed_tab(window, &workspace, &app_state, &path, "", None);
        close_tab(window, &workspace, id);

        let survived = path.exists();
        if survived {
            std::fs::remove_file(&path).ok();
        }

        assert!(
            survived,
            "cleanup without a baseline must fail closed and keep the file"
        );
    }

    /// A baseline recorded for another path never authorizes deleting this file,
    /// even when the bytes happen to match.
    #[gpui::test]
    fn an_empty_buffer_whose_baseline_belongs_to_another_path_is_not_deleted(
        cx: &mut TestAppContext,
    ) {
        let (workspace, app_state, window) = new_workspace(cx);
        let root = scripts_root(window, &app_state);
        let path = unique_root_script(&root, "other-path");
        let other_path = unique_root_script(&root, "other-path-source");

        let id = open_empty_file_backed_tab(
            window,
            &workspace,
            &app_state,
            &path,
            "",
            Some((&other_path, "")),
        );
        close_tab(window, &workspace, id);

        let survived = path.exists();
        if survived {
            std::fs::remove_file(&path).ok();
        }

        assert!(
            survived,
            "a baseline from another path must not authorize deleting this file"
        );
    }

    /// A file deleted outside dbflux cannot be read, so cleanup fails closed and
    /// never recreates it.
    ///
    /// The guard is the seam itself: with the file gone the document cannot
    /// prove it owns the missing path, so it presents nothing to delete. That
    /// assertion, not the surviving absence, is what distinguishes this test from
    /// a buffer-only seam — which would still report `Some(path)` here.
    #[gpui::test]
    fn an_empty_buffer_whose_file_was_deleted_outside_dbflux_is_not_recreated(
        cx: &mut TestAppContext,
    ) {
        let (workspace, app_state, window) = new_workspace(cx);
        let root = scripts_root(window, &app_state);
        let path = unique_root_script(&root, "deleted");

        let id = open_empty_file_backed_tab(
            window,
            &workspace,
            &app_state,
            &path,
            "",
            Some((&path, "")),
        );
        std::fs::remove_file(&path).expect("the external delete must succeed");

        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .and_then(|tab| tab.is_file_backed_empty(cx))
                    .is_none(),
                "a file deleted outside dbflux must present nothing for cleanup to delete"
            );
        });

        close_tab(window, &workspace, id);

        assert!(
            !path.exists(),
            "cleanup must not recreate a file that was deleted outside dbflux"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "the tab still closes; only the missing file is left alone"
            );
        });
    }

    /// A brand-new, never-edited empty script is still cleaned up when the user
    /// closes it unedited.
    ///
    /// This reaches the production state `new_query_tab` produces — a
    /// file-backed, clean, empty buffer whose file dbflux created and seeded as
    /// its own baseline — but the backing file carries a test-scoped unique name
    /// inside the scripts root instead of the user-plausible `Query N.sql`. That
    /// keeps the coverage without racing the developer's file namespace: the real
    /// flow's `next_available_name` → `create_file` is a TOCTOU window when
    /// several worktrees run this suite concurrently.
    #[gpui::test]
    fn a_brand_new_empty_script_is_cleaned_up_when_closed_unedited(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let root = scripts_root(window, &app_state);
        let path = unique_root_script(&root, "brand-new");

        let id = open_empty_file_backed_tab(
            window,
            &workspace,
            &app_state,
            &path,
            "",
            Some((&path, "")),
        );

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.read(cx);
            let tab = manager
                .document(id)
                .expect("the brand-new script tab is open");
            assert!(
                path.exists(),
                "the brand-new script's backing file exists before close"
            );
            assert!(
                tab.is_file_backed_empty(cx).is_some(),
                "a brand-new, never-edited empty script still owned by dbflux is deletable"
            );
        });

        close_tab(window, &workspace, id);

        let deleted = !path.exists();
        if path.exists() {
            std::fs::remove_file(&path).ok();
        }

        assert!(
            deleted,
            "an untouched brand-new empty script must still be cleaned up on close"
        );
        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "the closed tab is removed"
            );
        });
    }

    /// A scratch/untitled document has no scripts-root file, so the seam the
    /// cleanup reads reports nothing to delete and the close leaves the scripts
    /// directory untouched.
    #[gpui::test]
    fn a_scratch_document_is_unaffected_by_cleanup(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);

        let id = open_untitled_tab(window, &workspace, &app_state);

        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .and_then(|tab| tab.is_file_backed_empty(cx))
                    .is_none(),
                "a scratch document presents nothing for cleanup to delete"
            );
        });

        close_tab(window, &workspace, id);

        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document(id)
                    .is_none(),
                "the scratch tab closes without a file to delete"
            );
        });
    }
}
