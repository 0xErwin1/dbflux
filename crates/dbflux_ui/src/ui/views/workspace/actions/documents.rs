use super::*;
use crate::ui::document::file_format::{FileDocumentFormat, file_document_format};
use crate::ui::document::pane::CloseDisposition;
use crate::ui::document::{FileDocumentKey, ObjectSavedCallback};
use crate::ui::labels::{
    NoActiveConnectionKind, documents_default_title, documents_no_active_connection_message,
};
use dbflux_ui_base::app_state_entity::rescan_script_root_in_background;

impl Workspace {
    /// Opens a data file or object in the document for its format, or focuses
    /// the tab that already shows it. This is the one place that maps a
    /// [`FileDocumentFormat`] to a document; callers check
    /// [`file_document_format`] first and route here.
    ///
    /// `on_object_saved` is told an object's key after each save that
    /// replaces it.
    pub(in crate::ui::views::workspace) fn open_file_document(
        &mut self,
        file: FileDocumentKey,
        on_object_saved: Option<ObjectSavedCallback>,
        cx: &mut Context<Self>,
    ) {
        let name = match &file {
            FileDocumentKey::Local { path } => path.as_path(),
            FileDocumentKey::Object { key, .. } => std::path::Path::new(key),
        };

        match file_document_format(name) {
            Some(FileDocumentFormat::Delimited) => {
                self.open_delimited_file(file, on_object_saved, cx);
            }

            // Read-only: an object is never replaced, so nothing is told of
            // a save.
            Some(FileDocumentFormat::Parquet) => {
                self.open_parquet_file(file, cx);
            }

            Some(FileDocumentFormat::Spreadsheet) => {
                self.open_spreadsheet_file(file, on_object_saved, cx);
            }

            None => {
                log::warn!(
                    "Not opening {:?} as a file document: its format is not recognized",
                    name
                );
            }
        }
    }

    /// Opens a table in a new DataDocument tab, or focuses the existing one.
    pub(in crate::ui::views::workspace) fn open_table_document(
        &mut self,
        profile_id: uuid::Uuid,
        table: dbflux_core::TableRef,
        database: Option<String>,
        placement: TabPlacement,
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

        match decide_open_document_with_placement(has_connection, existing_id, placement) {
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

    /// Opens the table a foreign-key value points at, positioned on the
    /// referenced row: reuses (or opens) the table's tab, then replaces its
    /// filter with an equality on the key column. A tab whose rows an
    /// applied builder query owns keeps that query — the referenced row
    /// opens in a tab of its own, since the equality would land in a filter
    /// input the builder hides. A missing connection is reported by
    /// [`Self::open_table_document`]; a tab that cannot be found afterwards
    /// is left alone.
    pub(in crate::ui::views::workspace) fn open_table_at_referenced_row(
        &mut self,
        target: crate::ui::document::ReferencedRowTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let key = crate::ui::document::DocumentKey::Table {
            profile_id: target.profile_id,
            database: target.database.clone(),
            table: target.table.clone(),
        };

        let builder_tab = {
            let manager = self.tab_manager.read(cx);
            manager
                .find_by_key(&key, cx)
                .and_then(|id| manager.document(id))
                .is_some_and(|tab| tab.as_pane().has_applied_builder_query(cx))
        };

        self.open_table_document(
            target.profile_id,
            target.table.clone(),
            target.database.clone(),
            if builder_tab {
                TabPlacement::NewTab
            } else {
                TabPlacement::ReuseExisting
            },
            window,
            cx,
        );

        // With a tab opened beside the builder one, `find_by_key` still
        // returns the builder tab (the first match), so the fresh tab is
        // addressed by the activation opening it produced.
        let doc_id = if builder_tab {
            self.tab_manager.read(cx).active_id()
        } else {
            self.tab_manager.read(cx).find_by_key(&key, cx)
        };

        if let Some(doc_id) = doc_id {
            self.tab_manager.update(cx, |mgr, cx| {
                if let Some(tab) = mgr.document(doc_id) {
                    tab.as_pane().apply_table_filter(
                        &target.key_column,
                        &target.key_value,
                        window,
                        cx,
                    );
                }
            });
        }
    }

    pub(in crate::ui::views::workspace) fn open_collection_document(
        &mut self,
        profile_id: uuid::Uuid,
        collection: dbflux_core::CollectionRef,
        placement: TabPlacement,
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

        match decide_open_document_with_placement(has_connection, existing_id, placement) {
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
        placement: TabPlacement,
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

        match decide_open_document_with_placement(has_connection, existing_id, placement) {
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

    /// Opens the document of one secret store, or focuses the store's
    /// existing tab. A `path` of a hierarchical store opens at that path, and
    /// navigates an existing tab to it.
    pub(in crate::ui::views::workspace) fn open_secret_store_document(
        &mut self,
        profile_id: uuid::Uuid,
        store_id: String,
        path: Option<String>,
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
                &crate::ui::document::DocumentKey::SecretStore {
                    profile_id,
                    store_id: store_id.clone(),
                },
                cx,
            )
        } else {
            None
        };

        match decide_open_document(has_connection, existing_id) {
            OpenDocumentDecision::ErrorNoConnection => {
                let message =
                    documents_no_active_connection_message(NoActiveConnectionKind::SecretStore);
                Toast::error(message.clone())
                    .meta_right(now_hms())
                    .action(copy_action(message))
                    .push(cx);
                return;
            }
            OpenDocumentDecision::FocusExisting(id) => {
                self.tab_manager.update(cx, |mgr, cx| {
                    if path.is_some()
                        && let Some(tab) = mgr.documents().iter().find(|tab| tab.id() == id)
                    {
                        tab.navigate_secret_path(path.clone(), cx);
                    }
                    mgr.activate(id, cx);
                });
                return;
            }
            OpenDocumentDecision::OpenNew => {}
        }

        let doc = cx.new(|cx| {
            crate::ui::document::SecretStoreDocument::new(
                profile_id,
                store_id,
                path,
                self.app_state.clone(),
                window,
                cx,
            )
        });
        let pane = crate::ui::document::SecretStoreDocument::into_pane(doc, cx);

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
    /// context menu, both drained generically in `render.rs`. A CSV or TSV
    /// object opens in the delimited document instead.
    pub(in crate::ui::views::workspace) fn open_object_editor(
        &mut self,
        profile_id: uuid::Uuid,
        request: crate::ui::document::ObjectEditorRequest,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // A data file object opens as a table that reads it in pages, so it
        // is not held to the preview size limit the object editor applies.
        let opens_as_file_document =
            file_document_format(std::path::Path::new(&request.key)).is_some();

        if opens_as_file_document {
            self.open_file_document(
                FileDocumentKey::Object {
                    profile_id,
                    bucket: request.bucket,
                    key: request.key,
                },
                Some(request.on_saved),
                cx,
            );
            return;
        }

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

    /// Closes the tabs a batch gesture selects, through the same funnel as a
    /// single close.
    ///
    /// `select` receives every open document id in tab order and returns the
    /// ones to close, so each gesture keeps its own selection rule
    /// ([`crate::ui::document::TabManager::ids_to_close_others`] and siblings)
    /// while the closing itself has one home: [`Self::close_tabs`].
    pub(in crate::ui::views::workspace) fn close_tabs_by(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        select: impl FnOnce(&[crate::ui::document::DocumentId]) -> Vec<crate::ui::document::DocumentId>,
    ) {
        let ids = select(&self.tab_manager.read(cx).document_ids());
        self.close_tabs(window, cx, ids);
    }

    /// Closes a set of tabs, asking about the ones that need a decision.
    ///
    /// Documents that must be asked about are asked about in ONE confirmation
    /// carrying every one of them: closing a batch must not open a dialog per
    /// tab and overwrite the previous request. None of the asked-about documents
    /// is closed here, and the rest still close through [`Self::close_tab`].
    fn close_tabs(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        ids: Vec<crate::ui::document::DocumentId>,
    ) {
        let ids: Vec<crate::ui::document::DocumentId> = ids
            .into_iter()
            .filter(|doc_id| self.commit_pending_input(*doc_id, cx))
            .collect();

        let needs_confirmation =
            self.documents_requiring_close_confirmation(ids.iter().copied(), cx);

        if !needs_confirmation.is_empty() {
            let asked: Vec<crate::ui::document::DocumentId> =
                needs_confirmation.iter().map(|entry| entry.id).collect();
            self.ask_before_closing(needs_confirmation, window, cx);

            for doc_id in ids {
                if !asked.contains(&doc_id) {
                    self.close_tab(doc_id, window, cx);
                }
            }

            return;
        }

        for doc_id in ids {
            self.close_tab(doc_id, window, cx);
        }
    }

    /// Commits the input `doc_id` still holds in an open editor, so the close
    /// gate counts it as a pending change instead of closing over it.
    ///
    /// Returns `false` when the document kept input it could not commit; its
    /// tab must then stay open.
    fn commit_pending_input(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        cx: &mut Context<Self>,
    ) -> bool {
        self.tab_manager.update(cx, |manager, cx| {
            manager
                .document(doc_id)
                .is_none_or(|tab| tab.as_pane().commit_pending_input(cx))
        })
    }

    /// Returns the pending-edit entries of the documents in `ids` that must be
    /// asked about before they close: they hold edits and they do not persist
    /// them by closing.
    ///
    /// A document whose close policy applies is never listed, because closing
    /// already saves it. An untitled code buffer has no file to write, so only
    /// the user can decide whether its edits are kept or dropped.
    fn documents_requiring_close_confirmation(
        &self,
        ids: impl IntoIterator<Item = crate::ui::document::DocumentId>,
        cx: &App,
    ) -> Vec<crate::ui::overlays::modals::DirtySummaryEntry> {
        use crate::ui::overlays::modals::DirtySummaryEntry;

        let manager = self.tab_manager.read(cx);
        let dirty = manager.dirty_summaries(cx);

        ids.into_iter()
            .filter_map(|doc_id| {
                let (_, summary, action) = dirty.iter().find(|(id, _, _)| *id == doc_id)?;

                let decides_own_close = manager
                    .document(doc_id)
                    .is_some_and(|tab| tab.as_pane().has_close_policy(cx));

                if decides_own_close {
                    return None;
                }

                let name = manager
                    .document(doc_id)
                    .map(|d| d.tab_title(cx))
                    .unwrap_or_else(documents_default_title);

                Some(DirtySummaryEntry {
                    id: doc_id,
                    name,
                    summary: summary.clone(),
                    action: *action,
                })
            })
            .collect()
    }

    /// Opens the unsaved-changes confirmation for the documents that need a
    /// decision, and takes the keyboard so Enter and Escape resolve through the
    /// ConfirmModal keymap instead of editing the buffer behind the modal.
    fn ask_before_closing(
        &mut self,
        entries: Vec<crate::ui::overlays::modals::DirtySummaryEntry>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::overlays::modals::UnsavedChangesRequest;

        // The prompt now guards this close, not the quit it showed for. A
        // quit whose saves run is not the prompt's, and stays pending.
        if matches!(self.pending_quit, Some(PendingQuit::Asking { .. })) {
            self.pending_quit = None;
        }

        self.modal_unsaved_changes.update(cx, |modal, cx| {
            modal.open(UnsavedChangesRequest { entries }, cx);
        });
        self.focus_handle.focus(window, cx);
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
        // A document that cannot persist its own pending edits is asked about
        // before anything is written or removed. The gate lives here rather than
        // at one entry point because every close route reaches this funnel, and
        // a route that skipped it would force a Save As dialog instead of
        // asking. A value still in an open editor is committed first, so the
        // gate sees it.
        if !self.commit_pending_input(doc_id, cx) {
            return false;
        }

        let needs_confirmation = self.documents_requiring_close_confirmation([doc_id], cx);

        if !needs_confirmation.is_empty() {
            self.ask_before_closing(needs_confirmation, window, cx);
            return false;
        }

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

    /// Removes a tab directly, without consulting its close policy.
    ///
    /// Used by the funnel once a document's own policy agreed it may go, and by
    /// the discard outcome of the unsaved-changes confirmation: discarding means
    /// the tab goes without saving, so it must not re-enter the ask-first gate
    /// in [`Self::close_tab`].
    pub(in crate::ui::views::workspace) fn close_tab_now(
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
    /// Every route, this one included, goes through [`Self::close_tab`], which
    /// asks about the documents that cannot persist their own pending edits and
    /// otherwise defers to the document's own close policy. Returns `true` when
    /// the close was accepted.
    pub(in crate::ui::views::workspace) fn close_active_tab(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(doc_id) = self.tab_manager.read(cx).active_id() else {
            return true;
        };

        self.close_tab(doc_id, window, cx)
    }

    /// Deletes the backing file of an empty file-backed script about to be closed,
    /// but only while the file still holds exactly what dbflux last loaded or
    /// wrote there.
    ///
    /// The pane reports a candidate without reading anything: the path, and the
    /// bytes the document last loaded or wrote. Both the verification and the
    /// removal run on the background executor — reading the whole file, and the
    /// directory walk that follows the removal, block for as long as the disk takes
    /// to answer, and a close gesture must not inherit that wait. The tree is then
    /// rescanned in the background too, and adopted only if nothing changed in
    /// that folder meanwhile.
    ///
    /// Deleting on buffer emptiness alone could destroy foreign content, and a
    /// leftover empty script is the safer failure, so every uncertain case keeps
    /// the file: a change made outside dbflux, a file that is already gone, a file
    /// that cannot be read, and a document with no trustworthy baseline for the
    /// path (which the pane reports as no candidate at all).
    ///
    /// Only scripts in the managed scripts folder are candidates. A file opened
    /// from anywhere else, an external scripts folder included, belongs to the
    /// user and is kept even when it was emptied here.
    fn cleanup_empty_script(
        &mut self,
        doc_id: crate::ui::document::DocumentId,
        cx: &mut Context<Self>,
    ) {
        let Some(cleanup) = self
            .tab_manager
            .read(cx)
            .document(doc_id)
            .and_then(|tab| tab.pending_empty_script_cleanup(cx))
        else {
            return;
        };

        let Some(root) = self
            .app_state
            .read(cx)
            .scripts_directory()
            .map(|dir| dir.root_path().to_path_buf())
        else {
            return;
        };

        if !cleanup.path.starts_with(&root) {
            return;
        }

        let app_state = self.app_state.clone();
        let removed_path = cleanup.path.clone();
        cx.spawn(async move |_this, cx| {
            let removed = cx
                .background_executor()
                .spawn(async move {
                    match dbflux_core::ScriptsDirectory::remove_if_unchanged(
                        &root,
                        &cleanup.path,
                        &cleanup.expected_bytes,
                    ) {
                        Ok(removed) => removed,
                        Err(e) => {
                            log::warn!(
                                "Failed to remove the emptied script {}: {e}",
                                cleanup.path.display()
                            );
                            false
                        }
                    }
                })
                .await;

            if !removed {
                return;
            }

            cx.update(|cx| rescan_script_root_in_background(&app_state, &removed_path, cx));
        })
        .detach();
    }

    /// Opens a schema visualization document for a table.
    pub(in crate::ui::views::workspace) fn open_schema_viz_document(
        &mut self,
        profile_id: uuid::Uuid,
        database: Option<String>,
        schema: Option<String>,
        table: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{DocumentKey, SchemaVizDocument, SchemaVizMode};

        // Deduplication: one focused diagram per (profile_id, database, schema, table).
        let existing_id = self.tab_manager.read(cx).find_by_key(
            &DocumentKey::SchemaViz {
                profile_id,
                database: database.clone(),
                schema: schema.clone(),
                table: Some(table.clone()),
            },
            cx,
        );

        if let Some(existing_id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            return;
        }

        let mode = SchemaVizMode::Focused {
            table: table.clone(),
            schema: schema.clone(),
        };

        let doc = cx.new(|cx| {
            SchemaVizDocument::new(
                profile_id,
                database.clone(),
                mode,
                self.app_state.clone(),
                window,
                cx,
            )
        });

        let pane = SchemaVizDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        log::info!(
            "Opened schema viz document: {} (profile={}, db={:?}, schema={:?})",
            table,
            profile_id,
            database.as_deref(),
            schema.as_deref()
        );
    }

    /// Opens a global schema visualization document for a database.
    pub(in crate::ui::views::workspace) fn open_global_schema_viz_document(
        &mut self,
        profile_id: uuid::Uuid,
        database: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{DocumentKey, SchemaVizDocument, SchemaVizMode};

        // Deduplication: one Global diagram per (profile_id, database).
        let existing_id = self.tab_manager.read(cx).find_by_key(
            &DocumentKey::SchemaViz {
                profile_id,
                database: Some(database.clone()),
                schema: None,
                table: None,
            },
            cx,
        );

        if let Some(existing_id) = existing_id {
            self.tab_manager
                .update(cx, |mgr, cx| mgr.activate(existing_id, cx));
            return;
        }

        let doc = cx.new(|cx| {
            SchemaVizDocument::new(
                profile_id,
                Some(database.clone()),
                SchemaVizMode::Global,
                self.app_state.clone(),
                window,
                cx,
            )
        });

        let pane = SchemaVizDocument::into_pane(doc, cx);
        self.tab_manager
            .update(cx, |mgr, cx| mgr.open(Tab::Pane(Box::new(pane)), cx));

        log::info!(
            "Opened global schema viz for profile={} db={}",
            profile_id,
            database
        );
    }

    /// Opens (or activates) the native console tab of `database` on
    /// `profile_id`. Does nothing when the connection is closed or its driver
    /// offers no console.
    pub(in crate::ui::views::workspace) fn open_console_document(
        &mut self,
        profile_id: uuid::Uuid,
        database: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::{ConsoleDocument, DocumentKey};

        let existing_id = self.tab_manager.read(cx).find_by_key(
            &DocumentKey::Console {
                profile_id,
                database: database.clone(),
            },
            cx,
        );

        if let Some(existing_id) = existing_id {
            self.tab_manager
                .update(cx, |mgr, cx| mgr.activate(existing_id, cx));
            self.tab_manager
                .update(cx, |mgr, cx| mgr.focus_active(window, cx));
            return;
        }

        let Some(profile) = self
            .app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .and_then(|connected| connected.connection.metadata().native_console())
        else {
            return;
        };

        let app_state = self.app_state.clone();
        let doc =
            cx.new(|cx| ConsoleDocument::new(profile_id, database, profile, app_state, window, cx));

        let pane = ConsoleDocument::into_pane(doc, cx);
        self.tab_manager
            .update(cx, |mgr, cx| mgr.open(Tab::Pane(Box::new(pane)), cx));
        self.set_focus(FocusTarget::Document, window, cx);
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::ui::document::{CodeDocument, DataDocument, DocumentId, DocumentKey, Tab};
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

    /// A file deleted outside dbflux is never recreated by cleanup.
    ///
    /// The pane cannot know the file is gone without reading it, so it reports the
    /// candidate and leaves the verdict to the verification step: the background
    /// read finds no file, and a document that cannot prove it still owns what is on
    /// disk keeps what it finds — here, keeps it gone. The discriminating case for a
    /// seam that stopped verifying is the sibling test that keeps a file another
    /// process wrote into; this test pins the candidate report and the outcome.
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
            let candidate = workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .and_then(|tab| tab.pending_empty_script_cleanup(cx));

            assert!(
                candidate.is_some_and(|cleanup| cleanup.path == path),
                "the pane reports the candidate; whether the file is still ours is what\
                 the background step decides"
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
                tab.pending_empty_script_cleanup(cx).is_some(),
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
                    .and_then(|tab| tab.pending_empty_script_cleanup(cx))
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

    /// Opening a referenced row on a profile without a connection must not
    /// panic: `open_table_document` reports the error through its toast and no
    /// tab is opened, so the positioning step finds nothing and stays quiet.
    #[gpui::test]
    fn opening_a_referenced_row_without_a_connection_does_not_panic(cx: &mut TestAppContext) {
        let (workspace, _app_state, window) = new_workspace(cx);

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_table_at_referenced_row(
                    crate::ui::document::ReferencedRowTarget {
                        profile_id: uuid::Uuid::new_v4(),
                        database: None,
                        table: dbflux_core::TableRef::new("orders"),
                        key_column: "id".to_string(),
                        key_value: dbflux_core::Value::Int(7),
                    },
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            assert!(
                workspace
                    .read(cx)
                    .tab_manager
                    .read(cx)
                    .document_ids()
                    .is_empty(),
                "no tab opens for a profile without a connection"
            );
        });
    }

    // ---- Referenced-row navigation versus the query builder ----

    /// Answers every structured-SELECT request, so `apply_builder_draft_spec`
    /// leaves the tab with `visual_select` set — its refresh prefers the
    /// builder query over the raw filter.
    struct ReferencedRowGenerator;

    impl dbflux_core::QueryGenerator for ReferencedRowGenerator {
        fn supported_categories(&self) -> &'static [dbflux_core::MutationCategory] {
            &[]
        }

        fn generate_mutation(
            &self,
            _mutation: &dbflux_core::MutationRequest,
        ) -> Option<dbflux_core::GeneratedQuery> {
            None
        }

        fn generate_select(
            &self,
            _spec: &dbflux_core::VisualQuerySpec,
        ) -> Result<Option<dbflux_core::SelectQuery>, dbflux_core::QueryGenError> {
            Ok(Some(dbflux_core::SelectQuery {
                sql: "SELECT * FROM public.orders".to_string(),
                params: Vec::new(),
            }))
        }
    }

    /// Connection stub for the referenced-row navigation test: SQL dialect,
    /// structured SELECT generation, and a recorded table browse.
    struct ReferencedRowConnection {
        generator: ReferencedRowGenerator,
        browses: std::sync::Mutex<Vec<dbflux_core::TableBrowseRequest>>,
    }

    impl ReferencedRowConnection {
        fn browses(&self) -> Vec<dbflux_core::TableBrowseRequest> {
            self.browses.lock().expect("browse log").clone()
        }
    }

    impl Default for ReferencedRowConnection {
        fn default() -> Self {
            Self {
                generator: ReferencedRowGenerator,
                browses: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    fn orders_page() -> dbflux_core::QueryResult {
        let column = |name: &str, is_primary_key: bool| dbflux_core::ColumnMeta {
            name: name.to_string(),
            type_name: "int4".to_string(),
            kind: dbflux_core::ColumnKind::Integer,
            nullable: false,
            is_primary_key,
        };

        dbflux_core::QueryResult::table(
            vec![column("id", true), column("customer_id", false)],
            vec![vec![dbflux_core::Value::Int(7), dbflux_core::Value::Int(8)]],
            None,
            std::time::Duration::ZERO,
        )
    }

    impl dbflux_core::Connection for ReferencedRowConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            static METADATA: std::sync::OnceLock<dbflux_core::DriverMetadata> =
                std::sync::OnceLock::new();

            METADATA.get_or_init(|| {
                dbflux_core::DriverMetadataBuilder::new(
                    "workspace-referenced-row-test",
                    "Referenced Row Test",
                    dbflux_core::DatabaseCategory::Relational,
                    dbflux_core::QueryLanguage::Sql,
                )
                .build()
            })
        }

        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
            dbflux_core::SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }

        fn ping(&self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn execute(
            &self,
            _request: &dbflux_core::QueryRequest,
        ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
            Ok(orders_page())
        }

        fn browse_table(
            &self,
            request: &dbflux_core::TableBrowseRequest,
        ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
            self.browses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(request.clone());
            Ok(orders_page())
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
            Ok(dbflux_core::SchemaSnapshot::default())
        }

        fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
            Some(&self.generator)
        }
    }

    /// Registers `connection` as the workspace's only connected profile, the
    /// way `connect_object_store` does, and returns the profile id.
    fn connect_referenced_row_profile(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        connection: std::sync::Arc<dyn dbflux_core::Connection>,
    ) -> uuid::Uuid {
        let profile_id = uuid::Uuid::new_v4();

        window.update(|_, cx| {
            let app_state = workspace.read(cx).app_state.clone();

            app_state.update(cx, |state, _| {
                let profile = dbflux_core::ConnectionProfile::new(
                    "orders",
                    dbflux_core::DbConfig::SQLite {
                        path: std::path::PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                );

                state.connections_mut().insert(
                    profile_id,
                    dbflux_core::ConnectedProfile {
                        profile,
                        connection,
                        schema: None,
                        mutation_policy: dbflux_core::MutationPolicy::default(),
                        read_only_reason: None,
                        database_schemas: Default::default(),
                        table_details: Default::default(),
                        collection_children: Default::default(),
                        schema_types: Default::default(),
                        schema_columns: Default::default(),
                        schema_indexes: Default::default(),
                        schema_foreign_keys: Default::default(),
                        schema_routines: Default::default(),
                        dependents_cache: Default::default(),
                        active_database: None,
                        redis_key_cache: Default::default(),
                        database_connections: Default::default(),
                        proxy_tunnel: None,
                    },
                );
            });
        });

        profile_id
    }

    fn orders_builder_spec() -> dbflux_core::VisualQuerySpec {
        dbflux_core::VisualQuerySpec {
            source: dbflux_core::SourceTable {
                schema: Some("public".to_string()),
                table: "orders".to_string(),
                alias: "orders".to_string(),
            },
            projection: dbflux_core::Projection::All,
            joins: vec![],
            filter: None,
            group_by: vec![],
            aggregates: vec![],
            having: None,
            sort: vec![],
            limit: Some(100),
            offset: 0,
        }
    }

    /// A referenced row opened into a tab whose rows an applied builder query
    /// owns lands in a tab of its own: the builder tab keeps its query (the
    /// equality would land in a filter input the builder hides), and the
    /// fresh tab browses under the dialect-quoted equality from the first
    /// page.
    #[gpui::test]
    fn a_referenced_row_opens_a_fresh_tab_when_the_existing_one_runs_a_builder_query(
        cx: &mut TestAppContext,
    ) {
        let (workspace, _app_state, window) = new_workspace(cx);

        let connection = std::sync::Arc::new(ReferencedRowConnection::default());
        let profile_id = connect_referenced_row_profile(window, &workspace, connection.clone());

        let table = dbflux_core::TableRef::with_schema("public", "orders");

        // The tab already showing the table runs an applied builder query.
        window.update(|window, cx| {
            let app_state = workspace.read(cx).app_state.clone();
            let document = cx.new(|cx| {
                DataDocument::new_for_table(
                    profile_id,
                    table.clone(),
                    Some("app".to_string()),
                    app_state,
                    window,
                    cx,
                )
            });
            let grid = document.read(cx).data_grid_for_tests().clone();
            grid.update(cx, |grid, cx| {
                grid.apply_builder_draft_spec(orders_builder_spec(), cx)
            });

            let pane = DataDocument::into_pane(document, cx);
            workspace.update(cx, |workspace, cx| {
                workspace
                    .tab_manager
                    .update(cx, |mgr, cx| mgr.open(Tab::Pane(Box::new(pane)), cx));
            });
        });
        window.run_until_parked();

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_table_at_referenced_row(
                    crate::ui::document::ReferencedRowTarget {
                        profile_id,
                        database: Some("app".to_string()),
                        table: table.clone(),
                        key_column: "id".to_string(),
                        key_value: dbflux_core::Value::Int(7),
                    },
                    window,
                    cx,
                );
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            let manager = workspace.read(cx).tab_manager.read(cx);
            assert_eq!(
                manager.document_ids().len(),
                2,
                "the referenced row opens a tab of its own"
            );

            let builder_tab = manager
                .find_by_key(
                    &DocumentKey::Table {
                        profile_id,
                        database: Some("app".to_string()),
                        table: table.clone(),
                    },
                    cx,
                )
                .expect("the builder tab stays open");
            assert!(
                manager
                    .document(builder_tab)
                    .expect("the builder tab exists")
                    .as_pane()
                    .has_applied_builder_query(cx),
                "the builder tab keeps its applied query"
            );
        });

        let browses = connection.browses();
        let reload = browses
            .last()
            .expect("the fresh tab reloaded under the equality");
        assert_eq!(reload.pagination.offset(), 0);
        assert_eq!(reload.filter.as_deref(), Some("\"id\" = 7"));
        assert_eq!(
            browses
                .iter()
                .filter(|browse| browse.filter.is_some())
                .count(),
            1,
            "only the fresh tab browsed under the equality"
        );
    }
}

/// A value typed into an open grid editor and not yet committed must reach the
/// close gate: every close route either asks about it or keeps it, and never
/// drops it without a word.
#[cfg(test)]
mod pending_cell_input_close_tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::keymap::{Command, CommandDispatcher, FocusTarget};
    use crate::ui::document::tab_bar::TAB_MENU_CLOSE;
    use crate::ui::document::{DataDocument, DocumentId, Tab, TabBarEvent};
    use crate::ui::views::workspace::Workspace;
    use dbflux_core::{ColumnKind, ColumnMeta, QueryResult, Value};
    use dbflux_ui_base::AppStateEntity;
    use gpui::{
        AppContext as _, Bounds, Entity, Modifiers, MouseButton, Pixels, Point, TestAppContext,
        VisualTestContext, point,
    };
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    fn new_workspace(
        cx: &mut TestAppContext,
    ) -> (
        Entity<Workspace>,
        Entity<AppStateEntity>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);

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

        // Test windows open inactive, and gpui hides focus paths of an
        // inactive window from its focus listeners, which would keep the cell
        // editor from ever seeing its blur.
        window.update(|window, _| window.activate_window());
        window.run_until_parked();

        (workspace, app_state, window)
    }

    fn column(name: &str, kind: ColumnKind, is_primary_key: bool) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "text".to_string(),
            kind,
            nullable: true,
            is_primary_key,
        }
    }

    /// One row keyed by `id`, with a text column edited inline, a JSON column
    /// and a multi-line text column, both edited in the cell editor dialog.
    fn orders_result() -> QueryResult {
        QueryResult::table(
            vec![
                column("id", ColumnKind::Integer, true),
                column("name", ColumnKind::Text, false),
                column("data", ColumnKind::Unknown, false),
                column("notes", ColumnKind::Text, false),
            ],
            vec![vec![
                Value::Int(1),
                Value::Text("alice".to_string()),
                Value::Json("{\"a\":1}".to_string()),
                Value::Text("first line\nsecond line".to_string()),
            ]],
            None,
            Duration::ZERO,
        )
    }

    /// Opens an editable table tab that reads from no connection, makes it the
    /// focused document and returns its id.
    fn open_grid_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        app_state: &Entity<AppStateEntity>,
    ) -> DocumentId {
        let document = window.update(|window, cx| {
            cx.new(|cx| {
                DataDocument::new_for_test_table(
                    orders_result(),
                    vec!["id".to_string()],
                    app_state.clone(),
                    window,
                    cx,
                )
            })
        });
        let document_id = window.update(|_, cx| document.read(cx).id());

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                let pane = DataDocument::into_pane(document.clone(), cx);
                workspace.tab_manager.update(cx, |manager, cx| {
                    manager.open(Tab::Pane(Box::new(pane)), cx);
                });
                workspace.set_focus(FocusTarget::Document, window, cx);
            });
        });
        window.run_until_parked();

        document_id
    }

    /// Sends grid commands to the active document, as the keymap does.
    fn dispatch_to_grid(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        commands: &[Command],
    ) {
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.tab_manager.update(cx, |manager, cx| {
                    for command in commands {
                        manager.dispatch_active(*command, window, cx);
                    }
                });
            });
        });
        window.run_until_parked();
    }

    /// Opens the editor on the given column of the first row and types `text`
    /// into it, without committing it.
    fn type_into_cell(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        column_moves: usize,
        text: &str,
    ) {
        let mut commands = vec![Command::SelectFirst];
        commands.extend(std::iter::repeat_n(Command::ColumnRight, column_moves));
        commands.push(Command::Execute);
        dispatch_to_grid(window, workspace, &commands);

        window.simulate_input(text);
        window.run_until_parked();
    }

    fn change_summary(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        id: DocumentId,
    ) -> Option<String> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .and_then(|tab| tab.change_summary(cx))
        })
    }

    fn tab_is_open(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        id: DocumentId,
    ) -> bool {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_some()
        })
    }

    /// The number of documents the unsaved-changes prompt lists, or `None`
    /// when it is not open.
    fn prompt_entries(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Option<usize> {
        window.update(|_, cx| {
            let modal = workspace.read(cx).modal_unsaved_changes.read(cx);
            modal.is_visible().then(|| modal.selected_count())
        })
    }

    fn center(bounds: Bounds<Pixels>) -> Point<Pixels> {
        point(
            bounds.origin.x + bounds.size.width / 2.0,
            bounds.origin.y + bounds.size.height / 2.0,
        )
    }

    fn rendered_bounds(window: &mut VisualTestContext, selector: String) -> Bounds<Pixels> {
        let selector: &'static str = selector.leak();
        window.update(|window, _| window.refresh());
        window.run_until_parked();
        window
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} was not rendered"))
    }

    /// The close was held for the user's decision: the tab is still open, the
    /// prompt lists it, and the typed value is a pending change.
    fn assert_close_asks_about_the_typed_value(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        id: DocumentId,
        route: &str,
    ) {
        assert!(
            tab_is_open(window, workspace, id),
            "{route}: the tab must stay open while the typed value is unsaved"
        );
        assert_eq!(
            prompt_entries(window, workspace),
            Some(1),
            "{route}: the unsaved-changes prompt must list the grid"
        );
        assert!(
            change_summary(window, workspace, id).is_some(),
            "{route}: the typed value must be a pending change"
        );
    }

    /// Control for the fixture: Enter commits what was typed, so the value the
    /// other tests type really sits in the cell editor.
    #[gpui::test]
    fn enter_commits_the_value_typed_into_the_cell(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);

        type_into_cell(window, &workspace, 1, "bob");
        assert!(
            change_summary(window, &workspace, id).is_none(),
            "a value still in the editor is not a pending change yet"
        );

        dispatch_to_grid(window, &workspace, &[Command::Execute]);
        assert!(
            change_summary(window, &workspace, id).is_some(),
            "Enter stages the typed value"
        );
    }

    #[gpui::test]
    fn the_close_button_asks_about_a_value_typed_into_an_open_cell(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, id, "close button");
    }

    #[gpui::test]
    fn a_middle_click_asks_about_a_value_typed_into_an_open_cell(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let tab = center(rendered_bounds(window, format!("tab-{}", id.0)));
        window.simulate_mouse_down(tab, MouseButton::Middle, Modifiers::none());
        window.simulate_mouse_up(tab, MouseButton::Middle, Modifiers::none());
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, id, "middle click");
    }

    /// The right click moves focus out of the cell editor, and the frame drawn
    /// before the menu item is chosen delivers the editor's blur.
    #[gpui::test]
    fn the_tab_menu_close_asks_about_a_value_typed_into_an_open_cell(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let tab = center(rendered_bounds(window, format!("tab-{}", id.0)));
        window.simulate_mouse_down(tab, MouseButton::Right, Modifiers::none());
        window.simulate_mouse_up(tab, MouseButton::Right, Modifiers::none());
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.tab_bar.update(cx, |bar, cx| {
                    bar.context_menu_execute_at(TAB_MENU_CLOSE, cx)
                });
            });
        });
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, id, "tab menu close");
    }

    /// Only the tab menu commits on its way in. A plain click on the tab moves
    /// focus out of the cell editor like any other click outside the table,
    /// and that still cancels the edit.
    #[gpui::test]
    fn clicking_the_tab_still_cancels_the_typed_value(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let tab = center(rendered_bounds(window, format!("tab-{}", id.0)));
        window.simulate_click(tab, Modifiers::none());
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        assert!(tab_is_open(window, &workspace, id));
        assert!(
            change_summary(window, &workspace, id).is_none(),
            "focus leaving the cell editor drops the typed value, as before"
        );
    }

    #[gpui::test]
    fn the_close_tab_command_asks_about_a_value_typed_into_an_open_cell(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::CloseCurrentTab, window, cx);
            });
        });
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, id, "close tab command");
    }

    /// Close Others, Close All, Close to the Left and Close to the Right.
    #[gpui::test]
    fn batch_closes_ask_about_a_value_typed_into_an_open_cell(cx: &mut TestAppContext) {
        type CloseBatchRoute = (&'static str, fn(DocumentId, DocumentId) -> TabBarEvent);
        let routes: [CloseBatchRoute; 4] = [
            ("close others", |_grid, other| {
                TabBarEvent::CloseOtherTabs(other)
            }),
            ("close all", |_grid, _other| TabBarEvent::CloseAllTabs),
            ("close to the left", |_grid, other| {
                TabBarEvent::CloseTabsToLeft(other)
            }),
            ("close to the right", |_grid, other| {
                TabBarEvent::CloseTabsToRight(other)
            }),
        ];

        for (route, event) in routes {
            let (workspace, app_state, window) = new_workspace(cx);

            // The grid sits on the side of `other` that each batch closes.
            let left_of_other = route != "close to the right";
            let (id, other) = if left_of_other {
                let id = open_grid_tab(window, &workspace, &app_state);
                let other = open_grid_tab(window, &workspace, &app_state);
                (id, other)
            } else {
                let other = open_grid_tab(window, &workspace, &app_state);
                let id = open_grid_tab(window, &workspace, &app_state);
                (id, other)
            };

            window.update(|_, cx| {
                workspace.update(cx, |workspace, cx| {
                    workspace
                        .tab_manager
                        .update(cx, |manager, cx| manager.activate(id, cx));
                });
            });
            window.run_until_parked();
            type_into_cell(window, &workspace, 1, "bob");

            let event = event(id, other);
            window.update(|_, cx| {
                workspace.update(cx, |workspace, cx| {
                    workspace.tab_bar.update(cx, |_bar, cx| cx.emit(event));
                });
            });
            window.run_until_parked();

            assert_close_asks_about_the_typed_value(window, &workspace, id, route);
        }
    }

    /// The cell editor dialog sits inside the document, so the tab bar stays
    /// reachable while it holds an edited value.
    #[gpui::test]
    fn the_close_button_asks_about_a_value_edited_in_the_cell_editor_dialog(
        cx: &mut TestAppContext,
    ) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 3, " and more");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, id, "cell editor dialog");
    }

    /// A JSON value that does not parse cannot become a pending change, so the
    /// close leaves the tab open with the value in the dialog.
    #[gpui::test]
    fn an_invalid_json_value_in_the_dialog_keeps_the_tab_open(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 2, "{\"b\":");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        assert!(tab_is_open(window, &workspace, id));
        assert_eq!(
            prompt_entries(window, &workspace),
            None,
            "nothing could be committed, so there is nothing to ask about"
        );
    }

    /// Cancelling the prompt keeps the tab and the typed value, now a pending
    /// change the user can save or revert.
    #[gpui::test]
    fn cancelling_the_prompt_keeps_the_typed_value(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .modal_unsaved_changes
                    .update(cx, |modal, cx| modal.cancel(cx));
            });
        });
        window.run_until_parked();

        assert!(tab_is_open(window, &workspace, id));
        assert!(
            change_summary(window, &workspace, id).is_some(),
            "the typed value stays as a pending change"
        );
    }

    /// Saving from the prompt applies the typed value, and the tab closes only
    /// once the apply lands. This grid reads from no connection, so the apply
    /// cannot land and the tab stays open.
    #[gpui::test]
    fn saving_from_the_prompt_applies_the_typed_value(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .modal_unsaved_changes
                    .update(cx, |modal, cx| modal.confirm(cx));
            });
        });
        window.run_until_parked();

        assert!(
            tab_is_open(window, &workspace, id),
            "an apply that did not land must not take the tab away"
        );
    }

    /// Discarding from the prompt is the user's decision to drop the value.
    #[gpui::test]
    fn discarding_from_the_prompt_closes_the_tab(cx: &mut TestAppContext) {
        use crate::ui::overlays::modals::UnsavedChangesOutcome;

        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.modal_unsaved_changes.update(cx, |modal, cx| {
                    cx.emit(UnsavedChangesOutcome::DiscardAll(vec![id]));
                    modal.close(cx);
                });
            });
        });
        window.run_until_parked();

        assert!(!tab_is_open(window, &workspace, id));
    }

    /// A quit flushes every document before shutting down; the value still in
    /// the cell editor is committed first, so a document that persists its
    /// pending changes on shutdown persists this one too.
    #[gpui::test]
    fn the_shutdown_flush_commits_a_value_typed_into_an_open_cell(cx: &mut TestAppContext) {
        let (workspace, app_state, window) = new_workspace(cx);
        let id = open_grid_tab(window, &workspace, &app_state);
        type_into_cell(window, &workspace, 1, "bob");

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.flush_pending_document_edits(cx);
            });
        });

        assert!(
            change_summary(window, &workspace, id).is_some(),
            "the shutdown flush must commit the typed value"
        );
    }
}
