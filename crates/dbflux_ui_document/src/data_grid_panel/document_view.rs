use super::*;

impl DataGridPanel {
    pub(crate) fn focus_active_view(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus.focus_mode = GridFocusMode::Table;
        self.focus.edit_state = EditState::Navigating;

        if self.view_config.mode == super::super::data_view::DataViewMode::Document {
            if let Some(tree_state) = &self.document_view.document_tree_state {
                tree_state.update(cx, |state, cx| state.focus(window, cx));
            } else {
                self.focus_handle.focus(window, cx);
            }
        } else {
            self.focus_handle.focus(window, cx);
        }

        cx.emit(DataGridEvent::Focused);
        cx.notify();
    }

    /// Rebuild the views derived from `self.result` that are not the grid
    /// itself: the document tree for collection/JSON results and the
    /// variable-height card list. Called from both arms of `rebuild_table`.
    pub(super) fn rebuild_result_views(&mut self, cx: &mut Context<Self>) {
        // The grid keeps its presentation flag across a reload, but a result
        // that cannot be shown as a record (a grouped aggregate) still has to
        // push the panel out of record mode.
        self.apply_record_mode(cx);

        // Build document tree for collections, JSON-shaped query results and
        // tables whose facets include documents (a `jsonb` column).
        let should_build_tree = self.source.is_collection()
            || matches!(&self.source, DataSource::QueryResult { result, .. } if result.shape.is_json())
            || (matches!(self.source, DataSource::Table { .. }) && self.has_document_facet(cx));

        if should_build_tree {
            self.rebuild_document_tree(cx);
        }

        // Reset the variable-height card-list state to match the new row count.
        // Only the document-card fallback (no tree) consumes it, but building it
        // unconditionally keeps the row count in sync with `self.result`.
        self.document_view.document_card_list = Some(ListState::new(
            self.result.rows.len(),
            ListAlignment::Top,
            px(400.0),
        ));
    }

    pub(super) fn rebuild_document_tree(&mut self, cx: &mut Context<Self>) {
        // A table's rows open as documents with their JSON values expanded;
        // the tree is read-only there, since edits go through the grid.
        let table_documents = matches!(self.source, DataSource::Table { .. })
            .then(|| super::table_documents::table_rows_as_documents(&self.result));
        let tree_state = cx.new(|cx| {
            let mut state = DocumentTreeState::new(cx);
            match &table_documents {
                Some(documents) => {
                    state.load_from_result(documents, cx);
                    state.set_read_only(true, cx);
                }
                None => {
                    state
                        .load_from_result(self.collection.raw.as_ref().unwrap_or(&self.result), cx);
                }
            }
            state
        });

        let tree = cx.new(|cx| DocumentTree::new("document-tree", tree_state.clone(), cx));

        let subscription = cx.subscribe(
            &tree_state,
            |this, _state, event: &DocumentTreeEvent, cx| match event {
                DocumentTreeEvent::Focused => {
                    cx.emit(DataGridEvent::Focused);
                }
                DocumentTreeEvent::InlineEditCommitted { node_id, new_value } => {
                    if this.commits_document_patches(cx) {
                        this.commit_tree_edit(node_id, new_value, cx);
                    } else {
                        this.handle_document_tree_inline_edit(node_id, new_value, cx);
                    }
                }
                DocumentTreeEvent::DocumentPreviewRequested {
                    doc_index,
                    document_json,
                } => {
                    this.pending.document_preview = Some(PendingDocumentPreview {
                        doc_index: *doc_index,
                        document_json: document_json.clone(),
                    });
                    cx.notify();
                }
                DocumentTreeEvent::DeleteRequested(node_id) => {
                    if let Some(doc_idx) = node_id.doc_index() {
                        this.pending_delete_confirm = Some(PendingDeleteConfirm {
                            row_indices: vec![doc_idx],
                            is_table: false,
                        });
                        cx.notify();
                    }
                }
                DocumentTreeEvent::ContextMenuRequested {
                    doc_index,
                    position,
                    node_id,
                    node_value,
                } => {
                    // node_id is the tree cursor, built by NodeId::root(),
                    // child() and parent(), which all keep the leading
                    // document segment, so [1..] is in range. The event's
                    // doc_index comes from doc_index().unwrap_or(0) and does
                    // not prove it.
                    #[expect(
                        clippy::indexing_slicing,
                        reason = "node_id is the tree cursor, built by NodeId::root()/child()/parent(), which all keep the leading document segment, so [1..] is in range; the event's doc_index comes from doc_index().unwrap_or(0) and does not prove it"
                    )]
                    let field_path: Vec<String> = node_id.path[1..].to_vec();

                    this.context_menu = Some(TableContextMenu {
                        row: *doc_index,
                        col: 0,
                        position: *position,
                        sql_submenu_open: false,
                        copy_query_submenu_open: false,
                        filter_submenu_open: false,
                        order_submenu_open: false,
                        toolbar_submenu_open: false,
                        selected_index: 0,
                        submenu_selected_index: 0,
                        is_document_view: true,
                        is_column_header: false,
                        doc_field_path: if field_path.is_empty() {
                            None
                        } else {
                            Some(field_path)
                        },
                        doc_field_value: node_value.clone(),
                        row_actions: Vec::new(),
                    });
                    this.pending.context_menu_focus = true;
                    cx.emit(DataGridEvent::Focused);
                    cx.notify();
                }
                DocumentTreeEvent::CycleDataViewRequested => {
                    this.toggle_view_mode(cx);
                }
                DocumentTreeEvent::CursorMoved
                | DocumentTreeEvent::ExpandToggled
                | DocumentTreeEvent::ViewModeToggled
                | DocumentTreeEvent::SearchOpened
                | DocumentTreeEvent::SearchClosed => {}
            },
        );

        self.document_view.document_tree_state = Some(tree_state);
        self.document_view.document_tree = Some(tree);
        self.document_view.document_tree_subscription = Some(subscription);
    }
}
