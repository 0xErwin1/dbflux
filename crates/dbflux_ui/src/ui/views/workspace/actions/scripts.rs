use super::*;

impl Workspace {
    /// Opens a file dialog to pick a script file and opens it in a new tab.
    pub(in crate::ui::views::workspace) fn open_script_file(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let tab_manager = self.tab_manager.clone();

        cx.spawn(async move |this, cx| {
            let file_handle = rfd::AsyncFileDialog::new()
                .set_title("Open Script")
                .add_filter("SQL Files", &["sql"])
                .add_filter("JavaScript (MongoDB)", &["js", "mongodb"])
                .add_filter("Redis", &["redis", "red"])
                .add_filter("All Files", &["*"])
                .pick_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();

            // Check if this file is already open
            let already_open = match cx.update(|cx| {
                tab_manager.read(cx).find_by_key(
                    &crate::ui::document::DocumentKey::File { path: path.clone() },
                    cx,
                )
            }) {
                Ok(value) => value,
                Err(error) => {
                    log::warn!(
                        "Failed to inspect open tabs while opening script: {:?}",
                        error
                    );
                    None
                }
            };

            if let Some(id) = already_open {
                if let Err(error) = cx.update(|cx| {
                    tab_manager.update(cx, |mgr, cx| {
                        mgr.activate(id, cx);
                    });
                }) {
                    log::warn!("Failed to activate already-open script tab: {:?}", error);
                }
                return;
            }

            // Read file content on background thread
            let read_path = path.clone();
            let content = cx
                .background_executor()
                .spawn(async move { std::fs::read_to_string(&read_path) })
                .await;

            let content = match content {
                Ok(c) => c,
                Err(e) => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            format!("Failed to read file {}: {e}", path.display()),
                        ),
                        cx,
                    );
                    return;
                }
            };

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.open_script_with_content(path, content, cx);
                })
                .unwrap_or_else(|inner_error| {
                    log::warn!(
                        "Failed to update workspace while opening selected script: {:?}",
                        inner_error
                    );
                });
            }) {
                log::warn!(
                    "Failed to apply selected script content to workspace: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    /// Opens a script file from a known path (e.g., from sidebar recent files).
    pub fn open_script_from_path(&mut self, path: std::path::PathBuf, cx: &mut Context<Self>) {
        let tab_manager = self.tab_manager.clone();

        // Check if already open
        let already_open = tab_manager.read(cx).find_by_key(
            &crate::ui::document::DocumentKey::File { path: path.clone() },
            cx,
        );

        if let Some(id) = already_open {
            tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            return;
        }

        cx.spawn(async move |this, cx| {
            let read_path = path.clone();
            let content = cx
                .background_executor()
                .spawn(async move { std::fs::read_to_string(&read_path) })
                .await;

            let content = match content {
                Ok(c) => c,
                Err(e) => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            format!("Failed to read file {}: {e}", path.display()),
                        ),
                        cx,
                    );
                    return;
                }
            };

            if let Err(error) = cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.open_script_with_content(path, content, cx);
                })
                .unwrap_or_else(|inner_error| {
                    log::warn!(
                        "Failed to update workspace while opening script path: {:?}",
                        inner_error
                    );
                });
            }) {
                log::warn!(
                    "Failed to apply script content from explicit path to workspace: {:?}",
                    error
                );
            }
        })
        .detach();
    }

    /// Opens a read-only code document showing a routine's definition.
    ///
    /// Fetches the definition in the background (via `routine_definition`) and
    /// defers creation to the next render cycle where `Window` is available.
    /// Focuses the existing tab when already open.
    pub fn open_routine_definition(
        &mut self,
        profile_id: uuid::Uuid,
        schema: String,
        specific_name: String,
        title: String,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::document::DocumentKey;

        let dedup_key = DocumentKey::Routine {
            profile_id,
            schema: schema.clone(),
            specific_name: specific_name.clone(),
        };

        if let Some(existing_id) = self.tab_manager.read(cx).find_by_key(&dedup_key, cx) {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(existing_id, cx);
            });
            return;
        }

        let connections = self.app_state.read(cx).connections();
        let connected = match connections.get(&profile_id) {
            Some(c) => c,
            None => return,
        };

        let database = connected
            .active_database
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let connection = connected.connection.clone();

        let schema_fetch = schema.clone();
        let specific_name_fetch = specific_name.clone();

        cx.spawn(async move |this, cx| {
            let definition = cx
                .background_executor()
                .spawn(async move {
                    connection.routine_definition(&database, &schema_fetch, &specific_name_fetch)
                })
                .await;

            let body = match definition {
                Ok(def) => def,
                Err(e) => {
                    log::warn!(
                        "Failed to fetch routine definition for {}: {}",
                        specific_name,
                        e
                    );
                    format!("-- Failed to load routine definition:\n-- {}", e)
                }
            };

            cx.update(|cx| {
                this.update(cx, |ws, cx| {
                    ws.pending_open_routine = Some(PendingOpenRoutine {
                        profile_id,
                        schema,
                        specific_name,
                        title,
                        body,
                    });
                    cx.notify();
                })
                .ok();
            })
            .ok();
        })
        .detach();
    }

    pub(in crate::ui::views::workspace) fn finalize_open_routine(
        &mut self,
        pending: PendingOpenRoutine,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let doc = cx.new(|cx| {
            let connection_id = Some(pending.profile_id);
            let mut doc = CodeDocument::new_with_language(
                self.app_state.clone(),
                connection_id,
                dbflux_core::QueryLanguage::Sql,
                window,
                cx,
            )
            .with_title(pending.title)
            .with_read_only(cx)
            .with_routine_dedup(
                pending.profile_id,
                pending.schema,
                pending.specific_name,
            );

            doc.set_content(&pending.body, window, cx);
            doc
        });

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Opens a script file from a known path and content (called after file read).
    fn open_script_with_content(
        &mut self,
        path: std::path::PathBuf,
        content: String,
        cx: &mut Context<Self>,
    ) {
        use dbflux_core::{ExecutionContext, QueryLanguage};

        let language = QueryLanguage::from_path(&path).unwrap_or(QueryLanguage::Sql);
        let uses_connection_context = language.supports_connection_context();

        let exec_ctx = if uses_connection_context {
            ExecutionContext::parse_from_content(&content, language.clone())
        } else {
            ExecutionContext::default()
        };

        let connection_id = if uses_connection_context {
            exec_ctx
                .connection_id
                .filter(|id| self.app_state.read(cx).connections().contains_key(id))
                .or_else(|| self.app_state.read(cx).active_connection_id())
        } else {
            None
        };

        let body = if uses_connection_context {
            Self::strip_annotation_header(&content, &language)
        } else {
            &content
        };

        // Track in recent files
        self.app_state.update(cx, |state, cx| {
            state.record_recent_file(path.clone());
            cx.emit(AppStateChanged);
        });

        // We need window access; use pending_open_script pattern
        self.pending_open_script = Some(PendingOpenScript {
            title: path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("Untitled")
                .to_string(),
            path: Some(path),
            body: body.to_string(),
            language,
            connection_id,
            exec_ctx,
        });
        cx.notify();
    }

    pub(in crate::ui::views::workspace) fn finalize_open_script(
        &mut self,
        pending: PendingOpenScript,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let doc = cx.new(|cx| {
            let mut doc = CodeDocument::new_with_language(
                self.app_state.clone(),
                pending.connection_id,
                pending.language,
                window,
                cx,
            )
            .with_exec_ctx(pending.exec_ctx, cx);
            doc = doc.with_title(pending.title);

            if let Some(path) = pending.path {
                doc = doc.with_path(path);
            }

            doc.set_content(&pending.body, window, cx);
            doc
        });

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }
}
