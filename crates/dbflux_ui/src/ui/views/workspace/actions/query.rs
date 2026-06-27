use super::*;

impl Workspace {
    /// Creates a new SQL query tab backed by a script file.
    pub(in crate::ui::views::workspace) fn new_query_tab(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let query_language = self
            .app_state
            .read(cx)
            .active_connection_id()
            .and_then(|id| self.app_state.read(cx).connections().get(&id))
            .map(|conn| conn.connection.metadata().query_language.clone())
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let extension = query_language.default_extension();

        let script_path = self.app_state.update(cx, |state, cx| {
            let dir = state.scripts_directory_mut()?;
            let name = dir.next_available_name("Query", extension);
            let path = dir.create_file(None, &name, extension).ok();
            if path.is_some() {
                cx.emit(AppStateChanged);
            }
            path
        });

        let doc = cx.new(|cx| {
            let mut doc = CodeDocument::new(self.app_state.clone(), window, cx);
            if let Some(path) = script_path {
                let title = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Query")
                    .to_string();
                doc = doc.with_title(title).with_path(path);
            }
            doc
        });

        if !doc.read(cx).is_file_backed() {
            doc.read(cx).initial_auto_save(cx);
        }

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    pub(in crate::ui::views::workspace) fn new_query_tab_with_content(
        &mut self,
        sql: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let query_language = self
            .app_state
            .read(cx)
            .active_connection_id()
            .and_then(|id| self.app_state.read(cx).connections().get(&id))
            .map(|conn| conn.connection.metadata().query_language.clone())
            .unwrap_or(dbflux_core::QueryLanguage::Sql);

        let extension = query_language.default_extension();

        let script_path = self.app_state.update(cx, |state, cx| {
            let dir = state.scripts_directory_mut()?;
            let name = dir.next_available_name("Query", extension);
            let path = dir.create_file(None, &name, extension).ok();
            if path.is_some() {
                cx.emit(AppStateChanged);
            }
            path
        });

        let doc = cx.new(|cx| {
            let mut doc = CodeDocument::new(self.app_state.clone(), window, cx);
            if let Some(ref path) = script_path {
                let title = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Query")
                    .to_string();
                doc = doc.with_title(title).with_path(path.clone());
            }
            doc.set_content(&sql, window, cx);
            doc
        });

        if !doc.read(cx).is_file_backed() {
            doc.read(cx).initial_auto_save(cx);
        }

        // Write initial content to the script file (with annotation headers)
        if let Some(path) = script_path {
            let content = doc.read(cx).build_file_content(cx);
            if let Err(e) = std::fs::write(&path, &content) {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        format!("Failed to write initial script content: {e}"),
                    ),
                    cx,
                );
            }
        }

        let pane = CodeDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
    }

    /// Write the current tab state to the session manifest (dbflux.db-backed).
    pub(in crate::ui::views::workspace) fn write_session_manifest(&self, cx: &mut App) {
        use dbflux_core::SessionTab;

        let runtime = self.app_state.read(cx).storage_runtime();

        let repo = runtime.sessions();
        let manager = self.tab_manager.read(cx);
        let mut tabs = Vec::new();

        for doc_tab in manager.documents() {
            let Some(snap) = doc_tab.session_tab_snapshot(cx) else {
                continue;
            };

            tabs.push(
                dbflux_storage::repositories::state::sessions::WorkspaceTab {
                    id: snap.id.0.to_string(),
                    tab_kind: snap.kind.to_string(),
                    language: SessionTab::language_key(snap.language),
                    exec_ctx: snap.exec_ctx,
                    scratch_path: snap.scratch_path,
                    shadow_path: snap.shadow_path,
                    file_path: snap.file_path,
                    title: snap.title,
                    position: tabs.len(),
                    is_pinned: false,
                },
            );
        }

        let active_index = manager.active_id().and_then(|active_id| {
            tabs.iter()
                .position(|tab| tab.id == active_id.0.to_string())
        });

        let manifest = dbflux_storage::repositories::state::sessions::WorkspaceSessionManifest {
            version: 1,
            active_index,
            tabs,
        };

        if let Err(e) = repo.save_workspace_session(&manifest) {
            report_error(
                UserFacingError::new(ErrorKind::Storage, "Failed to save session manifest")
                    .with_cause(format!("{e}")),
                cx,
            );
        }
    }

    /// Restore tabs from the session manifest on startup (dbflux.db-backed).
    pub(in crate::ui::views::workspace) fn restore_session(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let manifest = {
            let app = self.app_state.read(cx);
            let runtime = app.storage_runtime();
            let repo = runtime.sessions();
            let artifacts = runtime.artifacts();

            match repo.restore_session(artifacts) {
                Ok(Some(session)) => session,
                Ok(None) => return,
                Err(e) => {
                    log::warn!("Failed to restore session from dbflux.db: {}", e);
                    return;
                }
            }
        };

        if manifest.tabs.is_empty() {
            return;
        }

        for tab in &manifest.tabs {
            let manifest_language = match tab.language.as_str() {
                "sql" => dbflux_core::QueryLanguage::Sql,
                "mongo" => dbflux_core::QueryLanguage::MongoQuery,
                "redis" => dbflux_core::QueryLanguage::RedisCommands,
                "cypher" => dbflux_core::QueryLanguage::Cypher,
                "lua" => dbflux_core::QueryLanguage::Lua,
                "python" => dbflux_core::QueryLanguage::Python,
                "bash" => dbflux_core::QueryLanguage::Bash,
                _ => dbflux_core::QueryLanguage::Sql,
            };

            let language = match &tab.tab_kind[..] {
                "FileBacked" => {
                    if let Some(ref fp) = tab.file_path {
                        dbflux_core::QueryLanguage::from_path(fp).unwrap_or(manifest_language)
                    } else {
                        manifest_language
                    }
                }
                "Scratch" => {
                    let title_path = std::path::Path::new(&tab.title);
                    dbflux_core::QueryLanguage::from_path(title_path).unwrap_or(manifest_language)
                }
                _ => manifest_language,
            };

            // Routine tabs are persisted with their descriptor encoded in exec_ctx:
            // connection_id=profile_id, schema=schema, container=specific_name.
            // Reconstruct as a read-only document; the definition is re-fetched when the
            // connection becomes available (handled by AppStateChanged in CodeDocument).
            if tab.tab_kind == "Routine" {
                let exec_ctx_json = tab.exec_ctx_json.as_str();
                let exec_ctx: dbflux_core::ExecutionContext = serde_json::from_str(exec_ctx_json)
                    .unwrap_or_else(|_| dbflux_core::ExecutionContext::default());

                let Some(profile_id) = exec_ctx.connection_id else {
                    log::warn!(
                        "Routine tab '{}' has no profile_id in exec_ctx — skipping",
                        tab.title
                    );
                    continue;
                };

                let Some(schema) = exec_ctx.schema.clone() else {
                    log::warn!(
                        "Routine tab '{}' has no schema in exec_ctx — skipping",
                        tab.title
                    );
                    continue;
                };

                let Some(specific_name) = exec_ctx.container.clone() else {
                    log::warn!(
                        "Routine tab '{}' has no specific_name (container) in exec_ctx — skipping",
                        tab.title
                    );
                    continue;
                };

                let title = tab.title.clone();

                let doc = cx.new(|cx| {
                    // Pass Some(profile_id) as connection_id so the exec context
                    // is pre-seeded; the connection might not be active yet.
                    CodeDocument::new_with_language(
                        self.app_state.clone(),
                        Some(profile_id),
                        language,
                        window,
                        cx,
                    )
                    .with_title(title)
                    .with_read_only(cx)
                    .with_routine_dedup(profile_id, schema, specific_name)
                    .with_routine_definition_pending()
                });

                // If the connection is already active at restore time, trigger
                // the definition fetch immediately via the same path used by
                // the AppStateChanged handler.
                doc.update(cx, |d, cx| {
                    d.try_fetch_pending_routine_definition(cx);
                });

                let pane = CodeDocument::into_pane(doc, cx);

                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.open(Tab::Pane(Box::new(pane)), cx);
                });

                continue;
            }

            let (content, path, scratch_path, shadow_path) = match tab.tab_kind.as_str() {
                "Scratch" => {
                    let sp = match tab.scratch_path.as_ref() {
                        Some(p) => p.clone(),
                        None => {
                            log::warn!(
                                "Scratch tab '{}' has no scratch_path in restored session — skipping",
                                tab.title
                            );
                            continue;
                        }
                    };
                    let content = std::fs::read_to_string(&sp).unwrap_or_default();
                    (content, None, Some(sp), None)
                }
                "FileBacked" => {
                    let fp = match tab.file_path.as_ref() {
                        Some(p) => p.clone(),
                        None => {
                            log::warn!(
                                "FileBacked tab '{}' has no file_path in restored session — skipping",
                                tab.title
                            );
                            continue;
                        }
                    };
                    let content = if let Some(ref sh) = tab.shadow_path {
                        let shadow_content = std::fs::read_to_string(sh).unwrap_or_default();
                        let original_modified =
                            std::fs::metadata(&fp).ok().and_then(|m| m.modified().ok());
                        let shadow_modified =
                            std::fs::metadata(sh).ok().and_then(|m| m.modified().ok());

                        if let (Some(orig_t), Some(shad_t)) = (original_modified, shadow_modified) {
                            if orig_t > shad_t {
                                log::warn!(
                                    "External edit detected for {}: using original file",
                                    fp.display()
                                );
                                std::fs::read_to_string(&fp).unwrap_or(shadow_content)
                            } else {
                                shadow_content
                            }
                        } else {
                            shadow_content
                        }
                    } else {
                        std::fs::read_to_string(&fp).unwrap_or_default()
                    };

                    (content, Some(fp), None, tab.shadow_path.clone())
                }
                _ => continue,
            };

            let exec_ctx_json = tab.exec_ctx_json.as_str();
            let exec_ctx: dbflux_core::ExecutionContext = serde_json::from_str(exec_ctx_json)
                .unwrap_or_else(|_| dbflux_core::ExecutionContext::default());

            let connection_id = exec_ctx
                .connection_id
                .filter(|id| self.app_state.read(cx).connections().contains_key(id));

            let body = Self::strip_annotation_header(&content, &language);

            let title = if tab.tab_kind == "Scratch" {
                tab.title.clone()
            } else {
                tab.file_path
                    .as_ref()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("Untitled")
                    .to_string()
            };

            let doc = cx.new(|cx| {
                let mut doc = CodeDocument::new_with_language(
                    self.app_state.clone(),
                    connection_id,
                    language,
                    window,
                    cx,
                );

                doc.set_session_paths(scratch_path.clone(), shadow_path.clone());

                if let Some(p) = path {
                    doc = doc.with_path(p);
                }

                doc = doc.with_title(title).with_exec_ctx(exec_ctx, cx);
                doc.set_content(body, window, cx);

                if tab.tab_kind == "FileBacked" && tab.shadow_path.is_some() {
                    doc.restore_dirty(cx);
                }

                doc
            });

            let pane = CodeDocument::into_pane(doc, cx);

            self.tab_manager.update(cx, |mgr, cx| {
                mgr.open(Tab::Pane(Box::new(pane)), cx);
            });
        }

        // Restore active tab
        if let Some(active_idx) = manifest.active_index {
            let docs: Vec<_> = self
                .tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|d| d.id())
                .collect();

            if let Some(id) = docs.get(active_idx) {
                self.tab_manager.update(cx, |mgr, cx| {
                    mgr.activate(*id, cx);
                });
            }
        }
    }
}
