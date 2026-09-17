use super::*;
use crate::ui::labels::{
    documents_default_title, documents_new_query_name, documents_save_session_failed_message,
    documents_write_initial_script_failed_message,
};

/// The editor body and the autosave authorization a session-restored
/// file-backed tab derives from one physical read plus the recovered shadow.
struct RestoredFileSelection {
    /// Raw file bytes to load into the editor, annotation header included.
    content: String,
    /// The bytes this document may treat as its on-disk baseline. `None` means
    /// autosave must refuse (`BaselineUnknown`) until an intentional Save.
    baseline: Option<String>,
}

/// Selects the editor body and the physical baseline for a restored file-backed
/// tab from a single physical read and, when present, the recovered shadow read.
///
/// The physical file is read exactly once; the body and the baseline both come
/// from that same read, so a change landing mid-restore can never pair an old
/// body with a newer baseline. A readable shadow is shown as the body, but it
/// only authorizes an automatic overwrite when its bytes are identical to the
/// physical read; otherwise the document gets no baseline and autosave refuses
/// (`BaselineUnknown`) until the user saves intentionally. Returns `None` when
/// neither the file nor its shadow could be read, so the caller skips the tab
/// instead of restoring an empty body over unknown disk state.
fn select_restored_file_content(
    physical: Result<String, std::io::Error>,
    shadow: Option<Result<String, std::io::Error>>,
) -> Option<RestoredFileSelection> {
    let shadow_content = shadow.and_then(Result::ok);

    match (physical, shadow_content) {
        // The file is readable and a shadow survived: show the recovered shadow,
        // and adopt the physical read as the baseline only when the shadow's bytes
        // are identical to it. Different bytes mean the shadow is unproven against
        // disk, so autosave must refuse rather than clobber the file.
        (Ok(disk), Some(shadow)) => {
            let same_as_disk = shadow == disk;
            Some(RestoredFileSelection {
                content: shadow,
                baseline: same_as_disk.then_some(disk),
            })
        }
        // No readable shadow: the body and the baseline are the same physical read.
        (Ok(disk), None) => Some(RestoredFileSelection {
            content: disk.clone(),
            baseline: Some(disk),
        }),
        // The file is unreadable but a shadow survived: preserve its text and seed
        // no baseline, so autosave refuses instead of recreating a missing file.
        (Err(_), Some(shadow)) => Some(RestoredFileSelection {
            content: shadow,
            baseline: None,
        }),
        // Neither source is readable: skip rather than restore empty content over
        // unknown disk state.
        (Err(_), None) => None,
    }
}

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
            let name = dir.next_available_name(&documents_new_query_name(), extension);
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
                    .map(str::to_string)
                    .unwrap_or_else(documents_new_query_name);
                doc = doc.with_title(title).with_path(path.clone());
                // `create_file` just wrote an empty file: those bytes are the
                // document's first trustworthy baseline, so an ordinary autosave
                // may land on the file this tab created.
                doc.seed_file_baseline(path, String::new());
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
            let name = dir.next_available_name(&documents_new_query_name(), extension);
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
                    .map(str::to_string)
                    .unwrap_or_else(documents_new_query_name);
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
            match std::fs::write(&path, &content) {
                Ok(()) => {
                    // Only bytes that actually landed are trustworthy as the
                    // baseline. A failed initial write leaves the document without
                    // one, so autosave refuses instead of guessing.
                    doc.update(cx, |document, _cx| {
                        document.seed_file_baseline(path, content);
                    });
                }
                Err(e) => {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            documents_write_initial_script_failed_message(e),
                        ),
                        cx,
                    );
                }
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
                UserFacingError::new(ErrorKind::Storage, documents_save_session_failed_message())
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

            let (content, path, scratch_path, shadow_path, physical_baseline) = match tab
                .tab_kind
                .as_str()
            {
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
                    (content, None, Some(sp), None, None)
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

                    // Read the physical file exactly once: both the editor body and
                    // the baseline come from this same read.
                    let physical = std::fs::read_to_string(&fp);
                    if let Err(e) = &physical
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        log::warn!(
                            "Could not read the physical baseline for {}: {e}; \
                             autosave will refuse until the file is reloaded",
                            fp.display()
                        );
                    }

                    let shadow = tab.shadow_path.as_ref().map(|shadow_path| {
                        let read = std::fs::read_to_string(shadow_path);
                        if let Err(e) = &read {
                            log::warn!(
                                "Could not read the recovered shadow {} for {}: {e}; \
                                 the physical file body is used instead",
                                shadow_path.display(),
                                fp.display()
                            );
                        }
                        read
                    });

                    let Some(selection) = select_restored_file_content(physical, shadow) else {
                        log::warn!(
                            "File-backed tab '{}' has neither a readable file ({}) nor a \
                             readable shadow — skipping so an empty body never replaces \
                             unknown disk state",
                            tab.title,
                            fp.display()
                        );
                        continue;
                    };

                    (
                        selection.content,
                        Some(fp),
                        None,
                        tab.shadow_path.clone(),
                        selection.baseline,
                    )
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
                    .map(str::to_string)
                    .unwrap_or_else(documents_default_title)
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
                    doc = doc.with_path(p.clone());
                    // The baseline is the same single physical read the body was chosen
                    // from. When a recovered shadow differed from that read,
                    // `selection.baseline` is `None`, so nothing is seeded here and
                    // autosave refuses (`BaselineUnknown`) until an intentional Save;
                    // a recovered shadow can never authorize overwriting disk bytes it
                    // does not match, and a missing file is never recreated blind.
                    if let Some(bytes) = physical_baseline {
                        doc.seed_file_baseline(p, bytes);
                    }
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

#[cfg(test)]
mod tests {
    use super::select_restored_file_content;
    use std::io::{Error, ErrorKind};

    fn unreadable() -> Error {
        Error::new(ErrorKind::NotFound, "missing on disk")
    }

    /// A recovered local shadow that differs from the bytes on disk must be shown
    /// without authorizing an automatic overwrite: seeding the physical read as the
    /// baseline would let a later autosave clobber that foreign content. The
    /// document must refuse (`BaselineUnknown`) instead.
    #[test]
    fn a_recovered_shadow_differing_from_disk_gets_no_baseline() {
        let selection = select_restored_file_content(
            Ok("FOREIGN;".to_string()),
            Some(Ok("LOCAL;".to_string())),
        )
        .expect("a readable file and shadow must yield a selection");

        assert_eq!(
            selection.content, "LOCAL;",
            "the recovered shadow body must be shown"
        );
        assert_eq!(
            selection.baseline, None,
            "a shadow that differs from disk must not adopt the disk bytes as its baseline"
        );
    }

    /// With no shadow, the body and the baseline come from the same single physical
    /// read, so an autosave may land on the file it just loaded.
    #[test]
    fn a_file_restored_without_a_shadow_seeds_its_own_read_as_baseline() {
        let selection = select_restored_file_content(Ok("BODY;".to_string()), None)
            .expect("a readable file must yield a selection");

        assert_eq!(selection.content, "BODY;");
        assert_eq!(selection.baseline, Some("BODY;".to_string()));
    }

    /// A shadow byte-identical to disk is the safe case: its bytes equal the
    /// physical read, so they may authorize the automatic overwrite.
    #[test]
    fn an_identical_shadow_keeps_a_safe_baseline() {
        let selection =
            select_restored_file_content(Ok("SAME;".to_string()), Some(Ok("SAME;".to_string())))
                .expect("identical bytes must yield a selection");

        assert_eq!(selection.content, "SAME;");
        assert_eq!(selection.baseline, Some("SAME;".to_string()));
    }

    /// A missing (or unreadable) physical file with a readable shadow preserves the
    /// recovered text but seeds no baseline, so autosave refuses instead of
    /// recreating a file another process removed.
    #[test]
    fn a_missing_file_with_a_readable_shadow_preserves_the_shadow() {
        let selection =
            select_restored_file_content(Err(unreadable()), Some(Ok("LOCAL;".to_string())))
                .expect("a readable shadow must yield a selection");

        assert_eq!(selection.content, "LOCAL;");
        assert_eq!(selection.baseline, None);
    }

    /// When the shadow cannot be read, the physical body is used and the baseline is
    /// that same read; nothing is left empty.
    #[test]
    fn an_unreadable_shadow_falls_back_to_the_physical_body() {
        let selection =
            select_restored_file_content(Ok("DISK;".to_string()), Some(Err(unreadable())))
                .expect("a readable file must yield a selection");

        assert_eq!(selection.content, "DISK;");
        assert_eq!(selection.baseline, Some("DISK;".to_string()));
    }

    /// When neither the file nor the shadow can be read, the caller must skip the
    /// tab rather than restore an empty body over unknown disk state.
    #[test]
    fn a_file_with_no_readable_source_yields_no_selection() {
        assert!(
            select_restored_file_content(Err(unreadable()), None).is_none(),
            "no readable file or shadow must skip the tab"
        );
        assert!(
            select_restored_file_content(Err(unreadable()), Some(Err(unreadable()))).is_none(),
            "unreadable file and shadow must skip the tab"
        );
    }
}
