use super::file_persistence::{
    ExecutedWrite, PhysicalWrite, WriteKind, WriteOutcome, execute_write,
};
use super::*;
use dbflux_ui_base::AsyncUpdateResultExt;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};

/// Build the file content, prepending the execution-context annotation header
/// when the editor surface is connection-backed.
///
/// `supports_connection_context` and `comment_prefix` are resolved from the
/// editor's profile (not the raw `QueryLanguage`) so a driver with a bespoke
/// surface — e.g. a connection-backed DynamoDB editor whose `QueryLanguage` is
/// `Custom("DynamoDB")` but whose profile reports connection support and a `--`
/// prefix — emits the header with the correct prefix.
fn build_file_content_for_language(
    editor_content: &str,
    exec_ctx: &ExecutionContext,
    supports_connection_context: bool,
    comment_prefix: &str,
) -> String {
    if !supports_connection_context {
        return editor_content.to_string();
    }

    let header = exec_ctx.to_comment_header_with_prefix(comment_prefix);
    if header.is_empty() {
        return editor_content.to_string();
    }

    let body = CodeDocument::strip_existing_annotations(editor_content, comment_prefix);
    format!("{}\n{}", header, body)
}

/// Reports a save that did not reach the filesystem: the Save As picker was
/// dismissed, could not be opened, or the write failed. The buffer stays dirty,
/// so a tab close waiting on that save must not proceed.
fn report_save_failed(entity: &Entity<CodeDocument>, cx: &AsyncApp) {
    cx.update(|cx| {
        entity.update(cx, |doc, cx| {
            doc.report_save_outcome(false, cx);
        });
    })
    .log_if_dropped();
}

/// Applies the result of one queued physical write: reports user-facing
/// failures, reconciles dirty state by write kind, and lets the queue start
/// the next write. The queue slot is freed inside the entity update, so the
/// next write starts only after this outcome is fully applied.
async fn finish_physical_write(
    entity: &Entity<CodeDocument>,
    executed: ExecutedWrite,
    cx: &mut AsyncApp,
) {
    let ExecutedWrite {
        write,
        outcome,
        new_baseline,
    } = executed;

    match outcome {
        WriteOutcome::Written => {
            let kind = write.kind;
            let saved_input = write.saved_input;
            let path = write.path;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.physical_writes.adopt_baseline(new_baseline);

                    match kind {
                        WriteKind::Explicit => {
                            let landed = doc.mark_clean_against(&saved_input, cx);
                            doc.report_save_outcome(landed, cx);
                        }
                        WriteKind::Auto => {
                            doc.reconcile_after_auto_save(&saved_input, cx);
                            doc.show_saved_label(cx);
                        }
                        WriteKind::SaveAs { used_fallback } => {
                            if let Some(scratch) = doc.session.scratch_path.take()
                                && let Err(e) = std::fs::remove_file(&scratch)
                            {
                                log::warn!("Failed to remove scratch {}: {e}", scratch.display());
                            }

                            doc.editor.path = Some(path.clone());
                            let landed = doc.mark_clean_against(&saved_input, cx);
                            doc.report_save_outcome(landed, cx);

                            doc.app_state.update(cx, |state, cx| {
                                state.record_recent_file(path.clone());
                                cx.emit(dbflux_ui_base::AppStateChanged);
                            });

                            if used_fallback {
                                dbflux_ui_base::toast::Toast::warning(dbflux_i18n::t!(
                                    "document.code.file_ops.native_picker_fallback",
                                    path = path.display().to_string()
                                ))
                                .meta_right(dbflux_ui_base::toast::now_hms())
                                .push(cx);
                            }
                        }
                    }

                    doc.pump_physical_writes(cx);
                });
            })
            .log_if_dropped();
        }
        WriteOutcome::ExternalConflict => {
            report_error_async(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!(
                        "document.code.file_ops.error.auto_save_failed",
                        path = write.path.display().to_string()
                    ),
                )
                .with_cause(
                    "the file changed outside dbflux after the last save; keeping the buffer dirty instead of overwriting it",
                ),
                cx,
            );
            start_next_physical_write(entity, cx);
        }
        WriteOutcome::ExternallyDeleted => {
            report_error_async(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!(
                        "document.code.file_ops.error.auto_save_failed",
                        path = write.path.display().to_string()
                    ),
                )
                .with_cause(
                    "the file was removed outside dbflux; keeping the buffer dirty instead of recreating it",
                ),
                cx,
            );
            start_next_physical_write(entity, cx);
        }
        WriteOutcome::BaselineUnknown => {
            report_error_async(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!(
                        "document.code.file_ops.error.auto_save_failed",
                        path = write.path.display().to_string()
                    ),
                )
                .with_cause(
                    "no trustworthy on-disk baseline was loaded for this file; refusing to overwrite or recreate it",
                ),
                cx,
            );
            start_next_physical_write(entity, cx);
        }
        WriteOutcome::Failed(e) => {
            match write.kind {
                WriteKind::Explicit | WriteKind::SaveAs { .. } => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            dbflux_i18n::t!("document.code.file_ops.error.save_failed", error = e),
                        ),
                        cx,
                    );
                    report_save_failed(entity, cx);
                }
                WriteKind::Auto => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            dbflux_i18n::t!(
                                "document.code.file_ops.error.auto_save_failed",
                                path = write.path.display().to_string()
                            ),
                        )
                        .with_cause(format!("{e}")),
                        cx,
                    );
                }
            }
            start_next_physical_write(entity, cx);
        }
    }
}

/// Frees the queue slot and starts the next queued write, if any.
fn start_next_physical_write(entity: &Entity<CodeDocument>, cx: &mut AsyncApp) {
    cx.update(|cx| {
        entity.update(cx, |doc, cx| {
            doc.pump_physical_writes(cx);
        });
    })
    .log_if_dropped();
}

impl CodeDocument {
    /// Saves as part of an interrupted close.
    ///
    /// The tab is meant to close, but only once the write lands: this marks the
    /// save as close-driven, and `report_save_outcome` asks the workspace to
    /// close the tab when it succeeds. A dismissed Save As, a failed write, or
    /// a buffer the user kept typing in clears the mark, so the tab keeps its
    /// changes. Repeating the request while a save is in flight is intentional:
    /// that save now reports to the close the second request asked for.
    pub fn save_for_close(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        self.close_after_save = true;
        // A code document always has a save path: file-backed buffers write in
        // place, an untitled buffer redirects to Save As.
        self.save_file(window, cx);
        true
    }

    /// Reports a finished save to the workspace.
    ///
    /// Only a save the interrupted-close flow started, and only one that
    /// actually landed, asks for the tab to close; every other outcome drops
    /// that intent so a later manual save cannot close a tab the user kept.
    pub(super) fn report_save_outcome(&mut self, succeeded: bool, cx: &mut Context<Self>) {
        let close_after_save = std::mem::take(&mut self.close_after_save);

        cx.emit(DocumentEvent::SaveFinished { succeeded });

        if succeeded && close_after_save {
            cx.emit(DocumentEvent::RequestClose);
        }
    }

    /// Save to the current path. If no path is set, redirects to Save As.
    pub fn save_file(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self.editor.path.clone() else {
            self.save_file_as(window, cx);
            return;
        };

        // The buffer may change while the write is in flight; the save only
        // clears the dirty state for the text that actually landed. Compared in
        // buffer terms, not file terms, because the written bytes also carry the
        // execution-context annotation header.
        let saved_input = self.editor.input_state.read(cx).value().to_string();
        let content = self.build_file_content(cx);

        // The queue serializes this with any autosave or Save As already in
        // flight, so a Ctrl+S can never interleave with another write.
        self.enqueue_physical_write(PhysicalWrite::explicit(path, content, saved_input), cx);
    }

    /// Open a "Save As" dialog and save to the chosen path.
    pub fn save_file_as(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        // The dialog can stay open long enough for the user to type: only the
        // text captured here is written, so only that text may be marked clean.
        let saved_input = self.editor.input_state.read(cx).value().to_string();
        let content = self.build_file_content(cx);
        let default_ext = self.effective_language().default_extension().to_string();
        let language_name = self.effective_language().display_name().to_string();

        let suggested_name = if let Some(path) = &self.editor.path {
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("untitled")
                .to_string()
        } else {
            let title = self.title.trim();

            if title.is_empty() {
                format!("untitled.{}", default_ext)
            } else if title.contains('.') {
                title.to_string()
            } else {
                format!("{}.{}", title, default_ext)
            }
        };

        let entity = cx.entity().clone();
        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        self._pending_save = Some(cx.spawn(async move |_this, cx| {
            let target: Option<(std::path::PathBuf, bool)> = if dialog_available {
                let file_handle = rfd::AsyncFileDialog::new()
                    .set_title(dbflux_i18n::t!("document.code.file_ops.save_as.title"))
                    .set_file_name(&suggested_name)
                    .add_filter(&language_name, &[&default_ext])
                    .add_filter(
                        dbflux_i18n::t!("document.code.file_ops.save_as.all_files"),
                        &["*"],
                    )
                    .save_file()
                    .await;

                file_handle.map(|handle| (handle.path().to_path_buf(), false))
            } else {
                match dbflux_ui_base::file_dialog::fallback_export_dir() {
                    Ok(dir) => Some((
                        dbflux_ui_base::file_dialog::unique_path_in(&dir, &suggested_name),
                        true,
                    )),
                    Err(err) => {
                        report_error_async(
                            UserFacingError::new(
                                ErrorKind::Storage,
                                dbflux_i18n::t!(
                                    "document.code.file_ops.error.dialog_unavailable",
                                    error = err
                                ),
                            ),
                            cx,
                        );
                        report_save_failed(&entity, cx);
                        return;
                    }
                }
            };

            let Some((path, used_fallback)) = target else {
                // Native dialog was available and user cancelled — no toast.
                report_save_failed(&entity, cx);
                return;
            };

            // The write itself joins the document's physical-write queue: it
            // lands on the background executor after any save in flight, and
            // the queue's completion retargets the document at the new path.
            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.enqueue_physical_write(
                        PhysicalWrite::save_as(path, content, saved_input, used_fallback),
                        cx,
                    );
                });
            })
            .log_if_dropped();
        }));
    }

    // === Auto-save (session persistence) ===

    /// Write scratch content to disk so session restore can find it.
    pub fn initial_auto_save(&self, cx: &App) {
        if self.is_file_backed() {
            return;
        }

        let Some(target) = self.session.scratch_path.as_ref() else {
            return;
        };

        let content = self.build_file_content(cx);

        if let Err(e) = std::fs::write(target, &content) {
            log::error!("Initial auto-save failed for {}: {}", target.display(), e);
        }
    }

    /// Schedule an auto-save after a 2-second debounce. Resets on each call.
    pub fn schedule_auto_save(&mut self, cx: &mut Context<Self>) {
        let auto_save_ms = self
            .app_state
            .read(cx)
            .general_settings()
            .auto_save_interval_ms;

        if self.is_file_backed() {
            let Some(path) = self.editor.path.clone() else {
                return;
            };

            // Captured now, like the shadow-only autosave this replaces: later
            // edits reset the debounce and arm a fresh capture.
            let saved_input = self.editor.input_state.read(cx).value().to_string();
            let content = self.build_file_content(cx);
            let shadow = self.session.shadow_path.clone();
            let entity = cx.entity().clone();

            self.session._auto_save_debounce = Some(cx.spawn(async move |_this, cx| {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(auto_save_ms))
                    .await;

                // The autosave writes the real file through the physical-write
                // queue, so it serializes with explicit saves and Save As and
                // is conflict-checked against the bytes this document owns.
                cx.update(|cx| {
                    entity.update(cx, |doc, cx| {
                        doc.enqueue_physical_write(
                            PhysicalWrite::auto(path, content, saved_input, shadow),
                            cx,
                        );
                    });
                })
                .log_if_dropped();
            }));

            return;
        }

        let Some(target) = self.session.scratch_path.clone() else {
            return;
        };

        let content = self.build_file_content(cx);
        let entity = cx.entity().clone();

        self.session._auto_save_debounce = Some(cx.spawn(async move |_this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_millis(auto_save_ms))
                .await;

            let write_result = cx
                .background_executor()
                .spawn({
                    let target = target.clone();
                    async move { std::fs::write(&target, &content) }
                })
                .await;

            match write_result {
                Ok(()) => {
                    log::debug!("Auto-saved to {}", target.display());
                    cx.update(|cx| {
                        entity.update(cx, |doc, cx| {
                            doc.show_saved_label(cx);
                        });
                    })
                    .ok();
                }
                Err(e) => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            dbflux_i18n::t!(
                                "document.code.file_ops.error.auto_save_failed",
                                path = target.display().to_string()
                            ),
                        )
                        .with_cause(format!("{e}")),
                        cx,
                    );
                }
            }
        }));
    }

    fn show_saved_label(&mut self, cx: &mut Context<Self>) {
        self.session.show_saved_label = true;
        cx.notify();

        self.session._saved_label_timer = Some(cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_secs(3))
                .await;

            cx.update(|cx| {
                if let Some(entity) = this.upgrade() {
                    entity.update(cx, |doc, cx| {
                        doc.session.show_saved_label = false;
                        cx.notify();
                    });
                }
            })
            .ok();
        }));
    }

    /// Reconciles dirty state after an autosave landed, without save/close
    /// events.
    ///
    /// An autosave is not a user-visible save: it never emits `SaveFinished`,
    /// so the close flow is untouched. The buffer is marked clean only against
    /// the text that actually landed; newer edits stay pending, and a debounce
    /// armed for them keeps running — the next autosave carries them once this
    /// write has finished.
    pub(super) fn reconcile_after_auto_save(
        &mut self,
        saved_input: &str,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.editor.input_state.read(cx).value() != saved_input {
            self.editor.original_content = saved_input.to_string();
            self.editor.is_dirty = true;
            cx.emit(DocumentEvent::MetaChanged);
            cx.notify();
            return false;
        }

        self.mark_clean(cx);
        true
    }

    /// Queues one write to the real file, starting it immediately when the
    /// queue is idle.
    fn enqueue_physical_write(&mut self, write: PhysicalWrite, cx: &mut Context<Self>) {
        if self.physical_writes.push(write) {
            self.pump_physical_writes(cx);
        }
    }

    /// Frees the running slot and starts the next queued write, if any.
    ///
    /// Every write's completion runs this inside its entity update, so the
    /// next write starts only after the previous outcome is fully applied:
    /// two physical writes for one document can never interleave, and a
    /// running write is never replaced by cancellation — it always lands.
    fn pump_physical_writes(&mut self, cx: &mut Context<Self>) {
        self.physical_writes.mark_finished();

        let Some(write) = self.physical_writes.next_to_start() else {
            return;
        };
        let baseline = self.physical_writes.baseline().cloned();

        let entity = cx.entity().clone();
        cx.spawn(async move |_this, cx| {
            let executed = cx
                .background_executor()
                .spawn(async move { execute_write(write, baseline.as_ref()) })
                .await;

            finish_physical_write(&entity, executed, cx).await;
        })
        .detach();
    }

    /// Flush auto-save content synchronously (called before closing a tab).
    pub fn flush_auto_save(&self, cx: &App) {
        let target = if self.is_file_backed() {
            self.session.shadow_path.as_ref()
        } else {
            self.session.scratch_path.as_ref()
        };

        let Some(target) = target else {
            return;
        };

        let content = self.build_file_content(cx);

        if let Err(e) = std::fs::write(target, &content) {
            log::error!("Flush auto-save failed for {}: {}", target.display(), e);
        }
    }

    // === Explicit save (Ctrl+S) ===

    /// Build the full file content, prepending execution context metadata.
    pub fn build_file_content(&self, cx: &App) -> String {
        let editor_content = self.editor.input_state.read(cx).value().to_string();

        build_file_content_for_language(
            &editor_content,
            &self.source.exec_ctx,
            self.supports_connection_context(),
            self.comment_prefix(),
        )
    }

    /// Strip existing annotation comments from the beginning of content.
    fn strip_existing_annotations<'a>(content: &'a str, prefix: &str) -> &'a str {
        let mut last_annotation_end = 0;

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.is_empty() {
                last_annotation_end += line.len() + 1; // +1 for newline
                continue;
            }

            if let Some(after_prefix) = trimmed.strip_prefix(prefix)
                && after_prefix.trim().starts_with('@')
            {
                last_annotation_end += line.len() + 1;
                continue;
            }

            break;
        }

        if last_annotation_end >= content.len() {
            ""
        } else {
            &content[last_annotation_end..]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::build_file_content_for_language;
    use dbflux_core::{ExecutionContext, ExecutionSourceContext};
    use uuid::Uuid;

    fn collection_window_exec_ctx() -> ExecutionContext {
        ExecutionContext {
            connection_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            database: Some("logs".into()),
            schema: None,
            container: None,
            source: Some(ExecutionSourceContext::CollectionWindow {
                targets: vec!["/aws/lambda/app".into()],
                start_ms: 10,
                end_ms: 20,
                query_mode: Some("cwli".into()),
            }),
        }
    }

    #[test]
    fn file_headers_remain_relational_only_when_source_window_exists() {
        let exec_ctx = collection_window_exec_ctx();

        let content = build_file_content_for_language("SELECT 1;", &exec_ctx, true, "--");

        assert!(content.contains("-- @connection:"));
        assert!(content.contains("-- @database: logs"));
        assert!(!content.contains("log_groups"));
        assert!(!content.contains("start_ms"));
        assert!(!content.contains("end_ms"));
    }

    #[test]
    fn connection_backed_custom_surface_emits_header_with_profile_prefix() {
        // A connection-backed DynamoDB editor: profile reports connection support
        // and a `--` prefix even though its raw `QueryLanguage` is `Custom`.
        let exec_ctx = collection_window_exec_ctx();

        let content = build_file_content_for_language("SELECT * FROM \"t\"", &exec_ctx, true, "--");

        assert!(content.contains("-- @connection:"));
        assert!(content.contains("-- @database: logs"));
    }

    #[test]
    fn no_header_when_surface_is_not_connection_backed() {
        let exec_ctx = collection_window_exec_ctx();

        let content = build_file_content_for_language("print('hi')", &exec_ctx, false, "#");

        assert_eq!(content, "print('hi')");
    }

    #[test]
    fn file_ops_keys_resolve_in_both_locales() {
        let keys = [
            "document.code.file_ops.save_as.title",
            "document.code.file_ops.save_as.all_files",
            "document.code.file_ops.error.save_failed",
            "document.code.file_ops.error.dialog_unavailable",
            "document.code.file_ops.error.save_script_failed",
            "document.code.file_ops.error.auto_save_failed",
            "document.code.file_ops.native_picker_fallback",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn file_ops_save_failed_interpolates_error() {
        let en = dbflux_i18n::t!(
            "document.code.file_ops.error.save_failed",
            locale = "en",
            error = "disk full"
        );

        assert_eq!(en, "Failed to save file: disk full");
    }

    #[test]
    fn file_ops_save_as_title_differs_between_locales() {
        let en = dbflux_i18n::t!("document.code.file_ops.save_as.title", locale = "en");
        let es = dbflux_i18n::t!("document.code.file_ops.save_as.title", locale = "es");

        assert_ne!(en, es);
    }
}
