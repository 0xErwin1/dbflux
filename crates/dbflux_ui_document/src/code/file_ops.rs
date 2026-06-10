use super::*;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};

fn build_file_content_for_language(
    editor_content: &str,
    exec_ctx: &ExecutionContext,
    language: QueryLanguage,
) -> String {
    if !language.supports_connection_context() {
        return editor_content.to_string();
    }

    let header = exec_ctx.to_comment_header(language.clone());
    if header.is_empty() {
        return editor_content.to_string();
    }

    let body = CodeDocument::strip_existing_annotations(editor_content, language);
    format!("{}\n{}", header, body)
}

impl CodeDocument {
    /// Save to the current path. If no path is set, redirects to Save As.
    pub fn save_file(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self.editor.path.clone() else {
            self.save_file_as(window, cx);
            return;
        };

        let content = self.build_file_content(cx);

        let entity = cx.entity().clone();
        self._pending_save = Some(cx.spawn(async move |_this, cx| {
            let write_result = cx.background_executor().spawn({
                let path = path.clone();
                async move { std::fs::write(&path, &content) }
            });

            match write_result.await {
                Ok(()) => {
                    cx.update(|cx| {
                        entity.update(cx, |doc, cx| {
                            doc.mark_clean(cx);
                        });
                    })
                    .ok();
                }
                Err(e) => {
                    report_error_async(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            format!("Failed to save file: {e}"),
                        ),
                        cx,
                    );
                }
            }
        }));
    }

    /// Open a "Save As" dialog and save to the chosen path.
    pub fn save_file_as(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let content = self.build_file_content(cx);
        let default_ext = self.editor.query_language.default_extension().to_string();
        let language_name = self.editor.query_language.display_name().to_string();

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
        let app_state = self.app_state.clone();
        let dialog_available = dbflux_ui_base::file_dialog::is_native_file_dialog_available();

        self._pending_save = Some(cx.spawn(async move |_this, cx| {
            let target: Option<(std::path::PathBuf, bool)> = if dialog_available {
                let file_handle = rfd::AsyncFileDialog::new()
                    .set_title("Save Script As")
                    .set_file_name(&suggested_name)
                    .add_filter(&language_name, &[&default_ext])
                    .add_filter("All Files", &["*"])
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
                                format!(
                                    "Save failed — file dialog unavailable and fallback directory could not be created: {err}"
                                ),
                            ),
                            cx,
                        );
                        return;
                    }
                }
            };

            let Some((path, used_fallback)) = target else {
                // Native dialog was available and user cancelled — no toast.
                return;
            };

            let write_result = std::fs::write(&path, &content);

            match write_result {
                Ok(()) => {
                    let path_for_update = path.clone();
                    cx.update(|cx| {
                        entity.update(cx, |doc, cx| {
                            if let Some(scratch) = doc.session.scratch_path.take() {
                                let _ = std::fs::remove_file(&scratch);
                            }

                            doc.editor.path = Some(path_for_update.clone());
                            doc.mark_clean(cx);
                        });

                        app_state.update(cx, |state, cx| {
                            state.record_recent_file(path_for_update.clone());
                            cx.emit(dbflux_ui_base::AppStateChanged);
                        });

                        if used_fallback {
                            dbflux_ui_base::toast::Toast::warning(format!(
                                "Native file picker unavailable — script saved to {} instead. Install xdg-desktop-portal, zenity, or kdialog for a save dialog.",
                                path_for_update.display()
                            ))
                            .meta_right(dbflux_ui_base::toast::now_hms())
                            .push(cx);
                        }
                    })
                    .ok();
                }
                Err(e) => {
                    report_error_async(
                        UserFacingError::new(ErrorKind::Storage, format!("Failed to save script: {e}")),
                        cx,
                    );
                }
            }
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
        let target = if self.is_file_backed() {
            self.session.shadow_path.clone()
        } else {
            self.session.scratch_path.clone()
        };

        let Some(target) = target else {
            return;
        };

        let content = self.build_file_content(cx);
        let entity = cx.entity().clone();
        let auto_save_ms = self
            .app_state
            .read(cx)
            .general_settings()
            .auto_save_interval_ms;

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
                    log::error!("Auto-save failed for {}: {}", target.display(), e);
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
            self.editor.query_language.clone(),
        )
    }

    /// Strip existing annotation comments from the beginning of content.
    fn strip_existing_annotations(content: &str, language: QueryLanguage) -> &str {
        let prefix = language.comment_prefix();
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
    use dbflux_core::{ExecutionContext, ExecutionSourceContext, QueryLanguage};
    use uuid::Uuid;

    #[test]
    fn file_headers_remain_relational_only_when_source_window_exists() {
        let exec_ctx = ExecutionContext {
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
        };

        let content = build_file_content_for_language("SELECT 1;", &exec_ctx, QueryLanguage::Sql);

        assert!(content.contains("-- @connection:"));
        assert!(content.contains("-- @database: logs"));
        assert!(!content.contains("log_groups"));
        assert!(!content.contains("start_ms"));
        assert!(!content.contains("end_ms"));
    }
}
