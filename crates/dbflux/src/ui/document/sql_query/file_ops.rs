use super::*;

impl SqlQueryDocument {
    /// Save to the current path. If no path is set, redirects to Save As.
    pub fn save_file(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self.path.clone() else {
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
                    log::error!("Failed to save file: {}", e);
                }
            }
        }));
    }

    /// Open a "Save As" dialog and save to the chosen path.
    pub fn save_file_as(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let content = self.build_file_content(cx);
        let default_ext = self.query_language.default_extension();
        let language_name = self.query_language.display_name();

        let suggested_name = if let Some(path) = &self.path {
            path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("untitled")
                .to_string()
        } else {
            format!("untitled.{}", default_ext)
        };

        let entity = cx.entity().clone();
        let app_state = self.app_state.clone();

        self._pending_save = Some(cx.spawn(async move |_this, cx| {
            let file_handle = rfd::AsyncFileDialog::new()
                .set_title("Save Script As")
                .set_file_name(&suggested_name)
                .add_filter(language_name, &[default_ext])
                .add_filter("All Files", &["*"])
                .save_file()
                .await;

            let Some(handle) = file_handle else {
                return;
            };

            let path = handle.path().to_path_buf();
            let write_result = std::fs::write(&path, &content);

            match write_result {
                Ok(()) => {
                    cx.update(|cx| {
                        entity.update(cx, |doc, cx| {
                            if let Some(scratch) = doc.scratch_path.take()
                                && let Some(store) = doc.app_state.read(cx).session_store()
                            {
                                store.delete(&scratch);
                            }

                            doc.path = Some(path.clone());
                            doc.mark_clean(cx);
                        });

                        app_state.update(cx, |state, cx| {
                            state.record_recent_file(path);
                            cx.emit(crate::app::AppStateChanged);
                        });
                    })
                    .ok();
                }
                Err(e) => {
                    log::error!("Failed to save file: {}", e);
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

        let Some(target) = self.scratch_path.as_ref() else {
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
            self.shadow_path.clone()
        } else {
            self.scratch_path.clone()
        };

        let Some(target) = target else {
            return;
        };

        let content = self.build_file_content(cx);
        let entity = cx.entity().clone();

        self._auto_save_debounce = Some(cx.spawn(async move |_this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_secs(2))
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
        self.show_saved_label = true;
        cx.notify();

        self._saved_label_timer = Some(cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_secs(3))
                .await;

            cx.update(|cx| {
                if let Some(entity) = this.upgrade() {
                    entity.update(cx, |doc, cx| {
                        doc.show_saved_label = false;
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
            self.shadow_path.as_ref()
        } else {
            self.scratch_path.as_ref()
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
    fn build_file_content(&self, cx: &App) -> String {
        let editor_content = self.input_state.read(cx).value().to_string();

        let header = self.exec_ctx.to_comment_header(self.query_language);
        if header.is_empty() {
            return editor_content;
        }

        // If the content already starts with existing annotations, strip them
        let body = Self::strip_existing_annotations(&editor_content, self.query_language);
        format!("{}\n{}", header, body)
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
