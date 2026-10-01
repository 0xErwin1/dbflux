use crate::*;
use dbflux_ui_base::app_state_entity::rescan_scripts_in_background;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

/// Reports a script file operation the user just triggered that failed, such as
/// a create, rename, move or delete refused by the filesystem or by the
/// scripts-folder boundaries.
pub(crate) fn report_script_operation_failure(error: impl std::fmt::Display, cx: &mut App) {
    report_error(
        UserFacingError::new(
            ErrorKind::User,
            crate::labels::script_operation_failed_label(&error.to_string()),
        ),
        cx,
    );
}

/// The error kind a failed external-folder change is reported under: a folder
/// the user picked that cannot be one is their input; a write that failed is
/// storage.
fn script_root_error_kind(error: &dbflux_app::app_state::ScriptRootError) -> ErrorKind {
    match error {
        dbflux_app::app_state::ScriptRootError::Storage(_) => ErrorKind::Storage,
        dbflux_app::app_state::ScriptRootError::Invalid(_) => ErrorKind::User,
        dbflux_app::app_state::ScriptRootError::ScriptsUnavailable
        | dbflux_app::app_state::ScriptRootError::UnknownRoot => ErrorKind::Config,
    }
}

fn report_reveal_failure(error: std::io::Error, cx: &mut App) {
    report_error(
        UserFacingError::new(
            ErrorKind::User,
            crate::labels::reveal_failed_label(&error.to_string()),
        ),
        cx,
    );
}

impl Sidebar {
    fn selected_scripts_parent_dir(&self, cx: &App) -> Option<std::path::PathBuf> {
        let entry = self.scripts_tree_state.read(cx).selected_entry()?;
        let item_id = entry.item().id.to_string();
        let node_id = parse_node_id(&item_id)?;

        match node_id {
            SchemaNodeId::ScriptsFolder { path: Some(p) }
            | SchemaNodeId::ScriptsRoot { path: p } => Some(std::path::PathBuf::from(p)),
            SchemaNodeId::ScriptFile { path } => std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf()),
            _ => None,
        }
    }

    fn default_script_extension(&self, cx: &App) -> &'static str {
        let state = self.app_state.read(cx);
        state
            .active_connection()
            .map(|c| c.connection.metadata().query_language.default_extension())
            .unwrap_or("sql")
    }

    /// For folders returns the folder path; for files returns the parent directory.
    pub(crate) fn parent_dir_from_item_id(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) })
            | Some(SchemaNodeId::ScriptsRoot { path: p }) => Some(std::path::PathBuf::from(p)),
            Some(SchemaNodeId::ScriptFile { path }) => std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_path_buf()),
            _ => None,
        }
    }

    pub(crate) fn create_script_file_in(
        &mut self,
        parent: Option<std::path::PathBuf>,
        cx: &mut Context<Self>,
    ) {
        let extension = self.default_script_extension(cx);
        let name = self.generate_unique_script_name(parent.as_deref(), extension, cx);

        let created = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            Some(dir.create_file(parent.as_deref(), &name, extension))
        });

        match created {
            Some(Ok(path)) => {
                self.refresh_scripts_tree(cx);
                cx.emit(SidebarEvent::OpenScript { path });
            }
            Some(Err(error)) => report_script_operation_failure(error, cx),
            None => {}
        }
    }

    pub(crate) fn create_script_file(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        self.create_script_file_in(parent, cx);
    }

    pub(crate) fn create_script_folder_in(
        &mut self,
        parent: Option<std::path::PathBuf>,
        cx: &mut Context<Self>,
    ) {
        let name = "new_folder";

        let created = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            Some(dir.create_folder(parent.as_deref(), name))
        });

        let path = match created {
            Some(Ok(path)) => path,
            Some(Err(error)) => {
                report_script_operation_failure(error, cx);
                return;
            }
            None => return,
        };

        self.refresh_scripts_tree(cx);

        let item_id = SchemaNodeId::ScriptsFolder {
            path: Some(path.to_string_lossy().to_string()),
        }
        .to_string();

        self.select_and_rename_item(&item_id, cx);
    }

    pub fn create_script_folder(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        self.create_script_folder_in(parent, cx);
    }

    pub(crate) fn import_script(&mut self, cx: &mut Context<Self>) {
        let parent = self.selected_scripts_parent_dir(cx);
        let extensions = dbflux_core::all_script_extensions();
        let app_state = self.app_state.clone();
        let sidebar = cx.entity().clone();

        let title = crate::labels::import_script_dialog_title();
        let filter_name = crate::labels::script_files_filter_label();

        let task = cx.background_executor().spawn(async move {
            let mut dialog = rfd::FileDialog::new().set_title(title.as_str());
            for ext in &extensions {
                dialog = dialog.add_filter(filter_name.as_str(), &[ext]);
            }
            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let source = match task.await {
                Some(path) => path,
                None => return,
            };

            cx.update(|cx| {
                let imported = app_state.update(cx, |state, _cx| {
                    let dir = state.scripts_directory_mut()?;
                    Some(dir.import(&source, parent.as_deref()))
                });

                match imported {
                    Some(Ok(path)) => {
                        sidebar.update(cx, |this, cx| {
                            this.refresh_scripts_tree(cx);
                            cx.emit(SidebarEvent::OpenScript { path });
                        });
                    }
                    Some(Err(error)) => report_script_operation_failure(error, cx),
                    None => {}
                }
            });
        })
        .detach();
    }

    /// Asks for a folder and registers it as an external scripts folder.
    ///
    /// The picker and the validation (canonicalizing, overlap checks) run off
    /// the UI thread; the new folder then shows as scanning until the
    /// background scan lands. Nothing is copied.
    pub fn add_external_scripts_folder(&mut self, cx: &mut Context<Self>) {
        let Some((managed_root, external_roots)) =
            self.app_state.read(cx).scripts_directory().map(|dir| {
                let external = dir
                    .external_roots()
                    .iter()
                    .map(|mounted| mounted.path().to_path_buf())
                    .collect::<Vec<_>>();
                (dir.root_path().to_path_buf(), external)
            })
        else {
            return;
        };

        let title = crate::labels::add_external_folder_dialog_title();
        let app_state = self.app_state.clone();

        cx.spawn(async move |_this, cx| {
            let Some(picked) = rfd::AsyncFileDialog::new()
                .set_title(title.as_str())
                .pick_folder()
                .await
            else {
                return;
            };

            let picked = picked.path().to_path_buf();
            let validated = cx
                .background_executor()
                .spawn(async move {
                    dbflux_core::ScriptsDirectory::validate_external_root_against(
                        &managed_root,
                        &external_roots,
                        &picked,
                    )
                })
                .await;

            cx.update(|cx| {
                let path = match validated {
                    Ok(path) => path,
                    Err(error) => {
                        report_error(
                            UserFacingError::new(
                                ErrorKind::User,
                                crate::labels::add_external_folder_failed_label(&error.to_string()),
                            ),
                            cx,
                        );
                        return;
                    }
                };

                let added = app_state.update(cx, |state, cx| {
                    let added = state.add_external_script_root(path);
                    if added.is_ok() {
                        cx.emit(AppStateChanged);
                    }
                    added
                });

                match added {
                    Ok(_) => rescan_scripts_in_background(&app_state, cx),
                    Err(error) => report_error(
                        UserFacingError::new(
                            script_root_error_kind(&error),
                            crate::labels::add_external_folder_failed_label(&error.to_string()),
                        ),
                        cx,
                    ),
                }
            });
        })
        .detach();
    }

    /// Unregisters the external scripts folder `item_id` names. The folder and
    /// its files are never touched.
    pub(crate) fn remove_external_scripts_folder(&mut self, item_id: &str, cx: &mut Context<Self>) {
        let Some(SchemaNodeId::ScriptsRoot { path }) = parse_node_id(item_id) else {
            return;
        };

        let Some(id) = self
            .app_state
            .read(cx)
            .scripts_directory()
            .and_then(|dir| dir.external_root_at(std::path::Path::new(&path)))
            .map(dbflux_core::MountedScriptRoot::id)
        else {
            return;
        };

        let removed = self.app_state.update(cx, |state, cx| {
            let removed = state.remove_external_script_root(id);
            if removed.is_ok() {
                cx.emit(AppStateChanged);
            }
            removed
        });

        if let Err(error) = removed {
            report_error(
                UserFacingError::new(
                    script_root_error_kind(&error),
                    crate::labels::remove_external_folder_failed_label(&error.to_string()),
                ),
                cx,
            );
        }
    }

    /// Re-scans every scripts root in the background, picking up files added,
    /// changed or removed outside DBFlux and folders that came back.
    pub(crate) fn rescan_scripts(&mut self, cx: &mut Context<Self>) {
        rescan_scripts_in_background(&self.app_state, cx);
    }

    pub(crate) fn handle_script_drop_with_position(
        &mut self,
        state: &ScriptsDragState,
        cx: &mut Context<Self>,
    ) {
        let Some(drop_target) = self.scripts_drop_target.take() else {
            return;
        };

        let Some(target_dir) = self.resolve_script_drop_target_dir(&drop_target, cx) else {
            return;
        };

        self.move_scripts(&state.all_paths(), &target_dir, cx);
    }

    pub(crate) fn handle_script_drop_to_root_with_position(
        &mut self,
        state: &ScriptsDragState,
        cx: &mut Context<Self>,
    ) {
        let root = match self.app_state.read(cx).scripts_directory() {
            Some(dir) => dir.root_path().to_path_buf(),
            None => return,
        };

        self.scripts_drop_target = None;
        self.move_scripts(&state.all_paths(), &root, cx);
    }

    pub(crate) fn move_selected_scripts_to_selected_folder(
        &mut self,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.scripts_multi_selection.is_empty() {
            return false;
        }

        let selected_entry = self.scripts_tree_state.read(cx).selected_entry().cloned();
        let Some(selected_entry) = selected_entry else {
            return false;
        };

        if !selected_entry.is_expanded() {
            return false;
        }

        let selected_item_id = selected_entry.item().id.to_string();
        let target_dir = self.resolve_script_drop_target_dir(
            &DropTarget {
                item_id: selected_item_id.clone(),
                position: DropPosition::Into,
            },
            cx,
        );

        let Some(target_dir) = target_dir else {
            return false;
        };

        let sources: Vec<std::path::PathBuf> = self
            .scripts_multi_selection
            .iter()
            .filter(|item_id| item_id.as_str() != selected_item_id)
            .filter_map(|item_id| match parse_node_id(item_id) {
                Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
                Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                    Some(std::path::PathBuf::from(p))
                }
                _ => None,
            })
            .collect();

        if sources.is_empty() {
            return false;
        }

        self.move_scripts(&sources, &target_dir, cx)
    }

    pub(crate) fn move_selected_scripts_out_of_folder(&mut self, cx: &mut Context<Self>) -> bool {
        if self.scripts_multi_selection.is_empty() {
            return false;
        }

        let mut sources: Vec<std::path::PathBuf> = self
            .scripts_multi_selection
            .iter()
            .filter_map(|item_id| match parse_node_id(item_id) {
                Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
                Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                    Some(std::path::PathBuf::from(p))
                }
                _ => None,
            })
            .collect();

        if sources.is_empty() {
            return false;
        }

        sources.sort();
        sources.dedup();

        let all_sources = sources.clone();
        sources.retain(|source| {
            !all_sources
                .iter()
                .any(|candidate| candidate != source && source.starts_with(candidate))
        });

        let mut parent_dirs: Vec<std::path::PathBuf> = sources
            .iter()
            .filter_map(|source| source.parent().map(std::path::Path::to_path_buf))
            .collect();

        parent_dirs.sort();
        parent_dirs.dedup();

        if parent_dirs.len() != 1 {
            return false;
        }

        let current_parent = match parent_dirs.pop() {
            Some(path) => path,
            None => return false,
        };

        let Some(dir) = self.app_state.read(cx).scripts_directory() else {
            return false;
        };

        if dir.is_root(&current_parent) {
            return false;
        }

        let Some(target_dir) = current_parent.parent().map(std::path::Path::to_path_buf) else {
            return false;
        };

        self.move_scripts(&sources, &target_dir, cx)
    }

    fn resolve_script_drop_target_dir(
        &self,
        drop_target: &DropTarget,
        cx: &Context<Self>,
    ) -> Option<std::path::PathBuf> {
        let root = self
            .app_state
            .read(cx)
            .scripts_directory()
            .map(|dir| dir.root_path().to_path_buf());

        let target_path = match parse_node_id(&drop_target.item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
            Some(SchemaNodeId::ScriptsFolder { path: None }) => root.clone(),
            // A root has no siblings to land between: anything dropped on it
            // goes inside it.
            Some(SchemaNodeId::ScriptsRoot { path }) => {
                return Some(std::path::PathBuf::from(path));
            }
            _ => None,
        }?;

        match drop_target.position {
            DropPosition::Into => {
                if target_path.is_dir() {
                    Some(target_path)
                } else {
                    target_path.parent().map(std::path::Path::to_path_buf)
                }
            }
            DropPosition::Before | DropPosition::After => target_path
                .parent()
                .map(std::path::Path::to_path_buf)
                .or(root.clone()),
        }
    }

    fn move_scripts(
        &mut self,
        sources: &[std::path::PathBuf],
        target_dir: &std::path::Path,
        cx: &mut Context<Self>,
    ) -> bool {
        let mut normalized_sources = sources.to_vec();
        normalized_sources.sort();
        normalized_sources.dedup();

        let all_sources = normalized_sources.clone();
        normalized_sources.retain(|source| {
            !all_sources
                .iter()
                .any(|candidate| candidate != source && source.starts_with(candidate))
        });

        let mut moved_any = false;
        let mut first_error = None;
        self.app_state.update(cx, |state, _cx| {
            let Some(dir) = state.scripts_directory_mut() else {
                return;
            };

            for source in &normalized_sources {
                if source == target_dir {
                    continue;
                }

                if source.parent() == Some(target_dir) {
                    continue;
                }

                match dir.move_entry(source, target_dir) {
                    Ok(_) => moved_any = true,
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        });

        if let Some(error) = first_error {
            report_script_operation_failure(error, cx);
        }

        if moved_any {
            self.refresh_scripts_tree(cx);
        }

        moved_any
    }

    pub(crate) fn delete_script(&mut self, path: &std::path::Path, cx: &mut Context<Self>) {
        let path = path.to_path_buf();
        let result = self.app_state.update(cx, |state, _cx| {
            Some(state.scripts_directory_mut()?.delete(&path))
        });

        match result {
            Some(Ok(())) => self.refresh_scripts_tree(cx),
            Some(Err(error)) => report_script_operation_failure(error, cx),
            None => {}
        }
    }

    fn resolve_script_path(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) })
            | Some(SchemaNodeId::ScriptsRoot { path: p }) => Some(std::path::PathBuf::from(p)),
            Some(SchemaNodeId::ScriptsFolder { path: None }) => {
                dirs::data_dir().map(|d| d.join("dbflux").join("scripts"))
            }
            _ => None,
        }
    }

    pub(crate) fn reveal_in_file_manager(&self, item_id: &str, cx: &mut Context<Self>) {
        let Some(path) = Self::resolve_script_path(item_id) else {
            return;
        };

        #[cfg(target_os = "macos")]
        {
            if path.is_file() {
                if let Err(e) = std::process::Command::new("open")
                    .arg("-R")
                    .arg(&path)
                    .spawn()
                {
                    report_reveal_failure(e, cx);
                }
            } else if let Err(e) = std::process::Command::new("open").arg(&path).spawn() {
                report_reveal_failure(e, cx);
            }
        }

        #[cfg(target_os = "windows")]
        {
            if path.is_file() {
                let select_arg = format!("/select,{}", path.display());
                if let Err(e) = std::process::Command::new("explorer")
                    .arg(&select_arg)
                    .spawn()
                {
                    report_reveal_failure(e, cx);
                }
            } else if let Err(e) = std::process::Command::new("explorer").arg(&path).spawn() {
                report_reveal_failure(e, cx);
            }
        }

        #[cfg(target_os = "linux")]
        {
            let target = if path.is_file() {
                path.parent().unwrap_or(&path).to_path_buf()
            } else {
                path
            };

            if let Err(_e) = std::process::Command::new("xdg-open").arg(&target).spawn()
                && let Err(e) = std::process::Command::new("gio")
                    .arg("open")
                    .arg(&target)
                    .spawn()
            {
                report_reveal_failure(e, cx);
            }
        }
    }

    pub(crate) fn copy_path_to_clipboard(&self, item_id: &str, cx: &mut Context<Self>) {
        let Some(path) = Self::resolve_script_path(item_id) else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(
            path.to_string_lossy().to_string(),
        ));
    }

    fn generate_unique_script_name(
        &self,
        parent: Option<&std::path::Path>,
        extension: &str,
        cx: &App,
    ) -> String {
        let state = self.app_state.read(cx);
        let dir = match state.scripts_directory() {
            Some(d) => d,
            None => return format!("untitled.{}", extension),
        };

        let base_dir = parent.unwrap_or_else(|| dir.root_path());

        for i in 1u32.. {
            let name = if i == 1 {
                format!("untitled.{}", extension)
            } else {
                format!("untitled_{}.{}", i, extension)
            };

            if !base_dir.join(&name).exists() {
                return name;
            }
        }

        format!("untitled.{}", extension)
    }
}
