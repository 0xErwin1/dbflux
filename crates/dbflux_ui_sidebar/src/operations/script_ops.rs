use crate::*;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};

fn report_reveal_failure(error: std::io::Error, cx: &mut App) {
    report_error(
        UserFacingError::new(ErrorKind::User, "Failed to reveal in file manager")
            .with_cause(error.to_string()),
        cx,
    );
}

impl Sidebar {
    fn selected_scripts_parent_dir(&self, cx: &App) -> Option<std::path::PathBuf> {
        let entry = self.scripts_tree_state.read(cx).selected_entry()?;
        let item_id = entry.item().id.to_string();
        let node_id = parse_node_id(&item_id)?;

        match node_id {
            SchemaNodeId::ScriptsFolder { path: Some(p) } => Some(std::path::PathBuf::from(p)),
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
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
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

        let path = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            dir.create_file(parent.as_deref(), &name, extension).ok()
        });

        if let Some(path) = path {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);

            cx.emit(SidebarEvent::OpenScript { path });
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

        let created_path = self.app_state.update(cx, |state, _cx| {
            let dir = state.scripts_directory_mut()?;
            dir.create_folder(parent.as_deref(), name).ok()
        });

        let Some(path) = created_path else {
            return;
        };

        self.app_state.update(cx, |state, _cx| {
            state.refresh_scripts();
        });
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

        let task = cx.background_executor().spawn(async move {
            let mut dialog = rfd::FileDialog::new().set_title("Import Script");
            for ext in &extensions {
                dialog = dialog.add_filter("Script files", &[ext]);
            }
            dialog.pick_file()
        });

        cx.spawn(async move |_this, cx| {
            let source = match task.await {
                Some(path) => path,
                None => return,
            };

            if let Err(error) = cx.update(|cx| {
                let path = app_state.update(cx, |state, _cx| {
                    let dir = state.scripts_directory_mut()?;
                    let imported = dir.import(&source, parent.as_deref()).ok()?;
                    state.refresh_scripts();
                    Some(imported)
                });

                if let Some(path) = path {
                    sidebar.update(cx, |this, cx| {
                        this.refresh_scripts_tree(cx);
                        cx.emit(SidebarEvent::OpenScript { path });
                    });
                }
            }) {
                log::warn!(
                    "Failed to apply imported script state to sidebar: {:?}",
                    error
                );
            }
        })
        .detach();
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

        let root = match self.app_state.read(cx).scripts_directory() {
            Some(dir) => dir.root_path().to_path_buf(),
            None => return false,
        };

        if current_parent == root {
            return false;
        }

        let target_dir = current_parent
            .parent()
            .map(std::path::Path::to_path_buf)
            .unwrap_or(root);

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

                if dir.move_entry(source, target_dir).is_ok() {
                    moved_any = true;
                }
            }
        });

        if moved_any {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }

        moved_any
    }

    pub(crate) fn delete_script(&mut self, path: &std::path::Path, cx: &mut Context<Self>) {
        let path = path.to_path_buf();
        let result = self.app_state.update(cx, |state, _cx| {
            state.scripts_directory_mut()?.delete(&path).ok()
        });

        if result.is_some() {
            self.app_state.update(cx, |state, _cx| {
                state.refresh_scripts();
            });
            self.refresh_scripts_tree(cx);
        }
    }

    fn resolve_script_path(item_id: &str) -> Option<std::path::PathBuf> {
        match parse_node_id(item_id) {
            Some(SchemaNodeId::ScriptFile { path }) => Some(std::path::PathBuf::from(path)),
            Some(SchemaNodeId::ScriptsFolder { path: Some(p) }) => {
                Some(std::path::PathBuf::from(p))
            }
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
