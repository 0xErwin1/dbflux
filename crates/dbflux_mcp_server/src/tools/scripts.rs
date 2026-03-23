//! Script management tools for MCP server.
//!
//! Provides CRUD operations and execution for database scripts stored in
//! the platform-specific scripts directory (~/.local/share/dbflux/scripts/).
//!
//! All tools use `ScriptsDirectory` from `dbflux_core` to manage file operations.

use crate::{
    DbFluxServer,
    helper::IntoErrorData,
    state::ServerState,
};
use dbflux_core::{QueryLanguage, QueryRequest};
use rmcp::{
    ErrorData,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content},
    schemars::JsonSchema,
    tool,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListScriptsParams {
    #[schemars(description = "Optional subfolder path to list (relative to scripts root)")]
    pub path: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetScriptParams {
    #[schemars(description = "Path to the script file (relative to scripts root)")]
    pub path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateScriptParams {
    #[schemars(description = "Script name (without extension)")]
    pub name: String,

    #[schemars(description = "Script content")]
    pub content: String,

    #[schemars(description = "File extension (e.g., 'sql', 'js', 'lua', 'py', 'sh')")]
    pub extension: String,

    #[schemars(description = "Optional subfolder path (relative to scripts root)")]
    pub folder: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpdateScriptParams {
    #[schemars(description = "Path to the script file (relative to scripts root)")]
    pub path: String,

    #[schemars(description = "New script content")]
    pub content: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteScriptParams {
    #[schemars(description = "Path to the script file (relative to scripts root)")]
    pub path: String,

    #[schemars(description = "Confirmation string - must match filename (not full path)")]
    pub confirm: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExecuteScriptParams {
    #[schemars(description = "Path to the script file (relative to scripts root)")]
    pub path: String,

    #[schemars(description = "Connection ID from DBFlux configuration")]
    pub connection_id: String,
}

#[derive(Debug, Serialize)]
struct ScriptEntryDto {
    path: String,
    name: String,
    #[serde(rename = "type")]
    entry_type: String,
    extension: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScriptContentDto {
    path: String,
    name: String,
    content: String,
    language: String,
    size: usize,
}

#[derive(Debug, Serialize)]
struct ScriptCreatedDto {
    path: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct ScriptUpdatedDto {
    path: String,
    size: usize,
}

pub const DELETE_CONFIRMATION_ERROR: &str =
    "Confirmation string must match filename (not full path)";

pub fn validate_delete_params(params: &DeleteScriptParams) -> Result<(), String> {
    let path = Path::new(&params.path);
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or("Invalid path")?;

    if params.confirm != filename {
        return Err(DELETE_CONFIRMATION_ERROR.to_string());
    }
    Ok(())
}

impl DbFluxServer {
    #[tool(description = "List scripts in the scripts directory")]
    async fn list_scripts(
        &self,
        Parameters(params): Parameters<ListScriptsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let path = params.path.clone();

        self.governance
            .authorize_and_execute(
                "list_scripts",
                None, // Global operation
                ExecutionClassification::Metadata,
                move || async move {
                    let result = Self::list_scripts_impl(state, path.as_deref())
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Get script content and metadata")]
    async fn get_script(
        &self,
        Parameters(params): Parameters<GetScriptParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let path = params.path.clone();

        self.governance
            .authorize_and_execute(
                "get_script",
                None, // Global operation
                ExecutionClassification::Read,
                move || async move {
                    let result = Self::get_script_impl(state, &path)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Create a new script file")]
    async fn create_script(
        &self,
        Parameters(params): Parameters<CreateScriptParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let name = params.name.clone();
        let content = params.content.clone();
        let extension = params.extension.clone();
        let folder = params.folder.clone();

        self.governance
            .authorize_and_execute(
                "create_script",
                None, // Global operation
                ExecutionClassification::Write,
                move || async move {
                    let result =
                        Self::create_script_impl(state, &name, &content, &extension, folder.as_deref())
                            .await
                            .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Update script content")]
    async fn update_script(
        &self,
        Parameters(params): Parameters<UpdateScriptParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        let state = self.state.clone();
        let path = params.path.clone();
        let content = params.content.clone();

        self.governance
            .authorize_and_execute(
                "update_script",
                None, // Global operation
                ExecutionClassification::Write,
                move || async move {
                    let result = Self::update_script_impl(state, &path, &content)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Delete a script file (requires confirmation)")]
    async fn delete_script(
        &self,
        Parameters(params): Parameters<DeleteScriptParams>,
    ) -> Result<CallToolResult, ErrorData> {
        use dbflux_policy::ExecutionClassification;

        // Validate confirmation matches filename
        validate_delete_params(&params).map_err(|e| ErrorData::invalid_params(e, None))?;

        let state = self.state.clone();
        let path = params.path.clone();

        self.governance
            .authorize_and_execute(
                "delete_script",
                None, // Global operation
                ExecutionClassification::Admin,
                move || async move {
                    Self::delete_script_impl(state, &path)
                        .await
                        .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        r#"{"status": "Script deleted successfully"}"#,
                    )]))
                },
            )
            .await
    }

    #[tool(description = "Execute a script against a database connection")]
    async fn execute_script(
        &self,
        Parameters(params): Parameters<ExecuteScriptParams>,
    ) -> Result<CallToolResult, ErrorData> {
        // Read script content to detect language and determine classification
        let state = self.state.clone();
        let script_path = params.path.clone();

        let (content, language) = Self::read_script_for_execution(&state, &script_path)
            .await
            .map_err(|e| e.into_error_data())?;

        // Detect classification based on content
        let classification = Self::detect_execution_classification(&content, &language);

        let connection_id = params.connection_id.clone();
        let state_clone = state.clone();

        self.governance
            .authorize_and_execute(
                "execute_script",
                Some(&params.connection_id),
                classification,
                move || async move {
                    let result = Self::execute_script_impl(
                        state_clone,
                        &content,
                        &connection_id,
                        &language,
                    )
                    .await
                    .map_err(|e| e.into_error_data())?;

                    Ok(CallToolResult::success(vec![Content::text(
                        serde_json::to_string_pretty(&result).unwrap(),
                    )]))
                },
            )
            .await
    }
}

// Implementation methods
impl DbFluxServer {
    async fn list_scripts_impl(
        state: ServerState,
        subfolder: Option<&str>,
    ) -> Result<Vec<ScriptEntryDto>, String> {
        use dbflux_core::ScriptsDirectory;

        let scripts_dir = ScriptsDirectory::new()
            .map_err(|e| format!("Failed to initialize scripts directory: {}", e))?;
        let root = scripts_dir.root_path();

        // Determine the directory to list
        let target_dir = if let Some(path) = subfolder {
            let path_buf = root.join(path);
            if !path_buf.starts_with(root) {
                return Err("Path is outside scripts root".to_string());
            }
            path_buf
        } else {
            root.to_path_buf()
        };

        // Convert entries to DTOs
        let entries: Vec<ScriptEntryDto> = scripts_dir
            .entries()
            .iter()
            .filter_map(|entry| {
                // Filter entries that match the target directory
                if subfolder.is_some() && !entry.path().starts_with(&target_dir) {
                    return None;
                }

                let relative_path = entry
                    .path()
                    .strip_prefix(root)
                    .ok()?
                    .to_str()?
                    .to_string();

                match entry {
                    dbflux_core::ScriptEntry::File {
                        name, extension, ..
                    } => Some(ScriptEntryDto {
                        path: relative_path,
                        name: name.clone(),
                        entry_type: "file".to_string(),
                        extension: Some(extension.clone()),
                    }),
                    dbflux_core::ScriptEntry::Folder { name, .. } => Some(ScriptEntryDto {
                        path: relative_path,
                        name: name.clone(),
                        entry_type: "folder".to_string(),
                        extension: None,
                    }),
                }
            })
            .collect();

        Ok(entries)
    }

    async fn get_script_impl(
        state: ServerState,
        script_path: &str,
    ) -> Result<ScriptContentDto, String> {
        use dbflux_core::ScriptsDirectory;

        let scripts_dir = ScriptsDirectory::new()
            .map_err(|e| format!("Failed to initialize scripts directory: {}", e))?;
        let root = scripts_dir.root_path();
        let full_path = root.join(script_path);

        if !full_path.starts_with(root) {
            return Err("Path is outside scripts root".to_string());
        }

        if !full_path.exists() {
            return Err("Script not found".to_string());
        }

        if !full_path.is_file() {
            return Err("Path is not a file".to_string());
        }

        let content = std::fs::read_to_string(&full_path)
            .map_err(|e| format!("Failed to read script: {}", e))?;

        let filename = full_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| "Invalid filename".to_string())?
            .to_string();

        let language = QueryLanguage::from_path(&full_path)
            .map(|l| l.display_name().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        let size = content.len();

        Ok(ScriptContentDto {
            path: script_path.to_string(),
            name: filename,
            content,
            language,
            size,
        })
    }

    async fn create_script_impl(
        state: ServerState,
        name: &str,
        content: &str,
        extension: &str,
        folder: Option<&str>,
    ) -> Result<ScriptCreatedDto, String> {
        use dbflux_core::ScriptsDirectory;

        let mut scripts_dir = ScriptsDirectory::new()
            .map_err(|e| format!("Failed to initialize scripts directory: {}", e))?;
        let root = scripts_dir.root_path().to_path_buf();

        // Determine parent directory
        let parent = if let Some(folder_path) = folder {
            let path_buf = root.join(folder_path);
            if !path_buf.starts_with(&root) {
                return Err("Folder path is outside scripts root".to_string());
            }
            Some(path_buf)
        } else {
            None
        };

        // Create the file
        let created_path = scripts_dir
            .create_file(parent.as_deref(), name, extension)
            .map_err(|e| format!("Failed to create script file: {}", e))?;

        // Write content
        std::fs::write(&created_path, content)
            .map_err(|e| format!("Failed to write script content: {}", e))?;

        let relative_path = created_path
            .strip_prefix(&root)
            .map_err(|_| "Failed to compute relative path".to_string())?
            .to_str()
            .ok_or_else(|| "Invalid path encoding".to_string())?
            .to_string();

        let filename = created_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| "Invalid filename".to_string())?
            .to_string();

        Ok(ScriptCreatedDto {
            path: relative_path,
            name: filename,
        })
    }

    async fn update_script_impl(
        state: ServerState,
        script_path: &str,
        content: &str,
    ) -> Result<ScriptUpdatedDto, String> {
        use dbflux_core::ScriptsDirectory;

        let scripts_dir = ScriptsDirectory::new()
            .map_err(|e| format!("Failed to initialize scripts directory: {}", e))?;
        let root = scripts_dir.root_path();
        let full_path = root.join(script_path);

        if !full_path.starts_with(root) {
            return Err("Path is outside scripts root".to_string());
        }

        if !full_path.exists() {
            return Err("Script not found".to_string());
        }

        if !full_path.is_file() {
            return Err("Path is not a file".to_string());
        }

        std::fs::write(&full_path, content)
            .map_err(|e| format!("Failed to write script: {}", e))?;

        Ok(ScriptUpdatedDto {
            path: script_path.to_string(),
            size: content.len(),
        })
    }

    async fn delete_script_impl(
        state: ServerState,
        script_path: &str,
    ) -> Result<(), String> {
        use dbflux_core::ScriptsDirectory;

        let mut scripts_dir = ScriptsDirectory::new()
            .map_err(|e| format!("Failed to initialize scripts directory: {}", e))?;
        let root = scripts_dir.root_path();
        let full_path = root.join(script_path);

        if !full_path.starts_with(root) {
            return Err("Path is outside scripts root".to_string());
        }

        if !full_path.exists() {
            return Err("Script not found".to_string());
        }

        scripts_dir.delete(&full_path)
            .map_err(|e| format!("Failed to delete script: {}", e))?;

        Ok(())
    }

    async fn read_script_for_execution(
        state: &ServerState,
        script_path: &str,
    ) -> Result<(String, QueryLanguage), String> {
        use dbflux_core::ScriptsDirectory;

        let scripts_dir = ScriptsDirectory::new()
            .map_err(|e| format!("Failed to initialize scripts directory: {}", e))?;
        let root = scripts_dir.root_path();
        let full_path = root.join(script_path);

        if !full_path.starts_with(root) {
            return Err("Path is outside scripts root".to_string());
        }

        if !full_path.exists() {
            return Err("Script not found".to_string());
        }

        let content = std::fs::read_to_string(&full_path)
            .map_err(|e| format!("Failed to read script: {}", e))?;

        let language = QueryLanguage::from_path(&full_path)
            .ok_or_else(|| "Cannot detect query language from file extension".to_string())?;

        Ok((content, language))
    }

    fn detect_execution_classification(
        content: &str,
        language: &QueryLanguage,
    ) -> dbflux_policy::ExecutionClassification {
        use dbflux_core::classify_query_for_governance;
        use dbflux_policy::ExecutionClassification;

        // Non-query scripts are always classified as Admin
        match language {
            QueryLanguage::Lua | QueryLanguage::Python | QueryLanguage::Bash => {
                return ExecutionClassification::Admin;
            }
            _ => {}
        }

        // For query languages, use the safety module to classify
        classify_query_for_governance(language, content)
    }

    async fn execute_script_impl(
        state: ServerState,
        content: &str,
        connection_id: &str,
        language: &QueryLanguage,
    ) -> Result<serde_json::Value, String> {
        // Only SQL/MongoDB/Redis queries are supported for execution
        match language {
            QueryLanguage::Sql
            | QueryLanguage::MongoQuery
            | QueryLanguage::RedisCommands
            | QueryLanguage::Cql
            | QueryLanguage::Cypher
            | QueryLanguage::InfluxQuery => {
                // Execute as query
                Self::execute_query_content(state, connection_id, content).await
            }
            QueryLanguage::Lua | QueryLanguage::Python | QueryLanguage::Bash => {
                Err("Script language not supported for execution (only database queries)".to_string())
            }
            QueryLanguage::Custom(_) => {
                Err("Custom language not supported for execution".to_string())
            }
        }
    }

    async fn execute_query_content(
        state: ServerState,
        connection_id: &str,
        query: &str,
    ) -> Result<serde_json::Value, String> {
        use crate::helper::serialize_query_result;

        let conn = Self::get_or_connect(state, connection_id).await?;

        let request = QueryRequest {
            sql: query.to_string(),
            params: Vec::new(),
            limit: None,
            offset: None,
            statement_timeout: None,
            database: None,
        };

        let result = conn
            .execute(&request)
            .map_err(|e| format!("Query execution failed: {}", e))?;

        Ok(serialize_query_result(&result))
    }
}
