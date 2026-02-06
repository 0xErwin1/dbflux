use super::*;

impl Sidebar {
    pub(super) fn get_code_generators_for_item(
        &self,
        item_id: &str,
        node_kind: SchemaNodeKind,
        cx: &App,
    ) -> Vec<ContextMenuItem> {
        let Some(parts) = parse_node_id(item_id)
            .as_ref()
            .and_then(ItemIdParts::from_node_id)
        else {
            return vec![];
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&parts.profile_id) else {
            return vec![];
        };

        let scope_filter = match node_kind {
            SchemaNodeKind::Table => {
                |s: CodeGenScope| matches!(s, CodeGenScope::Table | CodeGenScope::TableOrView)
            }
            SchemaNodeKind::View => {
                |s: CodeGenScope| matches!(s, CodeGenScope::View | CodeGenScope::TableOrView)
            }
            _ => return vec![],
        };

        let mut generators: Vec<_> = conn
            .connection
            .code_generators()
            .iter()
            .filter(|g| scope_filter(g.scope))
            .collect();

        generators.sort_by_key(|g| g.order);

        generators
            .into_iter()
            .map(|g| {
                let label = if g.destructive {
                    format!("\u{26A0} {}", g.label)
                } else {
                    g.label.to_string()
                };
                ContextMenuItem {
                    label,
                    action: ContextMenuAction::GenerateCode(g.id.to_string()),
                }
            })
            .collect()
    }

    pub(super) fn generate_code(
        &mut self,
        item_id: &str,
        generator_id: &str,
        cx: &mut Context<Self>,
    ) {
        let is_view = item_id.starts_with("view_");

        // For views, generate code directly (no columns needed)
        if is_view {
            self.generate_code_for_view(item_id, generator_id, cx);
            return;
        }

        // For tables, ensure details are loaded first
        let pending = PendingAction::GenerateCode {
            item_id: item_id.to_string(),
            generator_id: generator_id.to_string(),
        };

        match self.ensure_table_details(item_id, pending, cx) {
            TableDetailsStatus::Ready => {
                self.generate_code_impl(item_id, generator_id, cx);
            }
            TableDetailsStatus::Loading => {
                // Will be handled by complete_pending_action when done
            }
            TableDetailsStatus::NotFound => {
                log::warn!("Code generation failed: table not found");
            }
        }
    }

    fn generate_code_for_view(
        &mut self,
        item_id: &str,
        generator_id: &str,
        cx: &mut Context<Self>,
    ) {
        let Some(parts) = parse_node_id(item_id)
            .as_ref()
            .and_then(ItemIdParts::from_node_id)
        else {
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&parts.profile_id) else {
            return;
        };

        // Try to find view in database_schemas (MySQL/MariaDB)
        let view_from_db_schemas = conn
            .database_schemas
            .get(&parts.schema_name)
            .and_then(|db_schema| db_schema.views.iter().find(|v| v.name == parts.object_name));

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let view = view_from_db_schemas.or_else(|| Self::find_view_for_item(&parts, &conn.schema));

        let Some(view) = view else {
            log::warn!(
                "Code generation for view '{}' failed: view not found",
                parts.object_name
            );
            return;
        };

        // Create a TableInfo from the ViewInfo for code generation
        let table_info = TableInfo {
            name: view.name.clone(),
            schema: view.schema.clone(),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
        };

        match conn.connection.generate_code(generator_id, &table_info) {
            Ok(sql) => cx.emit(SidebarEvent::GenerateSql(sql)),
            Err(e) => {
                log::error!("Code generation for view failed: {}", e);
                self.pending_toast = Some(PendingToast {
                    message: format!("Code generation failed: {}", e),
                    is_error: true,
                });
                cx.notify();
            }
        }
    }

    pub(super) fn generate_code_impl(
        &mut self,
        item_id: &str,
        generator_id: &str,
        cx: &mut Context<Self>,
    ) {
        use crate::ui::sql_preview_modal::SqlGenerationType;

        let Some(parts) = parse_node_id(item_id)
            .as_ref()
            .and_then(ItemIdParts::from_node_id)
        else {
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&parts.profile_id) else {
            return;
        };

        // Try to convert to SqlGenerationType for preview modal
        let generation_type = SqlGenerationType::from_generator_id(generator_id);

        // First check the table_details cache (populated by ensure_table_details)
        let cache_key = (parts.schema_name.clone(), parts.object_name.clone());
        if let Some(table) = conn.table_details.get(&cache_key) {
            // For supported types, use the SQL preview modal
            if let Some(gen_type) = generation_type {
                cx.emit(SidebarEvent::RequestSqlPreview {
                    profile_id: parts.profile_id,
                    table_info: table.clone(),
                    generation_type: gen_type,
                });
                return;
            }

            // For unsupported types (CREATE TABLE, DROP, TRUNCATE), use driver generation
            match conn.connection.generate_code(generator_id, table) {
                Ok(sql) => cx.emit(SidebarEvent::GenerateSql(sql)),
                Err(e) => {
                    log::error!("Code generation failed: {}", e);
                    self.pending_toast = Some(PendingToast {
                        message: format!("Code generation failed: {}", e),
                        is_error: true,
                    });
                    cx.notify();
                }
            }
            return;
        }

        // Fallback: search in database_schemas (MySQL/MariaDB)
        let table_from_db_schemas =
            conn.database_schemas
                .get(&parts.schema_name)
                .and_then(|db_schema| {
                    db_schema
                        .tables
                        .iter()
                        .find(|t| t.name == parts.object_name)
                });

        // Fall back to schema.schemas (PostgreSQL/SQLite)
        let table =
            table_from_db_schemas.or_else(|| Self::find_table_for_item(&parts, &conn.schema));

        let Some(table) = table else {
            log::warn!(
                "Code generation for '{}' failed: table not found",
                parts.object_name
            );
            return;
        };

        // For supported types, use the SQL preview modal
        if let Some(gen_type) = generation_type {
            cx.emit(SidebarEvent::RequestSqlPreview {
                profile_id: parts.profile_id,
                table_info: table.clone(),
                generation_type: gen_type,
            });
            return;
        }

        // For unsupported types, use driver generation
        match conn.connection.generate_code(generator_id, table) {
            Ok(sql) => cx.emit(SidebarEvent::GenerateSql(sql)),
            Err(e) => {
                log::error!("Code generation failed: {}", e);
                self.pending_toast = Some(PendingToast {
                    message: format!("Code generation failed: {}", e),
                    is_error: true,
                });
                cx.notify();
            }
        }
    }

    pub(super) fn get_current_database(conn: &ConnectedProfile) -> String {
        conn.active_database
            .clone()
            .or_else(|| {
                conn.schema
                    .as_ref()
                    .and_then(|s| s.current_database().map(str::to_owned))
            })
            .unwrap_or_else(|| "main".to_string())
    }

    pub(super) fn get_capabilities_for_item(&self, item_id: &str, cx: &App) -> CodeGenCapabilities {
        let Some(profile_id) = Self::extract_profile_id_from_item(item_id) else {
            return CodeGenCapabilities::empty();
        };
        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return CodeGenCapabilities::empty();
        };
        conn.connection.code_gen_capabilities()
    }

    pub(super) fn extract_profile_id_from_item(item_id: &str) -> Option<Uuid> {
        // Try various prefixes used in item IDs
        let prefixes = [
            "idx_",
            "sidx_",
            "fk_",
            "sfk_",
            "customtype_",
            "table_",
            "view_",
        ];

        for prefix in prefixes {
            if let Some(rest) = item_id.strip_prefix(prefix)
                && rest.len() >= 36
                && let Ok(uuid) = Uuid::parse_str(&rest[..36])
            {
                return Some(uuid);
            }
        }

        None
    }

    pub(super) fn is_enum_type(&self, item_id: &str, cx: &App) -> bool {
        let Some(SchemaNodeId::CustomType {
            profile_id,
            schema: schema_name,
            name: type_name,
        }) = parse_node_id(item_id)
        else {
            return false;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return false;
        };

        let current_db = Self::get_current_database(conn);
        let cache_key = SchemaCacheKey::new(current_db, Some(schema_name));
        if let Some(types) = conn.schema_types.get(&cache_key) {
            return types
                .iter()
                .any(|t| t.name == type_name && t.kind == CustomTypeKind::Enum);
        }

        false
    }

    pub(super) fn generate_index_sql(
        &mut self,
        item_id: &str,
        action: IndexSqlAction,
        cx: &mut Context<Self>,
    ) {
        let (profile_id, context_name, index_name, is_schema_level) = match parse_node_id(item_id) {
            Some(SchemaNodeId::Index {
                profile_id,
                table,
                name,
            }) => (profile_id, table, name, false),
            Some(SchemaNodeId::SchemaIndex {
                profile_id,
                schema,
                name,
            }) => (profile_id, schema, name, true),
            _ => {
                log::warn!("Failed to parse index id: {}", item_id);
                return;
            }
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return;
        };

        let current_db = Self::get_current_database(conn);
        let code_gen = conn.connection.code_generator();

        // Find the index info
        let index_info = if is_schema_level {
            let cache_key = SchemaCacheKey::new(&current_db, Some(&context_name));
            conn.schema_indexes.get(&cache_key).and_then(|indexes| {
                indexes
                    .iter()
                    .find(|idx| idx.name == index_name)
                    .map(|idx| (idx.table_name.clone(), idx.columns.clone(), idx.is_unique))
            })
        } else {
            let table_name = context_name.clone();
            conn.table_details
                .values()
                .find(|t| t.name == table_name)
                .and_then(|t| t.indexes.as_ref())
                .and_then(|indexes| {
                    indexes
                        .iter()
                        .find(|idx| idx.name == index_name)
                        .map(|idx| (table_name.clone(), idx.columns.clone(), idx.is_unique))
                })
        };

        let sql = match action {
            IndexSqlAction::Create => {
                if let Some((table_name, columns, is_unique)) = &index_info {
                    let request = CreateIndexRequest {
                        index_name: &index_name,
                        table_name,
                        schema_name: Some(&context_name),
                        columns,
                        unique: *is_unique,
                    };
                    code_gen.generate_create_index(&request)
                } else {
                    let placeholder_cols = vec!["column1".to_string(), "column2".to_string()];
                    let request = CreateIndexRequest {
                        index_name: &index_name,
                        table_name: "table_name",
                        schema_name: Some("schema"),
                        columns: &placeholder_cols,
                        unique: false,
                    };
                    code_gen.generate_create_index(&request)
                }
            }

            IndexSqlAction::Drop => {
                let table_name = index_info.as_ref().map(|(t, _, _)| t.as_str());
                let request = DropIndexRequest {
                    index_name: &index_name,
                    table_name,
                    schema_name: Some(&context_name),
                };
                code_gen.generate_drop_index(&request)
            }

            IndexSqlAction::Reindex => {
                let request = ReindexRequest {
                    index_name: &index_name,
                    schema_name: Some(&context_name),
                };
                code_gen.generate_reindex(&request)
            }
        };

        if let Some(sql) = sql {
            cx.emit(SidebarEvent::GenerateSql(sql));
        }
    }

    pub(super) fn generate_foreign_key_sql(
        &mut self,
        item_id: &str,
        action: ForeignKeySqlAction,
        cx: &mut Context<Self>,
    ) {
        let (profile_id, context_name, fk_name, is_schema_level) = match parse_node_id(item_id) {
            Some(SchemaNodeId::ForeignKey {
                profile_id,
                table,
                name,
            }) => (profile_id, table, name, false),
            Some(SchemaNodeId::SchemaForeignKey {
                profile_id,
                schema,
                name,
            }) => (profile_id, schema, name, true),
            _ => {
                log::warn!("Failed to parse foreign key id: {}", item_id);
                return;
            }
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return;
        };

        let current_db = Self::get_current_database(conn);
        let code_gen = conn.connection.code_generator();

        // Find the FK info
        let fk_info = if is_schema_level {
            let cache_key = SchemaCacheKey::new(&current_db, Some(&context_name));
            conn.schema_foreign_keys.get(&cache_key).and_then(|fks| {
                fks.iter().find(|fk| fk.name == fk_name).map(|fk| {
                    (
                        fk.table_name.clone(),
                        fk.columns.clone(),
                        fk.referenced_schema.clone(),
                        fk.referenced_table.clone(),
                        fk.referenced_columns.clone(),
                        fk.on_delete.clone(),
                        fk.on_update.clone(),
                    )
                })
            })
        } else {
            let table_name = context_name.clone();
            conn.table_details
                .values()
                .find(|t| t.name == table_name)
                .and_then(|t| t.foreign_keys.as_ref())
                .and_then(|fks| {
                    fks.iter().find(|fk| fk.name == fk_name).map(|fk| {
                        (
                            table_name.clone(),
                            fk.columns.clone(),
                            fk.referenced_schema.clone(),
                            fk.referenced_table.clone(),
                            fk.referenced_columns.clone(),
                            fk.on_delete.clone(),
                            fk.on_update.clone(),
                        )
                    })
                })
        };

        let sql = match action {
            ForeignKeySqlAction::AddConstraint => {
                if let Some((
                    table_name,
                    columns,
                    ref_schema,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                )) = &fk_info
                {
                    let request = AddForeignKeyRequest {
                        constraint_name: &fk_name,
                        table_name,
                        schema_name: Some(&context_name),
                        columns,
                        ref_table,
                        ref_schema: ref_schema.as_deref(),
                        ref_columns,
                        on_delete: on_delete.as_deref(),
                        on_update: on_update.as_deref(),
                    };
                    code_gen.generate_add_foreign_key(&request)
                } else {
                    let placeholder_cols = vec!["column_name".to_string()];
                    let placeholder_ref_cols = vec!["ref_column".to_string()];
                    let request = AddForeignKeyRequest {
                        constraint_name: &fk_name,
                        table_name: "table_name",
                        schema_name: Some("schema"),
                        columns: &placeholder_cols,
                        ref_table: "ref_table",
                        ref_schema: None,
                        ref_columns: &placeholder_ref_cols,
                        on_delete: None,
                        on_update: None,
                    };
                    code_gen.generate_add_foreign_key(&request)
                }
            }

            ForeignKeySqlAction::DropConstraint => {
                let table_name = fk_info
                    .as_ref()
                    .map(|(t, ..)| t.as_str())
                    .unwrap_or("table_name");
                let request = DropForeignKeyRequest {
                    constraint_name: &fk_name,
                    table_name,
                    schema_name: Some(&context_name),
                };
                code_gen.generate_drop_foreign_key(&request)
            }
        };

        if let Some(sql) = sql {
            cx.emit(SidebarEvent::GenerateSql(sql));
        }
    }

    pub(super) fn generate_type_sql(
        &mut self,
        item_id: &str,
        action: TypeSqlAction,
        cx: &mut Context<Self>,
    ) {
        let Some(SchemaNodeId::CustomType {
            profile_id,
            schema: schema_name,
            name: type_name,
        }) = parse_node_id(item_id)
        else {
            log::warn!("Failed to parse custom type id: {}", item_id);
            return;
        };

        let state = self.app_state.read(cx);
        let Some(conn) = state.connections().get(&profile_id) else {
            return;
        };

        let code_gen = conn.connection.code_generator();
        let current_db = Self::get_current_database(conn);

        let cache_key = SchemaCacheKey::new(current_db, Some(&schema_name));
        let type_info = conn
            .schema_types
            .get(&cache_key)
            .and_then(|types| types.iter().find(|t| t.name == type_name));

        let sql = match action {
            TypeSqlAction::Create => {
                let definition = if let Some(type_info) = type_info {
                    match type_info.kind {
                        CustomTypeKind::Enum => {
                            let values = type_info.enum_values.clone().unwrap_or_default();
                            TypeDefinition::Enum { values }
                        }
                        CustomTypeKind::Domain => {
                            let base = type_info
                                .base_type
                                .clone()
                                .unwrap_or_else(|| "text".to_string());
                            TypeDefinition::Domain { base_type: base }
                        }
                        CustomTypeKind::Composite => TypeDefinition::Composite,
                    }
                } else {
                    TypeDefinition::Enum { values: vec![] }
                };

                let request = CreateTypeRequest {
                    type_name: &type_name,
                    schema_name: Some(&schema_name),
                    definition,
                };
                code_gen.generate_create_type(&request)
            }

            TypeSqlAction::AddEnumValue => {
                let request = AddEnumValueRequest {
                    type_name: &type_name,
                    schema_name: Some(&schema_name),
                    new_value: "new_value",
                };
                code_gen.generate_add_enum_value(&request)
            }

            TypeSqlAction::Drop => {
                let request = DropTypeRequest {
                    type_name: &type_name,
                    schema_name: Some(&schema_name),
                };
                code_gen.generate_drop_type(&request)
            }
        };

        if let Some(sql) = sql {
            cx.emit(SidebarEvent::GenerateSql(sql));
        }
    }
}
