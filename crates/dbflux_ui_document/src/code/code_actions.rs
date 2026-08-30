use super::*;
use crate::completion_support::normalize_identifier;
use dbflux_core::{CodeAction as CoreCodeAction, LanguageService, SchemaColumns};
use lsp_types::CodeAction as LspCodeAction;
use serde::{Deserialize, Serialize};
use std::ops::Range as StdRange;

/// JSON payload round-tripped through `lsp_types::CodeAction::data` so
/// `perform_code_action` can recover the exact core edit without re-running
/// detection against a buffer that may have changed since `code_actions` ran.
#[derive(Serialize, Deserialize)]
struct SerializedEdit {
    start: usize,
    end: usize,
    new_text: String,
}

/// Bridges the connected driver's [`LanguageService::code_actions_with_schema`]
/// to gpui-component's `CodeActionProvider`. Every branch here reads generic
/// core types (`CoreCodeAction`, `SchemaColumns`); the UI never branches on a
/// driver id or query language, matching the completion provider's pattern.
pub(super) struct SqlCodeActionProvider {
    app_state: Entity<AppStateEntity>,
    connection_id: Option<Uuid>,
    /// The document's selected database (document-local; the document
    /// reattaches the provider when it changes, same as completion).
    database: Option<String>,
}

impl SqlCodeActionProvider {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        connection_id: Option<Uuid>,
        database: Option<String>,
    ) -> Self {
        Self {
            app_state,
            connection_id,
            database,
        }
    }

    /// The database code actions scope schema lookups to: the document's
    /// selection, then the connection's active database, then the snapshot's
    /// current database. Mirrors `QueryCompletionProvider::effective_database`.
    fn effective_database(&self, connected: &dbflux_core::ConnectedProfile) -> Option<String> {
        self.database
            .clone()
            .or_else(|| connected.active_database.clone())
            .or_else(|| {
                connected
                    .schema
                    .as_ref()
                    .and_then(|snapshot| snapshot.current_database().map(String::from))
            })
    }

    fn schema_columns(&self, cx: &App) -> QualifySchemaColumns {
        let Some(connection_id) = self.connection_id else {
            return QualifySchemaColumns::default();
        };

        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&connection_id) else {
            return QualifySchemaColumns::default();
        };

        let effective_database = self.effective_database(connected);
        let lazy_per_database = connected.connection.schema_loading_strategy()
            == dbflux_core::SchemaLoadingStrategy::LazyPerDatabase;
        let database_in_scope = |database: &str| {
            effective_database
                .as_deref()
                .map_or(!lazy_per_database, |selected| selected == database)
        };

        let snapshot = connected.schema.as_ref().filter(|snapshot| {
            match (effective_database.as_deref(), snapshot.current_database()) {
                (Some(selected), Some(current)) => selected == current,
                _ => true,
            }
        });

        let mut columns = QualifySchemaColumns::default();

        if let Some(relational) = snapshot.and_then(|snapshot| snapshot.as_relational()) {
            for table in &relational.tables {
                columns.add_table(table);
            }
        }

        for (database, schema) in &connected.database_schemas {
            if !database_in_scope(database) {
                continue;
            }
            for table in &schema.tables {
                columns.add_table(table);
            }
        }

        for ((database, _, _), table) in &connected.table_details {
            if !database_in_scope(database) {
                continue;
            }
            columns.add_table(table);
        }

        columns
    }
}

/// Per-table column-name sets, keyed the same way `SqlCompletionMetadata`
/// keys `columns_by_table`: bare and schema-qualified, normalized. This is
/// deliberately narrower than `SqlCompletionMetadata` — code actions only
/// need column membership, not table/view names or CTE tracking.
#[derive(Default)]
struct QualifySchemaColumns {
    columns_by_table: HashMap<String, std::collections::BTreeSet<String>>,
}

impl QualifySchemaColumns {
    fn add_table(&mut self, table: &dbflux_core::TableInfo) {
        let Some(columns) = &table.columns else {
            return;
        };

        let mut keys = vec![normalize_identifier(&table.name)];
        if let Some(schema) = &table.schema {
            keys.push(normalize_identifier(&format!("{}.{}", schema, table.name)));
        }

        for column in columns {
            for key in &keys {
                self.columns_by_table
                    .entry(key.clone())
                    .or_default()
                    .insert(column.name.clone());
            }
        }
    }
}

impl SchemaColumns for QualifySchemaColumns {
    fn columns_of(&self, table_key: &str) -> Option<&std::collections::BTreeSet<String>> {
        self.columns_by_table.get(table_key)
    }
}

fn core_edit_to_lsp_action(action: CoreCodeAction) -> Option<LspCodeAction> {
    let data = serde_json::to_value(SerializedEdit {
        start: action.edit.range.start,
        end: action.edit.range.end,
        new_text: action.edit.new_text,
    })
    .ok()?;

    Some(LspCodeAction {
        title: action.title,
        kind: None,
        diagnostics: None,
        edit: None,
        command: None,
        is_preferred: None,
        disabled: None,
        data: Some(data),
    })
}

impl CodeActionProvider for SqlCodeActionProvider {
    fn id(&self) -> SharedString {
        "dbflux.sql".into()
    }

    fn code_actions(
        &self,
        state: Entity<InputState>,
        range: StdRange<usize>,
        _window: &mut Window,
        cx: &mut App,
    ) -> Task<anyhow::Result<Vec<LspCodeAction>>> {
        let source = state.read(cx).text().to_string();
        let offset = source.floor_char_boundary(range.start.min(source.len()));

        let Some(connection_id) = self.connection_id else {
            return Task::ready(Ok(Vec::new()));
        };

        let Some(connected) = self.app_state.read(cx).connections().get(&connection_id) else {
            return Task::ready(Ok(Vec::new()));
        };

        let service = connected.connection.language_service();
        let schema = self.schema_columns(cx);
        let actions = service.code_actions_with_schema(&source, offset, &schema);

        Task::ready(Ok(actions
            .into_iter()
            .filter_map(core_edit_to_lsp_action)
            .collect()))
    }

    fn perform_code_action(
        &self,
        state: Entity<InputState>,
        action: LspCodeAction,
        _push_to_history: bool,
        window: &mut Window,
        cx: &mut App,
    ) -> Task<anyhow::Result<()>> {
        let Some(data) = action.data else {
            return Task::ready(Err(anyhow::anyhow!("code action carries no edit payload")));
        };

        let edit: SerializedEdit = match serde_json::from_value(data) {
            Ok(edit) => edit,
            Err(error) => return Task::ready(Err(anyhow::anyhow!(error))),
        };

        state.update(cx, |input, cx| {
            let text = input.text();
            let start_utf16 = text.offset_to_offset_utf16(edit.start);
            let end_utf16 = text.offset_to_offset_utf16(edit.end);

            input.replace_text_in_range(Some(start_utf16..end_utf16), &edit.new_text, window, cx);
        });

        Task::ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CoreCodeAction, QualifySchemaColumns, SchemaColumns, SerializedEdit,
        core_edit_to_lsp_action,
    };
    use dbflux_components::controls::{Rope, RopeExt};

    fn column(name: &str) -> dbflux_core::ColumnInfo {
        dbflux_core::ColumnInfo {
            name: name.to_string(),
            type_name: "text".to_string(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            enum_values: None,
        }
    }

    fn table(name: &str, schema: Option<&str>, columns: &[&str]) -> dbflux_core::TableInfo {
        dbflux_core::TableInfo {
            name: name.to_string(),
            schema: schema.map(String::from),
            columns: Some(columns.iter().map(|c| column(c)).collect()),
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::default(),
            child_items: None,
            storage_hints: None,
        }
    }

    #[test]
    fn qualify_schema_columns_indexes_bare_and_schema_qualified_keys() {
        let mut columns = QualifySchemaColumns::default();
        columns.add_table(&table("invoices", Some("billing"), &["id", "amount"]));

        let bare = columns.columns_of("invoices").expect("bare key present");
        assert!(bare.contains("id"));
        assert!(bare.contains("amount"));

        let qualified = columns
            .columns_of("billing.invoices")
            .expect("schema-qualified key present");
        assert!(qualified.contains("id"));
    }

    #[test]
    fn qualify_schema_columns_ignores_tables_with_unloaded_columns() {
        let mut columns = QualifySchemaColumns::default();
        let mut unloaded = table("payments", None, &[]);
        unloaded.columns = None;
        columns.add_table(&unloaded);

        assert!(columns.columns_of("payments").is_none());
    }

    #[test]
    fn serialized_edit_round_trips_through_json() {
        let action = CoreCodeAction {
            title: "Add LIMIT 100".to_string(),
            edit: dbflux_core::CodeActionEdit {
                range: dbflux_core::TextRange { start: 3, end: 3 },
                new_text: " LIMIT 100".to_string(),
            },
        };

        let lsp_action = core_edit_to_lsp_action(action).expect("action converts to LSP shape");
        assert_eq!(lsp_action.title, "Add LIMIT 100");

        let edit: SerializedEdit =
            serde_json::from_value(lsp_action.data.expect("data payload present"))
                .expect("payload deserializes");
        assert_eq!(edit.start, 3);
        assert_eq!(edit.end, 3);
        assert_eq!(edit.new_text, " LIMIT 100");
    }

    // Byte offsets into multibyte text must convert to UTF-16 offsets before
    // reaching `EntityInputHandler::replace_text_in_range`, and back losslessly.
    #[test]
    fn multibyte_byte_offsets_round_trip_through_utf16() {
        let text = "émoji 🚀 text";
        let rope = Rope::from(text);

        let byte_offset = text.find("text").expect("marker present");
        let utf16_offset = rope.offset_to_offset_utf16(byte_offset);

        assert_eq!(rope.offset_utf16_to_offset(utf16_offset), byte_offset);
    }
}
