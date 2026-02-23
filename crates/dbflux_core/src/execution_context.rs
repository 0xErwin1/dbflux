use crate::QueryLanguage;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Per-document execution context (connection, database, schema).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub connection_id: Option<Uuid>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub container: Option<String>,
}

/// Prefix used for metadata annotations in file headers.
const ANNOTATION_PREFIX: &str = "@";

impl ExecutionContext {
    /// Parse context from the first lines of a file.
    ///
    /// Recognised annotations (language-aware comment prefix):
    /// ```text
    /// -- @connection: local-postgres
    /// -- @database: mydb
    /// -- @schema: public
    /// -- @container: users
    /// ```
    ///
    /// Only the first contiguous block of comment lines is inspected.
    pub fn parse_from_content(content: &str, language: QueryLanguage) -> Self {
        let prefix = language.comment_prefix();
        let mut ctx = Self::default();

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.is_empty() {
                continue;
            }

            if !trimmed.starts_with(prefix) {
                break;
            }

            let after_comment = trimmed[prefix.len()..].trim();

            if !after_comment.starts_with(ANNOTATION_PREFIX) {
                continue;
            }

            let annotation = &after_comment[ANNOTATION_PREFIX.len()..];

            if let Some((key, value)) = annotation.split_once(':') {
                let key = key.trim().to_lowercase();
                let value = value.trim().to_string();

                if value.is_empty() {
                    continue;
                }

                match key.as_str() {
                    "connection" => ctx.connection_id = Uuid::parse_str(&value).ok(),
                    "database" | "db" => ctx.database = Some(value),
                    "schema" => ctx.schema = Some(value),
                    "container" | "table" | "collection" => ctx.container = Some(value),
                    _ => {}
                }
            }
        }

        ctx
    }

    /// Serialize the context as comment lines to prepend to a file.
    ///
    /// Only set fields are emitted. Returns an empty string when nothing is set.
    pub fn to_comment_header(&self, language: QueryLanguage) -> String {
        let prefix = language.comment_prefix();
        let mut lines = Vec::new();

        if let Some(id) = &self.connection_id {
            lines.push(format!("{} @connection: {}", prefix, id));
        }
        if let Some(db) = &self.database {
            lines.push(format!("{} @database: {}", prefix, db));
        }
        if let Some(schema) = &self.schema {
            lines.push(format!("{} @schema: {}", prefix, schema));
        }
        if let Some(container) = &self.container {
            lines.push(format!("{} @container: {}", prefix, container));
        }

        if lines.is_empty() {
            return String::new();
        }

        let mut result = lines.join("\n");
        result.push('\n');
        result
    }

    pub fn has_connection(&self) -> bool {
        self.connection_id.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.connection_id.is_none()
            && self.database.is_none()
            && self.schema.is_none()
            && self.container.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_annotations() {
        let content = "\
-- @connection: 550e8400-e29b-41d4-a716-446655440000
-- @database: mydb
-- @schema: public
-- @container: users

SELECT * FROM users;
";
        let ctx = ExecutionContext::parse_from_content(content, QueryLanguage::Sql);

        assert_eq!(
            ctx.connection_id,
            Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
        );
        assert_eq!(ctx.database.as_deref(), Some("mydb"));
        assert_eq!(ctx.schema.as_deref(), Some("public"));
        assert_eq!(ctx.container.as_deref(), Some("users"));
    }

    #[test]
    fn parse_js_annotations() {
        let content = "\
// @connection: 550e8400-e29b-41d4-a716-446655440000
// @database: app
// @container: orders

db.orders.find({})
";
        let ctx = ExecutionContext::parse_from_content(content, QueryLanguage::MongoQuery);

        assert_eq!(ctx.database.as_deref(), Some("app"));
        assert_eq!(ctx.container.as_deref(), Some("orders"));
    }

    #[test]
    fn roundtrip_comment_header() {
        let ctx = ExecutionContext {
            connection_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            database: Some("mydb".into()),
            schema: Some("public".into()),
            container: None,
        };

        let header = ctx.to_comment_header(QueryLanguage::Sql);
        let parsed = ExecutionContext::parse_from_content(&header, QueryLanguage::Sql);

        assert_eq!(parsed.connection_id, ctx.connection_id);
        assert_eq!(parsed.database, ctx.database);
        assert_eq!(parsed.schema, ctx.schema);
    }

    #[test]
    fn empty_context_produces_no_header() {
        let ctx = ExecutionContext::default();
        assert!(ctx.to_comment_header(QueryLanguage::Sql).is_empty());
    }

    #[test]
    fn stops_at_non_comment_line() {
        let content = "\
-- @database: mydb
SELECT 1;
-- @schema: should_not_parse
";
        let ctx = ExecutionContext::parse_from_content(content, QueryLanguage::Sql);
        assert_eq!(ctx.database.as_deref(), Some("mydb"));
        assert!(ctx.schema.is_none());
    }
}
