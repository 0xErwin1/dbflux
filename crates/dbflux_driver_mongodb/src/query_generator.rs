use dbflux_core::{
    GeneratedQuery, MutationCategory, MutationRequest, QueryGenerator, QueryLanguage,
};

pub struct MongoShellGenerator;

impl QueryGenerator for MongoShellGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Document]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let text = match mutation {
            MutationRequest::DocumentInsert(insert) => generate_insert(insert),
            MutationRequest::DocumentUpdate(update) => generate_update(update),
            MutationRequest::DocumentDelete(delete) => generate_delete(delete),
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::MongoQuery,
            text,
        })
    }
}

fn generate_insert(insert: &dbflux_core::DocumentInsert) -> String {
    let collection = &insert.collection;

    if insert.documents.len() == 1 {
        let doc = serde_json::to_string_pretty(&insert.documents[0]).unwrap_or_default();
        format!("db.{collection}.insertOne({doc})")
    } else {
        let docs = serde_json::to_string_pretty(&insert.documents).unwrap_or_default();
        format!("db.{collection}.insertMany({docs})")
    }
}

fn generate_update(update: &dbflux_core::DocumentUpdate) -> String {
    let collection = &update.collection;
    let filter = serde_json::to_string_pretty(&update.filter.filter).unwrap_or_default();
    let update_doc = serde_json::to_string_pretty(&update.update).unwrap_or_default();

    let method = if update.many {
        "updateMany"
    } else {
        "updateOne"
    };

    if update.upsert {
        format!("db.{collection}.{method}({filter}, {update_doc}, {{ upsert: true }})")
    } else {
        format!("db.{collection}.{method}({filter}, {update_doc})")
    }
}

fn generate_delete(delete: &dbflux_core::DocumentDelete) -> String {
    let collection = &delete.collection;
    let filter = serde_json::to_string_pretty(&delete.filter.filter).unwrap_or_default();

    let method = if delete.many {
        "deleteMany"
    } else {
        "deleteOne"
    };

    format!("db.{collection}.{method}({filter})")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbflux_core::{DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate};

    #[test]
    fn insert_one_document() {
        let insert = DocumentInsert::one(
            "users".to_string(),
            serde_json::json!({"name": "Alice", "age": 30}),
        );
        let mutation = MutationRequest::DocumentInsert(insert);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert_eq!(result.language, QueryLanguage::MongoQuery);
        assert!(result.text.contains("insertOne"));
        assert!(result.text.contains("Alice"));
    }

    #[test]
    fn insert_many_documents() {
        let insert = DocumentInsert::many(
            "users".to_string(),
            vec![
                serde_json::json!({"name": "Alice"}),
                serde_json::json!({"name": "Bob"}),
            ],
        );
        let mutation = MutationRequest::DocumentInsert(insert);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("insertMany"));
    }

    #[test]
    fn update_one_document() {
        let update = DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::by_id("abc123"),
            serde_json::json!({"$set": {"name": "Bob"}}),
        );
        let mutation = MutationRequest::DocumentUpdate(update);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("updateOne"));
        assert!(result.text.contains("$set"));
    }

    #[test]
    fn update_many_with_upsert() {
        let update = DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"status": "old"})),
            serde_json::json!({"$set": {"archived": true}}),
        )
        .many()
        .upsert();
        let mutation = MutationRequest::DocumentUpdate(update);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("updateMany"));
        assert!(result.text.contains("upsert: true"));
    }

    #[test]
    fn delete_one_document() {
        let delete = DocumentDelete::new("users".to_string(), DocumentFilter::by_id("abc123"));
        let mutation = MutationRequest::DocumentDelete(delete);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("deleteOne"));
    }

    #[test]
    fn delete_many_documents() {
        let delete = DocumentDelete::new(
            "users".to_string(),
            DocumentFilter::new(serde_json::json!({"archived": true})),
        )
        .many();
        let mutation = MutationRequest::DocumentDelete(delete);

        let result = MongoShellGenerator.generate_mutation(&mutation).unwrap();
        assert!(result.text.contains("deleteMany"));
    }

    #[test]
    fn sql_mutation_returns_none() {
        let patch = dbflux_core::RowPatch::new(
            dbflux_core::RecordIdentity::composite(
                vec!["id".to_string()],
                vec![dbflux_core::Value::Int(1)],
            ),
            "users".to_string(),
            None,
            vec![("name".to_string(), dbflux_core::Value::Text("test".into()))],
        );
        let mutation = MutationRequest::SqlUpdate(patch);

        assert!(MongoShellGenerator.generate_mutation(&mutation).is_none());
    }
}
