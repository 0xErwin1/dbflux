use dbflux_core::{
    DocumentDelete, DocumentInsert, DocumentUpdate, GeneratedQuery, MutationCategory,
    MutationRequest, QueryGenerator, QueryLanguage,
};

fn json_text(value: &serde_json::Value) -> Option<String> {
    serde_json::to_string(value).ok()
}

fn generate_insert(insert: &DocumentInsert) -> Option<String> {
    if insert.documents.is_empty() {
        return None;
    }

    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "op".to_string(),
        serde_json::Value::String("put".to_string()),
    );
    envelope.insert(
        "table".to_string(),
        serde_json::Value::String(insert.collection.clone()),
    );

    if let Some(database) = insert.database.as_ref() {
        envelope.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    if insert.documents.len() == 1 {
        envelope.insert("item".to_string(), insert.documents.first()?.clone());
    } else {
        envelope.insert(
            "items".to_string(),
            serde_json::Value::Array(insert.documents.clone()),
        );
    }

    json_text(&serde_json::Value::Object(envelope))
}

fn generate_update(update: &DocumentUpdate) -> Option<String> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "op".to_string(),
        serde_json::Value::String("update".to_string()),
    );
    envelope.insert(
        "table".to_string(),
        serde_json::Value::String(update.collection.clone()),
    );
    envelope.insert("key".to_string(), update.filter.filter.clone());
    envelope.insert("update".to_string(), update.update.clone());
    envelope.insert("many".to_string(), serde_json::Value::Bool(update.many));
    envelope.insert("upsert".to_string(), serde_json::Value::Bool(update.upsert));

    if let Some(database) = update.database.as_ref() {
        envelope.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    json_text(&serde_json::Value::Object(envelope))
}

fn generate_delete(delete: &DocumentDelete) -> Option<String> {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "op".to_string(),
        serde_json::Value::String("delete".to_string()),
    );
    envelope.insert(
        "table".to_string(),
        serde_json::Value::String(delete.collection.clone()),
    );
    envelope.insert("key".to_string(), delete.filter.filter.clone());
    envelope.insert("many".to_string(), serde_json::Value::Bool(delete.many));

    if let Some(database) = delete.database.as_ref() {
        envelope.insert(
            "database".to_string(),
            serde_json::Value::String(database.clone()),
        );
    }

    json_text(&serde_json::Value::Object(envelope))
}

pub struct DynamoQueryGenerator;

impl QueryGenerator for DynamoQueryGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Document]
    }

    fn generate_mutation(&self, mutation: &MutationRequest) -> Option<GeneratedQuery> {
        let text = match mutation {
            MutationRequest::DocumentInsert(insert) => generate_insert(insert)?,
            MutationRequest::DocumentUpdate(update) => generate_update(update)?,
            MutationRequest::DocumentDelete(delete) => generate_delete(delete)?,
            _ => return None,
        };

        Some(GeneratedQuery {
            language: QueryLanguage::Custom("DynamoDB".to_string()),
            text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DynamoQueryGenerator;
    use crate::query_parser::parse_command_envelope;
    use dbflux_core::{
        DocumentDelete, DocumentFilter, DocumentInsert, DocumentUpdate, MutationRequest,
        QueryGenerator,
    };
    use serde_json::json;

    #[test]
    fn generated_insert_update_delete_envelopes_are_parseable() {
        let generator = DynamoQueryGenerator;

        let insert = MutationRequest::DocumentInsert(DocumentInsert::one(
            "users".to_string(),
            json!({"pk":"U#1","name":"alice"}),
        ));
        let insert_query = generator
            .generate_mutation(&insert)
            .expect("insert envelope should be generated");
        parse_command_envelope(&insert_query.text).expect("insert envelope should be parseable");

        let insert_many = MutationRequest::DocumentInsert(DocumentInsert::many(
            "users".to_string(),
            vec![json!({"pk":"U#2"}), json!({"pk":"U#3"})],
        ));
        let insert_many_query = generator
            .generate_mutation(&insert_many)
            .expect("insert-many envelope should be generated");
        parse_command_envelope(&insert_many_query.text)
            .expect("insert-many envelope should be parseable");

        let update = MutationRequest::DocumentUpdate(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(json!({"pk":"U#1"})),
            json!({"name":"bob"}),
        ));
        let update_query = generator
            .generate_mutation(&update)
            .expect("update envelope should be generated");
        parse_command_envelope(&update_query.text).expect("update envelope should be parseable");

        let update_upsert = MutationRequest::DocumentUpdate(
            DocumentUpdate::new(
                "users".to_string(),
                DocumentFilter::new(json!({"pk":"U#1"})),
                json!({"name":"bob"}),
            )
            .upsert(),
        );
        let update_upsert_query = generator
            .generate_mutation(&update_upsert)
            .expect("upsert update envelope should be generated");
        parse_command_envelope(&update_upsert_query.text)
            .expect("upsert update envelope should be parseable");

        let delete = MutationRequest::DocumentDelete(DocumentDelete::new(
            "users".to_string(),
            DocumentFilter::new(json!({"pk":"U#1"})),
        ));
        let delete_query = generator
            .generate_mutation(&delete)
            .expect("delete envelope should be generated");
        parse_command_envelope(&delete_query.text).expect("delete envelope should be parseable");
    }
}
