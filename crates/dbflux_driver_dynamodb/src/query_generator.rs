use dbflux_core::{
    DocumentDelete, DocumentInsert, DocumentUpdate, GeneratedQuery, MutationCategory,
    MutationRequest, QueryGenerator, QueryLanguage,
};

fn json_text(value: &serde_json::Value) -> Option<String> {
    serde_json::to_string(value).ok()
}

fn generate_insert(insert: &DocumentInsert) -> Option<String> {
    if insert.documents.len() != 1 {
        return None;
    }

    let item = json_text(insert.documents.first()?)?;
    Some(format!(
        r#"{{"op":"put","table":"{}","item":{}}}"#,
        insert.collection, item
    ))
}

fn generate_update(update: &DocumentUpdate) -> Option<String> {
    let key = json_text(&update.filter.filter)?;
    let change = json_text(&update.update)?;

    Some(format!(
        r#"{{"op":"update","table":"{}","key":{},"update":{},"many":{}}}"#,
        update.collection, key, change, update.many
    ))
}

fn generate_delete(delete: &DocumentDelete) -> Option<String> {
    let key = json_text(&delete.filter.filter)?;

    Some(format!(
        r#"{{"op":"delete","table":"{}","key":{},"many":{}}}"#,
        delete.collection, key, delete.many
    ))
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

        let update = MutationRequest::DocumentUpdate(DocumentUpdate::new(
            "users".to_string(),
            DocumentFilter::new(json!({"pk":"U#1"})),
            json!({"name":"bob"}),
        ));
        let update_query = generator
            .generate_mutation(&update)
            .expect("update envelope should be generated");
        parse_command_envelope(&update_query.text).expect("update envelope should be parseable");

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
