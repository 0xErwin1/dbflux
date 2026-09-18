//! [`dbflux_core::ScriptOperationHost`] implementation for MongoDB.
//!
//! Converts a driver-agnostic [`ScriptOperation`] into the existing
//! [`crate::driver::MongoOperation`] shape, dispatches it through the
//! `mongodb::sync` API, and converts the BSON result into
//! `Vec<serde_json::Value>` via relaxed extJSON — the shape
//! [`dbflux_core::ScriptOperationOutcome`] needs for JS consumption.
//!
//! This is deliberately a separate conversion path from
//! `driver::documents_to_result` (columnar `{columns, rows}` for grid
//! display); the two targets are different enough that reusing one for the
//! other would distort both.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bson::{Bson, Document};
use dbflux_core::{
    DbError, ScriptMethod, ScriptOperation, ScriptOperationCounts, ScriptOperationHost,
    ScriptOperationOutcome, ScriptTarget,
};
use mongodb::bson::doc;
use mongodb::sync::{Client, Database};

use crate::driver::{MongoOperation, format_mongo_query_error, json_array_to_bson_docs};

/// Hard cap on the number of documents a single script-dispatched `find`/
/// `aggregate` may return. Enforced during collection (`collect_cursor_documents_capped`),
/// never after — collecting first and checking after would materialise the
/// whole result set before refusing, defeating the memory protection.
pub(crate) const SCRIPT_DOCUMENT_CAP: usize = 10_000;

/// Dispatch boundary for a mongosh-style script running against one
/// connection. Constructed fresh per `execute()` call — it borrows the
/// locked `Client`/`Database` for the lifetime of one script run.
pub(crate) struct MongoScriptHost<'a> {
    client: &'a Client,
    db: &'a Database,
    cancelled: Arc<AtomicBool>,
}

impl<'a> MongoScriptHost<'a> {
    pub(crate) fn new(client: &'a Client, db: &'a Database, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            client,
            db,
            cancelled,
        }
    }
}

impl ScriptOperationHost for MongoScriptHost<'_> {
    fn dispatch(&self, op: &ScriptOperation) -> Result<ScriptOperationOutcome, DbError> {
        let operation = script_operation_to_mongo_operation(op)?;
        dispatch_mongo_operation(
            self.client,
            self.db,
            &op.target,
            &operation,
            &self.cancelled,
        )
    }
}

/// Converts a closed [`ScriptOperation`] into the driver's native
/// [`MongoOperation`], carrying JSON arguments through as BSON.
///
/// Pure and DB-free: every branch is exercised by unit tests that construct
/// a [`ScriptOperation`] and assert the resulting [`MongoOperation`] shape
/// without a live connection.
pub(crate) fn script_operation_to_mongo_operation(
    op: &ScriptOperation,
) -> Result<MongoOperation, DbError> {
    let args = &op.arguments;

    Ok(match op.method {
        ScriptMethod::FindDocuments => MongoOperation::Find {
            filter: arg_doc(args, 0)?,
            projection: arg_doc_opt(args, 1)?,
            sort: None,
            limit: None,
            skip: None,
        },
        ScriptMethod::AggregateDocuments => MongoOperation::Aggregate {
            pipeline: arg_doc_array(args, 0)?,
        },
        ScriptMethod::CountDocuments => MongoOperation::Count {
            filter: arg_doc(args, 0)?,
        },
        ScriptMethod::InsertOne => MongoOperation::InsertOne {
            document: arg_doc_required(args, 0, "insertOne")?,
        },
        ScriptMethod::InsertMany => MongoOperation::InsertMany {
            documents: arg_doc_array_required(args, 0, "insertMany")?,
        },
        ScriptMethod::UpdateOne => MongoOperation::UpdateOne {
            filter: arg_doc(args, 0)?,
            update: arg_doc_required(args, 1, "updateOne")?,
            upsert: arg_upsert(args, 2),
        },
        ScriptMethod::UpdateMany => MongoOperation::UpdateMany {
            filter: arg_doc(args, 0)?,
            update: arg_doc_required(args, 1, "updateMany")?,
            upsert: arg_upsert(args, 2),
        },
        ScriptMethod::ReplaceOne => MongoOperation::ReplaceOne {
            filter: arg_doc(args, 0)?,
            replacement: arg_doc_required(args, 1, "replaceOne")?,
            upsert: arg_upsert(args, 2),
        },
        ScriptMethod::DeleteOne => MongoOperation::DeleteOne {
            filter: arg_doc(args, 0)?,
        },
        ScriptMethod::DeleteMany => MongoOperation::DeleteMany {
            filter: arg_doc(args, 0)?,
        },
        ScriptMethod::DropContainer => MongoOperation::Drop,
        ScriptMethod::DropDatabase => MongoOperation::DropDatabase,
        ScriptMethod::ListContainers => MongoOperation::GetCollectionNames,
        ScriptMethod::CreateContainer => MongoOperation::CreateCollection {
            name: arg_string_required(args, 0, "createCollection")?,
        },
        ScriptMethod::DatabaseStats => MongoOperation::DbStats,
        ScriptMethod::ServerStatus => MongoOperation::ServerStatus,
        ScriptMethod::RunCommand => MongoOperation::RunCommand {
            command: arg_doc_required(args, 0, "runCommand")?,
        },
    })
}

fn arg_doc(args: &[serde_json::Value], index: usize) -> Result<Document, DbError> {
    match args.get(index) {
        Some(value) => crate::driver::json_to_bson_doc(value),
        None => Ok(Document::new()),
    }
}

fn arg_doc_opt(args: &[serde_json::Value], index: usize) -> Result<Option<Document>, DbError> {
    match args.get(index) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => crate::driver::json_to_bson_doc(value).map(Some),
    }
}

fn arg_doc_required(
    args: &[serde_json::Value],
    index: usize,
    method: &str,
) -> Result<Document, DbError> {
    let value = args
        .get(index)
        .ok_or_else(|| DbError::query_failed(format!("{method}() requires a document argument")))?;
    crate::driver::json_to_bson_doc(value)
}

fn arg_doc_array(args: &[serde_json::Value], index: usize) -> Result<Vec<Document>, DbError> {
    match args.get(index) {
        Some(value) => json_array_to_bson_docs(value),
        None => Ok(Vec::new()),
    }
}

fn arg_doc_array_required(
    args: &[serde_json::Value],
    index: usize,
    method: &str,
) -> Result<Vec<Document>, DbError> {
    let value = args
        .get(index)
        .ok_or_else(|| DbError::query_failed(format!("{method}() requires an array argument")))?;
    json_array_to_bson_docs(value)
}

fn arg_string_required(
    args: &[serde_json::Value],
    index: usize,
    method: &str,
) -> Result<String, DbError> {
    args.get(index)
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| DbError::query_failed(format!("{method}() requires a string argument")))
}

fn arg_upsert(args: &[serde_json::Value], index: usize) -> bool {
    args.get(index)
        .and_then(|value| value.get("upsert"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

/// Enforces [`SCRIPT_DOCUMENT_CAP`] while iterating a cursor, erroring on
/// document 10 001 instead of collecting the whole result set first. Kept
/// separate from `driver::collect_cursor_documents` (unbounded, used by the
/// stage-1 tabular path), which must never be reused here for that reason.
pub(crate) fn collect_cursor_documents_capped(
    cursor: mongodb::sync::Cursor<Document>,
    cancelled: &Arc<AtomicBool>,
    cap: usize,
) -> Result<Vec<Document>, DbError> {
    let mut documents = Vec::new();
    for result in cursor {
        if cancelled.load(Ordering::SeqCst) {
            log::info!("[SCRIPT] MongoDB script cursor collection cancelled");
            return Err(DbError::Cancelled);
        }
        let doc = result.map_err(|e| format_mongo_query_error(&e))?;
        if documents.len() >= cap {
            return Err(DbError::query_failed(format!(
                "result exceeds the {cap}-document cap for a single find()/aggregate() call \
                 in a script; narrow the filter/pipeline instead of relying on the full result"
            )));
        }
        documents.push(doc);
    }
    Ok(documents)
}

/// Converts BSON documents to JSON for script consumption via relaxed
/// extJSON (`Bson::into_relaxed_extjson`) — not canonical extJSON, which
/// wraps every number as `{"$numberInt": "5"}` and would make ordinary JS
/// arithmetic on a returned document (`doc.qty + 1`) concatenate a string
/// instead of adding. Relaxed extJSON keeps plain JSON numbers and only
/// wraps types JSON has no representation for (`{"$oid": ...}`, `{"$date":
/// ...}`), so `doc._id.$oid` reads naturally.
///
/// Known accepted lossiness: relaxed extJSON does not round-trip exactly (a
/// BSON `Double` of `1.0` becomes JSON `1`). Acceptable for v1 — documents
/// are read for inspection; writes go through filter/update arguments the
/// user wrote explicitly, not by echoing a read document back verbatim.
fn documents_to_json(documents: Vec<Document>) -> Vec<serde_json::Value> {
    documents
        .into_iter()
        .map(|doc| Bson::Document(doc).into_relaxed_extjson())
        .collect()
}

fn bson_to_json(bson: Bson) -> serde_json::Value {
    bson.into_relaxed_extjson()
}

fn dispatch_mongo_operation(
    client: &Client,
    db: &Database,
    target: &ScriptTarget,
    operation: &MongoOperation,
    cancelled: &Arc<AtomicBool>,
) -> Result<ScriptOperationOutcome, DbError> {
    let collection_name = match target {
        ScriptTarget::Database => None,
        ScriptTarget::Container(name) => Some(name.as_str()),
    };

    let require_collection = |method: &str| -> Result<&str, DbError> {
        collection_name
            .ok_or_else(|| DbError::query_failed(format!("{method}() requires a collection")))
    };

    match operation {
        MongoOperation::Find {
            filter, projection, ..
        } => {
            let collection = db.collection::<Document>(require_collection("find")?);
            let mut find_options = mongodb::options::FindOptions::default();
            find_options.projection = projection.clone();

            let cursor = collection
                .find(filter.clone())
                .with_options(find_options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;
            let documents =
                collect_cursor_documents_capped(cursor, cancelled, SCRIPT_DOCUMENT_CAP)?;

            Ok(ScriptOperationOutcome {
                documents: documents_to_json(documents),
                counts: ScriptOperationCounts::default(),
            })
        }

        MongoOperation::Aggregate { pipeline } => {
            let collection = db.collection::<Document>(require_collection("aggregate")?);
            let cursor = collection
                .aggregate(pipeline.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;
            let documents =
                collect_cursor_documents_capped(cursor, cancelled, SCRIPT_DOCUMENT_CAP)?;

            Ok(ScriptOperationOutcome {
                documents: documents_to_json(documents),
                counts: ScriptOperationCounts::default(),
            })
        }

        MongoOperation::Count { filter } => {
            let collection = db.collection::<Document>(require_collection("countDocuments")?);
            let count = collection
                .count_documents(filter.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: vec![serde_json::json!(count)],
                counts: ScriptOperationCounts::default(),
            })
        }

        MongoOperation::InsertOne { document } => {
            let collection = db.collection::<Document>(require_collection("insertOne")?);
            let result = collection
                .insert_one(document.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: vec![bson_to_json(result.inserted_id)],
                counts: ScriptOperationCounts {
                    inserted: Some(1),
                    ..Default::default()
                },
            })
        }

        MongoOperation::InsertMany { documents } => {
            let collection = db.collection::<Document>(require_collection("insertMany")?);
            let result = collection
                .insert_many(documents.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: Vec::new(),
                counts: ScriptOperationCounts {
                    inserted: Some(result.inserted_ids.len() as u64),
                    ..Default::default()
                },
            })
        }

        MongoOperation::UpdateOne {
            filter,
            update,
            upsert,
        } => {
            let collection = db.collection::<Document>(require_collection("updateOne")?);
            let mut options = mongodb::options::UpdateOptions::default();
            options.upsert = Some(*upsert);
            let result = collection
                .update_one(filter.clone(), update.clone())
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: Vec::new(),
                counts: ScriptOperationCounts {
                    matched: Some(result.matched_count),
                    modified: Some(result.modified_count),
                    upserted: result.upserted_id.is_some().then_some(1),
                    ..Default::default()
                },
            })
        }

        MongoOperation::UpdateMany {
            filter,
            update,
            upsert,
        } => {
            let collection = db.collection::<Document>(require_collection("updateMany")?);
            let mut options = mongodb::options::UpdateOptions::default();
            options.upsert = Some(*upsert);
            let result = collection
                .update_many(filter.clone(), update.clone())
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: Vec::new(),
                counts: ScriptOperationCounts {
                    matched: Some(result.matched_count),
                    modified: Some(result.modified_count),
                    upserted: result.upserted_id.is_some().then_some(1),
                    ..Default::default()
                },
            })
        }

        MongoOperation::ReplaceOne {
            filter,
            replacement,
            upsert,
        } => {
            let collection = db.collection::<Document>(require_collection("replaceOne")?);
            let mut options = mongodb::options::ReplaceOptions::default();
            options.upsert = Some(*upsert);
            let result = collection
                .replace_one(filter.clone(), replacement.clone())
                .with_options(options)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: Vec::new(),
                counts: ScriptOperationCounts {
                    matched: Some(result.matched_count),
                    modified: Some(result.modified_count),
                    upserted: result.upserted_id.is_some().then_some(1),
                    ..Default::default()
                },
            })
        }

        MongoOperation::DeleteOne { filter } => {
            let collection = db.collection::<Document>(require_collection("deleteOne")?);
            let result = collection
                .delete_one(filter.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: Vec::new(),
                counts: ScriptOperationCounts {
                    deleted: Some(result.deleted_count),
                    ..Default::default()
                },
            })
        }

        MongoOperation::DeleteMany { filter } => {
            let collection = db.collection::<Document>(require_collection("deleteMany")?);
            let result = collection
                .delete_many(filter.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: Vec::new(),
                counts: ScriptOperationCounts {
                    deleted: Some(result.deleted_count),
                    ..Default::default()
                },
            })
        }

        MongoOperation::Drop => {
            let collection = db.collection::<Document>(require_collection("drop")?);
            collection
                .drop()
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome::default())
        }

        MongoOperation::DropDatabase => {
            db.clone()
                .drop()
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome::default())
        }

        MongoOperation::GetCollectionNames => {
            let names = db
                .list_collection_names()
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: names.into_iter().map(serde_json::Value::String).collect(),
                counts: ScriptOperationCounts::default(),
            })
        }

        MongoOperation::CreateCollection { name } => {
            db.create_collection(name)
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome::default())
        }

        MongoOperation::DbStats => {
            let result = db
                .run_command(doc! { "dbStats": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: vec![bson_to_json(Bson::Document(result))],
                counts: ScriptOperationCounts::default(),
            })
        }

        MongoOperation::ServerStatus => {
            let result = db
                .run_command(doc! { "serverStatus": 1 })
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: vec![bson_to_json(Bson::Document(result))],
                counts: ScriptOperationCounts::default(),
            })
        }

        MongoOperation::RunCommand { command } => {
            let result = db
                .run_command(command.clone())
                .run()
                .map_err(|e| format_mongo_query_error(&e))?;

            Ok(ScriptOperationOutcome {
                documents: vec![bson_to_json(Bson::Document(result))],
                counts: ScriptOperationCounts::default(),
            })
        }

        // `client` is unused by every reachable branch above; kept as a
        // parameter for symmetry with `execute_mongo_query`/`execute_db_operation`
        // and to leave room for an admin-database command in a future
        // ScriptMethod variant without changing this function's signature.
        MongoOperation::GetName
        | MongoOperation::GetCollectionInfos
        | MongoOperation::AdminCommand { .. }
        | MongoOperation::Version
        | MongoOperation::HostInfo
        | MongoOperation::CurrentOp => {
            let _ = client;
            unreachable!(
                "script_operation_to_mongo_operation never produces this variant — \
                 it is not in ScriptMethod's closed set"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(
        target: ScriptTarget,
        method: ScriptMethod,
        args: Vec<serde_json::Value>,
    ) -> ScriptOperation {
        ScriptOperation::new(target, method, args)
    }

    #[test]
    fn find_documents_maps_filter_and_projection() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::FindDocuments,
            vec![
                serde_json::json!({"active": true}),
                serde_json::json!({"name": 1}),
            ],
        ))
        .expect("find should convert");

        match operation {
            MongoOperation::Find {
                filter,
                projection,
                sort,
                limit,
                skip,
            } => {
                assert_eq!(filter, doc! { "active": true });
                assert_eq!(projection, Some(doc! { "name": 1i64 }));
                assert_eq!(sort, None);
                assert_eq!(limit, None);
                assert_eq!(skip, None);
            }
            other => panic!("expected Find, got {other:?}"),
        }
    }

    #[test]
    fn find_documents_defaults_to_empty_filter_and_no_projection() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::FindDocuments,
            vec![],
        ))
        .expect("find should convert");

        match operation {
            MongoOperation::Find {
                filter, projection, ..
            } => {
                assert_eq!(filter, Document::new());
                assert_eq!(projection, None);
            }
            other => panic!("expected Find, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_documents_maps_pipeline() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("orders".to_string()),
            ScriptMethod::AggregateDocuments,
            vec![serde_json::json!([{"$match": {"status": "active"}}])],
        ))
        .expect("aggregate should convert");

        match operation {
            MongoOperation::Aggregate { pipeline } => {
                assert_eq!(pipeline, vec![doc! { "$match": { "status": "active" } }]);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn delete_many_maps_filter_faithfully() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::DeleteMany,
            vec![serde_json::json!({"archived": true})],
        ))
        .expect("deleteMany should convert");

        match operation {
            MongoOperation::DeleteMany { filter } => {
                assert_eq!(filter, doc! { "archived": true });
            }
            other => panic!("expected DeleteMany, got {other:?}"),
        }
    }

    #[test]
    fn update_one_carries_filter_update_and_upsert_option() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::UpdateOne,
            vec![
                serde_json::json!({"_id": 1}),
                serde_json::json!({"$set": {"active": false}}),
                serde_json::json!({"upsert": true}),
            ],
        ))
        .expect("updateOne should convert");

        match operation {
            MongoOperation::UpdateOne {
                filter,
                update,
                upsert,
            } => {
                assert_eq!(filter, doc! { "_id": 1i64 });
                assert_eq!(update, doc! { "$set": { "active": false } });
                assert!(upsert);
            }
            other => panic!("expected UpdateOne, got {other:?}"),
        }
    }

    #[test]
    fn update_one_without_options_defaults_upsert_to_false() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::UpdateOne,
            vec![
                serde_json::json!({"_id": 1}),
                serde_json::json!({"$set": {"active": false}}),
            ],
        ))
        .expect("updateOne should convert");

        match operation {
            MongoOperation::UpdateOne { upsert, .. } => assert!(!upsert),
            other => panic!("expected UpdateOne, got {other:?}"),
        }
    }

    #[test]
    fn insert_one_requires_a_document_argument() {
        let error = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::InsertOne,
            vec![],
        ))
        .expect_err("insertOne with no arguments must error");

        assert!(error.to_string().contains("insertOne"));
    }

    #[test]
    fn insert_many_maps_document_array() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Container("users".to_string()),
            ScriptMethod::InsertMany,
            vec![serde_json::json!([{"a": 1}, {"b": 2}])],
        ))
        .expect("insertMany should convert");

        match operation {
            MongoOperation::InsertMany { documents } => {
                assert_eq!(documents, vec![doc! { "a": 1i64 }, doc! { "b": 2i64 }]);
            }
            other => panic!("expected InsertMany, got {other:?}"),
        }
    }

    #[test]
    fn drop_container_and_drop_database_carry_no_arguments() {
        assert!(matches!(
            script_operation_to_mongo_operation(&op(
                ScriptTarget::Container("users".to_string()),
                ScriptMethod::DropContainer,
                vec![],
            ))
            .unwrap(),
            MongoOperation::Drop
        ));
        assert!(matches!(
            script_operation_to_mongo_operation(&op(
                ScriptTarget::Database,
                ScriptMethod::DropDatabase,
                vec![],
            ))
            .unwrap(),
            MongoOperation::DropDatabase
        ));
    }

    #[test]
    fn create_container_requires_a_name_string() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Database,
            ScriptMethod::CreateContainer,
            vec![serde_json::json!("archive")],
        ))
        .expect("createCollection should convert");

        match operation {
            MongoOperation::CreateCollection { name } => assert_eq!(name, "archive"),
            other => panic!("expected CreateCollection, got {other:?}"),
        }

        let error = script_operation_to_mongo_operation(&op(
            ScriptTarget::Database,
            ScriptMethod::CreateContainer,
            vec![],
        ))
        .expect_err("createCollection with no name must error");
        assert!(error.to_string().contains("createCollection"));
    }

    #[test]
    fn run_command_maps_the_command_document() {
        let operation = script_operation_to_mongo_operation(&op(
            ScriptTarget::Database,
            ScriptMethod::RunCommand,
            vec![serde_json::json!({"ping": 1})],
        ))
        .expect("runCommand should convert");

        match operation {
            MongoOperation::RunCommand { command } => assert_eq!(command, doc! { "ping": 1i64 }),
            other => panic!("expected RunCommand, got {other:?}"),
        }
    }

    #[test]
    fn database_level_methods_need_no_arguments() {
        for method in [
            ScriptMethod::ListContainers,
            ScriptMethod::DatabaseStats,
            ScriptMethod::ServerStatus,
            ScriptMethod::CountDocuments,
        ] {
            script_operation_to_mongo_operation(&op(ScriptTarget::Database, method, vec![]))
                .unwrap_or_else(|e| panic!("{method:?} must convert with no arguments: {e}"));
        }
    }
}
