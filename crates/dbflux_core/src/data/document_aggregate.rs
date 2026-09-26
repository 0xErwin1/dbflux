//! Aggregation pipelines run against a document collection.
//!
//! A pipeline is a JSON array of stage documents, each naming one `$` stage
//! operator (`[{ "$match": { ... } }, { "$group": { ... } }]`). Drivers that
//! run them report `DocumentFeatures::AGGREGATE` and implement
//! `Connection::aggregate_collection`.

use serde::{Deserialize, Serialize};

use crate::CollectionRef;

/// Asks a driver to run an aggregation pipeline over a collection and return
/// at most `limit` result documents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionAggregateRequest {
    pub collection: CollectionRef,
    /// Stage documents, in order.
    pub pipeline: Vec<serde_json::Value>,
    /// Most documents the result holds. A pipeline that yields more is cut
    /// and the result is marked truncated.
    pub limit: u32,
    /// Lets memory-hungry stages spill to disk on the server. Off unless the
    /// caller asks for it.
    #[serde(default)]
    pub allow_disk_use: bool,
}

impl CollectionAggregateRequest {
    pub fn new(collection: CollectionRef, pipeline: Vec<serde_json::Value>, limit: u32) -> Self {
        Self {
            collection,
            pipeline,
            limit,
            allow_disk_use: false,
        }
    }
}

/// Why a pipeline text cannot be run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AggregatePipelineError {
    /// The text is not JSON (relaxed keys and single quotes are accepted).
    #[error("invalid JSON: {0}")]
    Syntax(String),
    /// The JSON is not an array of stages.
    #[error("the pipeline must be an array of stages")]
    NotArray,
    /// Stage `index` (0-based) is not a document.
    #[error("stage {} must be a document", .index + 1)]
    StageNotDocument { index: usize },
    /// Stage `index` does not name exactly one field.
    #[error("stage {} must name exactly one stage operator", .index + 1)]
    StageFieldCount { index: usize },
    /// Stage `index` names a field that is not a `$` operator.
    #[error("stage {} field `{name}` is not a stage operator", .index + 1)]
    StageNotOperator { index: usize, name: String },
}

/// Parses pipeline text into its stage documents.
///
/// The text is a JSON array (relaxed keys and single-quoted strings are
/// accepted, like the query bar slots). Every element must be a document
/// naming exactly one `$`-prefixed stage operator. An empty array is a valid
/// pipeline that returns the collection's documents.
pub fn parse_aggregate_pipeline(
    text: &str,
) -> Result<Vec<serde_json::Value>, AggregatePipelineError> {
    let parsed = crate::parse_relaxed_json(text)
        .map_err(|error| AggregatePipelineError::Syntax(error.to_string()))?;

    let serde_json::Value::Array(stages) = parsed else {
        return Err(AggregatePipelineError::NotArray);
    };

    for (index, stage) in stages.iter().enumerate() {
        let serde_json::Value::Object(fields) = stage else {
            return Err(AggregatePipelineError::StageNotDocument { index });
        };

        let mut names = fields.keys();
        let (Some(name), None) = (names.next(), names.next()) else {
            return Err(AggregatePipelineError::StageFieldCount { index });
        };

        if !name.starts_with('$') || name.len() < 2 {
            return Err(AggregatePipelineError::StageNotOperator {
                index,
                name: name.clone(),
            });
        }
    }

    Ok(stages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_relaxed_pipeline_parses_into_its_stages() {
        let stages = parse_aggregate_pipeline(
            "[{ $match: { status: 'paid' } }, { $group: { _id: '$region', total: { $sum: 1 } } }]",
        )
        .expect("pipeline parses");

        assert_eq!(
            stages,
            vec![
                json!({ "$match": { "status": "paid" } }),
                json!({ "$group": { "_id": "$region", "total": { "$sum": 1 } } }),
            ]
        );
    }

    #[test]
    fn an_empty_pipeline_is_valid() {
        assert_eq!(parse_aggregate_pipeline("[]"), Ok(Vec::new()));
    }

    #[test]
    fn invalid_json_is_a_syntax_error() {
        assert!(matches!(
            parse_aggregate_pipeline("[{ $match: }]"),
            Err(AggregatePipelineError::Syntax(_))
        ));
    }

    #[test]
    fn a_single_stage_outside_an_array_is_refused() {
        assert_eq!(
            parse_aggregate_pipeline("{ $match: {} }"),
            Err(AggregatePipelineError::NotArray)
        );
    }

    #[test]
    fn every_stage_must_be_a_document_with_one_operator() {
        assert_eq!(
            parse_aggregate_pipeline("[{ $match: {} }, 3]"),
            Err(AggregatePipelineError::StageNotDocument { index: 1 })
        );
        assert_eq!(
            parse_aggregate_pipeline("[{ $match: {}, $limit: 5 }]"),
            Err(AggregatePipelineError::StageFieldCount { index: 0 })
        );
        assert_eq!(
            parse_aggregate_pipeline("[{}]"),
            Err(AggregatePipelineError::StageFieldCount { index: 0 })
        );
        assert_eq!(
            parse_aggregate_pipeline("[{ match: {} }]"),
            Err(AggregatePipelineError::StageNotOperator {
                index: 0,
                name: "match".to_string()
            })
        );
    }

    #[test]
    fn errors_name_stages_from_one() {
        assert_eq!(
            AggregatePipelineError::StageFieldCount { index: 1 }.to_string(),
            "stage 2 must name exactly one stage operator"
        );
    }
}
