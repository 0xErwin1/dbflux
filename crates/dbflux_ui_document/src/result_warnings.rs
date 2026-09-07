use dbflux_core::observability::EventSeverity;
use dbflux_core::{CrudResult, QueryResult};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError};
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(crate) enum ResultWarningContext {
    Query,
    TableBrowse,
    VisualQuery,
    CollectionBrowse,
    CrudReturning,
    MutationPreview,
}

impl ResultWarningContext {
    fn label(self) -> String {
        match self {
            Self::Query => dbflux_i18n::t!("document.shared.result_warnings.context.query"),
            Self::TableBrowse => {
                dbflux_i18n::t!("document.shared.result_warnings.context.table_browse")
            }
            Self::VisualQuery => {
                dbflux_i18n::t!("document.shared.result_warnings.context.visual_query")
            }
            Self::CollectionBrowse => {
                dbflux_i18n::t!("document.shared.result_warnings.context.collection_browse")
            }
            Self::CrudReturning => {
                dbflux_i18n::t!("document.shared.result_warnings.context.crud_returning")
            }
            Self::MutationPreview => {
                dbflux_i18n::t!("document.shared.result_warnings.context.mutation_preview")
            }
        }
    }
}

pub(crate) fn handoff_crud_returning_result(
    result: &mut CrudResult,
    report: impl FnMut(UserFacingError),
) {
    consume_crud_result_warnings(result, ResultWarningContext::CrudReturning, report);
}

pub(crate) fn handoff_bulk_crud_returning_results(
    results: &mut [CrudResult],
    report: impl FnMut(UserFacingError),
) {
    consume_bulk_crud_result_warnings(results, report);
}

pub(crate) fn handoff_sql_editor_result(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings(result, ResultWarningContext::Query, report);
}

pub(crate) fn handoff_table_browse_result(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings(result, ResultWarningContext::TableBrowse, report);
}

pub(crate) fn handoff_visual_query_result(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings(result, ResultWarningContext::VisualQuery, report);
}

pub(crate) fn handoff_collection_browse_result(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings(result, ResultWarningContext::CollectionBrowse, report);
}

pub(crate) fn handoff_mutation_preview_result(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings(result, ResultWarningContext::MutationPreview, report);
}

fn consume_query_result_warnings(
    result: &mut QueryResult,
    context: ResultWarningContext,
    report: impl FnMut(UserFacingError),
) {
    consume_warning_types_with(take_query_result_warning_types(result), context, report);
}

fn consume_crud_result_warnings(
    result: &mut CrudResult,
    context: ResultWarningContext,
    report: impl FnMut(UserFacingError),
) {
    consume_warning_types_with(take_crud_result_warning_types(result), context, report);
}

fn consume_bulk_crud_result_warnings(
    results: &mut [CrudResult],
    report: impl FnMut(UserFacingError),
) {
    let type_names = results
        .iter_mut()
        .flat_map(take_crud_result_warning_types)
        .collect();

    consume_warning_types_with(type_names, ResultWarningContext::CrudReturning, report);
}

fn take_query_result_warning_types(result: &mut QueryResult) -> Vec<String> {
    let mut type_names = result.take_unsupported_types();

    for additional_result in &mut result.additional_results {
        type_names.extend(additional_result.take_unsupported_types());
    }

    type_names
}

fn take_crud_result_warning_types(result: &mut CrudResult) -> Vec<String> {
    result.take_unsupported_types()
}

fn consume_warning_types_with(
    type_names: Vec<String>,
    context: ResultWarningContext,
    report: impl FnMut(UserFacingError),
) {
    unsupported_type_warnings(type_names, context)
        .into_iter()
        .for_each(report);
}

fn unsupported_type_warnings(
    type_names: Vec<String>,
    context: ResultWarningContext,
) -> Vec<UserFacingError> {
    type_names
        .into_iter()
        .filter(|type_name| !type_name.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|type_name| {
            UserFacingError::new(
                ErrorKind::Driver,
                dbflux_i18n::t!(
                    "document.shared.result_warnings.summary",
                    type_name = type_name,
                    context = context.label()
                ),
            )
            .with_cause(dbflux_i18n::t!(
                "document.shared.result_warnings.cause",
                type_name = type_name,
                context = context.label()
            ))
            .with_severity(EventSeverity::Warn)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ResultWarningContext, handoff_bulk_crud_returning_results,
        handoff_collection_browse_result, handoff_crud_returning_result, handoff_sql_editor_result,
        handoff_table_browse_result, handoff_visual_query_result, take_crud_result_warning_types,
        take_query_result_warning_types, unsupported_type_warnings,
    };
    use dbflux_core::observability::EventSeverity;
    use dbflux_core::{CrudResult, QueryResult};
    use dbflux_ui_base::user_error::UserFacingError;

    #[test]
    fn builds_one_warning_per_distinct_type() {
        let warnings = unsupported_type_warnings(
            vec![
                "varbit".to_string(),
                "bit".to_string(),
                "varbit".to_string(),
            ],
            ResultWarningContext::Query,
        );

        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].severity, EventSeverity::Warn);
        assert_eq!(
            warnings[0].summary,
            "Unsupported database type 'bit' in query result"
        );
        assert_eq!(
            warnings[1].summary,
            "Unsupported database type 'varbit' in query result"
        );
    }

    #[test]
    fn warning_text_contains_only_type_and_safe_context() {
        let warnings = unsupported_type_warnings(
            vec!["vector".to_string()],
            ResultWarningContext::CrudReturning,
        );

        let warning = &warnings[0];
        assert_eq!(
            warning.summary,
            "Unsupported database type 'vector' in mutation RETURNING result"
        );
        assert_eq!(
            warning.cause.as_deref(),
            Some("The mutation RETURNING result contains values of unsupported type 'vector'.")
        );
    }

    #[test]
    fn query_warning_metadata_is_consumed_once_before_result_storage() {
        let mut result = QueryResult::empty();
        result.set_unsupported_types(["bit".to_string()]);

        let mut additional_result = QueryResult::empty();
        additional_result.set_unsupported_types(["bit".to_string(), "varbit".to_string()]);
        result.additional_results.push(additional_result);

        assert_eq!(
            unsupported_type_warnings(
                take_query_result_warning_types(&mut result),
                ResultWarningContext::Query,
            )
            .len(),
            2
        );
        assert!(take_query_result_warning_types(&mut result).is_empty());
    }

    #[test]
    fn crud_warning_metadata_is_consumed_once_before_returning_application() {
        let mut result = CrudResult::empty();
        result.set_unsupported_types(["halfvec".to_string()]);

        assert_eq!(take_crud_result_warning_types(&mut result), vec!["halfvec"]);
        assert!(take_crud_result_warning_types(&mut result).is_empty());
    }

    #[test]
    fn sql_editor_history_handoff_reports_once_before_replay() {
        let mut result = QueryResult::empty();
        result.set_unsupported_types(["bit".to_string(), "bit".to_string()]);

        let mut additional_result = QueryResult::empty();
        additional_result.set_unsupported_types(["varbit".to_string()]);
        result.additional_results.push(additional_result);

        let mut summaries = Vec::new();
        handoff_sql_editor_result(&mut result, |warning| {
            summaries.push(warning.summary);
        });
        handoff_sql_editor_result(&mut result, |warning| {
            summaries.push(warning.summary);
        });

        assert_eq!(
            summaries,
            [
                "Unsupported database type 'bit' in query result",
                "Unsupported database type 'varbit' in query result",
            ]
        );
    }

    #[test]
    fn table_browse_refresh_handoff_reports_once_before_replay() {
        assert_query_handoff_reports_once("table browse result", |result, report| {
            handoff_table_browse_result(result, report)
        });
    }

    #[test]
    fn visual_query_handoff_reports_once_before_replay() {
        assert_query_handoff_reports_once("visual query result", |result, report| {
            handoff_visual_query_result(result, report)
        });
    }

    #[test]
    fn collection_browse_handoff_reports_once_before_replay() {
        assert_query_handoff_reports_once("collection browse result", |result, report| {
            handoff_collection_browse_result(result, report)
        });
    }

    #[test]
    fn crud_returning_handoff_reports_once_before_apply_or_discard() {
        let mut result = CrudResult::empty();
        result.set_unsupported_types(["bit".to_string(), "varbit".to_string()]);

        let mut summaries = Vec::new();
        handoff_crud_returning_result(&mut result, |warning| summaries.push(warning.summary));
        handoff_crud_returning_result(&mut result, |warning| summaries.push(warning.summary));

        assert_eq!(
            summaries,
            [
                "Unsupported database type 'bit' in mutation RETURNING result",
                "Unsupported database type 'varbit' in mutation RETURNING result",
            ]
        );
    }

    #[test]
    fn bulk_crud_handoff_deduplicates_partial_successes_before_reporting() {
        let mut first = CrudResult::empty();
        first.set_unsupported_types(["bit".to_string(), "varbit".to_string()]);
        let mut second = CrudResult::empty();
        second.set_unsupported_types(["bit".to_string()]);
        let mut results = vec![first, second];

        let mut summaries = Vec::new();
        handoff_bulk_crud_returning_results(&mut results, |warning| {
            summaries.push(warning.summary);
        });

        assert_eq!(
            summaries,
            [
                "Unsupported database type 'bit' in mutation RETURNING result",
                "Unsupported database type 'varbit' in mutation RETURNING result",
            ]
        );
        assert!(
            results
                .iter_mut()
                .all(|result| take_crud_result_warning_types(result).is_empty())
        );
    }

    #[test]
    fn context_labels_resolve_in_both_locales() {
        for context in [
            ResultWarningContext::Query,
            ResultWarningContext::TableBrowse,
            ResultWarningContext::VisualQuery,
            ResultWarningContext::CollectionBrowse,
            ResultWarningContext::CrudReturning,
            ResultWarningContext::MutationPreview,
        ] {
            let en_label = context.label();
            assert!(!en_label.is_empty());
        }
    }

    fn assert_query_handoff_reports_once(
        label: &str,
        consume: impl Fn(&mut QueryResult, &mut dyn FnMut(UserFacingError)),
    ) {
        let mut result = QueryResult::empty();
        result.set_unsupported_types(["bit".to_string(), "varbit".to_string()]);

        let mut summaries = Vec::new();
        let mut report = |warning: UserFacingError| summaries.push(warning.summary);
        consume(&mut result, &mut report);
        consume(&mut result, &mut report);

        assert_eq!(
            summaries,
            [
                format!("Unsupported database type 'bit' in {label}"),
                format!("Unsupported database type 'varbit' in {label}"),
            ]
        );
    }
}
