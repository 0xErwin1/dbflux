use dbflux_core::observability::EventSeverity;
use dbflux_core::{CrudResult, QueryResult};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::App;
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(crate) enum ResultWarningContext {
    Query,
    TableBrowse,
    VisualQuery,
    CollectionBrowse,
    CrudReturning,
}

impl ResultWarningContext {
    fn label(self) -> &'static str {
        match self {
            Self::Query => "query result",
            Self::TableBrowse => "table browse result",
            Self::VisualQuery => "visual query result",
            Self::CollectionBrowse => "collection browse result",
            Self::CrudReturning => "mutation RETURNING result",
        }
    }
}

pub(crate) fn consume_sql_editor_result_warnings(result: &mut QueryResult, cx: &mut App) {
    consume_sql_editor_result_warnings_with(result, |warning| report_error(warning, cx));
}

pub(crate) fn consume_table_browse_result_warnings(result: &mut QueryResult, cx: &mut App) {
    consume_table_browse_result_warnings_with(result, |warning| report_error(warning, cx));
}

pub(crate) fn consume_visual_query_result_warnings(result: &mut QueryResult, cx: &mut App) {
    consume_visual_query_result_warnings_with(result, |warning| report_error(warning, cx));
}

pub(crate) fn consume_collection_browse_result_warnings(result: &mut QueryResult, cx: &mut App) {
    consume_collection_browse_result_warnings_with(result, |warning| report_error(warning, cx));
}

pub(crate) fn consume_crud_result_warnings(
    result: &mut CrudResult,
    context: ResultWarningContext,
    cx: &mut App,
) {
    consume_crud_result_warnings_with(result, context, |warning| report_error(warning, cx));
}

fn consume_query_result_warnings_with(
    result: &mut QueryResult,
    context: ResultWarningContext,
    report: impl FnMut(UserFacingError),
) {
    consume_warning_types_with(take_query_result_warning_types(result), context, report);
}

fn consume_sql_editor_result_warnings_with(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings_with(result, ResultWarningContext::Query, report);
}

fn consume_table_browse_result_warnings_with(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings_with(result, ResultWarningContext::TableBrowse, report);
}

fn consume_visual_query_result_warnings_with(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings_with(result, ResultWarningContext::VisualQuery, report);
}

fn consume_collection_browse_result_warnings_with(
    result: &mut QueryResult,
    report: impl FnMut(UserFacingError),
) {
    consume_query_result_warnings_with(result, ResultWarningContext::CollectionBrowse, report);
}

fn consume_crud_result_warnings_with(
    result: &mut CrudResult,
    context: ResultWarningContext,
    report: impl FnMut(UserFacingError),
) {
    consume_warning_types_with(take_crud_result_warning_types(result), context, report);
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
                format!(
                    "Unsupported database type '{type_name}' in {}",
                    context.label()
                ),
            )
            .with_cause(format!(
                "The {} contains values of unsupported type '{type_name}'.",
                context.label()
            ))
            .with_severity(EventSeverity::Warn)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ResultWarningContext, consume_collection_browse_result_warnings_with,
        consume_crud_result_warnings_with, consume_sql_editor_result_warnings_with,
        consume_table_browse_result_warnings_with, consume_visual_query_result_warnings_with,
        take_crud_result_warning_types, take_query_result_warning_types, unsupported_type_warnings,
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
        consume_sql_editor_result_warnings_with(&mut result, |warning| {
            summaries.push(warning.summary);
        });
        consume_sql_editor_result_warnings_with(&mut result, |warning| {
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
            consume_table_browse_result_warnings_with(result, report)
        });
    }

    #[test]
    fn visual_query_handoff_reports_once_before_replay() {
        assert_query_handoff_reports_once("visual query result", |result, report| {
            consume_visual_query_result_warnings_with(result, report)
        });
    }

    #[test]
    fn collection_browse_handoff_reports_once_before_replay() {
        assert_query_handoff_reports_once("collection browse result", |result, report| {
            consume_collection_browse_result_warnings_with(result, report)
        });
    }

    #[test]
    fn crud_returning_handoff_reports_once_before_apply_or_discard() {
        let mut result = CrudResult::empty();
        result.set_unsupported_types(["bit".to_string(), "varbit".to_string()]);

        let mut summaries = Vec::new();
        consume_crud_result_warnings_with(
            &mut result,
            ResultWarningContext::CrudReturning,
            |warning| summaries.push(warning.summary),
        );
        consume_crud_result_warnings_with(
            &mut result,
            ResultWarningContext::CrudReturning,
            |warning| summaries.push(warning.summary),
        );

        assert_eq!(
            summaries,
            [
                "Unsupported database type 'bit' in mutation RETURNING result",
                "Unsupported database type 'varbit' in mutation RETURNING result",
            ]
        );
    }

    #[test]
    fn production_handoffs_invoke_the_destructive_consumers_once() {
        let execution = include_str!("code/execution.rs");
        let query = include_str!("data_grid_panel/query.rs");
        let mutations = include_str!("data_grid_panel/mutations.rs");
        let context_menu = include_str!("data_grid_panel/context_menu/mod.rs");

        assert_eq!(
            execution
                .matches("consume_sql_editor_result_warnings(")
                .count(),
            1
        );
        assert_eq!(
            query
                .matches("consume_table_browse_result_warnings(")
                .count(),
            1
        );
        assert_eq!(
            query
                .matches("consume_visual_query_result_warnings(")
                .count(),
            1
        );
        assert_eq!(
            query
                .matches("consume_collection_browse_result_warnings(")
                .count(),
            1
        );
        assert_eq!(
            mutations.matches("consume_crud_result_warnings(").count()
                + context_menu
                    .matches("consume_crud_result_warnings(")
                    .count(),
            10
        );
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
