//! The translated kind word a SQL suggestion shows right-aligned in the
//! completion menu (`label_details.description`).

use crate::completion_support::normalize_identifier;
use lsp_types::{CompletionItem, CompletionItemKind, CompletionItemLabelDetails};
use std::collections::HashSet;

#[cfg(test)]
mod tests;

/// Every kind word, translated. Built once per completion request, before the
/// loop over the suggestions, so a keystroke costs eight lookups whatever the
/// number of items.
pub(super) struct KindWords {
    table: String,
    view: String,
    schema: String,
    column: String,
    alias: String,
    keyword: String,
    cte: String,
    snippet: String,
}

impl KindWords {
    /// Resolves each word through `translate`, which receives a catalog key.
    pub(super) fn resolve(translate: impl Fn(&str) -> String) -> Self {
        Self {
            table: translate("document.code.completion_kind.table"),
            view: translate("document.code.completion_kind.view"),
            schema: translate("document.code.completion_kind.schema"),
            column: translate("document.code.completion_kind.column"),
            alias: translate("document.code.completion_kind.alias"),
            keyword: translate("document.code.completion_kind.keyword"),
            cte: translate("document.code.completion_kind.cte"),
            snippet: translate("document.code.completion_kind.snippet"),
        }
    }

    pub(super) fn from_catalog() -> Self {
        Self::resolve(dbflux_i18n::translate)
    }
}

/// Sets the kind word of every SQL suggestion in `items`.
///
/// A `STRUCT` is a CTE when its label is one of `cte_names` (a CTE shadows a
/// relation of the same name), a view when `is_view` says so, and a table
/// otherwise. Kinds without a word are left as they are.
pub(super) fn describe_items(
    items: &mut [CompletionItem],
    words: &KindWords,
    cte_names: &[String],
    is_view: impl Fn(&str) -> bool,
) {
    let ctes: HashSet<String> = cte_names
        .iter()
        .map(|name| normalize_identifier(name))
        .collect();

    for item in items {
        let Some(kind) = item.kind else {
            continue;
        };

        let word = if kind == CompletionItemKind::STRUCT {
            if ctes.contains(&normalize_identifier(&item.label)) {
                &words.cte
            } else if is_view(&item.label) {
                &words.view
            } else {
                &words.table
            }
        } else if kind == CompletionItemKind::MODULE {
            &words.schema
        } else if kind == CompletionItemKind::FIELD {
            &words.column
        } else if kind == CompletionItemKind::VARIABLE {
            &words.alias
        } else if kind == CompletionItemKind::KEYWORD {
            &words.keyword
        } else if kind == CompletionItemKind::SNIPPET {
            &words.snippet
        } else {
            continue;
        };

        item.label_details = Some(CompletionItemLabelDetails {
            detail: None,
            description: Some(word.clone()),
        });
    }
}
