//! Field-path completion for the document query bar slots.
//!
//! Paths come from the sampled schema (`customer.email`, `items.sku`). A path
//! may be typed bare (`customer.em`) or inside quotes (`"price.am`); the
//! replacement keeps whatever quote the user opened and quotes a dotted path
//! typed bare, since relaxed JSON only accepts plain identifiers unquoted.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use dbflux_components::controls::{CompletionProvider, Rope};
use gpui::{App, Task, Window};
use lsp_types::{CompletionContext, CompletionItemKind, CompletionResponse};

use crate::completion_support::{completion_replace_range, push_completion_item_ranked};

/// Where a field-path prefix starts and what replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathCompletion {
    /// Byte offset of the first replaced character.
    pub start: usize,
    /// Typed prefix, without an opening quote.
    pub prefix: String,
    /// Replacement texts, best match first.
    pub items: Vec<String>,
}

fn is_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte == b'.'
}

/// Completions for the field path that ends at `cursor` in `source`.
///
/// Returns `None` when the cursor does not follow a path prefix in a key
/// position, so the menu stays closed inside values and operators.
pub(crate) fn field_path_completions(
    paths: &[String],
    source: &str,
    cursor: usize,
) -> Option<PathCompletion> {
    let cursor = cursor.min(source.len());
    let bytes = source.as_bytes();

    let mut start = cursor;
    while start > 0 && bytes.get(start - 1).copied().is_some_and(is_path_byte) {
        start -= 1;
    }

    let prefix = source.get(start..cursor)?.to_string();
    if prefix.starts_with('$') {
        return None;
    }

    let quote = match start.checked_sub(1).and_then(|index| bytes.get(index)) {
        Some(b'"') => Some('"'),
        Some(b'\'') => Some('\''),
        _ => None,
    };

    let before_key = source
        .get(..start.saturating_sub(usize::from(quote.is_some())))?
        .trim_end();
    let in_key_position =
        before_key.is_empty() || before_key.ends_with('{') || before_key.ends_with(',');
    if !in_key_position {
        return None;
    }

    let lowered = prefix.to_lowercase();
    let mut matches: Vec<&String> = paths
        .iter()
        .filter(|path| path.to_lowercase().starts_with(&lowered) && **path != prefix)
        .collect();
    matches.sort_by_key(|path| (path.matches('.').count(), path.len()));

    if matches.is_empty() {
        return None;
    }

    let items = matches
        .into_iter()
        .map(|path| {
            let needs_quotes = quote.is_none() && path.contains('.');
            if needs_quotes {
                format!("\"{path}\"")
            } else {
                path.clone()
            }
        })
        .collect();

    Some(PathCompletion {
        start,
        prefix,
        items,
    })
}

/// Completion provider backed by the field paths of the latest schema sample.
pub(crate) struct DocumentFieldCompletionProvider {
    paths: Rc<RefCell<Vec<String>>>,
}

impl DocumentFieldCompletionProvider {
    pub fn new(paths: Rc<RefCell<Vec<String>>>) -> Self {
        Self { paths }
    }
}

impl CompletionProvider for DocumentFieldCompletionProvider {
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        _trigger: CompletionContext,
        _window: &mut Window,
        _cx: &mut App,
    ) -> Task<anyhow::Result<CompletionResponse>> {
        let source = text.to_string();
        let cursor = source.floor_char_boundary(offset.min(source.len()));

        let mut items = Vec::new();

        if let Some(completion) = field_path_completions(&self.paths.borrow(), &source, cursor) {
            let replace_range = completion_replace_range(&source, completion.start, cursor);
            let mut seen = HashSet::new();

            for (rank, item) in completion.items.iter().enumerate() {
                push_completion_item_ranked(
                    &mut items,
                    &mut seen,
                    item,
                    CompletionItemKind::FIELD,
                    &completion.prefix,
                    replace_range,
                    u8::try_from(rank.min(usize::from(u8::MAX))).unwrap_or(u8::MAX),
                );
            }
        }

        Task::ready(Ok(CompletionResponse::Array(items)))
    }

    fn is_completion_trigger(&self, _offset: usize, new_text: &str, _cx: &mut App) -> bool {
        if new_text.is_empty() {
            return true;
        }

        new_text.len() == 1
            && new_text.as_bytes().first().is_some_and(|byte| {
                byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'.' || *byte == b'"'
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths() -> Vec<String> {
        [
            "_id",
            "customer",
            "customer.email",
            "customer.tier",
            "created_at",
            "items",
            "items.sku",
        ]
        .iter()
        .map(|path| path.to_string())
        .collect()
    }

    #[test]
    fn a_bare_prefix_completes_and_quotes_dotted_paths() {
        let source = "{ cust";
        let completion = field_path_completions(&paths(), source, source.len()).expect("items");

        assert_eq!(completion.start, 2);
        assert_eq!(completion.prefix, "cust");
        assert_eq!(
            completion.items,
            vec!["customer", "\"customer.tier\"", "\"customer.email\""]
        );
    }

    #[test]
    fn a_quoted_prefix_keeps_the_open_quote() {
        let source = "{ \"customer.e";
        let completion = field_path_completions(&paths(), source, source.len()).expect("items");

        assert_eq!(completion.items, vec!["customer.email"]);
        assert_eq!(completion.start, 3);
    }

    #[test]
    fn keys_after_a_comma_complete() {
        let source = "{ status: \"x\", ite";
        let completion = field_path_completions(&paths(), source, source.len()).expect("items");

        assert_eq!(completion.items, vec!["items", "\"items.sku\""]);
    }

    #[test]
    fn values_and_operators_do_not_complete() {
        let value = "{ status: cre";
        assert!(field_path_completions(&paths(), value, value.len()).is_none());

        let operator = "{ total: { $g";
        assert!(field_path_completions(&paths(), operator, operator.len()).is_none());
    }

    #[test]
    fn an_exact_match_alone_offers_nothing() {
        let source = "{ items.sku";
        assert!(field_path_completions(&paths(), source, source.len()).is_none());
    }
}
