//! Snippets of the editor's language as completion items.
//!
//! A snippet shows up under its prefix with the `snippet` kind word. The
//! item writes the expanded body (the Tab preview of the menu shows it), and
//! the document starts the tabstop session when the editor reports that the
//! user committed the item: [`SnippetOffers`] remembers what the last menu
//! offered so the commit can be matched to its snippet.

use super::*;
use crate::snippet_picker::snippet_language_id;
use dbflux_core::{ParsedSnippet, parse_snippet};
use dbflux_storage::SnippetRecord;
use lsp_types::InsertTextFormat;
use std::ops::Range;

#[cfg(test)]
mod tests;

/// Rank group of snippet items: ahead of the keywords the prefix also matches.
const RANK_SNIPPET: u8 = 0;

/// One snippet the last completion request put in the menu.
struct SnippetOffer {
    label: String,
    parsed: ParsedSnippet,
}

/// What the last completion request offered, shared between the completion
/// provider (which fills it) and the document (which reads it when the editor
/// reports a committed completion item).
#[derive(Clone, Default)]
pub(crate) struct SnippetOffers {
    offers: Rc<RefCell<Vec<SnippetOffer>>>,
}

impl SnippetOffers {
    fn replace(&self, offers: Vec<SnippetOffer>) {
        *self.offers.borrow_mut() = offers;
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.offers.borrow().is_empty()
    }

    /// The offered snippet behind `item`, the item the user committed. The
    /// offer is picked by the item's identity (its label is the snippet's
    /// prefix, unique per language) and only when `text` still holds its
    /// expansion at `range`. The offers are spent, so a later edit does not
    /// match again.
    pub(crate) fn take_committed(
        &self,
        item: &CompletionItem,
        text: &str,
        range: Range<usize>,
    ) -> Option<ParsedSnippet> {
        if item.kind != Some(CompletionItemKind::SNIPPET) {
            return None;
        }
        let mut offers = self.offers.borrow_mut();
        let found = offers.iter().position(|offer| {
            offer.label == item.label && text.get(range.clone()) == Some(offer.parsed.text())
        })?;
        let offer = offers.swap_remove(found);
        offers.clear();
        Some(offer.parsed)
    }
}

/// Warnings already logged by one completion provider, so a broken snippet
/// or a failing store does not log on every keystroke.
#[derive(Default)]
pub(super) struct SnippetWarnings {
    snippet_ids: RefCell<HashSet<String>>,
    listing: Cell<bool>,
}

impl SnippetWarnings {
    pub(super) fn listing_failed(&self, error: &dyn std::fmt::Display) {
        if !self.listing.replace(true) {
            log::warn!("snippets could not be listed for completion: {error}");
        }
    }

    fn body_failed(&self, snippet: &SnippetRecord, error: &dyn std::fmt::Display) {
        if self.snippet_ids.borrow_mut().insert(snippet.id.clone()) {
            log::warn!(
                "snippet `{}` is left out of completion, its body does not parse: {error}",
                snippet.prefix
            );
        }
    }
}

/// The completion items for the snippets whose prefix starts with the word
/// being typed. Records the offers for [`SnippetOffers::take_committed`].
///
/// A snippet is offered only for a non-empty word that is not a member access
/// (`orders.sel`), so the menu is not flooded with snippets after `FROM `.
pub(super) fn snippet_completion_items(
    snippets: &[SnippetRecord],
    source: &str,
    cursor: usize,
    offers: &SnippetOffers,
    warnings: &SnippetWarnings,
) -> Vec<CompletionItem> {
    let (prefix_start, typed) = extract_identifier_prefix(source, cursor);
    let after_dot = prefix_start
        .checked_sub(1)
        .is_some_and(|dot| source.as_bytes().get(dot) == Some(&b'.'));
    if typed.is_empty() || after_dot {
        return Vec::new();
    }

    let typed_lower = typed.to_lowercase();
    let replace_range = completion_replace_range(source, prefix_start, cursor);
    let mut items = Vec::new();
    let mut recorded = Vec::new();

    for snippet in snippets {
        if !snippet.prefix.to_lowercase().starts_with(&typed_lower) {
            continue;
        }

        let parsed = match parse_snippet(&snippet.body) {
            Ok(parsed) => parsed,
            Err(error) => {
                warnings.body_failed(snippet, &error);
                continue;
            }
        };

        items.push(CompletionItem {
            label: snippet.prefix.clone(),
            detail: Some(snippet.name.clone()),
            kind: Some(CompletionItemKind::SNIPPET),
            insert_text_format: Some(InsertTextFormat::PLAIN_TEXT),
            filter_text: Some(typed.clone()),
            sort_text: Some(format!(
                "{}_{}",
                RANK_SNIPPET,
                snippet.prefix.to_lowercase()
            )),
            text_edit: Some(lsp_types::CompletionTextEdit::Edit(lsp_types::TextEdit {
                range: replace_range,
                new_text: parsed.text().to_string(),
            })),
            ..CompletionItem::default()
        });
        recorded.push(SnippetOffer {
            label: snippet.prefix.clone(),
            parsed,
        });
    }

    offers.replace(recorded);
    items
}

impl QueryCompletionProvider {
    /// Lets this provider offer the snippets of its language, recording what
    /// it offers in `offers`. Providers built without it (the console's
    /// single-line fields) offer none.
    pub(crate) fn with_snippets(mut self, offers: SnippetOffers) -> Self {
        self.snippets = Some(offers);
        self
    }

    pub(super) fn snippet_items(
        &self,
        source: &str,
        cursor: usize,
        cx: &mut App,
    ) -> Vec<CompletionItem> {
        let Some(offers) = &self.snippets else {
            return Vec::new();
        };

        let language = snippet_language_id(&self.query_language);
        let listed = self.app_state.update(cx, |state, _| {
            state.snippets.snippets_for_language(&language)
        });
        let snippets = match listed {
            Ok(snippets) => snippets,
            Err(error) => {
                self.snippet_warnings.listing_failed(&error);
                offers.replace(Vec::new());
                return Vec::new();
            }
        };

        let mut items =
            snippet_completion_items(&snippets, source, cursor, offers, &self.snippet_warnings);
        kinds::describe_items(&mut items, &KindWords::from_catalog(), &[], |_| false);
        items
    }
}
