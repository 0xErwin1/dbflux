//! Snippets of the editor's language as completion items.
//!
//! A snippet shows up under its prefix with the `snippet` kind word. The
//! item writes the expanded body (the Tab preview of the menu shows it), and
//! the document starts the tabstop session once the menu commits the item,
//! because the vendored menu has no commit hook: [`SnippetOffers`] remembers
//! what the last menu offered so the document can recognise the commit.

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
    start: usize,
    parsed: ParsedSnippet,
}

/// What the last completion request offered, shared between the completion
/// provider (which fills it) and the document (which reads it on a change).
#[derive(Clone, Default)]
pub(crate) struct SnippetOffers {
    offers: Rc<RefCell<Vec<SnippetOffer>>>,
}

impl SnippetOffers {
    fn replace(&self, offers: Vec<SnippetOffer>) {
        *self.offers.borrow_mut() = offers;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offers.borrow().is_empty()
    }

    /// Whether an offered expansion would end at `cursor`: a cheap test that
    /// keeps the editor's many notifications from copying its text.
    fn may_end_at(&self, cursor: usize) -> bool {
        self.offers
            .borrow()
            .iter()
            .any(|offer| offer.start + offer.parsed.text().len() == cursor)
    }

    /// The offered snippet that `text` now holds, expanded, ending at
    /// `cursor`: the editor committed its menu item. The offers are spent, so
    /// the edit that starts the session does not match again.
    pub(crate) fn take_committed(
        &self,
        text: &str,
        cursor: usize,
    ) -> Option<(ParsedSnippet, Range<usize>)> {
        let mut offers = self.offers.borrow_mut();
        let found = offers.iter().position(|offer| {
            let end = offer.start + offer.parsed.text().len();
            end == cursor && text.get(offer.start..end) == Some(offer.parsed.text())
        })?;
        let offer = offers.swap_remove(found);
        offers.clear();

        let end = offer.start + offer.parsed.text().len();
        Some((offer.parsed, offer.start..end))
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
            start: prefix_start,
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

impl CodeDocument {
    /// Starts the tabstop session when the completion menu has just committed
    /// a snippet item. While the menu is still open the editor is only
    /// previewing the item (Tab cycling), and no session starts.
    ///
    /// The menu writes its item without an `InputEvent::Change`, so the
    /// document watches the editor's notifications instead.
    pub(in crate::code) fn snippet_after_completion(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.editor.snippet_offers.is_empty() {
            return;
        }

        let input = self.editor.input_state.read(cx);
        if input.completion_menu_state().open {
            return;
        }
        let cursor = input.selected_range().end;
        if !self.editor.snippet_offers.may_end_at(cursor) {
            return;
        }
        let text = input.value().to_string();

        if let Some((parsed, range)) = self.editor.snippet_offers.take_committed(&text, cursor) {
            self.insert_snippet(&parsed, range, window, cx);
        }
    }
}
