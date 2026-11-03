//! Highlighting read-only text a host draws itself.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use gpui::{App, HighlightStyle};
use gpui_component::ActiveTheme;
use gpui_component::highlighter::{
    GrammarConfig, HighlightTheme, LanguageRegistry, SyntaxHighlighter,
};

/// How many recent results [`highlight`] keeps, so a view rebuilt on every
/// frame does not parse its text again.
const CACHED_RESULTS: usize = 8;

/// The most text the cache holds across its results. A text larger than this
/// is highlighted every time it is asked for rather than kept, so a few large
/// bodies cannot pin megabytes on the UI thread.
pub(super) const CACHED_BYTES: usize = 1024 * 1024;

struct CachedRuns {
    language: String,
    grammar: GrammarConfig,
    theme: Arc<HighlightTheme>,
    text: String,
    runs: Vec<(Range<usize>, HighlightStyle)>,
}

thread_local! {
    static RECENT: RefCell<VecDeque<CachedRuns>> = const { RefCell::new(VecDeque::new()) };
}

/// The syntax runs of `text` in `language`, styled by the current theme:
/// sorted, non-overlapping byte ranges ready for `StyledText::with_highlights`.
/// A language nobody registered, or one whose grammar cannot highlight,
/// yields no runs.
pub fn highlight(language: &str, text: &str, cx: &App) -> Vec<(Range<usize>, HighlightStyle)> {
    let Some(grammar) = LanguageRegistry::singleton().language(language) else {
        return Vec::new();
    };
    let theme = cx.theme().highlight_theme.clone();

    let cached = RECENT.with_borrow(|recent| {
        recent
            .iter()
            .find(|entry| {
                entry.language == language
                    && Arc::ptr_eq(&entry.theme, &theme)
                    && entry.grammar == grammar
                    && entry.text == text
            })
            .map(|entry| entry.runs.clone())
    });
    if let Some(runs) = cached {
        return runs;
    }

    let rope = gpui_component::Rope::from(text);
    let mut highlighter = SyntaxHighlighter::new(language);
    highlighter.update(None, &rope, None);
    let runs: Vec<_> = highlighter
        .styles(&(0..text.len()), &*theme)
        .into_iter()
        .filter(|(range, style)| !range.is_empty() && *style != HighlightStyle::default())
        .collect();

    if text.len() > CACHED_BYTES {
        return runs;
    }
    RECENT.with_borrow_mut(|recent| {
        let mut held: usize = recent.iter().map(|entry| entry.text.len()).sum();
        while recent.len() == CACHED_RESULTS || held + text.len() > CACHED_BYTES {
            let Some(evicted) = recent.pop_back() else {
                break;
            };
            held -= evicted.text.len();
        }
        recent.push_front(CachedRuns {
            language: language.to_string(),
            grammar,
            theme,
            text: text.to_string(),
            runs: runs.clone(),
        });
    });
    runs
}

/// The text bytes the cache holds now.
#[cfg(test)]
pub(super) fn cached_bytes() -> usize {
    RECENT.with_borrow(|recent| recent.iter().map(|entry| entry.text.len()).sum())
}

/// `top` drawn over `base`: where a top run covers a byte its style replaces
/// the base style, and among top runs the later one wins. The result is
/// sorted and non-overlapping, with equal neighbours joined, so a host lays
/// its own runs (search matches, references) over [`highlight`]'s.
pub fn overlay(
    base: impl IntoIterator<Item = (Range<usize>, HighlightStyle)>,
    top: impl IntoIterator<Item = (Range<usize>, HighlightStyle)>,
) -> Vec<(Range<usize>, HighlightStyle)> {
    // Each run's priority is its position in base-then-top order; a sweep
    // over the run edges keeps the covering runs and paints each stretch
    // with the highest one.
    let mut edges: Vec<(usize, bool, usize, HighlightStyle)> = base
        .into_iter()
        .chain(top)
        .filter(|(range, _)| range.start < range.end)
        .enumerate()
        .flat_map(|(priority, (range, style))| {
            [
                (range.start, true, priority, style),
                (range.end, false, priority, style),
            ]
        })
        .collect();
    // At one offset the runs that end leave before the runs that start join.
    edges.sort_unstable_by_key(|(offset, starts, priority, _)| (*offset, *starts, *priority));

    let mut active: BTreeMap<usize, HighlightStyle> = BTreeMap::new();
    let mut painted: Vec<(Range<usize>, HighlightStyle)> = Vec::new();
    let mut position = 0;

    for (offset, starts, priority, style) in edges {
        if offset > position
            && let Some((_, style)) = active.last_key_value()
        {
            push_joined(&mut painted, position..offset, *style);
        }
        position = offset;

        if starts {
            active.insert(priority, style);
        } else {
            active.remove(&priority);
        }
    }

    painted
}

fn push_joined(
    painted: &mut Vec<(Range<usize>, HighlightStyle)>,
    range: Range<usize>,
    style: HighlightStyle,
) {
    if let Some((last, last_style)) = painted.last_mut()
        && last.end == range.start
        && *last_style == style
    {
        last.end = range.end;
        return;
    }
    painted.push((range, style));
}
