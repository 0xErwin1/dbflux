//! The "Insert snippet" picker of the query code editor.
//!
//! A modal over the editor that lists the snippets of the editor's language,
//! filters them as the user types, previews the highlighted body and reports
//! the chosen snippet. The document turns the choice into an insertion at the
//! cursor (`CodeDocument::insert_snippet`), which starts the tabstop session.
//!
//! The keys are the history modal's (Up/Down, Ctrl+J/K, Enter, Escape). Under
//! the workspace they arrive as commands, which `CodeDocument::dispatch_command`
//! hands to the picker (`dispatch_to_snippet_picker`); the picker's own action
//! listeners answer the same keys when nothing above it claims them.

use dbflux_app::keymap::ContextId;
use dbflux_components::actions::{Cancel, Execute, SelectNext, SelectPrev};
use dbflux_components::composites::ListRow;
use dbflux_components::controls::{GpuiInput as Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::{Badge, BadgeTone, Text};
use dbflux_components::tokens::{self, ChromeColors, Spacing};
use dbflux_core::QueryLanguage;
use dbflux_storage::SnippetRecord;
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::{MoveDown, MoveUp};
use gpui_component::{ActiveTheme, Sizable};

mod preview;

#[cfg(test)]
mod tests;

/// The stable id snippets of `language` are stored under (`"sql"` for SQL).
///
/// Snippets are keyed by this string, not by the enum, because the enum has
/// `Custom(String)` and may grow. Only SQL ships snippets today; every other
/// language maps to its own id so its snippets show up once they exist.
pub(crate) fn snippet_language_id(language: &QueryLanguage) -> String {
    match language {
        QueryLanguage::Sql => "sql".to_string(),
        QueryLanguage::CloudWatchLogsInsightsQl => "cloudwatch_logs_insights".to_string(),
        QueryLanguage::OpenSearchPpl => "opensearch_ppl".to_string(),
        QueryLanguage::OpenSearchSql => "opensearch_sql".to_string(),
        QueryLanguage::MongoQuery => "mongo".to_string(),
        QueryLanguage::RedisCommands => "redis".to_string(),
        QueryLanguage::Cypher => "cypher".to_string(),
        QueryLanguage::InfluxQuery => "influxql".to_string(),
        QueryLanguage::Flux => "flux".to_string(),
        QueryLanguage::Cql => "cql".to_string(),
        QueryLanguage::Lua => "lua".to_string(),
        QueryLanguage::Python => "python".to_string(),
        QueryLanguage::Bash => "bash".to_string(),
        QueryLanguage::Custom(name) => name.trim().to_lowercase(),
    }
}

/// What the picker reports to the document that opened it.
#[derive(Clone, Debug)]
pub(crate) enum SnippetPickerEvent {
    /// Enter (or a click) on a snippet.
    Chosen(Box<SnippetRecord>),
    /// Escape or the close button.
    Closed,
}

/// The indices of `snippets` that match `query`, best match first. An empty
/// query keeps every snippet in its given order.
pub(crate) fn filter_snippets(snippets: &[SnippetRecord], query: &str) -> Vec<usize> {
    let query = query.trim();
    if query.is_empty() {
        return (0..snippets.len()).collect();
    }

    let matcher = SkimMatcherV2::default();
    let mut scored: Vec<(usize, i64)> = snippets
        .iter()
        .enumerate()
        .filter_map(|(index, snippet)| {
            let haystack = format!("{} {}", snippet.name, snippet.prefix);
            matcher
                .fuzzy_match(&haystack, query)
                .map(|score| (index, score))
        })
        .collect();
    scored.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    scored.into_iter().map(|(index, _)| index).collect()
}

pub(crate) struct SnippetPicker {
    language_name: String,
    snippets: Vec<SnippetRecord>,
    matches: Vec<usize>,
    selected: usize,
    query: String,
    input: Entity<InputState>,
    focus_handle: FocusHandle,
    list_scroll: ScrollHandle,
    _subscription: Subscription,
}

impl EventEmitter<SnippetPickerEvent> for SnippetPicker {}

impl SnippetPicker {
    pub(crate) fn new(
        language_name: String,
        snippets: Vec<SnippetRecord>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let placeholder = dbflux_i18n::t!(
            "document.code.snippets.picker.search_placeholder",
            language = language_name
        );
        let input = cx.new(|cx| InputState::new(window, cx).placeholder(placeholder));
        let subscription = cx.subscribe_in(
            &input,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if let InputEvent::Change = event {
                    this.query = input.read(cx).value().to_string();
                    this.matches = filter_snippets(&this.snippets, &this.query);
                    this.selected = 0;
                    cx.notify();
                }
            },
        );
        input.update(cx, |state, cx| state.focus(window, cx));

        Self {
            language_name,
            matches: (0..snippets.len()).collect(),
            snippets,
            selected: 0,
            query: String::new(),
            input,
            focus_handle: cx.focus_handle(),
            list_scroll: ScrollHandle::new(),
            _subscription: subscription,
        }
    }

    fn selected_snippet(&self) -> Option<&SnippetRecord> {
        self.matches
            .get(self.selected)
            .and_then(|index| self.snippets.get(*index))
    }

    pub(crate) fn select_next(&mut self, cx: &mut Context<Self>) {
        if self.selected + 1 < self.matches.len() {
            self.select(self.selected + 1, cx);
        }
    }

    pub(crate) fn select_prev(&mut self, cx: &mut Context<Self>) {
        if let Some(previous) = self.selected.checked_sub(1) {
            self.select(previous, cx);
        }
    }

    fn select(&mut self, index: usize, cx: &mut Context<Self>) {
        self.selected = index;
        self.list_scroll.scroll_to_item(index);
        cx.notify();
    }

    /// Reports the highlighted snippet. With no match there is nothing to
    /// choose, so Enter does nothing.
    pub(crate) fn choose(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(snippet) = self.selected_snippet().cloned() {
            cx.emit(SnippetPickerEvent::Chosen(Box::new(snippet)));
        }
    }

    pub(crate) fn close(&mut self, cx: &mut Context<Self>) {
        cx.emit(SnippetPickerEvent::Closed);
    }

    fn render_row(&self, position: usize, snippet: &SnippetRecord, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let name_color = theme.foreground;
        let prefix_color = theme.muted_foreground;

        let row = ListRow::new(("snippet-picker-row", position))
            .selected(position == self.selected)
            .selection_bar(true)
            .build(cx)
            .on_click(cx.listener(move |this, _, window, cx| {
                this.select(position, cx);
                this.choose(window, cx);
            }))
            .px(Spacing::MD)
            .py(Spacing::XS)
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(Text::body_sm(snippet.name.clone()).text_color(name_color)),
            )
            .child(Text::code(snippet.prefix.clone()).text_color(prefix_color))
            .when(snippet.builtin_key.is_some(), |row| {
                row.child(Badge::new(
                    dbflux_i18n::t!("document.code.snippets.picker.builtin"),
                    BadgeTone::Neutral,
                ))
            });

        div().child(row)
    }

    fn render_results(&self, cx: &mut Context<Self>) -> AnyElement {
        let rows: Vec<Div> = self
            .matches
            .iter()
            .enumerate()
            .filter_map(|(position, index)| {
                self.snippets
                    .get(*index)
                    .map(|snippet| self.render_row(position, snippet, cx))
            })
            .collect();
        let preview = self
            .selected_snippet()
            .map(|snippet| snippet.body.clone())
            .unwrap_or_default();
        let theme = cx.theme();
        let border = ChromeColors::ghost_border(theme);

        div()
            .flex()
            .h(tokens::ui(280.0))
            .child(
                div()
                    .id("snippet-picker-list")
                    .w(tokens::ui(300.0))
                    .h_full()
                    .flex_shrink_0()
                    .overflow_y_scroll()
                    .track_scroll(&self.list_scroll)
                    .border_r_1()
                    .border_color(border)
                    .children(rows),
            )
            .child(
                div()
                    .id("snippet-picker-preview")
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .overflow_y_scroll()
                    .p(Spacing::MD)
                    .child(preview::render_preview(&preview, theme)),
            )
            .into_any_element()
    }

    fn render_empty(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let (title, hint) = if self.snippets.is_empty() {
            (
                dbflux_i18n::t!(
                    "document.code.snippets.picker.empty",
                    language = self.language_name
                ),
                Some(dbflux_i18n::t!("document.code.snippets.picker.empty_hint")),
            )
        } else {
            (
                dbflux_i18n::t!(
                    "document.code.snippets.picker.no_match",
                    query = self.query.trim()
                ),
                None,
            )
        };

        div()
            .id("snippet-picker-empty")
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .h(tokens::ui(160.0))
            .px(Spacing::LG)
            .child(Text::body(title))
            .when_some(hint, |el, hint| {
                el.child(Text::caption(hint).text_color(theme.muted_foreground))
            })
            .into_any_element()
    }

    fn render_footer(&self, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let border = ChromeColors::ghost_border(theme);
        let has_match = !self.matches.is_empty();
        let count = self.matches.len();
        let count_label = if count == 1 {
            dbflux_i18n::t!("document.code.snippets.picker.count_one")
        } else {
            dbflux_i18n::t!("document.code.snippets.picker.count_many", count = count)
        };

        div()
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_t_1()
            .border_color(border)
            .when(has_match, |el| {
                el.child(
                    Text::caption(dbflux_i18n::t!("document.code.snippets.picker.hint_insert"))
                        .text_color(muted),
                )
            })
            .child(
                Text::caption(dbflux_i18n::t!("document.code.snippets.picker.hint_close"))
                    .text_color(muted),
            )
            .when(has_match, |el| {
                el.child(
                    div()
                        .ml_auto()
                        .child(Text::caption(count_label).text_color(muted)),
                )
            })
    }
}

impl Render for SnippetPicker {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let weak = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            weak.update(cx, |this, cx| this.close(cx)).ok();
        };

        let results = if self.matches.is_empty() {
            self.render_empty(cx)
        } else {
            self.render_results(cx)
        };

        let body = div()
            .id("snippet-picker")
            .key_context(ContextId::HistoryModal.as_gpui_context())
            .flex()
            .flex_col()
            // The single-line search field takes Up and Down for itself, so
            // the picker claims them before the field sees them.
            .capture_action(cx.listener(|this, _: &MoveDown, _, cx| this.select_next(cx)))
            .capture_action(cx.listener(|this, _: &MoveUp, _, cx| this.select_prev(cx)))
            .on_action(cx.listener(|this, _: &SelectNext, _, cx| this.select_next(cx)))
            .on_action(cx.listener(|this, _: &SelectPrev, _, cx| this.select_prev(cx)))
            .on_action(cx.listener(|this, _: &Execute, window, cx| this.choose(window, cx)))
            .on_action(cx.listener(|this, _: &Cancel, _, cx| this.close(cx)))
            .child(
                div()
                    .id("snippet-picker-search")
                    .p(Spacing::MD)
                    .child(Input::new(&self.input).small().w_full()),
            )
            .child(results)
            .child(self.render_footer(cx));

        Modal::new(dbflux_i18n::t!("document.code.snippets.picker.title"))
            .id("snippet-picker-modal")
            .icon(AppIcon::Code)
            .width(tokens::ui(720.0))
            .top_offset(px(80.0))
            .focus_handle(&self.focus_handle)
            .on_close(close)
            .child(body.into_any_element())
    }
}
