//! The picker against a real window: the filter, the keys and the empty
//! states, plus the keyboard coverage of its clickable rows.

use super::{SnippetPicker, SnippetPickerEvent, filter_snippets};
use crate::keyboard_coverage::SNIPPET_PICKER;
use crate::keyboard_test_support::init_keyboard_runtime;
use dbflux_storage::SnippetRecord;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture, MODAL_CHROME};
use gpui::{
    AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
    TestAppContext, VisualTestContext, Window, div,
};
use gpui_component::Root;
use std::cell::RefCell;
use std::rc::Rc;

fn record(prefix: &str, name: &str, builtin: bool) -> SnippetRecord {
    SnippetRecord {
        id: format!("id-{prefix}"),
        name: name.to_string(),
        prefix: prefix.to_string(),
        language: "sql".to_string(),
        body: format!("{prefix} ${{1:field}}$0"),
        builtin_key: builtin.then(|| format!("sql.{prefix}")),
        created_at: 0,
        updated_at: 0,
    }
}

fn snippets() -> Vec<SnippetRecord> {
    vec![
        record("cnt", "Count rows", true),
        record("sel", "Select rows", true),
        record("slow", "Slow queries", false),
    ]
}

#[test]
fn an_empty_query_keeps_every_snippet_in_order() {
    assert_eq!(filter_snippets(&snippets(), ""), [0, 1, 2]);
    assert_eq!(filter_snippets(&snippets(), "   "), [0, 1, 2]);
}

#[test]
fn the_query_matches_the_name_or_the_prefix() {
    assert_eq!(filter_snippets(&snippets(), "count"), [0]);
    assert_eq!(filter_snippets(&snippets(), "slow").first(), Some(&2));
    assert!(filter_snippets(&snippets(), "upsert").is_empty());
    assert_eq!(
        filter_snippets(&snippets(), "sel").first(),
        Some(&1),
        "the prefix is the best match"
    );
}

struct Host {
    picker: Entity<SnippetPicker>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().child(self.picker.clone())
    }
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Chosen(String),
    Closed,
}

struct Fixture<'a> {
    window: &'a mut VisualTestContext,
    outcomes: Rc<RefCell<Vec<Outcome>>>,
}

fn open(cx: &mut TestAppContext, snippets: Vec<SnippetRecord>) -> Fixture<'_> {
    init_keyboard_runtime(cx);

    let outcomes: Rc<RefCell<Vec<Outcome>>> = Rc::default();
    let log = outcomes.clone();
    let (_, window) = cx.add_window_view(move |window, cx| {
        let picker = cx.new(|cx| SnippetPicker::new("SQL".to_string(), snippets, window, cx));
        cx.subscribe(&picker, move |_, _, event: &SnippetPickerEvent, _| {
            log.borrow_mut().push(match event {
                SnippetPickerEvent::Chosen(snippet) => Outcome::Chosen(snippet.prefix.clone()),
                SnippetPickerEvent::Closed => Outcome::Closed,
            });
        })
        .detach();
        Root::new(cx.new(|_| Host { picker }), window, cx)
    });
    window.run_until_parked();

    Fixture { window, outcomes }
}

impl Fixture<'_> {
    fn type_text(&mut self, input: &str) {
        self.window.simulate_input(input);
        self.window.run_until_parked();
    }

    fn keys(&mut self, keystrokes: &str) {
        self.window.simulate_keystrokes(keystrokes);
        self.window.run_until_parked();
    }

    fn outcomes(&self) -> Vec<Outcome> {
        std::mem::take(&mut *self.outcomes.borrow_mut())
    }

    fn visible_rows(&mut self) -> usize {
        let capture = FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);
        frame
            .nodes()
            .filter(|(_, node)| node.id().starts_with("snippet-picker-row"))
            .count()
    }

    fn has_element(&mut self, id: &str) -> bool {
        let capture = FrameCapture::observe(self.window);
        let frame = capture.frame(self.window);
        frame.nodes().any(|(_, node)| node.id() == id)
    }
}

#[gpui::test]
fn the_picker_lists_the_snippets_and_filters_as_you_type(cx: &mut TestAppContext) {
    let mut picker = open(cx, snippets());
    assert_eq!(picker.visible_rows(), 3);

    picker.type_text("count");
    assert_eq!(picker.visible_rows(), 1);
}

#[gpui::test]
fn enter_chooses_the_highlighted_snippet(cx: &mut TestAppContext) {
    let mut picker = open(cx, snippets());

    picker.keys("enter");
    assert_eq!(picker.outcomes(), [Outcome::Chosen("cnt".to_string())]);

    picker.keys("down");
    picker.keys("enter");
    assert_eq!(picker.outcomes(), [Outcome::Chosen("sel".to_string())]);
}

#[gpui::test]
fn typing_then_enter_chooses_the_best_match(cx: &mut TestAppContext) {
    let mut picker = open(cx, snippets());

    picker.type_text("slow");
    picker.keys("enter");

    assert_eq!(picker.outcomes(), [Outcome::Chosen("slow".to_string())]);
}

#[gpui::test]
fn escape_closes_without_choosing(cx: &mut TestAppContext) {
    let mut picker = open(cx, snippets());

    picker.keys("escape");

    assert_eq!(picker.outcomes(), [Outcome::Closed]);
}

#[gpui::test]
fn enter_with_no_match_does_nothing_and_shows_the_query(cx: &mut TestAppContext) {
    let mut picker = open(cx, snippets());

    picker.type_text("upsert");
    picker.keys("enter");

    assert!(picker.outcomes().is_empty());
    assert_eq!(picker.visible_rows(), 0);
    assert!(picker.has_element("snippet-picker-empty"));
    let message = dbflux_i18n::t!("document.code.snippets.picker.no_match", query = "upsert");
    assert!(message.contains("upsert"), "{message}");
}

#[gpui::test]
fn a_language_without_snippets_shows_the_empty_state(cx: &mut TestAppContext) {
    let mut picker = open(cx, Vec::new());

    assert_eq!(picker.visible_rows(), 0);
    assert!(picker.has_element("snippet-picker-empty"));
    picker.keys("enter");
    assert!(picker.outcomes().is_empty());
    picker.keys("escape");
    assert_eq!(picker.outcomes(), [Outcome::Closed]);
}

#[gpui::test]
fn the_rows_have_a_keyboard_path(cx: &mut TestAppContext) {
    let picker = open(cx, snippets());
    let capture = FrameCapture::observe(picker.window);
    let frame = capture.frame(picker.window);

    let checked = Coverage::new(SNIPPET_PICKER)
        .with_surface(MODAL_CHROME)
        .assert_covered(&frame);

    assert!(
        checked
            .iter()
            .any(|id| id.starts_with("snippet-picker-row")),
        "{checked:?}"
    );
}
