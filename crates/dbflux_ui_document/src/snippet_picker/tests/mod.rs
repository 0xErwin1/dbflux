//! The picker against a real window: the filter, the keys and the empty
//! states, plus the keyboard coverage of its clickable rows.

mod dispatch;

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

mod preview {
    use super::super::preview::{ChipPalette, PreviewRun, PreviewTone, preview_runs};
    use gpui::TestAppContext;
    use gpui_component::ActiveTheme as _;

    const SEL: &str = "SELECT ${2:*}\nFROM ${1:table}\nWHERE ${3:condition};$0";

    fn run(range: std::ops::Range<usize>, tone: PreviewTone, chip: Option<usize>) -> PreviewRun {
        PreviewRun { range, tone, chip }
    }

    #[test]
    fn the_sel_preview_marks_every_placeholder_as_a_chip() {
        let runs = preview_runs(SEL);
        let text = |range: &std::ops::Range<usize>| &SEL[range.clone()];

        let chips: Vec<(&str, PreviewTone, Option<usize>)> = runs
            .iter()
            .filter(|run| run.chip.is_some())
            .map(|run| (text(&run.range), run.tone, run.chip))
            .collect();
        assert_eq!(
            chips,
            [
                ("${2:", PreviewTone::Delimiter, Some(0)),
                ("*", PreviewTone::Default, Some(0)),
                ("}", PreviewTone::Delimiter, Some(0)),
                ("${1:", PreviewTone::Delimiter, Some(1)),
                ("table", PreviewTone::Default, Some(1)),
                ("}", PreviewTone::Delimiter, Some(1)),
                ("${3:", PreviewTone::Delimiter, Some(2)),
                ("condition", PreviewTone::Default, Some(2)),
                ("}", PreviewTone::Delimiter, Some(2)),
                ("$0", PreviewTone::Delimiter, Some(3)),
            ]
        );
        assert_eq!(
            runs.first(),
            Some(&run(0..7, PreviewTone::Plain, None)),
            "the keyword text before the first chip stays plain"
        );
    }

    #[test]
    fn the_runs_cover_the_body_without_gaps() {
        let runs = preview_runs(SEL);
        let mut cursor = 0;
        for run in &runs {
            assert_eq!(run.range.start, cursor);
            cursor = run.range.end;
        }
        assert_eq!(cursor, SEL.len());
    }

    #[test]
    fn a_body_with_an_unclosed_placeholder_keeps_it_as_plain_text() {
        let runs = preview_runs("a ${1:b");
        assert_eq!(runs, [run(0..7, PreviewTone::Plain, None)]);
    }

    #[gpui::test]
    fn the_chip_colors_are_neutral_theme_colors(cx: &mut TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        cx.update(|cx| {
            let purple = gpui::hsla(0.8, 0.6, 0.6, 1.0);
            let theme = gpui_component::Theme::global_mut(cx);
            theme.primary = purple;
            theme.accent = purple;
            let theme = cx.theme();
            let palette = ChipPalette::from_theme(theme);

            assert_eq!(palette.background, theme.secondary);
            assert_eq!(palette.border, theme.border);
            assert_eq!(
                palette.color(PreviewTone::Delimiter),
                theme.muted_foreground
            );
            assert_eq!(palette.color(PreviewTone::Default), theme.foreground);
            assert_eq!(palette.color(PreviewTone::Plain), theme.foreground);

            for color in [
                palette.background,
                palette.border,
                palette.delimiter,
                palette.text,
            ] {
                assert_ne!(color, purple, "chips never use the primary or accent color");
            }
        });
    }
}
