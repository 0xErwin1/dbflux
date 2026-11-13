use super::*;
use gpui_component::highlighter::{HighlightTheme, SyntaxHighlighter};

/// Capture names a probe theme gives a distinct color each, so a span's
/// color names the capture that styled it.
const PROBE_CAPTURES: &[&str] = &[
    "attribute",
    "boolean",
    "comment",
    "constant",
    "function",
    "keyword",
    "number",
    "operator",
    "property",
    "punctuation",
    "punctuation.bracket",
    "punctuation.delimiter",
    "string",
    "string.special",
    "tag",
    "type",
    "variable",
];

fn probe_theme() -> HighlightTheme {
    let mut syntax = serde_json::Map::new();
    for (index, name) in PROBE_CAPTURES.iter().enumerate() {
        syntax.insert(
            name.to_string(),
            serde_json::json!({ "color": format!("#0000{:02x}", index + 1) }),
        );
    }
    serde_json::from_value(serde_json::json!({
        "name": "probe",
        "appearance": "dark",
        "style": { "syntax": syntax },
    }))
    .expect("probe theme")
}

/// The capture each highlighted token of `source` resolves to.
fn captures(language: &str, source: &str) -> Vec<(String, String)> {
    let theme = probe_theme();
    let rope = gpui_component::Rope::from(source);
    let mut highlighter = SyntaxHighlighter::new(language);
    highlighter.update(None, &rope, None);

    highlighter
        .styles(&(0..source.len()), &theme)
        .into_iter()
        .filter_map(|(range, style)| {
            let color = style.color?;
            let name = PROBE_CAPTURES.iter().find(|name| {
                theme.style.syntax.style(name).and_then(|style| style.color) == Some(color)
            })?;
            let text = source[range].trim().to_string();
            (!text.is_empty()).then(|| (text, name.to_string()))
        })
        .collect()
}

fn capture_of<'a>(captures: &'a [(String, String)], text: &str) -> Option<&'a str> {
    captures
        .iter()
        .find(|(token, _)| token == text)
        .map(|(_, capture)| capture.as_str())
}

#[test]
fn a_registered_grammar_highlights_under_its_name_and_its_aliases() {
    let grammar = Grammar::new(
        "probe-keys",
        tree_sitter_json::LANGUAGE,
        "(pair key: (_) @property)",
    );
    assert!(!is_registered("probe-keys"));

    grammar.register();
    grammar.register_as("probe-keys-alias");

    assert!(is_registered("probe-keys"));
    assert!(is_registered("probe-keys-alias"));
    for name in ["probe-keys", "probe-keys-alias"] {
        let captures = captures(name, r#"{"key": 1}"#);
        assert_eq!(
            capture_of(&captures, r#""key""#),
            Some("property"),
            "{name}"
        );
        assert_eq!(capture_of(&captures, "1"), None, "{name}: {captures:?}");
    }
}

#[test]
fn an_unknown_language_is_not_registered() {
    assert!(!is_registered("probe-never-registered"));
}

#[test]
fn bundled_grammars_register_under_their_names() {
    register_bundled();

    for name in ["json", "javascript", "bash", "python", "xml", "html"] {
        assert!(is_registered(name), "{name}");
    }
}

#[test]
fn bundled_json_paints_keys_as_properties() {
    register_bundled();
    let captures = captures("json", r#"{"key": "value", "n": 1.5, "ok": true}"#);

    assert_eq!(
        capture_of(&captures, r#""key""#),
        Some("property"),
        "{captures:?}"
    );
    assert_eq!(capture_of(&captures, r#""value""#), Some("string"));
    assert_eq!(capture_of(&captures, "1.5"), Some("number"));
    assert_eq!(capture_of(&captures, "true"), Some("boolean"));
    assert_eq!(capture_of(&captures, "{"), Some("punctuation"));
}

#[test]
fn bundled_scripts_highlight_their_keywords() {
    register_bundled();

    let javascript = captures("javascript", "const o = { key: 1 };");
    assert_eq!(
        capture_of(&javascript, "const"),
        Some("keyword"),
        "{javascript:?}"
    );
    assert_eq!(capture_of(&javascript, "key"), Some("property"));

    let bash = captures("bash", "if true; then echo \"hi\"; fi");
    assert_eq!(capture_of(&bash, "if"), Some("keyword"), "{bash:?}");
    assert_eq!(capture_of(&bash, "\"hi\""), Some("string"));

    let python = captures("python", "def f():\n    return 1\n");
    assert_eq!(capture_of(&python, "def"), Some("keyword"), "{python:?}");
    assert_eq!(capture_of(&python, "f"), Some("function"));
}

#[test]
fn bundled_markup_paints_tags_and_attributes() {
    register_bundled();

    let xml = captures("xml", r#"<item id="7">text</item>"#);
    assert_eq!(capture_of(&xml, "item"), Some("tag"), "{xml:?}");
    // The crate's query captures attribute names as `property`; both captures
    // take the property role.
    assert_eq!(capture_of(&xml, "id"), Some("property"), "{xml:?}");

    let html = captures("html", r#"<a href="/x">link</a>"#);
    assert_eq!(capture_of(&html, "a"), Some("tag"), "{html:?}");
    assert_eq!(capture_of(&html, "href"), Some("attribute"), "{html:?}");
}

/// The html grammar injects javascript into a script element; the injected
/// code takes the javascript grammar's captures.
#[test]
fn bundled_html_highlights_an_injected_script() {
    register_bundled();

    let html = captures("html", "<script>const x = 1;</script>");
    assert_eq!(capture_of(&html, "script"), Some("tag"), "{html:?}");
    assert_eq!(capture_of(&html, "const"), Some("keyword"), "{html:?}");
}

fn install_dark_theme(cx: &mut gpui::TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });
}

fn color_at(
    runs: &[(std::ops::Range<usize>, gpui::HighlightStyle)],
    range: std::ops::Range<usize>,
) -> Option<gpui::Hsla> {
    runs.iter()
        .find(|(run, _)| *run == range)
        .and_then(|(_, style)| style.color)
}

#[gpui::test]
fn highlight_paints_runs_in_the_current_theme(cx: &mut gpui::TestAppContext) {
    install_dark_theme(cx);
    register_bundled();
    let text = r#"{"key": 1}"#;

    let runs = cx.update(|cx| highlight("json", text, cx));
    let colors = crate::tokens::SyntaxColors::dark();

    assert_eq!(color_at(&runs, 1..6), Some(colors.property), "{runs:?}");
    assert_eq!(color_at(&runs, 8..9), Some(colors.number), "{runs:?}");
    assert!(
        runs.windows(2).all(|pair| pair[0].0.end <= pair[1].0.start),
        "runs are sorted and do not overlap: {runs:?}"
    );
    assert!(
        runs.iter()
            .all(|(_, style)| *style != gpui::HighlightStyle::default()),
        "no unstyled runs: {runs:?}"
    );
}

#[gpui::test]
fn highlight_of_an_unknown_language_is_plain(cx: &mut gpui::TestAppContext) {
    install_dark_theme(cx);

    let runs = cx.update(|cx| highlight("probe-unknown-language", "{\"key\": 1}", cx));

    assert!(runs.is_empty(), "{runs:?}");
}

#[gpui::test]
fn highlight_with_a_query_that_does_not_compile_is_plain(cx: &mut gpui::TestAppContext) {
    install_dark_theme(cx);
    Grammar::new(
        "probe-broken-query",
        tree_sitter_json::LANGUAGE,
        "(no_such_node) @keyword",
    )
    .register();

    let runs = cx.update(|cx| highlight("probe-broken-query", "{\"key\": 1}", cx));

    assert!(runs.is_empty(), "{runs:?}");
}

/// A style whose color differs for every `index`.
fn colored(index: u8) -> gpui::HighlightStyle {
    gpui::HighlightStyle {
        color: Some(gpui::Hsla {
            h: f32::from(index) / 16.0,
            s: 1.0,
            l: 0.5,
            a: 1.0,
        }),
        ..Default::default()
    }
}

#[test]
fn overlay_draws_the_top_runs_over_the_base_runs() {
    let base = vec![(0..4, colored(1)), (6..10, colored(2))];
    let top = vec![(2..7, colored(10)), (9..9, colored(11))];

    assert_eq!(
        overlay(base, top),
        vec![(0..2, colored(1)), (2..7, colored(10)), (7..10, colored(2)),]
    );
}

#[test]
fn overlay_lets_a_later_top_run_win_and_joins_equal_neighbours() {
    let top = vec![
        (0..4, colored(10)),
        (2..6, colored(11)),
        (6..8, colored(11)),
    ];

    assert_eq!(
        overlay(Vec::new(), top),
        vec![(0..2, colored(10)), (2..8, colored(11))]
    );
}

/// Re-registering a language under the same name is seen by the next call,
/// even for text highlighted before.
#[gpui::test]
fn highlight_follows_a_grammar_registered_again(cx: &mut gpui::TestAppContext) {
    install_dark_theme(cx);
    let text = r#"{"key": 1}"#;
    Grammar::new(
        "probe-again",
        tree_sitter_json::LANGUAGE,
        "(number) @number",
    )
    .register();
    let before = cx.update(|cx| highlight("probe-again", text, cx));

    Grammar::new(
        "probe-again",
        tree_sitter_json::LANGUAGE,
        "(pair key: (_) @property)",
    )
    .register();
    let after = cx.update(|cx| highlight("probe-again", text, cx));

    let colors = crate::tokens::SyntaxColors::dark();
    assert_eq!(color_at(&before, 8..9), Some(colors.number), "{before:?}");
    assert_eq!(color_at(&after, 1..6), Some(colors.property), "{after:?}");
    assert_eq!(color_at(&after, 8..9), None, "{after:?}");
}

#[gpui::test]
fn the_cache_keeps_its_texts_under_its_byte_bound(cx: &mut gpui::TestAppContext) {
    install_dark_theme(cx);
    let large = format!("[{}1]", "1, ".repeat(highlight::CACHED_BYTES / 3));
    let small = r#"{"key": 1}"#;

    cx.update(|cx| highlight("json", small, cx));
    let runs = cx.update(|cx| highlight("json", &large, cx));
    assert!(!runs.is_empty(), "a large text is still highlighted");
    assert!(
        highlight::cached_bytes() <= highlight::CACHED_BYTES,
        "the cache holds {} bytes",
        highlight::cached_bytes()
    );
    for _ in 0..12 {
        let text = format!("[{}2]", "2, ".repeat(highlight::CACHED_BYTES / 12));
        cx.update(|cx| highlight("json", &text, cx));
        assert!(highlight::cached_bytes() <= highlight::CACHED_BYTES);
    }
}
