//! Snippets in the completion menu, against the pure item builder and a real
//! `CodeDocument` editor (typing, the menu, Enter).

use super::{SnippetOffers, SnippetWarnings, snippet_completion_items};
use crate::code::CodeDocument;
use crate::keyboard_test_support::init_keyboard_runtime;
use crate::snippet_picker::snippet_language_id;
use dbflux_core::QueryLanguage;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_storage::{SnippetDraft, SnippetRecord};
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use gpui_component::Root;
use lsp_types::CompletionItemKind;
use std::cell::RefCell;
use std::rc::Rc;

fn record(prefix: &str, name: &str, body: &str) -> SnippetRecord {
    SnippetRecord {
        id: format!("id-{prefix}"),
        name: name.to_string(),
        prefix: prefix.to_string(),
        language: "sql".to_string(),
        body: body.to_string(),
        builtin_key: None,
        created_at: 0,
        updated_at: 0,
    }
}

fn items_for(
    snippets: &[SnippetRecord],
    source: &str,
) -> (Vec<lsp_types::CompletionItem>, SnippetOffers) {
    let offers = SnippetOffers::default();
    let items = snippet_completion_items(
        snippets,
        source,
        source.len(),
        &offers,
        &SnippetWarnings::default(),
    );
    (items, offers)
}

#[test]
fn language_ids_are_stable_lowercase_strings() {
    assert_eq!(snippet_language_id(&QueryLanguage::Sql), "sql");
    assert_eq!(snippet_language_id(&QueryLanguage::MongoQuery), "mongo");
    assert_eq!(snippet_language_id(&QueryLanguage::RedisCommands), "redis");
    assert_eq!(snippet_language_id(&QueryLanguage::Lua), "lua");
    assert_eq!(
        snippet_language_id(&QueryLanguage::Custom(" Gremlin ".to_string())),
        "gremlin"
    );
}

#[test]
fn a_snippet_is_offered_under_its_prefix_with_the_snippet_kind() {
    let snippets = [
        record("sel", "Select rows", "SELECT ${1:*}$0"),
        record("upd", "Update rows", "UPDATE ${1:t}$0"),
    ];
    let (items, offers) = items_for(&snippets, "SELECT 1;\nse");

    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item.label, "sel");
    assert_eq!(item.detail.as_deref(), Some("Select rows"));
    assert_eq!(item.kind, Some(CompletionItemKind::SNIPPET));
    assert!(!offers.is_empty());

    // The item writes the expanded body over the typed prefix, never the raw
    // `${1:*}` placeholder syntax.
    let Some(lsp_types::CompletionTextEdit::Edit(edit)) = &item.text_edit else {
        panic!("a snippet item carries a text edit");
    };
    assert_eq!(edit.new_text, "SELECT *");
    assert_eq!(edit.range.start.line, 1);
    assert_eq!(edit.range.start.character, 0);
    assert_eq!(edit.range.end.character, 2);
}

#[test]
fn prefix_matching_ignores_case_and_needs_a_typed_word() {
    let snippets = [record("sel", "Select rows", "SELECT $0")];

    assert_eq!(items_for(&snippets, "SE").0.len(), 1);
    assert!(items_for(&snippets, "x").0.is_empty());
    assert!(
        items_for(&snippets, "SELECT * FROM ").0.is_empty(),
        "no word typed, no snippets after FROM "
    );
    assert!(
        items_for(&snippets, "orders.sel").0.is_empty(),
        "a member access is not a snippet position"
    );
}

#[test]
fn a_snippet_whose_body_does_not_parse_is_left_out() {
    let snippets = [
        record("bad", "Broken", "${1:${2:nested}}"),
        record("bar", "Fine", "bar $0"),
    ];
    let (items, offers) = items_for(&snippets, "ba");

    assert_eq!(
        items
            .iter()
            .map(|item| item.label.as_str())
            .collect::<Vec<_>>(),
        ["bar"]
    );
    assert!(!offers.is_empty());
}

#[test]
fn only_the_offered_expansion_at_the_caret_counts_as_a_commit() {
    let snippets = [record("sel", "Select rows", "SELECT ${1:*}$0")];
    let (_, offers) = items_for(&snippets, "se");

    assert!(
        offers.take_committed("se", 2).is_none(),
        "nothing expanded yet"
    );
    assert!(offers.take_committed("SELECT *x", 9).is_none());
    let (parsed, range) = offers
        .take_committed("SELECT *", 8)
        .expect("the expansion ends at the caret");
    assert_eq!(parsed.text(), "SELECT *");
    assert_eq!(range, 0..8);
    assert!(offers.is_empty(), "a commit spends the offers");
}

struct Fixture<'a> {
    document: Entity<CodeDocument>,
    app_state: Entity<AppStateEntity>,
    window: &'a mut VisualTestContext,
}

fn open(cx: &mut TestAppContext, language: QueryLanguage) -> Fixture<'_> {
    init_keyboard_runtime(cx);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let storage = StorageRuntime::in_memory().expect("isolated storage runtime");
            AppStateEntity::new_with_storage_runtime(storage).expect("test storage setup")
        })
    });

    let slot: Rc<RefCell<Option<Entity<CodeDocument>>>> = Rc::new(RefCell::new(None));
    let writer = slot.clone();
    let state = app_state.clone();
    let (_, window) = cx.add_window_view(|window, cx| {
        let document =
            cx.new(|cx| CodeDocument::new_with_language(state, None, language, window, cx));
        writer.replace(Some(document.clone()));
        Root::new(document, window, cx)
    });
    let document = slot.borrow().clone().expect("document");
    window.update(|window, cx| document.update(cx, |document, cx| document.focus(window, cx)));
    window.run_until_parked();

    Fixture {
        document,
        app_state,
        window,
    }
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

    fn text(&mut self) -> String {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .value()
                .to_string()
        })
    }

    fn menu(&mut self) -> Vec<lsp_types::CompletionItem> {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            let input = document.read(cx).editor.input_state.read(cx);
            let menu = input.completion_menu_state();
            if menu.open {
                menu.items.clone()
            } else {
                Vec::new()
            }
        })
    }

    fn session_active(&mut self) -> bool {
        let document = self.document.clone();
        self.window
            .update(|_, cx| document.read(cx).snippet_session_active())
    }

    fn selection(&mut self) -> std::ops::Range<usize> {
        let document = self.document.clone();
        self.window.update(|_, cx| {
            document
                .read(cx)
                .editor
                .input_state
                .read(cx)
                .selected_range()
        })
    }

    fn add_snippet(&mut self, prefix: &str, language: &str, body: &str) {
        self.app_state.update(self.window, |state, _| {
            state
                .snippets
                .create(&SnippetDraft {
                    name: format!("{prefix} snippet"),
                    prefix: prefix.to_string(),
                    language: language.to_string(),
                    body: body.to_string(),
                })
                .expect("snippet stored");
        });
    }
}

#[gpui::test]
fn sql_completion_lists_the_snippet_with_its_kind_word(cx: &mut TestAppContext) {
    let mut editor = open(cx, QueryLanguage::Sql);
    editor.type_text("sel");

    let menu = editor.menu();
    let snippet = menu
        .iter()
        .find(|item| item.kind == Some(CompletionItemKind::SNIPPET))
        .unwrap_or_else(|| panic!("the sel snippet is offered: {menu:?}"));
    assert_eq!(snippet.label, "sel");
    assert_eq!(snippet.detail.as_deref(), Some("Select rows"));
    assert_eq!(
        snippet
            .label_details
            .as_ref()
            .and_then(|details| details.description.clone()),
        Some(dbflux_i18n::t!("document.code.completion_kind.snippet"))
    );
}

#[gpui::test]
fn another_language_does_not_list_sql_snippets(cx: &mut TestAppContext) {
    let mut editor = open(cx, QueryLanguage::Lua);
    editor.type_text("sel");

    assert!(
        editor
            .menu()
            .iter()
            .all(|item| item.kind != Some(CompletionItemKind::SNIPPET)),
        "{:?}",
        editor.menu()
    );

    editor.add_snippet("lp", "lua", "print(${1:value})$0");
    editor.type_text(" lp");
    assert!(
        editor
            .menu()
            .iter()
            .any(|item| item.kind == Some(CompletionItemKind::SNIPPET) && item.label == "lp"),
        "a snippet of the editor's own language shows up: {:?}",
        editor.menu()
    );
}

#[gpui::test]
fn committing_a_snippet_inserts_the_expansion_and_starts_the_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, QueryLanguage::Sql);
    editor.type_text("sel");
    assert!(
        !editor.session_active(),
        "offering a snippet starts nothing"
    );

    editor.keys("enter");

    assert_eq!(
        editor.text().trim_end(),
        "SELECT *\nFROM table\nWHERE condition;"
    );
    assert!(!editor.text().contains("${"), "never the raw body");
    assert!(editor.session_active());
    let selection = editor.selection();
    assert_eq!(
        &editor.text()[selection],
        "table",
        "the first field is selected"
    );

    editor.keys("tab");
    assert_eq!(&editor.text()[editor.selection()], "*");
}

#[gpui::test]
fn the_tab_preview_does_not_start_a_session(cx: &mut TestAppContext) {
    let mut editor = open(cx, QueryLanguage::Sql);
    editor.add_snippet("selz", "sql", "SELECT ${1:a}$0");
    editor.type_text("selz");
    assert!(!editor.menu().is_empty());

    editor.keys("tab");

    assert!(
        !editor.session_active(),
        "previewing with Tab only writes the item"
    );
}

#[gpui::test]
fn an_unparseable_stored_body_is_not_offered(cx: &mut TestAppContext) {
    let mut editor = open(cx, QueryLanguage::Sql);
    editor.add_snippet("brk", "sql", "${1:${2:nested}}");
    editor.type_text("brk");

    assert!(
        editor
            .menu()
            .iter()
            .all(|item| item.kind != Some(CompletionItemKind::SNIPPET)),
        "{:?}",
        editor.menu()
    );
}

#[gpui::test]
fn a_keyword_completion_still_inserts_plain_text(cx: &mut TestAppContext) {
    let mut editor = open(cx, QueryLanguage::Sql);
    editor.type_text("selec");

    let menu = editor.menu();
    assert!(
        menu.iter()
            .any(|item| item.kind == Some(CompletionItemKind::KEYWORD) && item.label == "SELECT"),
        "{menu:?}"
    );
    assert!(
        menu.iter()
            .all(|item| item.kind != Some(CompletionItemKind::SNIPPET)),
        "sel does not start with selec"
    );

    editor.keys("enter");

    assert_eq!(editor.text().trim_end(), "SELECT");
    assert!(!editor.session_active());
}
