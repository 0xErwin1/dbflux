// Explicit imports: a parent glob would make `#[test]` resolve to
// `gpui::test`, whose expansion recurses without bound.
use super::SnippetsSection;
use dbflux_storage::SnippetRecord;
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{
    AppContext as _, Entity, ParentElement as _, TestAppContext, VisualTestContext, Window,
};
use std::cell::RefCell;
use std::rc::Rc;

struct Harness {
    app_state: Entity<AppStateEntity>,
    section: Entity<SnippetsSection>,
}

struct SectionView {
    section: Entity<SnippetsSection>,
}

impl gpui::Render for SectionView {
    fn render(
        &mut self,
        _window: &mut Window,
        _cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        gpui::div().child(self.section.clone())
    }
}

fn open(cx: &mut TestAppContext) -> (Harness, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    let slot = Rc::new(RefCell::new(None));
    let app_for_view = app_state.clone();
    let slot_for_view = slot.clone();
    let (_view, window) = cx.add_window_view(move |window, cx| {
        let section = cx.new(|cx| SnippetsSection::new(app_for_view.clone(), window, cx));
        slot_for_view.borrow_mut().replace(section.clone());
        SectionView { section }
    });
    window.run_until_parked();

    let section = slot.borrow().clone().expect("section built");
    (Harness { app_state, section }, window)
}

fn stored(harness: &Harness, window: &mut VisualTestContext) -> Vec<SnippetRecord> {
    window.update(|_, cx| {
        harness
            .app_state
            .update(cx, |state, _| state.snippets.all().expect("list snippets"))
    })
}

/// Types a new snippet into the form, as the user would, and presses Save.
fn save_new(
    harness: &Harness,
    window: &mut VisualTestContext,
    name: &str,
    prefix: &str,
    body: &str,
) {
    window.update(|window, cx| {
        harness.section.update(cx, |section, cx| {
            section.begin_new_snippet(window, cx);
            section
                .input_name
                .update(cx, |input, cx| input.set_value(name, window, cx));
            section
                .input_prefix
                .update(cx, |input, cx| input.set_value(prefix, window, cx));
            section
                .input_body
                .update(cx, |input, cx| input.set_value(body, window, cx));
            section.save_snippet(window, cx);
        });
    });
    window.run_until_parked();
}

#[gpui::test]
fn the_list_shows_the_seeded_built_in_snippets(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    window.update(|_, cx| {
        let section = harness.section.read(cx);
        let mut prefixes: Vec<&str> = section
            .snippets
            .iter()
            .map(|snippet| snippet.prefix.as_str())
            .collect();
        prefixes.sort_unstable();

        assert_eq!(prefixes, ["cnt", "cte", "del", "ins", "sel", "upd"]);
        assert!(
            section
                .snippets
                .iter()
                .all(|snippet| snippet.builtin_key.is_some() && snippet.language == "sql")
        );
        assert!(
            section.editing_id.is_some(),
            "the first snippet opens in the form"
        );
    });
}

#[gpui::test]
fn saving_a_new_valid_snippet_creates_it(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);

    save_new(
        &harness,
        window,
        "Slow queries",
        "slow",
        "SELECT * FROM ${1:table};$0",
    );

    let snippets = stored(&harness, window);
    let created = snippets
        .iter()
        .find(|snippet| snippet.prefix == "slow")
        .expect("the new snippet is stored");
    assert_eq!(created.name, "Slow queries");
    assert_eq!(created.language, "sql");
    assert_eq!(created.body, "SELECT * FROM ${1:table};$0");
    assert_eq!(created.builtin_key, None);
    assert_eq!(snippets.len(), 7);

    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert_eq!(section.editing_id.as_deref(), Some(created.id.as_str()));
        assert!(
            section
                .snippets
                .iter()
                .any(|snippet| snippet.prefix == "slow"),
            "the list shows the new snippet"
        );
        assert!(section.errors.is_empty());
    });
}

#[gpui::test]
fn each_validation_error_is_shown_inline_and_stores_nothing(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let before = stored(&harness, window);

    let cases: [(&str, &str, &str, &str); 5] = [
        ("", "x1", "SELECT 1", "name"),
        ("Name", "", "SELECT 1", "prefix"),
        ("Name", "x1", "", "body"),
        ("Name", "sel", "SELECT 1", "prefix"),
        ("Name", "x1", "SELECT *\nFROM ${1:table", "body"),
    ];
    let expected = [
        "Name is required",
        "Prefix is required",
        "Body is required",
        "Another SQL snippet already uses the prefix sel",
        "Unclosed placeholder at line 2, column 6: ${1:table",
    ];

    for ((name, prefix, body, field), message) in cases.into_iter().zip(expected) {
        save_new(&harness, window, name, prefix, body);

        window.update(|_, cx| {
            let errors = &harness.section.read(cx).errors;
            let shown = [
                ("name", errors.name.as_deref()),
                ("prefix", errors.prefix.as_deref()),
                ("body", errors.body.as_deref()),
            ];

            for (shown_field, text) in shown {
                if shown_field == field {
                    assert_eq!(text, Some(message), "{field} error for {name:?}/{prefix:?}");
                } else {
                    assert_eq!(text, None, "{shown_field} stays clean for {field} error");
                }
            }
        });
        assert_eq!(
            stored(&harness, window),
            before,
            "{message}: nothing stored"
        );
    }
}

#[gpui::test]
fn saving_an_existing_snippet_updates_it(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let before = stored(&harness, window);
    let select = before
        .iter()
        .find(|snippet| snippet.prefix == "sel")
        .expect("seeded sel")
        .clone();

    window.update(|window, cx| {
        harness.section.update(cx, |section, cx| {
            section.select_snippet(&select.id, window, cx);
            section
                .input_name
                .update(cx, |input, cx| input.set_value("Pick rows", window, cx));
            section.save_snippet(window, cx);
        });
    });
    window.run_until_parked();

    let after = stored(&harness, window);
    assert_eq!(after.len(), before.len(), "an update adds no row");
    let updated = after
        .iter()
        .find(|snippet| snippet.id == select.id)
        .expect("same row");
    assert_eq!(updated.name, "Pick rows");
    assert_eq!(updated.prefix, "sel");
    assert_eq!(updated.body, select.body);
    assert_eq!(updated.builtin_key, select.builtin_key);
}

#[gpui::test]
fn deleting_a_snippet_removes_it_only_after_the_confirmation(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let before = stored(&harness, window);
    let target = before.first().expect("a seeded snippet").clone();

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.request_delete_snippet(target.id.clone(), cx)
        });
    });
    window.run_until_parked();
    assert_eq!(stored(&harness, window), before, "asking does not delete");

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .delete_confirm
                .update(cx, |modal, cx| modal.cancel(cx));
        });
    });
    window.run_until_parked();
    assert_eq!(stored(&harness, window), before, "cancelling keeps it");

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.request_delete_snippet(target.id.clone(), cx)
        });
    });
    window.run_until_parked();
    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section
                .delete_confirm
                .update(cx, |modal, cx| modal.confirm(cx));
        });
    });
    window.run_until_parked();

    let after = stored(&harness, window);
    assert_eq!(after.len(), before.len() - 1);
    assert!(after.iter().all(|snippet| snippet.id != target.id));
    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert!(
            section
                .snippets
                .iter()
                .all(|snippet| snippet.id != target.id)
        );
        assert_eq!(section.pending_delete_id, None);
    });
}

#[gpui::test]
fn cancelling_a_new_snippet_returns_to_the_one_that_was_open(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let before = stored(&harness, window);
    let open_id = window.update(|_, cx| harness.section.read(cx).editing_id.clone());

    window.update(|window, cx| {
        harness.section.update(cx, |section, cx| {
            section.begin_new_snippet(window, cx);
            assert_eq!(section.editing_id, None, "the form is blank");
            section
                .input_name
                .update(cx, |input, cx| input.set_value("Draft", window, cx));
            section.cancel_new_snippet(window, cx);
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let section = harness.section.read(cx);
        assert_eq!(section.editing_id, open_id);
        assert!(!section.new_form_open);
        assert!(!section.has_unsaved_changes(cx));
    });
    assert_eq!(
        stored(&harness, window),
        before,
        "cancelling stores nothing"
    );
}

#[gpui::test]
fn the_confirmation_names_the_snippet(cx: &mut TestAppContext) {
    let (harness, window) = open(cx);
    let target = stored(&harness, window)
        .into_iter()
        .find(|snippet| snippet.prefix == "cnt")
        .expect("seeded cnt");

    window.update(|_, cx| {
        harness.section.update(cx, |section, cx| {
            section.request_delete_snippet(target.id.clone(), cx)
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let confirm = harness.section.read(cx).delete_confirm.read(cx);
        assert!(confirm.is_visible());
        assert_eq!(
            confirm.current_body(),
            Some("Are you sure you want to delete snippet \"Count rows\"?")
        );
    });
}

const SNIPPETS_CATALOG_KEYS: &[&str] = &[
    "settings.nav.snippets",
    "settings.snippets.header.title",
    "settings.snippets.header.subtitle",
    "settings.snippets.list.new",
    "settings.snippets.list.empty",
    "settings.snippets.list.builtin",
    "settings.snippets.empty.title",
    "settings.snippets.empty.hint",
    "settings.snippets.form.group.snippet",
    "settings.snippets.form.group.body",
    "settings.snippets.form.name",
    "settings.snippets.form.name_placeholder",
    "settings.snippets.form.prefix",
    "settings.snippets.form.prefix_placeholder",
    "settings.snippets.form.prefix_hint",
    "settings.snippets.form.language",
    "settings.snippets.form.language_hint",
    "settings.snippets.form.body",
    "settings.snippets.form.body_hint",
    "settings.snippets.language.sql",
    "settings.snippets.action.save",
    "settings.snippets.action.update",
    "settings.snippets.action.delete",
    "settings.snippets.action.cancel",
    "settings.snippets.delete.title",
    "settings.snippets.delete.message",
    "settings.snippets.validation.name_required",
    "settings.snippets.validation.prefix_required",
    "settings.snippets.validation.body_required",
    "settings.snippets.validation.duplicate_prefix",
    "settings.snippets.validation.parse.unclosed_placeholder",
    "settings.snippets.validation.parse.nested_placeholder",
    "settings.snippets.validation.parse.unsupported_choice",
    "settings.snippets.validation.parse.unsupported_variable",
    "settings.snippets.validation.parse.invalid_placeholder",
    "settings.snippets.toast.saved",
    "settings.snippets.toast.deleted",
    "settings.snippets.error.save",
    "settings.snippets.error.delete",
    "settings.snippets.error.load",
];

#[test]
fn snippets_keys_resolve_in_every_shipped_locale() {
    for locale in ["en", "es", "ko", "zh_Hans", "pt_BR"] {
        for key in SNIPPETS_CATALOG_KEYS {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(!value.is_empty(), "{key} is empty for {locale}");
            assert_ne!(value, *key, "{key} did not resolve for {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} is missing from the {locale} catalog"
            );
        }
    }
}

#[test]
fn snippets_strings_are_translated_not_copied() {
    let english = dbflux_i18n::t!("settings.snippets.validation.name_required", locale = "en");

    assert_eq!(english, "Name is required");
    for locale in ["es", "ko", "zh_Hans", "pt_BR"] {
        assert_ne!(
            dbflux_i18n::t!(
                "settings.snippets.validation.name_required",
                locale = locale
            ),
            english,
            "{locale} must not reuse the English text"
        );
    }
}
