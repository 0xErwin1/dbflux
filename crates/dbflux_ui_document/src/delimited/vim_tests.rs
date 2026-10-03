//! Vim mode in the text view of a delimited file: the raw text's editor
//! follows the Vim setting as the object editor's buffer does, Vim's
//! editing commands do nothing while the text is read-only, and Escape
//! leaves Insert mode before it hands the keyboard to the tab.

use dbflux_app::keymap::ContextId;
use dbflux_components::vim::VimMode;
use dbflux_ui_base::keyboard_coverage::FrameCapture;
use std::path::PathBuf;

use gpui::{Entity, TestAppContext, VisualTestContext};

use super::columns_tests::start_cell_edit;
use super::document::DelimitedDocument;
use super::document_tests::{CITIES, covered_ids, open_local};
use super::editing_tests::{is_dirty, local_file, press, read, save, select};
use super::tests::TestDirectory;
use super::text::TextEnd;
use super::text_view::{DelimitedView, TextMode};
use super::text_view_tests::{context, is_editable, mode, shown_rows, text, text_end, view};

/// Opens `bytes` as a local CSV file with Vim mode set to `vim`. Returns
/// the file's path, the document and its window.
fn open_with_vim<'a>(
    cx: &'a mut TestAppContext,
    directory: &TestDirectory,
    bytes: &[u8],
    vim: bool,
) -> (
    PathBuf,
    Entity<DelimitedDocument>,
    &'a mut VisualTestContext,
) {
    let path = local_file(directory, "cities.csv", bytes);

    cx.update(|cx| dbflux_components::vim::set_vim_enabled(cx, vim));

    let (document, window) = open_local(cx, path.clone());
    (path, document, window)
}

fn set_vim(window: &mut VisualTestContext, enabled: bool) {
    window.update(|_, cx| dbflux_components::vim::set_vim_enabled(cx, enabled));
    window.run_until_parked();
}

/// The byte offset of the text view's cursor.
fn cursor(document: &Entity<DelimitedDocument>, window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .expect("the text view was shown")
            .read(cx)
            .cursor()
    })
}

/// The Vim mode of the text view's editor, `None` while Vim mode is off.
fn vim_mode(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Option<VimMode> {
    window.update(|_, cx| {
        document
            .read(cx)
            .text
            .shown
            .as_ref()
            .expect("the text view was shown")
            .vim
            .mode()
    })
}

fn key_context_entries(
    document: &Entity<DelimitedDocument>,
    window: &mut VisualTestContext,
) -> Vec<(String, String)> {
    window.update(|_, cx| {
        document
            .read(cx)
            .key_context_entries(cx)
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    })
}

/// Whether an element with `id` is drawn.
fn draws(window: &mut VisualTestContext, id: &str) -> bool {
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    frame.nodes().any(|(_, node)| node.id() == id)
}

/// With Vim mode on the raw text starts in Normal mode: `j` moves instead
/// of typing, `i` inserts, the first Escape returns to Normal mode and keeps
/// the keyboard in the text, and the second hands it to the tab.
#[gpui::test]
fn vim_mode_edits_the_raw_text_and_escape_steps_out(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-insert");
    let (_path, document, window) = open_with_vim(cx, &directory, CITIES, true);
    select(&document, window, 0);

    assert!(key_context_entries(&document, window).is_empty());

    press(window, "t");
    assert_eq!(view(&document, window), DelimitedView::Text);
    assert_eq!(cursor(&document, window), "name,city\n".len());
    assert_eq!(vim_mode(&document, window), Some(VimMode::Normal));
    assert_eq!(
        key_context_entries(&document, window),
        [("vim_mode".to_string(), "normal".to_string())]
    );

    press(window, "j");
    assert_eq!(text(&document, window), "name,city\nAna,Lima\nBo,Quito\n");
    assert_eq!(cursor(&document, window), "name,city\nAna,Lima\n".len());

    press(window, "i");
    assert_eq!(vim_mode(&document, window), Some(VimMode::Insert));
    window.simulate_input("X");
    window.run_until_parked();
    assert_eq!(text(&document, window), "name,city\nAna,Lima\nXBo,Quito\n");
    assert!(is_dirty(&document, window));

    press(window, "escape");
    assert_eq!(
        context(&document, window),
        ContextId::TextInput,
        "the first Escape only leaves Insert mode"
    );
    assert_eq!(vim_mode(&document, window), Some(VimMode::Normal));

    press(window, "x");
    assert_eq!(text(&document, window), "name,city\nAna,Lima\nBo,Quito\n");

    press(window, "escape");
    assert_eq!(
        context(&document, window),
        ContextId::Results,
        "Escape in Normal mode hands the keyboard to the tab"
    );

    press(window, "enter");
    assert_eq!(context(&document, window), ContextId::TextInput);
}

/// `x` and `dd` edit the raw text; leaving the text applies the edit to the
/// table, a save writes it, and Vim still drives the text rendered again.
#[gpui::test]
fn x_and_dd_edit_the_text_which_is_applied_and_saved(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-x-dd");
    let (path, document, window) = open_with_vim(cx, &directory, CITIES, true);
    select(&document, window, 0);

    press(window, "t");
    press(window, "x");
    assert_eq!(text(&document, window), "name,city\nna,Lima\nBo,Quito\n");

    press(window, "j x k d d");
    assert_eq!(text(&document, window), "name,city\no,Quito\n");

    press(window, "escape");
    press(window, "t");

    assert_eq!(view(&document, window), DelimitedView::Table);
    assert_eq!(shown_rows(&document, window), ["o|Quito"]);

    save(&document, window);

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), b"name,city\no,Quito\n");

    press(window, "t");
    assert_eq!(text(&document, window), "name,city\no,Quito\n");

    press(window, "g g x");
    assert_eq!(text(&document, window), "ame,city\no,Quito\n");
}

/// Aligned text is read-only: Vim's motions move the cursor, its editing
/// commands and Insert mode change nothing.
#[gpui::test]
fn vim_edits_do_nothing_in_the_aligned_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-aligned");
    let (_path, document, window) = open_with_vim(cx, &directory, CITIES, true);

    press(window, "t escape shift-t enter");
    assert_eq!(mode(&document, window), TextMode::Aligned);
    assert!(!is_editable(&document, window));

    let before = text(&document, window);
    let start = cursor(&document, window);

    press(window, "j");
    assert_ne!(cursor(&document, window), start, "motions still move");

    press(window, "x d d");
    press(window, "i");
    window.simulate_input("Z");
    window.run_until_parked();

    assert_eq!(text(&document, window), before);
    assert!(!is_dirty(&document, window));
}

/// A capped raw text is read-only, so Vim's editing commands do nothing in
/// it either.
#[gpui::test]
fn vim_edits_do_nothing_in_a_capped_raw_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-capped");

    let mut bytes = b"n\n".to_vec();
    bytes.extend(std::iter::repeat_n(b"1\n".as_slice(), 50_005).flatten());
    let (path, _) = directory.file("many.csv", &bytes);

    let options = dbflux_delimited::ReaderOptions {
        page_size: std::num::NonZeroUsize::new(100_000).expect("a non-zero page size"),
        window_size: std::num::NonZeroU64::new(1024 * 1024).expect("a non-zero window"),
    };

    cx.update(|cx| dbflux_components::vim::set_vim_enabled(cx, true));
    let (document, window) = super::document_tests::open(cx, move |cx| {
        DelimitedDocument::open_local_with(path, options, cx)
    });

    press(window, "t");
    assert_eq!(mode(&document, window), TextMode::Raw);
    assert!(matches!(
        text_end(&document, window),
        TextEnd::Capped { .. }
    ));

    let before = text(&document, window);

    press(window, "x d d");
    press(window, "i");
    window.simulate_input("Z");
    window.run_until_parked();

    assert_eq!(text(&document, window), before);
    assert!(!is_dirty(&document, window));
}

/// Turning the setting off makes keys type in an open text view at once,
/// and turning it on again brings Normal mode back.
#[gpui::test]
fn vim_mode_follows_the_setting_in_an_open_text_view(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-setting");
    let (_path, document, window) = open_with_vim(cx, &directory, CITIES, true);
    select(&document, window, 0);

    press(window, "t");

    set_vim(window, false);
    assert_eq!(vim_mode(&document, window), None);
    window.simulate_input("j");
    window.run_until_parked();
    assert_eq!(text(&document, window), "name,city\njAna,Lima\nBo,Quito\n");

    // Normal mode is back: `x` deletes the character after the typed `j`.
    set_vim(window, true);
    press(window, "x");
    assert_eq!(text(&document, window), "name,city\njna,Lima\nBo,Quito\n");
}

/// With Vim mode on, Normal-mode keys act on the text while the editor
/// holds the keyboard and never reach the tab's commands: `t` and `Shift+T`
/// neither switch the view nor the mode, and `u` undoes inside the text.
#[gpui::test]
fn normal_mode_keys_never_reach_the_tab_commands(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-normal-keys");
    let (_path, document, window) = open_with_vim(cx, &directory, CITIES, true);
    select(&document, window, 0);

    press(window, "t");

    // `m a` sets a mark, where the tab would open its pane actions.
    press(window, "t shift-t m a k j");
    assert_eq!(view(&document, window), DelimitedView::Text);
    assert_eq!(mode(&document, window), TextMode::Raw);
    assert_eq!(context(&document, window), ContextId::TextInput);
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));

    press(window, "x");
    assert_eq!(text(&document, window), "name,city\nna,Lima\nBo,Quito\n");

    press(window, "u");
    assert_eq!(text(&document, window), String::from_utf8_lossy(CITIES));
    assert_eq!(view(&document, window), DelimitedView::Text);
}

/// The save key saves through the document, which applies the text first,
/// in Normal mode and in Insert mode.
#[gpui::test]
fn the_save_key_saves_the_text_in_normal_and_insert_mode(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-save-key");
    let (path, document, window) = open_with_vim(cx, &directory, CITIES, true);
    select(&document, window, 0);

    press(window, "t x");
    press(window, "ctrl-s");

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), b"name,city\nna,Lima\nBo,Quito\n");

    press(window, "i");
    window.simulate_input("Q");
    press(window, "ctrl-s");

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), b"name,city\nQna,Lima\nBo,Quito\n");
    assert_eq!(view(&document, window), DelimitedView::Text);
}

/// A Vim leader sequence bound to Save saves through the document from
/// Normal mode.
#[gpui::test]
fn a_leader_save_saves_the_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-leader-save");
    let (path, document, window) = open_with_vim(cx, &directory, CITIES, true);
    select(&document, window, 0);

    window.update(|_, cx| {
        let leader = ContextId::VimNormal.default_predicate();

        cx.bind_keys([gpui::KeyBinding::new(
            "space s",
            dbflux_components::vim::LeaderCommand::new("save_query"),
            Some(leader),
        )]);
    });

    press(window, "t x");
    press(window, "space s");

    assert!(!is_dirty(&document, window));
    assert_eq!(read(&path), b"name,city\nna,Lima\nBo,Quito\n");
}

/// The mode indicator is drawn below the text while Vim mode is on, and
/// never while it is off or the table is shown.
#[gpui::test]
fn the_mode_indicator_shows_with_vim_mode_in_the_text_view(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-indicator");
    let (_path, document, window) = open_with_vim(cx, &directory, CITIES, true);

    assert!(!draws(window, "vim-mode-indicator"), "the table is shown");

    press(window, "t");
    assert!(draws(window, "vim-mode-indicator"));

    press(window, "escape t");
    assert!(!draws(window, "vim-mode-indicator"), "the table is shown");
    assert!(key_context_entries(&document, window).is_empty());

    press(window, "t");

    set_vim(window, false);
    assert!(!draws(window, "vim-mode-indicator"));
}

/// With Vim mode on, the text view is still covered by the keyboard
/// coverage registry.
#[gpui::test]
fn the_text_view_with_vim_mode_is_covered(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-coverage");
    let (_path, document, window) = open_with_vim(cx, &directory, CITIES, true);

    press(window, "t");

    let checked = covered_ids(&document, window);
    assert!(
        checked
            .iter()
            .any(|id| id == "segmented-delimited-text-mode-raw"),
        "{checked:?}"
    );
}

/// The modal cell editor brings its own Vim mode, as in the data grid: `x`
/// deletes in Normal mode and the save key stages the value.
#[gpui::test]
fn the_modal_cell_editor_follows_vim_mode(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("vim-cell-editor");
    let long = "abcdefghij".repeat(15);
    let bytes = format!("name,note\nAna,{long}\n");
    let (path, document, window) = open_with_vim(cx, &directory, bytes.as_bytes(), true);

    assert!(start_cell_edit(&document, window, 0, 1));

    press(window, "x");
    press(window, "ctrl-s");
    save(&document, window);

    assert_eq!(
        read(&path),
        format!("name,note\nAna,{}\n", &long[1..]).into_bytes()
    );
}
