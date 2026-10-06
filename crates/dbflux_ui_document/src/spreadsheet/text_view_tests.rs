//! The text view of a spreadsheet: the shown sheet's values as read-only
//! CSV, how it follows pending edits and sheet switches, the cut of a large
//! sheet, and the switch between it and the table.

use dbflux_app::keymap::ContextId;
use dbflux_components::components::data_table::model::CellValue;
use gpui::{Entity, Focusable as _, TestAppContext, VisualTestContext};

use super::document::SpreadsheetDocument;
use super::tests::{TestDirectory, items_workbook, open_local, table_state, xlsx_bytes};
use super::text_view::{SheetView, TextLimits};

fn view(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> SheetView {
    window.update(|_, cx| document.read(cx).view())
}

fn show(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext, view: SheetView) {
    window.update(|_, cx| document.update(cx, |document, cx| document.show_view(view, cx)));
    window.run_until_parked();
}

/// The text the text view's editor holds.
fn text(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> String {
    window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .expect("the text view was shown")
            .read(cx)
            .value()
            .to_string()
    })
}

fn text_is_focused(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|window, cx| {
        document
            .read(cx)
            .text_input()
            .is_some_and(|input| input.focus_handle(cx).is_focused(window))
    })
}

fn table_is_focused(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> bool {
    let table_state = table_state(document, window);

    window.update(|window, cx| table_state.read(cx).focus_handle().is_focused(window))
}

fn context(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> ContextId {
    window.update(|_, cx| document.read(cx).active_context())
}

fn stage(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
    value: &str,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.stage_cell_value(row, column, CellValue::text(value));
            cx.notify();
        })
    });
    window.run_until_parked();
}

fn press(window: &mut VisualTestContext, keystrokes: &str) {
    window.simulate_keystrokes(keystrokes);
    window.run_until_parked();
}

#[gpui::test]
fn text_view_renders_the_sheet_as_csv(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-csv");
    let bytes = xlsx_bytes(|workbook| {
        let sheet = workbook.add_worksheet();
        sheet.write(0, 0, "plain")?;
        sheet.write(0, 1, "a,b")?;
        sheet.write(0, 2, "say \"hi\"")?;
        sheet.write(1, 0, "line\nbreak")?;
        sheet.write(1, 1, 42)?;
        Ok(())
    });
    let path = directory.file("quoting.xlsx", &bytes);

    let (document, window) = open_local(cx, path);
    assert_eq!(view(&document, window), SheetView::Table);

    show(&document, window, SheetView::Text);

    assert_eq!(view(&document, window), SheetView::Text);
    assert_eq!(
        text(&document, window),
        "plain,\"a,b\",\"say \"\"hi\"\"\"\n\"line\nbreak\",42,\n"
    );
    assert!(window.debug_bounds("spreadsheet-text-view").is_some());
    assert!(
        window.debug_bounds("spreadsheet-text-cut").is_none(),
        "a small sheet is shown whole"
    );
}

#[gpui::test]
fn text_view_includes_pending_edits(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-edits");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    stage(&document, window, 1, 0, "Quill");
    window.update(|_, cx| document.update(cx, |document, cx| document.append_row(cx)));
    window.run_until_parked();
    stage(&document, window, 3, 0, "Nib, fine");

    show(&document, window, SheetView::Text);

    assert_eq!(
        text(&document, window),
        "Name,Amount,Double\nQuill,3,6\nInk,7,14\n\"Nib, fine\",,\n"
    );

    // An edit made in the table after the text was shown is in the text the
    // next time it is shown.
    show(&document, window, SheetView::Table);
    stage(&document, window, 2, 1, "8");
    show(&document, window, SheetView::Text);

    assert_eq!(
        text(&document, window),
        "Name,Amount,Double\nQuill,3,6\nInk,8,14\n\"Nib, fine\",,\n"
    );
}

#[gpui::test]
fn switching_sheets_updates_the_text_view(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-sheets");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    show(&document, window, SheetView::Text);
    assert!(text(&document, window).starts_with("Name,Amount,Double\n"));

    window.update(|window, cx| {
        document.update(cx, |document, cx| document.select_sheet(1, window, cx))
    });
    window.run_until_parked();

    assert_eq!(view(&document, window), SheetView::Text);
    assert_eq!(text(&document, window), "first\nsecond\nthird\n");
}

#[gpui::test]
fn text_built_for_the_previous_sheet_is_never_shown(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-stale-arrival");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);
    show(&document, window, SheetView::Text);

    let input = window.update(|_, cx| {
        document
            .read(cx)
            .text_input()
            .cloned()
            .expect("the text view was shown")
    });
    let shown = std::rc::Rc::new(std::cell::RefCell::new(Vec::<String>::new()));
    let _observation = window.update(|_, cx| {
        let shown = shown.clone();
        cx.observe(&input, move |input, cx| {
            shown.borrow_mut().push(input.read(cx).value().to_string());
        })
    });

    // The edit starts a build of the Items text, and the sheet switch comes
    // before that build ends.
    let table_state = table_state(&document, window);
    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.stage_cell_value(1, 0, CellValue::text("Quill"));
            cx.notify();
        })
    });
    window.update(|window, cx| {
        document.update(cx, |document, cx| document.select_sheet(1, window, cx))
    });
    window.run_until_parked();

    assert_eq!(text(&document, window), "first\nsecond\nthird\n");
    assert!(
        shown.borrow().iter().all(|text| !text.contains("Quill")),
        "{:?}",
        shown.borrow()
    );
}

#[gpui::test]
fn a_large_sheet_text_is_cut_with_a_note(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-cut");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);

    window.update(|_, cx| {
        document.update(cx, |document, _cx| {
            document.set_text_limits(TextLimits {
                max_bytes: 1024,
                max_lines: 2,
            })
        })
    });

    show(&document, window, SheetView::Text);

    assert_eq!(text(&document, window), "Name,Amount,Double\nPen,3,6\n");
    assert_eq!(
        window.update(|_, cx| document.read(cx).text_cut()),
        Some((2, 3))
    );
    assert!(window.debug_bounds("spreadsheet-text-cut").is_some());

    window.update(|_, cx| {
        document.update(cx, |document, _cx| {
            document.set_text_limits(TextLimits {
                max_bytes: 30,
                max_lines: 100,
            })
        })
    });
    show(&document, window, SheetView::Table);
    show(&document, window, SheetView::Text);

    assert_eq!(
        text(&document, window),
        "Name,Amount,Double\nPen,3,6\n",
        "the text stops at the last row under the byte limit"
    );
}

#[gpui::test]
fn t_switches_between_table_and_text(cx: &mut TestAppContext) {
    let directory = TestDirectory::new("text-keys");
    let path = directory.file("items.xlsx", &items_workbook());

    let (document, window) = open_local(cx, path);
    assert!(table_is_focused(&document, window));

    press(window, "t");

    assert_eq!(view(&document, window), SheetView::Text);
    assert!(text_is_focused(&document, window));
    assert_eq!(context(&document, window), ContextId::TextInput);

    // The text is read-only: a typed key changes nothing.
    let before = text(&document, window);
    press(window, "x");
    assert_eq!(text(&document, window), before);
    assert_eq!(view(&document, window), SheetView::Text);

    press(window, "escape");
    assert_eq!(context(&document, window), ContextId::Results);
    assert!(!text_is_focused(&document, window));

    press(window, "enter");
    assert!(text_is_focused(&document, window));

    press(window, "escape");
    press(window, "t");

    assert_eq!(view(&document, window), SheetView::Table);
    assert!(table_is_focused(&document, window));
    assert_eq!(context(&document, window), ContextId::Results);
}
