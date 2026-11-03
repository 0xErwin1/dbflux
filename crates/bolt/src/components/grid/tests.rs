use super::{CellPaint, GridColors, cell_paint, header_cells};
use crate::data::{
    CellAlignment, CellTone, CellView, ColumnKind, ColumnView, RowSource, RowVisualState,
    StaticRows,
};
use crate::tokens::GridMetrics;
use gpui::TestAppContext;
use gpui_component::ActiveTheme;

/// A neutral brand at the default palette and density, so the tests render
/// against the same theme engine the products install.
fn install_test_theme(cx: &mut TestAppContext) {
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

/// The paint decisions of one cell, computed against the live theme.
/// Returns the decision, the theme it was computed against and the null
/// color, so assertions compare against the same values.
fn paint_of(
    cx: &mut TestAppContext,
    view: &CellView,
    group_child: bool,
    row_pending_delete: bool,
    is_active: bool,
) -> (CellPaint, gpui_component::Theme, gpui::Hsla) {
    install_test_theme(cx);
    let paint = std::rc::Rc::new(std::cell::RefCell::new(None));
    let paint_for_assert = paint.clone();
    cx.update(move |cx| {
        let theme = cx.theme().clone();
        let null_color = crate::tokens::SyntaxColors::for_current(cx).number;
        let colors = GridColors::of(&theme);
        paint_for_assert.replace(Some((
            cell_paint(
                view,
                group_child,
                row_pending_delete,
                is_active,
                &colors,
                null_color,
            ),
            theme,
            null_color,
        )));
    });
    paint
        .borrow()
        .clone()
        .expect("the paint decision must be computed")
}

/// A transition cell takes the transition color whatever its tone says:
/// the content outranks the tone in the text color.
#[gpui::test]
fn a_transition_cell_takes_the_transition_color_over_its_tone(cx: &mut TestAppContext) {
    let (transitioning, theme, _) = paint_of(
        cx,
        &CellView::transition("old", "new", CellTone::Normal, CellAlignment::Left).with_dirty(true),
        false,
        false,
        false,
    );
    assert_eq!(
        transitioning.text_color, theme.warning,
        "the transition beats the cell's own color"
    );
    assert!(transitioning.transition);
    assert!(
        !transitioning.dirty_mark,
        "a transition never draws the dirty mark"
    );

    // Even a null tone under a transition stays in the transition color.
    let (nulled, theme, null_color) = paint_of(
        cx,
        &CellView::transition("old", "NULL", CellTone::Null, CellAlignment::Left),
        false,
        false,
        false,
    );
    assert_eq!(nulled.text_color, theme.warning);
    assert_ne!(nulled.text_color, null_color);

    // And the same tone without the transition draws normally, which is
    // what makes the precedence observable.
    let (plain_dirty, theme, _) = paint_of(
        cx,
        &CellView::new("new", CellTone::Normal, CellAlignment::Left).with_dirty(true),
        false,
        false,
        false,
    );
    assert_eq!(plain_dirty.text_color, theme.foreground);
}

/// A plain staged edit draws the dirty mark; a transition draws the
/// transition wash instead. The two treatments are mutually exclusive.
#[gpui::test]
fn a_plain_dirty_cell_draws_the_mark_and_a_transition_does_not(cx: &mut TestAppContext) {
    let (plain, _, _) = paint_of(
        cx,
        &CellView::new("new", CellTone::Normal, CellAlignment::Left).with_dirty(true),
        false,
        false,
        false,
    );
    assert!(plain.dirty_mark, "the relational dirty mark draws");
    assert!(!plain.transition);

    let (transition, _, _) = paint_of(
        cx,
        &CellView::transition("old", "new", CellTone::Normal, CellAlignment::Left).with_dirty(true),
        false,
        false,
        false,
    );
    assert!(transition.transition, "the transition wash draws");
    assert!(!transition.dirty_mark);
}

/// The accepted-defect regression: a relational staged null draws the
/// null styling (null color, italic) and the amber dirty mark at once.
/// The two facts are orthogonal — the tone says what the value is, the
/// dirty flag says it holds an uncommitted edit — so the cell needs
/// neither sacrificed for the other.
///
/// The view here is the one the product's `RowSource` adapter produces for
/// a staged null over `"old"`: tone `Null`, `dirty` set, plain `NULL` text.
/// That the adapter derives exactly this view is pinned in the adapter's
/// own suite (`a_staged_null_edit_draws_the_null_styling_and_the_dirty_mark_together`
/// and `a_staged_null_edit_reads_dirty_so_its_mark_survive`s there).
#[gpui::test]
fn a_staged_null_edit_draws_the_null_styling_and_the_dirty_mark_together(cx: &mut TestAppContext) {
    let mut rows = StaticRows::new();
    rows.push_column(ColumnView::new("v", ColumnKind::Text));
    rows.push_row(
        vec![CellView::new("NULL", CellTone::Null, CellAlignment::Left).with_dirty(true)],
        RowVisualState::Clean,
    );
    let view = rows.cell(0, 0).expect("cell");

    let (staged_null, _, null_color) = paint_of(cx, &view, false, false, false);
    assert_eq!(
        staged_null.text_color, null_color,
        "the staged null draws the null color"
    );
    assert!(staged_null.italic, "the staged null draws the null italic");
    assert!(
        staged_null.dirty_mark,
        "the staged null still draws the dirty mark"
    );
    assert!(!staged_null.transition);
}

/// The pending-delete combinations: the row strikes every cell, the
/// row mutes the color of its ordinary cells, and a null or muted cell
/// keeps its own italic on top.
#[gpui::test]
fn pending_delete_rows_strike_all_cells_and_mute_the_ordinary_ones(cx: &mut TestAppContext) {
    let alignment = CellAlignment::Left;

    let (struck, theme, _) = paint_of(
        cx,
        &CellView::new("x", CellTone::Normal, alignment),
        false,
        true,
        false,
    );
    assert!(struck.line_through);
    assert!(!struck.italic, "a struck ordinary cell does not italicize");
    assert_eq!(
        struck.text_color, theme.muted_foreground,
        "the row's ordinary cells take the pending-delete color"
    );

    let (struck_null, _, null_color) = paint_of(
        cx,
        &CellView::new("NULL", CellTone::Null, alignment),
        false,
        true,
        false,
    );
    assert!(struck_null.line_through);
    assert!(struck_null.italic, "a null cell keeps its own italic");
    assert_ne!(
        struck_null.text_color, null_color,
        "the row mutes the null color, as the old painter drew it"
    );

    let (struck_muted, theme, _) = paint_of(
        cx,
        &CellView::new("auto", CellTone::Muted, alignment),
        false,
        true,
        false,
    );
    assert!(struck_muted.line_through);
    assert!(struck_muted.italic, "a muted cell keeps its own italic");
    assert_eq!(struck_muted.text_color, theme.muted_foreground);
}

/// A group-child cell of a document grid draws the wash; the fact comes
/// from `RowSource::column`, exactly as the painter reads it. (That the
/// product's adapter sets `group_child` from its document presentation is
/// pinned in the adapter suite, `a_grouped_document_column_washes_its_cells`.)
#[gpui::test]
fn a_group_child_cell_draws_the_wash(cx: &mut TestAppContext) {
    let mut rows = StaticRows::new();
    rows.push_column(ColumnView::new("a", ColumnKind::Text));
    rows.push_column(ColumnView::new("b", ColumnKind::Text).with_group_child(true));
    rows.push_row(
        vec![
            CellView::new("x", CellTone::Normal, CellAlignment::Left),
            CellView::new("y", CellTone::Normal, CellAlignment::Left),
        ],
        RowVisualState::Clean,
    );

    let (washed, _, _) = paint_of(
        cx,
        &rows.cell(0, 1).expect("cell"),
        rows.column(1).expect("column").group_child,
        false,
        false,
    );
    assert!(
        washed.group_wash,
        "the grouped column's cell draws the wash"
    );

    let (unwashed, _, _) = paint_of(
        cx,
        &rows.cell(0, 0).expect("cell"),
        rows.column(0).expect("column").group_child,
        false,
        false,
    );
    assert!(!unwashed.group_wash);
}

/// The italic and strike decisions: null, muted and missing cells
/// italicize; a pending-delete row strikes all its cells whatever their
/// tone, and nothing else strikes.
#[gpui::test]
fn null_muted_and_missing_italicize_and_pending_delete_strikes(cx: &mut TestAppContext) {
    let alignment = CellAlignment::Left;
    for (view, expect_italic) in [
        (CellView::new("x", CellTone::Null, alignment), true),
        (CellView::new("x", CellTone::Muted, alignment), true),
        (CellView::new("x", CellTone::Missing, alignment), true),
        (CellView::new("x", CellTone::Normal, alignment), false),
        (CellView::new("x", CellTone::Nested, alignment), false),
        // The edit state does not italicize; a staged text edit draws
        // upright, as the old painter drew it.
        (
            CellView::new("x", CellTone::Normal, alignment).with_dirty(true),
            false,
        ),
    ] {
        let (paint, _, _) = paint_of(cx, &view, false, false, false);
        assert_eq!(
            paint.italic, expect_italic,
            "tone {:?} italic decision",
            view.tone
        );
        assert!(!paint.line_through);
    }
}

/// The status tones a run result needs map from the palette's semantic
/// colors: a passing value draws in the success color, a warning in the
/// warning color, a failure in the danger color — upright, unstruck, like
/// any tone that is not a null or a legend.
#[gpui::test]
fn the_status_tones_map_from_the_palettes_semantic_colors(cx: &mut TestAppContext) {
    let (passed, theme, _) = paint_of(
        cx,
        &CellView::new("pass", CellTone::Success, CellAlignment::Left),
        false,
        false,
        false,
    );
    assert_eq!(passed.text_color, theme.success);
    assert!(!passed.italic);
    assert!(!passed.line_through);

    let (warned, theme, _) = paint_of(
        cx,
        &CellView::new("warn", CellTone::Warning, CellAlignment::Left),
        false,
        false,
        false,
    );
    assert_eq!(warned.text_color, theme.warning);

    let (failed, theme, _) = paint_of(
        cx,
        &CellView::new("fail", CellTone::Danger, CellAlignment::Left),
        false,
        false,
        false,
    );
    assert_eq!(failed.text_color, theme.danger);

    // The semantic tones are not the ordinary ones: each of the three maps
    // from its own palette color, not from the foreground.
    assert_ne!(passed.text_color, failed.text_color);
    assert_ne!(warned.text_color, theme.foreground);
}

/// A semantic status keeps its color under the active tint: the focused
/// success cell still reads as success, as a null cell does today.
#[gpui::test]
fn an_active_status_cell_keeps_its_semantic_color(cx: &mut TestAppContext) {
    let (active, theme, _) = paint_of(
        cx,
        &CellView::new("pass", CellTone::Success, CellAlignment::Left),
        false,
        false,
        true,
    );
    assert_eq!(active.text_color, theme.success);
    assert_ne!(
        active.text_color,
        crate::tokens::ChromeColors::strong(&theme),
        "the active tint does not overwrite a semantic status"
    );
}

/// The header derives one cell per column the source answers with, named
/// after the column, carrying its type label, drawn at the host's
/// laid-out width (or the grid's default for an unsized column), and id'd
/// from the caller's base plus the column index.
#[test]
fn the_header_cells_carry_the_column_names_widths_and_ids() {
    let mut rows = StaticRows::new();
    rows.push_column(ColumnView::new("status", ColumnKind::Text).with_type_name("text"));
    rows.push_column(ColumnView::new("rows", ColumnKind::Integer).with_group_child(true));

    let cells = header_cells("run-header", &rows, &[140.0]);

    assert_eq!(cells.len(), 2);
    assert_eq!(cells[0].name, "status");
    assert_eq!(cells[0].type_name, "text");
    assert_eq!(cells[0].width, 140.0);
    assert!(!cells[0].group_child);
    assert_eq!(
        cells[0].id,
        gpui::ElementId::named_usize("run-header", 0),
        "the id comes from the caller's base"
    );

    assert_eq!(cells[1].name, "rows");
    assert_eq!(cells[1].type_name, "");
    assert_eq!(
        cells[1].width,
        f32::from(GridMetrics::DEFAULT_COLUMN_WIDTH),
        "an unsized column falls back to the grid default, as the rows do"
    );
    assert!(cells[1].group_child);
    assert_eq!(cells[1].id, gpui::ElementId::named_usize("run-header", 1));
}

/// A column past the source's count answers no header cell, as no row
/// cell is answered for a stale index.
#[test]
fn the_header_answers_no_cell_past_the_column_count() {
    let mut rows = StaticRows::new();
    rows.push_column(ColumnView::new("only", ColumnKind::Text));

    assert_eq!(header_cells("run-header", &rows, &[]).len(), 1);
}

/// The ordinary tone colors: foreground, the active cell's strong tint,
/// and the null color.
#[gpui::test]
fn ordinary_cells_take_the_foreground_and_the_active_cell_the_strong_tint(cx: &mut TestAppContext) {
    let (normal, theme, _) = paint_of(
        cx,
        &CellView::new("x", CellTone::Normal, CellAlignment::Left),
        false,
        false,
        false,
    );
    assert_eq!(normal.text_color, theme.foreground);

    let (active, theme, _) = paint_of(
        cx,
        &CellView::new("x", CellTone::Normal, CellAlignment::Left),
        false,
        false,
        true,
    );
    assert_eq!(
        active.text_color,
        crate::tokens::ChromeColors::strong(&theme)
    );

    let (null_cell, _, null_color) = paint_of(
        cx,
        &CellView::new("NULL", CellTone::Null, CellAlignment::Left),
        false,
        false,
        true,
    );
    assert_eq!(
        null_cell.text_color, null_color,
        "null outranks the active tint, as the painter draws it today"
    );
}
