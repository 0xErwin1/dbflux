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

/// Each header cell is a column header under its id, named by the column
/// name and, when the column has one, its type label.
#[gpui::test]
fn the_header_cells_are_named_column_headers(cx: &mut TestAppContext) {
    use crate::accessibility::{FrameCapture, accessibility_node, accessible_name};

    struct Header;

    impl gpui::Render for Header {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            use gpui::{ParentElement as _, Styled as _};

            let mut rows = StaticRows::new();
            rows.push_column(ColumnView::new("status", ColumnKind::Text).with_type_name("text"));
            rows.push_column(ColumnView::new("rows", ColumnKind::Integer));
            let colors = GridColors::of(cx.theme());

            gpui::div().size_full().child(super::render_header(
                "run-header",
                &rows,
                &[140.0, 120.0],
                gpui::px(40.0),
                260.0,
                gpui::px(12.5),
                gpui::px(10.5),
                &colors,
                None,
            ))
        }
    }

    install_test_theme(cx);
    let (_, window) = cx.add_window_view(|_, _| Header);
    let capture = FrameCapture::observe(window);
    let frame = capture.frame(window);

    assert_eq!(
        gpui::ElementId::named_usize("run-header", 0).to_string(),
        "run-header-0",
        "the cell's node id is the header id the caller's base names"
    );
    assert_eq!(
        accessible_name(&frame, "run-header-0").as_deref(),
        Some("status, text")
    );
    assert_eq!(
        accessible_name(&frame, "run-header-1").as_deref(),
        Some("rows")
    );
    assert_eq!(
        accessibility_node(&frame, "run-header-0").map(|node| node.role()),
        Some(gpui::Role::ColumnHeader)
    );
    assert!(
        window.debug_bounds("run-header-0").is_some(),
        "the cell records its id as its selector"
    );
}

#[test]
fn a_header_cell_without_a_type_label_is_named_by_its_column_alone() {
    assert_eq!(super::header_cell_name("rows", ""), "rows");
    assert_eq!(super::header_cell_name("status", "text"), "status, text");
}

/// A header with two columns whose resize behaviour the test picks.
struct ResizableHeader {
    drag: Option<super::ColumnResizeDrag>,
}

impl gpui::Render for ResizableHeader {
    fn render(
        &mut self,
        _window: &mut gpui::Window,
        cx: &mut gpui::Context<Self>,
    ) -> impl gpui::IntoElement {
        use gpui::{ParentElement as _, Styled as _};

        let mut rows = StaticRows::new();
        rows.push_column(ColumnView::new("status", ColumnKind::Text));
        rows.push_column(ColumnView::new("rows", ColumnKind::Integer));
        let colors = GridColors::of(cx.theme());
        let resize = self.drag.as_ref().map(|drag| super::HeaderResize {
            drag,
            hover: colors.border,
        });

        gpui::div().size_full().child(super::render_header(
            "run-header",
            &rows,
            &[140.0, 120.0],
            gpui::px(40.0),
            260.0,
            gpui::px(12.5),
            gpui::px(10.5),
            &colors,
            resize,
        ))
    }
}

/// The point on a header cell's trailing edge where its resize handle sits.
fn handle_point(
    window: &mut gpui::VisualTestContext,
    cell: &'static str,
) -> gpui::Point<gpui::Pixels> {
    let bounds = window
        .debug_bounds(cell)
        .expect("the header cell records its bounds");

    gpui::point(
        bounds.right() - super::RESIZE_HANDLE_WIDTH / 2.0,
        bounds.center().y,
    )
}

#[gpui::test]
fn a_header_with_resize_carries_a_handle_that_resizes_only_its_column(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let drag = super::ColumnResizeDrag::new();
    let drag_for_view = drag.clone();
    let (_, window) = cx.add_window_view(move |_, _| ResizableHeader {
        drag: Some(drag_for_view),
    });

    let point = handle_point(window, "run-header-1");
    window.simulate_mouse_down(point, gpui::MouseButton::Left, gpui::Modifiers::none());

    assert!(drag.is_active(), "pressing the handle starts the drag");
    let resize = drag
        .update(point.x + gpui::px(30.0))
        .expect("an active drag reports a resize");
    assert_eq!(resize.column, 1, "only the pressed column resizes");
    assert_eq!(resize.width, 150.0);
}

#[gpui::test]
fn the_first_columns_handle_resizes_the_first_column(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let drag = super::ColumnResizeDrag::new();
    let drag_for_view = drag.clone();
    let (_, window) = cx.add_window_view(move |_, _| ResizableHeader {
        drag: Some(drag_for_view),
    });

    let point = handle_point(window, "run-header-0");
    window.simulate_mouse_down(point, gpui::MouseButton::Left, gpui::Modifiers::none());

    let resize = drag
        .update(point.x - gpui::px(20.0))
        .expect("an active drag reports a resize");
    assert_eq!(resize.column, 0);
    assert_eq!(resize.width, 120.0);
}

#[gpui::test]
fn a_header_without_resize_lays_out_the_cells_as_before(cx: &mut TestAppContext) {
    install_test_theme(cx);
    let (_, plain) = cx.add_window_view(|_, _| ResizableHeader { drag: None });
    let plain_bounds = [
        plain.debug_bounds("run-header-0"),
        plain.debug_bounds("run-header-1"),
    ];

    let drag = super::ColumnResizeDrag::new();
    let (_, resizable) = cx.add_window_view(move |_, _| ResizableHeader { drag: Some(drag) });
    let resizable_bounds = [
        resizable.debug_bounds("run-header-0"),
        resizable.debug_bounds("run-header-1"),
    ];

    assert!(plain_bounds.iter().all(Option::is_some));
    assert_eq!(
        plain_bounds, resizable_bounds,
        "handles are overlaid and never move a cell"
    );
}

/// The grid inside its horizontal overflow, composed the way a product
/// composes it: one root that follows a column drag, and a header plus a
/// body sharing a single sideways scroll.
mod overflow {
    use std::cell::RefCell;
    use std::ops::Range;
    use std::rc::Rc;
    use std::sync::Arc;

    use gpui::{
        AnyElement, Bounds, Context, InteractiveElement, IntoElement, Modifiers, MouseButton,
        ParentElement, Pixels, Point, Render, ScrollDelta, ScrollHandle, ScrollWheelEvent, Styled,
        TestAppContext, TouchPhase, UniformListScrollHandle, VisualTestContext, Window, div, point,
        px, uniform_list,
    };
    use gpui_base::ScrollbarHandle as _;
    use gpui_component::ActiveTheme;

    use super::super::{
        ColumnResize, ColumnResizeDrag, GridCellCoord, GridColors, GridHost, GridOverflow,
        HeaderResize, RESIZE_HANDLE_WIDTH, render_header, render_rows, track_column_resize,
    };
    use super::install_test_theme;
    use crate::data::{
        CellAlignment, CellTone, CellView, ColumnKind, ColumnView, RowVisualState, StaticRows,
    };
    use crate::tokens::GridMetrics;

    /// A viewport narrower than the grid's columns.
    const NARROW_PANE: Pixels = px(300.0);
    /// A viewport wider than them.
    const WIDE_PANE: Pixels = px(900.0);
    const COLUMN_WIDTH: f32 = 200.0;
    const COLUMN_COUNT: usize = 3;

    /// A host with a grid, its horizontal overflow, and the column drag
    /// mounted on its root.
    struct OverflowHost {
        handle: ScrollHandle,
        list: UniformListScrollHandle,
        drag: ColumnResizeDrag,
        resizes: Rc<RefCell<Vec<ColumnResize>>>,
        pane: Pixels,
        wrapped: bool,
        virtualized: bool,
        resizable: bool,
        row_count: usize,
    }

    impl OverflowHost {
        fn new() -> Self {
            Self {
                handle: ScrollHandle::new(),
                list: UniformListScrollHandle::new(),
                drag: ColumnResizeDrag::new(),
                resizes: Rc::new(RefCell::new(Vec::new())),
                pane: NARROW_PANE,
                wrapped: true,
                virtualized: false,
                resizable: false,
                row_count: 2,
            }
        }

        fn total_width(&self) -> f32 {
            f32::from(GridMetrics::ROW_NUMBER_WIDTH) + COLUMN_WIDTH * COLUMN_COUNT as f32
        }
    }

    /// The grid's columns and rows: `row_count` rows of three cells each.
    fn grid_rows(row_count: usize) -> StaticRows {
        let mut rows = StaticRows::new();

        for column in 0..COLUMN_COUNT {
            rows.push_column(
                ColumnView::new(format!("column {column}"), ColumnKind::Text)
                    .with_fixed_width(px(COLUMN_WIDTH)),
            );
        }

        for row in 0..row_count {
            let cells = (0..COLUMN_COUNT)
                .map(|column| {
                    CellView::new(
                        format!("cell {row}:{column}"),
                        CellTone::Normal,
                        CellAlignment::Left,
                    )
                })
                .collect();
            rows.push_row(cells, RowVisualState::Clean);
        }

        rows
    }

    /// The rows `range` of `source`, painted by the grid's own row renderer.
    fn rows_of(
        range: Range<usize>,
        source: &StaticRows,
        total_width: f32,
        colors: &GridColors,
        cx: &mut gpui::App,
    ) -> Vec<AnyElement> {
        let widths = [COLUMN_WIDTH; COLUMN_COUNT];
        let is_selected = |_: GridCellCoord| false;
        let host = GridHost {
            column_widths: &widths,
            active: None,
            editing: None,
            is_selected: &is_selected,
            reference_row: false,
            on_cell_click: Arc::new(|_, _, _, _| {}),
            on_cell_context: Arc::new(|_, _, _, _| {}),
            braces_icon: gpui_component::IconName::ChevronRight.into(),
            chevron_icon: gpui_component::IconName::ChevronRight.into(),
            reference_icon: gpui_component::IconName::ChevronRight.into(),
        };

        render_rows(
            range,
            source,
            &host,
            None,
            total_width,
            GridMetrics::FONT,
            GridMetrics::ROW_HEIGHT,
            GridMetrics::HEADER_ICON,
            crate::colors(cx).muted,
            colors,
        )
    }

    impl Render for OverflowHost {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let colors = GridColors::of(cx.theme());
            let widths = [COLUMN_WIDTH; COLUMN_COUNT];
            let total_width = self.total_width();
            let source = Arc::new(grid_rows(self.row_count));
            let resize = self.resizable.then_some(HeaderResize {
                drag: &self.drag,
                hover: colors.border,
            });
            let header = render_header(
                "grid-header-cell",
                source.as_ref(),
                &widths,
                crate::fonts::grid_header_height(cx),
                total_width,
                GridMetrics::FONT,
                GridMetrics::TYPE_FONT,
                &colors,
                resize,
            );

            let body = if self.virtualized {
                let rows_source = Arc::clone(&source);
                uniform_list(
                    "grid-rows",
                    self.row_count,
                    move |range, _window, cx: &mut gpui::App| {
                        rows_of(range, rows_source.as_ref(), total_width, &colors, cx)
                    },
                )
                .track_scroll(&self.list)
                .flex_1()
                .min_h_0()
                .into_any_element()
            } else {
                div()
                    .debug_selector(|| "grid-body".to_string())
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .children(rows_of(
                        0..self.row_count,
                        source.as_ref(),
                        total_width,
                        &colors,
                        cx,
                    ))
                    .into_any_element()
            };

            let content = if self.wrapped {
                GridOverflow::new("grid-overflow", &self.handle, total_width)
                    .header(header)
                    .rows(body)
                    .flex_1()
                    .min_h_0()
                    .into_any_element()
            } else {
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(header)
                    .child(body)
                    .into_any_element()
            };

            let resizes = self.resizes.clone();

            track_column_resize(
                div()
                    .flex()
                    .flex_col()
                    .w(self.pane)
                    .h(px(180.0))
                    .child(content),
                &self.drag,
                move |resize, _window, _cx| resizes.borrow_mut().push(resize),
            )
        }
    }

    fn open(
        host: OverflowHost,
        cx: &mut TestAppContext,
    ) -> (gpui::Entity<OverflowHost>, &mut VisualTestContext) {
        install_test_theme(cx);
        let (entity, window) = cx.add_window_view(move |_, _| host);
        window.run_until_parked();
        (entity, window)
    }

    fn bounds(window: &mut VisualTestContext, selector: &'static str) -> Bounds<Pixels> {
        window
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} is laid out"))
    }

    /// A wheel event at `position`, in content pixels.
    fn wheel_at(window: &mut VisualTestContext, position: Point<Pixels>, delta: Point<Pixels>) {
        window.simulate_event(ScrollWheelEvent {
            position,
            delta: ScrollDelta::Pixels(delta),
            modifiers: Modifiers::none(),
            touch_phase: TouchPhase::Moved,
        });
        window.run_until_parked();
    }

    /// A sideways wheel over the grid.
    fn wheel_sideways(
        window: &mut VisualTestContext,
        host: &gpui::Entity<OverflowHost>,
        dx: Pixels,
    ) {
        let position = window.update(|_, cx| host.read(cx).handle.bounds().center());

        wheel_at(window, position, point(dx, px(0.0)));
    }

    fn horizontal_offset(
        window: &mut VisualTestContext,
        host: &gpui::Entity<OverflowHost>,
    ) -> Pixels {
        window.update(|_, cx| host.read(cx).handle.offset().x)
    }

    /// Gives the grid a pane `pane` wide and lets it lay out again.
    fn resize(window: &mut VisualTestContext, host: &gpui::Entity<OverflowHost>, pane: Pixels) {
        window.update(|_, cx| {
            host.update(cx, |host, cx| {
                host.pane = pane;
                cx.notify();
            });
        });
        window.run_until_parked();
    }

    /// A grid whose columns add up past its viewport scrolls sideways, and
    /// the header and the rows move by the same amount, so a column stays
    /// over its own cells.
    #[gpui::test]
    fn a_grid_wider_than_its_viewport_scrolls_its_header_and_rows_together(
        cx: &mut TestAppContext,
    ) {
        let (host, window) = open(OverflowHost::new(), cx);
        let viewport = bounds(window, "grid-overflow");
        let header_before = bounds(window, "grid-header-cell-1");
        let body_before = bounds(window, "grid-body");

        assert_eq!(viewport.size.width, NARROW_PANE);
        assert!(
            window.update(|_, cx| host.read(cx).handle.max_offset().x) > px(0.0),
            "three {COLUMN_WIDTH} px columns overflow the viewport"
        );

        wheel_sideways(window, &host, px(-40.0));

        assert_eq!(horizontal_offset(window, &host), px(-40.0));
        assert_eq!(
            bounds(window, "grid-header-cell-1").left() - header_before.left(),
            px(-40.0),
            "the header follows the sideways scroll"
        );
        assert_eq!(
            bounds(window, "grid-body").left() - body_before.left(),
            px(-40.0),
            "the rows follow it by the same amount"
        );
    }

    /// The rows keep their own vertical scroll: a vertical wheel over the
    /// body moves the list and leaves the sideways offset alone.
    #[gpui::test]
    fn a_vertical_wheel_still_scrolls_the_rows_of_an_overflowing_grid(cx: &mut TestAppContext) {
        let mut host = OverflowHost::new();
        host.virtualized = true;
        host.row_count = 60;
        let (host, window) = open(host, cx);
        let header = bounds(window, "grid-header-cell-0");
        let over_body = point(header.center().x, header.bottom() + px(20.0));

        wheel_at(window, over_body, point(px(0.0), px(-40.0)));

        let list = window.update(|_, cx| host.read(cx).list.0.borrow().base_handle.offset());
        assert!(list.y < px(0.0), "the body scrolled vertically");
        assert_eq!(list.x, px(0.0), "the grid did not scroll sideways");
        assert_eq!(horizontal_offset(window, &host), px(0.0));
    }

    /// A grid that fits its viewport draws exactly where it drew without the
    /// overflow — the same header cells over a body spanning the same
    /// rectangle — and no scroll moves it.
    #[gpui::test]
    fn a_grid_that_fits_lays_out_as_it_did_without_the_overflow(cx: &mut TestAppContext) {
        let mut plain_host = OverflowHost::new();
        plain_host.pane = WIDE_PANE;
        plain_host.wrapped = false;
        let (_, plain) = open(plain_host, cx);
        let plain_cells = [
            plain.debug_bounds("grid-header-cell-0"),
            plain.debug_bounds("grid-header-cell-1"),
            plain.debug_bounds("grid-header-cell-2"),
        ];
        let plain_body = plain.debug_bounds("grid-body");

        let mut wrapped_host = OverflowHost::new();
        wrapped_host.pane = WIDE_PANE;
        let (host, window) = open(wrapped_host, cx);

        assert_eq!(
            bounds(window, "grid-overflow").size.width,
            WIDE_PANE,
            "the grid fills the width it is given"
        );
        for (index, cell) in [
            "grid-header-cell-0",
            "grid-header-cell-1",
            "grid-header-cell-2",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                plain_cells[index],
                window.debug_bounds(cell),
                "{cell} draws where it drew without the overflow"
            );
        }

        assert_eq!(
            window.debug_bounds("grid-body"),
            plain_body,
            "the rows span the rectangle they span without the wrapper"
        );
        assert_eq!(
            window.update(|_, cx| host.read(cx).handle.content_size().width),
            WIDE_PANE,
            "the grid's content fills the pane, as it does without the wrapper"
        );

        let before = bounds(window, "grid-header-cell-1");
        wheel_sideways(window, &host, px(-40.0));

        assert_eq!(
            window.update(|_, cx| host.read(cx).handle.max_offset().x),
            px(0.0),
            "a grid that fits has nothing to scroll"
        );
        assert_eq!(
            bounds(window, "grid-header-cell-1"),
            before,
            "a wheel does not move a grid that fits"
        );
    }

    /// A header drag resizes its column while the grid is scrolled: the
    /// handle is wherever the header is drawn and the drag follows pointer
    /// movement, not the scroll position.
    #[gpui::test]
    fn a_column_resize_keeps_working_while_the_grid_is_scrolled(cx: &mut TestAppContext) {
        let mut host = OverflowHost::new();
        host.resizable = true;
        let (host, window) = open(host, cx);

        wheel_sideways(window, &host, px(-40.0));
        assert_eq!(horizontal_offset(window, &host), px(-40.0));

        // The first column's trailing edge, wherever the scroll put it.
        let cell = bounds(window, "grid-header-cell-0");
        let grab = point(cell.right() - RESIZE_HANDLE_WIDTH / 2.0, cell.center().y);
        window.simulate_mouse_down(grab, MouseButton::Left, Modifiers::none());
        window.simulate_mouse_move(
            grab + point(px(30.0), px(0.0)),
            MouseButton::Left,
            Modifiers::none(),
        );

        let resizes = window.update(|_, cx| host.read(cx).resizes.borrow().clone());
        assert_eq!(
            resizes,
            vec![ColumnResize {
                column: 0,
                width: COLUMN_WIDTH + 30.0,
            }],
            "the drag resizes its own column by the pointer delta"
        );
    }

    /// The horizontal scrollbar draws in a strip along the bottom of the
    /// viewport, the way the data table draws it.
    #[gpui::test]
    fn the_horizontal_scrollbar_overlays_the_bottom_of_the_viewport(cx: &mut TestAppContext) {
        let (_, window) = open(OverflowHost::new(), cx);
        let viewport = bounds(window, "grid-overflow");
        let strip = bounds(window, "grid-overflow-scrollbar");

        assert_eq!(strip.size.height, GridMetrics::SCROLLBAR_WIDTH);
        assert_eq!(strip.size.width, viewport.size.width);
        assert_eq!(strip.bottom(), viewport.bottom());
    }

    /// A grid whose columns overflow ends its rows above the scrollbar's
    /// strip, so the last row is reachable at the bottom of the vertical
    /// scroll instead of sitting under the bar.
    #[gpui::test]
    fn an_overflowing_grid_reserves_the_strip_below_its_rows(cx: &mut TestAppContext) {
        let (_, window) = open(OverflowHost::new(), cx);
        let viewport = bounds(window, "grid-overflow");
        let body = bounds(window, "grid-body");
        let strip = bounds(window, "grid-overflow-scrollbar");

        assert_eq!(
            body.bottom(),
            viewport.bottom() - GridMetrics::SCROLLBAR_WIDTH,
            "the rows stop one strip above the viewport edge"
        );
        assert_eq!(
            strip.top(),
            body.bottom(),
            "the bar sits in the strip the rows leave free"
        );
    }

    /// A grid that fits draws no bar, so it keeps the height of its whole
    /// viewport: the strip is reserved only while the columns overflow.
    #[gpui::test]
    fn a_grid_that_fits_keeps_the_full_height_of_its_viewport(cx: &mut TestAppContext) {
        let mut host = OverflowHost::new();
        host.pane = WIDE_PANE;
        let (_, window) = open(host, cx);
        let viewport = bounds(window, "grid-overflow");

        assert_eq!(
            bounds(window, "grid-body").bottom(),
            viewport.bottom(),
            "no strip is reserved when the columns fit"
        );
    }

    /// The reserve follows the width the grid is given: narrowing the pane
    /// past the columns takes the strip, and widening it back returns the
    /// height to the rows.
    #[gpui::test]
    fn the_reserved_strip_follows_the_width_the_grid_is_given(cx: &mut TestAppContext) {
        let mut host = OverflowHost::new();
        host.pane = WIDE_PANE;
        let (host, window) = open(host, cx);

        resize(window, &host, NARROW_PANE);
        assert_eq!(
            bounds(window, "grid-body").bottom(),
            bounds(window, "grid-overflow").bottom() - GridMetrics::SCROLLBAR_WIDTH,
            "a pane narrower than the columns reserves the strip"
        );

        resize(window, &host, WIDE_PANE);
        assert_eq!(
            bounds(window, "grid-body").bottom(),
            bounds(window, "grid-overflow").bottom(),
            "a pane wider than the columns keeps the full height"
        );
    }
}
