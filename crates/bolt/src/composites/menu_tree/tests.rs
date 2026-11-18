//! Pure-logic tests for the cascading menu: placement, keyboard navigation and
//! the hover-intent decision. None of these need a window.

use super::{
    BackAction, ChildAction, FlyoutSide, FlyoutSpec, HOVER_CLOSE_GRACE, HOVER_OPEN_DELAY,
    HoverIntent, HoverTick, LevelMetrics, MenuNav, MenuNode, in_corridor, level_height,
    place_flyout,
};
use gpui::{Bounds, Pixels, Size, point, px, size};
use std::time::{Duration, Instant};

mod placement {
    use super::*;

    /// A 800 x 600 window keeps the arithmetic in these tests readable.
    fn viewport() -> Size<Pixels> {
        size(px(800.0), px(600.0))
    }

    fn spec(width: f32, height: f32) -> FlyoutSpec {
        FlyoutSpec {
            size: size(px(width), px(height)),
            viewport: viewport(),
            margin: px(5.0),
            overlap: px(5.0),
        }
    }

    fn parent_at(left: f32, top: f32, width: f32, height: f32) -> Bounds<Pixels> {
        Bounds {
            origin: point(px(left), px(top)),
            size: size(px(width), px(height)),
        }
    }

    #[test]
    fn flyout_opens_right_when_it_fits_beside_the_parent() {
        let placement = place_flyout(
            parent_at(100.0, 50.0, 200.0, 300.0),
            px(150.0),
            px(5.0),
            spec(200.0, 220.0),
        );

        assert_eq!(placement.side, FlyoutSide::Right);
        // 100 + 200 - 5 overlap: the flyout's leading edge meets the parent's
        // trailing edge instead of leaving a seam.
        assert_eq!(placement.origin.x, px(295.0));
        // Top-aligned to the row that opened it, less the frame padding, so the
        // flyout's first row sits level with the parent row.
        assert_eq!(placement.origin.y, px(145.0));
    }

    #[test]
    fn flyout_flips_left_when_it_would_overflow_the_right_edge() {
        let placement = place_flyout(
            parent_at(600.0, 50.0, 200.0, 300.0),
            px(150.0),
            px(5.0),
            spec(200.0, 220.0),
        );

        assert_eq!(placement.side, FlyoutSide::Left);
        // 600 - 200 + 5 overlap: mirrored, the flyout's trailing edge overlaps
        // the parent's leading edge.
        assert_eq!(placement.origin.x, px(405.0));
    }

    #[test]
    fn flyout_shifts_up_when_its_bottom_would_leave_the_window() {
        let placement = place_flyout(
            parent_at(100.0, 400.0, 200.0, 190.0),
            px(550.0),
            px(5.0),
            spec(200.0, 200.0),
        );

        assert_eq!(placement.side, FlyoutSide::Right);
        // 545 + 200 = 745 overflows 595, so it moves up by exactly the overflow.
        assert_eq!(placement.origin.y, px(395.0));
        assert!(!placement.scrolls);
    }

    #[test]
    fn flyout_keeps_its_top_inside_the_window_when_the_row_is_at_the_top() {
        let placement = place_flyout(
            parent_at(100.0, 0.0, 200.0, 300.0),
            px(2.0),
            px(5.0),
            spec(200.0, 220.0),
        );

        // 2 - 5 would be above the window; the margin wins.
        assert_eq!(placement.origin.y, px(5.0));
    }

    #[test]
    fn flyout_taller_than_the_window_scrolls_instead_of_growing() {
        let placement = place_flyout(
            parent_at(100.0, 50.0, 200.0, 300.0),
            px(150.0),
            px(5.0),
            spec(200.0, 900.0),
        );

        assert!(placement.scrolls);
        // The window less one margin at each edge.
        assert_eq!(placement.max_height, px(590.0));
        assert_eq!(placement.origin.y, px(5.0));
    }

    #[test]
    fn flyout_stays_right_when_neither_side_fits_and_the_parent_sits_left() {
        let narrow = FlyoutSpec {
            size: size(px(200.0), px(220.0)),
            viewport: size(px(300.0), px(600.0)),
            margin: px(5.0),
            overlap: px(5.0),
        };
        let placement = place_flyout(
            parent_at(10.0, 50.0, 200.0, 300.0),
            px(150.0),
            px(5.0),
            narrow,
        );

        assert_eq!(placement.side, FlyoutSide::Right);
        // Clamped inside the window rather than placed off-screen.
        assert_eq!(placement.origin.x, px(95.0));
    }

    #[test]
    fn flyout_flips_left_when_neither_side_fits_and_the_parent_sits_right() {
        let narrow = FlyoutSpec {
            size: size(px(200.0), px(220.0)),
            viewport: size(px(300.0), px(600.0)),
            margin: px(5.0),
            overlap: px(5.0),
        };
        let placement = place_flyout(
            parent_at(100.0, 50.0, 200.0, 300.0),
            px(150.0),
            px(5.0),
            narrow,
        );

        assert_eq!(placement.side, FlyoutSide::Left);
        assert_eq!(placement.origin.x, px(5.0));
    }

    #[test]
    fn level_height_sums_rows_details_headers_separators_and_padding() {
        let metrics = LevelMetrics {
            row_height: px(30.0),
            detail_row_height: px(44.0),
            header_height: px(40.0),
            separator_height: px(11.0),
            padding_y: px(5.0),
        };
        let nodes = vec![
            MenuNode::header("caption"),
            MenuNode::row("One"),
            MenuNode::row("Two"),
            MenuNode::separator(),
            MenuNode::row("Three").detail("explains it"),
        ];

        // 40 header + 30 + 30 rows + 11 separator + 44 detail row + 2 * 5 padding.
        assert_eq!(level_height(&nodes, &metrics), px(165.0));
    }
}

mod navigation {
    use super::*;

    /// root
    ///   0 header
    ///   1 Open (leaf)
    ///   2 separator
    ///   3 Blocked (disabled)
    ///   4 More > [ A, B, C ]
    ///   5 Export > [ PNG, SVG ]
    ///   6 Close (leaf)
    fn menu() -> Vec<MenuNode> {
        vec![
            MenuNode::header("caption"),
            MenuNode::action("open", "Open"),
            MenuNode::separator(),
            MenuNode::action("blocked", "Blocked").disabled(),
            MenuNode::row("More")
                .child(MenuNode::action("a", "A"))
                .child(MenuNode::action("b", "B"))
                .child(MenuNode::action("c", "C")),
            MenuNode::row("Export")
                .child(MenuNode::action("png", "PNG"))
                .child(MenuNode::action("svg", "SVG")),
            MenuNode::action("close", "Close"),
        ]
    }

    fn open_on(index: usize) -> (Vec<MenuNode>, MenuNav) {
        let root = menu();
        let mut nav = MenuNav::new();
        nav.reset(&root);
        while nav.cursor().highlight(0) != Some(index) {
            assert!(nav.step(&root, true), "the row is reachable");
        }
        (root, nav)
    }

    #[test]
    fn reset_lands_on_the_first_selectable_row() {
        let root = menu();
        let mut nav = MenuNav::new();
        nav.reset(&root);

        assert_eq!(nav.cursor().highlight(0), Some(1));
        assert_eq!(nav.cursor().depth(), 1);
        assert_eq!(nav.cursor().open_flyouts(), 0);
    }

    #[test]
    fn step_skips_separators_headers_and_disabled_rows() {
        let (root, mut nav) = open_on(1);

        assert!(nav.step(&root, true));
        assert_eq!(nav.cursor().highlight(0), Some(4));
        assert!(nav.step(&root, true));
        assert_eq!(nav.cursor().highlight(0), Some(5));
        assert!(nav.step(&root, true));
        assert_eq!(nav.cursor().highlight(0), Some(6));
    }

    #[test]
    fn step_wraps_at_both_ends_of_the_level() {
        let (root, mut nav) = open_on(6);

        assert!(nav.step(&root, true));
        assert_eq!(nav.cursor().highlight(0), Some(1));
        assert!(nav.step(&root, false));
        assert_eq!(nav.cursor().highlight(0), Some(6));
    }

    #[test]
    fn open_child_enters_the_flyout_and_lands_on_its_first_row() {
        let (root, mut nav) = open_on(4);

        assert_eq!(nav.open_child(&root), ChildAction::Opened);
        assert_eq!(nav.cursor().highlight(0), Some(4));
        assert_eq!(nav.cursor().highlight(1), Some(0));
        assert_eq!(nav.cursor().open_flyouts(), 1);
    }

    #[test]
    fn step_inside_an_open_flyout_walks_the_child_rows_only() {
        let (root, mut nav) = open_on(4);
        nav.open_child(&root);

        assert!(nav.step(&root, true));
        assert_eq!(nav.cursor().highlight(1), Some(1));
        // The parent level did not move and its flyout stayed open.
        assert_eq!(nav.cursor().highlight(0), Some(4));
    }

    #[test]
    fn open_child_on_a_leaf_asks_the_caller_to_activate_it() {
        let (root, mut nav) = open_on(1);
        assert_eq!(nav.open_child(&root), ChildAction::Activate);

        let (root, mut nav) = open_on(4);
        nav.open_child(&root);
        assert_eq!(nav.open_child(&root), ChildAction::Activate);
    }

    #[test]
    fn open_child_on_a_row_that_is_no_longer_selectable_does_nothing() {
        let live = vec![MenuNode::row("More").child(MenuNode::action("a", "A"))];
        let mut nav = MenuNav::new();
        nav.reset(&live);
        assert_eq!(nav.open_child(&live), ChildAction::Opened);

        // The caller rebuilt the level and the row the cursor rests on is
        // disabled now: the cursor keeps its path but may not open anything.
        let rebuilt = vec![MenuNode::action("gone", "Gone").disabled()];

        assert_eq!(nav.open_child(&rebuilt), ChildAction::None);
    }

    #[test]
    fn set_level_refuses_an_inert_row() {
        let root = menu();
        let mut nav = MenuNav::new();
        nav.reset(&root);

        assert!(!nav.set_level(0, 2, &root), "a separator");
        assert!(!nav.set_level(0, 3, &root), "a disabled row");
        assert_eq!(nav.cursor().highlight(0), Some(1));
    }

    #[test]
    fn back_closes_only_the_deepest_level_and_returns_to_the_parent_row() {
        let (root, mut nav) = open_on(4);
        nav.open_child(&root);

        assert_eq!(nav.back(), BackAction::ClosedLevel);
        assert_eq!(nav.cursor().highlight(1), None);
        assert_eq!(nav.cursor().highlight(0), Some(4));
        assert_eq!(nav.cursor().open_flyouts(), 0);
    }

    #[test]
    fn back_at_the_root_asks_the_caller_to_dismiss() {
        let (_, mut nav) = open_on(4);

        assert_eq!(nav.back(), BackAction::Dismissed);
        assert!(nav.cursor().highlight(0).is_some());

        nav.close_all();
        assert_eq!(nav.cursor().depth(), 0);
        assert_eq!(nav.cursor().highlight(0), None);
    }

    #[test]
    fn hover_switches_the_open_flyout_to_another_parent_row() {
        let (root, mut nav) = open_on(4);
        nav.open_child(&root);

        assert!(nav.hover(0, 5, &root));
        assert_eq!(nav.cursor().highlight(0), Some(5));
        assert_eq!(nav.cursor().highlight(1), Some(0));
    }

    #[test]
    fn hover_on_a_leaf_closes_the_branch_it_leaves() {
        let (root, mut nav) = open_on(4);
        nav.open_child(&root);

        assert!(nav.hover(0, 1, &root));
        assert_eq!(nav.cursor().highlight(0), Some(1));
        assert_eq!(nav.cursor().open_flyouts(), 0);
    }

    #[test]
    fn hover_on_an_inert_row_is_ignored() {
        let (root, mut nav) = open_on(4);

        assert!(!nav.hover(0, 2, &root));
        assert!(!nav.hover(0, 3, &root));
        assert_eq!(nav.cursor().highlight(0), Some(4));
    }

    #[test]
    fn the_current_row_is_the_one_at_the_deepest_open_level() {
        let (root, mut nav) = open_on(4);
        nav.open_child(&root);
        nav.step(&root, true);

        assert_eq!(
            nav.current(&root)
                .and_then(|node| node.id.clone())
                .as_deref(),
            Some("b")
        );
    }
}

mod hover {
    use super::*;

    fn at(millis: u64) -> Instant {
        Instant::now() + Duration::from_millis(millis)
    }

    #[test]
    fn hover_opens_only_after_the_delay() {
        let mut intent = HoverIntent::new();
        intent.enter(0, 3, at(0));

        assert_eq!(
            intent.tick(at(HOVER_OPEN_DELAY.as_millis() as u64 - 1)),
            HoverTick::Nothing
        );
        assert_eq!(
            intent.tick(at(HOVER_OPEN_DELAY.as_millis() as u64)),
            HoverTick::Open(0, 3)
        );
    }

    #[test]
    fn hovering_a_second_row_replaces_the_pending_open() {
        let mut intent = HoverIntent::new();
        intent.enter(0, 3, at(0));
        intent.enter(0, 4, at(40));

        assert_eq!(intent.pending(), Some((0, 4)));
        assert_eq!(
            intent.tick(at(HOVER_OPEN_DELAY.as_millis() as u64)),
            HoverTick::Nothing
        );
        assert_eq!(
            intent.tick(at(40 + HOVER_OPEN_DELAY.as_millis() as u64)),
            HoverTick::Open(0, 4)
        );
    }

    #[test]
    fn hovering_the_row_whose_flyout_is_already_open_does_nothing() {
        let mut intent = HoverIntent::new();
        intent.mark_open(0, Some(3));
        intent.enter(0, 3, at(0));

        assert_eq!(intent.pending(), None);
        assert_eq!(intent.tick(at(1000)), HoverTick::Nothing);
    }

    #[test]
    fn leaving_keeps_the_flyout_through_the_grace_period() {
        let mut intent = HoverIntent::new();
        intent.mark_open(1, Some(0));
        intent.leave(1, at(0));

        assert_eq!(
            intent.tick(at(HOVER_CLOSE_GRACE.as_millis() as u64 - 1)),
            HoverTick::Nothing
        );
    }

    #[test]
    fn leaving_closes_the_level_after_the_grace_period() {
        let mut intent = HoverIntent::new();
        intent.mark_open(1, Some(0));
        intent.leave(1, at(0));

        assert_eq!(
            intent.tick(at(HOVER_CLOSE_GRACE.as_millis() as u64)),
            HoverTick::CloseLevel(1)
        );
    }

    #[test]
    fn travelling_toward_the_child_never_closes_it() {
        let row = Bounds {
            origin: point(px(100.0), px(100.0)),
            size: size(px(190.0), px(30.0)),
        };
        let flyout = Bounds {
            origin: point(px(300.0), px(90.0)),
            size: size(px(200.0), px(310.0)),
        };

        let mut intent = HoverIntent::new();
        intent.mark_open(0, Some(0));
        intent.leave(0, at(0));
        intent.pointer_moved(point(px(295.0), px(103.0)), row, flyout, FlyoutSide::Right);

        assert!(intent.in_corridor());
        assert_eq!(intent.tick(at(1000)), HoverTick::Nothing);
    }

    #[test]
    fn the_corridor_is_the_triangle_between_the_row_and_the_flyout() {
        let row = Bounds {
            origin: point(px(100.0), px(100.0)),
            size: size(px(190.0), px(30.0)),
        };
        let flyout = Bounds {
            origin: point(px(300.0), px(90.0)),
            size: size(px(200.0), px(310.0)),
        };

        // On the row, and inside the flyout.
        assert!(in_corridor(
            point(px(200.0), px(115.0)),
            row,
            flyout,
            FlyoutSide::Right
        ));
        assert!(in_corridor(
            point(px(400.0), px(200.0)),
            row,
            flyout,
            FlyoutSide::Right
        ));
        // In the triangle, on its way to the flyout.
        assert!(in_corridor(
            point(px(295.0), px(103.0)),
            row,
            flyout,
            FlyoutSide::Right
        ));
        // Below the triangle's lower edge.
        assert!(!in_corridor(
            point(px(295.0), px(125.0)),
            row,
            flyout,
            FlyoutSide::Right
        ));
        // Away from both, on the far side of the row.
        assert!(!in_corridor(
            point(px(60.0), px(115.0)),
            row,
            flyout,
            FlyoutSide::Right
        ));
    }

    #[test]
    fn the_corridor_mirrors_for_a_flyout_opening_left() {
        let row = Bounds {
            origin: point(px(400.0), px(100.0)),
            size: size(px(190.0), px(30.0)),
        };
        let flyout = Bounds {
            origin: point(px(190.0), px(90.0)),
            size: size(px(200.0), px(310.0)),
        };

        assert!(in_corridor(
            point(px(395.0), px(103.0)),
            row,
            flyout,
            FlyoutSide::Left
        ));
        assert!(!in_corridor(
            point(px(395.0), px(125.0)),
            row,
            flyout,
            FlyoutSide::Left
        ));
    }
}
