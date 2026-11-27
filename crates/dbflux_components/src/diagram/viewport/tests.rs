use super::*;

fn viewport(width: f32, height: f32) -> Size<Pixels> {
    Size {
        width: px(width),
        height: px(height),
    }
}

fn point(x: f32, y: f32) -> Point<Pixels> {
    Point::new(px(x), px(y))
}

fn f(pixels: Pixels) -> f32 {
    pixels.into()
}

#[test]
fn zoom_steps_stay_inside_the_limits() {
    assert_eq!(zoom_in(1.0), 1.25);
    assert_eq!(zoom_out(1.0), 0.8);
    assert_eq!(zoom_in(3.9), ZOOM_MAX);
    assert_eq!(zoom_out(0.26), ZOOM_MIN);
    assert!(!can_zoom_in(ZOOM_MAX));
    assert!(can_zoom_in(1.0));
    assert!(!can_zoom_out(ZOOM_MIN));
    assert!(can_zoom_out(1.0));
}

#[test]
fn wheel_zoom_keeps_the_point_under_the_cursor_fixed() {
    let mouse = point(300.0, 200.0);
    let pan = point(40.0, -20.0);
    let zoom = 1.0;

    let (new_zoom, new_pan) = wheel_zoom(mouse, pan, zoom, px(1.0)).expect("zoom changes");

    assert!((new_zoom - 1.1).abs() < 1e-6);
    let graph_before = ((300.0 - 40.0) / zoom, (200.0 + 20.0) / zoom);
    let graph_after = (
        (300.0 - f(new_pan.x)) / new_zoom,
        (200.0 - f(new_pan.y)) / new_zoom,
    );
    assert!((graph_before.0 - graph_after.0).abs() < 1e-3);
    assert!((graph_before.1 - graph_after.1).abs() < 1e-3);
}

#[test]
fn wheel_zoom_out_uses_the_mirror_factor_and_stops_at_the_limit() {
    let (zoom, _) = wheel_zoom(point(0.0, 0.0), point(0.0, 0.0), 1.0, px(-1.0)).expect("changes");
    assert!((zoom - 0.9).abs() < 1e-6);

    assert!(wheel_zoom(point(0.0, 0.0), point(0.0, 0.0), ZOOM_MIN, px(-1.0)).is_none());
    assert!(wheel_zoom(point(0.0, 0.0), point(0.0, 0.0), ZOOM_MAX, px(1.0)).is_none());
}

#[test]
fn fit_keeps_the_leftmost_node_inside_the_margin() {
    let (zoom, pan) =
        fit_diagram((0.0, 0.0, 900.0, 400.0), viewport(1000.0, 600.0), 1.0).expect("fits");

    assert_eq!(zoom, 1.0);
    assert_eq!(f(pan.x), FIT_MARGIN);
    assert_eq!(f(pan.y), FIT_MARGIN);
}

#[test]
fn fit_shrinks_a_wide_diagram_and_anchors_its_left_edge() {
    let (zoom, pan) =
        fit_diagram((-200.0, 0.0, 1800.0, 400.0), viewport(1000.0, 600.0), 1.0).expect("fits");

    assert_eq!(zoom, 904.0 / 2000.0);
    assert!((f(pan.x) + (-200.0) * zoom - FIT_MARGIN).abs() < 1e-3);
}

#[test]
fn fit_never_zooms_past_the_cap_or_below_the_floor() {
    let (small, _) =
        fit_diagram((0.0, 0.0, 10.0, 10.0), viewport(1000.0, 600.0), 1.5).expect("fits");
    assert_eq!(small, 1.5);

    let (huge, _) = fit_diagram(
        (0.0, 0.0, 100_000.0, 100_000.0),
        viewport(1000.0, 600.0),
        1.5,
    )
    .expect("fits");
    assert_eq!(huge, ZOOM_MIN);
}

#[test]
fn fit_waits_for_a_measured_viewport_and_a_real_box() {
    assert!(fit_diagram((0.0, 0.0, 100.0, 100.0), viewport(0.0, 0.0), 1.0).is_none());
    assert!(fit_diagram((10.0, 0.0, 0.0, 100.0), viewport(800.0, 600.0), 1.0).is_none());
}

#[test]
fn content_bounds_covers_every_rectangle() {
    let bounds = content_bounds([
        (0.0, 10.0, 100.0, 50.0),
        (-20.0, 0.0, 10.0, 10.0),
        (200.0, 80.0, 40.0, 40.0),
    ]);

    assert_eq!(bounds, Some((-20.0, 0.0, 240.0, 120.0)));
    assert_eq!(content_bounds([]), None);
}

#[test]
fn center_on_puts_the_point_in_the_middle_of_the_viewport() {
    let pan = center_on(500.0, 100.0, viewport(1000.0, 400.0), 2.0);

    assert_eq!((f(pan.x), f(pan.y)), (500.0 - 1000.0, 200.0 - 200.0));
}

#[test]
fn center_on_assumes_a_default_viewport_until_one_is_measured() {
    let pan = center_on(0.0, 0.0, viewport(0.0, 0.0), 1.0);

    assert_eq!((f(pan.x), f(pan.y)), (400.0, 300.0));
}

#[test]
fn a_dragged_node_follows_the_pointer_in_graph_space() {
    let position = dragged_node_position(
        point(250.0, 180.0),
        point(10.0, 20.0),
        point(40.0, 20.0),
        2.0,
    );

    assert_eq!(position, (100.0, 70.0));
}

#[test]
fn viewport_local_position_subtracts_the_origin() {
    let local = viewport_local_position(point(120.0, 80.0), point(20.0, 30.0));

    assert_eq!((f(local.x), f(local.y)), (100.0, 50.0));
}

#[test]
fn snapping_lands_on_the_lattice() {
    assert_eq!(snap_to_lattice(0.0), 0.0);
    assert_eq!(snap_to_lattice(11.0), 0.0);
    assert_eq!(snap_to_lattice(13.0), GRID_LATTICE);
    assert_eq!(snap_to_lattice(-13.0), -GRID_LATTICE);
}

#[test]
fn pixel_alignment_rounds_the_translation_to_a_physical_pixel() {
    let aligned = pixel_aligned_pan(point(10.5, -3.25), point(0.0, 0.0), 1.0, 1.0);

    assert_eq!((f(aligned.x), f(aligned.y)), (11.0, -3.0));
}

#[test]
fn pixel_alignment_ignores_an_invalid_display_scale() {
    for scale in [0.0, -1.0, f32::NAN, f32::INFINITY] {
        let pan = point(10.4, 3.3);
        assert_eq!(pixel_aligned_pan(pan, point(0.2, 0.1), 1.5, scale), pan);
    }
}
