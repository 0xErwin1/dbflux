use super::*;

fn at(x: f32, y: f32) -> NodeBounds {
    NodeBounds {
        x,
        y,
        width: 100.0,
        height: 40.0,
    }
}

/// Keys 0..=3 laid out as
///
/// ```text
/// 0   1
/// 2   3
/// ```
fn grid() -> Vec<(usize, NodeBounds)> {
    vec![
        (3, at(300.0, 200.0)),
        (0, at(0.0, 0.0)),
        (2, at(0.0, 200.0)),
        (1, at(300.0, 0.0)),
    ]
}

#[test]
fn reading_order_is_top_to_bottom_then_left_to_right() {
    assert_eq!(spatial_order(&grid()), vec![0, 1, 2, 3]);
}

#[test]
fn each_direction_picks_the_nearest_node_that_way() {
    let nodes = grid();

    assert_eq!(next_in_direction(&nodes, 0, Direction::Right), Some(1));
    assert_eq!(next_in_direction(&nodes, 0, Direction::Down), Some(2));
    assert_eq!(next_in_direction(&nodes, 3, Direction::Left), Some(2));
    assert_eq!(next_in_direction(&nodes, 3, Direction::Up), Some(1));
}

#[test]
fn the_nearest_candidate_wins_over_a_farther_one() {
    let nodes = vec![(0, at(0.0, 0.0)), (1, at(250.0, 0.0)), (2, at(600.0, 0.0))];

    assert_eq!(next_in_direction(&nodes, 0, Direction::Right), Some(1));
    assert_eq!(next_in_direction(&nodes, 2, Direction::Left), Some(1));
}

#[test]
fn nodes_within_the_threshold_are_not_in_that_direction() {
    let nodes = vec![(0, at(0.0, 0.0)), (1, at(0.0, 100.0)), (2, at(10.0, 100.0))];

    // 2 is 10 px to the right of 1, inside the threshold, so Right wraps to the
    // first node instead of picking it.
    assert_eq!(next_in_direction(&nodes, 1, Direction::Right), Some(0));
}

#[test]
fn non_finite_coordinates_do_not_break_the_ordering() {
    let nodes = vec![
        (0, at(30.0, 0.0)),
        (1, at(20.0, f32::NAN)),
        (2, at(10.0, 1.0)),
    ];

    assert_eq!(spatial_order(&nodes).len(), 3);
    assert!(next_in_direction(&nodes, 0, Direction::Down).is_some());
}

#[test]
fn with_nothing_further_the_selection_wraps_to_the_far_end() {
    let nodes = grid();

    assert_eq!(next_in_direction(&nodes, 1, Direction::Right), Some(0));
    assert_eq!(next_in_direction(&nodes, 0, Direction::Left), Some(3));
    assert_eq!(next_in_direction(&nodes, 2, Direction::Down), Some(0));
    assert_eq!(next_in_direction(&nodes, 0, Direction::Up), Some(3));
}

#[test]
fn an_unknown_current_node_selects_nothing() {
    assert_eq!(next_in_direction(&grid(), 9, Direction::Right), None);
}
