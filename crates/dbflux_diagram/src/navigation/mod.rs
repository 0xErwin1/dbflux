//! Keyboard selection between nodes.
//!
//! Selection moves to the nearest node in the direction pressed, and wraps to the
//! opposite end of the reading order when there is none, so repeated presses
//! always reach every node.

use std::cmp::Ordering;

use crate::routing::NodeBounds;

/// Distance a node must clear the current one by to count as being in a
/// direction, so nodes on the same row or column are not picked sideways.
const DIRECTION_THRESHOLD: f32 = 20.0;

/// Direction of a selection move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
    Up,
    Down,
}

/// Node keys in reading order: top to bottom, left to right within a row.
pub fn spatial_order<K: Copy>(nodes: &[(K, NodeBounds)]) -> Vec<K> {
    let mut sorted: Vec<&(K, NodeBounds)> = nodes.iter().collect();
    sorted.sort_by(|(_, a), (_, b)| {
        a.y.partial_cmp(&b.y)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.x.partial_cmp(&b.x).unwrap_or(Ordering::Equal))
    });
    sorted.into_iter().map(|(key, _)| *key).collect()
}

/// The node to select when `direction` is pressed with `current` selected.
///
/// The nearest node past the threshold wins. With none, the selection wraps to
/// the far end of the reading order: the last node for Left and Up, the first for
/// Right and Down. `None` when `current` is not among `nodes`.
pub fn next_in_direction<K: Copy + PartialEq>(
    nodes: &[(K, NodeBounds)],
    current: K,
    direction: Direction,
) -> Option<K> {
    let sorted = spatial_order(nodes);
    sorted.iter().position(|key| *key == current)?;

    let bounds_of = |key: K| nodes.iter().find(|(k, _)| *k == key).map(|(_, b)| *b);
    let current_bounds = bounds_of(current)?;

    let (coordinate, towards_lower): (fn(&NodeBounds) -> f32, bool) = match direction {
        Direction::Left => (|bounds| bounds.x, true),
        Direction::Right => (|bounds| bounds.x, false),
        Direction::Up => (|bounds| bounds.y, true),
        Direction::Down => (|bounds| bounds.y, false),
    };
    let origin = coordinate(&current_bounds);

    let candidates = sorted.iter().copied().filter(|&key| {
        key != current
            && bounds_of(key).is_some_and(|bounds| {
                if towards_lower {
                    coordinate(&bounds) < origin - DIRECTION_THRESHOLD
                } else {
                    coordinate(&bounds) > origin + DIRECTION_THRESHOLD
                }
            })
    });
    let order = |a: &K, b: &K| {
        let a = bounds_of(*a)
            .map(|bounds| coordinate(&bounds))
            .unwrap_or_default();
        let b = bounds_of(*b)
            .map(|bounds| coordinate(&bounds))
            .unwrap_or_default();
        a.partial_cmp(&b).unwrap_or(Ordering::Equal)
    };

    let nearest = if towards_lower {
        candidates.max_by(order)
    } else {
        candidates.min_by(order)
    };

    nearest.or_else(|| {
        if towards_lower {
            sorted.last().copied()
        } else {
            sorted.first().copied()
        }
    })
}

#[cfg(test)]
mod tests;
