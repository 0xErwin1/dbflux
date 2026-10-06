#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::grid::{display_duration, display_number};

#[test]
fn numbers_display_without_float_noise() {
    assert_eq!(display_number(0.1 + 0.2), "0.3");
    assert_eq!(display_number(50.5), "50.5");
    assert_eq!(display_number(3.0), "3");
    assert_eq!(display_number(-0.0), "0");
    assert_eq!(display_number(-1234.5678), "-1234.5678");
    assert_eq!(display_number(1.0 / 3.0), "0.333333333333333");
    assert_eq!(display_number(123_456_789_012.0), "123456789012");
}

#[test]
fn durations_display_as_elapsed_time() {
    assert_eq!(display_duration(0.5), "12:00:00");
    assert_eq!(display_duration(1.5), "36:00:00");
    assert_eq!(display_duration(-0.25), "-6:00:00");
}
