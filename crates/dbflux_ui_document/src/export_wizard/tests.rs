use gpui::{Pixels, px};

use super::SEGMENT_SIZE_INPUT_WIDTH;

fn rem_size(size: f32) -> Pixels {
    px(size) // guardrail-allow: a rem size under test, not layout spacing
}

#[test]
fn text_input_width_follows_the_ui_font_size() {
    assert_eq!(
        SEGMENT_SIZE_INPUT_WIDTH.to_pixels(rem_size(16.0)),
        rem_size(120.0)
    );
    assert_eq!(
        SEGMENT_SIZE_INPUT_WIDTH.to_pixels(rem_size(32.0)),
        rem_size(240.0)
    );
}
