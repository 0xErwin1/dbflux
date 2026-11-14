use gpui::prelude::*;
use gpui::{
    Context, Focusable as _, HighlightStyle, IntoElement, Render, TestAppContext, Window, div, px,
};

use super::{TextDecoration, decorate_input, styled_ranges};
use crate::controls::{Input, InputState};

struct Field {
    state: gpui::Entity<InputState>,
}

impl Render for Field {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
            .child(Input::new(&self.state).id("decorated-field"))
    }
}

fn wash() -> HighlightStyle {
    HighlightStyle {
        color: Some(gpui::red()),
        background_color: Some(gpui::blue()),
        underline: Some(gpui::UnderlineStyle {
            thickness: px(1.),
            color: Some(gpui::green()),
            wavy: false,
        }),
        ..Default::default()
    }
}

#[test]
fn styled_ranges_gives_every_range_the_style() {
    assert_eq!(
        styled_ranges([0..2, 5..9], wash()),
        vec![
            TextDecoration::new(0..2, wash()),
            TextDecoration::new(5..9, wash())
        ]
    );
    assert!(styled_ranges(std::iter::empty(), wash()).is_empty());
}

#[gpui::test]
fn decorations_follow_typing_in_a_focused_input(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    let (view, visual) = cx.add_window_view(|window, cx| {
        let state = cx.new(|cx| InputState::new(window, cx).default_value("a {{x}} b"));
        Field { state }
    });
    visual.run_until_parked();

    let state = visual.update(|_, cx| view.read(cx).state.clone());
    let collection = visual.update(|window, cx| {
        state.update(cx, |state, cx| {
            state.focus(window, cx);
            state.set_selected_range(0..0, cx);
        });
        decorate_input(&state, styled_ranges(std::iter::once(2..7), wash()), cx)
    });
    visual.run_until_parked();
    assert!(visual.update(|window, cx| state.read(cx).focus_handle(cx).is_focused(window)));

    visual.simulate_input("XY");
    visual.run_until_parked();
    assert_eq!(visual.update(|_, cx| collection.get_ranges(cx)), vec![4..9]);

    visual.update(|_, cx| collection.set(styled_ranges(std::iter::once(0..1), wash()), cx));
    assert_eq!(visual.update(|_, cx| collection.get_ranges(cx)), vec![0..1]);
    visual.update(|_, cx| collection.clear(cx));
    assert!(visual.update(|_, cx| collection.get_ranges(cx)).is_empty());
}
