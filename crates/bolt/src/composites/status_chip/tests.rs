use std::cell::Cell;
use std::rc::Rc;

use gpui::{
    Context, IntoElement, Modifiers, ParentElement as _, Render, Styled as _, TestAppContext,
    Window, div,
};

use super::*;

#[test]
fn the_wash_needs_a_tone_and_replaces_the_island_fill() {
    let theme = gpui_component::Theme::default();
    let tone = gpui::red();
    assert_ne!(
        island_fill(&theme),
        tone.opacity(ShellMetrics::STATUS_CHIP_ALPHA)
    );

    assert_eq!(StatusChip::new("plain").fill(&theme), island_fill(&theme));
    assert_eq!(
        StatusChip::new("toned").tone(tone).fill(&theme),
        island_fill(&theme),
        "a tone alone colors the text, not the fill"
    );
    assert_eq!(
        StatusChip::new("washed").wash(true).fill(&theme),
        island_fill(&theme),
        "a wash without a tone keeps the island fill"
    );
    assert_eq!(
        StatusChip::new("washed").tone(tone).wash(true).fill(&theme),
        tone.opacity(ShellMetrics::STATUS_CHIP_ALPHA)
    );
}

struct Host {
    clicks: Rc<Cell<usize>>,
}

impl Render for Host {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let clicks = self.clicks.clone();
        let success = cx.theme().success;

        div()
            .size_full()
            .flex()
            .child(
                StatusChip::new("chip-connection")
                    .tone(success)
                    .wash(true)
                    .build(cx)
                    .child("local"),
            )
            .child(
                StatusChip::new("chip-tasks")
                    .clickable(true)
                    .build(cx)
                    .child("2 tasks")
                    .on_click(move |_, _, _| clicks.set(clicks.get() + 1)),
            )
    }
}

#[gpui::test]
fn chips_are_found_by_id_at_the_chip_height(cx: &mut TestAppContext) {
    cx.update(gpui_component::init);
    cx.update(|cx| {
        crate::theme::install(
            crate::brand::Brand::neutral(),
            crate::palette::PaletteMode::Dark,
            crate::density::Density::Default,
            cx,
        );
    });

    let clicks = Rc::new(Cell::new(0));
    let (_view, window) = cx.add_window_view({
        let clicks = clicks.clone();
        move |_, _| Host { clicks }
    });
    window.run_until_parked();

    let connection = window
        .debug_bounds("chip-connection")
        .expect("the chip is found by its id");
    let tasks = window.debug_bounds("chip-tasks").expect("chip laid out");
    let rem = window.update(|window, _| window.rem_size());
    let height = ShellMetrics::STATUS_CHIP_HEIGHT.to_pixels(rem);
    assert_eq!(connection.size.height, height);
    assert_eq!(tasks.size.height, height);

    window.simulate_click(tasks.center(), Modifiers::default());
    window.run_until_parked();
    assert_eq!(clicks.get(), 1);
}
