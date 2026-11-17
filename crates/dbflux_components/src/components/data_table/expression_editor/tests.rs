use super::suggestion_step;
use gpui::Keystroke;

fn step(keystroke: &str) -> Option<isize> {
    suggestion_step(&Keystroke::parse(keystroke).expect("valid keystroke"))
}

#[test]
fn plain_arrows_step_the_suggestions() {
    assert_eq!(step("down"), Some(1));
    assert_eq!(step("up"), Some(-1));
    assert_eq!(step("enter"), None);
}

#[test]
fn a_shifted_arrow_is_left_to_the_input() {
    assert_eq!(step("shift-down"), None);
    assert_eq!(step("shift-up"), None);
}

#[test]
fn an_arrow_with_any_modifier_is_left_to_the_input() {
    for keystroke in [
        "ctrl-down",
        "ctrl-up",
        "alt-down",
        "alt-up",
        "cmd-down",
        "cmd-up",
        "ctrl-shift-down",
    ] {
        assert_eq!(step(keystroke), None, "{keystroke}");
    }
}
