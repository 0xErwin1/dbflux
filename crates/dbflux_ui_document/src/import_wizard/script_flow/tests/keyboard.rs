use dbflux_transfer::sql_script::OnError;

use super::support::{FakeKind, SCRIPT, Scratch, ScriptFake, open_wizard, pick, read};

fn press(window: &mut gpui::VisualTestContext, keystrokes: &str) {
    for keystroke in keystrokes.split(' ') {
        window.simulate_keystrokes(keystroke);
        window.update(|window, _| window.refresh());
        window.run_until_parked();
    }
}

/// The cursor rests on "On error" first: L picks the other choice, as the
/// mouse does, and J moves to the transaction checkbox, which Enter toggles.
#[gpui::test]
fn the_cursor_reaches_on_error_and_the_transaction(cx: &mut gpui::TestAppContext) {
    let scratch = Scratch::new("keys");
    let mut opened = open_wizard(cx, ScriptFake::new(FakeKind::SQL), false);
    pick(&mut opened, scratch.write("shop.sql", SCRIPT.as_bytes()));
    opened.window.update(|window, cx| {
        opened
            .wizard
            .update(cx, |this, cx| this.focus_handle.focus(window, cx));
    });

    press(opened.window, "l");
    assert_eq!(
        read(&mut opened, |wizard| wizard
            .script
            .as_ref()
            .map(|flow| flow.options.on_error)),
        Some(OnError::Continue)
    );
    assert_eq!(
        read(&mut opened, |wizard| wizard.configure_fields().len()),
        1,
        "with Continue the transaction is off, so the cursor has one stop"
    );

    press(opened.window, "h");
    assert_eq!(
        read(&mut opened, |wizard| wizard
            .script
            .as_ref()
            .map(|flow| flow.options.on_error)),
        Some(OnError::Stop)
    );

    press(opened.window, "j enter");
    assert_eq!(
        read(&mut opened, |wizard| wizard
            .script
            .as_ref()
            .map(|flow| flow.options.transaction)),
        Some(false)
    );
}
