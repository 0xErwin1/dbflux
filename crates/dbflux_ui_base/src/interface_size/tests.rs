use dbflux_components::fonts;
use dbflux_core::{GeneralSettings, InterfaceSizeChange};
use dbflux_storage::bootstrap::StorageRuntime;
use gpui::{AppContext as _, Entity, TestAppContext};

use super::change_interface_size;
use crate::app_state_entity::{AppStateEntity, publish_font_settings};

fn app_state(cx: &mut TestAppContext) -> Entity<AppStateEntity> {
    cx.update(dbflux_components::theme::init);
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test storage setup")
        })
    });
    cx.update(|cx| publish_font_settings(&app_state, cx));
    app_state
}

fn saved_sizes(app_state: &Entity<AppStateEntity>, cx: &mut TestAppContext) -> (f64, f64, f64) {
    cx.update(|cx| {
        let dto = app_state
            .read(cx)
            .storage_runtime()
            .general_settings()
            .get()
            .expect("read saved settings")
            .expect("general settings row");
        (dto.ui_font_size, dto.editor_font_size, dto.grid_font_size)
    })
}

fn active_sizes(cx: &mut TestAppContext) -> (f32, f32, f32) {
    cx.update(|cx| {
        let current = fonts::current(cx);
        (current.ui_size, current.editor_size, current.grid_size)
    })
}

/// A step saves the new sizes, keeps them in the app state the Settings
/// window reads, and publishes them to the running fonts.
#[gpui::test]
fn a_step_saves_and_applies_every_font_size(cx: &mut TestAppContext) {
    let app_state = app_state(cx);

    let changed =
        cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Increase, cx));
    cx.run_until_parked();

    assert!(changed);
    assert_eq!(active_sizes(cx), (14.0, 14.0, 13.5));
    assert_eq!(saved_sizes(&app_state, cx), (14.0, 14.0, 13.5));
    cx.update(|cx| {
        let settings = app_state.read(cx).general_settings();
        assert_eq!(settings.ui_font_size, 14.0);
        assert_eq!(settings.editor_font_size, 14.0);
        assert_eq!(settings.grid_font_size, 13.5);
    });

    cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Decrease, cx));
    cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Decrease, cx));
    cx.run_until_parked();

    assert_eq!(active_sizes(cx), (12.0, 12.0, 11.5));
    assert_eq!(saved_sizes(&app_state, cx), (12.0, 12.0, 11.5));
}

/// Reset restores the defaults, and a change that moves nothing saves
/// nothing.
#[gpui::test]
fn reset_restores_the_defaults_and_a_no_op_saves_nothing(cx: &mut TestAppContext) {
    let app_state = app_state(cx);

    let changed = cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Reset, cx));
    assert!(!changed, "a reset at the defaults changes nothing");

    for _ in 0..3 {
        cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Increase, cx));
    }
    let changed = cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Reset, cx));
    cx.run_until_parked();

    assert!(changed);
    let defaults = (
        GeneralSettings::DEFAULT_UI_FONT_SIZE,
        GeneralSettings::DEFAULT_EDITOR_FONT_SIZE,
        GeneralSettings::DEFAULT_GRID_FONT_SIZE,
    );
    assert_eq!(active_sizes(cx), defaults);
    assert_eq!(saved_sizes(&app_state, cx), (13.0, 13.0, 12.5));
}

/// At the largest size every step up is a no-op.
#[gpui::test]
fn a_step_past_every_limit_changes_nothing(cx: &mut TestAppContext) {
    let app_state = app_state(cx);

    for _ in 0..40 {
        cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Increase, cx));
    }
    let changed =
        cx.update(|cx| change_interface_size(&app_state, InterfaceSizeChange::Increase, cx));
    cx.run_until_parked();

    assert!(!changed);
    assert_eq!(active_sizes(cx), (32.0, 32.0, 32.0));
    assert_eq!(saved_sizes(&app_state, cx), (32.0, 32.0, 32.0));
}

/// Holding the key applies every step at once and coalesces the writes, so
/// the disk sees the final size without one write per key repeat.
#[gpui::test]
fn repeated_steps_apply_at_once_and_save_once(cx: &mut TestAppContext) {
    let app_state = app_state(cx);

    cx.update(|cx| {
        for _ in 0..5 {
            change_interface_size(&app_state, InterfaceSizeChange::Increase, cx);
        }
    });

    cx.update(|cx| {
        let settings = app_state.read(cx).general_settings();
        assert_eq!(settings.ui_font_size, 18.0);
    });

    cx.run_until_parked();

    assert_eq!(active_sizes(cx), (18.0, 18.0, 17.5));
    assert_eq!(saved_sizes(&app_state, cx), (18.0, 18.0, 17.5));
    assert_eq!(
        cx.update(|cx| crate::settings_writer::writes_started(cx)),
        1
    );
}
