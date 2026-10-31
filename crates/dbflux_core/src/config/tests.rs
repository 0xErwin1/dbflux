use super::{GeneralSettings, InterfaceSizeChange};

fn sizes(settings: &GeneralSettings) -> (f32, f32, f32) {
    (
        settings.ui_font_size,
        settings.editor_font_size,
        settings.grid_font_size,
    )
}

#[test]
fn interface_size_steps_move_every_font_size_by_one_pixel() {
    let mut settings = GeneralSettings::default();

    settings.change_interface_size(InterfaceSizeChange::Increase);
    assert_eq!(sizes(&settings), (14.0, 14.0, 13.5));

    settings.change_interface_size(InterfaceSizeChange::Decrease);
    settings.change_interface_size(InterfaceSizeChange::Decrease);
    assert_eq!(sizes(&settings), (12.0, 12.0, 11.5));
}

#[test]
fn interface_size_steps_stop_each_size_at_its_limit() {
    let mut settings = GeneralSettings {
        ui_font_size: 32.0,
        editor_font_size: 31.5,
        grid_font_size: 20.0,
        ..GeneralSettings::default()
    };

    settings.change_interface_size(InterfaceSizeChange::Increase);
    assert_eq!(sizes(&settings), (32.0, 32.0, 21.0));

    let mut settings = GeneralSettings {
        ui_font_size: 8.0,
        editor_font_size: 8.5,
        grid_font_size: 10.0,
        ..GeneralSettings::default()
    };

    settings.change_interface_size(InterfaceSizeChange::Decrease);
    assert_eq!(sizes(&settings), (8.0, 8.0, 9.0));
}

#[test]
fn interface_size_reset_restores_the_default_sizes_only() {
    let mut settings = GeneralSettings {
        ui_font_size: 20.0,
        editor_font_size: 9.0,
        grid_font_size: f32::NAN,
        editor_font_family: Some("Fira Code".to_string()),
        ..GeneralSettings::default()
    };

    settings.change_interface_size(InterfaceSizeChange::Reset);
    assert_eq!(sizes(&settings), (13.0, 13.0, 12.5));
    assert_eq!(settings.editor_font_family.as_deref(), Some("Fira Code"));
}

#[test]
fn interface_size_step_replaces_a_non_finite_size_with_its_default() {
    let mut settings = GeneralSettings {
        grid_font_size: f32::NAN,
        ..GeneralSettings::default()
    };

    settings.change_interface_size(InterfaceSizeChange::Increase);
    assert_eq!(settings.grid_font_size, 12.5);
}
