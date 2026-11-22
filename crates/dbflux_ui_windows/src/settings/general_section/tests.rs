use super::{FontFamilyField, FontFamilyOption, GeneralFormRow, GeneralPage, GeneralSection};
use dbflux_core::{AppStyle, ScrollbarVisibility, ThemeSetting};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext, Entity, TestAppContext, WindowOptions};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn open_section_window(
    cx: &mut TestAppContext,
    page: GeneralPage,
) -> (gpui::WindowHandle<GeneralSection>, Entity<ToastHost>) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    let toast_host = cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host: host.clone() });
        host
    });
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test app state")
        })
    });
    let window = cx
        .update(|cx| {
            cx.open_window(WindowOptions::default(), |window, cx| {
                cx.new(|cx| GeneralSection::new(app_state, page, window, cx))
            })
        })
        .expect("general settings window opens");

    (window, toast_host)
}

fn with_general_section(
    test: impl FnOnce(
        &mut GeneralSection,
        &Entity<ToastHost>,
        &mut gpui::Window,
        &mut gpui::Context<GeneralSection>,
    ),
) {
    with_page_section(GeneralPage::General, test);
}

fn with_appearance_section(
    test: impl FnOnce(
        &mut GeneralSection,
        &Entity<ToastHost>,
        &mut gpui::Window,
        &mut gpui::Context<GeneralSection>,
    ),
) {
    with_page_section(GeneralPage::Appearance, test);
}

fn with_page_section(
    page: GeneralPage,
    test: impl FnOnce(
        &mut GeneralSection,
        &Entity<ToastHost>,
        &mut gpui::Window,
        &mut gpui::Context<GeneralSection>,
    ),
) {
    let mut cx = TestAppContext::single();
    let (window, toast_host) = open_section_window(&mut cx, page);

    window
        .update(&mut cx, |section, window, cx| {
            test(section, &toast_host, window, cx)
        })
        .expect("general section updates");
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("window closes");
}

/// One step of a test on the General section: it runs, then the
/// background settings writer finishes before the next step.
type SectionPhase =
    Box<dyn FnOnce(&mut GeneralSection, &mut gpui::Window, &mut gpui::Context<GeneralSection>)>;

/// Runs each phase in order, letting the background settings writer
/// finish after every one, so a phase can check what an earlier save
/// wrote to the database.
fn with_general_section_phases(phases: Vec<SectionPhase>) {
    with_page_section_phases(GeneralPage::General, phases);
}

fn with_appearance_section_phases(phases: Vec<SectionPhase>) {
    with_page_section_phases(GeneralPage::Appearance, phases);
}

fn with_page_section_phases(page: GeneralPage, phases: Vec<SectionPhase>) {
    let mut cx = TestAppContext::single();
    let (window, _) = open_section_window(&mut cx, page);

    for phase in phases {
        window
            .update(&mut cx, |section, window, cx| phase(section, window, cx))
            .expect("general section updates");
        cx.run_until_parked();
    }

    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("window closes");
}

/// Runs `test`, lets the background settings writer finish, then runs
/// `after_saved` to check what reached the database.
fn with_general_section_saved(
    test: impl FnOnce(&mut GeneralSection, &mut gpui::Window, &mut gpui::Context<GeneralSection>)
    + 'static,
    after_saved: impl FnOnce(&mut GeneralSection, &mut gpui::Window, &mut gpui::Context<GeneralSection>)
    + 'static,
) {
    with_general_section_phases(vec![Box::new(test), Box::new(after_saved)]);
}

fn font_size_inputs(section: &GeneralSection, cx: &gpui::App) -> [String; 3] {
    [
        &section.input_ui_font_size,
        &section.input_editor_font_size,
        &section.input_grid_font_size,
    ]
    .map(|input| input.read(cx).value().to_string())
}

fn zoom_with_settings_open(
    before_zoom: impl FnOnce(&mut GeneralSection, &mut gpui::Window, &mut gpui::Context<GeneralSection>),
    after_zoom: impl FnOnce(&mut GeneralSection, &mut gpui::Context<GeneralSection>),
) {
    let mut cx = TestAppContext::single();
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test app state")
        })
    });
    let window = cx
        .update(|cx| {
            cx.open_window(WindowOptions::default(), |window, cx| {
                cx.new(|cx| {
                    GeneralSection::new(app_state.clone(), GeneralPage::Appearance, window, cx)
                })
            })
        })
        .expect("general settings window opens");

    window
        .update(&mut cx, |section, window, cx| {
            before_zoom(section, window, cx)
        })
        .expect("general section updates before the zoom");
    let changed = cx.update(|cx| {
        dbflux_ui_base::interface_size::change_interface_size(
            &app_state,
            dbflux_core::InterfaceSizeChange::Increase,
            cx,
        )
    });
    assert!(changed, "the zoom changes the saved sizes");
    window
        .update(&mut cx, |section, _, cx| after_zoom(section, cx))
        .expect("general section updates after the zoom");
    window
        .update(&mut cx, |_, window, _| window.remove_window())
        .expect("window closes");
}

#[test]
fn zoom_while_settings_is_open_shows_the_new_sizes_without_unsaved_changes() {
    zoom_with_settings_open(
        |_, _, _| {},
        |section, cx| {
            assert_eq!(font_size_inputs(section, cx), ["14", "14", "13.5"]);
            assert_eq!(section.general_change_count(cx), 0);
        },
    );
}

#[test]
fn zoom_while_settings_is_open_keeps_a_font_size_the_user_is_editing() {
    zoom_with_settings_open(
        |section, window, cx| {
            section
                .input_ui_font_size
                .update(cx, |input, cx| input.set_value("20", window, cx));
        },
        |section, cx| {
            assert_eq!(font_size_inputs(section, cx), ["20", "14", "13.5"]);
            assert_eq!(section.general_change_count(cx), 1);
        },
    );
}

fn stored_editor_row_limit(section: &GeneralSection, cx: &gpui::App) -> Option<i64> {
    section
        .app_state
        .read(cx)
        .storage_runtime()
        .general_settings()
        .get()
        .expect("stored general settings readable")
        .map(|settings| settings.editor_row_limit)
}

#[test]
fn parse_editor_row_limit_accepts_only_positive_whole_numbers_that_fit_storage() {
    assert_eq!(GeneralSection::parse_editor_row_limit("5000"), Some(5_000));
    assert_eq!(GeneralSection::parse_editor_row_limit(" 42 "), Some(42));
    assert_eq!(GeneralSection::parse_editor_row_limit("1"), Some(1));
    assert_eq!(
        GeneralSection::parse_editor_row_limit("9223372036854775807"),
        usize::try_from(i64::MAX).ok()
    );

    for rejected in [
        "0",
        "-1",
        "",
        "   ",
        "garbage",
        "1.5",
        "10k",
        "9223372036854775808",
    ] {
        assert_eq!(
            GeneralSection::parse_editor_row_limit(rejected),
            None,
            "{rejected:?} must be rejected"
        );
    }
}

#[test]
fn editor_row_limit_input_shows_the_saved_value_and_is_keyboard_reachable() {
    with_general_section(|section, _, window, cx| {
        assert_eq!(
            section.input_editor_row_limit.read(cx).value().as_ref(),
            "10000"
        );

        let index = section
            .gen_form_rows()
            .iter()
            .position(|row| *row == GeneralFormRow::EditorRowLimit)
            .expect("editor row limit row is navigable");
        for _ in 0..index {
            section.gen_move_down();
        }
        assert_eq!(section.gen_form_cursor, index);

        section.gen_activate_current_field(window, cx);
        assert!(section.gen_editing_field);
    });
}

#[test]
fn valid_editor_row_limit_marks_dirty_and_saves() {
    with_general_section_saved(
        |section, window, cx| {
            section
                .input_editor_row_limit
                .update(cx, |input, cx| input.set_value("5000", window, cx));
            assert!(section.has_unsaved_general_changes(cx));

            section.save_general_settings(window, cx);

            assert_eq!(
                section
                    .app_state
                    .read(cx)
                    .general_settings()
                    .editor_row_limit,
                5_000
            );
        },
        |section, _window, cx| {
            assert_eq!(stored_editor_row_limit(section, cx), Some(5_000));
            assert!(!section.has_unsaved_general_changes(cx));
        },
    );
}

/// Picking a leader in the Vim leader dropdown is an unsaved change until
/// Save, which stores it and hands it to open editors.
#[test]
fn choosing_a_vim_leader_marks_dirty_and_saves() {
    with_general_section_saved(
        |section, window, cx| {
            assert_eq!(section.gen_settings.vim_leader, "space");

            assert_eq!(
                section
                    .dropdown_vim_leader
                    .read(cx)
                    .selected_label()
                    .map(|label| label.to_string()),
                Some(dbflux_i18n::t!("settings.general.vim_leader.option.space")),
                "the dropdown shows the saved leader"
            );

            section.gen_settings.vim_leader = GeneralSection::vim_leader_for_index(1).to_string();
            assert_eq!(section.gen_settings.vim_leader, ",");
            assert_eq!(section.general_change_count(cx), 1);

            section.save_general_settings(window, cx);

            assert_eq!(
                section.app_state.read(cx).general_settings().vim_leader,
                ","
            );
        },
        |section, _window, cx| {
            let stored = section
                .app_state
                .read(cx)
                .storage_runtime()
                .general_settings()
                .get()
                .expect("stored general settings readable")
                .map(|settings| settings.vim_leader);
            assert_eq!(stored.as_deref(), Some(","));
            assert!(!section.has_unsaved_general_changes(cx));
        },
    );
}

#[test]
fn vim_leader_choices_round_trip_through_their_dropdown_index() {
    for (index, stored) in ["space", ",", "\\"].into_iter().enumerate() {
        assert_eq!(GeneralSection::vim_leader_for_index(index), stored);
        assert_eq!(GeneralSection::vim_leader_index(stored), Some(index));
    }

    assert_eq!(GeneralSection::vim_leader_index("ctrl+k"), None);
    assert_eq!(GeneralSection::vim_leader_for_index(9), "space");
}

#[test]
fn toast_timeout_choices_round_trip_through_their_dropdown_index() {
    for (index, stored) in [4u32, 8, 15, 0].into_iter().enumerate() {
        assert_eq!(GeneralSection::toast_timeout_for_index(index), stored);
        assert_eq!(GeneralSection::toast_timeout_index(stored), Some(index));
    }

    assert_eq!(GeneralSection::toast_timeout_index(30), None);
    assert_eq!(
        GeneralSection::toast_timeout_for_index(99),
        dbflux_core::GeneralSettings::DEFAULT_TOAST_AUTO_DISMISS_SECS
    );
}

/// Picking a toast timeout is an unsaved change until Save, which stores
/// it. `0` means every toast waits for the user.
#[test]
fn choosing_a_toast_timeout_marks_dirty_and_saves() {
    with_general_section_saved(
        |section, window, cx| {
            assert_eq!(section.gen_settings.toast_auto_dismiss_secs, 8);

            section.gen_settings.toast_auto_dismiss_secs =
                GeneralSection::toast_timeout_for_index(3);
            assert_eq!(section.general_change_count(cx), 1);

            section.save_general_settings(window, cx);

            assert_eq!(
                section
                    .app_state
                    .read(cx)
                    .general_settings()
                    .toast_auto_dismiss_secs,
                0
            );
        },
        |section, _window, cx| {
            let stored = section
                .app_state
                .read(cx)
                .storage_runtime()
                .general_settings()
                .get()
                .expect("stored general settings readable")
                .map(|settings| settings.toast_auto_dismiss_secs);
            assert_eq!(stored, Some(0));
            assert!(!section.has_unsaved_general_changes(cx));
        },
    );
}

#[test]
fn toast_timeout_copy_resolves_in_every_locale() {
    for key in [
        "settings.general.notifications.group",
        "settings.general.toast_timeout.label",
        "settings.general.toast_timeout.hint",
        "settings.general.toast_timeout.option.4s",
        "settings.general.toast_timeout.option.8s",
        "settings.general.toast_timeout.option.15s",
        "settings.general.toast_timeout.option.never",
    ] {
        let english = dbflux_i18n::t!(key, locale = "en");
        assert!(!english.is_empty(), "{key} resolved empty for en");
        assert_ne!(english, format!("en.{key}"), "{key} missing in en");

        // A missing key falls back to English, so a translated catalog must
        // give a different value.
        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(!value.is_empty(), "{key} resolved empty for {locale}");
            assert_ne!(value, english, "{key} missing in {locale}");
        }
    }
}

#[test]
fn unsaved_change_count_counts_each_changed_setting() {
    with_general_section(|section, _, window, cx| {
        assert_eq!(section.general_change_count(cx), 0);

        section.gen_settings.vim_mode = !section.gen_settings.vim_mode;
        section.gen_settings.toast_auto_dismiss_secs = 0;
        section
            .input_max_history
            .update(cx, |input, cx| input.set_value("42", window, cx));

        assert_eq!(section.general_change_count(cx), 3);
        assert!(section.has_unsaved_general_changes(cx));
    });
}

#[test]
fn invalid_editor_row_limit_shows_an_error_and_saves_nothing() {
    with_general_section(|section, toast_host, window, cx| {
        let stored_before = stored_editor_row_limit(section, cx);

        for value in ["0", "garbage", "", "-1", "9223372036854775808"] {
            section
                .input_editor_row_limit
                .update(cx, |input, cx| input.set_value(value, window, cx));

            section.save_general_settings(window, cx);

            assert_eq!(
                section.gen_field_error,
                Some((
                    GeneralFormRow::EditorRowLimit,
                    dbflux_i18n::t!("settings.general.editor_row_limit.error").to_string()
                )),
                "{value:?} must show the validation error under the field"
            );
            assert_eq!(
                toast_host.read(cx).last_toast_title(),
                None,
                "a form validation error stays in the settings window"
            );
            assert_eq!(section.gen_settings.editor_row_limit, 10_000);
            assert_eq!(
                section
                    .app_state
                    .read(cx)
                    .general_settings()
                    .editor_row_limit,
                10_000
            );
            assert_eq!(
                stored_editor_row_limit(section, cx),
                stored_before,
                "{value:?} must not change persisted settings"
            );
        }
    });
}

#[test]
fn editor_row_limit_copy_resolves_in_every_locale() {
    for key in [
        "settings.general.editor_row_limit.label",
        "settings.general.editor_row_limit.error",
    ] {
        for locale in ["en", "es", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(!value.is_empty(), "{key} resolved empty for {locale}");
            assert_ne!(
                value,
                format!("{locale}.{key}"),
                "{key} missing in {locale}"
            );
        }
    }
}

fn stored_general_settings(section: &GeneralSection, cx: &gpui::App) -> Option<(f64, f64, f64)> {
    section
        .app_state
        .read(cx)
        .storage_runtime()
        .general_settings()
        .get()
        .expect("stored general settings readable")
        .map(|settings| {
            (
                settings.ui_font_size,
                settings.editor_font_size,
                settings.grid_font_size,
            )
        })
}

fn installed(names: &[&str]) -> Vec<gpui::SharedString> {
    names
        .iter()
        .map(|name| gpui::SharedString::from(*name))
        .collect()
}

#[test]
fn parse_font_size_accepts_decimals_within_the_supported_range() {
    assert_eq!(GeneralSection::parse_font_size("12.5"), Some(12.5));
    assert_eq!(GeneralSection::parse_font_size(" 14 "), Some(14.0));
    assert_eq!(GeneralSection::parse_font_size("8"), Some(8.0));
    assert_eq!(GeneralSection::parse_font_size("32"), Some(32.0));

    for rejected in ["7.9", "32.5", "0", "-13", "", "abc", "NaN", "inf", "13px"] {
        assert_eq!(
            GeneralSection::parse_font_size(rejected),
            None,
            "{rejected:?} must be rejected"
        );
    }
}

#[test]
fn font_family_options_start_with_the_default_that_stores_none() {
    let options = GeneralSection::font_family_options(
        FontFamilyField::Editor,
        None,
        &installed(&["Fira Code", "Inter"]),
    );

    assert_eq!(
        options,
        vec![
            FontFamilyOption {
                label: "Default (JetBrains Mono)".into(),
                family: None,
            },
            FontFamilyOption {
                label: "Fira Code".into(),
                family: Some("Fira Code".into()),
            },
            FontFamilyOption {
                label: "Inter".into(),
                family: Some("Inter".into()),
            },
        ]
    );
    assert_eq!(GeneralSection::font_family_index(&options, None), 0);
    assert_eq!(
        GeneralSection::font_family_index(&options, Some("Inter")),
        2
    );

    let grid = GeneralSection::font_family_options(FontFamilyField::Grid, None, &[]);
    assert_eq!(grid[0].label.as_ref(), "Same as editor");
    assert_eq!(grid[0].family, None);
}

#[test]
fn a_saved_family_that_is_not_installed_stays_selectable() {
    let options = GeneralSection::font_family_options(
        FontFamilyField::Ui,
        Some("Missing Sans"),
        &installed(&["Inter"]),
    );

    assert_eq!(options[0].label.as_ref(), "Default (Archivo)");
    assert_eq!(
        options[1],
        FontFamilyOption {
            label: "Missing Sans (not installed)".into(),
            family: Some("Missing Sans".into()),
        }
    );
    assert_eq!(
        GeneralSection::font_family_index(&options, Some("Missing Sans")),
        1
    );

    let installed_saved = GeneralSection::font_family_options(
        FontFamilyField::Ui,
        Some("Inter"),
        &installed(&["Inter"]),
    );
    assert_eq!(
        installed_saved.len(),
        2,
        "no extra entry for an installed family"
    );
}

#[test]
fn font_rows_follow_the_language_row_and_focus_their_controls() {
    with_appearance_section(|section, _, window, cx| {
        let rows = section.gen_form_rows();
        let language = rows
            .iter()
            .position(|row| *row == GeneralFormRow::Language)
            .expect("language row");

        assert_eq!(
            rows[language + 1..language + 7],
            [
                GeneralFormRow::UiFontFamily,
                GeneralFormRow::UiFontSize,
                GeneralFormRow::EditorFontFamily,
                GeneralFormRow::EditorFontSize,
                GeneralFormRow::GridFontFamily,
                GeneralFormRow::GridFontSize,
            ]
        );

        for _ in 0..language + 2 {
            section.gen_move_down();
        }
        section.gen_activate_current_field(window, cx);
        assert!(
            section.gen_editing_field,
            "the UI font size input takes focus"
        );
    });
}

#[test]
fn font_settings_mark_dirty_and_save() {
    with_appearance_section_phases(vec![
        Box::new(|section, window, cx| {
            assert_eq!(
                section.input_grid_font_size.read(cx).value().as_ref(),
                "12.5"
            );

            section
                .input_ui_font_size
                .update(cx, |input, cx| input.set_value("16", window, cx));
            section
                .input_grid_font_size
                .update(cx, |input, cx| input.set_value("14.5", window, cx));
            section.set_font_family(FontFamilyField::Editor, Some("Fira Code".into()));
            assert_eq!(section.general_change_count(cx), 3);

            section.save_general_settings(window, cx);

            let saved = section.app_state.read(cx).general_settings().clone();
            assert_eq!(saved.ui_font_size, 16.0);
            assert_eq!(saved.editor_font_size, 13.0);
            assert_eq!(saved.grid_font_size, 14.5);
            assert_eq!(saved.editor_font_family.as_deref(), Some("Fira Code"));
            assert_eq!(saved.ui_font_family, None);
        }),
        Box::new(|section, window, cx| {
            assert_eq!(
                stored_general_settings(section, cx),
                Some((16.0, 13.0, 14.5))
            );
            assert!(!section.has_unsaved_general_changes(cx));

            section.set_font_family(FontFamilyField::Editor, None);
            assert_eq!(section.general_change_count(cx), 1);
            section.save_general_settings(window, cx);
            assert_eq!(
                section
                    .app_state
                    .read(cx)
                    .general_settings()
                    .editor_font_family,
                None,
                "the default entry clears the family"
            );
        }),
        Box::new(|section, _window, cx| {
            let stored_family = section
                .app_state
                .read(cx)
                .storage_runtime()
                .general_settings()
                .get()
                .expect("stored general settings readable")
                .and_then(|settings| settings.editor_font_family);
            assert_eq!(
                stored_family, None,
                "the cleared family reaches the database"
            );
        }),
    ]);
}

#[test]
fn invalid_font_size_shows_an_error_and_saves_nothing() {
    with_appearance_section(|section, toast_host, window, cx| {
        let stored_before = stored_general_settings(section, cx);

        for (input, key) in [
            (
                section.input_ui_font_size.clone(),
                "settings.general.ui_font_size.error",
            ),
            (
                section.input_editor_font_size.clone(),
                "settings.general.editor_font_size.error",
            ),
            (
                section.input_grid_font_size.clone(),
                "settings.general.grid_font_size.error",
            ),
        ] {
            let original = input.read(cx).value().to_string();
            input.update(cx, |input, cx| input.set_value("40", window, cx));

            section.save_general_settings(window, cx);

            assert_eq!(
                section
                    .gen_field_error
                    .as_ref()
                    .map(|(_, message)| message.clone()),
                Some(dbflux_i18n::t!(
                    key,
                    min = dbflux_core::GeneralSettings::MIN_FONT_SIZE,
                    max = dbflux_core::GeneralSettings::MAX_FONT_SIZE
                ))
            );
            assert_eq!(toast_host.read(cx).last_toast_title(), None);
            assert_eq!(stored_general_settings(section, cx), stored_before);

            input.update(cx, |input, cx| input.set_value(original, window, cx));
        }

        assert_eq!(
            section.app_state.read(cx).general_settings().ui_font_size,
            13.0
        );
    });
}

#[test]
fn font_setting_copy_resolves_in_every_locale() {
    let keys = [
        "settings.general.ui_font_family.label",
        "settings.general.ui_font_family.help",
        "settings.general.ui_font_family.option.default",
        "settings.general.ui_font_size.label",
        "settings.general.ui_font_size.help",
        "settings.general.ui_font_size.error",
        "settings.general.editor_font_family.label",
        "settings.general.editor_font_family.help",
        "settings.general.editor_font_family.option.default",
        "settings.general.editor_font_size.label",
        "settings.general.editor_font_size.help",
        "settings.general.editor_font_size.error",
        "settings.general.grid_font_family.label",
        "settings.general.grid_font_family.help",
        "settings.general.grid_font_family.option.same_as_editor",
        "settings.general.grid_font_size.label",
        "settings.general.grid_font_size.help",
        "settings.general.grid_font_size.error",
        "settings.general.font_family.option.not_installed",
        "settings.general.font_family.search_placeholder",
        "settings.general.font_family.no_matches",
    ];

    for key in keys {
        let english = dbflux_i18n::t!(key, locale = "en");
        assert!(!english.is_empty(), "{key} resolved empty for en");
        assert_ne!(english, format!("en.{key}"), "{key} missing in en");

        // A missing key falls back to English, so a translated catalog must
        // give a different value.
        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(!value.is_empty(), "{key} resolved empty for {locale}");
            assert_ne!(value, english, "{key} missing in {locale}");
        }
    }
}

/// Opens the General section under a `Root`, with "Missing Sans" saved as
/// the interface family so its select has a known entry at index 1.
fn open_font_select_section(
    cx: &mut TestAppContext,
) -> (Entity<GeneralSection>, &mut gpui::VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let mut state = AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test app state");
            let mut settings = state.general_settings().clone();
            settings.ui_font_family = Some("Missing Sans".to_string());
            state.update_general_settings(settings);
            state
        })
    });

    let slot: std::rc::Rc<std::cell::RefCell<Option<Entity<GeneralSection>>>> =
        std::rc::Rc::default();
    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let section =
                cx.new(|cx| GeneralSection::new(app_state, GeneralPage::Appearance, window, cx));
            slot.replace(Some(section.clone()));
            gpui_component::Root::new(section, window, cx)
        }
    });
    window.run_until_parked();

    let section = slot.borrow().clone().expect("the section is built");
    (section, window)
}

fn open_ui_family_select(section: &Entity<GeneralSection>, window: &mut gpui::VisualTestContext) {
    window.update(|window, cx| {
        let select = section.read(cx).select_ui_font_family.clone();
        select.update(cx, |select, cx| select.focus(window, cx));
    });
    window.simulate_keystrokes("enter");
    window.run_until_parked();
}

/// The value of the open font search field, read from the window's
/// accessibility tree; `None` while no select is open.
#[expect(
    clippy::unwrap_in_result,
    reason = "test helper: retains the existing panic-on-poisoned-lock policy \
                  and the existing hard failure when the window rendered no \
                  frame; the `Option` return is the `find_map` miss, meaning the \
                  search input is absent from the accessibility tree"
)]
fn font_search_value(window: &mut gpui::VisualTestContext) -> Option<String> {
    let capture = Arc::new(FrameCapture::default());
    window.update(|window, _| {
        window.observe_frames(&capture);
        window.refresh();
    });
    window.run_until_parked();

    let frame = capture
        .0
        .lock()
        .expect("frame capture lock")
        .clone()
        .expect("the window rendered a frame");
    let search_label = dbflux_i18n::t!("settings.general.font_family.search_placeholder");

    frame.nodes().find_map(|(_, node)| {
        let accessible = frame.accessibility_node(node)?;
        (accessible.role() == gpui::Role::TextInput
            && accessible.label() == Some(search_label.as_str()))
        .then(|| accessible.value().unwrap_or_default().to_owned())
    })
}

fn ui_family_cursor(
    section: &Entity<GeneralSection>,
    window: &mut gpui::VisualTestContext,
) -> Option<gpui_component::IndexPath> {
    window.update(|_, cx| {
        let select = section.read(cx).select_ui_font_family.clone();
        select.read(cx).selected_index(cx)
    })
}

fn ui_family_value(
    section: &Entity<GeneralSection>,
    window: &mut gpui::VisualTestContext,
) -> Option<Option<gpui::SharedString>> {
    window.update(|_, cx| {
        let select = section.read(cx).select_ui_font_family.clone();
        select.read(cx).selected_value().cloned()
    })
}

#[gpui::test]
fn escape_after_searching_keeps_the_saved_family_and_its_full_list_position(
    cx: &mut TestAppContext,
) {
    let (section, window) = open_font_select_section(cx);
    let saved = Some(Some(gpui::SharedString::from("Missing Sans")));
    assert_eq!(ui_family_value(&section, window), saved);
    assert_eq!(
        ui_family_cursor(&section, window),
        Some(gpui_component::IndexPath::new(1))
    );

    open_ui_family_select(&section, window);
    window.simulate_input("default");
    window.run_until_parked();
    window.simulate_keystrokes("escape");
    window.run_until_parked();

    assert_eq!(ui_family_value(&section, window), saved);
    assert_eq!(
        ui_family_cursor(&section, window),
        Some(gpui_component::IndexPath::new(1)),
        "the cursor returns to the saved family in the unfiltered list"
    );
}

#[gpui::test]
fn reopening_after_a_search_starts_from_the_full_list(cx: &mut TestAppContext) {
    let (section, window) = open_font_select_section(cx);

    open_ui_family_select(&section, window);
    window.simulate_input("zzzz");
    window.run_until_parked();
    window.simulate_keystrokes("escape");
    window.run_until_parked();

    open_ui_family_select(&section, window);

    assert_eq!(
        font_search_value(window).as_deref(),
        Some(""),
        "the search field starts empty on every open"
    );
    assert_eq!(
        ui_family_cursor(&section, window),
        Some(gpui_component::IndexPath::new(1)),
        "a query left from the last open would hide the saved family"
    );
}

#[gpui::test]
fn enter_after_searching_selects_the_highlighted_family(cx: &mut TestAppContext) {
    let (section, window) = open_font_select_section(cx);

    open_ui_family_select(&section, window);
    window.simulate_input("default");
    window.run_until_parked();
    window.simulate_keystrokes("enter");
    window.run_until_parked();

    assert_eq!(ui_family_value(&section, window), Some(None));
    assert_eq!(
        window.update(|_, cx| section.read(cx).gen_settings.ui_font_family.clone()),
        None,
        "choosing the default entry clears the saved family"
    );
    assert_eq!(
        ui_family_cursor(&section, window),
        Some(gpui_component::IndexPath::new(0))
    );
}

#[gpui::test]
fn enter_selects_the_first_match_after_a_search_that_matched_nothing(cx: &mut TestAppContext) {
    let (section, window) = open_font_select_section(cx);

    open_ui_family_select(&section, window);
    // One keystroke per frame, as a person types: the list draws no rows
    // for "zz", then the edit back to a query with matches.
    for key in ["z", "z", "backspace", "backspace"] {
        window.simulate_keystrokes(key);
        window.run_until_parked();
    }
    window.simulate_keystrokes("enter");
    window.run_until_parked();

    assert_eq!(
        ui_family_value(&section, window),
        Some(None),
        "Enter picks the first match once the search finds one"
    );
}

#[test]
fn a_rejected_font_size_moves_the_cursor_to_its_field() {
    with_appearance_section(|section, _, window, cx| {
        section
            .input_editor_font_size
            .update(cx, |input, cx| input.set_value("40", window, cx));
        section.save_general_settings(window, cx);

        let editor_size_row = section
            .gen_form_rows()
            .iter()
            .position(|row| *row == GeneralFormRow::EditorFontSize)
            .expect("editor font size row");
        assert_eq!(section.gen_form_cursor, editor_size_row);
        assert!(section.content_focused);
        assert!(
            section.gen_editing_field,
            "the invalid field takes focus for editing"
        );
        assert_eq!(section.pending_reveal, Some(GeneralFormRow::EditorFontSize));
    });
}

#[gpui::test]
fn an_invalid_size_shows_its_error_under_the_field(cx: &mut TestAppContext) {
    let (section, window) = open_font_select_section(cx);

    window.update(|window, cx| {
        section.update(cx, |section, cx| {
            section
                .input_grid_font_size
                .update(cx, |input, cx| input.set_value("40", window, cx));
            section.save_general_settings(window, cx);
        });
    });
    window.run_until_parked();

    let field = window
        .debug_bounds("general-grid-font-size-control")
        .expect("the grid size field renders");
    let error = window
        .debug_bounds("general-grid-font-size-error")
        .expect("the error renders in the settings window");

    assert!(
        error.origin.y >= field.origin.y + field.size.height,
        "the error sits under the field: field {field:?}, error {error:?}"
    );
}

const APPEARANCE_ROWS: [GeneralFormRow; 10] = [
    GeneralFormRow::Theme,
    GeneralFormRow::Style,
    GeneralFormRow::Language,
    GeneralFormRow::UiFontFamily,
    GeneralFormRow::UiFontSize,
    GeneralFormRow::EditorFontFamily,
    GeneralFormRow::EditorFontSize,
    GeneralFormRow::GridFontFamily,
    GeneralFormRow::GridFontSize,
    GeneralFormRow::Scrollbars,
];

#[test]
fn appearance_page_lists_the_moved_rows_in_the_artboard_order_and_general_none_of_them() {
    with_appearance_section(|section, _, _, _| {
        let mut expected = APPEARANCE_ROWS.to_vec();
        expected.push(GeneralFormRow::SaveButton);

        assert_eq!(section.gen_form_rows(), expected);
    });

    with_general_section(|section, _, _, _| {
        let rows = section.gen_form_rows();

        for moved in APPEARANCE_ROWS {
            assert!(!rows.contains(&moved), "{moved:?} must not be on General");
        }
        assert!(rows.contains(&GeneralFormRow::VimMode));
    });
}

#[test]
fn each_page_counts_only_its_own_unsaved_changes() {
    with_appearance_section(|section, _, window, cx| {
        section.gen_settings.vim_mode = !section.gen_settings.vim_mode;
        section
            .input_max_history
            .update(cx, |input, cx| input.set_value("42", window, cx));
        assert_eq!(section.general_change_count(cx), 0);
        assert!(!section.has_unsaved_general_changes(cx));

        section.gen_settings.style = AppStyle::Compact;
        section.gen_settings.language = "en".to_string();
        section.gen_settings.scrollbar_visibility = ScrollbarVisibility::Always;
        assert_eq!(section.general_change_count(cx), 3);
    });

    with_general_section(|section, _, window, cx| {
        section.gen_settings.style = AppStyle::Compact;
        section.gen_settings.scrollbar_visibility = ScrollbarVisibility::Always;
        // A changed font size on the hidden Appearance input is not General's.
        section
            .input_ui_font_size
            .update(cx, |input, cx| input.set_value("16", window, cx));
        assert_eq!(section.general_change_count(cx), 0);
    });
}

#[test]
fn saving_the_appearance_page_persists_theme_density_language_and_scrollbars() {
    with_appearance_section_phases(vec![
        Box::new(|section, window, cx| {
            section.gen_settings.theme = ThemeSetting::Light;
            section.gen_settings.style = AppStyle::Compact;
            section.gen_settings.language = "es".to_string();
            section.gen_settings.scrollbar_visibility = ScrollbarVisibility::Always;
            assert_eq!(section.general_change_count(cx), 4);

            section.save_general_settings(window, cx);

            let saved = section.app_state.read(cx).general_settings().clone();
            assert_eq!(saved.theme, ThemeSetting::Light);
            assert_eq!(saved.style, AppStyle::Compact);
            assert_eq!(saved.language, "es");
            assert_eq!(saved.scrollbar_visibility, ScrollbarVisibility::Always);
        }),
        Box::new(|section, _window, cx| {
            let stored = section
                .app_state
                .read(cx)
                .storage_runtime()
                .general_settings()
                .get()
                .expect("stored general settings readable")
                .expect("settings were stored");

            assert_eq!(stored.style, "compact");
            assert_eq!(stored.language, "es");
            assert_eq!(stored.scrollbar_visibility, "always");
            assert!(!section.has_unsaved_general_changes(cx));
        }),
    ]);
}

#[test]
fn saving_the_appearance_page_keeps_the_general_settings_untouched() {
    with_appearance_section(|section, _, window, cx| {
        let before = section.app_state.read(cx).general_settings().clone();
        section.gen_settings.style = AppStyle::Compact;

        section.save_general_settings(window, cx);

        let saved = section.app_state.read(cx).general_settings().clone();
        assert_eq!(saved.vim_mode, before.vim_mode);
        assert_eq!(saved.max_history_entries, before.max_history_entries);
        assert_eq!(saved.scrollbar_visibility, before.scrollbar_visibility);
    });
}

#[test]
fn appearance_copy_resolves_in_every_locale_and_general_no_longer_says_appearance() {
    for key in [
        "settings.nav.appearance",
        "settings.general.appearance.subtitle",
    ] {
        let english = dbflux_i18n::t!(key, locale = "en");
        assert!(!english.is_empty(), "{key} resolved empty for en");
        assert_ne!(english, format!("en.{key}"), "{key} missing in en");

        // A missing key falls back to English, so a translated catalog must
        // give a different value.
        for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(!value.is_empty(), "{key} resolved empty for {locale}");
            assert_ne!(value, english, "{key} missing in {locale}");
        }
    }

    assert_eq!(
        dbflux_i18n::t!("settings.general.appearance.subtitle", locale = "en"),
        "Theme, density, language and fonts."
    );
    assert!(
        !dbflux_i18n::t!("settings.general.header.subtitle", locale = "en").contains("Appearance")
    );
}

#[test]
fn theme_segments_list_follow_system_then_dark_then_light() {
    let labels: Vec<_> = GeneralSection::theme_items()
        .into_iter()
        .map(|item| item.label)
        .collect();

    assert_eq!(labels, vec!["Follow system", "Dark", "Light"]);
}

#[test]
fn theme_option_keys_resolve_in_every_locale() {
    let keys = [
        "settings.general.theme.option.follow_system",
        "settings.general.theme.option.dark",
        "settings.general.theme.option.light",
    ];

    for key in keys {
        for locale in ["en", "es", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert_ne!(value, format!("{locale}.{key}"));
            assert!(!value.is_empty(), "{key} is empty in {locale}");
        }
    }

    assert_ne!(
        dbflux_i18n::t!("settings.general.theme.option.follow_system", locale = "en"),
        dbflux_i18n::t!("settings.general.theme.option.follow_system", locale = "es")
    );
}

#[test]
fn theme_index_and_reverse_mapping_cover_all_supported_themes() {
    assert_eq!(GeneralSection::theme_index(ThemeSetting::System), 0);
    assert_eq!(GeneralSection::theme_index(ThemeSetting::Dark), 1);
    assert_eq!(GeneralSection::theme_index(ThemeSetting::Light), 2);

    assert_eq!(GeneralSection::theme_for_index(0), ThemeSetting::System);
    assert_eq!(GeneralSection::theme_for_index(1), ThemeSetting::Dark);
    assert_eq!(GeneralSection::theme_for_index(2), ThemeSetting::Light);
    assert_eq!(GeneralSection::theme_for_index(99), ThemeSetting::Dark);
}

#[test]
fn density_segments_expose_exactly_two_labels() {
    let labels: Vec<_> = GeneralSection::style_items()
        .into_iter()
        .map(|item| item.label)
        .collect();

    assert_eq!(labels, vec!["Default", "Compact"]);
}

#[test]
fn style_label_maps_every_variant_to_a_key_in_every_locale() {
    let cases = [
        (AppStyle::Default, "settings.general.style.option.default"),
        (AppStyle::Compact, "settings.general.style.option.compact"),
    ];

    for (style, key) in cases {
        assert_eq!(GeneralSection::style_label(style), dbflux_i18n::t!(key));

        for locale in ["en", "es", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(
                !value.is_empty() && value != format!("{locale}.{key}"),
                "{key} missing in {locale}"
            );
        }
    }

    assert_ne!(
        dbflux_i18n::t!("settings.general.style.option.compact", locale = "en"),
        dbflux_i18n::t!("settings.general.style.option.compact", locale = "es")
    );
}

#[test]
fn style_index_and_reverse_mapping_cover_all_variants() {
    assert_eq!(GeneralSection::style_index(AppStyle::Default), 0);
    assert_eq!(GeneralSection::style_index(AppStyle::Compact), 1);

    assert_eq!(GeneralSection::style_for_index(0), AppStyle::Default);
    assert_eq!(GeneralSection::style_for_index(1), AppStyle::Compact);
    // Out-of-range falls back to Default
    assert_eq!(GeneralSection::style_for_index(99), AppStyle::Default);
}

#[test]
fn scrollbar_segments_label_every_visibility_in_every_locale() {
    let labels: Vec<_> = GeneralSection::scrollbar_visibility_items()
        .into_iter()
        .map(|item| item.label)
        .collect();
    assert_eq!(labels, vec!["While scrolling", "On hover", "Always"]);

    for key in [
        "settings.general.scrollbars.label",
        "settings.general.scrollbars.help",
        "settings.general.scrollbars.option.while_scrolling",
        "settings.general.scrollbars.option.on_hover",
        "settings.general.scrollbars.option.always",
    ] {
        for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(
                !value.is_empty() && value != format!("{locale}.{key}"),
                "{key} missing in {locale}"
            );
        }
    }
    assert_eq!(
        dbflux_i18n::t!("settings.general.scrollbars.label", locale = "en"),
        "Show scrollbars"
    );
}

#[test]
fn scrollbar_visibility_index_and_reverse_mapping_cover_all_variants() {
    let variants = [
        ScrollbarVisibility::WhileScrolling,
        ScrollbarVisibility::OnHover,
        ScrollbarVisibility::Always,
    ];

    for (index, visibility) in variants.into_iter().enumerate() {
        assert_eq!(
            GeneralSection::scrollbar_visibility_index(visibility),
            index
        );
        assert_eq!(
            GeneralSection::scrollbar_visibility_for_index(index),
            visibility
        );
    }
    assert_eq!(
        GeneralSection::scrollbar_visibility_for_index(99),
        ScrollbarVisibility::WhileScrolling
    );
}

#[test]
fn language_dropdown_orders_system_then_english_then_deterministic_remainder() {
    let labels: Vec<_> = GeneralSection::language_items()
        .into_iter()
        .map(|item| item.label)
        .collect();
    let available = dbflux_i18n::Language::available();

    assert_eq!(labels.len(), available.len() + 1);
    let expected_system_label = GeneralSection::system_language_label(dbflux_i18n::resolve(
        None,
        dbflux_i18n::detect_system_locale().as_deref(),
    ));
    assert_eq!(
        labels.first().map(|label| label.to_string()),
        Some(expected_system_label)
    );
    assert_eq!(labels.get(1).map(|label| label.as_ref()), Some("English"));

    let storage_ids: Vec<_> = available
        .iter()
        .skip(1)
        .map(|language| language.as_storage_str())
        .collect();
    let mut sorted_storage_ids = storage_ids.clone();
    sorted_storage_ids.sort_unstable();
    assert_eq!(storage_ids, sorted_storage_ids);

    for (label, language) in labels.iter().skip(1).zip(available) {
        assert_eq!(label, &language.native_name());
    }
}

#[test]
fn system_language_label_names_the_resolved_language_in_parentheses() {
    assert_eq!(
        GeneralSection::system_language_label(dbflux_i18n::Language::ENGLISH),
        "System (English)"
    );
}

#[test]
fn language_index_and_reverse_mapping_round_trip_every_available_locale() {
    assert_eq!(GeneralSection::language_index(""), 0);
    assert_eq!(GeneralSection::language_for_index(0), "");

    let available = dbflux_i18n::Language::available();
    for (position, language) in available.iter().enumerate() {
        let index = position + 1;
        let storage_id = language.as_storage_str();
        assert_eq!(GeneralSection::language_index(storage_id), index);
        assert_eq!(GeneralSection::language_for_index(index), storage_id);
    }

    assert_eq!(GeneralSection::language_index("de"), 0);
    assert_eq!(GeneralSection::language_for_index(available.len() + 1), "");
}

/// Keeps the latest rendered accessibility frame of the window it observes.
#[derive(Default)]
struct FrameCapture(Mutex<Option<gpui::AccessibilityFrame>>);

impl gpui::FrameObserver for FrameCapture {
    fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
        *self.0.lock().expect("frame capture lock") = Some(frame.clone());
    }
}

/// Renders the General section in its own window and returns the element
/// id and accessible name of every checkbox in the rendered frame.
fn render_general_checkboxes(cx: &mut TestAppContext) -> HashMap<String, Option<String>> {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("test storage runtime"),
            )
            .expect("test app state")
        })
    });

    let capture = Arc::new(FrameCapture::default());
    let window = cx
        .update(|cx| {
            cx.open_window(WindowOptions::default(), |window, cx| {
                window.observe_frames(&capture);
                cx.new(|cx| GeneralSection::new(app_state, GeneralPage::General, window, cx))
            })
        })
        .expect("general settings window opens");
    cx.run_until_parked();

    let frame = capture
        .0
        .lock()
        .expect("frame capture lock")
        .clone()
        .expect("the window rendered a frame");

    let checkboxes = frame
        .nodes()
        .filter_map(|(_, node)| {
            let accessible = frame.accessibility_node(node)?;
            (accessible.role() == gpui::Role::CheckBox).then(|| {
                (
                    node.id().to_owned(),
                    accessible.label().map(ToOwned::to_owned),
                )
            })
        })
        .collect();

    window
        .update(cx, |_, window, _| window.remove_window())
        .expect("general settings window closes");

    checkboxes
}

#[::core::prelude::v1::test]
fn general_checkboxes_are_named_after_their_visible_labels() {
    let mut cx = TestAppContext::single();
    let checkboxes = render_general_checkboxes(&mut cx);

    let expected = [
        ("vim-mode", "settings.general.vim_mode.label"),
        (
            "table-alias",
            "settings.general.table_alias_completion.label",
        ),
        (
            "table-alias-as",
            "settings.general.table_alias_use_as.label",
        ),
        ("grid-fk-labels", "settings.general.grid_fk_labels.label"),
        ("restore-session", "settings.general.restore_session.label"),
        ("reopen-conns", "settings.general.reopen_connections.label"),
        (
            "pause-on-error",
            "settings.general.pause_refresh_on_error.label",
        ),
        (
            "refresh-visible",
            "settings.general.refresh_only_if_visible.label",
        ),
        (
            "confirm-dangerous",
            "settings.general.confirm_dangerous.label",
        ),
        ("requires-where", "settings.general.requires_where.label"),
        (
            "requires-preview",
            "settings.general.requires_preview.label",
        ),
    ];

    for (id, key) in expected {
        let label = dbflux_i18n::t!(key);
        assert!(!label.is_empty(), "{key} resolved empty");
        assert_eq!(
            checkboxes.get(id),
            Some(&Some(label)),
            "checkbox {id} in {checkboxes:?}"
        );
    }

    let unnamed: Vec<_> = checkboxes
        .iter()
        .filter(|(_, label)| label.as_deref().is_none_or(str::is_empty))
        .map(|(id, _)| id.as_str())
        .collect();
    assert!(unnamed.is_empty(), "checkboxes without a name: {unnamed:?}");
}

#[test]
fn dropdown_placeholders_reuse_or_extend_settings_general_catalog_keys() {
    assert_eq!(dbflux_i18n::t!("settings.general.theme.label"), "Theme");
    assert_eq!(dbflux_i18n::t!("settings.general.style.label"), "Density");
    assert_eq!(
        dbflux_i18n::t!("settings.general.language.label"),
        "Language"
    );
    assert_eq!(
        dbflux_i18n::t!("settings.general.default_focus.label"),
        "Focus on launch"
    );
    assert_eq!(
        dbflux_i18n::t!("settings.general.placeholder.refresh_policy"),
        "Refresh policy"
    );

    for locale in ["en", "es"] {
        let value = dbflux_i18n::t!(
            "settings.general.placeholder.refresh_policy",
            locale = locale
        );

        assert!(
            !value.is_empty(),
            "settings.general.placeholder.refresh_policy resolved empty for locale {locale}"
        );
        assert_ne!(
            value,
            format!("{locale}.settings.general.placeholder.refresh_policy"),
            "settings.general.placeholder.refresh_policy fell back to the raw key for locale {locale}"
        );
    }
}

/// Opens the General section under a `Root`, which sets the rem size and
/// lays the section out in a real window.
fn open_general_section(
    cx: &mut TestAppContext,
) -> (
    Entity<GeneralSection>,
    Entity<ToastHost>,
    &mut gpui::VisualTestContext,
) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    let toast_host = cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host: host.clone() });
        host
    });

    let app_state = cx.update(|cx| {
        cx.new(|_| {
            AppStateEntity::new_with_storage_runtime(
                StorageRuntime::in_memory().expect("isolated storage runtime"),
            )
            .expect("test app state")
        })
    });

    let slot: std::rc::Rc<std::cell::RefCell<Option<Entity<GeneralSection>>>> =
        std::rc::Rc::default();
    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let section =
                cx.new(|cx| GeneralSection::new(app_state, GeneralPage::General, window, cx));
            slot.replace(Some(section.clone()));
            gpui_component::Root::new(section, window, cx)
        }
    });
    window.run_until_parked();

    let section = slot.borrow().clone().expect("the section is built");
    (section, toast_host, window)
}

fn redraw(window: &mut gpui::VisualTestContext) {
    window.run_until_parked();
    window.update(|window, _| window.refresh());
    window.run_until_parked();
}

fn save(section: &Entity<GeneralSection>, window: &mut gpui::VisualTestContext) {
    window.update(|window, cx| {
        section.update(cx, |section, cx| section.save_general_settings(window, cx));
    });
    redraw(window);
}

fn set_input(
    section: &Entity<GeneralSection>,
    window: &mut gpui::VisualTestContext,
    input: fn(&GeneralSection) -> Entity<dbflux_components::controls::InputState>,
    value: &'static str,
) {
    window.update(|window, cx| {
        let input = input(section.read(cx));
        input.update(cx, |input, cx| input.set_value(value, window, cx));
    });
}

#[gpui::test]
fn saving_keeps_an_inspector_width_the_window_never_saw(cx: &mut TestAppContext) {
    let (section, _toasts, window) = open_general_section(cx);

    // The inspector is resized while this settings window stays open.
    window.update(|_, cx| {
        let app_state = section.read(cx).app_state.clone();
        let mut settings = app_state.read(cx).general_settings().clone();
        settings.workspace_inspector_width_px = Some(480.0);
        app_state.update(cx, |state, _cx| {
            state.update_general_settings(settings.clone());
        });
    });

    save(&section, window);

    window.update(|_, cx| {
        assert_eq!(
            section
                .read(cx)
                .app_state
                .read(cx)
                .general_settings()
                .workspace_inspector_width_px,
            Some(480.0),
            "saving another setting must not write back a stale width"
        );
    });
}

#[gpui::test]
fn the_object_preview_hint_sits_under_its_field(cx: &mut TestAppContext) {
    let (_section, _toasts, window) = open_general_section(cx);

    let control = window
        .debug_bounds("general-object-preview-limit-control")
        .expect("the object preview field renders");
    let row = window
        .debug_bounds("general-object-preview-limit-row")
        .expect("the object preview row renders");

    assert!(
        row.bottom()
            > control.bottom()
                + crate::tokens::FormMetrics::ROW_PADDING_Y
                + crate::tokens::FormMetrics::HELP_GAP,
        "the hint adds a line under the field instead of sitting beside it: \
             control {control:?}, row {row:?}"
    );
}

#[test]
fn a_rejected_save_moves_the_cursor_to_the_first_invalid_field() {
    with_general_section(|section, _, window, cx| {
        section
            .input_max_history
            .update(cx, |input, cx| input.set_value("5", window, cx));
        section
            .input_key_value_size_limit
            .update(cx, |input, cx| input.set_value("0", window, cx));

        section.save_general_settings(window, cx);

        let max_history_row = section
            .gen_form_rows()
            .iter()
            .position(|row| *row == GeneralFormRow::MaxHistory)
            .expect("max history row");
        assert_eq!(section.gen_form_cursor, max_history_row);
        assert!(section.content_focused);
        assert!(
            section.gen_editing_field,
            "the invalid field takes focus for editing"
        );
    });
}

#[gpui::test]
fn a_validation_error_shows_under_its_field_and_not_as_a_toast(cx: &mut TestAppContext) {
    let (section, toasts, window) = open_general_section(cx);

    set_input(
        &section,
        window,
        |section| section.input_editor_row_limit.clone(),
        "0",
    );
    save(&section, window);

    let control = window
        .debug_bounds("editor-row-limit-control")
        .expect("the editor row limit field renders");
    let error = window
        .debug_bounds("editor-row-limit-error")
        .expect("the error renders in the settings window");

    assert!(
        error.top() >= control.bottom(),
        "the error sits under the field: control {control:?}, error {error:?}"
    );
    assert_eq!(
        window.update(|_, cx| toasts.read(cx).last_toast_title()),
        None,
        "the main window gets no toast for a settings form error"
    );
}

#[gpui::test]
fn a_rejected_save_scrolls_the_invalid_field_below_the_page_head(cx: &mut TestAppContext) {
    let (section, _toasts, window) = open_general_section(cx);
    window.simulate_resize(gpui::size(gpui::px(1200.0), gpui::px(480.0)));
    redraw(window);

    let fully_visible = |window: &mut gpui::VisualTestContext, selector: &'static str| {
        let field = window
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} renders"));
        let viewport = window
            .debug_bounds("settings-form-viewport")
            .expect("the form viewport renders");

        assert!(
            field.top() >= viewport.top() && field.bottom() <= viewport.bottom(),
            "{selector} is fully visible below the page head: \
                 field {field:?}, viewport {viewport:?}"
        );
    };

    set_input(
        &section,
        window,
        |section| section.input_key_value_size_limit.clone(),
        "0",
    );
    save(&section, window);
    fully_visible(window, "general-key-value-size-limit-control");

    // From the bottom of the form, a field near the top comes back into
    // view below the page head, not under it.
    set_input(
        &section,
        window,
        |section| section.input_key_value_size_limit.clone(),
        "10",
    );
    window.simulate_event(gpui::ScrollWheelEvent {
        position: gpui::point(gpui::px(600.0), gpui::px(300.0)),
        delta: gpui::ScrollDelta::Pixels(gpui::point(gpui::px(0.0), gpui::px(-5000.0))),
        ..Default::default()
    });
    redraw(window);
    set_input(
        &section,
        window,
        |section| section.input_max_history.clone(),
        "5",
    );
    save(&section, window);
    fully_visible(window, "general-max-history-control");
}
