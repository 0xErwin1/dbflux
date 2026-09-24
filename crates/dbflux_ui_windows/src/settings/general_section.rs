use super::SettingsSection;
use super::SettingsSectionId;
use super::section_trait::SectionFocusEvent;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{AppStyle, GeneralSettings, RefreshPolicySetting, StartupFocus, ThemeSetting};
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::*;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum GeneralFormRow {
    Theme,
    Style,
    Language,
    VimMode,
    RestoreSession,
    ReopenConnections,
    DefaultFocus,
    MaxHistory,
    AutoSaveInterval,
    DefaultRefreshPolicy,
    DefaultRefreshInterval,
    MaxBackgroundTasks,
    PauseRefreshOnError,
    RefreshOnlyIfVisible,
    ConfirmDangerous,
    RequiresWhere,
    RequiresPreview,
    EditorRowLimit,
    ObjectPreviewLimit,
    KeyValueSizeLimit,
    ShareStableDb,
    SaveButton,
}

pub(super) struct GeneralSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) gen_settings: GeneralSettings,
    pub(super) gen_form_cursor: usize,
    pub(super) gen_editing_field: bool,
    /// Nightly-only: whether this build is opted into the stable database.
    /// Backed by a pre-database marker file, applied on the next launch.
    pub(super) gen_share_stable_db: bool,
    pub(super) dropdown_theme: Entity<Dropdown>,
    pub(super) dropdown_style: Entity<Dropdown>,
    pub(super) dropdown_language: Entity<Dropdown>,
    pub(super) dropdown_default_focus: Entity<Dropdown>,
    pub(super) dropdown_refresh_policy: Entity<Dropdown>,
    pub(super) input_max_history: Entity<InputState>,
    pub(super) input_auto_save: Entity<InputState>,
    pub(super) input_refresh_interval: Entity<InputState>,
    pub(super) input_max_bg_tasks: Entity<InputState>,
    pub(super) input_editor_row_limit: Entity<InputState>,
    pub(super) input_object_preview_limit: Entity<InputState>,
    pub(super) input_key_value_size_limit: Entity<InputState>,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for GeneralSection {}

impl GeneralSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let settings = app_state.read(cx).general_settings().clone();
        let theme_index = Self::theme_index(settings.theme);
        let style_index = Self::style_index(settings.style);
        let language_index = Self::language_index(&settings.language);
        let startup_focus_index = Self::startup_focus_index(settings.default_focus_on_startup);
        let refresh_policy_index = Self::refresh_policy_index(settings.default_refresh_policy);
        let max_history = settings.max_history_entries.to_string();
        let auto_save_interval = settings.auto_save_interval_ms.to_string();
        let refresh_interval = settings.default_refresh_interval_secs.to_string();
        let max_background_tasks = settings.max_concurrent_background_tasks.to_string();
        let editor_row_limit = settings.editor_row_limit.to_string();
        let object_preview_limit = settings.object_preview_size_limit_mib.to_string();
        let key_value_size_limit = settings.key_value_size_limit_mib.to_string();

        let dropdown_theme = cx.new(move |_cx| {
            Dropdown::new("general-theme")
                .placeholder(dbflux_i18n::t!("settings.general.theme.label"))
                .items(Self::theme_items())
                .selected_index(Some(theme_index))
        });
        let dropdown_style = cx.new(move |_cx| {
            Dropdown::new("general-style")
                .placeholder(dbflux_i18n::t!("settings.general.style.label"))
                .items(Self::style_items())
                .selected_index(Some(style_index))
        });
        let dropdown_language = cx.new(move |_cx| {
            Dropdown::new("general-language")
                .placeholder(dbflux_i18n::t!("settings.general.language.label"))
                .items(Self::language_items())
                .selected_index(Some(language_index))
        });
        let dropdown_default_focus = cx.new(move |_cx| {
            Dropdown::new("general-default-focus")
                .placeholder(dbflux_i18n::t!("settings.general.default_focus.label"))
                .items(Self::startup_focus_items())
                .selected_index(Some(startup_focus_index))
        });
        let dropdown_refresh_policy = cx.new(move |_cx| {
            Dropdown::new("general-refresh-policy")
                .placeholder(dbflux_i18n::t!(
                    "settings.general.placeholder.refresh_policy"
                ))
                .items(Self::refresh_policy_items())
                .selected_index(Some(refresh_policy_index))
        });

        let input_max_history = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("1000")
                .default_value(max_history.clone())
        });
        let input_auto_save = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("2000")
                .default_value(auto_save_interval.clone())
        });
        let input_refresh_interval = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("5")
                .default_value(refresh_interval.clone())
        });
        let input_max_bg_tasks = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("8")
                .default_value(max_background_tasks.clone())
        });

        let input_editor_row_limit = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("10000")
                .default_value(editor_row_limit.clone())
        });

        let input_object_preview_limit = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("10")
                .default_value(object_preview_limit.clone())
        });

        let input_key_value_size_limit = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("10")
                .default_value(key_value_size_limit.clone())
        });

        let theme_subscription = cx.subscribe(
            &dropdown_theme,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.theme = Self::theme_for_index(event.index);
                cx.notify();
            },
        );

        let style_subscription = cx.subscribe(
            &dropdown_style,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.style = Self::style_for_index(event.index);
                cx.notify();
            },
        );

        let language_subscription = cx.subscribe(
            &dropdown_language,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.language = Self::language_for_index(event.index).to_string();
                cx.notify();
            },
        );

        let focus_subscription = cx.subscribe(
            &dropdown_default_focus,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.default_focus_on_startup =
                    Self::startup_focus_for_index(event.index);
                cx.notify();
            },
        );

        let refresh_policy_subscription = cx.subscribe(
            &dropdown_refresh_policy,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.default_refresh_policy =
                    Self::refresh_policy_for_index(event.index);
                cx.notify();
            },
        );

        let blur_max_history =
            cx.subscribe(&input_max_history, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        let blur_auto_save = cx.subscribe(&input_auto_save, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_refresh_interval = cx.subscribe(
            &input_refresh_interval,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        let blur_max_bg_tasks =
            cx.subscribe(&input_max_bg_tasks, |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            });

        let blur_editor_row_limit = cx.subscribe(
            &input_editor_row_limit,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        let blur_object_preview_limit = cx.subscribe(
            &input_object_preview_limit,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        let blur_key_value_size_limit = cx.subscribe(
            &input_key_value_size_limit,
            |this, _, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Blur) {
                    if this.switching_input {
                        this.switching_input = false;
                        return;
                    }
                    cx.emit(SectionFocusEvent::RequestFocusReturn);
                }
            },
        );

        Self {
            app_state,
            gen_settings: settings,
            gen_form_cursor: 0,
            gen_editing_field: false,
            gen_share_stable_db: dbflux_storage::paths::nightly_shares_stable_db(),
            dropdown_theme,
            dropdown_style,
            dropdown_language,
            dropdown_default_focus,
            dropdown_refresh_policy,
            input_max_history,
            input_auto_save,
            input_refresh_interval,
            input_max_bg_tasks,
            input_editor_row_limit,
            input_object_preview_limit,
            input_key_value_size_limit,
            content_focused: false,
            switching_input: false,
            _subscriptions: vec![
                theme_subscription,
                style_subscription,
                language_subscription,
                focus_subscription,
                refresh_policy_subscription,
                blur_max_history,
                blur_auto_save,
                blur_refresh_interval,
                blur_max_bg_tasks,
                blur_editor_row_limit,
                blur_object_preview_limit,
                blur_key_value_size_limit,
            ],
        }
    }

    fn theme_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new(dbflux_i18n::t!("settings.general.theme.option.ayu_dark")),
            DropdownItem::new(dbflux_i18n::t!("settings.general.theme.option.ayu_mirage")),
            DropdownItem::new(dbflux_i18n::t!("settings.general.theme.option.ayu_light")),
        ]
    }

    fn style_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new(Self::style_label(AppStyle::Default)),
            DropdownItem::new(Self::style_label(AppStyle::Compact)),
        ]
    }

    fn style_label(style: AppStyle) -> String {
        match style {
            AppStyle::Default => dbflux_i18n::t!("settings.general.style.option.default"),
            AppStyle::Compact => dbflux_i18n::t!("settings.general.style.option.compact"),
        }
    }

    fn language_items() -> Vec<DropdownItem> {
        std::iter::once(DropdownItem::new(dbflux_i18n::t!(
            "settings.general.language.option.system"
        )))
        .chain(
            dbflux_i18n::Language::available()
                .iter()
                .map(|language| DropdownItem::new(language.native_name())),
        )
        .collect()
    }

    fn startup_focus_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new(dbflux_i18n::t!(
                "settings.general.default_focus.option.sidebar"
            )),
            DropdownItem::new(dbflux_i18n::t!(
                "settings.general.default_focus.option.last_tab"
            )),
        ]
    }

    fn refresh_policy_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new(dbflux_i18n::t!(
                "settings.general.refresh_policy.option.manual"
            )),
            DropdownItem::new(dbflux_i18n::t!(
                "settings.general.refresh_policy.option.interval"
            )),
        ]
    }

    fn theme_index(theme: ThemeSetting) -> usize {
        match theme {
            ThemeSetting::Dark => 0,
            ThemeSetting::Mirage => 1,
            ThemeSetting::Light => 2,
        }
    }

    fn theme_for_index(index: usize) -> ThemeSetting {
        match index {
            1 => ThemeSetting::Mirage,
            2 => ThemeSetting::Light,
            _ => ThemeSetting::Dark,
        }
    }

    pub(super) fn style_index(style: AppStyle) -> usize {
        match style {
            AppStyle::Default => 0,
            AppStyle::Compact => 1,
        }
    }

    pub(super) fn style_for_index(index: usize) -> AppStyle {
        match index {
            1 => AppStyle::Compact,
            _ => AppStyle::Default,
        }
    }

    fn language_index(persisted: &str) -> usize {
        match dbflux_i18n::LanguagePreference::from_storage_str(persisted) {
            dbflux_i18n::LanguagePreference::System => 0,
            dbflux_i18n::LanguagePreference::Explicit(language) => {
                dbflux_i18n::Language::available()
                    .iter()
                    .position(|available| *available == language)
                    .map(|position| position + 1)
                    .unwrap_or(0)
            }
        }
    }

    fn language_for_index(index: usize) -> &'static str {
        let preference = match index
            .checked_sub(1)
            .and_then(|position| dbflux_i18n::Language::available().get(position).copied())
        {
            Some(language) => dbflux_i18n::LanguagePreference::Explicit(language),
            None => dbflux_i18n::LanguagePreference::System,
        };
        preference.as_storage_str()
    }

    fn startup_focus_index(focus: StartupFocus) -> usize {
        match focus {
            StartupFocus::Sidebar => 0,
            StartupFocus::LastTab => 1,
        }
    }

    fn startup_focus_for_index(index: usize) -> StartupFocus {
        match index {
            1 => StartupFocus::LastTab,
            _ => StartupFocus::Sidebar,
        }
    }

    fn refresh_policy_index(policy: RefreshPolicySetting) -> usize {
        match policy {
            RefreshPolicySetting::Manual => 0,
            RefreshPolicySetting::Interval => 1,
        }
    }

    fn refresh_policy_for_index(index: usize) -> RefreshPolicySetting {
        match index {
            1 => RefreshPolicySetting::Interval,
            _ => RefreshPolicySetting::Manual,
        }
    }
}

impl SettingsSection for GeneralSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::General
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        GeneralSection::handle_key_event(self, event, window, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.gen_editing_field = false;
        self.close_open_dropdown(cx);
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_general_changes(cx)
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_general_footer_actions(cx))
    }
}

impl Render for GeneralSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_general_section(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{GeneralFormRow, GeneralSection};
    use dbflux_core::{AppStyle, ThemeSetting};
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext, Entity, TestAppContext, WindowOptions};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn with_general_section(
        test: impl FnOnce(
            &mut GeneralSection,
            &Entity<ToastHost>,
            &mut gpui::Window,
            &mut gpui::Context<GeneralSection>,
        ),
    ) {
        let mut cx = TestAppContext::single();
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
                    cx.new(|cx| GeneralSection::new(app_state, window, cx))
                })
            })
            .expect("general settings window opens");

        window
            .update(&mut cx, |section, window, cx| {
                test(section, &toast_host, window, cx)
            })
            .expect("general section updates");
        window
            .update(&mut cx, |_, window, _| window.remove_window())
            .expect("window closes");
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
        with_general_section(|section, _, window, cx| {
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
            assert_eq!(stored_editor_row_limit(section, cx), Some(5_000));
            assert!(!section.has_unsaved_general_changes(cx));
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
                    toast_host.read(cx).last_toast_title(),
                    Some(dbflux_i18n::t!("settings.general.editor_row_limit.error").to_string()),
                    "{value:?} must show the validation error"
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

    #[test]
    fn theme_dropdown_exposes_exactly_three_ayu_labels() {
        let labels: Vec<_> = GeneralSection::theme_items()
            .into_iter()
            .map(|item| item.label)
            .collect();

        assert_eq!(labels, vec!["Ayu Dark", "Ayu Mirage", "Ayu Light"]);
    }

    #[test]
    fn theme_option_keys_resolve_in_every_locale_and_keep_the_ayu_family_name() {
        let keys = [
            "settings.general.theme.option.ayu_dark",
            "settings.general.theme.option.ayu_mirage",
            "settings.general.theme.option.ayu_light",
        ];

        for key in keys {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    value.starts_with("Ayu "),
                    "{key} must keep the Ayu name in {locale}, got {value:?}"
                );
                assert_ne!(value, format!("{locale}.{key}"));
            }
        }

        assert_eq!(
            dbflux_i18n::t!("settings.general.theme.option.ayu_mirage", locale = "es"),
            "Ayu Mirage"
        );
        assert_ne!(
            dbflux_i18n::t!("settings.general.theme.option.ayu_dark", locale = "en"),
            dbflux_i18n::t!("settings.general.theme.option.ayu_dark", locale = "es")
        );
    }

    #[test]
    fn theme_index_and_reverse_mapping_cover_all_supported_ayu_themes() {
        assert_eq!(GeneralSection::theme_index(ThemeSetting::Dark), 0);
        assert_eq!(GeneralSection::theme_index(ThemeSetting::Mirage), 1);
        assert_eq!(GeneralSection::theme_index(ThemeSetting::Light), 2);

        assert_eq!(GeneralSection::theme_for_index(0), ThemeSetting::Dark);
        assert_eq!(GeneralSection::theme_for_index(1), ThemeSetting::Mirage);
        assert_eq!(GeneralSection::theme_for_index(2), ThemeSetting::Light);
        assert_eq!(GeneralSection::theme_for_index(99), ThemeSetting::Dark);
    }

    #[test]
    fn style_dropdown_exposes_exactly_two_labels() {
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
    fn language_dropdown_orders_system_then_english_then_deterministic_remainder() {
        let labels: Vec<_> = GeneralSection::language_items()
            .into_iter()
            .map(|item| item.label)
            .collect();
        let available = dbflux_i18n::Language::available();

        assert_eq!(labels.len(), available.len() + 1);
        assert_eq!(labels.first().map(|label| label.as_ref()), Some("System"));
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
                    cx.new(|cx| GeneralSection::new(app_state, window, cx))
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
        assert_eq!(dbflux_i18n::t!("settings.general.style.label"), "Style");
        assert_eq!(
            dbflux_i18n::t!("settings.general.language.label"),
            "Language"
        );
        assert_eq!(
            dbflux_i18n::t!("settings.general.default_focus.label"),
            "Default focus"
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
}
