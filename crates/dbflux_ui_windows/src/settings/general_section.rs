use super::SettingsSection;
use super::SettingsSectionId;
use super::section_trait::SectionFocusEvent;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::SegmentedItem;
use dbflux_components::typography::AppFonts;
use dbflux_core::{AppStyle, GeneralSettings, RefreshPolicySetting, StartupFocus, ThemeSetting};
use dbflux_ui_base::AppStateEntity;
use gpui::prelude::*;
use gpui::*;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use gpui_component::IndexPath;
use gpui_component::select::{SearchableVec, SelectEvent, SelectItem, SelectState};

/// The leader keys Settings > General offers, in the keymap's stored key
/// form: Space, and Vim's usual alternatives, comma and backslash (Vim's own
/// default).
const VIM_LEADER_CHOICES: [&str; 3] = ["space", ",", "\\"];

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum GeneralFormRow {
    Theme,
    Style,
    Language,
    UiFontFamily,
    UiFontSize,
    EditorFontFamily,
    EditorFontSize,
    GridFontFamily,
    GridFontSize,
    VimMode,
    VimLeader,
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

/// Each numeric field's last drawn bounds and the scroll offset of that frame.
pub(super) type FieldBounds = Rc<RefCell<HashMap<GeneralFormRow, (Bounds<Pixels>, Pixels)>>>;
/// One of the three font families Settings > General configures.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum FontFamilyField {
    Ui,
    Editor,
    Grid,
}

/// An entry of a font family select: its label and the family it stores,
/// `None` for the bundled default (or, for the grid, the editor family).
#[derive(Clone, Debug, PartialEq)]
pub(super) struct FontFamilyOption {
    pub(super) label: SharedString,
    pub(super) family: Option<SharedString>,
}

impl SelectItem for FontFamilyOption {
    type Value = Option<SharedString>;

    fn title(&self) -> SharedString {
        self.label.clone()
    }

    fn value(&self) -> &Self::Value {
        &self.family
    }
}

pub(super) type FontFamilySelect = Entity<SelectState<SearchableVec<FontFamilyOption>>>;

pub(super) struct GeneralSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) gen_settings: GeneralSettings,
    pub(super) gen_form_cursor: usize,
    pub(super) gen_editing_field: bool,
    /// Nightly-only: whether this build is opted into the stable database.
    /// Backed by a pre-database marker file, applied on the next launch.
    pub(super) gen_share_stable_db: bool,
    pub(super) dropdown_language: Entity<Dropdown>,
    pub(super) dropdown_refresh_policy: Entity<Dropdown>,
    pub(super) dropdown_vim_leader: Entity<Dropdown>,
    pub(super) select_ui_font_family: FontFamilySelect,
    pub(super) select_editor_font_family: FontFamilySelect,
    pub(super) select_grid_font_family: FontFamilySelect,
    pub(super) input_ui_font_size: Entity<InputState>,
    pub(super) input_editor_font_size: Entity<InputState>,
    pub(super) input_grid_font_size: Entity<InputState>,
    pub(super) input_max_history: Entity<InputState>,
    pub(super) input_auto_save: Entity<InputState>,
    pub(super) input_refresh_interval: Entity<InputState>,
    pub(super) input_max_bg_tasks: Entity<InputState>,
    pub(super) input_editor_row_limit: Entity<InputState>,
    pub(super) input_object_preview_limit: Entity<InputState>,
    pub(super) input_key_value_size_limit: Entity<InputState>,
    /// The last rejected field and its message, shown under that field
    /// until the next save attempt.
    pub(super) gen_field_error: Option<(GeneralFormRow, String)>,
    /// A rejected field to scroll into view on the next render.
    pub(super) pending_reveal: Option<GeneralFormRow>,
    /// Frames the pending reveal has adjusted the scroll in.
    pub(super) reveal_attempts: u8,
    /// Scroll position of the form body.
    pub(super) form_scroll: ScrollHandle,
    /// Bounds of the visible form area below the page head, from the last
    /// frame.
    pub(super) form_viewport: Rc<Cell<Bounds<Pixels>>>,
    /// Bounds of each numeric field from the last frame and the scroll offset
    /// they were drawn at, for scrolling a rejected field into view.
    pub(super) field_bounds: FieldBounds,
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
        let language_index = Self::language_index(&settings.language);
        let refresh_policy_index = Self::refresh_policy_index(settings.default_refresh_policy);
        let vim_leader_index = Self::vim_leader_index(&settings.vim_leader);
        let max_history = settings.max_history_entries.to_string();
        let auto_save_interval = settings.auto_save_interval_ms.to_string();
        let refresh_interval = settings.default_refresh_interval_secs.to_string();
        let max_background_tasks = settings.max_concurrent_background_tasks.to_string();
        let editor_row_limit = settings.editor_row_limit.to_string();
        let object_preview_limit = settings.object_preview_size_limit_mib.to_string();
        let key_value_size_limit = settings.key_value_size_limit_mib.to_string();

        let dropdown_language = cx.new(move |_cx| {
            Dropdown::new("general-language")
                .placeholder(dbflux_i18n::t!("settings.general.language.label"))
                .items(Self::language_items())
                .selected_index(Some(language_index))
                .leading_icon(AppIcon::Globe)
        });
        let dropdown_refresh_policy = cx.new(move |_cx| {
            Dropdown::new("general-refresh-policy")
                .placeholder(dbflux_i18n::t!(
                    "settings.general.placeholder.refresh_policy"
                ))
                .items(Self::refresh_policy_items())
                .selected_index(Some(refresh_policy_index))
        });
        let dropdown_vim_leader = cx.new(move |_cx| {
            Dropdown::new("general-vim-leader")
                .placeholder(dbflux_i18n::t!("settings.general.vim_leader.label"))
                .items(Self::vim_leader_items())
                .selected_index(vim_leader_index)
        });

        let installed_fonts = dbflux_components::fonts::installed_font_names(cx);
        let select_ui_font_family = Self::new_font_family_select(
            FontFamilyField::Ui,
            settings.ui_font_family.as_deref(),
            &installed_fonts,
            window,
            cx,
        );
        let select_editor_font_family = Self::new_font_family_select(
            FontFamilyField::Editor,
            settings.editor_font_family.as_deref(),
            &installed_fonts,
            window,
            cx,
        );
        let select_grid_font_family = Self::new_font_family_select(
            FontFamilyField::Grid,
            settings.grid_font_family.as_deref(),
            &installed_fonts,
            window,
            cx,
        );

        let input_ui_font_size = Self::new_font_size_input(
            settings.ui_font_size,
            GeneralSettings::DEFAULT_UI_FONT_SIZE,
            window,
            cx,
        );
        let input_editor_font_size = Self::new_font_size_input(
            settings.editor_font_size,
            GeneralSettings::DEFAULT_EDITOR_FONT_SIZE,
            window,
            cx,
        );
        let input_grid_font_size = Self::new_font_size_input(
            settings.grid_font_size,
            GeneralSettings::DEFAULT_GRID_FONT_SIZE,
            window,
            cx,
        );

        let mut font_subscriptions = Vec::new();

        for (field, select) in [
            (FontFamilyField::Ui, &select_ui_font_family),
            (FontFamilyField::Editor, &select_editor_font_family),
            (FontFamilyField::Grid, &select_grid_font_family),
        ] {
            font_subscriptions.push(cx.subscribe(
                select,
                move |this, _, event: &SelectEvent<SearchableVec<FontFamilyOption>>, cx| {
                    let SelectEvent::Confirm(choice) = event;
                    this.set_font_family(field, choice.clone().flatten());
                    cx.notify();
                },
            ));
        }

        for input in [
            &input_ui_font_size,
            &input_editor_font_size,
            &input_grid_font_size,
        ] {
            font_subscriptions.push(cx.subscribe(input, Self::return_focus_on_blur));
        }

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

        let language_subscription = cx.subscribe(
            &dropdown_language,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.language = Self::language_for_index(event.index).to_string();
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

        let vim_leader_subscription = cx.subscribe(
            &dropdown_vim_leader,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.vim_leader = Self::vim_leader_for_index(event.index).to_string();
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
            dropdown_language,
            dropdown_refresh_policy,
            dropdown_vim_leader,
            select_ui_font_family,
            select_editor_font_family,
            select_grid_font_family,
            input_ui_font_size,
            input_editor_font_size,
            input_grid_font_size,
            input_max_history,
            input_auto_save,
            input_refresh_interval,
            input_max_bg_tasks,
            input_editor_row_limit,
            input_object_preview_limit,
            input_key_value_size_limit,
            gen_field_error: None,
            pending_reveal: None,
            reveal_attempts: 0,
            form_scroll: ScrollHandle::new(),
            form_viewport: Rc::default(),
            field_bounds: Rc::default(),
            content_focused: false,
            switching_input: false,
            _subscriptions: vec![
                language_subscription,
                refresh_policy_subscription,
                vim_leader_subscription,
                blur_max_history,
                blur_auto_save,
                blur_refresh_interval,
                blur_max_bg_tasks,
                blur_editor_row_limit,
                blur_object_preview_limit,
                blur_key_value_size_limit,
            ]
            .into_iter()
            .chain(font_subscriptions)
            .collect(),
        }
    }

    fn return_focus_on_blur(
        &mut self,
        _input: Entity<InputState>,
        event: &InputEvent,
        cx: &mut Context<Self>,
    ) {
        if !matches!(event, InputEvent::Blur) {
            return;
        }

        if self.switching_input {
            self.switching_input = false;
            return;
        }

        cx.emit(SectionFocusEvent::RequestFocusReturn);
    }

    fn new_font_size_input(
        size: f32,
        default: f32,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<InputState> {
        cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(default.to_string())
                .default_value(size.to_string())
        })
    }

    fn new_font_family_select(
        field: FontFamilyField,
        saved: Option<&str>,
        installed: &[SharedString],
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> FontFamilySelect {
        let options = Self::font_family_options(field, saved, installed);
        let selected = Self::font_family_index(&options, saved);

        cx.new(|cx| {
            SelectState::new(
                SearchableVec::new(options),
                Some(IndexPath::new(selected)),
                window,
                cx,
            )
            .searchable(true)
        })
    }

    /// Entries of a font family select: the default first, then a saved
    /// family the platform does not report (so saving keeps it), then every
    /// installed family.
    pub(super) fn font_family_options(
        field: FontFamilyField,
        saved: Option<&str>,
        installed: &[SharedString],
    ) -> Vec<FontFamilyOption> {
        let default = FontFamilyOption {
            label: Self::font_family_default_label(field).into(),
            family: None,
        };

        let missing_saved = saved
            .filter(|family| !installed.iter().any(|name| name.as_ref() == *family))
            .map(|family| FontFamilyOption {
                label: dbflux_i18n::t!(
                    "settings.general.font_family.option.not_installed",
                    family = family
                )
                .into(),
                family: Some(SharedString::from(family.to_string())),
            });

        let installed = installed.iter().map(|name| FontFamilyOption {
            label: name.clone(),
            family: Some(name.clone()),
        });

        std::iter::once(default)
            .chain(missing_saved)
            .chain(installed)
            .collect()
    }

    /// Index of the entry storing `saved` in `options`, the default entry
    /// when nothing matches.
    pub(super) fn font_family_index(options: &[FontFamilyOption], saved: Option<&str>) -> usize {
        options
            .iter()
            .position(|option| option.family.as_deref() == saved)
            .unwrap_or(0)
    }

    fn font_family_default_label(field: FontFamilyField) -> String {
        match field {
            FontFamilyField::Ui => dbflux_i18n::t!(
                "settings.general.ui_font_family.option.default",
                family = AppFonts::INTERFACE
            ),
            FontFamilyField::Editor => dbflux_i18n::t!(
                "settings.general.editor_font_family.option.default",
                family = AppFonts::MONO
            ),
            FontFamilyField::Grid => {
                dbflux_i18n::t!("settings.general.grid_font_family.option.same_as_editor")
            }
        }
    }

    /// Stores the family chosen for `field`; `None` selects the default.
    pub(super) fn set_font_family(&mut self, field: FontFamilyField, family: Option<SharedString>) {
        let family = family.map(|family| family.to_string());

        match field {
            FontFamilyField::Ui => self.gen_settings.ui_font_family = family,
            FontFamilyField::Editor => self.gen_settings.editor_font_family = family,
            FontFamilyField::Grid => self.gen_settings.grid_font_family = family,
        }
    }

    /// Parses a font size input: a number, decimals allowed, from
    /// `GeneralSettings::MIN_FONT_SIZE` to `MAX_FONT_SIZE`.
    pub(super) fn parse_font_size(value: &str) -> Option<f32> {
        let size = value.trim().parse::<f32>().ok()?;
        let range = GeneralSettings::MIN_FONT_SIZE..=GeneralSettings::MAX_FONT_SIZE;

        range.contains(&size).then_some(size)
    }

    /// Theme segments, in index order (see [`Self::theme_index`]).
    pub(super) fn theme_items() -> Vec<SegmentedItem> {
        vec![
            SegmentedItem::new(
                "follow-system",
                dbflux_i18n::t!("settings.general.theme.option.follow_system"),
            )
            .icon(AppIcon::Layers),
            SegmentedItem::new(
                "dark",
                dbflux_i18n::t!("settings.general.theme.option.dark"),
            )
            .icon(AppIcon::Eye),
            SegmentedItem::new(
                "light",
                dbflux_i18n::t!("settings.general.theme.option.light"),
            )
            .icon(AppIcon::Eye),
        ]
    }

    /// Density segments, in index order (see [`Self::style_index`]).
    pub(super) fn style_items() -> Vec<SegmentedItem> {
        vec![
            SegmentedItem::new("default", Self::style_label(AppStyle::Default)),
            SegmentedItem::new("compact", Self::style_label(AppStyle::Compact)),
        ]
    }

    fn style_label(style: AppStyle) -> String {
        match style {
            AppStyle::Default => dbflux_i18n::t!("settings.general.style.option.default"),
            AppStyle::Compact => dbflux_i18n::t!("settings.general.style.option.compact"),
        }
    }

    fn language_items() -> Vec<DropdownItem> {
        let system_language =
            dbflux_i18n::resolve(None, dbflux_i18n::detect_system_locale().as_deref());

        std::iter::once(DropdownItem::new(Self::system_language_label(
            system_language,
        )))
        .chain(
            dbflux_i18n::Language::available()
                .iter()
                .map(|language| DropdownItem::new(language.native_name())),
        )
        .collect()
    }

    /// The "follow the OS" entry, naming the language the OS locale resolves
    /// to so the user sees what "System" currently means.
    fn system_language_label(system_language: dbflux_i18n::Language) -> String {
        format!(
            "{} ({})",
            dbflux_i18n::t!("settings.general.language.option.system"),
            system_language.native_name()
        )
    }

    /// Focus-on-launch segments, in index order (see
    /// [`Self::startup_focus_index`]).
    pub(super) fn startup_focus_items() -> Vec<SegmentedItem> {
        vec![
            SegmentedItem::new(
                "sidebar",
                dbflux_i18n::t!("settings.general.default_focus.option.sidebar"),
            ),
            SegmentedItem::new(
                "last-tab",
                dbflux_i18n::t!("settings.general.default_focus.option.last_tab"),
            ),
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

    /// Vim leader choices, in index order (see [`Self::vim_leader_index`]).
    fn vim_leader_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new(dbflux_i18n::t!("settings.general.vim_leader.option.space")),
            DropdownItem::new(dbflux_i18n::t!("settings.general.vim_leader.option.comma")),
            DropdownItem::new(dbflux_i18n::t!(
                "settings.general.vim_leader.option.backslash"
            )),
        ]
    }

    /// The dropdown index of the stored leader `vim_leader`, or `None` for a
    /// key the dropdown does not offer.
    pub(super) fn vim_leader_index(vim_leader: &str) -> Option<usize> {
        VIM_LEADER_CHOICES
            .iter()
            .position(|choice| *choice == vim_leader)
    }

    /// The stored leader of the dropdown index `index`.
    pub(super) fn vim_leader_for_index(index: usize) -> &'static str {
        VIM_LEADER_CHOICES.get(index).copied().unwrap_or("space")
    }

    pub(super) fn theme_index(theme: ThemeSetting) -> usize {
        match theme {
            ThemeSetting::System => 0,
            ThemeSetting::Dark => 1,
            ThemeSetting::Light => 2,
        }
    }

    pub(super) fn theme_for_index(index: usize) -> ThemeSetting {
        match index {
            0 => ThemeSetting::System,
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

    pub(super) fn startup_focus_index(focus: StartupFocus) -> usize {
        match focus {
            StartupFocus::Sidebar => 0,
            StartupFocus::LastTab => 1,
        }
    }

    pub(super) fn startup_focus_for_index(index: usize) -> StartupFocus {
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

    fn unsaved_change_count(&self, cx: &App) -> usize {
        self.general_change_count(cx)
    }

    fn save_from_shortcut(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.save_general_settings(window, cx);
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
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.reveal_pending_field(window, cx);
        self.render_general_section(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{FontFamilyField, FontFamilyOption, GeneralFormRow, GeneralSection};
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

    /// Picking a leader in the Vim leader dropdown is an unsaved change until
    /// Save, which stores it and hands it to open editors.
    #[test]
    fn choosing_a_vim_leader_marks_dirty_and_saves() {
        with_general_section(|section, _, window, cx| {
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
        });
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
    fn unsaved_change_count_counts_each_changed_setting() {
        with_general_section(|section, _, window, cx| {
            assert_eq!(section.general_change_count(cx), 0);

            section.gen_settings.vim_mode = !section.gen_settings.vim_mode;
            section.gen_settings.theme = GeneralSection::theme_for_index(
                (GeneralSection::theme_index(section.gen_settings.theme) + 1) % 3,
            );
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

    fn stored_general_settings(
        section: &GeneralSection,
        cx: &gpui::App,
    ) -> Option<(f64, f64, f64)> {
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
        with_general_section(|section, _, window, cx| {
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
        with_general_section(|section, _, window, cx| {
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
        });
    }

    #[test]
    fn invalid_font_size_shows_an_error_and_saves_nothing() {
        with_general_section(|section, toast_host, window, cx| {
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
                    toast_host.read(cx).last_toast_title(),
                    Some(dbflux_i18n::t!(
                        key,
                        min = dbflux_core::GeneralSettings::MIN_FONT_SIZE,
                        max = dbflux_core::GeneralSettings::MAX_FONT_SIZE
                    ))
                );
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
        ];

        for key in keys {
            for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
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
                let section = cx.new(|cx| GeneralSection::new(app_state, window, cx));
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
}
