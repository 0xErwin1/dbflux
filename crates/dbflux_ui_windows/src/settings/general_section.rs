use super::SettingsSection;
use super::SettingsSectionId;
use super::section_trait::SectionFocusEvent;
use dbflux_components::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::SegmentedItem;
use dbflux_components::typography::AppFonts;
use dbflux_core::{
    AppStyle, GeneralSettings, RefreshPolicySetting, ScrollbarVisibility, StartupFocus,
    ThemeSetting,
};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::prelude::*;
use gpui::*;
use gpui_component::IndexPath;
use gpui_component::select::{SearchableVec, SelectEvent, SelectItem, SelectState};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

/// The leader keys Settings > General offers, in the keymap's stored key
/// form: Space, and Vim's usual alternatives, comma and backslash (Vim's own
/// default).
const VIM_LEADER_CHOICES: [&str; 3] = ["space", ",", "\\"];

/// The auto-dismiss delays Settings > General offers, in seconds. `0` keeps
/// every toast until the user dismisses it.
const TOAST_TIMEOUT_CHOICES: [u32; 4] = [4, 8, 15, 0];

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum GeneralFormRow {
    Theme,
    Style,
    Scrollbars,
    Language,
    UiFontFamily,
    UiFontSize,
    EditorFontFamily,
    EditorFontSize,
    GridFontFamily,
    GridFontSize,
    VimMode,
    VimLeader,
    TableAlias,
    TableAliasUseAs,
    GridFkLabels,
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
    ToastTimeout,
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

/// Which of the pages that edit the stored general settings a
/// [`GeneralSection`] shows. Both read and write the same
/// `GeneralSettings`; each lists, validates and counts only its own fields.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum GeneralPage {
    General,
    Appearance,
}

pub(super) struct GeneralSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) page: GeneralPage,
    pub(super) gen_settings: GeneralSettings,
    pub(super) gen_form_cursor: usize,
    pub(super) gen_editing_field: bool,
    /// Nightly-only: whether this build is opted into the stable database.
    /// Backed by a pre-database marker file, applied on the next launch.
    pub(super) gen_share_stable_db: bool,
    pub(super) dropdown_language: Entity<Dropdown>,
    pub(super) dropdown_refresh_policy: Entity<Dropdown>,
    pub(super) dropdown_vim_leader: Entity<Dropdown>,
    pub(super) dropdown_toast_timeout: Entity<Dropdown>,
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
        page: GeneralPage,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let settings = app_state.read(cx).general_settings().clone();
        let language_index = Self::language_index(&settings.language);
        let refresh_policy_index = Self::refresh_policy_index(settings.default_refresh_policy);
        let vim_leader_index = Self::vim_leader_index(&settings.vim_leader);
        let toast_timeout_index = Self::toast_timeout_index(settings.toast_auto_dismiss_secs);
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
        let dropdown_toast_timeout = cx.new(move |_cx| {
            Dropdown::new("general-toast-timeout")
                .placeholder(dbflux_i18n::t!("settings.general.toast_timeout.label"))
                .items(Self::toast_timeout_items())
                .selected_index(toast_timeout_index)
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

        let toast_timeout_subscription = cx.subscribe(
            &dropdown_toast_timeout,
            |this, _, event: &DropdownSelectionChanged, cx| {
                this.gen_settings.toast_auto_dismiss_secs =
                    Self::toast_timeout_for_index(event.index);
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

        let saved_font_sizes_subscription =
            cx.subscribe_in(&app_state, window, Self::sync_saved_font_sizes);

        Self {
            app_state,
            page,
            gen_settings: settings,
            gen_form_cursor: 0,
            gen_editing_field: false,
            gen_share_stable_db: dbflux_storage::paths::nightly_shares_stable_db(),
            dropdown_language,
            dropdown_refresh_policy,
            dropdown_vim_leader,
            dropdown_toast_timeout,
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
                toast_timeout_subscription,
                blur_max_history,
                blur_auto_save,
                blur_refresh_interval,
                blur_max_bg_tasks,
                blur_editor_row_limit,
                blur_object_preview_limit,
                blur_key_value_size_limit,
                saved_font_sizes_subscription,
            ]
            .into_iter()
            .chain(font_subscriptions)
            .collect(),
        }
    }

    /// Font sizes also change outside this window (the interface zoom
    /// commands). Show the new saved size in every field the user has not
    /// edited, so Save does not write the old size back.
    fn sync_saved_font_sizes(
        &mut self,
        app_state: &Entity<AppStateEntity>,
        _event: &AppStateChanged,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let saved = app_state.read(cx).general_settings();
        let fields = [
            (
                &self.input_ui_font_size,
                &mut self.gen_settings.ui_font_size,
                saved.ui_font_size,
            ),
            (
                &self.input_editor_font_size,
                &mut self.gen_settings.editor_font_size,
                saved.editor_font_size,
            ),
            (
                &self.input_grid_font_size,
                &mut self.gen_settings.grid_font_size,
                saved.grid_font_size,
            ),
        ];

        for (input, synced, saved_size) in fields {
            if *synced == saved_size {
                continue;
            }

            let untouched = input.read(cx).value().as_ref() == synced.to_string();
            *synced = saved_size;

            if untouched {
                input.update(cx, |input, cx| {
                    input.set_value(saved_size.to_string(), window, cx)
                });
            }
        }

        cx.notify();
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

    /// Scrollbar visibility segments, in index order (see
    /// [`Self::scrollbar_visibility_index`]).
    pub(super) fn scrollbar_visibility_items() -> Vec<SegmentedItem> {
        [
            ("while-scrolling", ScrollbarVisibility::WhileScrolling),
            ("on-hover", ScrollbarVisibility::OnHover),
            ("always", ScrollbarVisibility::Always),
        ]
        .into_iter()
        .map(|(id, visibility)| {
            SegmentedItem::new(id, Self::scrollbar_visibility_label(visibility))
        })
        .collect()
    }

    fn scrollbar_visibility_label(visibility: ScrollbarVisibility) -> String {
        match visibility {
            ScrollbarVisibility::WhileScrolling => {
                dbflux_i18n::t!("settings.general.scrollbars.option.while_scrolling")
            }
            ScrollbarVisibility::OnHover => {
                dbflux_i18n::t!("settings.general.scrollbars.option.on_hover")
            }
            ScrollbarVisibility::Always => {
                dbflux_i18n::t!("settings.general.scrollbars.option.always")
            }
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

    /// Toast timeout choices, in index order (see [`Self::toast_timeout_index`]).
    fn toast_timeout_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new(dbflux_i18n::t!("settings.general.toast_timeout.option.4s")),
            DropdownItem::new(dbflux_i18n::t!("settings.general.toast_timeout.option.8s")),
            DropdownItem::new(dbflux_i18n::t!("settings.general.toast_timeout.option.15s")),
            DropdownItem::new(dbflux_i18n::t!(
                "settings.general.toast_timeout.option.never"
            )),
        ]
    }

    /// The dropdown index of the stored auto-dismiss delay, or `None` for a
    /// value the dropdown does not offer.
    pub(super) fn toast_timeout_index(secs: u32) -> Option<usize> {
        TOAST_TIMEOUT_CHOICES
            .iter()
            .position(|choice| *choice == secs)
    }

    /// The stored auto-dismiss delay, in seconds, of the dropdown index
    /// `index`.
    pub(super) fn toast_timeout_for_index(index: usize) -> u32 {
        TOAST_TIMEOUT_CHOICES
            .get(index)
            .copied()
            .unwrap_or(GeneralSettings::DEFAULT_TOAST_AUTO_DISMISS_SECS)
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

    pub(super) fn scrollbar_visibility_index(visibility: ScrollbarVisibility) -> usize {
        match visibility {
            ScrollbarVisibility::WhileScrolling => 0,
            ScrollbarVisibility::OnHover => 1,
            ScrollbarVisibility::Always => 2,
        }
    }

    pub(super) fn scrollbar_visibility_for_index(index: usize) -> ScrollbarVisibility {
        match index {
            1 => ScrollbarVisibility::OnHover,
            2 => ScrollbarVisibility::Always,
            _ => ScrollbarVisibility::WhileScrolling,
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
        match self.page {
            GeneralPage::General => SettingsSectionId::General,
            GeneralPage::Appearance => SettingsSectionId::Appearance,
        }
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

        match self.page {
            GeneralPage::General => self.render_general_section(cx).into_any_element(),
            GeneralPage::Appearance => self.render_appearance_section(cx).into_any_element(),
        }
    }
}

#[cfg(test)]
mod tests;
