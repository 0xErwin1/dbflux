use gpui::{BoxShadow, Hsla, Pixels, Point, px, rgb};

pub struct Spacing;

impl Spacing {
    /// Half-step below XS — form-row label padding, chart pills. (6 px)
    ///
    /// Note: XXS (6) > XS (4); non-monotonic by design, locked to px(6.).
    pub const XXS: Pixels = px(6.0);
    pub const XS: Pixels = px(4.0);
    pub const SM: Pixels = px(8.0);
    pub const MD: Pixels = px(12.0);
    pub const LG: Pixels = px(16.0);
    pub const XL: Pixels = px(24.0);
}

pub struct Heights;

impl Heights {
    pub const ROW: Pixels = px(28.0);
    pub const ROW_COMPACT: Pixels = px(24.0);
    pub const HEADER: Pixels = px(40.0);
    pub const TOOLBAR: Pixels = px(32.0);
    pub const TAB: Pixels = px(36.0);
    pub const INPUT: Pixels = px(32.0);
    pub const BUTTON: Pixels = px(28.0);
    /// Standard inline control height (input, dropdown, button) when packed
    /// into a toolbar/filter bar. Use this to keep heterogeneous controls aligned.
    pub const CONTROL: Pixels = px(28.0);
    pub const ICON_SM: Pixels = px(16.0);
    pub const ICON_MD: Pixels = px(20.0);
    pub const ICON_LG: Pixels = px(24.0);
    /// Height of the active-tab indicator stripe — a 1 px absolutely-positioned
    /// child div rendered at the bottom edge of the active tab item.
    pub const TAB_STRIPE: Pixels = px(1.0);
    /// Fixed height of the SQL results panel in Split layout.
    pub const RESULTS_PANEL: Pixels = px(220.0);
}

pub struct FontSizes;

/// Static font-size constants matching `AppStyle::Default` (the project's
/// baseline density). For style-aware sizing at render sites, prefer the
/// `density::font_*(cx)` accessors so the active `AppStyle` is honoured.
impl FontSizes {
    /// Section label — uppercase display-face labels (Default: 11 px).
    pub const LABEL: Pixels = px(11.0);
    /// Extra-small — used for badges, captions, tooltips (Default: 12 px).
    pub const XS: Pixels = px(12.0);
    /// Small — used for labels and secondary metadata (Default: 13 px).
    pub const SM: Pixels = px(13.0);
    /// Base — primary body and input text (Default: 13 px).
    pub const BASE: Pixels = px(13.0);
    /// Large — emphasized labels and nav items (Default: 15 px).
    pub const LG: Pixels = px(15.0);
    /// Extra-large — sub-headings and panel titles (Default: 18 px).
    pub const XL: Pixels = px(18.0);
    /// Title — window-level headings (Default: 20 px).
    pub const TITLE: Pixels = px(20.0);
}

pub struct Radii;

/// Static border-radius constants matching `AppStyle::Default` (square
/// corners). For style-aware radii at render sites, prefer the
/// `density::radius_*(cx)` accessors so the active `AppStyle` is honoured.
impl Radii {
    /// Small radius — controls, inputs, badges (Default: 0 px).
    pub const SM: Pixels = px(0.0);
    /// Medium radius — dropdowns, popovers (Default: 0 px).
    pub const MD: Pixels = px(0.0);
    /// Large radius — modals, cards (Default: 0 px).
    pub const LG: Pixels = px(0.0);
    /// Full radius — pill shapes, avatars, status dots.
    pub const FULL: Pixels = px(9999.0);
}

/// 45° corner-cut depths for chamfered shapes (see `primitives::Chamfer`).
/// The cut is measured along each axis from the cut corner.
pub struct ChamferCut;

impl ChamferCut {
    /// Keycaps, badges, counters (4 px).
    pub const KEYCAP: Pixels = px(4.0);
    /// Controls 28–32 px tall: buttons, selects, icon buttons (6 px).
    pub const CONTROL: Pixels = px(6.0);
    /// Inputs, document tabs (top-left only), segmented controls (8 px).
    pub const INPUT: Pixels = px(8.0);
    /// Menus, large buttons, popovers, toasts, overlays (12 px).
    pub const OVERLAY: Pixels = px(12.0);
    /// Cards (14 px).
    pub const CARD: Pixels = px(14.0);
    /// Modals and hero frames (18 px).
    pub const MODAL: Pixels = px(18.0);
}

/// Geometry of `modals::Modal`, taken from the P1Modals board.
pub struct ModalMetrics;

impl ModalMetrics {
    /// Header bar height.
    pub const HEADER_HEIGHT: Pixels = px(46.0);
    /// Horizontal padding of header, body and footer; also the body padding.
    pub const PADDING: Pixels = px(18.0);
    /// Gap between header items and between icon and title.
    pub const HEADER_GAP: Pixels = px(10.0);
    /// Gap between body blocks.
    pub const BODY_GAP: Pixels = px(14.0);
    /// Vertical padding of the footer.
    pub const FOOTER_PADDING_Y: Pixels = px(12.0);
    /// Gap between footer buttons.
    pub const FOOTER_GAP: Pixels = px(8.0);
    /// Title size (Archivo 700).
    pub const TITLE_SIZE: Pixels = px(14.0);
    /// Leading header icon.
    pub const ICON: Pixels = px(16.0);
    /// Close icon at the end of the header.
    pub const CLOSE_ICON: Pixels = px(13.0);
    /// Thickness of the danger edge along the top of the header.
    pub const DANGER_EDGE: Pixels = px(2.0);
    /// Smallest height of the body area, padding included.
    pub const BODY_MIN_HEIGHT: Pixels = px(96.0);
    /// Default width.
    pub const WIDTH: Pixels = px(480.0);
}

/// Geometry of the three tab kinds (document, result, inline), taken from the
/// AppByzTable, AppByzEditor and P1ConnForm boards.
pub struct TabMetrics;

impl TabMetrics {
    /// Height of the document tab bar; tabs sit at its bottom.
    pub const DOCUMENT_BAR_HEIGHT: Pixels = px(42.0);
    /// Height of one document tab.
    pub const DOCUMENT_TAB_HEIGHT: Pixels = px(36.0);
    /// Space above a document tab inside its bar.
    pub const DOCUMENT_TAB_TOP: Pixels = px(6.0);
    /// Horizontal padding of a document tab.
    pub const DOCUMENT_TAB_PADDING_X: Pixels = px(14.0);
    /// Left padding of the active document tab, which clears its cut.
    pub const DOCUMENT_TAB_ACTIVE_PADDING_LEFT: Pixels = px(16.0);
    /// Gap between icon, title and trailing items of a document tab.
    pub const DOCUMENT_TAB_GAP: Pixels = px(9.0);
    /// Leading icon of a document tab.
    pub const ICON: Pixels = px(15.0);
    /// Close icon and spinner of a document tab.
    pub const CLOSE_ICON: Pixels = px(13.0);
    /// Left padding of the document tab bar.
    pub const DOCUMENT_BAR_PADDING_LEFT: Pixels = px(8.0);
    /// Gap between neighbouring tabs of a document or result bar.
    pub const BAR_GAP: Pixels = px(2.0);
    /// Height of the result tab bar.
    pub const RESULT_BAR_HEIGHT: Pixels = px(40.0);
    /// Horizontal padding of the result tab bar.
    pub const RESULT_BAR_PADDING_X: Pixels = px(10.0);
    /// Height of one result tab.
    pub const RESULT_TAB_HEIGHT: Pixels = px(34.0);
    /// Horizontal padding of a result tab.
    pub const RESULT_TAB_PADDING_X: Pixels = px(12.0);
    /// Gap inside a result or inline tab.
    pub const RESULT_TAB_GAP: Pixels = px(8.0);
    /// Size of the row count and statement range of a result tab.
    pub const RESULT_META_SIZE: Pixels = px(11.0);
    /// Height of one inline tab.
    pub const INLINE_TAB_HEIGHT: Pixels = px(42.0);
    /// Horizontal padding of an inline tab.
    pub const INLINE_TAB_PADDING_X: Pixels = px(14.0);
    /// Horizontal padding of the inline tab bar.
    pub const INLINE_BAR_PADDING_X: Pixels = px(12.0);
    /// Thickness of the byzantine edge that marks the active tab.
    pub const ACTIVE_EDGE: Pixels = px(2.0);
}

/// Geometry of the panel and section headers, taken from AppByzTable and
/// P1SettingsGeneral.
pub struct HeaderMetrics;

impl HeaderMetrics {
    /// Panel header height.
    pub const PANEL_HEIGHT: Pixels = px(40.0);
    /// Left padding of a panel header.
    pub const PANEL_PADDING_LEFT: Pixels = px(16.0);
    /// Right padding of a panel header, which sits next to its actions.
    pub const PANEL_PADDING_RIGHT: Pixels = px(12.0);
    /// Gap between the items of a panel header.
    pub const PANEL_GAP: Pixels = px(8.0);
    /// Top padding of a settings page head.
    pub const SECTION_PADDING_TOP: Pixels = px(22.0);
    /// Bottom padding of a settings page head.
    pub const SECTION_PADDING_BOTTOM: Pixels = px(8.0);
    /// Gap between the title and the description of a settings page head.
    pub const SECTION_GAP: Pixels = px(4.0);
    /// Top padding of a section label row.
    pub const LABEL_PADDING_TOP: Pixels = px(18.0);
    /// Bottom padding and bottom margin of a section label row.
    pub const LABEL_PADDING_BOTTOM: Pixels = px(6.0);
}

/// Geometry of `controls::Button` and `composites::SplitButton`, taken from
/// the DSApp "Buttons" row and the DSStates board.
pub struct ButtonMetrics;

impl ButtonMetrics {
    /// Toolbar and dense-form buttons.
    pub const HEIGHT_SM: Pixels = px(28.0);
    /// Default buttons (DSStates).
    pub const HEIGHT_MD: Pixels = px(32.0);
    /// Large call-to-action buttons.
    pub const HEIGHT_LG: Pixels = px(44.0);

    /// Width of an icon-only button per size: the DSApp icon buttons are two
    /// pixels wider than tall, DSStates draws the 32 px ghost icon 40 wide,
    /// and the large one is square.
    pub const ICON_ONLY_WIDTH_SM: Pixels = px(30.0);
    pub const ICON_ONLY_WIDTH_MD: Pixels = px(40.0);
    pub const ICON_ONLY_WIDTH_LG: Pixels = px(44.0);

    /// Horizontal padding of a labeled button (small and medium).
    pub const PADDING_X: Pixels = px(12.0);
    /// Horizontal padding of a large labeled button.
    pub const PADDING_X_LG: Pixels = px(16.0);
    /// Left padding of a split button's main action, whose only cut is the
    /// top-left corner.
    pub const SPLIT_MAIN_PADDING_LEFT: Pixels = px(14.0);
    /// Gap between icon, label, and trailing keycap.
    pub const GAP: Pixels = px(8.0);

    pub const FONT_SM: Pixels = px(12.5);
    pub const FONT_MD: Pixels = px(13.0);

    /// Icon leading a label.
    pub const ICON: Pixels = px(15.0);
    /// Icon of an icon-only button.
    pub const ICON_ONLY: Pixels = px(16.0);

    /// Width of a split button's menu segment.
    pub const SPLIT_MENU_WIDTH: Pixels = px(26.0);
    /// Gap between a split button's two segments.
    pub const SPLIT_SEAM: Pixels = px(1.0);

    /// Fill alphas of the danger variant (rest, hover, pressed) and of a
    /// selected ghost or secondary button (DSStates).
    pub const SOFT_FILL_REST: f32 = 0.14;
    pub const SOFT_FILL_HOVER: f32 = 0.22;
    pub const SOFT_FILL_PRESSED: f32 = 0.30;

    /// Opacity of a disabled button, fill and content together.
    pub const DISABLED_OPACITY: f32 = 0.45;
}

/// Geometry of `primitives::Kbd`, from the keycaps in AppByzTable and DSApp.
pub struct KbdMetrics;

impl KbdMetrics {
    pub const FONT: Pixels = px(10.5);
    pub const LINE_HEIGHT: Pixels = px(14.0);
    pub const PADDING_X: Pixels = px(6.0);
    pub const PADDING_Y: Pixels = px(2.0);
    /// Padding of a keycap drawn on a filled (primary) button.
    pub const ON_FILL_PADDING_X: Pixels = px(5.0);
    pub const ON_FILL_PADDING_Y: Pixels = px(1.0);
    /// Fill alpha of a keycap drawn on a filled button.
    pub const ON_FILL_ALPHA: f32 = 0.14;
    /// Gap between the keycaps of a chord and the plus that joins them.
    pub const CHORD_GAP: Pixels = px(3.0);
}

/// Border-width tokens. WIDTH context only — `.border_*` widths, stripe
/// thicknesses. Do NOT use for margins, paddings, or radii.
pub struct Borders;

impl Borders {
    /// Hairline border — default control/separator edge. (1 px)
    pub const THIN: Pixels = px(1.0);
    /// Emphasis border — danger accents, active-state edges. (2 px)
    pub const MEDIUM: Pixels = px(2.0);
    /// Keyboard focus ring traced around chamfered controls. (1.5 px)
    pub const FOCUS_RING: Pixels = px(1.5);
}

/// Metrics of the chamfered input family: text fields, select triggers, the
/// filter field, segmented controls, checkboxes and select menus.
pub struct Fields;

impl Fields {
    /// Text field and select trigger height. (30 px)
    pub const HEIGHT: Pixels = px(30.0);
    /// Height of a small text field packed into a dense toolbar. (24 px)
    pub const HEIGHT_SMALL: Pixels = px(24.0);
    /// Horizontal padding inside a text field or select trigger. (10 px)
    pub const PADDING_X: Pixels = px(10.0);
    /// Gap between the parts of a field: icon, value, suffix. (8 px)
    pub const GAP: Pixels = px(8.0);
    /// Value text size inside a text field or select trigger. (12.5 px)
    pub const TEXT: Pixels = px(12.5);
    /// Select trigger chevron size. (12 px)
    pub const CHEVRON: Pixels = px(12.0);

    /// Filter field height (WHERE ... LIMIT). (34 px)
    pub const FILTER_HEIGHT: Pixels = px(34.0);
    /// Horizontal padding inside the filter field. (12 px)
    pub const FILTER_PADDING_X: Pixels = px(12.0);
    /// Gap between the parts of the filter field. (10 px)
    pub const FILTER_GAP: Pixels = px(10.0);
    /// Filter field leading icon size. (15 px)
    pub const FILTER_ICON: Pixels = px(15.0);
    /// Width reserved for the LIMIT value inside the filter field. (48 px)
    pub const FILTER_LIMIT_WIDTH: Pixels = px(48.0);

    /// Segment height inside a segmented control. (26 px)
    pub const SEGMENT_HEIGHT: Pixels = px(26.0);
    /// Padding between the segmented track and its segments. (2 px)
    pub const SEGMENT_TRACK_PADDING: Pixels = px(2.0);
    /// Horizontal padding of one segment. (10 px)
    pub const SEGMENT_PADDING_X: Pixels = px(10.0);
    /// Gap between a segment's icon and label. (6 px)
    pub const SEGMENT_GAP: Pixels = px(6.0);
    /// Segment icon size. (13 px)
    pub const SEGMENT_ICON: Pixels = px(13.0);

    /// Checkbox box size. (16 px)
    pub const CHECKBOX_SIZE: Pixels = px(16.0);
    /// Check mark size inside a checked box. (12 px)
    pub const CHECK_MARK: Pixels = px(12.0);
    /// Gap between a checkbox and its label. (10 px)
    pub const CHECKBOX_GAP: Pixels = px(10.0);

    /// Vertical padding of a select menu. (8 px)
    pub const MENU_PADDING_Y: Pixels = px(8.0);
    /// Horizontal inset of a select menu row inside the menu. (6 px)
    pub const MENU_ROW_INSET: Pixels = px(6.0);
    /// Select menu row height. (30 px)
    pub const MENU_ROW_HEIGHT: Pixels = px(30.0);
    /// Maximum select menu height before it scrolls. (220 px)
    pub const MENU_MAX_HEIGHT: Pixels = px(220.0);
    /// Alpha of the tint wash behind the highlighted select menu row.
    pub const MENU_HIGHLIGHT_ALPHA: f32 = 0.14;
    /// Opacity of a disabled control's rest fill.
    pub const DISABLED_OPACITY: f32 = 0.45;
}

/// Centralized box-shadow definitions.
///
/// Use these instead of constructing `BoxShadow` at call sites so the shadow
/// treatment stays consistent across the app.
pub struct Shadows;

impl Shadows {
    /// Medium shadow — used for elevated panels, dropdowns, and tooltips.
    ///
    /// Equivalent to a subtle single-layer downward shadow with moderate blur.
    pub fn md() -> BoxShadow {
        BoxShadow {
            color: gpui::hsla(0.0, 0.0, 0.0, 0.24),
            offset: Point {
                x: px(0.0),
                y: px(4.0),
            },
            blur_radius: px(8.0),
            spread_radius: px(0.0),
            inset: false,
        }
    }

    /// Large shadow — used for modals, overlays, and floating windows.
    ///
    /// Two-layer shadow: a large diffuse spread plus a tight close shadow for
    /// depth perception.
    pub fn lg() -> BoxShadow {
        BoxShadow {
            color: gpui::hsla(0.0, 0.0, 0.0, 0.32),
            offset: Point {
                x: px(0.0),
                y: px(8.0),
            },
            blur_radius: px(24.0),
            spread_radius: px(0.0),
            inset: false,
        }
    }

    /// Left-edge shadow for slide-in inspector panels.
    ///
    /// Casts the shadow to the left (negative x offset) to give the panel a
    /// sense of depth relative to the content it overlays.
    pub fn inspector_left() -> BoxShadow {
        BoxShadow {
            color: gpui::hsla(0.0, 0.0, 0.0, 0.28),
            offset: Point {
                x: px(-6.0),
                y: px(0.0),
            },
            blur_radius: px(16.0),
            spread_radius: px(0.0),
            inset: false,
        }
    }
}

pub struct ChromeColors;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChromeColorSlot {
    Background,
    Secondary,
    Border,
    Input,
    Popover,
}

impl ChromeColorSlot {
    pub fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        match self {
            Self::Background => theme.background,
            Self::Secondary => theme.secondary,
            Self::Border => theme.border,
            Self::Input => theme.input,
            Self::Popover => theme.popover,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChromeEdgeRole {
    Surface,
    Separator,
    Control,
    Popover,
    ModalSeparator,
}

impl ChromeEdgeRole {
    pub fn color_slot(self) -> ChromeColorSlot {
        match self {
            Self::Surface | Self::Separator | Self::Control | Self::ModalSeparator => {
                ChromeColorSlot::Input
            }
            Self::Popover => ChromeColorSlot::Border,
        }
    }

    pub fn resolve(self, theme: &gpui_component::Theme) -> Hsla {
        self.color_slot().resolve(theme)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChromeSurfaceRole {
    ControlShell,
    PopoverShell,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChromeSurfaceInspection {
    pub background: ChromeColorSlot,
    pub edge: ChromeEdgeRole,
    pub radius: Pixels,
}

impl ChromeSurfaceRole {
    pub fn inspect(self) -> ChromeSurfaceInspection {
        match self {
            Self::ControlShell => ChromeSurfaceInspection {
                background: ChromeColorSlot::Secondary,
                edge: ChromeEdgeRole::Control,
                radius: Radii::SM,
            },
            Self::PopoverShell => ChromeSurfaceInspection {
                background: ChromeColorSlot::Popover,
                edge: ChromeEdgeRole::Popover,
                radius: Radii::MD,
            },
        }
    }
}

impl ChromeColors {
    /// Structural separator between major UI regions: the palette line.
    pub fn ghost_border(theme: &gpui_component::Theme) -> Hsla {
        theme.border
    }

    /// Text-accent tint: `#D48CC8` on dark, byzantine `#702963` on light.
    ///
    /// The palette assigns the tint to the focus ring, so this reads `ring`.
    pub fn tint(theme: &gpui_component::Theme) -> Hsla {
        theme.ring
    }

    /// Strong text for titles and data: `#F7F4F7` on dark, `#141118` on light.
    ///
    /// The palette assigns strong text to `accent_foreground`, so this reads it.
    pub fn strong(theme: &gpui_component::Theme) -> Hsla {
        theme.accent_foreground
    }
}

/// Syntax roles of the Bolt Byzantium palette, per variant.
///
/// The code editor's highlight theme and the schema-tree icons both read these
/// roles, so SQL text and tree glyphs share one color language.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SyntaxColors {
    pub keyword: Hsla,
    pub string: Hsla,
    /// Numbers and NULL literals.
    pub number: Hsla,
    pub comment: Hsla,
    /// Types and built-in identifiers.
    pub type_name: Hsla,
    pub function: Hsla,
    /// Operators and punctuation.
    pub operator: Hsla,
    /// Plain identifiers.
    pub plain: Hsla,
}

impl SyntaxColors {
    pub fn dark() -> Self {
        Self {
            keyword: rgb(0xD48CC8).into(),
            string: rgb(0x7BE0A0).into(),
            number: rgb(0xB79CFF).into(),
            comment: rgb(0x8E8996).into(),
            type_name: rgb(0x6EA8FF).into(),
            function: rgb(0xFFC23D).into(),
            operator: rgb(0xC6C3CC).into(),
            plain: rgb(0xF7F4F7).into(),
        }
    }

    pub fn light() -> Self {
        Self {
            keyword: rgb(0x702963).into(),
            string: rgb(0x1C7F45).into(),
            number: rgb(0x6B4FD8).into(),
            comment: rgb(0x6B6572).into(),
            type_name: rgb(0x1F5FD1).into(),
            function: rgb(0xB7791F).into(),
            operator: rgb(0x3B3740).into(),
            plain: rgb(0x141118).into(),
        }
    }

    /// Return the `SyntaxColors` for the currently active theme.
    ///
    /// Reads `ThemeSettingGlobal` from `cx`; falls back to Dark when absent.
    pub fn for_current(cx: &gpui::App) -> Self {
        match crate::semantic::ThemeSettingGlobal::get(cx) {
            dbflux_core::ThemeSetting::Light => Self::light(),
            dbflux_core::ThemeSetting::Dark | dbflux_core::ThemeSetting::System => Self::dark(),
        }
    }

    /// Schema-tree table icon.
    pub fn table(&self) -> Hsla {
        self.type_name
    }

    /// Schema-tree view icon.
    pub fn view(&self) -> Hsla {
        self.function
    }

    /// Schema-tree column icon.
    pub fn column(&self) -> Hsla {
        self.operator
    }

    /// Schema-tree custom type icon.
    pub fn type_item(&self) -> Hsla {
        self.number
    }

    /// Schema-tree folder icon.
    pub fn folder_dim(&self) -> Hsla {
        self.comment
    }

    /// Schema-tree database icon.
    pub fn database(&self) -> Hsla {
        self.string
    }

    /// Schema-tree schema icon.
    pub fn schema(&self) -> Hsla {
        self.keyword
    }
}

/// Row-state background tints for the data grid, derived from the active
/// theme's semantic colors so both palettes get matching washes.
pub struct RowColors;

impl RowColors {
    /// Even-row alternating tint — delegates to the theme's built-in `table_even`.
    pub fn even(theme: &gpui_component::Theme) -> Hsla {
        theme.table_even
    }

    /// Odd rows use the transparent base surface (no tint).
    pub fn odd(_theme: &gpui_component::Theme) -> Hsla {
        gpui::hsla(0.0, 0.0, 0.0, 0.0)
    }

    /// Pending-insert row: success at 15%.
    pub fn insert(theme: &gpui_component::Theme) -> Hsla {
        Hsla {
            a: 0.15,
            ..theme.success
        }
    }

    /// Dirty (unsaved edit) row: warning at 20%.
    pub fn dirty(theme: &gpui_component::Theme) -> Hsla {
        Hsla {
            a: 0.20,
            ..theme.warning
        }
    }

    /// Pending-delete row: danger at 10%.
    pub fn delete(theme: &gpui_component::Theme) -> Hsla {
        Hsla {
            a: 0.10,
            ..theme.danger
        }
    }

    /// Row with a validation error: danger at 15%.
    pub fn error(theme: &gpui_component::Theme) -> Hsla {
        Hsla {
            a: 0.15,
            ..theme.danger
        }
    }

    /// In-flight save row: warning at 10%.
    pub fn saving(theme: &gpui_component::Theme) -> Hsla {
        Hsla {
            a: 0.10,
            ..theme.warning
        }
    }
}

/// Geometry of the status and feedback components (badge, environment tag,
/// status diamond, banner, toast, spinner), measured from the Bolt Byzantium
/// app boards.
pub struct Feedback;

impl Feedback {
    /// Badge height (20 px).
    pub const BADGE_HEIGHT: Pixels = px(20.0);
    /// Badge horizontal padding (7 px).
    pub const BADGE_PADDING_X: Pixels = px(7.0);
    /// Badge label size (11 px, semibold).
    pub const BADGE_FONT: Pixels = px(11.0);
    /// Opacity of the kind color behind a badge label.
    pub const BADGE_FILL_ALPHA: f32 = 0.14;

    /// Environment tag padding: 1 px vertical, 6 px horizontal.
    pub const ENV_TAG_PADDING_Y: Pixels = px(1.0);
    pub const ENV_TAG_PADDING_X: Pixels = px(6.0);
    /// Environment tag label size (10 px, bold, 0.08 em tracking).
    pub const ENV_TAG_FONT: Pixels = px(10.0);
    pub const ENV_TAG_TRACKING_EM: f32 = 0.08;
    /// Opacity of the kind color behind an environment tag.
    pub const ENV_TAG_FILL_ALPHA: f32 = 0.16;

    /// Status diamond next to connection names and in the status bar (7 px).
    pub const STATUS_DIAMOND: Pixels = px(7.0);
    /// Compact status diamond, used for the document-tab dirty marker (6 px).
    pub const STATUS_DIAMOND_COMPACT: Pixels = px(6.0);
    /// Gap between the diamond and its label (6 px).
    pub const STATUS_GAP: Pixels = px(6.0);

    /// Banner padding: 10 px vertical, 14 px horizontal.
    pub const BANNER_PADDING_Y: Pixels = px(10.0);
    pub const BANNER_PADDING_X: Pixels = px(14.0);
    /// Gap between the banner icon and its text (10 px).
    pub const BANNER_GAP: Pixels = px(10.0);
    /// Banner icon size (15 px).
    pub const BANNER_ICON: Pixels = px(15.0);
    /// Banner edge stripe width (3 px).
    pub const BANNER_STRIPE: Pixels = px(3.0);
    /// Opacity of the kind color on the banner field.
    pub const BANNER_FILL_ALPHA: f32 = 0.08;

    /// Toast width (440 px).
    pub const TOAST_WIDTH: Pixels = px(440.0);
    /// Toast padding: 12 px vertical, 14 px horizontal.
    pub const TOAST_PADDING_Y: Pixels = px(12.0);
    pub const TOAST_PADDING_X: Pixels = px(14.0);
    /// Gap between toast rows (8 px) and inside the title row (10 px).
    pub const TOAST_ROW_GAP: Pixels = px(8.0);
    pub const TOAST_TITLE_GAP: Pixels = px(10.0);
    /// Gap between a toast title and its subtitle (2 px).
    pub const TOAST_TITLE_LINE_GAP: Pixels = px(2.0);
    /// Toast kind icon (16 px) and close icon (12 px).
    pub const TOAST_ICON: Pixels = px(16.0);
    pub const TOAST_CLOSE_ICON: Pixels = px(12.0);
    /// Toast edge stripe width (4 px).
    pub const TOAST_STRIPE: Pixels = px(4.0);
    /// Toast body text (12.5 px) and timestamp / percentage (11 px, mono).
    pub const TOAST_BODY_FONT: Pixels = px(12.5);
    pub const TOAST_META_FONT: Pixels = px(11.0);
    /// Toast progress track height (4 px).
    pub const TOAST_PROGRESS_HEIGHT: Pixels = px(4.0);
    /// Distance between the toast stack and the document area edges (16 px).
    pub const TOAST_STACK_INSET: Pixels = px(16.0);

    /// Spinner box (16 px) holding the bolt glyph (12 x 14 px).
    pub const SPINNER_BOX: Pixels = px(16.0);
    pub const SPINNER_BOLT_WIDTH: Pixels = px(12.0);
    pub const SPINNER_BOLT_HEIGHT: Pixels = px(14.0);
    /// Opacity of the unfilled part of the bolt.
    pub const SPINNER_TRACK_ALPHA: f32 = 0.25;
}

/// Shared animation timing constants.
pub struct Anim;

impl Anim {
    /// Interval between pulse steps in milliseconds.
    pub const PULSE_INTERVAL_MS: u64 = 100;

    /// Duration of a cross-fade transition in milliseconds.
    pub const FADE_MS: u64 = 120;

    /// Foundations motion "fast": hover and press color transitions.
    pub const FAST_MS: u64 = 150;
}

/// Chart-specific geometry tokens — fonts, gaps, swatch/dot sizes, row heights,
/// and reserved column widths used by chart element factories (`axis_bar`,
/// `point_inspector`, `legend`).
///
/// Chart chrome uses smaller fonts than the standard UI scale and a handful of
/// chart-only widths that do not belong in the generic `Widths` namespace.
/// Canvas paint geometry (line widths, tick lengths) lives directly in
/// `chart/engine.rs` and is exempt from the spacing guardrail.
pub struct ChartGeometry;

impl ChartGeometry {
    /// Tiny chart font — counter text, tick labels. (10 px, smaller than `FontSizes::XS`)
    pub const FONT_TINY: Pixels = px(10.0);

    /// Chart label font — legend chips, dropdown rows. (11 px)
    pub const FONT_LABEL: Pixels = px(11.0);

    /// Hairline accent stripe inside chart chrome. (1 px)
    pub const HAIRLINE: Pixels = px(1.0);

    /// Accent stripe (medium emphasis) — divider lines, checked-state borders. (2 px)
    pub const ACCENT_STRIPE: Pixels = px(2.0);

    /// Tick/gap accent — small gaps between chart sub-elements and tick spacing. (3 px)
    pub const TICK_GAP: Pixels = px(3.0);

    /// Color swatch / status dot dimension. (10 px square)
    pub const SWATCH: Pixels = px(10.0);

    /// Row height in chart dropdowns and inspector lists. (11 px)
    pub const ROW: Pixels = px(11.0);

    /// Reserved width for short axis tick labels. (60 px)
    pub const SHORT_LABEL_COL: Pixels = px(60.0);

    /// Reserved width for the point-inspector value column. (80 px)
    pub const VALUE_COL: Pixels = px(80.0);

    /// Axis-bar dropdown panel width. (140 px)
    pub const DROPDOWN_PANEL: Pixels = px(140.0);
}

pub struct Widths;

impl Widths {
    /// Width of the row inspector overlay panel.
    pub const INSPECTOR: Pixels = px(320.0);

    /// Label column width in settings form grid rows (drivers, hooks sections).
    ///
    /// Applied to the fixed-width left column that holds field labels and
    /// dropdown controls in two-column settings forms. (220 px)
    pub const SETTINGS_FORM_LABEL: Pixels = px(220.0);

    /// Dropdown column width in connection manager form rows.
    ///
    /// Applied to dropdown and field-control wrappers in the connection manager
    /// tabs (hooks, render, access, drivers). (240 px)
    pub const CM_FORM_DROPDOWN: Pixels = px(240.0);

    /// Left list-panel width in settings sections with a master/detail layout.
    ///
    /// Applied to the left panel (`border_r_1`) listing selectable items in
    /// MCP (clients, roles, policies) and driver settings sections. (300 px)
    pub const SETTINGS_LIST_PANEL: Pixels = px(300.0);

    /// Left list-panel width for the Connection Manager MCP tab's trusted
    /// client list.
    ///
    /// The Connection Manager window (720x620) is narrower than the Settings
    /// window (950x700), so this panel uses a smaller width than
    /// `SETTINGS_LIST_PANEL`. (220 px)
    pub const CONNECTION_MCP_LIST_PANEL: Pixels = px(220.0);
}

#[cfg(test)]
mod tests {
    use super::{
        Borders, ChartGeometry, ChromeColorSlot, ChromeEdgeRole, ChromeSurfaceRole, FontSizes,
        Radii, Shadows, Spacing,
    };
    use gpui::px;

    // Static-constant baseline: matches AppStyle::Default (project's flat,
    // larger-text default). Style-aware sites use density::font_*/radius_*.
    #[test]
    fn font_sizes_match_default_style_scale() {
        assert_eq!(FontSizes::LABEL, px(11.0));
        assert_eq!(FontSizes::XS, px(12.0));
        assert_eq!(FontSizes::SM, px(13.0));
        assert_eq!(FontSizes::BASE, px(13.0));
        assert_eq!(FontSizes::LG, px(15.0));
        assert_eq!(FontSizes::XL, px(18.0));
        assert_eq!(FontSizes::TITLE, px(20.0));
    }

    #[test]
    fn radii_match_default_style_scale() {
        assert_eq!(Radii::SM, px(0.0));
        assert_eq!(Radii::MD, px(0.0));
        assert_eq!(Radii::LG, px(0.0));
        assert_eq!(Radii::FULL, px(9999.0));
    }

    #[test]
    fn shadows_md_has_expected_geometry() {
        let shadow = Shadows::md();
        assert_eq!(shadow.offset.y, px(4.0));
        assert_eq!(shadow.blur_radius, px(8.0));
        assert_eq!(shadow.spread_radius, px(0.0));
        assert!((shadow.color.a - 0.24).abs() < 0.001);
    }

    #[test]
    fn shadows_lg_has_expected_geometry() {
        let shadow = Shadows::lg();
        assert_eq!(shadow.offset.y, px(8.0));
        assert_eq!(shadow.blur_radius, px(24.0));
        assert_eq!(shadow.spread_radius, px(0.0));
        assert!((shadow.color.a - 0.32).abs() < 0.001);
    }

    #[test]
    fn chrome_edge_roles_map_to_low_emphasis_theme_slots() {
        assert_eq!(ChromeEdgeRole::Surface.color_slot(), ChromeColorSlot::Input);
        assert_eq!(
            ChromeEdgeRole::Separator.color_slot(),
            ChromeColorSlot::Input
        );
        assert_eq!(ChromeEdgeRole::Control.color_slot(), ChromeColorSlot::Input);
        assert_eq!(
            ChromeEdgeRole::Popover.color_slot(),
            ChromeColorSlot::Border
        );
        assert_eq!(
            ChromeEdgeRole::ModalSeparator.color_slot(),
            ChromeColorSlot::Input
        );
    }

    #[test]
    fn chrome_surface_roles_capture_tight_controls_and_popover_shells() {
        let control = ChromeSurfaceRole::ControlShell.inspect();
        assert_eq!(control.background, ChromeColorSlot::Secondary);
        assert_eq!(control.edge, ChromeEdgeRole::Control);
        assert_eq!(control.radius, Radii::SM);

        let popover = ChromeSurfaceRole::PopoverShell.inspect();
        assert_eq!(popover.background, ChromeColorSlot::Popover);
        assert_eq!(popover.edge, ChromeEdgeRole::Popover);
        assert_eq!(popover.radius, Radii::MD);
    }

    #[test]
    fn spacing_xxs_equals_px_6() {
        assert_eq!(Spacing::XXS, px(6.0));
    }

    #[test]
    fn borders_thin_equals_px_1() {
        assert_eq!(Borders::THIN, px(1.0));
    }

    #[test]
    fn borders_medium_equals_px_2() {
        assert_eq!(Borders::MEDIUM, px(2.0));
    }

    #[test]
    fn chart_geometry_tokens_match_documented_values() {
        assert_eq!(ChartGeometry::FONT_TINY, px(10.0));
        assert_eq!(ChartGeometry::FONT_LABEL, px(11.0));
        assert_eq!(ChartGeometry::HAIRLINE, px(1.0));
        assert_eq!(ChartGeometry::ACCENT_STRIPE, px(2.0));
        assert_eq!(ChartGeometry::TICK_GAP, px(3.0));
        assert_eq!(ChartGeometry::SWATCH, px(10.0));
        assert_eq!(ChartGeometry::ROW, px(11.0));
        assert_eq!(ChartGeometry::SHORT_LABEL_COL, px(60.0));
        assert_eq!(ChartGeometry::VALUE_COL, px(80.0));
        assert_eq!(ChartGeometry::DROPDOWN_PANEL, px(140.0));
    }
}
