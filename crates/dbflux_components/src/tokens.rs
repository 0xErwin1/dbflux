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
    pub const INPUT: Pixels = px(30.0);
    pub const BUTTON: Pixels = px(30.0);
    /// Standard control height (input, dropdown, button) when packed into a
    /// toolbar or filter bar. Use this to keep heterogeneous controls aligned.
    pub const CONTROL: Pixels = px(30.0);
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
    /// Controls 24–30 px tall: buttons, selects, icon buttons (6 px).
    pub const CONTROL: Pixels = px(6.0);
    /// Inputs, document tabs (top-left only), segmented controls (8 px).
    pub const INPUT: Pixels = px(8.0);
    /// Large 44 px buttons (10 px).
    pub const LARGE_CONTROL: Pixels = px(10.0);
    /// Menus, popovers, toasts, overlays (12 px).
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

    /// Lead sentence at the top of a body: 13.5 px on a 1.6 line height.
    pub const LEAD_FONT: Pixels = px(13.5);
    pub const LEAD_LINE_HEIGHT: f32 = 1.6;
    /// Statement block: cut 8, 10 px vertical and 12 px horizontal padding,
    /// 12.5 px mono.
    pub const CODE_PADDING_Y: Pixels = px(10.0);
    pub const CODE_PADDING_X: Pixels = px(12.0);
    pub const CODE_FONT: Pixels = px(12.5);
    /// A labelled field: 12 px muted label, 6 px above its control.
    pub const FIELD_LABEL_FONT: Pixels = px(12.0);
    pub const FIELD_GAP: Pixels = px(6.0);
    /// Rows of a framed list (unsaved documents, accounts): 40 px tall, 12 px
    /// sides, 10 px gap, 12 px muted detail.
    pub const LIST_ROW_HEIGHT: Pixels = px(40.0);
    pub const LIST_ROW_PADDING_X: Pixels = px(12.0);
    pub const LIST_ROW_GAP: Pixels = px(10.0);
    pub const LIST_DETAIL_FONT: Pixels = px(12.0);
    /// Leading icon of a framed list row, and of a field's inline icons
    /// (the lock and the reveal eye of a secret). (15 px)
    pub const LIST_ICON: Pixels = px(15.0);
    /// Dependent objects under a drop: 28 px rows, 8 px gap.
    pub const DEPENDENT_ROW_HEIGHT: Pixels = px(28.0);
    pub const DEPENDENT_ROW_GAP: Pixels = px(8.0);
    /// Framed table (schema drift): 30 px rows, 11.5 px header labels.
    pub const TABLE_ROW_HEIGHT: Pixels = px(30.0);
    pub const TABLE_HEADER_FONT: Pixels = px(11.5);
    /// Device code of the sign-in dialog: Archivo Expanded 900 at 26 px with
    /// 0.08 em tracking, 12 px before its caption.
    pub const DEVICE_CODE_FONT: Pixels = px(26.0);
    pub const DEVICE_CODE_TRACKING_EM: f32 = 0.08;
    pub const DEVICE_CODE_GAP: Pixels = px(12.0);
    /// Progress row: 10 px gap, 4 px track, 11.5 px mono counter.
    pub const PROGRESS_GAP: Pixels = px(10.0);
    pub const PROGRESS_HEIGHT: Pixels = px(4.0);
    pub const META_FONT: Pixels = px(11.5);
    /// Form rows of a wizard step: a 120 px label column 16 px before the
    /// control, 7 px above and below, the label 7 px down to sit on the
    /// control's text line.
    pub const FORM_LABEL_WIDTH: Pixels = px(120.0);
    pub const FORM_ROW_GAP: Pixels = px(16.0);
    pub const FORM_ROW_PADDING_Y: Pixels = px(7.0);
    pub const FORM_LABEL_OFFSET: Pixels = px(7.0);
}

/// Geometry of the three tab kinds (document, result, inline), taken from the
/// Isl* (document tabs), AppByzEditor and P1ConnForm boards.
pub struct TabMetrics;

impl TabMetrics {
    /// Document tab row at the top of the document island: 46 px, 10 px
    /// side padding, 4 px between tabs.
    pub const DOCUMENT_BAR_HEIGHT: Pixels = px(46.0);
    pub const DOCUMENT_BAR_PADDING_X: Pixels = px(10.0);
    pub const DOCUMENT_BAR_GAP: Pixels = px(4.0);
    /// One document tab: a 30 px chip, 12 px side padding, 8 px between
    /// icon, title and trailing items.
    pub const DOCUMENT_TAB_HEIGHT: Pixels = px(30.0);
    pub const DOCUMENT_TAB_PADDING_X: Pixels = px(12.0);
    pub const DOCUMENT_TAB_GAP: Pixels = px(8.0);
    /// Narrowest and widest a document tab gets before its title truncates.
    pub const DOCUMENT_TAB_MIN_WIDTH: Pixels = px(100.0);
    pub const DOCUMENT_TAB_MAX_WIDTH: Pixels = px(220.0);
    /// Leading icon of a document tab. (14 px)
    pub const ICON: Pixels = px(14.0);
    /// Close icon and spinner of a document tab. (12 px)
    pub const CLOSE_ICON: Pixels = px(12.0);
    /// Space between the last tab and the new-tab button. (4 px)
    pub const NEW_TAB_MARGIN_LEFT: Pixels = px(4.0);
    /// Gap between neighbouring tabs of a result bar.
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
    /// Height of a collapsible bar docked at the bottom of an area.
    pub const BAR_HEIGHT: Pixels = px(30.0);
    /// Horizontal padding of a collapsible bar.
    pub const BAR_PADDING_X: Pixels = px(12.0);
    /// Gap between the items of a collapsible bar.
    pub const BAR_GAP: Pixels = px(10.0);
    /// Text size of a collapsible bar.
    pub const BAR_FONT: Pixels = px(12.0);
    /// Top padding of a settings page head.
    pub const SECTION_PADDING_TOP: Pixels = px(22.0);
    /// Horizontal padding of a settings page head, the page margin. (28 px)
    pub const SECTION_PADDING_X: Pixels = px(28.0);
    /// Bottom padding of a settings page head.
    pub const SECTION_PADDING_BOTTOM: Pixels = px(8.0);
    /// Gap between the title and the description of a settings page head.
    pub const SECTION_GAP: Pixels = px(4.0);
    /// Top padding of a section label row.
    pub const LABEL_PADDING_TOP: Pixels = px(18.0);
    /// Bottom padding and bottom margin of a section label row.
    pub const LABEL_PADDING_BOTTOM: Pixels = px(6.0);
    /// Icon of a section label row, drawn in the tint. (15 px)
    pub const LABEL_ICON: Pixels = px(15.0);
    /// Text size of a section label row. (10 px)
    pub const LABEL_FONT: Pixels = px(10.0);
}

/// Geometry of `controls::Button` and `composites::SplitButton`. The default
/// size matches the 30 px buttons of the DSApp "Buttons" row, the
/// AppByzTable and AppByzEditor toolbars and the P1SettingsGeneral footer;
/// the inline size follows the small row buttons of the key-value boards
/// ("Scan more", "Load more") shrunk to 24 px; the large size is kept for
/// the rare call-to-action a board draws at 44 px.
pub struct ButtonMetrics;

impl ButtonMetrics {
    /// Default buttons: toolbars, footers, dialogs and forms. Same height as
    /// inputs and selects.
    pub const HEIGHT: Pixels = px(30.0);
    /// Inline buttons inside table rows, list rows, chips and card rows.
    pub const HEIGHT_INLINE: Pixels = px(24.0);
    /// Large call-to-action buttons.
    pub const HEIGHT_LARGE: Pixels = px(44.0);

    /// Width of an icon-only button per size: the toolbar icon buttons are
    /// 32 wide on a 30 px height; inline and large ones are square.
    pub const ICON_ONLY_WIDTH: Pixels = px(32.0);
    pub const ICON_ONLY_WIDTH_INLINE: Pixels = px(24.0);
    pub const ICON_ONLY_WIDTH_LARGE: Pixels = px(44.0);

    /// Horizontal padding of a labeled button per size.
    pub const PADDING_X: Pixels = px(12.0);
    pub const PADDING_X_INLINE: Pixels = px(10.0);
    pub const PADDING_X_LARGE: Pixels = px(16.0);
    /// Left padding of a split button's main action, whose only cut is the
    /// top-left corner.
    pub const SPLIT_MAIN_PADDING_LEFT: Pixels = px(14.0);
    /// Gap between icon, label, and trailing keycap per size.
    pub const GAP: Pixels = px(8.0);
    pub const GAP_INLINE: Pixels = px(6.0);

    /// Label size per size.
    pub const FONT: Pixels = px(12.5);
    pub const FONT_INLINE: Pixels = px(12.0);
    pub const FONT_LARGE: Pixels = px(13.0);

    /// Icon leading a label, per size.
    pub const ICON: Pixels = px(15.0);
    pub const ICON_INLINE: Pixels = px(12.0);
    /// Icon of an icon-only button, per size.
    pub const ICON_ONLY: Pixels = px(16.0);
    pub const ICON_ONLY_INLINE: Pixels = px(13.0);

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
    /// Icon before the label of a select trigger. (14 px)
    pub const LEADING_ICON: Pixels = px(14.0);

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

    /// The desk under the islands: the main window ground and the frame of
    /// the Settings and Connection Manager windows. `#050507` on dark,
    /// `#E6E1E9` on light.
    pub fn desk(theme: &gpui_component::Theme) -> Hsla {
        if theme.mode.is_dark() {
            rgb(0x050507).into()
        } else {
            rgb(0xE6E1E9).into()
        }
    }

    /// Hairline that traces an island's outline, cuts included: white at
    /// 5 % on dark, strong text (`#141118`) at 6 % on light.
    pub fn island_edge(theme: &gpui_component::Theme) -> Hsla {
        if theme.mode.is_dark() {
            Hsla::from(rgb(0xFFFFFF)).opacity(0.05)
        } else {
            Hsla::from(rgb(0x141118)).opacity(0.06)
        }
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
    /// Badge icon (11 px), 5 px before the label.
    pub const BADGE_ICON: Pixels = px(11.0);
    pub const BADGE_GAP: Pixels = px(5.0);

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

/// Geometry of context menus (`composites::menu_item`), from the cell menu in
/// AppByzMenu and the DSApp "Context menu".
pub struct MenuMetrics;

impl MenuMetrics {
    /// Vertical padding inside the menu frame. (8 px)
    pub const PADDING_Y: Pixels = px(8.0);
    /// Row height. (30 px)
    pub const ROW_HEIGHT: Pixels = px(30.0);
    /// Horizontal inset of a row inside the menu frame. (6 px)
    pub const ROW_INSET: Pixels = px(6.0);
    /// Horizontal padding inside a row. (10 px)
    pub const ROW_PADDING_X: Pixels = px(10.0);
    /// Gap between the icon, the label and the trailing shortcut. (11 px)
    pub const ROW_GAP: Pixels = px(11.0);
    /// Row label size. (13 px)
    pub const ROW_FONT: Pixels = px(13.0);
    /// Leading row icon. (15 px)
    pub const ICON: Pixels = px(15.0);
    /// Trailing submenu chevron. (13 px)
    pub const SUBMENU_ICON: Pixels = px(13.0);

    /// Header row: icon (13 px), gap (8 px), mono label (11 px), padding
    /// 2 px top, 16 px sides, 8 px bottom.
    pub const HEADER_ICON: Pixels = px(13.0);
    pub const HEADER_GAP: Pixels = px(8.0);
    pub const HEADER_FONT: Pixels = px(11.0);
    pub const HEADER_PADDING_TOP: Pixels = px(2.0);
    pub const HEADER_PADDING_X: Pixels = px(16.0);
    pub const HEADER_PADDING_BOTTOM: Pixels = px(8.0);

    /// Separator margins: 5 px vertical, 10 px horizontal.
    pub const SEPARATOR_MARGIN_Y: Pixels = px(5.0);
    pub const SEPARATOR_MARGIN_X: Pixels = px(10.0);

    /// Alpha of the tint (or danger) wash behind the selected row.
    pub const SELECTED_ALPHA: f32 = 0.14;
    /// Opacity of a disabled row.
    pub const DISABLED_OPACITY: f32 = 0.45;
}

/// Geometry of trees (`TreeNav` rows and the connections sidebar), from the
/// sidebar in AppByzTable, P1Sidebar and the DSApp "Tree".
pub struct TreeMetrics;

impl TreeMetrics {
    /// Row height. (26 px)
    pub const ROW_HEIGHT: Pixels = px(26.0);
    /// Indent added per depth level. (14 px)
    pub const INDENT: Pixels = px(14.0);
    /// Padding before the depth-0 chevron and after the trailing slot. (12 px)
    pub const PADDING_X: Pixels = px(12.0);
    /// Gap between chevron, icon, label and trailing slot. (7 px)
    pub const GAP: Pixels = px(7.0);
    /// Expand chevron. (12 px)
    pub const CHEVRON: Pixels = px(12.0);
    /// Node icon or driver logo. (15 px)
    pub const ICON: Pixels = px(15.0);
    /// Distance from the row's left edge to the first indent guide, which
    /// runs through the depth-0 chevron. (19 px)
    pub const GUIDE_OFFSET: Pixels = px(19.0);
    /// Label size. (13 px)
    pub const FONT: Pixels = px(13.0);
    /// Trailing count and latency size. (11 px)
    pub const META_FONT: Pixels = px(11.0);
    /// Width of the tint bar on the left of the selected row. (2 px)
    pub const SELECTION_BAR: Pixels = px(2.0);
}

/// Geometry of the result grid (`components::DataTable`), from AppByzTable
/// and the DSApp "Result grid".
pub struct GridMetrics;

impl GridMetrics {
    /// Header row height, including its 1 px line-2 bottom edge. (40 px)
    pub const HEADER_HEIGHT: Pixels = px(40.0);
    /// Row height: 30 px of content plus a 1 px divider. (31 px)
    pub const ROW_HEIGHT: Pixels = px(31.0);
    /// Horizontal cell padding. (10 px)
    pub const CELL_PADDING_X: Pixels = px(10.0);
    /// Row-number column width. (46 px)
    pub const ROW_NUMBER_WIDTH: Pixels = px(46.0);
    /// Cell text and column name size, JetBrains Mono. (12.5 px)
    pub const FONT: Pixels = px(12.5);
    /// Column type size, JetBrains Mono. (10.5 px)
    pub const TYPE_FONT: Pixels = px(10.5);
    /// PK / FK key icon and sort arrow in the header. (12 px)
    pub const HEADER_ICON: Pixels = px(12.0);
    /// Gap between the parts of a header cell. (6 px)
    pub const HEADER_GAP: Pixels = px(6.0);
    /// Alpha of the tint wash behind the focused and selected cells.
    pub const CELL_SELECTED_ALPHA: f32 = 0.12;
}

/// Geometry of the result chrome: the footer under a result grid and the
/// view-switch row above it (AppByzTable, AppByzEditor).
pub struct ResultMetrics;

impl ResultMetrics {
    /// Footer height. (36 px)
    pub const FOOTER_HEIGHT: Pixels = px(36.0);
    /// Footer horizontal padding. (12 px)
    pub const FOOTER_PADDING_X: Pixels = px(12.0);
    /// Gap between footer groups. (14 px)
    pub const FOOTER_GAP: Pixels = px(14.0);
    /// Footer text size. (12 px)
    pub const FOOTER_FONT: Pixels = px(12.0);
    /// Footer icons (row count, sort, pager chevrons). (13 px)
    pub const FOOTER_ICON: Pixels = px(13.0);
    /// Gap between an icon and its footer label. (6 px)
    pub const FOOTER_ITEM_GAP: Pixels = px(6.0);
    /// Gap between the pager chevrons and the page label. (10 px)
    pub const PAGER_GAP: Pixels = px(10.0);

    /// View-switch row under the result tabs: height (42 px), gap (12 px),
    /// statement range size (11.5 px, mono).
    pub const VIEW_ROW_HEIGHT: Pixels = px(42.0);
    pub const VIEW_ROW_GAP: Pixels = px(12.0);
    pub const STATEMENT_FONT: Pixels = px(11.5);
    /// Search-in-results field width. (240 px)
    pub const SEARCH_WIDTH: Pixels = px(240.0);
}

/// Geometry of the migrate wizard's Run phase, from P1Migrate.
pub struct MigrateRunMetrics;

impl MigrateRunMetrics {
    pub const SECTION_GAP: Pixels = px(18.0);
    pub const CONTENT_PADDING_X: Pixels = px(26.0);
    pub const CONTENT_PADDING_Y: Pixels = px(22.0);
    pub const HEADER_GAP: Pixels = px(12.0);
    pub const DRIVER_ICON: Pixels = px(22.0);
    pub const ARROW_ICON: Pixels = px(16.0);
    pub const SUMMARY_GAP: Pixels = px(8.0);
    pub const PERCENT_FONT: Pixels = px(34.0);
    pub const OVERALL_BAR_HEIGHT: Pixels = px(6.0);
    pub const TABLE_HEADER_HEIGHT: Pixels = px(32.0);
    pub const TABLE_ROW_HEIGHT: Pixels = px(36.0);
    pub const TABLE_PADDING_X: Pixels = px(14.0);
    pub const STATUS_COLUMN: Pixels = px(30.0);
    pub const STATUS_ICON: Pixels = px(15.0);
    pub const PROGRESS_COLUMN: Pixels = px(220.0);
    pub const ROWS_COLUMN: Pixels = px(160.0);
    pub const TABLE_BAR_WIDTH: Pixels = px(150.0);
    pub const TABLE_BAR_HEIGHT: Pixels = px(4.0);
    pub const BAR_GAP: Pixels = px(8.0);
    pub const PERCENT_CAPTION_FONT: Pixels = px(11.0);
}

/// Geometry of the command palette, from P1Palette.
pub struct PaletteMetrics;

impl PaletteMetrics {
    /// Card width (640 px) and its distance from the top of the window (90 px).
    pub const WIDTH: Pixels = px(640.0);
    pub const TOP_OFFSET: Pixels = px(90.0);

    /// Search row: 56 px tall, 18 px sides, 12 px gap, 18 px search icon,
    /// 17 px query text, 11.5 px mono match counter.
    pub const SEARCH_HEIGHT: Pixels = px(56.0);
    pub const PADDING_X: Pixels = px(18.0);
    pub const SEARCH_GAP: Pixels = px(12.0);
    pub const SEARCH_ICON: Pixels = px(18.0);
    pub const QUERY_FONT: Pixels = px(17.0);
    pub const COUNT_FONT: Pixels = px(11.5);

    /// Section header: 12 px above, 6 px below, 10 px expanded caps.
    pub const SECTION_PADDING_TOP: Pixels = px(12.0);
    pub const SECTION_PADDING_BOTTOM: Pixels = px(6.0);
    pub const SECTION_FONT: Pixels = px(10.0);

    /// Result row: 38 px tall, 12 px gap, 16 px icon, 14 px name, 11.5 px
    /// qualifiers.
    pub const ROW_HEIGHT: Pixels = px(38.0);
    pub const ROW_GAP: Pixels = px(12.0);
    pub const ROW_ICON: Pixels = px(16.0);
    pub const ROW_FONT: Pixels = px(14.0);
    pub const QUALIFIER_FONT: Pixels = px(11.5);
    /// Alpha of the tint wash behind the selected row, and the width of its
    /// leading tint bar (2 px).
    pub const SELECTED_ALPHA: f32 = 0.12;
    pub const SELECTION_BAR: Pixels = px(2.0);
    /// Space under the last result row. (8 px)
    pub const LIST_PADDING_BOTTOM: Pixels = px(8.0);

    /// Footer hints: 38 px tall, 14 px between every keycap and label, 12 px
    /// text.
    pub const FOOTER_HEIGHT: Pixels = px(38.0);
    pub const FOOTER_GAP: Pixels = px(14.0);
    pub const FOOTER_FONT: Pixels = px(12.0);
}

/// Geometry of the wizard stepper (`composites::wizard_rail`), from P1Migrate
/// and the DSAppPlan "Stepper".
pub struct StepperMetrics;

impl StepperMetrics {
    /// Step badge (26 px square, cut 4).
    pub const BADGE: Pixels = px(26.0);
    /// Check or play glyph inside a badge. (13 px)
    pub const BADGE_ICON: Pixels = px(13.0);
    /// Height of one step in the vertical rail. (44 px)
    pub const RAIL_STEP_HEIGHT: Pixels = px(44.0);
    /// Horizontal padding of a rail step. (16 px)
    pub const RAIL_PADDING_X: Pixels = px(16.0);
    /// Space above the first rail step. (12 px)
    pub const RAIL_PADDING_TOP: Pixels = px(12.0);
    /// Gap between a badge and its label. (10 px)
    pub const GAP: Pixels = px(10.0);
    /// Connector between steps of the horizontal stepper. (28 x 1 px)
    pub const CONNECTOR_WIDTH: Pixels = px(28.0);
    /// Step label size. (13 px)
    pub const FONT: Pixels = px(13.0);

    /// Horizontal stepper at the top of a dialog body (P1Flows): 22 px
    /// badges holding a 12 px check, 8 px between badge, label and the
    /// 20 x 1 px connectors, 12.5 px labels.
    pub const INLINE_BADGE: Pixels = px(22.0);
    pub const INLINE_BADGE_ICON: Pixels = px(12.0);
    pub const INLINE_GAP: Pixels = px(8.0);
    pub const INLINE_CONNECTOR_WIDTH: Pixels = px(20.0);
    pub const INLINE_FONT: Pixels = px(12.5);
}

/// Geometry of the table view's header and filter rows (AppByzTable).
pub struct TableViewMetrics;

impl TableViewMetrics {
    /// Header row: 40 px tall, 16 px side padding, 8 px gap.
    pub const HEADER_HEIGHT: Pixels = px(40.0);
    pub const HEADER_PADDING_X: Pixels = px(16.0);
    pub const HEADER_GAP: Pixels = px(8.0);
    /// Edit status: 14 px check icon, 6 px gap, 12 px text.
    pub const STATUS_ICON: Pixels = px(14.0);
    pub const STATUS_GAP: Pixels = px(6.0);
    pub const STATUS_FONT: Pixels = px(12.0);
    /// Vertical divider between groups of a toolbar: 1 x 20 px.
    pub const DIVIDER_HEIGHT: Pixels = px(20.0);
    /// Space on each side of the header's divider. (4 px)
    pub const HEADER_DIVIDER_MARGIN_X: Pixels = px(4.0);
    /// Filter row: 50 px tall, 14 px side padding, 8 px gap.
    pub const FILTER_ROW_HEIGHT: Pixels = px(50.0);
    pub const FILTER_ROW_PADDING_X: Pixels = px(14.0);
    pub const FILTER_ROW_GAP: Pixels = px(8.0);
}

/// Geometry of a document collection (P1DocTable, P2DocNested, P1DocSchema).
pub struct CollectionMetrics;

impl CollectionMetrics {
    /// Query bar row: 50 px tall, 14 px side padding, 8 px gap. Each slot is a
    /// 32 px field with 10 px padding, 8 px between its keyword and its JSON,
    /// 12.5 px mono text. Fixed slot widths: project 190, sort 200, limit 124.
    /// The limit slot is wider than the board's 110 so its keyword, the
    /// input's own 8 px side padding and a five-digit value fit unclipped.
    pub const QUERY_ROW_HEIGHT: Pixels = px(50.0);
    pub const QUERY_ROW_PADDING_X: Pixels = px(14.0);
    pub const QUERY_ROW_GAP: Pixels = px(8.0);
    pub const SLOT_HEIGHT: Pixels = px(32.0);
    pub const SLOT_PADDING_X: Pixels = px(10.0);
    pub const SLOT_GAP: Pixels = px(8.0);
    pub const SLOT_FONT: Pixels = px(12.5);
    pub const PROJECT_SLOT_WIDTH: Pixels = px(190.0);
    pub const SORT_SLOT_WIDTH: Pixels = px(200.0);
    pub const LIMIT_SLOT_WIDTH: Pixels = px(124.0);
    /// View row under the query bar: 42 px tall, 14 px side padding.
    pub const VIEW_ROW_HEIGHT: Pixels = px(42.0);
    pub const VIEW_ROW_PADDING_X: Pixels = px(14.0);
    pub const VIEW_ROW_GAP: Pixels = px(8.0);
    /// Keyboard hint after the view switch: 8 px lead, 6 px gap, 12.5 px text.
    pub const HINT_MARGIN_LEFT: Pixels = px(8.0);
    pub const HINT_GAP: Pixels = px(6.0);
    pub const HINT_FONT: Pixels = px(12.5);
    /// Pending-changes label. (12 px)
    pub const PENDING_FONT: Pixels = px(12.0);
    /// Header metadata chip: 6 px lead, 2 x 8 px padding, 11 px mono.
    pub const META_CHIP_MARGIN_LEFT: Pixels = px(6.0);
    pub const META_CHIP_PADDING_X: Pixels = px(8.0);
    pub const META_CHIP_PADDING_Y: Pixels = px(2.0);
    pub const META_CHIP_FONT: Pixels = px(11.0);

    /// Grid header with a presence bar under each name: 44 px, 5 px between
    /// the two lines, 48 x 3 px bar, 10 px labels. A column group adds a
    /// 26 px row above (11.5 px mono); without presence the name row is 30 px.
    pub const PRESENCE_HEADER_HEIGHT: Pixels = px(44.0);
    pub const GROUPED_HEADER_HEIGHT: Pixels = px(30.0);
    pub const GROUP_ROW_HEIGHT: Pixels = px(26.0);
    pub const GROUP_FONT: Pixels = px(11.5);
    pub const HEADER_LINE_GAP: Pixels = px(5.0);
    pub const PRESENCE_BAR_WIDTH: Pixels = px(48.0);
    pub const PRESENCE_BAR_HEIGHT: Pixels = px(3.0);
    pub const HEADER_META_FONT: Pixels = px(10.0);
    /// Tint washes behind a column group: group header 8 %, child headers
    /// 5 %, child cells 3 %. A pending edit washes its cell in warning at 8 %
    /// inside a 1.5 px warning ring.
    pub const GROUP_HEADER_ALPHA: f32 = 0.08;
    pub const GROUP_CHILD_HEADER_ALPHA: f32 = 0.05;
    pub const GROUP_CHILD_CELL_ALPHA: f32 = 0.03;
    pub const EDITED_CELL_ALPHA: f32 = 0.08;
    /// Icon inside a nested-value cell. (12 px)
    pub const NESTED_ICON: Pixels = px(12.0);

    /// Server-change card: 460 px wide, 24 px from the right and 56 px from
    /// the bottom of the grid, 16 px padding, 10 px gap, 3 px warning edge,
    /// 12.5 px body, 8 x 10 px code block in 12 px mono.
    pub const CONFLICT_WIDTH: Pixels = px(460.0);
    pub const CONFLICT_RIGHT: Pixels = px(24.0);
    pub const CONFLICT_BOTTOM: Pixels = px(56.0);
    pub const CONFLICT_PADDING: Pixels = px(16.0);
    pub const CONFLICT_GAP: Pixels = px(10.0);
    pub const CONFLICT_EDGE: Pixels = px(3.0);
    pub const CONFLICT_BODY_FONT: Pixels = px(12.5);
    pub const CONFLICT_CODE_PADDING_X: Pixels = px(10.0);
    pub const CONFLICT_CODE_PADDING_Y: Pixels = px(8.0);
    pub const CONFLICT_CODE_FONT: Pixels = px(12.0);

    /// Schema view: 44 px toolbar (14 px padding, 8 px gap), 140 px sample
    /// size field, 34 px column header in 11.5 px, rows at least 52 px tall
    /// with 18 px side padding. Columns: field 240, types 360, presence 100.
    pub const SCHEMA_TOOLBAR_HEIGHT: Pixels = px(44.0);
    pub const SCHEMA_SAMPLE_WIDTH: Pixels = px(140.0);
    pub const SCHEMA_HEADER_HEIGHT: Pixels = px(34.0);
    pub const SCHEMA_HEADER_FONT: Pixels = px(11.5);
    pub const SCHEMA_ROW_MIN_HEIGHT: Pixels = px(52.0);
    pub const SCHEMA_PADDING_X: Pixels = px(18.0);
    pub const SCHEMA_FIELD_WIDTH: Pixels = px(240.0);
    pub const SCHEMA_TYPES_WIDTH: Pixels = px(360.0);
    pub const SCHEMA_PRESENCE_WIDTH: Pixels = px(100.0);
    /// Type bar: 8 px tall with 30 px clearance to the next column, 6 px to
    /// its legend; legend swatches 8 px, 4 px to their label, 10 px apart,
    /// 11 px text. Field names 13 px mono, values 12.5 px.
    pub const TYPE_BAR_HEIGHT: Pixels = px(8.0);
    pub const TYPE_BAR_CLEARANCE: Pixels = px(30.0);
    pub const TYPE_BAR_GAP: Pixels = px(6.0);
    pub const LEGEND_SWATCH: Pixels = px(8.0);
    pub const LEGEND_GAP: Pixels = px(4.0);
    pub const LEGEND_SPACING: Pixels = px(10.0);
    pub const LEGEND_FONT: Pixels = px(11.0);
    pub const FIELD_FONT: Pixels = px(13.0);
    pub const VALUE_FONT: Pixels = px(12.5);
    /// Aggregate view: the pipeline editor is 148 px tall on a cut-6 ground
    /// field, the section has 10 px top and bottom padding (14 px sides,
    /// the query row's), and the inline error sits 6 px under the editor.
    pub const PIPELINE_EDITOR_HEIGHT: Pixels = px(148.0);
    pub const PIPELINE_PADDING_Y: Pixels = px(10.0);
    pub const PIPELINE_EDITOR_PADDING_Y: Pixels = px(6.0);
    pub const PIPELINE_ERROR_GAP: Pixels = px(6.0);
}

/// Geometry of the query editor's context bar, production banner and
/// toolbar (AppByzEditor).
pub struct EditorMetrics;

impl EditorMetrics {
    /// Context bar and toolbar: 46 px tall, 14 px side padding.
    pub const BAR_HEIGHT: Pixels = px(46.0);
    pub const BAR_PADDING_X: Pixels = px(14.0);
    /// Gap between the context bar's selectors and separators. (8 px)
    pub const CONTEXT_GAP: Pixels = px(8.0);
    /// Leading icon of a context selector. (14 px)
    pub const SELECTOR_ICON: Pixels = px(14.0);
    /// Chevron between two context selectors. (12 px)
    pub const SEPARATOR_ICON: Pixels = px(12.0);
    /// Production banner: 34 px tall, 16 px side padding, 10 px gap, 15 px
    /// icon, danger fill at 8 % and bottom line at 25 %.
    pub const BANNER_HEIGHT: Pixels = px(34.0);
    pub const BANNER_PADDING_X: Pixels = px(16.0);
    pub const BANNER_GAP: Pixels = px(10.0);
    pub const BANNER_ICON: Pixels = px(15.0);
    pub const BANNER_FILL_ALPHA: f32 = 0.08;
    pub const BANNER_LINE_ALPHA: f32 = 0.25;
    /// Toolbar: 6 px between buttons, 6 px on each side of a group divider.
    pub const TOOLBAR_GAP: Pixels = px(6.0);
    pub const TOOLBAR_DIVIDER_MARGIN_X: Pixels = px(6.0);
    /// Last-run summary at the end of the toolbar: 12 px mono text, 13 px
    /// icon, 6 px gap.
    pub const LAST_RUN_FONT: Pixels = px(12.0);
    pub const LAST_RUN_ICON: Pixels = px(13.0);
    pub const LAST_RUN_GAP: Pixels = px(6.0);
    /// Code text: 13 px JetBrains Mono on 22 px rows.
    pub const CODE_FONT: Pixels = px(13.0);
    pub const CODE_LINE_HEIGHT: Pixels = px(22.0);
    /// Script file readout at the end of the context bar: 11.5 px mono text,
    /// 13 px file icon, 8 px between path and state, 5 px inside the state.
    pub const FILE_FONT: Pixels = px(11.5);
    pub const FILE_ICON: Pixels = px(13.0);
    pub const FILE_GAP: Pixels = px(8.0);
    pub const FILE_STATE_GAP: Pixels = px(5.0);
    /// Statement gutter: a 24 px run-marker slot with an 18 px hit target
    /// and a 12 px play glyph, 14 px between the line numbers and a 3 px
    /// statement bar, the statement fill at 6 % of the tint and the cursor
    /// line at 11 %.
    pub const GUTTER_MARKER_SLOT: Pixels = px(24.0);
    pub const GUTTER_MARKER_SIZE: Pixels = px(18.0);
    pub const GUTTER_MARKER_ICON: Pixels = px(12.0);
    pub const GUTTER_BAR_GAP: Pixels = px(14.0);
    pub const GUTTER_BAR_WIDTH: Pixels = px(3.0);
    pub const STATEMENT_FILL_ALPHA: f32 = 0.06;
    pub const CURSOR_LINE_FILL_ALPHA: f32 = 0.11;
    /// Result sub-toolbar: 11.5 px mono statement caption, 240 px search field
    /// with a 13 px search icon.
    pub const RESULT_CAPTION_FONT: Pixels = px(11.5);
    pub const RESULT_SEARCH_WIDTH: Pixels = px(240.0);
    pub const RESULT_SEARCH_ICON: Pixels = px(13.0);
    /// Smallest height the results pane can be dragged to: the result tabs
    /// (34), the view row (42), the grid header (40), one grid row (31) and
    /// the grid footer (36), so the footer never slides out of view.
    pub const RESULTS_MIN_HEIGHT: Pixels = px(183.0);
}

/// The query history island beside an editor (IslEditor): 300 px wide, a
/// 46 px header with 16 px side padding, entries with 10 x 16 px padding and
/// 4 px between a 12 px mono query line and an 11.5 px meta line; the current
/// entry is filled with the tint at 10 %.
pub struct HistoryPanelMetrics;

impl HistoryPanelMetrics {
    pub const WIDTH: Pixels = px(300.0);
    pub const HEADER_HEIGHT: Pixels = px(46.0);
    pub const PADDING_X: Pixels = px(16.0);
    /// Space under the tab switch and the search field. (10 px)
    pub const SECTION_GAP: Pixels = px(10.0);
    pub const ENTRY_PADDING_Y: Pixels = px(10.0);
    pub const ENTRY_GAP: Pixels = px(4.0);
    pub const QUERY_FONT: Pixels = px(12.0);
    pub const META_FONT: Pixels = px(11.5);
    pub const SELECTED_FILL_ALPHA: f32 = 0.10;
}

/// Geometry of the row inspector rail (AppByzTable, DSAppPlan "RowInspector").
pub struct InspectorMetrics;

impl InspectorMetrics {
    /// Default rail width. (380 px)
    pub const WIDTH: Pixels = px(380.0);
    /// Header (IslTable): 46 px tall, 16 px left and 12 px right padding,
    /// 10 px gap, 15 px leading icon, 11.5 px mono row key.
    pub const HEADER_HEIGHT: Pixels = px(46.0);
    pub const HEADER_PADDING_LEFT: Pixels = px(16.0);
    pub const HEADER_PADDING_RIGHT: Pixels = px(12.0);
    pub const HEADER_GAP: Pixels = px(10.0);
    pub const HEADER_ICON: Pixels = px(15.0);
    pub const KEY_FONT: Pixels = px(11.5);
    /// Horizontal padding of the section labels and field rows. (16 px)
    pub const PADDING_X: Pixels = px(16.0);
    /// REFERENCES section label: 12 px above, 8 px below.
    pub const REFERENCES_LABEL_PADDING_TOP: Pixels = px(12.0);
    pub const REFERENCES_LABEL_PADDING_BOTTOM: Pixels = px(8.0);
    /// Field row: 10 px vertical padding, 4 px between the label line and
    /// the value box; an 11.5 px label on a 14 px line with the column type
    /// in mono at its right end.
    pub const FIELD_PADDING_Y: Pixels = px(10.0);
    pub const FIELD_GAP: Pixels = px(4.0);
    pub const FIELD_LABEL_FONT: Pixels = px(11.5);
    pub const FIELD_LABEL_LINE_HEIGHT: Pixels = px(14.0);
    pub const FIELD_LABEL_GAP: Pixels = px(6.0);
    pub const FIELD_ICON: Pixels = px(12.0);
    /// Value box: 12.5 px mono on a 16 px line, 6 x 8 px padding, on the
    /// ground under a 4 px cut, so the box is 28 px tall.
    pub const FIELD_VALUE_FONT: Pixels = px(12.5);
    pub const FIELD_VALUE_LINE_HEIGHT: Pixels = px(16.0);
    pub const FIELD_VALUE_PADDING_Y: Pixels = px(6.0);
    pub const FIELD_VALUE_PADDING_X: Pixels = px(8.0);
    /// Reference row: 32 px tall, 16 px side and 6 px bottom margin, 10 px
    /// side padding and gap, 13 px icon, 12 px chevron.
    pub const REFERENCE_HEIGHT: Pixels = px(32.0);
    pub const REFERENCE_MARGIN_X: Pixels = px(16.0);
    pub const REFERENCE_MARGIN_BOTTOM: Pixels = px(6.0);
    pub const REFERENCE_PADDING_X: Pixels = px(10.0);
    pub const REFERENCE_GAP: Pixels = px(10.0);
    pub const REFERENCE_ICON: Pixels = px(13.0);
    pub const REFERENCE_CHEVRON: Pixels = px(12.0);
    /// Footer: 46 px tall, 16 px left and 12 px right padding, 8 px between
    /// buttons.
    pub const FOOTER_HEIGHT: Pixels = px(46.0);
    pub const FOOTER_PADDING_LEFT: Pixels = px(16.0);
    pub const FOOTER_PADDING_RIGHT: Pixels = px(12.0);
    pub const FOOTER_GAP: Pixels = px(8.0);
}

/// Geometry of the key-value browser (P1KvHash, P1KvString, P2KvZset,
/// P2KvStream, P2KvFilter): toolbars, the key list, the value pane, the
/// expiry popover, the bulk-delete confirmation and the command console.
pub struct KeyValueMetrics;

impl KeyValueMetrics {
    /// Document toolbar and filter row: 44 px tall, 14 px sides, 8 px gap.
    pub const TOOLBAR_HEIGHT: Pixels = px(44.0);
    pub const TOOLBAR_PADDING_X: Pixels = px(14.0);
    pub const TOOLBAR_GAP: Pixels = px(8.0);
    /// Divider between the type filter and the layout toggle: 1 x 20 px,
    /// 4 px each side.
    pub const TOOLBAR_DIVIDER_HEIGHT: Pixels = px(20.0);
    pub const TOOLBAR_DIVIDER_MARGIN_X: Pixels = px(4.0);
    /// Narrowest the key pattern field shrinks to before the filter row
    /// wraps its controls onto a second line. (220 px)
    pub const PATTERN_MIN_WIDTH: Pixels = px(220.0);
    /// Key list column. (440 px, IslKvStream)
    pub const KEY_LIST_WIDTH: Pixels = px(440.0);
    /// Key list header: 30 px, 11.5 px text.
    pub const LIST_HEADER_HEIGHT: Pixels = px(30.0);
    pub const LIST_HEADER_FONT: Pixels = px(11.5);
    /// Key list rows: 28 px, 14 px left and 12 px right padding, 16 px per
    /// tree level.
    pub const LIST_ROW_HEIGHT: Pixels = px(28.0);
    pub const LIST_PADDING_LEFT: Pixels = px(14.0);
    pub const LIST_PADDING_RIGHT: Pixels = px(12.0);
    pub const LIST_INDENT: Pixels = px(16.0);
    /// Key row: 12.5 px mono name, 8 px gap, 11.5 px TTL and size cells.
    pub const LIST_ROW_FONT: Pixels = px(12.5);
    pub const LIST_ROW_GAP: Pixels = px(8.0);
    pub const LIST_META_FONT: Pixels = px(11.5);
    /// TTL and size columns. (76 px, 66 px)
    pub const TTL_COLUMN: Pixels = px(76.0);
    pub const SIZE_COLUMN: Pixels = px(66.0);
    /// Folder row: 13 px mono name, 7 px gap, 12 px chevron, 14 px folder
    /// icon, 11 px count in a 140 px column.
    pub const FOLDER_ROW_FONT: Pixels = px(13.0);
    pub const FOLDER_GAP: Pixels = px(7.0);
    pub const FOLDER_CHEVRON: Pixels = px(12.0);
    pub const FOLDER_ICON: Pixels = px(14.0);
    pub const FOLDER_COUNT_FONT: Pixels = px(11.0);
    pub const FOLDER_COUNT_COLUMN: Pixels = px(140.0);
    /// Space where a folder row draws its chevron, kept on key rows so names
    /// line up. (12 px)
    pub const CHEVRON_SLOT: Pixels = px(12.0);
    /// Type badge: 26 x 18 px, 10 px bold mono, color at 13 % fill.
    pub const TYPE_BADGE_WIDTH: Pixels = px(26.0);
    pub const TYPE_BADGE_HEIGHT: Pixels = px(18.0);
    pub const TYPE_BADGE_FONT: Pixels = px(10.0);
    pub const TYPE_BADGE_FILL_ALPHA: f32 = 0.13;
    /// Selected key row wash. (12 %)
    pub const SELECTED_ROW_ALPHA: f32 = 0.12;
    /// Selected member row wash. (7 %)
    pub const SELECTED_MEMBER_ALPHA: f32 = 0.07;
    /// Key list footer: 40 px, 14 px sides, 10 px gap, 12 px text, 13 px
    /// icon, 70 px progress bar (110 px while searching), 4 px tall.
    pub const FOOTER_HEIGHT: Pixels = px(40.0);
    pub const FOOTER_PADDING_X: Pixels = px(14.0);
    pub const FOOTER_GAP: Pixels = px(10.0);
    pub const FOOTER_FONT: Pixels = px(12.0);
    pub const FOOTER_ICON: Pixels = px(13.0);
    pub const PROGRESS_WIDTH: Pixels = px(70.0);
    pub const PROGRESS_WIDTH_SEARCHING: Pixels = px(110.0);
    pub const PROGRESS_HEIGHT: Pixels = px(4.0);
    /// Search-in-progress card: 24 px above and below, 18 px sides, 16 px
    /// padding, 12 px gap, 12.5 px body at 1.5 line height.
    pub const SEARCH_CARD_MARGIN_Y: Pixels = px(24.0);
    pub const SEARCH_CARD_MARGIN_X: Pixels = px(18.0);
    pub const SEARCH_CARD_PADDING: Pixels = px(16.0);
    pub const SEARCH_CARD_GAP: Pixels = px(12.0);
    pub const SEARCH_CARD_FONT: Pixels = px(12.5);
    pub const SEARCH_CARD_ICON: Pixels = px(15.0);
    /// Value header: 46 px, 16 px sides, 10 px gap, 14 px bold mono key.
    pub const VALUE_HEADER_HEIGHT: Pixels = px(46.0);
    pub const VALUE_PADDING_X: Pixels = px(16.0);
    pub const VALUE_HEADER_GAP: Pixels = px(10.0);
    pub const KEY_NAME_FONT: Pixels = px(14.0);
    /// Metadata row: 36 px, 18 px between items, 6 px inside, 13 px icons,
    /// 11 px edit glyph, 12 px text.
    pub const META_ROW_HEIGHT: Pixels = px(36.0);
    pub const META_GAP: Pixels = px(18.0);
    pub const META_ITEM_GAP: Pixels = px(6.0);
    pub const META_ICON: Pixels = px(13.0);
    pub const META_EDIT_ICON: Pixels = px(11.0);
    pub const META_FONT: Pixels = px(12.0);
    /// Value toolbar (filter, order, View as): 42 px.
    pub const VALUE_TOOLBAR_HEIGHT: Pixels = px(42.0);
    pub const MEMBER_FILTER_WIDTH: Pixels = px(260.0);
    pub const COMPRESSION_WIDTH: Pixels = px(110.0);
    /// Member tables: 32 px header, 32 px hash rows, 30 px ranked rows,
    /// 12 px sides.
    pub const MEMBER_HEADER_HEIGHT: Pixels = px(32.0);
    pub const MEMBER_ROW_HEIGHT: Pixels = px(32.0);
    pub const RANKED_ROW_HEIGHT: Pixels = px(30.0);
    pub const MEMBER_PADDING_X: Pixels = px(12.0);
    pub const INDEX_COLUMN: Pixels = px(40.0);
    pub const FIELD_COLUMN: Pixels = px(180.0);
    pub const FORMAT_COLUMN: Pixels = px(90.0);
    pub const ACTION_COLUMN: Pixels = px(36.0);
    pub const ACTION_ICON: Pixels = px(13.0);
    /// Format badge on member rows: 20 px, 7 px sides, 11 px semibold.
    pub const FORMAT_BADGE_HEIGHT: Pixels = px(20.0);
    pub const FORMAT_BADGE_PADDING_X: Pixels = px(7.0);
    pub const FORMAT_BADGE_FONT: Pixels = px(11.0);
    /// Sorted-set columns: 56 px rank, 120 px score, 260 px bar, 6 px bar.
    pub const RANK_COLUMN: Pixels = px(56.0);
    pub const SCORE_COLUMN: Pixels = px(120.0);
    pub const BAR_COLUMN: Pixels = px(260.0);
    pub const BAR_HEIGHT: Pixels = px(6.0);
    /// Stream columns: 210 px ID, 150 px time, 110 px per field.
    pub const ENTRY_ID_COLUMN: Pixels = px(210.0);
    pub const ENTRY_TIME_COLUMN: Pixels = px(150.0);
    pub const ENTRY_FIELD_COLUMN: Pixels = px(110.0);
    pub const RANGE_INPUT_WIDTH: Pixels = px(170.0);
    /// Consumer groups panel: 360 px, 42 px header, 34 px rows, 14 px sides,
    /// 70/70/110 px columns, 14 px callout margin, 12 px callout padding.
    pub const GROUPS_WIDTH: Pixels = px(360.0);
    pub const GROUPS_HEADER_HEIGHT: Pixels = px(42.0);
    pub const GROUPS_TABLE_HEADER_HEIGHT: Pixels = px(30.0);
    pub const GROUPS_ROW_HEIGHT: Pixels = px(34.0);
    pub const GROUPS_PADDING_X: Pixels = px(14.0);
    pub const GROUPS_COUNT_COLUMN: Pixels = px(70.0);
    pub const GROUPS_ID_COLUMN: Pixels = px(110.0);
    pub const CALLOUT_MARGIN: Pixels = px(14.0);
    pub const CALLOUT_PADDING: Pixels = px(12.0);
    pub const CALLOUT_GAP: Pixels = px(6.0);
    pub const CALLOUT_STRIPE: Pixels = px(3.0);
    pub const CALLOUT_FILL_ALPHA: f32 = 0.08;
    /// Key-hint footer of the value pane. (36 px)
    pub const HINT_ROW_HEIGHT: Pixels = px(36.0);
    /// String value: 12 px top padding, 22 px lines, 36 px line-number
    /// gutter with 14 px right padding, 20 px per JSON level.
    pub const VALUE_PADDING_TOP: Pixels = px(12.0);
    pub const VALUE_LINE_HEIGHT: Pixels = px(22.0);
    pub const LINE_NUMBER_WIDTH: Pixels = px(36.0);
    pub const LINE_NUMBER_PADDING_RIGHT: Pixels = px(14.0);
    pub const JSON_INDENT: Pixels = px(20.0);
    /// Large value gate: 16 px margin, 14 x 16 px padding, 12 px gap,
    /// 3 gap between the two lines.
    pub const GATE_MARGIN: Pixels = px(16.0);
    pub const GATE_PADDING_Y: Pixels = px(14.0);
    pub const GATE_PADDING_X: Pixels = px(16.0);
    pub const GATE_GAP: Pixels = px(12.0);
    pub const GATE_LINE_GAP: Pixels = px(3.0);
    /// Expiry popover: 380 px, 16 px from the pane's left edge and 84 px
    /// from its top, 16 px padding, 12 px gap, 110 px duration field, 180 px
    /// date field.
    pub const EXPIRY_WIDTH: Pixels = px(380.0);
    pub const EXPIRY_OFFSET_LEFT: Pixels = px(16.0);
    pub const EXPIRY_OFFSET_TOP: Pixels = px(84.0);
    pub const EXPIRY_PADDING: Pixels = px(16.0);
    pub const EXPIRY_GAP: Pixels = px(12.0);
    pub const EXPIRY_DURATION_WIDTH: Pixels = px(110.0);
    pub const EXPIRY_AT_WIDTH: Pixels = px(180.0);
    /// Bulk delete confirmation: 560 px wide, 30 px match rows.
    pub const BULK_MODAL_WIDTH: Pixels = px(560.0);
    pub const BULK_ROW_HEIGHT: Pixels = px(30.0);
    /// Command console: 32 px header, 14 px sides, 10 px gap, 12 px chevron,
    /// 14 px icon, 12.5 px mono at 21 px lines, 168 px of transcript.
    pub const CONSOLE_HEADER_HEIGHT: Pixels = px(32.0);
    pub const CONSOLE_PADDING_X: Pixels = px(14.0);
    pub const CONSOLE_PADDING_BOTTOM: Pixels = px(10.0);
    pub const CONSOLE_GAP: Pixels = px(10.0);
    pub const CONSOLE_CHEVRON: Pixels = px(12.0);
    pub const CONSOLE_ICON: Pixels = px(14.0);
    pub const CONSOLE_FONT: Pixels = px(12.5);
    pub const CONSOLE_LINE_HEIGHT: Pixels = px(21.0);
    pub const CONSOLE_TRANSCRIPT_HEIGHT: Pixels = px(168.0);
}

/// Geometry of the extracted navigation helpers: breadcrumb, empty state and
/// list rows (AppByzTable header, DSAppPlan).
pub struct NavigationMetrics;

impl NavigationMetrics {
    /// Breadcrumb gap between segments and chevrons. (8 px)
    pub const BREADCRUMB_GAP: Pixels = px(8.0);
    /// Breadcrumb driver logo and current-object icon. (15 px)
    pub const BREADCRUMB_ICON: Pixels = px(15.0);
    /// Breadcrumb chevron. (12 px)
    pub const BREADCRUMB_CHEVRON: Pixels = px(12.0);
    /// Breadcrumb text size. (13 px)
    pub const BREADCRUMB_FONT: Pixels = px(13.0);
    /// Breadcrumb metadata chip: 2 px vertical, 8 px horizontal padding,
    /// 11 px mono text, 6 px from the last segment.
    pub const BREADCRUMB_META_PADDING_Y: Pixels = px(2.0);
    pub const BREADCRUMB_META_PADDING_X: Pixels = px(8.0);
    pub const BREADCRUMB_META_FONT: Pixels = px(11.0);
    pub const BREADCRUMB_META_MARGIN: Pixels = px(6.0);

    /// Empty state card width (460 px), padding (34 px) and gap (14 px).
    pub const EMPTY_WIDTH: Pixels = px(460.0);
    pub const EMPTY_PADDING: Pixels = px(34.0);
    pub const EMPTY_GAP: Pixels = px(14.0);
    /// Empty state icon. (30 px)
    pub const EMPTY_ICON: Pixels = px(30.0);
    /// Empty state title (16 px, bold) and sentence (13 px).
    pub const EMPTY_TITLE_FONT: Pixels = px(16.0);
    pub const EMPTY_BODY_FONT: Pixels = px(13.0);
    /// Gap between empty state actions (8 px) and inside one (10 px).
    pub const EMPTY_ACTION_GAP: Pixels = px(8.0);
    pub const EMPTY_ACTION_INNER_GAP: Pixels = px(10.0);
    /// Empty state action icon. (15 px)
    pub const EMPTY_ACTION_ICON: Pixels = px(15.0);
}

/// Geometry of the application shell: title bar, activity rail, sidebar
/// island, status bar and the empty workspace (Isl* boards, P1Empty).
pub struct ShellMetrics;

impl ShellMetrics {
    /// Title bar: 44 px on the desk, 14 px side padding, no line.
    pub const TITLE_BAR_HEIGHT: Pixels = px(44.0);
    pub const TITLE_BAR_PADDING_X: Pixels = px(14.0);

    /// Command search field, centered in the title bar: 420 by 30 px, cut
    /// 6, 10 px padding, 8 px gap, 14 px search icon, 12.5 px text.
    pub const COMMAND_SEARCH_WIDTH: Pixels = px(420.0);
    pub const COMMAND_SEARCH_HEIGHT: Pixels = px(30.0);
    pub const COMMAND_SEARCH_PADDING_X: Pixels = px(10.0);
    pub const COMMAND_SEARCH_GAP: Pixels = px(8.0);
    pub const COMMAND_SEARCH_ICON: Pixels = px(14.0);
    pub const COMMAND_SEARCH_FONT: Pixels = px(12.5);

    /// Notification bell: a 34 by 30 px tinted button, cut 6, 16 px icon.
    pub const BELL_WIDTH: Pixels = px(34.0);
    pub const BELL_HEIGHT: Pixels = px(30.0);
    pub const BELL_ICON: Pixels = px(16.0);
    /// Count badge on the bell: 15 px tall, at least 16 px wide, 4 px
    /// padding, 10 px bold text, 5 px past the bell's top and right edges.
    pub const BELL_BADGE_HEIGHT: Pixels = px(15.0);
    pub const BELL_BADGE_MIN_WIDTH: Pixels = px(16.0);
    pub const BELL_BADGE_PADDING_X: Pixels = px(4.0);
    pub const BELL_BADGE_FONT: Pixels = px(10.0);
    pub const BELL_BADGE_OFFSET: Pixels = px(-5.0);

    /// Activity rail: 46 px wide on the desk, no fill and no line.
    pub const RAIL_WIDTH: Pixels = px(46.0);
    /// Rail buttons: 38 px square, cut 6, 19 px icon, 6 px apart, 4 px from
    /// the top and the bottom of the rail.
    pub const RAIL_BUTTON: Pixels = px(38.0);
    pub const RAIL_ICON: Pixels = px(19.0);
    pub const RAIL_GAP: Pixels = px(6.0);
    pub const RAIL_PADDING_Y: Pixels = px(4.0);
    /// Wash of the active rail button, over the tint.
    pub const RAIL_ACTIVE_ALPHA: f32 = 0.16;

    /// Sidebar island: 290 px wide by default.
    pub const SIDEBAR_WIDTH: Pixels = px(290.0);
    /// Sidebar header: 46 px, 16 px left and 12 px right padding, 8 px gap.
    pub const SIDEBAR_HEADER_HEIGHT: Pixels = px(46.0);
    /// Section label of the sidebar header and the empty workspace cards. (10 px)
    pub const SECTION_LABEL_FONT: Pixels = px(10.0);
    /// Sidebar filter: 12 px side and 10 px bottom padding around a 30 px
    /// field; 14 px search icon.
    pub const SIDEBAR_FILTER_PADDING_X: Pixels = px(12.0);
    pub const SIDEBAR_FILTER_PADDING_BOTTOM: Pixels = px(10.0);
    pub const SIDEBAR_FILTER_ICON: Pixels = px(14.0);
    /// Sidebar footer: 46 px with no line above it, 16 px padding, 10 px gap.
    pub const SIDEBAR_FOOTER_HEIGHT: Pixels = px(46.0);
    pub const SIDEBAR_FOOTER_PADDING_X: Pixels = px(16.0);
    pub const SIDEBAR_FOOTER_GAP: Pixels = px(10.0);
    pub const SIDEBAR_FOOTER_FONT: Pixels = px(12.0);

    /// Status bar: 38 px on the desk, 8 px side padding and 8 px between
    /// its chips, 12 px text.
    pub const STATUS_BAR_HEIGHT: Pixels = px(38.0);
    pub const STATUS_BAR_PADDING_X: Pixels = px(8.0);
    pub const STATUS_BAR_GAP: Pixels = px(8.0);
    pub const STATUS_FONT: Pixels = px(12.0);
    /// Status chip: a 26 px island chip, cut 6, 12 px padding, 8 px gap,
    /// 13 px icon.
    pub const STATUS_CHIP_HEIGHT: Pixels = px(26.0);
    pub const STATUS_CHIP_PADDING_X: Pixels = px(12.0);
    pub const STATUS_CHIP_GAP: Pixels = px(8.0);
    pub const STATUS_ICON: Pixels = px(13.0);
    /// Wash of the connection chip, over the success color.
    pub const STATUS_CHIP_ALPHA: f32 = 0.12;

    /// Expanded tasks panel: 190 px tall, 34 px header, 30 px rows, 14 px
    /// padding, 10 px gap.
    pub const TASKS_PANEL_HEIGHT: Pixels = px(190.0);
    pub const TASKS_HEADER_HEIGHT: Pixels = px(34.0);
    pub const TASK_ROW_HEIGHT: Pixels = px(30.0);
    pub const TASKS_PADDING_X: Pixels = px(14.0);
    pub const TASKS_GAP: Pixels = px(10.0);
    /// Task rows: 12.5 px text, 11 px mono metadata, 11 px chevron, 14 px
    /// status icon.
    pub const TASK_FONT: Pixels = px(12.5);
    pub const TASK_META_FONT: Pixels = px(11.0);
    pub const TASK_CHEVRON: Pixels = px(11.0);
    pub const TASK_ICON: Pixels = px(14.0);
    /// Progress track: 160 by 4 px.
    pub const TASK_PROGRESS_WIDTH: Pixels = px(160.0);
    pub const TASK_PROGRESS_HEIGHT: Pixels = px(4.0);
    /// Error line under a failed task: 6 px top, 8 px bottom padding, text
    /// aligned with the task name (58 px), 11.5 px mono, on a 6% danger wash.
    pub const TASK_ERROR_PADDING_TOP: Pixels = px(6.0);
    pub const TASK_ERROR_PADDING_BOTTOM: Pixels = px(8.0);
    pub const TASK_ERROR_INDENT: Pixels = px(58.0);
    pub const TASK_ERROR_FONT: Pixels = px(11.5);
    pub const TASK_ERROR_ALPHA: f32 = 0.06;
    /// The task row the keyboard points at: a 10% tint wash.
    pub const TASK_SELECTED_ALPHA: f32 = 0.10;

    /// Empty workspace: 700 px column, 26 px between blocks, 16 px between
    /// the glyph and the title and between the cards. Wide enough that
    /// longer translations of "Command palette" stay on one line next to
    /// their shortcut keys.
    pub const EMPTY_WIDTH: Pixels = px(700.0);
    pub const EMPTY_GAP: Pixels = px(26.0);
    pub const EMPTY_HEAD_GAP: Pixels = px(16.0);
    pub const EMPTY_GLYPH: Pixels = px(44.0);
    /// Title: 22 px Archivo Expanded Black.
    pub const EMPTY_TITLE_FONT: Pixels = px(22.0);
    pub const EMPTY_TITLE_GAP: Pixels = px(4.0);
    /// Card label: 12 px top, 14 px side and 8 px bottom padding.
    pub const CARD_LABEL_PADDING_TOP: Pixels = px(12.0);
    pub const CARD_PADDING_X: Pixels = px(14.0);
    pub const CARD_LABEL_PADDING_BOTTOM: Pixels = px(8.0);
    /// Start rows: 36 px, 12 px gap, 16 px icon. Recent rows: 36 px, 10 px
    /// gap, 15 px icon, 11.5 px metadata. Both row heights match so the two
    /// cards are the same height when each holds the same number of rows.
    pub const START_ROW_HEIGHT: Pixels = px(36.0);
    pub const START_ROW_GAP: Pixels = px(12.0);
    pub const START_ROW_ICON: Pixels = px(16.0);
    pub const RECENT_ROW_HEIGHT: Pixels = px(36.0);
    pub const RECENT_ROW_GAP: Pixels = px(10.0);
    pub const RECENT_ROW_ICON: Pixels = px(15.0);
    pub const RECENT_META_FONT: Pixels = px(11.5);

    /// Failed connection block under its tree row: 34 px from the sidebar's
    /// left edge at the first level, 10 px from its right edge, 2 px above
    /// and 6 px below, 10 by 12 px padding, a 2 px danger edge on an 8%
    /// danger wash, 11.5 px mono text on a 1.5 line height, 8 px above the
    /// action buttons, which are 6 px apart.
    pub const FAILURE_MARGIN_RIGHT: Pixels = px(10.0);
    pub const FAILURE_MARGIN_TOP: Pixels = px(2.0);
    pub const FAILURE_PADDING_Y: Pixels = px(10.0);
    pub const FAILURE_PADDING_X: Pixels = px(12.0);
    pub const FAILURE_EDGE: Pixels = px(2.0);
    pub const FAILURE_ALPHA: f32 = 0.08;
    pub const FAILURE_FONT: Pixels = px(11.5);
    pub const FAILURE_LINE_HEIGHT: Pixels = px(17.25);
    pub const FAILURE_ACTIONS_GAP_TOP: Pixels = px(8.0);
    pub const FAILURE_ACTION_GAP: Pixels = px(6.0);
    pub const FAILURE_ACTION_ICON: Pixels = px(13.0);
    /// Inline status of a tree row ("retry", "connecting"): 11 px, 5 px gap.
    pub const ROW_STATUS_FONT: Pixels = px(11.0);
    pub const ROW_STATUS_GAP: Pixels = px(5.0);
}

/// Geometry of the notifications center (IslNotifications,
/// IslNotificationStates).
pub struct NotificationMetrics;

impl NotificationMetrics {
    /// Tint wash behind the bell while its popover is open. (26 %)
    pub const BELL_OPEN_ALPHA: f32 = 0.26;

    /// The popover: 440 px wide, at most 70 % of the window tall, cut 12,
    /// 2 px under the title bar.
    pub const POPOVER_WIDTH: Pixels = px(440.0);
    pub const POPOVER_MAX_HEIGHT_FRACTION: f32 = 0.70;
    pub const POPOVER_GAP_TOP: Pixels = px(2.0);

    /// Header: 46 px, 14 px left and 10 px right padding, 10 px gaps, a
    /// 15 px bell and an 11.5 px mono unread count.
    pub const HEADER_HEIGHT: Pixels = px(46.0);
    pub const HEADER_PADDING_LEFT: Pixels = px(14.0);
    pub const HEADER_PADDING_RIGHT: Pixels = px(10.0);
    pub const HEADER_GAP: Pixels = px(10.0);
    pub const HEADER_ICON: Pixels = px(15.0);
    pub const UNREAD_FONT: Pixels = px(11.5);

    /// Filter chips: 26 px tall, cut 4, 10 px side padding, 6 px apart and
    /// between label and count, 12 px label and 11 px mono count; the row
    /// is padded 10 px on top, 14 px on the sides and 4 px below.
    pub const CHIP_HEIGHT: Pixels = px(26.0);
    pub const CHIP_PADDING_X: Pixels = px(10.0);
    pub const CHIP_GAP: Pixels = px(6.0);
    pub const CHIP_FONT: Pixels = px(12.0);
    pub const COUNT_FONT: Pixels = px(11.0);
    pub const CHIPS_PADDING_TOP: Pixels = px(10.0);
    pub const CHIPS_PADDING_X: Pixels = px(14.0);
    pub const CHIPS_PADDING_BOTTOM: Pixels = px(4.0);

    /// Group label: 12 px above, 6 px below, 12 px left, 14 px right, 8 px
    /// between the label and its count, 10 px expanded caps.
    pub const GROUP_PADDING_TOP: Pixels = px(12.0);
    pub const GROUP_PADDING_BOTTOM: Pixels = px(6.0);
    pub const GROUP_PADDING_LEFT: Pixels = px(12.0);
    pub const GROUP_PADDING_RIGHT: Pixels = px(14.0);
    pub const GROUP_GAP: Pixels = px(8.0);
    pub const GROUP_FONT: Pixels = px(10.0);

    /// Rows: 10 px vertical padding, 12 px left and 14 px right, 10 px
    /// between the unread diamond, the icon square and the text; a 7 px
    /// diamond 7 px from the top; a 28 px raised square, cut 4, with a
    /// 15 px icon; 4 px between the title, meta and action lines, 8 px
    /// between the title and its chip, 6 px between actions. Read rows are
    /// drawn at 72 %.
    pub const ROW_PADDING_Y: Pixels = px(10.0);
    pub const ROW_PADDING_LEFT: Pixels = px(12.0);
    pub const ROW_PADDING_RIGHT: Pixels = px(14.0);
    pub const ROW_GAP: Pixels = px(10.0);
    pub const DIAMOND: Pixels = px(7.0);
    pub const DIAMOND_OFFSET_TOP: Pixels = px(7.0);
    pub const ICON_BOX: Pixels = px(28.0);
    pub const ICON: Pixels = px(15.0);
    pub const TEXT_GAP: Pixels = px(4.0);
    pub const TITLE_GAP: Pixels = px(8.0);
    pub const TITLE_FONT: Pixels = px(13.0);
    pub const META_FONT: Pixels = px(12.0);
    pub const ACTIONS_GAP: Pixels = px(6.0);
    pub const ACTIONS_MARGIN_TOP: Pixels = px(4.0);
    pub const READ_OPACITY: f32 = 0.72;
    /// The row the keyboard points at: a 10% tint wash.
    pub const SELECTED_ALPHA: f32 = 0.10;

    /// Footer: 40 px, 14 px side padding, 12 px text.
    pub const FOOTER_HEIGHT: Pixels = px(40.0);
    pub const FOOTER_PADDING_X: Pixels = px(14.0);
    pub const FOOTER_FONT: Pixels = px(12.0);

    /// Empty state: 46 px above, 50 px below, 30 px on the sides, 10 px
    /// gaps, a 44 px raised square with cut 8 and a 20 px check, and a
    /// 12.5 px message on a 1.5 line height.
    pub const EMPTY_PADDING_TOP: Pixels = px(46.0);
    pub const EMPTY_PADDING_BOTTOM: Pixels = px(50.0);
    pub const EMPTY_PADDING_X: Pixels = px(30.0);
    pub const EMPTY_GAP: Pixels = px(10.0);
    pub const EMPTY_ICON_BOX: Pixels = px(44.0);
    pub const EMPTY_ICON: Pixels = px(20.0);
    pub const EMPTY_MESSAGE_FONT: Pixels = px(12.5);
    pub const EMPTY_MESSAGE_LINE_HEIGHT: Pixels = px(18.75);

    /// Drop shadow under the popover: 28 px down, 70 px blur, black at
    /// 60 % on dark and 20 % on light.
    pub const SHADOW_OFFSET_Y: Pixels = px(28.0);
    pub const SHADOW_BLUR: Pixels = px(70.0);
    pub const SHADOW_ALPHA_DARK: f32 = 0.60;
    pub const SHADOW_ALPHA_LIGHT: f32 = 0.20;

    /// The popover's drop shadow for the current theme mode.
    pub fn shadow(theme: &gpui_component::Theme) -> BoxShadow {
        let alpha = if theme.mode.is_dark() {
            Self::SHADOW_ALPHA_DARK
        } else {
            Self::SHADOW_ALPHA_LIGHT
        };

        BoxShadow {
            color: gpui::hsla(0.0, 0.0, 0.0, alpha),
            offset: Point {
                x: px(0.0),
                y: Self::SHADOW_OFFSET_Y,
            },
            blur_radius: Self::SHADOW_BLUR,
            spread_radius: px(0.0),
            inset: false,
        }
    }
}

/// Geometry of the islands: the chamfered panes that float on the desk
/// (Isl* boards).
pub struct IslandMetrics;

impl IslandMetrics {
    /// Cut of the top-left and bottom-right corners of every island. (14 px)
    pub const CUT: Pixels = ChamferCut::CARD;
    /// Space between neighbouring islands and between an island and the
    /// window's side edges. (8 px)
    pub const GAP: Pixels = px(8.0);
    /// Thickness of the hairline along the island outline; zero draws none.
    pub const EDGE: Pixels = px(1.0);

    /// Title row of a window whose body is islands (Settings, Connection
    /// Manager): 40 px on the desk, 16 px side padding, no line.
    pub const WINDOW_TITLE_HEIGHT: Pixels = px(40.0);
    pub const WINDOW_TITLE_PADDING_X: Pixels = px(16.0);
}

/// Geometry of the master list of a master-detail page (P1SettingsMcp,
/// P1SettingsSsh): a 12 px toolbar with 8 px between its buttons, rows with
/// 10 by 14 px padding, 3 px between their lines, a 14 px icon 8 px before
/// the name and an 11.5 px mono detail line.
pub struct MasterListMetrics;

impl MasterListMetrics {
    pub const TOOLBAR_PADDING: Pixels = px(12.0);
    pub const TOOLBAR_GAP: Pixels = px(8.0);
    pub const ROW_PADDING_Y: Pixels = px(10.0);
    pub const ROW_PADDING_X: Pixels = px(14.0);
    pub const ROW_LINE_GAP: Pixels = px(3.0);
    pub const ROW_ICON: Pixels = px(14.0);
    pub const ROW_ICON_GAP: Pixels = px(8.0);
    pub const ROW_DETAIL_FONT: Pixels = px(11.5);
}

/// Geometry shared by the document views (P1Audit, P1Approvals, P1Dashboard,
/// P1Chart, P1Buckets, P1Objects, P1ObjectEditor, P1Builder, P1Schema,
/// P2Series): the document header and toolbar rows, the event and object
/// tables, the detail blocks and the footer.
pub struct DocumentMetrics;

impl DocumentMetrics {
    /// Header row: 44 px (48 px when it carries the view's controls), 14 px
    /// side padding, 8 px gap, 16 px tint icon, 14 px bold title.
    pub const HEADER_HEIGHT: Pixels = px(44.0);
    pub const HEADER_HEIGHT_TALL: Pixels = px(48.0);
    pub const PADDING_X: Pixels = px(14.0);
    pub const GAP: Pixels = px(8.0);
    pub const TITLE_ICON: Pixels = px(16.0);
    pub const TITLE_FONT: Pixels = px(14.0);
    /// Toolbar row under the header: 48 px, 9 px above and below its 30 px
    /// controls when they wrap onto a second line.
    pub const TOOLBAR_HEIGHT: Pixels = px(48.0);
    pub const TOOLBAR_PADDING_Y: Pixels = px(9.0);
    /// Vertical rule between toolbar groups: 20 px tall, 4 px margins.
    pub const TOOLBAR_RULE_HEIGHT: Pixels = px(20.0);
    pub const TOOLBAR_RULE_MARGIN_X: Pixels = px(4.0);
    /// Search field in a toolbar. (300 px)
    pub const SEARCH_WIDTH: Pixels = px(300.0);
    pub const SEARCH_ICON: Pixels = px(14.0);
    /// Leading icon of a toolbar select. (14 px)
    pub const SELECT_ICON: Pixels = px(14.0);
    /// Table rows: 32 px header and rows, 11.5 px header labels, 12.5 px
    /// cells, 12 px mono metadata cells.
    pub const TABLE_ROW_HEIGHT: Pixels = px(32.0);
    pub const TABLE_HEADER_FONT: Pixels = px(11.5);
    pub const TABLE_CELL_FONT: Pixels = px(12.5);
    pub const TABLE_META_FONT: Pixels = px(12.0);
    pub const TABLE_CHEVRON: Pixels = px(11.0);
    pub const TABLE_ICON: Pixels = px(13.0);
    /// Wash behind an expanded table row. (tint at 7 %)
    pub const EXPANDED_ROW_ALPHA: f32 = 0.07;
    /// Detail block under an expanded row: 14 px top, 16 px bottom, 54 px
    /// left (aligned with the Time column) and 14 px right padding, 16 px
    /// between fields, 4 px between label and value, 11 px labels.
    pub const DETAIL_PADDING_TOP: Pixels = px(14.0);
    pub const DETAIL_PADDING_BOTTOM: Pixels = px(16.0);
    pub const DETAIL_PADDING_LEFT: Pixels = px(54.0);
    pub const DETAIL_FIELD_GAP: Pixels = px(16.0);
    pub const DETAIL_LABEL_GAP: Pixels = px(4.0);
    pub const DETAIL_LABEL_FONT: Pixels = px(11.0);
    pub const DETAIL_FIELD_MIN_WIDTH: Pixels = px(140.0);
    /// Code block inside a detail: 14 px above, 12 by 14 px padding, 12 px
    /// mono at 1.7 line height; the action row sits 12 px below.
    pub const DETAIL_BLOCK_MARGIN_TOP: Pixels = px(14.0);
    pub const DETAIL_BLOCK_PADDING_Y: Pixels = px(12.0);
    pub const DETAIL_BLOCK_PADDING_X: Pixels = px(14.0);
    pub const DETAIL_BLOCK_FONT: Pixels = px(12.0);
    pub const DETAIL_BLOCK_LINE_HEIGHT: f32 = 1.7;
    pub const DETAIL_ACTIONS_MARGIN_TOP: Pixels = px(12.0);
    /// Timeline strip: 10 by 14 px padding, 52 px bars 2 px apart, 12 px to
    /// the legend, 8 px legend swatches, 11.5 px legend text.
    pub const TIMELINE_PADDING_Y: Pixels = px(10.0);
    pub const TIMELINE_BAR_HEIGHT: Pixels = px(52.0);
    pub const TIMELINE_BAR_GAP: Pixels = px(2.0);
    pub const TIMELINE_LEGEND_GAP: Pixels = px(12.0);
    pub const TIMELINE_SWATCH: Pixels = px(8.0);
    pub const TIMELINE_LEGEND_ROW_GAP: Pixels = px(6.0);
    pub const TIMELINE_LEGEND_FONT: Pixels = px(11.5);
    /// Number of bars in the timeline strip.
    pub const TIMELINE_BUCKETS: i64 = 48;
}

/// Geometry of the object-storage views (P1Buckets, P1Objects,
/// P1ObjectEditor): their tables, the bucket details strip and the object
/// preview rail.
pub struct ObjectStoreMetrics;

impl ObjectStoreMetrics {
    /// Search field in the header. (280 px)
    pub const SEARCH_WIDTH: Pixels = px(280.0);
    /// Table: 16 px side padding, 34 px bucket rows and 32 px object rows,
    /// a 15 px icon 9 px before the 13 px mono name (12.5 px for objects).
    pub const TABLE_PADDING_X: Pixels = px(16.0);
    pub const BUCKET_ROW_HEIGHT: Pixels = px(34.0);
    pub const OBJECT_ROW_HEIGHT: Pixels = px(32.0);
    pub const NAME_ICON: Pixels = px(15.0);
    pub const NAME_GAP: Pixels = px(9.0);
    pub const NAME_FONT: Pixels = px(13.0);
    pub const OBJECT_NAME_FONT: Pixels = px(12.5);
    /// Bucket columns after the name: region, objects, size, versioning,
    /// created.
    pub const REGION_WIDTH: Pixels = px(140.0);
    pub const OBJECTS_WIDTH: Pixels = px(120.0);
    pub const SIZE_WIDTH: Pixels = px(130.0);
    pub const VERSIONING_WIDTH: Pixels = px(130.0);
    pub const CREATED_WIDTH: Pixels = px(150.0);
    /// Bucket details strip: 14 by 16 px padding, 28 px between fields, a
    /// 16 px icon before the bold name, 13 px mono values.
    pub const DETAILS_PADDING_Y: Pixels = px(14.0);
    pub const DETAILS_GAP: Pixels = px(28.0);
    pub const DETAILS_ICON: Pixels = px(16.0);
    pub const DETAILS_VALUE_FONT: Pixels = px(13.0);
    /// Loader icon standing in for a value that is still being fetched.
    /// (12 px)
    pub const LOADING_ICON: Pixels = px(12.0);
    /// Path field: 6 px between its parts, a 12 px copy icon.
    pub const PATH_GAP: Pixels = px(6.0);
    pub const PATH_COPY_ICON: Pixels = px(12.0);
    /// Footer of the object-storage views. (34 px)
    pub const FOOTER_HEIGHT: Pixels = px(34.0);
    /// Object editor: 10 px above the first line, a 32 px footer with 16 px
    /// between its items.
    pub const EDITOR_PADDING_TOP: Pixels = px(10.0);
    pub const EDITOR_FOOTER_HEIGHT: Pixels = px(32.0);
    pub const EDITOR_FOOTER_GAP: Pixels = px(16.0);
}

/// Geometry of the object preview rail (P1Objects): its header, the
/// "Interpret as" row, the metadata section and the action row.
pub struct PreviewRailMetrics;

impl PreviewRailMetrics {
    /// Header: 10 px between the icon, the name and the buttons.
    pub const HEADER_GAP: Pixels = px(10.0);
    /// "Interpret as" row: 40 px, 5 px above and below a wrapped control.
    pub const INTERPRET_HEIGHT: Pixels = px(40.0);
    pub const INTERPRET_PADDING_Y: Pixels = px(5.0);
    /// Metadata section: 10 px above and below, a 10 px label, 5 px above
    /// and below each row.
    pub const SECTION_PADDING_Y: Pixels = px(10.0);
    pub const SECTION_LABEL_FONT: Pixels = px(10.0);
    pub const ROW_PADDING_Y: Pixels = px(5.0);
    /// Action row: 12 px above and below, 6 px between buttons.
    pub const ACTIONS_PADDING_Y: Pixels = px(12.0);
    pub const ACTIONS_GAP: Pixels = px(6.0);
}

/// Geometry of the MCP approvals view (P1Approvals): the pending list, the
/// detail title, its field grid and payload block, and the action row.
pub struct ApprovalsMetrics;

impl ApprovalsMetrics {
    /// Pending list: 380 px wide, 16 by 12 px padding, 6 px between the
    /// lines of a row, a 14 px icon, 11.5 px metadata, a 12 % tint wash on
    /// the selected row and 4 px between the key hints.
    pub const LIST_WIDTH: Pixels = px(380.0);
    pub const LIST_PADDING_X: Pixels = px(16.0);
    pub const LIST_PADDING_Y: Pixels = px(12.0);
    pub const ROW_GAP: Pixels = px(6.0);
    pub const ROW_ICON: Pixels = px(14.0);
    pub const META_FONT: Pixels = px(11.5);
    pub const SELECTED_ALPHA: f32 = 0.12;
    pub const HINT_GAP: Pixels = px(4.0);
    /// Section labels (PENDING, WHAT IT WILL RUN). (10 px)
    pub const SECTION_LABEL_FONT: Pixels = px(10.0);
    /// Detail title row: 52 px, 20 px side padding, 10 px gap, 18 px icon,
    /// 15 px bold mono tool name.
    pub const TITLE_HEIGHT: Pixels = px(52.0);
    pub const DETAIL_PADDING_X: Pixels = px(20.0);
    pub const TITLE_GAP: Pixels = px(10.0);
    pub const TITLE_ICON: Pixels = px(18.0);
    pub const TITLE_FONT: Pixels = px(15.0);
    /// Detail body: 22 px between sections, a three-column field grid with
    /// 18 px gaps, 10 px under a section label, 13 px value icons 6 px
    /// before the value.
    pub const SECTION_GAP: Pixels = px(22.0);
    pub const GRID_GAP: Pixels = px(18.0);
    pub const GRID_COLUMNS: usize = 3;
    pub const SECTION_TITLE_GAP: Pixels = px(10.0);
    pub const VALUE_ICON: Pixels = px(13.0);
    pub const VALUE_ICON_GAP: Pixels = px(6.0);
    /// Payload block: 16 by 14 px padding, 13 px mono at 1.75 line height.
    pub const CODE_PADDING_X: Pixels = px(16.0);
    pub const CODE_PADDING_Y: Pixels = px(14.0);
    pub const CODE_FONT: Pixels = px(13.0);
    pub const CODE_LINE_HEIGHT: f32 = 1.75;
    /// Action row: 14 px above and below.
    pub const FOOTER_PADDING_Y: Pixels = px(14.0);
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

/// Geometry of the schema diagram (P1Schema): the toolbar, the table
/// cards and the dot grid.
pub struct SchemaMetrics;

impl SchemaMetrics {
    /// Toolbar: 46 px, the zoom readout 44 px wide in 12 px mono.
    pub const TOOLBAR_HEIGHT: Pixels = px(46.0);
    pub const ZOOM_WIDTH: Pixels = px(44.0);
    pub const ZOOM_FONT: Pixels = px(12.0);
    /// Layout select width. (160 px)
    pub const LAYOUT_WIDTH: Pixels = px(160.0);
    /// Table card: 10 px side padding, 8 px header gap, a 13 px tint icon
    /// and 12 px bold mono name; rows with a 6 px gap, an 11 px key icon and
    /// 11.5 px mono text.
    pub const CARD_PADDING_X: Pixels = px(10.0);
    pub const HEADER_GAP: Pixels = px(8.0);
    pub const HEADER_ICON: Pixels = px(13.0);
    pub const HEADER_FONT: Pixels = px(12.0);
    pub const ROW_GAP: Pixels = px(6.0);
    pub const ROW_ICON: Pixels = px(11.0);
    pub const ROW_FONT: Pixels = px(11.5);
    /// Label of a card's index section. (10 px)
    pub const INDEX_LABEL_FONT: Pixels = px(10.0);
    /// Type column width inside a row. (56 px)
    pub const TYPE_WIDTH: Pixels = px(56.0);
    /// Wash on the selected card's header. (tint at 12 %)
    pub const SELECTED_HEADER_ALPHA: f32 = 0.12;
    /// Dot grid: 2 px dots on the line color.
    pub const DOT: Pixels = px(2.0);
    /// Types / Indexes toggle segments. (24 px)
    pub const TOGGLE_HEIGHT: Pixels = px(24.0);
}

/// Geometry of the schema inspector rail (P1Schema): 12 by 14 px padding,
/// 14 px between blocks, 6 px between rows, a 12.5 px summary, 12 px mono
/// rows with an 11 px icon 6 px before the value.
pub struct SchemaInspectorMetrics;

impl SchemaInspectorMetrics {
    pub const PADDING_X: Pixels = px(14.0);
    pub const PADDING_Y: Pixels = px(12.0);
    pub const GAP: Pixels = px(14.0);
    pub const ROW_GAP: Pixels = px(6.0);
    pub const SUMMARY_FONT: Pixels = px(12.5);
    pub const LABEL_FONT: Pixels = px(10.0);
    pub const ROW_FONT: Pixels = px(12.0);
    pub const ICON: Pixels = px(11.0);
    pub const ICON_GAP: Pixels = px(6.0);
    /// Header: 44 px tall, 14 px side padding, 8 px gap, a 15 px table icon.
    pub const HEADER_HEIGHT: Pixels = px(44.0);
    pub const HEADER_PADDING_X: Pixels = px(14.0);
    pub const HEADER_GAP: Pixels = px(8.0);
    pub const HEADER_ICON: Pixels = px(15.0);
}

/// Geometry of the document inspector (IslDocTable "Document" panel): the
/// header with the document size, and the rows of the nested field tree.
pub struct DocumentInspectorMetrics;

impl DocumentInspectorMetrics {
    /// Header: 44 px tall, 14 px side padding, 10 px gap, a 15 px braces
    /// icon and the 11 px mono size note.
    pub const HEADER_HEIGHT: Pixels = px(44.0);
    pub const HEADER_PADDING_X: Pixels = px(14.0);
    pub const HEADER_GAP: Pixels = px(10.0);
    pub const HEADER_ICON: Pixels = px(15.0);
    pub const SIZE_FONT: Pixels = px(11.0);
    /// Tree: 8 px above and below the rows.
    pub const BODY_PADDING_Y: Pixels = px(8.0);
    /// Row: 26 px tall, 14 px side padding plus 16 px per level, 6 px
    /// between chevron, key, colon and value, 12.5 px mono text.
    pub const ROW_HEIGHT: Pixels = px(26.0);
    pub const ROW_PADDING_X: Pixels = px(14.0);
    pub const INDENT: Pixels = px(16.0);
    pub const ROW_GAP: Pixels = px(6.0);
    pub const CHEVRON: Pixels = px(11.0);
    pub const ROW_FONT: Pixels = px(12.5);
    /// Type chip column at the right: 44 px wide, 10 px text.
    pub const TYPE_WIDTH: Pixels = px(44.0);
    pub const TYPE_FONT: Pixels = px(10.0);
    /// A row with a staged edit: a 2 px warning edge at its left.
    pub const PENDING_EDGE: Pixels = px(2.0);
}

/// Geometry of the visual query builder rail (P1Builder): its header, the
/// mode switch, the section cards and the footer.
pub struct BuilderMetrics;

impl BuilderMetrics {
    /// 14 px side padding for the header, cards column and footer.
    pub const RAIL_PADDING_X: Pixels = px(14.0);
    /// Header: 46 px, 10 px gap, a 16 px tint icon.
    pub const HEADER_HEIGHT: Pixels = px(46.0);
    pub const HEADER_GAP: Pixels = px(10.0);
    pub const HEADER_ICON: Pixels = px(16.0);
    /// Mode switch row: 10 px above and below.
    pub const MODE_PADDING_Y: Pixels = px(10.0);
    /// Cards: 10 px apart, 12 px padding, 10 px between header and body, an
    /// 8 px gap and a 14 px icon in the header, a 10 px label.
    pub const SECTION_GAP: Pixels = px(10.0);
    pub const CARD_PADDING: Pixels = px(12.0);
    pub const CARD_GAP: Pixels = px(10.0);
    pub const CARD_HEADER_GAP: Pixels = px(8.0);
    pub const CARD_ICON: Pixels = px(14.0);
    pub const CARD_LABEL_FONT: Pixels = px(10.0);
    /// Chips in the Columns card: 6 px apart.
    pub const CHIP_GAP: Pixels = px(6.0);
    /// Rows inside a card: 6 px between controls.
    pub const ROW_GAP: Pixels = px(6.0);
    /// Filter row controls: 120 px column select, 64 px comparator.
    pub const FILTER_COLUMN_WIDTH: Pixels = px(120.0);
    pub const FILTER_COMPARATOR_WIDTH: Pixels = px(64.0);
    /// Sort and limit row: a 150 px column dropdown and a 70 px limit field.
    pub const SORT_COLUMN_WIDTH: Pixels = px(150.0);
    pub const SORT_LIMIT_WIDTH: Pixels = px(70.0);
    /// "valid" status: 11.5 px text, 12 px icon 5 px before it.
    pub const STATUS_FONT: Pixels = px(11.5);
    pub const STATUS_ICON: Pixels = px(12.0);
    pub const STATUS_GAP: Pixels = px(5.0);
    /// SQL preview editor height. (140 px)
    pub const PREVIEW_HEIGHT: Pixels = px(140.0);
    /// Footer: 12 px above and below, 8 px between buttons.
    pub const FOOTER_PADDING_Y: Pixels = px(12.0);
    pub const FOOTER_GAP: Pixels = px(8.0);
    /// Link text ("+ Filter · + Group"). (12 px)
    pub const LINK_FONT: Pixels = px(12.0);
    /// Keyboard cursor: the row it is on gets a tint wash at this alpha, and
    /// the field it points at a 1 px tint ring.
    pub const CURSOR_ROW_ALPHA: f32 = 0.10;
    /// Space kept around a row the cursor scrolls into view. (8 px)
    pub const CURSOR_REVEAL_MARGIN: Pixels = px(8.0);
}

/// Geometry of the dashboard grid (P1Dashboard): the grid padding and
/// gutter, the panel card and its header, the section dividers and the
/// resize affordances.
pub struct DashboardMetrics;

impl DashboardMetrics {
    /// Grid: 16 px around the panels and 12 px between them, laid out as a
    /// 10 px container padding plus a 6 px gutter on each panel.
    pub const GRID_PADDING: Pixels = px(10.0);
    pub const PANEL_GUTTER: Pixels = px(6.0);
    /// Panel header: 36 px, 12 px side padding, 8 px gap, a 14 px kind
    /// icon, a 12 px drag grip, a 12.5 px semibold title.
    pub const PANEL_HEADER_HEIGHT: Pixels = px(36.0);
    pub const PANEL_PADDING: Pixels = px(12.0);
    pub const PANEL_ICON: Pixels = px(14.0);
    pub const PANEL_GRIP: Pixels = px(12.0);
    pub const PANEL_TITLE_FONT: Pixels = px(12.5);
    /// Resize affordances: an 8 px hit strip on the right and bottom edges
    /// and a 14 px corner triangle.
    pub const RESIZE_STRIP: Pixels = px(8.0);
    pub const RESIZE_CORNER: Pixels = px(14.0);
    /// Section divider: 12 px between the chevron, label and rule, a 12 px
    /// chevron and a 10 px label.
    pub const DIVIDER_GAP: Pixels = px(12.0);
    pub const DIVIDER_CHEVRON: Pixels = px(12.0);
    pub const DIVIDER_LABEL_FONT: Pixels = px(10.0);
    /// Room under the last row so it clears the tasks splitter. (24 px)
    pub const BOTTOM_SLACK: Pixels = px(24.0);
    /// Empty state height. (240 px)
    pub const EMPTY_HEIGHT: Pixels = px(240.0);
}

/// Geometry of the chart document (P1Chart): the axis row, the chart area
/// padding and the Stats rail.
pub struct ChartDocumentMetrics;

impl ChartDocumentMetrics {
    /// Axis row under the header. (46 px)
    pub const AXIS_ROW_HEIGHT: Pixels = px(46.0);
    /// Chart area: 24 px above, 28 px on the sides and below.
    pub const AREA_PADDING_TOP: Pixels = px(24.0);
    pub const AREA_PADDING: Pixels = px(28.0);
    /// Stats rail: 300 px wide, 14 by 16 px padding, 10 px under a section
    /// label, 6 px above and below each row, 16 px above SERIES, 30 px
    /// series rows with a 10 px swatch, a 13 px close icon.
    pub const RAIL_WIDTH: Pixels = px(300.0);
    pub const RAIL_PADDING_Y: Pixels = px(14.0);
    pub const RAIL_PADDING_X: Pixels = px(16.0);
    pub const RAIL_LABEL_GAP: Pixels = px(10.0);
    pub const RAIL_ROW_PADDING_Y: Pixels = px(6.0);
    pub const RAIL_SECTION_GAP: Pixels = px(16.0);
    pub const RAIL_SERIES_ROW_HEIGHT: Pixels = px(30.0);
    pub const RAIL_SWATCH: Pixels = px(10.0);
    pub const RAIL_ICON: Pixels = px(13.0);
    pub const RAIL_LABEL_FONT: Pixels = px(10.0);
    /// Width of the metric picker rail. (320 px)
    pub const PICKER_WIDTH: Pixels = px(320.0);
}

/// Geometry of the chart axis row (P1Chart): the X, Y, Group and Agg
/// selects with their role labels.
pub struct AxisBarMetrics;

impl AxisBarMetrics {
    /// 8 px between a role label and its select, and between fields.
    pub const GAP: Pixels = px(8.0);
    /// Role labels ("X", "Y", "Group"). (12 px)
    pub const ROLE_FONT: Pixels = px(12.0);
    /// Select widths: 190 px for X, 150 px for Y and Group, 110 px for Agg.
    pub const X_WIDTH: Pixels = px(190.0);
    pub const FIELD_WIDTH: Pixels = px(150.0);
    pub const AGG_WIDTH: Pixels = px(110.0);
    /// Picker offset below the select. (34 px)
    pub const PICKER_OFFSET: Pixels = px(34.0);
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
    /// the MCP (clients, roles, policies) settings sections. (280 px)
    pub const SETTINGS_LIST_PANEL: Pixels = px(280.0);

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
