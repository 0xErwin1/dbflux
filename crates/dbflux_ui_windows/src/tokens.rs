//! Geometry of the Settings and Connection Manager windows, taken from the
//! P1Settings*, P1ConnForm and P1DriverPicker boards.

use gpui::{Pixels, px};

/// Settings window: navigation column, page body and footer.
pub struct SettingsMetrics;

impl SettingsMetrics {
    /// Navigation island width. (230 px)
    pub const NAV_WIDTH: Pixels = px(230.0);
    /// Padding around the navigation search field. (12 px)
    pub const NAV_SEARCH_PADDING: Pixels = px(12.0);
    /// Navigation group label: 12 px top, 16 px side, 6 px bottom padding.
    pub const NAV_GROUP_PADDING_TOP: Pixels = px(12.0);
    pub const NAV_GROUP_PADDING_X: Pixels = px(16.0);
    pub const NAV_GROUP_PADDING_BOTTOM: Pixels = px(6.0);
    /// Navigation row: 32 px tall, 16 px side padding, 10 px gap, 15 px icon.
    pub const NAV_ROW_HEIGHT: Pixels = px(32.0);
    pub const NAV_ROW_PADDING_X: Pixels = px(16.0);
    pub const NAV_ROW_GAP: Pixels = px(10.0);
    pub const NAV_ICON: Pixels = px(15.0);
    /// Search icon of the navigation search field. (14 px)
    pub const NAV_SEARCH_ICON: Pixels = px(14.0);

    /// Horizontal padding of a settings page body. (28 px)
    pub const BODY_PADDING_X: Pixels = px(28.0);
    /// Top padding of the detail pane of a master-detail page. (4 px)
    pub const DETAIL_PADDING_TOP: Pixels = px(4.0);
    /// Bottom padding of a page head that draws a line under it. (14 px)
    pub const PAGE_HEAD_PADDING_BOTTOM: Pixels = px(14.0);

    /// Footer on the window frame: 56 px with no line above it, 18 px
    /// padding, 8 px gap.
    pub const FOOTER_HEIGHT: Pixels = px(56.0);
    pub const FOOTER_PADDING_X: Pixels = px(18.0);
    pub const FOOTER_GAP: Pixels = px(8.0);
    /// Unsaved-changes marker: a 7 px diamond, 8 px before 12.5 px text.
    pub const DIRTY_MARKER: Pixels = px(7.0);
    pub const DIRTY_FONT: Pixels = px(12.5);

    /// Master list of a master-detail page: 280 px wide, 12 px toolbar
    /// padding with 8 px between its buttons.
    pub const LIST_WIDTH: Pixels = px(280.0);
    pub const LIST_TOOLBAR_PADDING: Pixels = px(12.0);
    /// Master list row: 10 px by 14 px padding, 3 px between its lines,
    /// 8 px between the icon and the name, 14 px icon, 11.5 px mono detail.
    pub const LIST_ROW_PADDING_Y: Pixels = px(10.0);
    pub const LIST_ROW_PADDING_X: Pixels = px(14.0);
    pub const LIST_ROW_LINE_GAP: Pixels = px(3.0);
    pub const LIST_ROW_ICON: Pixels = px(14.0);
    pub const LIST_ROW_META_FONT: Pixels = px(11.5);

    /// Form rows of a settings page: 200 px label column.
    pub const FORM_LABEL_WIDTH: Pixels = px(200.0);
    /// Width of a short numeric field (history entries, intervals). (140 px)
    pub const NUMBER_FIELD_WIDTH: Pixels = px(140.0);
    /// Width of a select trigger in a form row. (280 px)
    pub const SELECT_WIDTH: Pixels = px(280.0);
    /// Width of a text field in a detail form. (360 px)
    pub const TEXT_FIELD_WIDTH: Pixels = px(360.0);
    /// Width of the port field next to a host field. (80 px)
    pub const PORT_FIELD_WIDTH: Pixels = px(80.0);
}

/// Form rows shared by the Settings pages and the Connection Manager form.
pub struct FormMetrics;

impl FormMetrics {
    /// Vertical padding of a form row. (7 px)
    pub const ROW_PADDING_Y: Pixels = px(7.0);
    /// Gap between the label column and the control column. (16 px)
    pub const ROW_GAP: Pixels = px(16.0);
    /// Top padding of the label, which centers it on a 30 px control. (7 px)
    pub const LABEL_PADDING_TOP: Pixels = px(7.0);
    /// Gap between a control and its helper line. (6 px)
    pub const HELP_GAP: Pixels = px(6.0);
    /// Helper line text size. (12 px)
    pub const HELP_FONT: Pixels = px(12.0);
    /// Checkbox row: 6 px vertical padding, 3 px between the label and its
    /// description.
    pub const CHECK_ROW_PADDING_Y: Pixels = px(6.0);
    pub const CHECK_ROW_LINE_GAP: Pixels = px(3.0);
    /// Gap between the controls placed side by side in one row. (8 px)
    pub const INLINE_GAP: Pixels = px(8.0);
}

/// Notes under the execution classes of an MCP policy.
#[cfg(feature = "mcp")]
pub struct PolicyNoteMetrics;

#[cfg(feature = "mcp")]
impl PolicyNoteMetrics {
    /// Defaults note: a 13 px icon 10 px from its text, with 10 px above and
    /// 2 px below.
    pub const ICON: Pixels = px(13.0);
    pub const GAP: Pixels = px(10.0);
    pub const PADDING_TOP: Pixels = px(10.0);
    pub const PADDING_BOTTOM: Pixels = px(2.0);
    /// Space above the "Allow all without approval" banner. (12 px)
    pub const BANNER_MARGIN_TOP: Pixels = px(12.0);
}

/// Connection Manager window: form header, tabs, body and footer, and the
/// driver picker.
pub struct ConnectionFormMetrics;

impl ConnectionFormMetrics {
    /// Label column of the connection form. (170 px)
    pub const LABEL_WIDTH: Pixels = px(170.0);
    /// Horizontal padding of the header and the form body. (26 px)
    pub const PADDING_X: Pixels = px(26.0);
    /// Vertical padding of the form header. (16 px)
    pub const HEADER_PADDING_Y: Pixels = px(16.0);
    /// Gap between the parts of the form header. (12 px)
    pub const HEADER_GAP: Pixels = px(12.0);
    /// Driver logo in the form header. (28 px)
    pub const HEADER_LOGO: Pixels = px(28.0);
    /// Form title (18 px bold) and its subtitle (12.5 px), 2 px apart.
    pub const TITLE_FONT: Pixels = px(18.0);
    pub const SUBTITLE_FONT: Pixels = px(12.5);
    pub const TITLE_GAP: Pixels = px(2.0);
    /// Name field of the form header. (240 px)
    pub const NAME_FIELD_WIDTH: Pixels = px(240.0);
    /// Top padding of the form body. (4 px)
    pub const BODY_PADDING_TOP: Pixels = px(4.0);
    /// Bottom margin of the test-result banner. (14 px)
    pub const BANNER_MARGIN_BOTTOM: Pixels = px(14.0);
    /// Width of the database field. (300 px)
    pub const DATABASE_FIELD_WIDTH: Pixels = px(300.0);
    /// Width of the port field next to the host field. (90 px)
    pub const PORT_FIELD_WIDTH: Pixels = px(90.0);
    /// Width of the value-source select before a credential field. (170 px)
    pub const SOURCE_SELECT_WIDTH: Pixels = px(170.0);

    /// Environment chips: 28 px tall, 12 px padding, 7 px gap, 6 px apart,
    /// 12.5 px semibold text, a 7 px diamond and a 13% wash of the
    /// environment color on the selected chip.
    pub const ENV_CHIP_HEIGHT: Pixels = px(28.0);
    pub const ENV_CHIP_PADDING_X: Pixels = px(12.0);
    pub const ENV_CHIP_GAP: Pixels = px(7.0);
    pub const ENV_CHIPS_GAP: Pixels = px(6.0);
    pub const ENV_CHIP_FONT: Pixels = px(12.5);
    pub const ENV_CHIP_DIAMOND: Pixels = px(7.0);
    pub const ENV_CHIP_WASH_ALPHA: f32 = 0.13;

    /// Driver picker header: 18 by 24 px padding, 14 px gap, 3 px between
    /// the title and its subtitle, 340 px filter field.
    pub const PICKER_HEADER_PADDING_Y: Pixels = px(18.0);
    pub const PICKER_PADDING_X: Pixels = px(24.0);
    pub const PICKER_HEADER_GAP: Pixels = px(14.0);
    pub const PICKER_TITLE_GAP: Pixels = px(3.0);
    pub const PICKER_FILTER_WIDTH: Pixels = px(340.0);
    /// Driver picker body: 16 px bottom padding; category label 16 px above
    /// and 8 px below.
    pub const PICKER_BODY_PADDING_BOTTOM: Pixels = px(16.0);
    pub const PICKER_CATEGORY_PADDING_TOP: Pixels = px(16.0);
    pub const PICKER_CATEGORY_PADDING_BOTTOM: Pixels = px(8.0);
    /// Driver cards: 10 px apart, 14 px padding, 12 px gap, 26 px logo,
    /// 3 px between the name and the detail, 11.5 px mono detail, at least
    /// 220 px wide and at most four per row.
    pub const CARD_GAP: Pixels = px(10.0);
    pub const CARD_PADDING: Pixels = px(14.0);
    pub const CARD_INNER_GAP: Pixels = px(12.0);
    pub const CARD_LOGO: Pixels = px(26.0);
    pub const CARD_LINE_GAP: Pixels = px(3.0);
    pub const CARD_DETAIL_FONT: Pixels = px(11.5);
    /// Line boxes of the card's name (15 px) and hint (14 px), which keep
    /// the card at the board's 58 px.
    pub const CARD_NAME_LINE_HEIGHT: Pixels = px(15.0);
    pub const CARD_DETAIL_LINE_HEIGHT: Pixels = px(14.0);
    pub const CARD_MIN_WIDTH: Pixels = px(220.0);
    pub const CARD_MAX_COLUMNS: usize = 4;
    /// Selected card: 1.5 px tint ring over an 8% tint wash.
    pub const CARD_SELECTED_RING: Pixels = px(1.5);
    pub const CARD_SELECTED_ALPHA: f32 = 0.08;
    /// Check mark of the selected card. (14 px)
    pub const CARD_CHECK: Pixels = px(14.0);
}
