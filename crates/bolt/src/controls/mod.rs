//! Generic controls a screen composes out of tokens and the theme.

mod button;
mod checkbox;
mod dropdown;
mod input;
mod input_decorations;
mod key_capture;
mod multi_select;
mod option_group;
mod readonly_text_view;
mod search_field;
mod select_trigger;
mod type_to_confirm;
mod value_field;

pub use button::{
    Button, ButtonFills, ButtonSize, ButtonVariant, button_colors, is_activation_key,
};
pub use checkbox::Checkbox;
pub use dropdown::{
    BadgeLabel, Dropdown, DropdownDismissed, DropdownItem, DropdownSelectionChanged,
};
pub use input::{
    CodeActionProvider, CompletionProvider, GpuiInput, INPUT_CONTEXT, Input, InputContentType,
    InputEnter, InputEscape, InputEvent, InputIndentInline, InputMoveDown, InputMoveUp,
    InputOutdentInline, InputPosition, InputSearch, InputState, ReadOnlyAccessibility,
    ReadOnlyEditor, Rope, RopeExt, TriggerCompletion,
};
pub use input_decorations::{
    TextDecoration, TextDecorationCollection, decorate_input, styled_ranges,
};
pub use key_capture::{KeyCapture, KeyCaptureHandler};
pub use multi_select::{MultiSelect, MultiSelectChanged};
pub use option_group::{
    OptionGroup, OptionGroupItem, option_group_description_id, option_group_id,
    option_group_item_id,
};
pub use readonly_text_view::ReadonlyTextView;
pub use search_field::{SearchCount, SearchField, SearchTrigger};
pub use select_trigger::SelectTrigger;
pub use type_to_confirm::{TypeToConfirm, TypeToConfirmEvent};
pub use value_field::ValueField;
pub(crate) use value_field::{one_line, value_field_base};
