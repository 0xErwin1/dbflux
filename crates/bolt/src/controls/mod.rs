//! Generic controls a screen composes out of tokens and the theme.

mod button;
mod checkbox;
mod dropdown;
mod input;
mod key_capture;
mod multi_select;
mod readonly_text_view;
mod search_field;
mod select_trigger;
mod type_to_confirm;

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
pub use key_capture::{KeyCapture, KeyCaptureHandler};
pub use multi_select::{MultiSelect, MultiSelectChanged};
pub use readonly_text_view::ReadonlyTextView;
pub use search_field::{SearchCount, SearchField};
pub use select_trigger::SelectTrigger;
pub use type_to_confirm::{TypeToConfirm, TypeToConfirmEvent};
