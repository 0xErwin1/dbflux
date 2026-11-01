//! Generic controls a screen composes out of tokens and the theme.

mod button;
mod checkbox;
mod dropdown;
mod input;
mod readonly_text_view;
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
pub use readonly_text_view::ReadonlyTextView;
pub use select_trigger::SelectTrigger;
pub use type_to_confirm::{TypeToConfirm, TypeToConfirmEvent};
