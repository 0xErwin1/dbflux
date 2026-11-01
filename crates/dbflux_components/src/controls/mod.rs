// The generic controls moved to `bolt`; re-exported at these paths so call
// sites keep compiling. The product couplings — the keymap action the
// dropdowns answer, the connection-environment badge and the translated
// placeholder — stay here as extensions over the generic controls. Bolt's
// keymap seam lives at `bolt::keymap`.

mod dropdown;

pub use bolt::controls::{
    BadgeLabel, Button, ButtonFills, ButtonSize, ButtonVariant, Checkbox, Dropdown,
    DropdownDismissed, DropdownItem, DropdownSelectionChanged, DropdownStrings, button_colors,
    is_activation_key,
};
pub use bolt::controls::{
    CodeActionProvider, CompletionProvider, GpuiInput, INPUT_CONTEXT, Input, InputContentType,
    InputEnter, InputEscape, InputEvent, InputIndentInline, InputMoveDown, InputMoveUp,
    InputOutdentInline, InputPosition, InputSearch, InputState, ReadOnlyAccessibility,
    ReadOnlyEditor, ReadonlyTextView, Rope, RopeExt, TriggerCompletion,
};
#[cfg(test)]
pub(crate) use dropdown::bind_dropdown_keys_for_tests;
pub use dropdown::{DropdownEnvironment, install_keymap_seams};
