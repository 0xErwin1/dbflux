//! The keymap engine: default bindings, user overrides, and the native GPUI
//! bindings generated from them.
//!
//! Every binding is a GPUI [`KeyBinding`] with a context predicate in GPUI's
//! language (`Editor && vim_mode == normal`, `Sidebar && !Modal`, `A > B`),
//! so GPUI resolves precedence, key sequences (`g g`, `Ctrl+K Ctrl+S`) and
//! their timeout. Two kinds of key context feed those predicates:
//!
//! - A window root sets the identifier of the context that owns the keyboard
//!   (see [`root_key_context`]): the workspace computes it from its focus
//!   model, because keyboard focus often stays on the root while a panel is
//!   the active one. Bindings of these contexts dispatch [`RunCommand`], which
//!   the root handles.
//! - An element sets its own identifier on itself (`DataTable`, `Input`,
//!   `Modal`, `DocumentTree`, the modal editors). Its bindings dispatch the
//!   element's own actions and win over the root because they sit deeper.
//!
//! These helpers live in `dbflux_ui_base` rather than `dbflux_components`
//! because they require `dbflux_app::keymap` types, which `dbflux_components`
//! intentionally does not depend on.

mod defaults;
#[cfg(test)]
mod tests;

use dbflux_app::keymap::{
    BindingSlot, Command, ContextId, EffectiveBinding, KeyChord, KeySequence, KeymapOverrides,
    KeymapStack, Modifiers, PredicateOverlap,
};
use dbflux_components::actions as component_actions;
use dbflux_components::components::{data_table, document_tree};
use dbflux_components::controls::{InputMoveDown, InputMoveUp, TriggerCompletion};
use dbflux_components::key_contexts;
use gpui::{
    Action, App, DummyKeyboardMapper, Global, KeyBinding, KeyBindingContextPredicate,
    KeyBindingMetaIndex, KeyContext, Keystroke, SharedString,
};
use std::rc::Rc;
use std::sync::{Arc, LazyLock, RwLock};

pub use dbflux_components::actions::RunCommand;

// ============================================================================
// GPUI keystroke conversion helpers
// ============================================================================

/// Creates a [`KeyChord`] from a GPUI [`Keystroke`].
pub fn key_chord_from_gpui(keystroke: &Keystroke) -> KeyChord {
    KeyChord {
        key: keystroke.key.clone(),
        modifiers: modifiers_from_gpui(&keystroke.modifiers),
    }
}

/// Creates [`Modifiers`] from a GPUI [`gpui::Modifiers`] struct.
pub fn modifiers_from_gpui(mods: &gpui::Modifiers) -> Modifiers {
    Modifiers {
        ctrl: mods.control,
        alt: mods.alt,
        shift: mods.shift,
        platform: mods.platform,
    }
}

/// Splits a [`KeyChord`] into the display labels of its keys, in the order a
/// `Chord` badge row renders them (for example `["Ctrl", "Shift", "N"]`).
pub fn chord_display_parts(chord: &KeyChord) -> Vec<SharedString> {
    let mut parts: Vec<SharedString> = Vec::new();

    if chord.modifiers.ctrl {
        parts.push("Ctrl".into());
    }
    if chord.modifiers.alt {
        parts.push("Alt".into());
    }
    if chord.modifiers.shift {
        parts.push("Shift".into());
    }
    if chord.modifiers.platform {
        parts.push("Cmd".into());
    }

    parts.push(SharedString::from(display_key(&chord.key)));
    parts
}

/// The keys the effective keymap gives `command` in `context` (or a context
/// it inherits from) as one keycap label, `None` when the command has no
/// shortcut there.
pub fn shortcut_label(context: ContextId, command: Command) -> Option<SharedString> {
    effective_keymap()
        .keys_for_command(context, command)
        .map(key_sequence_label)
}

/// The display labels of each chord of `keys`, one group per chord.
pub fn key_sequence_display_parts(keys: &KeySequence) -> Vec<Vec<SharedString>> {
    keys.chords().iter().map(chord_display_parts).collect()
}

/// A key sequence as one line of text (`Ctrl K  Ctrl S`), for labels that
/// are not keycaps: the keys of each chord joined by spaces, chords by two.
pub fn key_sequence_label(keys: &KeySequence) -> SharedString {
    key_sequence_display_parts(keys)
        .into_iter()
        .map(|parts| {
            parts
                .iter()
                .map(|part| part.as_ref())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("  ")
        .into()
}

fn display_key(key: &str) -> String {
    match key {
        "down" => "↓".to_string(),
        "up" => "↑".to_string(),
        "left" => "←".to_string(),
        "right" => "→".to_string(),
        "enter" => "Enter".to_string(),
        "escape" => "Esc".to_string(),
        "backspace" => "⌫".to_string(),
        "delete" => "Del".to_string(),
        "tab" => "Tab".to_string(),
        "space" => "Space".to_string(),
        "home" => "Home".to_string(),
        "end" => "End".to_string(),
        "pageup" => "PgUp".to_string(),
        "pagedown" => "PgDn".to_string(),
        _ => key.to_uppercase(),
    }
}

/// Formats a chord in GPUI keystroke syntax, for example `ctrl-shift-p`.
fn gpui_keystroke(chord: &KeyChord) -> String {
    let modifiers = &chord.modifiers;
    let mut parts: Vec<&str> = Vec::new();

    if modifiers.platform {
        parts.push("cmd");
    }
    if modifiers.ctrl {
        parts.push("ctrl");
    }
    if modifiers.alt {
        parts.push("alt");
    }
    if modifiers.shift {
        parts.push("shift");
    }
    parts.push(&chord.key);

    parts.join("-")
}

/// Formats a key sequence in GPUI keystroke syntax, chords separated by a
/// space (`ctrl-k ctrl-s`).
pub fn gpui_keystrokes(keys: &KeySequence) -> String {
    keys.chords()
        .iter()
        .map(gpui_keystroke)
        .collect::<Vec<_>>()
        .join(" ")
}

// ============================================================================
// Default keymap
// ============================================================================

/// Returns a reference to the default [`KeymapStack`] with all default keybindings.
///
/// This is the keymap before user overrides. Code that shows a shortcut
/// reads [`effective_keymap`] instead.
pub fn default_keymap() -> &'static KeymapStack {
    &defaults::DEFAULT_KEYMAP
}

// ============================================================================
// Effective keymap (defaults + user overrides)
// ============================================================================

struct EffectiveKeymap {
    overrides: KeymapOverrides,
    keymap: Arc<KeymapStack>,
}

static EFFECTIVE_KEYMAP: LazyLock<RwLock<EffectiveKeymap>> = LazyLock::new(|| {
    RwLock::new(EffectiveKeymap {
        overrides: KeymapOverrides::new(),
        keymap: Arc::new(default_keymap().clone()),
    })
});

/// The keymap in force: the defaults with the user's overrides applied.
///
/// Every shortcut label reads this, so a change made in the settings shows
/// on the next render.
pub fn effective_keymap() -> Arc<KeymapStack> {
    EFFECTIVE_KEYMAP
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .keymap
        .clone()
}

/// The user overrides the effective keymap was built from.
pub fn keymap_overrides() -> KeymapOverrides {
    EFFECTIVE_KEYMAP
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .overrides
        .clone()
}

/// Rebuilds the effective keymap from `overrides` without touching the GPUI
/// bindings. [`apply_keymap_overrides`] is the entry point for the app.
fn install_keymap_overrides(overrides: KeymapOverrides) {
    let keymap = Arc::new(overrides.apply(default_keymap()));

    let mut effective = EFFECTIVE_KEYMAP
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    effective.overrides = overrides;
    effective.keymap = keymap;
}

/// Makes `overrides` the user's keymap customization: rebuilds the effective
/// keymap and regenerates every native GPUI binding derived from it, so the
/// change applies in every window without a restart.
pub fn apply_keymap_overrides(overrides: KeymapOverrides, cx: &mut App) {
    install_keymap_overrides(overrides);
    refresh_derived_keybindings(cx);
}

// ============================================================================
// Native GPUI bindings derived from the effective keymap
// ============================================================================

/// Marks the GPUI bindings generated from the keymap, so a refresh removes
/// exactly those and leaves every other registered binding (the input
/// component's editing keys, for example) alone.
const DERIVED_BINDING_META: KeyBindingMetaIndex = KeyBindingMetaIndex(0xDBF1);

#[derive(Default)]
struct DerivedKeybindingSources(Vec<fn() -> Vec<KeyBinding>>);

impl Global for DerivedKeybindingSources {}

/// Registers every keymap binding as a native GPUI binding, and starts
/// recording the last key pressed (see [`last_keystroke`]). Call once at
/// startup, after `gpui_component::init`: bindings added later win at the
/// same context depth, which lets the keymap take keys the input component
/// also binds (the primary modifier + Enter).
pub fn init_keymap(cx: &mut App) {
    register_derived_keybindings(keymap_keybindings, cx);

    cx.set_global(dbflux_components::actions::ShortcutLabels(Box::new(
        |context, command| {
            let context = ContextId::all_variants()
                .iter()
                .copied()
                .find(|candidate| candidate.id() == context)?;
            let command = Command::from_action_id(command)?;
            shortcut_label(context, command)
        },
    )));

    let recorder = cx.intercept_keystrokes(|event, _window, cx| {
        cx.default_global::<LastKeystroke>().0 = Some(event.keystroke.clone());
    });
    cx.default_global::<LastKeystroke>().1 = Some(recorder);
}

/// The last key pressed in any window, and the interceptor that records it.
#[derive(Default)]
struct LastKeystroke(Option<Keystroke>, Option<gpui::Subscription>);

impl Global for LastKeystroke {}

/// The key whose binding is being dispatched: interceptors run before key
/// bindings, so while a binding's action runs this is the keystroke that
/// matched it (the last one of a key sequence).
pub fn last_keystroke(cx: &App) -> Option<Keystroke> {
    cx.try_global::<LastKeystroke>()
        .and_then(|recorded| recorded.0.clone())
}

/// Registers `source` as a producer of native GPUI bindings generated from
/// the effective keymap, and binds what it produces now. Every registered
/// producer runs again whenever the overrides change.
pub fn register_derived_keybindings(source: fn() -> Vec<KeyBinding>, cx: &mut App) {
    cx.default_global::<DerivedKeybindingSources>()
        .0
        .push(source);
    refresh_derived_keybindings(cx);
}

fn refresh_derived_keybindings(cx: &mut App) {
    let sources = cx
        .try_global::<DerivedKeybindingSources>()
        .map(|sources| sources.0.clone())
        .unwrap_or_default();

    let derived: Vec<KeyBinding> = sources
        .iter()
        .flat_map(|source| source())
        .map(|binding| binding.with_meta(DERIVED_BINDING_META))
        .collect();

    let keymap = cx.key_bindings();
    let retained: Vec<KeyBinding> = keymap
        .borrow()
        .bindings()
        .filter(|binding| binding.meta() != Some(DERIVED_BINDING_META))
        .cloned()
        .collect();

    {
        let mut keymap = keymap.borrow_mut();
        keymap.clear();
        keymap.add_bindings(retained);
        keymap.add_bindings(derived);
    }

    cx.refresh_windows();
}

/// Every binding of the effective keymap as a native GPUI binding, in the
/// registration order [`KeymapOverrides::effective_bindings`] defines.
///
/// A binding whose stored predicate does not parse is skipped with an
/// error in the log: predicates are validated before they are saved, so this
/// only happens to rows edited outside the app.
pub fn keymap_keybindings() -> Vec<KeyBinding> {
    keymap_overrides()
        .effective_bindings(default_keymap())
        .iter()
        .filter_map(native_binding)
        .collect()
}

fn native_binding(binding: &EffectiveBinding) -> Option<KeyBinding> {
    let predicate = match KeyBindingContextPredicate::parse(&binding.predicate) {
        Ok(predicate) => Rc::new(predicate),
        Err(error) => {
            log::error!(
                "Skipping keybinding of {:?}: invalid context predicate `{}`: {error}",
                binding.slot.command,
                binding.predicate
            );
            return None;
        }
    };

    let keystrokes = gpui_keystrokes(&binding.keys);
    let action = binding_action(&binding.slot, binding.is_user);

    match KeyBinding::load(
        &keystrokes,
        action,
        Some(predicate),
        false,
        None,
        &DummyKeyboardMapper,
    ) {
        Ok(binding) => Some(binding),
        Err(error) => {
            log::error!(
                "Skipping keybinding of {:?}: invalid keys `{keystrokes}`: {error}",
                binding.slot.command
            );
            None
        }
    }
}

/// The action a binding dispatches: the element's own action for the
/// contexts an element handles, [`RunCommand`] otherwise.
pub fn binding_action(slot: &BindingSlot, is_user: bool) -> Box<dyn Action> {
    if let Some(action) = element_action(slot.context, slot.command) {
        return action;
    }

    let command = slot.command.action_id();

    if is_user {
        Box::new(RunCommand::from_user_binding(command))
    } else {
        Box::new(RunCommand::new(command))
    }
}

/// The element action that performs `command` in the element context
/// `context`, or `None` when the command goes through [`RunCommand`].
fn element_action(context: ContextId, command: Command) -> Option<Box<dyn Action>> {
    match context {
        ContextId::DocumentTree => document_tree_action(command),
        ContextId::DataTable => data_table_action(command),
        ContextId::Input => input_action(command),
        ContextId::Modal => modal_action(command),
        ContextId::CellEditorModal | ContextId::DocumentPreviewModal => {
            modal_editor_action(command)
        }
        _ => None,
    }
}

fn document_tree_action(command: Command) -> Option<Box<dyn Action>> {
    use document_tree::actions;

    let action: Box<dyn Action> = match command {
        Command::SelectPrev => Box::new(actions::MoveUp),
        Command::SelectNext => Box::new(actions::MoveDown),
        Command::ColumnLeft => Box::new(actions::MoveLeft),
        Command::ColumnRight => Box::new(actions::MoveRight),
        Command::SelectFirst => Box::new(actions::MoveToTop),
        Command::SelectLast => Box::new(actions::MoveToBottom),
        Command::PageUp => Box::new(actions::PageUp),
        Command::PageDown => Box::new(actions::PageDown),
        Command::ExpandCollapse => Box::new(actions::ToggleExpand),
        Command::Execute => Box::new(actions::StartEdit),
        Command::PreviewDocument => Box::new(actions::OpenPreview),
        Command::Delete => Box::new(actions::DeleteDocument),
        Command::ToggleRawView => Box::new(actions::ToggleViewMode),
        Command::CycleDocumentView => Box::new(actions::CycleDataView),
        Command::FocusSearch => Box::new(actions::OpenSearch),
        Command::NextMatch => Box::new(actions::NextMatch),
        Command::PrevMatch => Box::new(actions::PrevMatch),
        Command::Cancel => Box::new(actions::CloseSearch),
        _ => return None,
    };

    Some(action)
}

fn data_table_action(command: Command) -> Option<Box<dyn Action>> {
    use data_table::actions;

    let action: Box<dyn Action> = match command {
        Command::SelectPrev => Box::new(actions::MoveUp),
        Command::SelectNext => Box::new(actions::MoveDown),
        Command::ColumnLeft => Box::new(actions::MoveLeft),
        Command::ColumnRight => Box::new(actions::MoveRight),
        Command::ExtendSelectPrev => Box::new(actions::SelectUp),
        Command::ExtendSelectNext => Box::new(actions::SelectDown),
        Command::ExtendSelectLeft => Box::new(actions::SelectLeft),
        Command::ExtendSelectRight => Box::new(actions::SelectRight),
        Command::MoveToRowStart => Box::new(actions::MoveToLineStart),
        Command::MoveToRowEnd => Box::new(actions::MoveToLineEnd),
        Command::SelectFirst => Box::new(actions::MoveToTop),
        Command::SelectLast => Box::new(actions::MoveToBottom),
        Command::ExtendSelectRowStart => Box::new(actions::SelectToLineStart),
        Command::ExtendSelectRowEnd => Box::new(actions::SelectToLineEnd),
        Command::ExtendSelectFirst => Box::new(actions::SelectToTop),
        Command::ExtendSelectLast => Box::new(actions::SelectToBottom),
        Command::SelectAll => Box::new(actions::SelectAll),
        Command::Cancel => Box::new(actions::ClearSelection),
        Command::ResultsCopyCell => Box::new(actions::Copy),
        Command::ResultsCopyRow => Box::new(actions::CopyRow),
        Command::Execute => Box::new(actions::StartEdit),
        Command::SaveRow => Box::new(actions::SaveRow),
        Command::ResultsDeleteRow => Box::new(actions::DeleteRow),
        Command::ResultsAddRow => Box::new(actions::AddRow),
        Command::ResultsDuplicateRow => Box::new(actions::DuplicateRow),
        Command::ResultsSetNull => Box::new(actions::SetNull),
        Command::Undo => Box::new(actions::Undo),
        Command::Redo => Box::new(actions::Redo),
        Command::ToggleColumnGroup => Box::new(actions::ToggleColumnGroup),
        Command::StepOut => Box::new(actions::StepOut),
        _ => return None,
    };

    Some(action)
}

/// Undo and redo stay the input component's own actions so a code editor
/// with Vim editing keeps routing them through its undo groups.
fn input_action(command: Command) -> Option<Box<dyn Action>> {
    let action: Box<dyn Action> = match command {
        Command::SelectNext => Box::new(InputMoveDown),
        Command::SelectPrev => Box::new(InputMoveUp),
        Command::TriggerCompletion => Box::new(TriggerCompletion),
        Command::Undo => Box::new(gpui_component::input::Undo),
        Command::Redo => Box::new(gpui_component::input::Redo),
        _ => return None,
    };

    Some(action)
}

fn modal_action(command: Command) -> Option<Box<dyn Action>> {
    let action: Box<dyn Action> = match command {
        Command::Cancel => Box::new(component_actions::Cancel),
        Command::Execute => Box::new(component_actions::Execute),
        Command::SelectPrev => Box::new(component_actions::ScrollUp),
        Command::SelectNext => Box::new(component_actions::ScrollDown),
        Command::PageUp => Box::new(component_actions::ScrollPageUp),
        Command::PageDown => Box::new(component_actions::ScrollPageDown),
        Command::SelectFirst => Box::new(component_actions::ScrollToTop),
        Command::SelectLast => Box::new(component_actions::ScrollToBottom),
        _ => return None,
    };

    Some(action)
}

fn modal_editor_action(command: Command) -> Option<Box<dyn Action>> {
    let action: Box<dyn Action> = match command {
        Command::Cancel => Box::new(component_actions::Cancel),
        Command::SaveQuery => Box::new(component_actions::SaveEdit),
        _ => return None,
    };

    Some(action)
}

/// The command a [`RunCommand`] names, or `None` (logged) when the id is
/// unknown.
pub fn run_command(action: &RunCommand) -> Option<Command> {
    let command = Command::from_action_id(&action.command);

    if command.is_none() {
        log::warn!("Ignoring unknown keymap command `{}`", action.command);
    }

    command
}

// ============================================================================
// Key contexts set by window roots
// ============================================================================

/// Identifier the workspace window root always carries.
pub const WORKSPACE_KEY_CONTEXT: &str = "Workspace";

/// Identifier the settings window root always carries.
pub const SETTINGS_WINDOW_KEY_CONTEXT: &str = "SettingsWindow";

/// Identifier the connection manager window root always carries.
pub const CONNECTION_MANAGER_WINDOW_KEY_CONTEXT: &str = "ConnectionManagerWindow";

/// Key under which the code editor reports its Vim mode.
pub const VIM_MODE_KEY: &str = "vim_mode";

/// Key under which the code editor reports its query language.
pub const LANGUAGE_KEY: &str = "language";

/// The key context of a window root whose keyboard belongs to `context`:
/// `root`, the context's identifier, `Global` when the context inherits the
/// global bindings, and the extra key=value `entries` (`vim_mode=normal`).
pub fn root_key_context(
    root: &'static str,
    context: ContextId,
    entries: &[(SharedString, SharedString)],
) -> KeyContext {
    let mut key_context = KeyContext::default();
    key_context.add(root);
    key_context.add(context.as_gpui_context());

    if inherits_global(context) {
        key_context.add(ContextId::Global.as_gpui_context());
    }

    for (key, value) in entries {
        key_context.set(key.clone(), value.clone());
    }

    key_context
}

fn inherits_global(context: ContextId) -> bool {
    let mut current = Some(context);

    while let Some(ancestor) = current {
        if ancestor == ContextId::Global {
            return true;
        }
        current = ancestor.parent();
    }

    false
}

// ============================================================================
// Predicate validation and overlap
// ============================================================================

/// Identifiers and keys the app sets on its key contexts. A predicate naming
/// anything else parses but never matches, which is worth a warning.
pub fn known_context_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = ContextId::all_variants()
        .iter()
        .map(ContextId::as_gpui_context)
        .collect();

    names.extend([
        WORKSPACE_KEY_CONTEXT,
        SETTINGS_WINDOW_KEY_CONTEXT,
        CONNECTION_MANAGER_WINDOW_KEY_CONTEXT,
        VIM_MODE_KEY,
        LANGUAGE_KEY,
        "section",
        "tab",
        "os",
    ]);
    names.extend(CONTAINER_KEY_CONTEXTS);
    names
}

/// Element contexts whose bindings dispatch [`RunCommand`] to the element,
/// which handles its own commands and propagates the others.
pub const RUN_COMMAND_ELEMENT_CONTEXTS: &[ContextId] = &[ContextId::KeyValue];

/// Key context of the code editor pane (see
/// [`dbflux_components::key_contexts`] for every container identifier).
pub const CODE_EDITOR_KEY_CONTEXT: &str = key_contexts::CODE_EDITOR;

/// Identifiers containers set on themselves so predicates can name the
/// panel focus is in (`CodeEditor > Input`). They never carry default
/// bindings.
pub const CONTAINER_KEY_CONTEXTS: &[&str] = key_contexts::ALL;

/// Why a predicate cannot be saved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateError(pub String);

/// Parses `text` as a context predicate. On success returns the identifiers
/// it names that the app never sets, which make the binding never match.
pub fn validate_predicate(text: &str) -> Result<Vec<String>, PredicateError> {
    let text = text.trim();

    if text.is_empty() {
        return Err(PredicateError("the predicate is empty".to_string()));
    }

    let predicate = KeyBindingContextPredicate::parse(text)
        .map_err(|error| PredicateError(error.to_string()))?;

    let known = known_context_names();
    let mut unknown = Vec::new();
    collect_unknown_names(&predicate, &known, &mut unknown);
    Ok(unknown)
}

fn collect_unknown_names(
    predicate: &KeyBindingContextPredicate,
    known: &[&str],
    unknown: &mut Vec<String>,
) {
    use KeyBindingContextPredicate as Predicate;

    let mut note = |name: &str| {
        if !known.contains(&name) && !unknown.iter().any(|seen| seen == name) {
            unknown.push(name.to_string());
        }
    };

    match predicate {
        Predicate::Identifier(name) => note(name),
        Predicate::Equal(key, _) | Predicate::NotEqual(key, _) => note(key),
        Predicate::Not(inner) => collect_unknown_names(inner, known, unknown),
        Predicate::Descendant(first, second)
        | Predicate::And(first, second)
        | Predicate::Or(first, second) => {
            collect_unknown_names(first, known, unknown);
            collect_unknown_names(second, known, unknown);
        }
    }
}

/// Whether a binding on `keys` in `context_predicate` sits on unmodified
/// printable keys where text is typed: in a text field, or in the code
/// editor outside Vim's Normal and Visual modes. Such a binding takes the
/// key before it reaches the field and breaks input method composition.
pub fn binds_typed_text(keys: &KeySequence, context_predicate: &str) -> bool {
    let printable = keys.first().key.chars().count() == 1
        && !keys.first().modifiers.ctrl
        && !keys.first().modifiers.alt
        && !keys.first().modifiers.platform;

    if !printable {
        return false;
    }

    let Ok(predicate) = KeyBindingContextPredicate::parse(context_predicate) else {
        return false;
    };

    text_entry_stacks()
        .iter()
        .any(|stack| predicate.depth_of(stack).is_some())
}

/// Key context stacks in which the focused element accepts typed text.
fn text_entry_stacks() -> Vec<Vec<KeyContext>> {
    let input = |root: KeyContext| vec![root, KeyContext::parse("Input").unwrap_or_default()];

    vec![
        input(root_key_context(
            WORKSPACE_KEY_CONTEXT,
            ContextId::TextInput,
            &[],
        )),
        input(root_key_context(
            WORKSPACE_KEY_CONTEXT,
            ContextId::Editor,
            &[],
        )),
        input(root_key_context(
            WORKSPACE_KEY_CONTEXT,
            ContextId::Editor,
            &[(VIM_MODE_KEY.into(), "insert".into())],
        )),
        input(root_key_context(
            WORKSPACE_KEY_CONTEXT,
            ContextId::Editor,
            &[(VIM_MODE_KEY.into(), "replace".into())],
        )),
        input(root_key_context(
            WORKSPACE_KEY_CONTEXT,
            ContextId::CommandPalette,
            &[],
        )),
    ]
}

/// Overlap of context predicates, evaluated with GPUI's predicate language
/// against the key context stacks the app produces: one per context a window
/// root can report (with each Vim mode for the editor), and one per element
/// context inside its usual host.
pub struct GpuiPredicateOverlap {
    stacks: Vec<Vec<KeyContext>>,
}

impl GpuiPredicateOverlap {
    pub fn new() -> Self {
        Self {
            stacks: overlap_stacks(),
        }
    }
}

impl Default for GpuiPredicateOverlap {
    fn default() -> Self {
        Self::new()
    }
}

impl PredicateOverlap for GpuiPredicateOverlap {
    fn overlaps(&self, first: &str, second: &str) -> bool {
        let (Ok(first), Ok(second)) = (
            KeyBindingContextPredicate::parse(first),
            KeyBindingContextPredicate::parse(second),
        ) else {
            return first == second;
        };

        self.stacks
            .iter()
            .any(|stack| first.depth_of(stack).is_some() && second.depth_of(stack).is_some())
    }
}

fn overlap_stacks() -> Vec<Vec<KeyContext>> {
    let element = |name: &str| KeyContext::parse(name).unwrap_or_default();
    let mut stacks = Vec::new();

    for context in ContextId::all_variants() {
        if context.is_element_context() {
            continue;
        }

        let root_name = match context {
            ContextId::Settings => SETTINGS_WINDOW_KEY_CONTEXT,
            ContextId::ConnectionManager | ContextId::FormNavigation => {
                CONNECTION_MANAGER_WINDOW_KEY_CONTEXT
            }
            _ => WORKSPACE_KEY_CONTEXT,
        };

        stacks.push(vec![root_key_context(root_name, *context, &[])]);
    }

    for mode in [
        "normal",
        "insert",
        "replace",
        "visual",
        "visual_line",
        "visual_block",
    ] {
        stacks.push(vec![
            root_key_context(
                WORKSPACE_KEY_CONTEXT,
                ContextId::Editor,
                &[(VIM_MODE_KEY.into(), mode.into())],
            ),
            element(CODE_EDITOR_KEY_CONTEXT),
            element("Input"),
        ]);
    }

    let results_root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Results, &[]);
    let text_root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);
    let editor_root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Editor, &[]);

    stacks.push(vec![results_root.clone(), element(data_table::CONTEXT)]);
    stacks.push(vec![results_root.clone(), element(document_tree::CONTEXT)]);
    stacks.push(vec![text_root.clone(), element("Input")]);
    stacks.push(vec![
        editor_root,
        element(CODE_EDITOR_KEY_CONTEXT),
        element("Input"),
    ]);
    stacks.push(vec![text_root.clone(), element("Modal")]);
    stacks.push(vec![text_root, element("Modal"), element("Input")]);
    stacks.push(vec![results_root.clone(), element("Modal CellEditorModal")]);
    stacks.push(vec![results_root, element("Modal DocumentPreviewModal")]);

    stacks
}
