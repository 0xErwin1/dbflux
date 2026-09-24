use gpui::prelude::*;
use gpui::{
    App, ElementId, Entity, FontWeight, IntoElement, KeyBinding, SharedString, Window, actions,
};
use gpui_component::Sizable;

use crate::tokens::FontSizes;
use crate::typography::AppFonts;

pub use gpui_component::RopeExt;
pub use gpui_component::input::{
    CodeActionProvider, CompletionProvider, Enter as InputEnter, Escape as InputEscape,
    IndentInline as InputIndentInline, Input as GpuiInput, InputContentType, InputEvent,
    InputState, MoveDown as InputMoveDown, MoveUp as InputMoveUp,
    OutdentInline as InputOutdentInline, Position as InputPosition, Rope, Search as InputSearch,
};

actions!(
    dbflux_input,
    [
        /// Manually open the completion popover for the focused single-line
        /// input. Bound to `ctrl-space` by default.
        TriggerCompletion
    ]
);

/// Key context for `gpui-component`'s `InputState` element.
const INPUT_CONTEXT: &str = "Input";

/// Register DBFlux-specific keybindings that complement the defaults from
/// `gpui_component::init`. Call this once at app startup, after
/// `gpui_component::init`.
///
/// Adds vim-style `ctrl-j` / `ctrl-k` chords as aliases for `MoveDown` /
/// `MoveUp` inside any focused input. When a completion menu is open on an
/// `EditorState`, the engine routes these actions to the menu first, so the
/// chords navigate suggestions too.
pub fn register_input_overrides(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("ctrl-j", InputMoveDown, Some(INPUT_CONTEXT)),
        KeyBinding::new("ctrl-k", InputMoveUp, Some(INPUT_CONTEXT)),
        KeyBinding::new("ctrl-space", TriggerCompletion, Some(INPUT_CONTEXT)),
    ]);
}

/// Thin wrapper around `gpui_component::input::Input` that pre-applies
/// DBFlux design token defaults (height, size).
#[derive(IntoElement)]
pub struct Input {
    state: Entity<InputState>,
    id: Option<ElementId>,
    aria_label: Option<SharedString>,
    small: bool,
    placeholder: Option<gpui::SharedString>,
    disabled: bool,
    w_full: bool,
    appearance: bool,
    cleanable: bool,
    secret: bool,
}

impl Input {
    pub fn new(state: &Entity<InputState>) -> Self {
        Self {
            state: state.clone(),
            id: None,
            aria_label: None,
            small: false,
            placeholder: None,
            disabled: false,
            w_full: false,
            appearance: true,
            cleanable: false,
            secret: false,
        }
    }

    /// Sets the element id of the input frame.
    ///
    /// Without it the id is derived from the state entity, so it changes
    /// between runs. Give an input that UI automation must find a stable id,
    /// unique within its window.
    pub fn id(mut self, id: impl Into<ElementId>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Sets the accessible name of the input, normally the visible label of
    /// the field it edits. Without it the name falls back to the placeholder.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }

    pub fn small(mut self) -> Self {
        self.small = true;
        self
    }

    pub fn placeholder(mut self, text: impl Into<gpui::SharedString>) -> Self {
        self.placeholder = Some(text.into());
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn w_full(mut self) -> Self {
        self.w_full = true;
        self
    }

    pub fn appearance(mut self, appearance: bool) -> Self {
        self.appearance = appearance;
        self
    }

    pub fn cleanable(mut self, cleanable: bool) -> Self {
        self.cleanable = cleanable;
        self
    }

    /// Marks the input as holding a secret.
    ///
    /// The input gets the password content type, so its value never reaches
    /// the accessibility tree or rendered-frame observers (UI automation), even
    /// while a show-password toggle draws it in plain text. What is drawn on
    /// screen still follows the state's `masked` flag.
    pub fn secret(mut self, secret: bool) -> Self {
        self.secret = secret;
        self
    }
}

impl RenderOnce for Input {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let mut input = GpuiInput::new(&self.state)
            .appearance(self.appearance)
            .disabled(self.disabled)
            .font_family(AppFonts::BODY)
            .font_weight(FontWeight::MEDIUM)
            .text_size(if self.small {
                FontSizes::SM
            } else {
                FontSizes::BASE
            });

        if self.small {
            input = input.small();
        }

        if self.w_full {
            input = input.w_full();
        }

        if self.cleanable {
            input = input.cleanable(true);
        }

        if self.secret {
            input = input.content_type(InputContentType::Password);
        }

        if let Some(id) = self.id {
            input = input.id(id);
        }

        if let Some(label) = self.aria_label {
            input = input.aria_label(label);
        }

        input
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use gpui::{
        AccessibilityFrame, AppContext as _, Context, Focusable as _, FrameObserver, IntoElement,
        ParentElement as _, Render, Role, Styled as _, TestAppContext, VisualTestContext, Window,
        div,
    };

    use super::{Input, InputState};

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

    impl FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    /// Node id, role and accessible name of every node in a frame.
    type ObservedNode = (String, Role, Option<String>);

    struct Field {
        state: gpui::Entity<InputState>,
        named: bool,
    }

    impl Render for Field {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let input = Input::new(&self.state);
            let input = if self.named {
                input.id("cm-field-host").aria_label("Host")
            } else {
                input
            };

            div().size_full().child(input)
        }
    }

    /// Open a window holding one input and observe its frames.
    fn open_field(
        named: bool,
        cx: &mut TestAppContext,
    ) -> (
        Arc<FrameCapture>,
        gpui::Entity<Field>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (view, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            let state = cx.new(|cx| InputState::new(window, cx).placeholder("localhost"));
            Field { state, named }
        });
        visual.run_until_parked();

        (capture, view, visual)
    }

    /// Redraw the window so the next frame reflects the latest focus and state.
    fn settle_frame(visual: &mut VisualTestContext) {
        visual.update(|window, _| window.refresh());
        visual.run_until_parked();
    }

    fn render_field(named: bool, cx: &mut TestAppContext) -> Vec<ObservedNode> {
        let (capture, _view, _visual) = open_field(named, cx);

        let frame = capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame");

        frame
            .nodes()
            .map(|(_, node)| {
                let accessible = frame.accessibility_node(node);
                let role = accessible.map_or_else(|| node.fallback_role(), |node| node.role());
                let label = accessible
                    .and_then(|node| node.label())
                    .map(ToOwned::to_owned);
                (node.id().to_owned(), role, label)
            })
            .collect()
    }

    #[gpui::test]
    fn id_and_label_reach_the_rendered_input(cx: &mut TestAppContext) {
        let nodes = render_field(true, cx);

        let (_, role, label) = nodes
            .iter()
            .find(|(id, _, _)| id == "cm-field-host")
            .expect("the input is found by the id it was given");

        assert_eq!(*role, Role::TextInput);
        assert_eq!(label.as_deref(), Some("Host"));
    }

    #[gpui::test]
    fn input_without_id_keeps_the_entity_id_and_placeholder_name(cx: &mut TestAppContext) {
        let nodes = render_field(false, cx);

        let (id, _, _) = nodes
            .iter()
            .find(|(_, role, _)| *role == Role::TextInput)
            .expect("the input is rendered");

        assert!(id.starts_with("input-"), "unexpected default id {id}");
        assert!(
            nodes
                .iter()
                .any(|(_, _, label)| label.as_deref() == Some("localhost")),
            "the placeholder is no longer the fallback name: {nodes:?}"
        );
    }

    #[gpui::test]
    fn input_focused_by_id_has_no_text_input_handler(cx: &mut TestAppContext) {
        let (_capture, view, visual) = open_field(true, cx);

        let focused =
            visual.update(|window, cx| window.focus_observed_element("cm-field-host", cx));
        assert!(focused, "the input frame is focusable by its id");
        settle_frame(visual);

        let replaced = visual.update(|window, cx| window.replace_input_text("db.example", cx));
        assert!(
            !replaced,
            "the input frame's focus handle unexpectedly owns a text input handler"
        );

        let state = visual.update(|_, cx| view.read(cx).state.clone());
        visual.update(|window, cx| {
            let handle = state.focus_handle(cx);
            window.focus(&handle, cx);
        });
        settle_frame(visual);

        let replaced = visual.update(|window, cx| window.replace_input_text("db.example", cx));
        assert!(
            replaced,
            "the editor's own focus handle owns the input handler"
        );
        assert_eq!(
            visual.update(|_, cx| state.read(cx).value()).as_ref(),
            "db.example"
        );
    }

    #[gpui::test]
    fn input_value_is_set_by_id_through_the_accessibility_action(cx: &mut TestAppContext) {
        let (_capture, view, visual) = open_field(true, cx);

        let applied = visual.update(|window, cx| {
            window.set_observed_element_value("cm-field-host", "db.example", cx)
        });
        assert!(applied, "the input frame handles the SetValue action");
        settle_frame(visual);

        let state = visual.update(|_, cx| view.read(cx).state.clone());
        assert_eq!(
            visual.update(|_, cx| state.read(cx).value()).as_ref(),
            "db.example"
        );

        let applied =
            visual.update(|window, cx| window.set_observed_element_value("missing", "value", cx));
        assert!(!applied, "an unknown id has no SetValue listener");
    }
}
