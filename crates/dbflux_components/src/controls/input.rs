use std::panic::Location;

use gpui::prelude::*;
use gpui::{
    AnyElement, App, Bounds, DefiniteLength, ElementId, Entity, FontWeight, GlobalElementId,
    InspectorElementId, IntoElement, KeyBinding, LayoutId, Pixels, SharedString, StyleRefinement,
    Window, actions,
};
use gpui_component::Sizable;
use gpui_component::input::{Editor as GpuiEditor, EditorState};

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

/// A code editor that shows text the user cannot change.
///
/// It is `gpui_component`'s `Editor` with `readonly(true)`, which rejects typing and
/// pasting but keeps focus, selection and copy. The wrapper also reports the editor as
/// read-only to assistive technology and to UI automation, which then refuses to replace
/// its value; `Editor::readonly` alone does neither. Layout is exactly that of the
/// editor: the wrapper adds no layout node of its own.
pub struct ReadOnlyEditor {
    editor: GpuiEditor,
}

impl ReadOnlyEditor {
    pub fn new(state: &Entity<EditorState>) -> Self {
        Self {
            editor: GpuiEditor::new(state).readonly(true),
        }
    }

    pub fn appearance(mut self, appearance: bool) -> Self {
        self.editor = self.editor.appearance(appearance);
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.editor = self.editor.disabled(disabled);
        self
    }

    /// Sets the editor height, like `Editor::h`.
    pub fn h(mut self, height: impl Into<DefiniteLength>) -> Self {
        self.editor = self.editor.h(height);
        self
    }
}

impl Styled for ReadOnlyEditor {
    fn style(&mut self) -> &mut StyleRefinement {
        self.editor.style()
    }
}

impl IntoElement for ReadOnlyEditor {
    type Element = ReadOnlyAccessibility;

    fn into_element(self) -> Self::Element {
        ReadOnlyAccessibility {
            child: self.editor.into_any_element(),
        }
    }
}

/// Marks the first accessibility node its child builds as read-only.
///
/// It delegates layout, prepaint and paint to the child, so it has no bounds, id or
/// style of its own.
pub struct ReadOnlyAccessibility {
    child: AnyElement,
}

impl IntoElement for ReadOnlyAccessibility {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}

impl Element for ReadOnlyAccessibility {
    type RequestLayoutState = ();
    type PrepaintState = ();

    fn id(&self) -> Option<ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        (self.child.request_layout(window, cx), ())
    }

    fn prepaint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        _bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        window: &mut Window,
        cx: &mut App,
    ) -> Self::PrepaintState {
        window.with_accessibility_read_only(|window| {
            self.child.prepaint(window, cx);
        });
    }

    fn paint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        _bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        _prepaint: &mut Self::PrepaintState,
        window: &mut Window,
        cx: &mut App,
    ) {
        self.child.paint(window, cx);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use gpui::{
        AccessibilityFrame, AppContext as _, Bounds, Context, Focusable as _, FrameObserver,
        IntoElement, Modifiers, ParentElement as _, Pixels, Point, Render, Role, Styled as _,
        TestAppContext, VisualTestContext, Window, div, point, prelude::FluentBuilder as _, px,
    };

    use super::{EditorState, GpuiEditor, GpuiInput, Input, InputState, ReadOnlyEditor};
    use crate::tokens::Heights;

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

        let has_handler = visual.update(|window, _| window.has_input_handler());
        assert!(!has_handler, "no input handler is installed for the frame");

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

    #[gpui::test]
    fn clicking_the_input_center_gives_its_editor_the_input_handler(cx: &mut TestAppContext) {
        let (capture, view, visual) = open_field(true, cx);

        let center = capture
            .0
            .lock()
            .expect("frame capture lock")
            .as_ref()
            .and_then(|frame| {
                frame
                    .nodes()
                    .find(|(_, node)| node.id() == "cm-field-host")
                    .map(|(_, node)| node.bounds().center())
            })
            .expect("the input is found by the id it was given");

        visual.simulate_click(center, Modifiers::none());
        settle_frame(visual);

        let inserted = visual.update(|window, cx| window.insert_input_text("db.example", cx));
        assert!(inserted, "a click at the input center focuses its editor");

        let state = visual.update(|_, cx| view.read(cx).state.clone());
        assert_eq!(
            visual.update(|_, cx| state.read(cx).value()).as_ref(),
            "db.example"
        );
    }

    /// Where the automation server clicks a text input to focus its editor: vertically
    /// centered, a third of the width from the left edge and at most 40 px from it.
    fn text_area_point(bounds: Bounds<Pixels>) -> Point<Pixels> {
        point(
            bounds.origin.x + (bounds.size.width / 3.0).min(px(40.0)),
            bounds.center().y,
        )
    }

    /// An input with a trailing clear button, and optionally a leading icon.
    struct DecoratedField {
        state: gpui::Entity<InputState>,
        width: Pixels,
        leading_icon: bool,
    }

    impl Render for DecoratedField {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let input = GpuiInput::new(&self.state)
                .id("decorated-field")
                .cleanable(true)
                .when(self.leading_icon, |input| {
                    input.prefix(div().size(Heights::ICON_SM))
                });

            div().size_full().child(div().w(self.width).child(input))
        }
    }

    /// Open a window holding one decorated input with text, so its clear button is drawn,
    /// and return the point the automation server clicks.
    fn open_decorated_field(
        width: Pixels,
        leading_icon: bool,
        cx: &mut TestAppContext,
    ) -> (
        gpui::Entity<DecoratedField>,
        Point<Pixels>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (view, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            let state = cx.new(|cx| InputState::new(window, cx).default_value("abc"));
            DecoratedField {
                state,
                width,
                leading_icon,
            }
        });
        visual.run_until_parked();

        let bounds = capture
            .0
            .lock()
            .expect("frame capture lock")
            .as_ref()
            .and_then(|frame| {
                frame
                    .nodes()
                    .find(|(_, node)| node.id() == "decorated-field")
                    .map(|(_, node)| node.bounds())
            })
            .expect("the input is found by the id it was given");

        (view, text_area_point(bounds), visual)
    }

    /// Click `target` and type after it, as `focus_element` then `type_text` do.
    fn click_and_type(
        view: &gpui::Entity<DecoratedField>,
        target: Point<Pixels>,
        visual: &mut VisualTestContext,
    ) -> (bool, String) {
        visual.simulate_click(target, Modifiers::none());
        settle_frame(visual);

        let inserted = visual.update(|window, cx| window.insert_input_text("!", cx));
        let state = visual.update(|_, cx| view.read(cx).state.clone());
        let value = visual.update(|_, cx| state.read(cx).value().to_string());
        (inserted, value)
    }

    #[gpui::test]
    fn the_text_area_of_a_narrow_input_with_a_clear_button_focuses_its_editor(
        cx: &mut TestAppContext,
    ) {
        // At this width the bounds center lands on the clear button.
        let (view, target, visual) = open_decorated_field(px(56.0), false, cx);

        let (inserted, value) = click_and_type(&view, target, visual);

        assert!(
            inserted,
            "the click reached the editor, not the clear button"
        );
        assert_eq!(value.len(), 4, "one character was typed into {value:?}");
    }

    #[gpui::test]
    fn the_text_area_of_an_input_with_a_leading_icon_focuses_its_editor(cx: &mut TestAppContext) {
        let (view, target, visual) = open_decorated_field(px(320.0), true, cx);

        let (inserted, value) = click_and_type(&view, target, visual);

        assert!(
            inserted,
            "the click reached the editor, not the leading icon"
        );
        assert_eq!(value.len(), 4, "one character was typed into {value:?}");
    }

    /// The builder chains the read-only editors of DBFlux's views use.
    #[derive(Clone, Copy, Debug)]
    enum ReadOnlyLayout {
        /// The audit viewer's event details.
        AuditDetails,
        /// The object browser's decoded preview.
        ObjectPreview,
        /// The query builder's SQL preview.
        QueryPreview,
    }

    const LAYOUTS: [ReadOnlyLayout; 3] = [
        ReadOnlyLayout::AuditDetails,
        ReadOnlyLayout::ObjectPreview,
        ReadOnlyLayout::QueryPreview,
    ];

    /// One code editor, either wrapped in `ReadOnlyEditor` or built as a plain
    /// `Editor::readonly(true)`, or as a plain editable `Editor`.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum EditorKind {
        Wrapped,
        PlainReadOnly,
        Editable,
    }

    struct EditorField {
        state: gpui::Entity<EditorState>,
        layout: ReadOnlyLayout,
        kind: EditorKind,
    }

    impl Render for EditorField {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            let state = &self.state;
            let editor = match (self.layout, self.kind) {
                (ReadOnlyLayout::AuditDetails, EditorKind::Wrapped) => ReadOnlyEditor::new(state)
                    .appearance(false)
                    .w_full()
                    .h(px(96.0))
                    .into_any_element(),
                (ReadOnlyLayout::AuditDetails, kind) => GpuiEditor::new(state)
                    .readonly(kind == EditorKind::PlainReadOnly)
                    .appearance(false)
                    .w_full()
                    .h(px(96.0))
                    .into_any_element(),
                (ReadOnlyLayout::ObjectPreview, EditorKind::Wrapped) => ReadOnlyEditor::new(state)
                    .appearance(false)
                    .disabled(false)
                    .w_full()
                    .h_full()
                    .into_any_element(),
                (ReadOnlyLayout::ObjectPreview, kind) => GpuiEditor::new(state)
                    .appearance(false)
                    .readonly(kind == EditorKind::PlainReadOnly)
                    .disabled(false)
                    .w_full()
                    .h_full()
                    .into_any_element(),
                (ReadOnlyLayout::QueryPreview, EditorKind::Wrapped) => ReadOnlyEditor::new(state)
                    .appearance(false)
                    .w_full()
                    .h(px(140.0))
                    .into_any_element(),
                (ReadOnlyLayout::QueryPreview, kind) => GpuiEditor::new(state)
                    .readonly(kind == EditorKind::PlainReadOnly)
                    .appearance(false)
                    .w_full()
                    .h(px(140.0))
                    .into_any_element(),
            };

            div().size_full().flex().flex_col().child(editor)
        }
    }

    /// What a test learns about the editor node of one rendered frame.
    struct ObservedEditor {
        id: String,
        bounds: Bounds<Pixels>,
        read_only: bool,
    }

    /// Open a window holding one code editor and observe its frames.
    fn open_editor(
        layout: ReadOnlyLayout,
        kind: EditorKind,
        cx: &mut TestAppContext,
    ) -> (
        Arc<FrameCapture>,
        gpui::Entity<EditorField>,
        &mut VisualTestContext,
    ) {
        cx.update(gpui_component::init);

        let capture = Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (view, visual) = cx.add_window_view(move |window, cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            let state = cx.new(|cx| EditorState::new(window, cx).default_value("SELECT 1"));
            EditorField {
                state,
                layout,
                kind,
            }
        });
        visual.run_until_parked();

        (capture, view, visual)
    }

    fn observed_editor(capture: &FrameCapture) -> ObservedEditor {
        let guard = capture.0.lock().expect("frame capture lock");
        let frame = guard.as_ref().expect("the window rendered a frame");
        frame
            .nodes()
            .find_map(|(_, node)| {
                let accessible = frame.accessibility_node(node)?;
                (accessible.role() == Role::MultilineTextInput).then(|| ObservedEditor {
                    id: node.id().to_owned(),
                    bounds: node.bounds(),
                    read_only: accessible.is_read_only(),
                })
            })
            .expect("the editor is rendered as a multi-line text input")
    }

    fn editor_value(view: &gpui::Entity<EditorField>, visual: &mut VisualTestContext) -> String {
        let state = visual.update(|_, cx| view.read(cx).state.clone());
        visual.update(|_, cx| state.read(cx).value().to_string())
    }

    #[gpui::test]
    fn read_only_editors_are_reported_read_only_and_refuse_a_new_value(cx: &mut TestAppContext) {
        for layout in LAYOUTS {
            let (capture, view, visual) = open_editor(layout, EditorKind::Wrapped, cx);
            let editor = observed_editor(&capture);
            assert!(editor.read_only, "{layout:?} is not reported read-only");

            let applied = visual.update(|window, cx| {
                window.set_observed_element_value(&editor.id, "DROP TABLE users", cx)
            });
            settle_frame(visual);

            assert!(!applied, "{layout:?} accepted a new value");
            assert_eq!(editor_value(&view, visual), "SELECT 1", "{layout:?}");
            assert!(
                observed_editor(&capture).read_only,
                "{layout:?} after a redraw"
            );
        }
    }

    #[gpui::test]
    fn read_only_editors_keep_the_layout_of_the_plain_editor(cx: &mut TestAppContext) {
        for layout in LAYOUTS {
            let (wrapped, _, _) = open_editor(layout, EditorKind::Wrapped, cx);
            let (plain, _, _) = open_editor(layout, EditorKind::PlainReadOnly, cx);

            let wrapped = observed_editor(&wrapped);
            let plain = observed_editor(&plain);

            assert!(!plain.read_only, "Editor::readonly alone reports nothing");
            assert_eq!(wrapped.bounds, plain.bounds, "{layout:?}");
            assert!(
                wrapped.bounds.size.height > px(0.0),
                "{layout:?} has no height"
            );
        }
    }

    #[gpui::test]
    fn an_editable_editor_still_accepts_a_new_value(cx: &mut TestAppContext) {
        let (capture, view, visual) =
            open_editor(ReadOnlyLayout::QueryPreview, EditorKind::Editable, cx);
        let editor = observed_editor(&capture);
        assert!(!editor.read_only);

        let applied = visual
            .update(|window, cx| window.set_observed_element_value(&editor.id, "SELECT 2", cx));
        settle_frame(visual);

        assert!(applied, "the editable editor handles the SetValue action");
        assert_eq!(editor_value(&view, visual), "SELECT 2");
    }
}
