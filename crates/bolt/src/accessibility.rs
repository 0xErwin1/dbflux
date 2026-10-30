//! Accessibility checks for rendered surfaces.
//!
//! Assistive technology and UI automation announce an element by the name its
//! accessibility node carries: the label set with `aria_label`, or, for a
//! naming role, the text the element draws (see [`accessible_name`]). An
//! element that carries neither is announced as a bare control, so a screen
//! reader cannot tell one icon-only button from another.
//!
//! [`unnamed_clickables`] lists the clickable elements of a rendered frame
//! that carry no name, [`assert_clickables_named`] fails a test with that
//! list, and [`FrameCapture`] keeps the latest frame of a window under test.
//! A surface test renders the surface and asserts the names of the controls
//! it owns, the way a keyboard-coverage check asserts their keyboard paths.

use gpui::{AccessibilityFrame, FrameObserver, Role};
use std::sync::{Arc, Mutex};

/// Whether a node with `role` takes its accessible name from the text it
/// draws.
///
/// The set mirrors what the UI automation bridge and assistive technology
/// read: a clickable box, row or tab is named by the text inside it, while a
/// container is not. Source of truth for the observable behavior: the bridge at
/// `vendor/gpui-mcp/crates/gpui-mcp/src/observer.rs`.
fn name_from_content(role: Role) -> bool {
    matches!(
        role,
        Role::Button
            | Role::DefaultButton
            | Role::CheckBox
            | Role::RadioButton
            | Role::Switch
            | Role::Link
            | Role::MenuItem
            | Role::MenuItemCheckBox
            | Role::MenuItemRadio
            | Role::ListBoxOption
            | Role::MenuListOption
            | Role::Tab
    )
}

/// The accessibility node `id` of `frame`, when the frame carries one.
///
/// Use it to read what a control announces beyond its name: its role, its
/// toggled or expanded state, the actions it supports.
pub fn accessibility_node<'a>(
    frame: &'a AccessibilityFrame,
    id: &str,
) -> Option<&'a gpui::accesskit::Node> {
    let (_, node) = frame.nodes().find(|(_, node)| node.id() == id)?;

    frame.accessibility_node(node)
}

/// The accessible name of the node `id` of `frame`, or `None` when the node
/// has none.
///
/// The name is the node's own label when it has one, otherwise the text its
/// naming role draws, trimmed. An empty or whitespace-only name counts as no
/// name.
pub fn accessible_name(frame: &AccessibilityFrame, id: &str) -> Option<String> {
    let (_, node) = frame.nodes().find(|(_, node)| node.id() == id)?;
    let accessible = frame.accessibility_node(node)?;

    node_name(accessible, node)
}

/// The name of one accessibility node, following the naming rule above.
fn node_name(accessible: &gpui::accesskit::Node, node: &gpui::FrameNode) -> Option<String> {
    if let Some(label) = accessible.label() {
        let label = label.trim();

        if !label.is_empty() {
            return Some(label.to_owned());
        }
    }

    if !name_from_content(accessible.role()) {
        return None;
    }

    let text = node.content_text().trim();

    (!text.is_empty()).then(|| text.to_owned())
}

/// Every clickable element of `frame` that carries no accessible name, as
/// `id (path)` lines, sorted.
///
/// A clickable element is one whose accessibility node supports
/// [`gpui::AccessibleAction::Click`], the same set the keyboard-coverage check
/// walks.
pub fn unnamed_clickables(frame: &AccessibilityFrame) -> Vec<String> {
    let mut unnamed: Vec<String> = frame
        .nodes()
        .filter_map(|(_, node)| {
            let accessible = frame.accessibility_node(node)?;

            if !accessible.supports_action(gpui::AccessibleAction::Click)
                || node_name(accessible, node).is_some()
            {
                return None;
            }

            Some(format!("{} ({})", node.id(), node.path()))
        })
        .collect();

    unnamed.sort();
    unnamed
}

/// Panics listing every clickable element of `frame` with no accessible name,
/// and returns the ids of the elements it checked, sorted.
///
/// A test of a surface whose controls all carry a name asserts on this; the
/// returned ids prove the surface it meant to check was drawn.
#[expect(
    clippy::panic,
    reason = "assert_clickables_named is a check helper: failing is its contract and the panic carries the whole list of unnamed elements"
)]
#[track_caller]
pub fn assert_clickables_named(frame: &AccessibilityFrame) -> Vec<String> {
    let unnamed = unnamed_clickables(frame);

    if unnamed.is_empty() {
        let mut checked: Vec<String> = frame
            .nodes()
            .filter_map(|(_, node)| {
                let accessible = frame.accessibility_node(node)?;

                accessible
                    .supports_action(gpui::AccessibleAction::Click)
                    .then(|| node.id().to_string())
            })
            .collect();
        checked.sort();

        return checked;
    }

    let mut message = format!(
        "{} clickable element(s) have no accessible name:",
        unnamed.len()
    );
    for element in unnamed {
        message.push_str("\n  - ");
        message.push_str(&element);
    }
    message.push_str(
        "\nname each one with `.aria_label(...)` (a `Button` with a label or a tooltip \
         names itself), or draw its text inside it",
    );

    panic!("{message}");
}

/// Keeps the latest accessibility frame of the window it observes.
#[derive(Default)]
pub struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

impl FrameObserver for FrameCapture {
    fn accessibility_updated(&self, frame: &AccessibilityFrame) {
        let mut latest = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *latest = Some(frame.clone());
    }
}

impl FrameCapture {
    /// Starts observing the frames of `window`, which draws one frame.
    pub fn observe(window: &mut gpui::VisualTestContext) -> Arc<Self> {
        let capture = Arc::new(Self::default());

        window.update(|window, _| {
            window.observe_frames(&capture);
            window.refresh();
        });
        window.run_until_parked();

        capture
    }

    /// Draws a new frame of `window` and returns it.
    #[expect(
        clippy::panic,
        reason = "frame() is a test-observation helper: asking for a frame before any frame was drawn is a broken test and must fail here"
    )]
    #[track_caller]
    pub fn frame(&self, window: &mut gpui::VisualTestContext) -> AccessibilityFrame {
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        match self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            Some(frame) => frame,
            None => panic!("the observed window has not drawn a frame"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{accessible_name, assert_clickables_named, unnamed_clickables};
    use gpui::{
        Context, InteractiveElement as _, IntoElement, ParentElement as _, Render,
        StatefulInteractiveElement as _, Styled as _, Window, div,
    };

    struct Surface;

    impl Render for Surface {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .child(
                    div()
                        .id("named")
                        .aria_label("Run query")
                        .on_click(|_, _, _| {}),
                )
                .child(
                    div()
                        .id("text-named")
                        .on_click(|_, _, _| {})
                        .child("Export"),
                )
                .child(div().id("unnamed").on_click(|_, _, _| {}))
                .child(
                    div()
                        .id("not-clickable")
                        .aria_label("A label without an action"),
                )
        }
    }

    fn frame(cx: &mut gpui::TestAppContext) -> gpui::AccessibilityFrame {
        let capture = std::sync::Arc::new(super::FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_view, window) = cx.add_window_view(move |window, _cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            Surface
        });
        window.run_until_parked();

        capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame")
    }

    #[gpui::test]
    fn a_clickable_element_is_named_by_its_label_or_by_its_text(cx: &mut gpui::TestAppContext) {
        let frame = frame(cx);

        assert_eq!(
            accessible_name(&frame, "named").as_deref(),
            Some("Run query")
        );
        assert_eq!(
            accessible_name(&frame, "text-named").as_deref(),
            Some("Export")
        );
        assert_eq!(accessible_name(&frame, "unnamed"), None);
        assert_eq!(accessible_name(&frame, "missing"), None);
    }

    #[gpui::test]
    fn only_the_clickable_element_without_a_name_is_reported(cx: &mut gpui::TestAppContext) {
        let frame = frame(cx);

        assert_eq!(
            unnamed_clickables(&frame).len(),
            1,
            "{:?}",
            unnamed_clickables(&frame)
        );
        assert!(
            unnamed_clickables(&frame)[0].starts_with("unnamed ("),
            "{:?}",
            unnamed_clickables(&frame)
        );
    }

    #[gpui::test]
    #[should_panic(expected = "clickable element(s) have no accessible name")]
    fn assert_clickables_named_names_the_unnamed_element(cx: &mut gpui::TestAppContext) {
        let frame = frame(cx);

        assert_clickables_named(&frame);
    }
}
