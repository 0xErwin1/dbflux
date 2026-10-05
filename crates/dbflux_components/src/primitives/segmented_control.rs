//! `SegmentedControl` — a horizontal row of mutually-exclusive option segments.
//!
//! A chamfered track (ground fill, 1 px line, 2 px padding) holding 26 px
//! segments. The active segment is a raised thumb with a 4 px cut; each
//! segment shows an optional icon before its label.

use std::sync::Arc;

use gpui::prelude::*;
use gpui::{AccessibleAction, App, Role, SharedString, Toggled, Window, div};
use gpui_component::ActiveTheme;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon, focus_underline};
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes};
use crate::typography::AppFonts;

/// A single option within a `SegmentedControl`.
#[derive(Debug, Clone)]
pub struct SegmentedItem {
    pub id: SharedString,
    pub label: SharedString,
    pub icon: Option<AppIcon>,
}

impl SegmentedItem {
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon: None,
        }
    }

    /// Draws `icon` before the label.
    pub fn icon(mut self, icon: AppIcon) -> Self {
        self.icon = Some(icon);
        self
    }
}

/// Selects the next active id given the current items, the current active id,
/// and the id of the segment that was clicked.
///
/// This is a pure helper so it can be unit-tested without a GPUI context.
pub fn new_active_id(items: &[SegmentedItem], _current: &str, clicked: &str) -> SharedString {
    items
        .iter()
        .find(|item| item.id.as_ref() == clicked)
        .map(|item| item.id.clone())
        .unwrap_or_else(|| SharedString::from(clicked.to_string()))
}

/// The segment `step` places away from `active` in `ids` (negative steps go
/// left), stopping at either end. `None` when `active` is not one of `ids`
/// or the step does not move off it.
///
/// A segmented field that is a single stop in a form moves its choice with
/// Left and Right through this, the way a radio group does.
pub fn stepped_segment<'a>(ids: &[&'a str], active: &str, step: isize) -> Option<&'a str> {
    let current = ids.iter().position(|id| *id == active)?;
    let last = ids.len().checked_sub(1)?;
    let target = current.saturating_add_signed(step).min(last);

    (target != current).then(|| ids[target])
}

/// A horizontal row of mutually-exclusive segments.
///
/// - Track: `ChamferCut::CONTROL`, `theme.background` fill, 1 px `theme.border`
///   line, `Fields::SEGMENT_TRACK_PADDING` inside.
/// - Segment: `Fields::SEGMENT_HEIGHT` tall, icon and label 6 px apart,
///   `FontSizes::XS` in the interface face.
/// - Active segment: raised thumb (`theme.secondary`, `ChamferCut::KEYCAP`),
///   strong text.
/// - Inactive segment: muted text; hover lays the hover wash under it.
/// - Keyboard focus: the track never rings. While focus is visible, the
///   focused segment carries a 2 px tint underline inside it; selection is
///   shown only by the raised thumb.
/// - Semantics: a control named with [`Self::group`] is a radio group
///   `segmented-<group>` whose segments are radio buttons
///   `segmented-<group>-<id>` (element id and accessibility id), named by
///   their label and toggled when active, so automation and assistive
///   technology can target one option of one control. An unnamed control
///   stays out of the accessibility tree: its segment ids repeat across
///   controls, and duplicate accessible nodes are dropped.
#[derive(IntoElement)]
pub struct SegmentedControl {
    items: Vec<SegmentedItem>,
    active_id: SharedString,
    focused: bool,
    focused_id: Option<SharedString>,
    group: Option<SharedString>,
    on_select: Arc<dyn Fn(&SharedString, &mut Window, &mut App)>,
}

/// Element and accessibility id of the track of the control named `group`.
pub fn segmented_group_id(group: &str) -> SharedString {
    SharedString::from(format!("segmented-{group}"))
}

/// Element and accessibility id of the segment `item` of the control named
/// `group`, or a group-less id when the control has no name.
pub fn segmented_item_id(group: Option<&str>, item: &str) -> SharedString {
    match group {
        Some(group) => SharedString::from(format!("segmented-{group}-{item}")),
        None => SharedString::from(format!("seg-ctl-item-{item}")),
    }
}

impl SegmentedControl {
    pub fn new(
        items: Vec<SegmentedItem>,
        active_id: impl Into<SharedString>,
        on_select: impl Fn(&SharedString, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            items,
            active_id: active_id.into(),
            focused: false,
            focused_id: None,
            group: None,
            on_select: Arc::new(on_select),
        }
    }

    /// Marks the control as holding the keyboard cursor. The focused segment
    /// is the one set with [`Self::focused_item`], or the active one.
    pub fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    /// The segment the keyboard cursor is on, for owners that move it with
    /// the arrow keys independently of the selection.
    pub fn focused_item(mut self, id: impl Into<SharedString>) -> Self {
        self.focused_id = Some(id.into());
        self
    }

    /// Names the control, which gives it and each of its segments a stable
    /// id (see [`segmented_group_id`] and [`segmented_item_id`]). Use a short
    /// kebab-case name that is unique in the window, such as `theme`.
    pub fn group(mut self, group: impl Into<SharedString>) -> Self {
        self.group = Some(group.into());
        self
    }
}

impl RenderOnce for SegmentedControl {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        if self.items.is_empty() {
            return div().into_any_element();
        }

        let theme = cx.theme().clone();
        let tint = ChromeColors::tint(&theme);
        let active_id = self.active_id.clone();
        let focused_id = self
            .focused
            .then(|| self.focused_id.clone().unwrap_or_else(|| active_id.clone()));
        let on_select = self.on_select;
        let group = self.group;
        let is_named = group.is_some();

        let segments = self.items.into_iter().map(|item| {
            let is_active = item.id == active_id;
            let is_focused = focused_id.as_ref() == Some(&item.id);
            let segment_id = segmented_item_id(group.as_deref(), item.id.as_ref());

            let thumb = if is_active {
                Chamfer::new(ChamferCut::KEYCAP).fill(theme.secondary)
            } else {
                Chamfer::new(ChamferCut::KEYCAP)
                    .fill_hover(theme.accent)
                    .interactive(SharedString::from(format!("{segment_id}-thumb")))
            };

            let text_color = if is_active {
                theme.accent_foreground
            } else {
                theme.muted_foreground
            };

            let clicked_id = item.id.clone();
            let on_select = on_select.clone();
            let accessible_id = item.id.clone();
            let on_accessible_select = on_select.clone();
            let accessible_label = item.label.clone();

            div()
                .id(segment_id.clone())
                .when(is_named, |segment| {
                    segment
                        .role(Role::RadioButton)
                        .accessibility_id(segment_id)
                        .aria_label(accessible_label)
                        .aria_toggled(if is_active {
                            Toggled::True
                        } else {
                            Toggled::False
                        })
                        .on_a11y_action(AccessibleAction::Click, move |_, window, cx| {
                            on_accessible_select(&accessible_id, window, cx);
                        })
                })
                .relative()
                .flex()
                .items_center()
                .justify_center()
                .gap(Fields::SEGMENT_GAP)
                .h(Fields::SEGMENT_HEIGHT)
                .px(Fields::SEGMENT_PADDING_X)
                .cursor_pointer()
                .text_color(text_color)
                .child(thumb)
                .when_some(item.icon, |this, icon| {
                    this.child(Icon::new(icon).size(Fields::SEGMENT_ICON).color(text_color))
                })
                .child(item.label)
                .when(is_focused, |this| this.child(focus_underline(tint)))
                .on_click(move |_, window, cx| {
                    on_select(&clicked_id, window, cx);
                })
                .into_any_element()
        });

        let track_id = group
            .as_deref()
            .map(segmented_group_id)
            .unwrap_or_else(|| SharedString::from("seg-ctl-track"));

        div()
            .id(track_id.clone())
            .when(is_named, |track| {
                track.role(Role::RadioGroup).accessibility_id(track_id)
            })
            .relative()
            .flex()
            .flex_none()
            .items_center()
            .p(Fields::SEGMENT_TRACK_PADDING)
            .font_family(AppFonts::INTERFACE)
            .text_size(FontSizes::XS)
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .children(segments)
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{FOCUS_MARKER_SELECTOR, FOCUS_RING_SELECTOR};
    use crate::tokens::BASE_REM;
    use gpui::{
        Context, Entity, FocusHandle, Modifiers, Render, TestAppContext, VisualTestContext, point,
        px,
    };

    struct SegmentsHost {
        focus_handle: FocusHandle,
        active: SharedString,
        focused_item: Option<SharedString>,
    }

    impl Render for SegmentsHost {
        fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
            let entity = cx.entity();
            let control = SegmentedControl::new(items(), self.active.clone(), move |id, _, cx| {
                let id = id.clone();
                entity.update(cx, |host, cx| {
                    host.active = id;
                    cx.notify();
                });
            })
            .focused(true);

            let control = match self.focused_item.clone() {
                Some(id) => control.focused_item(id),
                None => control,
            };

            div().track_focus(&self.focus_handle).size_full().child(
                div()
                    .flex()
                    .debug_selector(|| "segments".to_string())
                    .child(control),
            )
        }
    }

    fn open_segments<'a>(
        focused_item: Option<&str>,
        cx: &'a mut TestAppContext,
    ) -> (Entity<SegmentsHost>, &'a mut VisualTestContext) {
        cx.update(crate::theme::init);

        let focused_item = focused_item.map(|id| SharedString::from(id.to_string()));
        let (host, window) = cx.add_window_view(move |_, cx| SegmentsHost {
            focus_handle: cx.focus_handle(),
            active: SharedString::from("prefer"),
            focused_item,
        });
        window.update(|window, cx| host.read(cx).focus_handle.clone().focus(window, cx));
        window.run_until_parked();

        (host, window)
    }

    #[gpui::test]
    fn focused_segment_is_underlined_after_keys_only(cx: &mut TestAppContext) {
        let (host, window) = open_segments(None, cx);

        window.simulate_keystrokes("right");
        assert!(window.debug_bounds(FOCUS_MARKER_SELECTOR).is_some());
        assert!(
            window.debug_bounds(FOCUS_RING_SELECTOR).is_none(),
            "the track never draws a ring"
        );

        let track = window
            .debug_bounds("segments")
            .expect("the control is laid out");
        let first_segment = point(track.left() + px(10.0), track.center().y);
        window.simulate_click(first_segment, Modifiers::default());
        window.run_until_parked();

        assert_eq!(
            window.update(|_, cx| host.read(cx).active.clone()).as_ref(),
            "disable"
        );
        assert!(
            window.debug_bounds(FOCUS_MARKER_SELECTOR).is_none(),
            "a click leaves no focus marker"
        );
        assert!(window.debug_bounds(FOCUS_RING_SELECTOR).is_none());

        window.simulate_keystrokes("tab");
        assert!(window.debug_bounds(FOCUS_MARKER_SELECTOR).is_some());
        assert!(window.debug_bounds(FOCUS_RING_SELECTOR).is_none());
    }

    #[gpui::test]
    fn control_is_as_tall_as_a_field(cx: &mut TestAppContext) {
        assert_eq!(
            Fields::SEGMENT_HEIGHT + Fields::SEGMENT_TRACK_PADDING * 2.0,
            Fields::HEIGHT
        );

        let (_host, window) = open_segments(None, cx);
        let track = window
            .debug_bounds("segments")
            .expect("the control is laid out");

        assert_eq!(
            track.size.height,
            Fields::HEIGHT.to_pixels(gpui::px(BASE_REM))
        );
    }

    #[gpui::test]
    fn marker_follows_the_roving_item_not_the_selection(cx: &mut TestAppContext) {
        let (_host, window) = open_segments(None, cx);
        window.simulate_keystrokes("right");
        let on_active = window
            .debug_bounds(FOCUS_MARKER_SELECTOR)
            .expect("the active segment is marked");

        let (_host, window) = open_segments(Some("require"), cx);
        window.simulate_keystrokes("right");
        let on_roving = window
            .debug_bounds(FOCUS_MARKER_SELECTOR)
            .expect("the roving segment is marked");

        assert!(
            on_roving.left() > on_active.left(),
            "the marker sits on `require`, right of the active `prefer`"
        );
    }

    #[derive(Default)]
    struct FrameCapture(std::sync::Mutex<Option<gpui::AccessibilityFrame>>);

    impl gpui::FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    struct NamedSegments;

    impl Render for NamedSegments {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(
                SegmentedControl::new(
                    vec![
                        SegmentedItem::new("dark", "Dark"),
                        SegmentedItem::new("light", "Light"),
                    ],
                    "dark",
                    |_, _, _| {},
                )
                .group("theme"),
            )
        }
    }

    #[gpui::test]
    fn named_segments_are_radio_buttons_with_stable_ids(cx: &mut TestAppContext) {
        cx.update(crate::theme::init);

        let capture = std::sync::Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_view, window) = cx.add_window_view(move |window, _cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            NamedSegments
        });
        window.run_until_parked();

        let frame = capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame");

        let node_named = |id: &str| {
            let (_, node) = frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .unwrap_or_else(|| panic!("{id} is in the frame"));
            frame
                .accessibility_node(node)
                .unwrap_or_else(|| panic!("{id} is an accessible node"))
                .clone()
        };

        assert_eq!(node_named("segmented-theme").role(), Role::RadioGroup);

        let dark = node_named("segmented-theme-dark");
        assert_eq!(dark.role(), Role::RadioButton);
        assert_eq!(dark.label(), Some("Dark"));
        assert_eq!(dark.toggled(), Some(Toggled::True));
        assert!(dark.supports_action(AccessibleAction::Click));

        let light = node_named("segmented-theme-light");
        assert_eq!(light.toggled(), Some(Toggled::False));
    }

    #[test]
    fn item_ids_are_scoped_by_group() {
        assert_eq!(
            segmented_item_id(Some("density"), "compact").as_ref(),
            "segmented-density-compact"
        );
        assert_eq!(segmented_group_id("density").as_ref(), "segmented-density");
        assert_eq!(
            segmented_item_id(None, "compact").as_ref(),
            "seg-ctl-item-compact"
        );
    }

    fn items() -> Vec<SegmentedItem> {
        vec![
            SegmentedItem::new("disable", "disable"),
            SegmentedItem::new("allow", "allow"),
            SegmentedItem::new("prefer", "prefer"),
            SegmentedItem::new("require", "require"),
        ]
    }

    #[test]
    fn clicking_another_item_returns_its_id() {
        let result = new_active_id(&items(), "prefer", "require");
        assert_eq!(result.as_ref(), "require");
    }

    #[test]
    fn clicking_current_active_returns_same_id() {
        let result = new_active_id(&items(), "prefer", "prefer");
        assert_eq!(result.as_ref(), "prefer");
    }

    #[test]
    fn clicking_first_item_returns_first_id() {
        let result = new_active_id(&items(), "require", "disable");
        assert_eq!(result.as_ref(), "disable");
    }

    #[test]
    fn empty_items_list_returns_clicked_id_without_panic() {
        let empty: Vec<SegmentedItem> = vec![];
        let result = new_active_id(&empty, "", "require");
        assert_eq!(result.as_ref(), "require");
    }

    #[test]
    fn active_id_not_in_items_returns_id_unchanged() {
        let result = new_active_id(&items(), "unknown", "allow");
        assert_eq!(result.as_ref(), "allow");
    }

    #[test]
    fn stepping_moves_within_the_ends() {
        let ids = ["blocking", "detached"];

        assert_eq!(
            super::stepped_segment(&ids, "blocking", 1),
            Some("detached")
        );
        assert_eq!(
            super::stepped_segment(&ids, "detached", -1),
            Some("blocking")
        );
        assert_eq!(super::stepped_segment(&ids, "detached", 1), None);
        assert_eq!(super::stepped_segment(&ids, "blocking", -1), None);
        assert_eq!(super::stepped_segment(&ids, "missing", 1), None);
    }
}
