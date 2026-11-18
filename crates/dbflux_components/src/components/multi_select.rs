//! The multi-select moved to `bolt::controls`; re-exported at this path so
//! the call sites keep compiling. What stays here is the product half: the
//! keyboard and pointer suites run against the product's keymap bindings and
//! theme, and the catalogue keys the trigger draws are asserted against the
//! product catalogue.

pub use bolt::controls::{MultiSelect, MultiSelectChanged};

#[cfg(test)]
mod tests {
    #[test]
    fn multi_select_keys_resolve_in_both_locales() {
        let keys = [
            "controls.multi_select.placeholder",
            "controls.multi_select.clear_all",
            "controls.multi_select.summary_all",
            "controls.multi_select.summary_count",
            "controls.multi_select.more.one",
            "controls.multi_select.more.many",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    /// Keyboard tests against a real window, with the `Dropdown` keys the
    /// app keymap binds (see `bind_dropdown_keys_for_tests`).
    mod keyboard {
        use super::super::{MultiSelect, MultiSelectChanged};
        use crate::controls::{DropdownItem, bind_dropdown_keys_for_tests};
        use gpui::{
            AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
            ParentElement as _, Render, SharedString, Styled as _, TestAppContext,
            VisualTestContext, Window, div,
        };

        struct Owner {
            focus: FocusHandle,
            multi_select: Entity<MultiSelect>,
            changes: Vec<Vec<SharedString>>,
            _subscription: gpui::Subscription,
        }

        impl Render for Owner {
            fn render(
                &mut self,
                _window: &mut Window,
                _cx: &mut Context<Self>,
            ) -> impl IntoElement {
                div()
                    .size_full()
                    .track_focus(&self.focus)
                    .child(self.multi_select.clone())
            }
        }

        fn setup(cx: &mut TestAppContext) -> (Entity<Owner>, &mut VisualTestContext) {
            cx.update(crate::theme::init);
            bind_dropdown_keys_for_tests(cx);

            let (owner, window) = cx.add_window_view(|window, cx| {
                let multi_select = cx.new(|cx| {
                    let mut multi_select = MultiSelect::new("keyboard-multi-select");
                    multi_select.set_items(
                        vec![
                            DropdownItem::new("One"),
                            DropdownItem::new("Two"),
                            DropdownItem::new("Three"),
                        ],
                        cx,
                    );
                    multi_select
                });

                let subscription = cx.subscribe_in(
                    &multi_select,
                    window,
                    |this: &mut Owner, _, event: &MultiSelectChanged, _window, _cx| {
                        this.changes.push(event.selected_values.clone());
                    },
                );

                Owner {
                    focus: cx.focus_handle(),
                    multi_select,
                    changes: Vec::new(),
                    _subscription: subscription,
                }
            });
            window.run_until_parked();

            window.update(|window, cx| {
                let focus = owner.read(cx).focus.clone();
                focus.focus(window, cx);

                let multi_select = owner.read(cx).multi_select.clone();
                multi_select.update(cx, |multi_select, cx| multi_select.focus(window, cx));
            });
            window.run_until_parked();

            (owner, window)
        }

        #[gpui::test]
        fn space_toggles_the_highlighted_item_and_escape_returns_focus(cx: &mut TestAppContext) {
            let (owner, window) = setup(cx);

            window.simulate_keystrokes("space");
            window.run_until_parked();
            let open = window.update(|_, cx| owner.read(cx).multi_select.read(cx).is_open());
            assert!(open, "Space opens the list");

            window.simulate_keystrokes("space j j space k k space");
            window.run_until_parked();

            let values =
                window.update(|_, cx| owner.read(cx).multi_select.read(cx).selected_values());
            assert_eq!(
                values,
                vec![SharedString::from("Three")],
                "One on, Three on, One off again"
            );

            window.simulate_keystrokes("escape");
            window.run_until_parked();

            let (open, owner_focused, changes) = window.update(|window, cx| {
                let owner = owner.read(cx);
                (
                    owner.multi_select.read(cx).is_open(),
                    owner.focus.is_focused(window),
                    owner.changes.len(),
                )
            });
            assert!(!open, "Escape closes the list");
            assert!(owner_focused, "Escape hands focus back");
            assert_eq!(changes, 3, "every toggle reports the new selection");
        }
    }

    /// Pointer tests that press and release the mouse on the rendered
    /// elements, located through the window's accessibility frame.
    mod pointer {
        use std::sync::{Arc, Mutex};

        use super::super::{MultiSelect, MultiSelectChanged};
        use crate::controls::DropdownItem;
        use gpui::{
            AccessibilityFrame, AppContext as _, Context, Entity, FrameObserver, IntoElement,
            Modifiers, MouseButton, ParentElement as _, Pixels, Point, Render, SharedString,
            Styled as _, TestAppContext, VisualTestContext, Window, div, point, px,
        };

        /// Keeps the latest rendered accessibility frame of the window it observes.
        #[derive(Default)]
        struct FrameCapture(Mutex<Option<AccessibilityFrame>>);

        impl FrameObserver for FrameCapture {
            fn accessibility_updated(&self, frame: &AccessibilityFrame) {
                *self.0.lock().expect("frame capture lock") = Some(frame.clone());
            }
        }

        struct Owner {
            multi_select: Entity<MultiSelect>,
            changes: Vec<Vec<SharedString>>,
            _subscription: gpui::Subscription,
        }

        impl Render for Owner {
            fn render(
                &mut self,
                _window: &mut Window,
                _cx: &mut Context<Self>,
            ) -> impl IntoElement {
                div()
                    .size_full()
                    .child(div().w(px(200.0)).child(self.multi_select.clone()))
            }
        }

        fn setup(
            cx: &mut TestAppContext,
        ) -> (Arc<FrameCapture>, Entity<Owner>, &mut VisualTestContext) {
            cx.update(crate::theme::init);
            cx.update(gpui_component::init);

            let capture = Arc::new(FrameCapture::default());
            let capture_for_window = capture.clone();
            let (owner, window) = cx.add_window_view(move |window, cx| {
                window.observe_frames(&capture_for_window);
                window.refresh();

                let multi_select = cx.new(|cx| {
                    let mut multi_select = MultiSelect::new("pointer-multi-select");
                    multi_select.set_items(
                        vec![
                            DropdownItem::new("One"),
                            DropdownItem::new("Two"),
                            DropdownItem::new("Three"),
                        ],
                        cx,
                    );
                    multi_select
                });

                let subscription = cx.subscribe_in(
                    &multi_select,
                    window,
                    |this: &mut Owner, _, event: &MultiSelectChanged, _window, _cx| {
                        this.changes.push(event.selected_values.clone());
                    },
                );

                Owner {
                    multi_select,
                    changes: Vec::new(),
                    _subscription: subscription,
                }
            });
            window.run_until_parked();

            (capture, owner, window)
        }

        /// Redraw the window so the next frame reflects the latest state.
        fn settle_frame(window: &mut VisualTestContext) {
            window.update(|window, _| window.refresh());
            window.run_until_parked();
        }

        fn center_of(capture: &FrameCapture, id: &str) -> Point<Pixels> {
            let guard = capture.0.lock().expect("frame capture lock");
            let frame = guard.as_ref().expect("the window rendered a frame");

            let found = frame
                .nodes()
                .find(|(_, node)| node.id() == id)
                .map(|(_, node)| node.bounds().center());

            found.unwrap_or_else(|| {
                let ids: Vec<&str> = frame.nodes().map(|(_, node)| node.id()).collect();
                panic!("element {id} is not rendered; frame holds {ids:?}")
            })
        }

        fn press_and_release(window: &mut VisualTestContext, position: Point<Pixels>) {
            window.simulate_mouse_down(position, MouseButton::Left, Modifiers::none());
            window.run_until_parked();
            window.simulate_mouse_up(position, MouseButton::Left, Modifiers::none());
            settle_frame(window);
        }

        fn is_open(owner: &Entity<Owner>, window: &mut VisualTestContext) -> bool {
            window.update(|_, cx| owner.read(cx).multi_select.read(cx).is_open())
        }

        fn open_menu(
            capture: &FrameCapture,
            owner: &Entity<Owner>,
            window: &mut VisualTestContext,
        ) {
            let trigger = center_of(capture, "ms-trigger");
            press_and_release(window, trigger);
            assert!(
                is_open(owner, window),
                "a click on the trigger opens the list"
            );
        }

        #[gpui::test]
        fn clicking_a_row_toggles_it_and_keeps_the_list_open(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let row = center_of(&capture, "ms-menu-rows.ms-item-1");
            press_and_release(window, row);

            let (values, changes) = window.update(|_, cx| {
                let owner = owner.read(cx);
                (
                    owner.multi_select.read(cx).selected_values(),
                    owner.changes.clone(),
                )
            });
            assert_eq!(values, vec![SharedString::from("Two")]);
            assert_eq!(changes, vec![vec![SharedString::from("Two")]]);
            assert!(
                is_open(&owner, window),
                "the list stays open after a toggle"
            );
        }

        #[gpui::test]
        fn pressing_outside_the_list_closes_it(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let viewport = window.update(|window, _| window.viewport_size());
            let outside = point(viewport.width * 0.95, viewport.height * 0.95);
            press_and_release(window, outside);

            assert!(!is_open(&owner, window), "a press outside closes the list");
        }

        #[gpui::test]
        fn clicking_the_trigger_while_open_closes_the_list(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let trigger = center_of(&capture, "ms-trigger");
            press_and_release(window, trigger);

            assert!(
                !is_open(&owner, window),
                "a second trigger click closes the list"
            );
        }

        /// The rows of the open list and the boxes they draw are named after
        /// their item, so a screen reader can tell them apart.
        #[gpui::test]
        fn every_control_of_the_open_list_is_named(cx: &mut TestAppContext) {
            let (capture, owner, window) = setup(cx);
            open_menu(&capture, &owner, window);

            let frame = capture
                .0
                .lock()
                .expect("frame capture lock")
                .clone()
                .expect("the window rendered a frame");
            let unnamed = crate::accessibility::unnamed_clickables(&frame);
            assert_eq!(unnamed, Vec::<String>::new());

            // A node keeps its element id only while that id names it alone,
            // so the box of an item may be filed under its path inside the row.
            let name_of = |suffix: &str| -> Option<String> {
                let id = frame
                    .nodes()
                    .map(|(_, node)| node.id().to_string())
                    .find(|id| id == suffix || id.ends_with(&format!(".{suffix}")))?;

                crate::accessibility::accessible_name(&frame, &id)
            };

            assert_eq!(name_of("ms-item-0").as_deref(), Some("One"));
            assert_eq!(name_of("ms-item-1").as_deref(), Some("Two"));
            assert_eq!(name_of("ms-item-2").as_deref(), Some("Three"));
        }
    }
}
