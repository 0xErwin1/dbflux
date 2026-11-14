//! The point inspector dock, adapted to the product's catalogue.
//!
//! The element itself lives in `bolt::chart::point_inspector`; it takes its
//! translated labels as a supplied [`PointInspectorText`] value, because it
//! translates at element-build time where no `App` is at hand. This wrapper
//! keeps the historical signature and answers the labels from `dbflux_i18n`
//! exactly as the inline code did before the move.

use gpui::{AnyElement, App, ClickEvent, Window};

use bolt::semantic::ChartColors;

pub use bolt::chart::point_inspector::{DataPointRef, PointInspectorText, SourceRowRef};

/// Build the PointInspector dock element.
///
/// The host owns the table the point came from, so it passes what
/// "Show in tree" does (`on_show_in_tree`, run on click).
///
/// # Arguments
/// * `source` — the source row reference for the hovered point.
/// * `row_values` — key-value pairs from the source row (column name, display value).
/// * `series_name` — label of the hovered series.
/// * `hovered_x` — formatted X value (timestamp or numeric).
/// * `hovered_y` — formatted Y value.
/// * `delta_prev` — optional formatted delta vs the previous decimated sample.
/// * `delta_avg` — optional formatted delta vs the window average.
/// * `colors` — semantic chart colors for the active theme.
/// * `on_show_in_tree` — runs when "Show in tree" is clicked.
#[allow(clippy::too_many_arguments)]
pub fn point_inspector_element(
    source: SourceRowRef,
    row_values: &[(String, String)],
    series_name: &str,
    hovered_x: &str,
    hovered_y: &str,
    delta_prev: Option<&str>,
    delta_avg: Option<&str>,
    colors: &ChartColors,
    on_show_in_tree: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
) -> AnyElement {
    let text = PointInspectorText {
        series: dbflux_i18n::t!("chart.point_inspector.series").into(),
        hovered_point: dbflux_i18n::t!("chart.point_inspector.hovered_point").into(),
        time: dbflux_i18n::t!("chart.point_inspector.time").into(),
        value: dbflux_i18n::t!("chart.point_inspector.value").into(),
        source_row: dbflux_i18n::t!("chart.point_inspector.source_row").into(),
        quick_actions: dbflux_i18n::t!("chart.point_inspector.quick_actions").into(),
        quick_action_label: dbflux_i18n::t!("chart.point_inspector.show_in_tree").into(),
        quick_action_slug: "show-in-tree",
    };

    bolt::chart::point_inspector::point_inspector_element(
        source,
        row_values,
        series_name,
        hovered_x,
        hovered_y,
        delta_prev,
        delta_avg,
        &text,
        colors,
        on_show_in_tree,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use gpui::{IntoElement, div, prelude::*};

    #[test]
    fn point_inspector_keys_resolve_in_both_locales() {
        let keys = [
            "chart.point_inspector.series",
            "chart.point_inspector.hovered_point",
            "chart.point_inspector.time",
            "chart.point_inspector.value",
            "chart.point_inspector.source_row",
            "chart.point_inspector.quick_actions",
            "chart.point_inspector.show_in_tree",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn source_row_label_is_translated_in_every_locale() {
        let key = "chart.point_inspector.source_row";
        let english = dbflux_i18n::t!(key, locale = "en");
        assert_eq!(english, "SOURCE ROW");

        for locale in ["es", "pt_BR", "ko", "zh_Hans"] {
            let value = dbflux_i18n::t!(key, locale = locale);

            assert!(
                !value.is_empty() && value != format!("{locale}.{key}") && value != english,
                "{key} is not translated in {locale}"
            );
        }
    }

    #[test]
    fn point_inspector_show_in_tree_differs_between_locales() {
        let en = dbflux_i18n::t!("chart.point_inspector.show_in_tree", locale = "en");
        let es = dbflux_i18n::t!("chart.point_inspector.show_in_tree", locale = "es");
        assert_ne!(en, es);
    }

    #[derive(Default)]
    struct InspectorHarness {
        /// Row indexes "Show in tree" was clicked for.
        shown: std::rc::Rc<std::cell::RefCell<Vec<usize>>>,
    }

    impl gpui::Render for InspectorHarness {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            let row_values = [("value".to_string(), "42".to_string())];

            div()
                .id("inspector-harness")
                .size_full()
                .child(point_inspector_element(
                    SourceRowRef { row_idx: 4 },
                    &row_values,
                    "cpu",
                    "12:00:00",
                    "42",
                    None,
                    None,
                    &ChartColors::dark(),
                    {
                        let shown = self.shown.clone();
                        move |_, _, _| shown.borrow_mut().push(4)
                    },
                ))
        }
    }

    /// Keeps the latest rendered accessibility frame of the window it observes.
    #[derive(Default)]
    struct FrameCapture(std::sync::Mutex<Option<gpui::AccessibilityFrame>>);

    impl gpui::FrameObserver for FrameCapture {
        fn accessibility_updated(&self, frame: &gpui::AccessibilityFrame) {
            *self.0.lock().expect("frame capture lock") = Some(frame.clone());
        }
    }

    /// The quick-actions row renders only the working "Show in tree" action:
    /// its rendered text carries no label of a placeholder button.
    #[gpui::test]
    fn inspector_quick_actions_render_only_show_in_tree(cx: &mut gpui::TestAppContext) {
        let capture = std::sync::Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let (_view, visual) = cx.add_window_view(move |window, _cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            InspectorHarness::default()
        });
        visual.run_until_parked();

        let frame = capture
            .0
            .lock()
            .expect("frame capture lock")
            .clone()
            .expect("the window rendered a frame");

        let quick_actions_text = frame
            .nodes()
            .map(|(_, node)| node)
            .find(|node| node.id() == "inspector-quick-actions")
            .map(|node| node.content_text().to_owned())
            .expect("the quick-actions row is rendered");

        assert_eq!(
            quick_actions_text,
            dbflux_i18n::t!("chart.point_inspector.show_in_tree")
        );
    }

    /// "Show in tree" runs on click (release), with its own handler, and is
    /// exposed as clickable.
    #[gpui::test]
    fn show_in_tree_runs_its_handler_on_click(cx: &mut gpui::TestAppContext) {
        let capture = std::sync::Arc::new(FrameCapture::default());
        let capture_for_window = capture.clone();
        let shown: std::rc::Rc<std::cell::RefCell<Vec<usize>>> = std::rc::Rc::default();
        let shown_for_window = shown.clone();
        let (_view, visual) = cx.add_window_view(move |window, _cx| {
            window.observe_frames(&capture_for_window);
            window.refresh();
            InspectorHarness {
                shown: shown_for_window,
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
                    .map(|(_, node)| node)
                    .find(|node| node.id() == "inspector-action-show-in-tree-row-4")
                    .map(|node| node.bounds())
            })
            .expect("the Show in tree button is rendered");

        visual.simulate_click(bounds.center(), gpui::Modifiers::default());
        visual.run_until_parked();

        assert_eq!(shown.borrow().as_slice(), [4]);
    }
}
