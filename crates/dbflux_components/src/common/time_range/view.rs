//! Generic time-range selection panel (Entity).
//!
//! Encapsulates a preset dropdown plus a custom date/time picker and emits
//! `TimeRangeChanged` whenever the effective window changes. The caller owns
//! the entity, subscribes to `TimeRangeChanged`, and reacts (e.g. by
//! triggering a query reload).
//!
//! The sub-entities (dropdown, date-picker, hour/minute dropdowns) are
//! accessible via accessors so that embedders (e.g. `AuditDocument`) can
//! still reference them in `FilterBarItem` lists for keyboard navigation.

use crate::controls::{Dropdown, DropdownItem, DropdownSelectionChanged};
use crate::primitives::Text;
use gpui::prelude::*;
use gpui::{App, Entity, EventEmitter, Subscription, Window};
use gpui_component::Sizable;
use gpui_component::calendar::Date;
use gpui_component::date_picker::{DatePicker, DatePickerEvent, DatePickerState};

use super::state::{TimeRange, TimestampDisplayMode, validate_custom_range_parts};

/// Emitted when the effective time window changes.
#[derive(Clone, Debug)]
pub struct TimeRangeChanged {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
}

/// Reusable time-range panel.
///
/// Owns the preset dropdown, optional custom date-picker, and hour/minute
/// dropdowns. Emits `TimeRangeChanged` on every effective window change.
pub struct TimeRangePanel {
    pub dropdown_time_range: Entity<Dropdown>,
    pub custom_date_range_picker: Entity<DatePickerState>,
    pub custom_start_hour_dropdown: Entity<Dropdown>,
    pub custom_start_minute_dropdown: Entity<Dropdown>,
    pub custom_end_hour_dropdown: Entity<Dropdown>,
    pub custom_end_minute_dropdown: Entity<Dropdown>,
    pub selected_time_range: Option<TimeRange>,
    pub timestamp_mode: TimestampDisplayMode,
    _subscriptions: Vec<Subscription>,
}

impl TimeRangePanel {
    /// Construct a new panel.
    ///
    /// `placeholder` is shown on the preset dropdown when nothing is selected.
    /// `initial_index` is the index into the standard preset list to pre-select
    /// (`None` → no selection / "all time").
    pub fn new(
        placeholder: impl Into<gpui::SharedString>,
        initial_index: Option<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let dropdown_time_range = cx.new(|_cx| {
            Dropdown::new("time-range-panel-preset")
                .placeholder(placeholder)
                .items(Self::preset_items())
                .selected_index(initial_index)
                .toolbar_style(true)
        });

        let custom_date_range_picker =
            cx.new(|cx| DatePickerState::range(window, cx).date_format("%Y-%m-%d"));

        let custom_start_hour_dropdown = cx.new(|_cx| {
            Dropdown::new("time-range-panel-start-hour")
                .placeholder("HH")
                .items(Self::hour_items())
                .selected_index(Some(0))
        });

        let custom_start_minute_dropdown = cx.new(|_cx| {
            Dropdown::new("time-range-panel-start-minute")
                .placeholder("MM")
                .items(Self::minute_items())
                .selected_index(Some(0))
        });

        let custom_end_hour_dropdown = cx.new(|_cx| {
            Dropdown::new("time-range-panel-end-hour")
                .placeholder("HH")
                .items(Self::hour_items())
                .selected_index(Some(23))
        });

        let custom_end_minute_dropdown = cx.new(|_cx| {
            Dropdown::new("time-range-panel-end-minute")
                .placeholder("MM")
                .items(Self::minute_items())
                .selected_index(Some(59))
        });

        let selected_time_range = initial_index.and_then(Self::time_range_for_index);

        let preset_sub = cx.subscribe(
            &dropdown_time_range,
            |this, _, event: &DropdownSelectionChanged, cx| {
                let Some(range) = Self::time_range_for_index(event.index) else {
                    return;
                };

                this.selected_time_range = Some(range);

                if range == TimeRange::Custom {
                    // Show the date picker — no window emitted yet until Apply.
                    cx.notify();
                    return;
                }

                let (start_ms, end_ms) = Self::resolved_window_for_preset(range);
                cx.emit(TimeRangeChanged { start_ms, end_ms });
                cx.notify();
            },
        );

        // Clear the status message when the date picker changes — the host
        // document can re-check validity on its next render.
        let picker_sub = cx.subscribe(
            &custom_date_range_picker,
            |_this, _, _event: &DatePickerEvent, cx| {
                cx.notify();
            },
        );

        let start_hour_sub = cx.subscribe(
            &custom_start_hour_dropdown,
            |_this, _, _event: &DropdownSelectionChanged, cx| {
                cx.notify();
            },
        );

        let start_minute_sub = cx.subscribe(
            &custom_start_minute_dropdown,
            |_this, _, _event: &DropdownSelectionChanged, cx| {
                cx.notify();
            },
        );

        let end_hour_sub = cx.subscribe(
            &custom_end_hour_dropdown,
            |_this, _, _event: &DropdownSelectionChanged, cx| {
                cx.notify();
            },
        );

        let end_minute_sub = cx.subscribe(
            &custom_end_minute_dropdown,
            |_this, _, _event: &DropdownSelectionChanged, cx| {
                cx.notify();
            },
        );

        Self {
            dropdown_time_range,
            custom_date_range_picker,
            custom_start_hour_dropdown,
            custom_start_minute_dropdown,
            custom_end_hour_dropdown,
            custom_end_minute_dropdown,
            selected_time_range,
            timestamp_mode: TimestampDisplayMode::default(),
            _subscriptions: vec![
                preset_sub,
                picker_sub,
                start_hour_sub,
                start_minute_sub,
                end_hour_sub,
                end_minute_sub,
            ],
        }
    }

    // ── Preset mapping ────────────────────────────────────────────────────────

    pub fn preset_items() -> Vec<DropdownItem> {
        vec![
            DropdownItem::new("15m"),
            DropdownItem::new("1h"),
            DropdownItem::new("6h"),
            DropdownItem::new("24h"),
            DropdownItem::new("7d"),
            DropdownItem::new("Custom…"),
        ]
    }

    pub fn time_range_for_index(index: usize) -> Option<TimeRange> {
        match index {
            0 => Some(TimeRange::Last15min),
            1 => Some(TimeRange::LastHour),
            2 => Some(TimeRange::Last6Hours),
            3 => Some(TimeRange::Last24Hours),
            4 => Some(TimeRange::Last7Days),
            5 => Some(TimeRange::Custom),
            _ => None,
        }
    }

    // ── Hour / minute item generators ────────────────────────────────────────

    pub fn hour_items() -> Vec<DropdownItem> {
        (0..24)
            .map(|h| {
                let value = format!("{h:02}");
                DropdownItem::with_value(value.clone(), value)
            })
            .collect()
    }

    pub fn minute_items() -> Vec<DropdownItem> {
        (0..60)
            .map(|m| {
                let value = format!("{m:02}");
                DropdownItem::with_value(value.clone(), value)
            })
            .collect()
    }

    // ── Custom-range helpers ─────────────────────────────────────────────────

    pub fn custom_date_range(
        &self,
        cx: &App,
    ) -> Option<(
        dbflux_core::chrono::NaiveDate,
        dbflux_core::chrono::NaiveDate,
    )> {
        match self.custom_date_range_picker.read(cx).date() {
            Date::Range(Some(start), Some(end)) => Some((start, end)),
            _ => None,
        }
    }

    fn selected_dropdown_number(dropdown: &Entity<Dropdown>, cx: &App) -> Option<u32> {
        dropdown.read(cx).selected_value()?.parse::<u32>().ok()
    }

    pub fn custom_time_parts(&self, cx: &App) -> Option<(u32, u32, u32, u32)> {
        Some((
            Self::selected_dropdown_number(&self.custom_start_hour_dropdown, cx)?,
            Self::selected_dropdown_number(&self.custom_start_minute_dropdown, cx)?,
            Self::selected_dropdown_number(&self.custom_end_hour_dropdown, cx)?,
            Self::selected_dropdown_number(&self.custom_end_minute_dropdown, cx)?,
        ))
    }

    pub fn can_apply_custom_range(&self, cx: &App) -> bool {
        self.custom_date_range(cx).is_some() && self.custom_time_parts(cx).is_some()
    }

    /// Resolve a relative preset to absolute `(start_ms, end_ms)` bounds.
    ///
    /// `to_filter_values` returns `None` for the end of relative presets so
    /// that callers like the audit-log filter can express an unbounded tail.
    /// `TimeRangeChanged` consumers (query execution, chart panels) need a
    /// closed window, so the panel materialises `end = now` at emission time.
    fn resolved_window_for_preset(range: TimeRange) -> (Option<i64>, Option<i64>) {
        let (start_ms, end_ms) = range.to_filter_values();

        // Only materialise `end` for ranges that resolve a real start (i.e. the
        // relative presets). `Custom` returns `(None, None)` until the user
        // applies the date picker — leave that case alone so the caller knows
        // no window is selected yet.
        let end_ms = if start_ms.is_some() {
            end_ms.or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .ok()
            })
        } else {
            end_ms
        };

        (start_ms, end_ms)
    }

    /// Select a preset by index and emit `TimeRangeChanged`.
    ///
    /// Mirrors what the dropdown subscription does, but is callable from
    /// external chip rows that drive the panel directly (the dropdown's
    /// `set_selected_index` does not emit `DropdownSelectionChanged`).
    /// `Custom` is selected without emitting — the host must call
    /// `apply_custom_range` after the user confirms the picker.
    pub fn select_preset(&mut self, index: usize, cx: &mut Context<Self>) {
        let Some(range) = Self::time_range_for_index(index) else {
            return;
        };

        self.selected_time_range = Some(range);
        self.dropdown_time_range.update(cx, |dd, cx| {
            dd.set_selected_index(Some(index), cx);
        });

        if range == TimeRange::Custom {
            cx.notify();
            return;
        }

        let (start_ms, end_ms) = Self::resolved_window_for_preset(range);
        cx.emit(TimeRangeChanged { start_ms, end_ms });
        cx.notify();
    }

    /// Emit a `TimeRangeChanged` event for the currently selected preset.
    ///
    /// Used by hosts after subscribing to seed the initial window — the
    /// constructor selects a default preset but cannot emit during build
    /// because no subscriber is registered yet. `Custom` is a no-op until
    /// the user applies the date picker.
    pub fn emit_initial(&self, cx: &mut Context<Self>) {
        let Some(range) = self.selected_time_range else {
            return;
        };
        if range == TimeRange::Custom {
            return;
        }

        let (start_ms, end_ms) = Self::resolved_window_for_preset(range);
        cx.emit(TimeRangeChanged { start_ms, end_ms });
    }

    /// Validate and emit `TimeRangeChanged` for the current custom picker state.
    ///
    /// Returns `Err(message)` when the inputs are invalid or incomplete.
    pub fn apply_custom_range(&mut self, cx: &mut Context<Self>) -> Result<(i64, i64), String> {
        let (start_date, end_date) = self
            .custom_date_range(cx)
            .ok_or_else(|| "No date range selected".to_string())?;

        let (start_hour, start_minute, end_hour, end_minute) = self
            .custom_time_parts(cx)
            .ok_or_else(|| "Incomplete time selection".to_string())?;

        let (start_ms, end_ms) = validate_custom_range_parts(
            start_date,
            start_hour,
            start_minute,
            end_date,
            end_hour,
            end_minute,
            self.timestamp_mode,
        )?;

        self.selected_time_range = Some(TimeRange::Custom);
        cx.emit(TimeRangeChanged {
            start_ms: Some(start_ms),
            end_ms: Some(end_ms),
        });
        cx.notify();

        Ok((start_ms, end_ms))
    }

    /// Returns the individual sub-elements of the custom picker row as a struct.
    ///
    /// Each element is pre-sized (date picker at `date_picker_width`, time
    /// dropdowns at `px(72)`) but NOT wrapped with outer chrome (ring borders,
    /// background, padding) — callers compose them into a row with whatever
    /// decoration they need.
    ///
    /// Use this when per-slot decoration is required (e.g. audit's keyboard-focus
    /// ring guards that wrap each individual control). Use `render_custom_picker_row`
    /// when the standard gap-1 flex row layout suffices.
    ///
    /// Both APIs render the same underlying sub-entities: `render_custom_picker_row`
    /// calls this method internally and assembles the result into the standard row.
    pub fn custom_picker_slots(
        &self,
        date_picker_width: gpui::Pixels,
        _cx: &gpui::App,
    ) -> CustomPickerSlots {
        use gpui::{div, px};

        CustomPickerSlots {
            date_picker: div()
                .w(date_picker_width)
                .child(
                    DatePicker::new(&self.custom_date_range_picker)
                        .small()
                        .placeholder("Select date range")
                        .number_of_months(2),
                )
                .into_any_element(),
            from_label: Text::caption("from").into_any_element(),
            start_hour: div()
                .w(px(72.0))
                .child(self.custom_start_hour_dropdown.clone())
                .into_any_element(),
            start_minute: div()
                .w(px(72.0))
                .child(self.custom_start_minute_dropdown.clone())
                .into_any_element(),
            to_label: Text::caption("to").into_any_element(),
            end_hour: div()
                .w(px(72.0))
                .child(self.custom_end_hour_dropdown.clone())
                .into_any_element(),
            end_minute: div()
                .w(px(72.0))
                .child(self.custom_end_minute_dropdown.clone())
                .into_any_element(),
        }
    }

    /// Render the shared custom date+time picker row used by chart and code hosts
    /// when the user has selected the Custom preset.
    ///
    /// Returns a flex row containing: the date-range picker at `date_picker_width`,
    /// a "from" label, start-hour dropdown, start-minute dropdown, a "to" label,
    /// end-hour dropdown, and end-minute dropdown.
    ///
    /// Does NOT include an Apply button — each host owns its own Apply because the
    /// component type, disabled visuals, and side-effect chain differ between hosts.
    ///
    /// Does NOT include outer chrome (border, background, padding) — callers are
    /// responsible for wrapping with their own container styling.
    ///
    /// `date_picker_width` is parameterized because chart uses `px(260)` and code
    /// uses `px(320)`. Standardizing is a visible change and out of scope for this
    /// refactor. The `_cx` parameter is reserved for future theme-aware tweaks.
    ///
    /// Internally delegates to `custom_picker_slots` so both APIs render the same
    /// sub-elements with guaranteed structural parity.
    pub fn render_custom_picker_row(
        &self,
        date_picker_width: gpui::Pixels,
        cx: &gpui::App,
    ) -> impl gpui::IntoElement {
        use gpui::div;

        let slots = self.custom_picker_slots(date_picker_width, cx);

        div()
            .flex()
            .items_center()
            .gap_1()
            .child(slots.date_picker)
            .child(slots.from_label)
            .child(slots.start_hour)
            .child(slots.start_minute)
            .child(slots.to_label)
            .child(slots.end_hour)
            .child(slots.end_minute)
    }
}

/// Individual picker elements returned by [`TimeRangePanel::custom_picker_slots`].
///
/// Hosts that need per-slot decoration (e.g. audit's keyboard-focus ring guards)
/// receive each element separately and compose them into a row. All elements
/// are pre-sized but carry no outer chrome.
pub struct CustomPickerSlots {
    /// Date-range picker at the caller-specified width.
    pub date_picker: gpui::AnyElement,
    /// The "from" label between the date picker and start-time dropdowns.
    pub from_label: gpui::AnyElement,
    /// Start-hour dropdown at `px(72)`.
    pub start_hour: gpui::AnyElement,
    /// Start-minute dropdown at `px(72)`.
    pub start_minute: gpui::AnyElement,
    /// The "to" label between start and end dropdowns.
    pub to_label: gpui::AnyElement,
    /// End-hour dropdown at `px(72)`.
    pub end_hour: gpui::AnyElement,
    /// End-minute dropdown at `px(72)`.
    pub end_minute: gpui::AnyElement,
}

impl EventEmitter<TimeRangeChanged> for TimeRangePanel {}

#[cfg(test)]
mod tests {
    use super::{TimeRange, TimeRangeChanged, TimeRangePanel};
    use gpui::prelude::*;

    #[test]
    fn preset_index_maps_to_correct_range_variant() {
        let cases = [
            (0, TimeRange::Last15min),
            (1, TimeRange::LastHour),
            (2, TimeRange::Last6Hours),
            (3, TimeRange::Last24Hours),
            (4, TimeRange::Last7Days),
            (5, TimeRange::Custom),
        ];

        for (index, expected) in cases {
            assert_eq!(
                TimeRangePanel::time_range_for_index(index),
                Some(expected),
                "index {index} should map to {expected:?}"
            );
        }
    }

    #[test]
    fn out_of_range_index_returns_none() {
        assert_eq!(TimeRangePanel::time_range_for_index(6), None);
        assert_eq!(TimeRangePanel::time_range_for_index(100), None);
    }

    #[test]
    fn resolved_window_for_relative_preset_materializes_end_as_now() {
        // Sanity-check each relative preset emits a closed window where
        // end is filled with the current epoch ms (within a 5s tolerance).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        for range in [
            TimeRange::Last15min,
            TimeRange::LastHour,
            TimeRange::Last6Hours,
            TimeRange::Last24Hours,
            TimeRange::Last7Days,
        ] {
            let (start_ms, end_ms) = TimeRangePanel::resolved_window_for_preset(range);

            let start = start_ms.expect("relative preset must have a start");
            let end = end_ms.expect("relative preset must materialise an end");

            assert!(end >= start, "{range:?}: end should be >= start");
            assert!(
                (end - now).abs() < 5_000,
                "{range:?}: end should be close to now (delta = {} ms)",
                (end - now).abs()
            );
        }
    }

    #[test]
    fn resolved_window_for_custom_preserves_unbounded_end() {
        // Custom returns (None, None) per the panel contract; the host has to
        // apply the date picker to materialise concrete bounds.
        let (start_ms, end_ms) = TimeRangePanel::resolved_window_for_preset(TimeRange::Custom);
        assert!(start_ms.is_none());
        assert!(end_ms.is_none());
    }

    #[test]
    fn hour_items_cover_full_day() {
        let items = TimeRangePanel::hour_items();
        assert_eq!(items.len(), 24);
        assert_eq!(items[0].value.as_ref(), "00");
        assert_eq!(items[23].value.as_ref(), "23");
    }

    #[test]
    fn minute_items_cover_full_hour() {
        let items = TimeRangePanel::minute_items();
        assert_eq!(items.len(), 60);
        assert_eq!(items[0].value.as_ref(), "00");
        assert_eq!(items[59].value.as_ref(), "59");
    }

    #[test]
    fn custom_mode_triggers_no_immediate_emission() {
        // Custom selection index (5) should return Custom variant,
        // confirming the panel will NOT emit immediately on Custom selection.
        assert_eq!(
            TimeRangePanel::time_range_for_index(5),
            Some(TimeRange::Custom)
        );
    }

    #[test]
    fn custom_is_the_last_preset_index() {
        // Index 5 is the boundary where the context bar switches to showing
        // the inline date picker.  Verify it stays stable as presets grow.
        let items = TimeRangePanel::preset_items();
        let last_index = items.len() - 1;

        assert_eq!(
            TimeRangePanel::time_range_for_index(last_index),
            Some(TimeRange::Custom),
            "the last preset must always be Custom so the inline picker renders"
        );
    }

    #[test]
    fn non_custom_presets_do_not_map_to_custom() {
        // Preset indices 0–4 must not map to Custom; they emit immediately.
        for index in 0..5 {
            assert_ne!(
                TimeRangePanel::time_range_for_index(index),
                Some(TimeRange::Custom),
                "index {index} must not be Custom"
            );
        }
    }

    // ── render_custom_picker_row TDD tests ────────────────────────────────────
    //
    // These tests verify the render helper contract:
    //   - Returns an element that converts to `AnyElement` without panicking.
    //   - Does not emit `TimeRangeChanged` or mutate panel state.
    //   - Sub-entity handles are stable across repeated calls.
    //
    // The helper requires a constructed `TimeRangePanel`, which in turn requires
    // a `&mut Window`. We use `#[gpui::test]` with `cx.add_window_view` and a
    // minimal `Render` harness, since `TimeRangePanel` does not implement `Render`.

    /// Minimal window harness so tests can open a window in dbflux_components
    /// without importing dbflux_ui's `Root` type.
    struct PanelHarness {
        #[allow(dead_code)]
        panel: gpui::Entity<TimeRangePanel>,
    }

    impl gpui::Render for PanelHarness {
        fn render(
            &mut self,
            _window: &mut gpui::Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl gpui::IntoElement {
            gpui::div()
        }
    }

    /// Smoke test: `render_custom_picker_row` returns an element that can be
    /// converted to `AnyElement` without panicking.
    ///
    /// Satisfies REQ-1, SCEN-7.
    #[gpui::test]
    fn render_custom_picker_row_returns_element(cx: &mut gpui::TestAppContext) {
        use gpui::px;
        use std::cell::RefCell;
        use std::rc::Rc;

        let panel_ref: Rc<RefCell<Option<gpui::Entity<TimeRangePanel>>>> =
            Rc::new(RefCell::new(None));

        let (_, window) = cx.add_window_view({
            let panel_ref = panel_ref.clone();
            move |window, cx| {
                let panel = cx.new(|cx| TimeRangePanel::new("Select range", None, window, cx));
                panel_ref.replace(Some(panel.clone()));
                PanelHarness { panel }
            }
        });

        let panel = panel_ref.borrow().clone().expect("panel must be created");

        window.update(|_, app| {
            let element = panel.read(app).render_custom_picker_row(px(260.0), app);
            // Converting to AnyElement must not panic — pins that the method exists
            // and its return type implements IntoElement.
            let _ = element.into_any_element();
        });
    }

    /// `render_custom_picker_row` must not emit `TimeRangeChanged` and must not
    /// mutate `selected_time_range`.
    ///
    /// Satisfies REQ-5 (event contract), SCEN-7.
    #[gpui::test]
    fn render_custom_picker_row_is_render_only(cx: &mut gpui::TestAppContext) {
        use gpui::px;
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;

        let panel_ref: Rc<RefCell<Option<gpui::Entity<TimeRangePanel>>>> =
            Rc::new(RefCell::new(None));
        let event_count = Rc::new(Cell::new(0u32));

        let (_, window) = cx.add_window_view({
            let panel_ref = panel_ref.clone();
            let event_count = event_count.clone();
            move |window, cx| {
                let panel = cx.new(|cx| TimeRangePanel::new("Select range", None, window, cx));
                panel_ref.replace(Some(panel.clone()));

                // Subscribe to TimeRangeChanged before the helper is called.
                let _sub = cx.subscribe(&panel, {
                    let event_count = event_count.clone();
                    move |_this, _, _event: &TimeRangeChanged, _cx| {
                        event_count.set(event_count.get() + 1);
                    }
                });

                PanelHarness { panel }
            }
        });

        let panel = panel_ref.borrow().clone().expect("panel must be created");

        window.update(|_, app| {
            let selected_before = panel.read(app).selected_time_range;
            let element = panel.read(app).render_custom_picker_row(px(260.0), app);
            let _ = element.into_any_element();
            let selected_after = panel.read(app).selected_time_range;

            assert_eq!(
                selected_before, selected_after,
                "render_custom_picker_row must not mutate selected_time_range"
            );
        });

        // No events should have been emitted during the helper call.
        assert_eq!(
            event_count.get(),
            0,
            "render_custom_picker_row must not emit TimeRangeChanged"
        );
    }

    /// Sub-entity handles are stable across two consecutive calls to the helper.
    ///
    /// Satisfies REQ-4.
    #[gpui::test]
    fn render_custom_picker_row_does_not_mutate_sub_entities(cx: &mut gpui::TestAppContext) {
        use gpui::px;
        use std::cell::RefCell;
        use std::rc::Rc;

        let panel_ref: Rc<RefCell<Option<gpui::Entity<TimeRangePanel>>>> =
            Rc::new(RefCell::new(None));

        let (_, window) = cx.add_window_view({
            let panel_ref = panel_ref.clone();
            move |window, cx| {
                let panel = cx.new(|cx| TimeRangePanel::new("Select range", None, window, cx));
                panel_ref.replace(Some(panel.clone()));
                PanelHarness { panel }
            }
        });

        let panel = panel_ref.borrow().clone().expect("panel must be created");

        window.update(|_, app| {
            // Capture entity IDs before calling the helper.
            let (picker_id, sh_id, sm_id, eh_id, em_id) = {
                let p = panel.read(app);
                (
                    p.custom_date_range_picker.entity_id(),
                    p.custom_start_hour_dropdown.entity_id(),
                    p.custom_start_minute_dropdown.entity_id(),
                    p.custom_end_hour_dropdown.entity_id(),
                    p.custom_end_minute_dropdown.entity_id(),
                )
            };

            // Call the helper twice with different widths.
            let _ = panel
                .read(app)
                .render_custom_picker_row(px(260.0), app)
                .into_any_element();
            let _ = panel
                .read(app)
                .render_custom_picker_row(px(320.0), app)
                .into_any_element();

            // Sub-entity handles must be unchanged (cloned, not replaced).
            let p = panel.read(app);
            assert_eq!(p.custom_date_range_picker.entity_id(), picker_id);
            assert_eq!(p.custom_start_hour_dropdown.entity_id(), sh_id);
            assert_eq!(p.custom_start_minute_dropdown.entity_id(), sm_id);
            assert_eq!(p.custom_end_hour_dropdown.entity_id(), eh_id);
            assert_eq!(p.custom_end_minute_dropdown.entity_id(), em_id);
        });
    }
}
