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

use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use gpui::prelude::*;
use gpui::{App, Entity, EventEmitter, Subscription, Window};
use gpui_component::calendar::Date;
use gpui_component::date_picker::{DatePickerEvent, DatePickerState};

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

                let (start_ms, end_ms) = range.to_filter_values();
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
            DropdownItem::new("Last 5 min"),
            DropdownItem::new("Last 30 min"),
            DropdownItem::new("Last 1 h"),
            DropdownItem::new("Last 3 h"),
            DropdownItem::new("Last 12 h"),
            DropdownItem::new("Custom"),
        ]
    }

    pub fn time_range_for_index(index: usize) -> Option<TimeRange> {
        match index {
            0 => Some(TimeRange::Last5min),
            1 => Some(TimeRange::Last30min),
            2 => Some(TimeRange::LastHour),
            3 => Some(TimeRange::Last3Hours),
            4 => Some(TimeRange::Last12Hours),
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
}

impl EventEmitter<TimeRangeChanged> for TimeRangePanel {}

#[cfg(test)]
mod tests {
    use super::{TimeRange, TimeRangePanel};

    #[test]
    fn preset_index_maps_to_correct_range_variant() {
        let cases = [
            (0, TimeRange::Last5min),
            (1, TimeRange::Last30min),
            (2, TimeRange::LastHour),
            (3, TimeRange::Last3Hours),
            (4, TimeRange::Last12Hours),
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
}
