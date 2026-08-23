//! Pure phase state machine for the export wizard: the fixed rail ordering,
//! the guards that gate each forward transition, run-state tracking, and the
//! segment-size input validation. No GPUI — unit testable without a wizard
//! entity. Rendering and the run itself live in `mod.rs` / `run.rs`.
//!
//! Unlike the migrate wizard, no phase here depends on live metadata (the
//! sidebar already resolved the table selection before the wizard opens), so
//! the forward/back transitions are pure functions rather than living on the
//! wizard entity.

/// The four fixed rail entries. Declaration order doubles as the `Ord` used
/// by the rail to decide which entries are already completed
/// (`entry < current_phase`), mirroring the migrate wizard's `WizardPhase`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExportPhase {
    Tables,
    FormatOptions,
    Confirm,
    Run,
}

impl ExportPhase {
    /// The rail's display label for this phase, resolved through the
    /// translation catalog. Exhaustive by construction so a new phase fails
    /// this crate's build until its `document.export_wizard.rail.*` entry is
    /// added here.
    pub fn label(self) -> String {
        match self {
            ExportPhase::Tables => dbflux_i18n::t!("document.export_wizard.rail.tables"),
            ExportPhase::FormatOptions => {
                dbflux_i18n::t!("document.export_wizard.rail.format_options")
            }
            ExportPhase::Confirm => dbflux_i18n::t!("document.export_wizard.rail.confirm"),
            ExportPhase::Run => dbflux_i18n::t!("document.export_wizard.rail.run"),
        }
    }
}

/// All rail entries in display order, for `render_wizard_rail` to iterate.
pub const RAIL_PHASES: [ExportPhase; 4] = [
    ExportPhase::Tables,
    ExportPhase::FormatOptions,
    ExportPhase::Confirm,
    ExportPhase::Run,
];

/// Whether `entry` should render a checkmark given the wizard is currently
/// on `current` — an already-passed rail entry.
pub fn is_completed(entry: ExportPhase, current: ExportPhase) -> bool {
    entry < current
}

/// One rail row's presentation state: a completed entry shows a checkmark,
/// the current entry is highlighted. Derived purely from the linear phase
/// ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RailEntry {
    pub phase: ExportPhase,
    pub completed: bool,
    pub current: bool,
}

/// The rail's four entries with their completion/current flags resolved
/// against `current` — the single source of truth the renderer iterates.
pub fn rail_entries(current: ExportPhase) -> Vec<RailEntry> {
    RAIL_PHASES
        .iter()
        .map(|&phase| RailEntry {
            phase,
            completed: is_completed(phase, current),
            current: phase == current,
        })
        .collect()
}

/// The phase reached by pressing the footer's Continue button from `phase`,
/// or `None` for phases whose forward action lives elsewhere: `Confirm`
/// starts the export through its own "Start Export" button, and `Run` has no
/// forward step.
pub fn next_phase(phase: ExportPhase) -> Option<ExportPhase> {
    match phase {
        ExportPhase::Tables => Some(ExportPhase::FormatOptions),
        ExportPhase::FormatOptions => Some(ExportPhase::Confirm),
        ExportPhase::Confirm | ExportPhase::Run => None,
    }
}

/// The phase reached by pressing the footer's Back button from `phase`, or
/// `None` for the first phase and for `Run` (back-navigation is frozen while
/// a run is live or done — Cancel/Close is the only way out from there).
pub fn prev_phase(phase: ExportPhase) -> Option<ExportPhase> {
    match phase {
        ExportPhase::Tables | ExportPhase::Run => None,
        ExportPhase::FormatOptions => Some(ExportPhase::Tables),
        ExportPhase::Confirm => Some(ExportPhase::FormatOptions),
    }
}

/// Tracks the export run itself, separate from `ExportPhase` so `Run` can
/// stay a single rail entry while progress/completion vary underneath.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RunState {
    #[default]
    Idle,
    Running,
    Done,
}

/// Default segment/chunk size offered by the Format & Options phase — the
/// same default `ExportOptions` used before the wizard existed.
pub const DEFAULT_SEGMENT_SIZE: u32 = 500;

/// Parses a typed segment/chunk-size value, rejecting non-numeric text and
/// non-positive values — the engine streams at least one row per segment, so
/// zero (and anything that does not parse as a whole number) is invalid.
pub fn parse_segment_size(text: &str) -> Option<u32> {
    match text.trim().parse::<u32>() {
        Ok(0) | Err(_) => None,
        Ok(value) => Some(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_ordering_matches_rail_declaration_order() {
        assert!(ExportPhase::Tables < ExportPhase::FormatOptions);
        assert!(ExportPhase::FormatOptions < ExportPhase::Confirm);
        assert!(ExportPhase::Confirm < ExportPhase::Run);
    }

    #[test]
    fn phase_labels_cover_every_rail_entry() {
        assert_eq!(
            ExportPhase::Tables.label(),
            dbflux_i18n::t!("document.export_wizard.rail.tables")
        );
        assert_eq!(
            ExportPhase::FormatOptions.label(),
            dbflux_i18n::t!("document.export_wizard.rail.format_options")
        );
        assert_eq!(
            ExportPhase::Confirm.label(),
            dbflux_i18n::t!("document.export_wizard.rail.confirm")
        );
        assert_eq!(
            ExportPhase::Run.label(),
            dbflux_i18n::t!("document.export_wizard.rail.run")
        );
    }

    /// Every `document.export_wizard.rail.*` key resolves to a non-empty,
    /// non-fallback value in both locales — exhaustive over every
    /// `ExportPhase` variant (no wildcard arm in `label()`).
    #[test]
    fn phase_labels_resolve_in_both_locales() {
        for phase in RAIL_PHASES {
            for locale in ["en", "es"] {
                let key = match phase {
                    ExportPhase::Tables => "document.export_wizard.rail.tables",
                    ExportPhase::FormatOptions => "document.export_wizard.rail.format_options",
                    ExportPhase::Confirm => "document.export_wizard.rail.confirm",
                    ExportPhase::Run => "document.export_wizard.rail.run",
                };
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    /// Every rail phase label diverges between locales.
    #[test]
    fn phase_labels_differ_between_locales() {
        for phase in RAIL_PHASES {
            let en = dbflux_i18n::t!(
                match phase {
                    ExportPhase::Tables => "document.export_wizard.rail.tables",
                    ExportPhase::FormatOptions => "document.export_wizard.rail.format_options",
                    ExportPhase::Confirm => "document.export_wizard.rail.confirm",
                    ExportPhase::Run => "document.export_wizard.rail.run",
                },
                locale = "en"
            );
            let es = dbflux_i18n::t!(
                match phase {
                    ExportPhase::Tables => "document.export_wizard.rail.tables",
                    ExportPhase::FormatOptions => "document.export_wizard.rail.format_options",
                    ExportPhase::Confirm => "document.export_wizard.rail.confirm",
                    ExportPhase::Run => "document.export_wizard.rail.run",
                },
                locale = "es"
            );
            assert_ne!(en, es, "{phase:?} rail label must differ between en and es");
        }
    }

    #[test]
    fn rail_entries_mark_passed_phases_completed_and_only_current_as_current() {
        let entries = rail_entries(ExportPhase::Confirm);
        assert_eq!(entries.len(), RAIL_PHASES.len());

        let completed: Vec<ExportPhase> = entries
            .iter()
            .filter(|entry| entry.completed)
            .map(|entry| entry.phase)
            .collect();
        assert_eq!(
            completed,
            vec![ExportPhase::Tables, ExportPhase::FormatOptions]
        );

        let current: Vec<ExportPhase> = entries
            .iter()
            .filter(|entry| entry.current)
            .map(|entry| entry.phase)
            .collect();
        assert_eq!(current, vec![ExportPhase::Confirm]);

        assert!(
            entries
                .iter()
                .all(|entry| !(entry.completed && entry.current))
        );
    }

    #[test]
    fn rail_entries_on_first_phase_have_no_completed_entries() {
        let entries = rail_entries(ExportPhase::Tables);
        assert!(entries.iter().all(|entry| !entry.completed));
        assert!(entries[0].current);
    }

    #[test]
    fn next_phase_advances_through_the_forward_flow_and_stops_at_confirm() {
        assert_eq!(
            next_phase(ExportPhase::Tables),
            Some(ExportPhase::FormatOptions)
        );
        assert_eq!(
            next_phase(ExportPhase::FormatOptions),
            Some(ExportPhase::Confirm)
        );
        assert_eq!(next_phase(ExportPhase::Confirm), None);
        assert_eq!(next_phase(ExportPhase::Run), None);
    }

    #[test]
    fn prev_phase_steps_backwards_and_stops_at_tables_and_run() {
        assert_eq!(prev_phase(ExportPhase::Tables), None);
        assert_eq!(
            prev_phase(ExportPhase::FormatOptions),
            Some(ExportPhase::Tables)
        );
        assert_eq!(
            prev_phase(ExportPhase::Confirm),
            Some(ExportPhase::FormatOptions)
        );
        assert_eq!(prev_phase(ExportPhase::Run), None);
    }

    #[test]
    fn run_state_defaults_to_idle() {
        assert_eq!(RunState::default(), RunState::Idle);
    }

    #[test]
    fn parse_segment_size_accepts_positive_whole_numbers() {
        assert_eq!(parse_segment_size("500"), Some(500));
        assert_eq!(parse_segment_size("1"), Some(1));
        assert_eq!(parse_segment_size("  250  "), Some(250));
    }

    #[test]
    fn parse_segment_size_rejects_zero_and_non_numeric_input() {
        assert_eq!(parse_segment_size("0"), None);
        assert_eq!(parse_segment_size("abc"), None);
        assert_eq!(parse_segment_size(""), None);
        assert_eq!(parse_segment_size("-5"), None);
    }
}
