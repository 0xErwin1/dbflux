//! Options phase of the migration wizard: exactly two engine-backed controls,
//! nothing decorative. **Segment/commit size** feeds
//! `MigrationOptions::segment_size` (default 500, minimum 1) and **disable
//! referential integrity** feeds `MigrationOptions::disable_referential_integrity`,
//! rendered only when the target driver advertises
//! `DriverCapabilities::DISABLE_FK_CHECKS` (R7) — an unsupported target simply
//! never sees the control, it is not an error state. Per-table mapping mode
//! lives in the Tables Mapping grid (`mapping.rs`), not here.

use dbflux_components::controls::{Checkbox, GpuiInput as Input, InputEvent, InputState};
use dbflux_components::primitives::{FocusShape, Text, focus_ring};
use dbflux_components::tokens::{ChamferCut, Spacing};
use dbflux_core::keymap_types::Command;
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::Sizable;

const DEFAULT_SEGMENT_SIZE: u32 = 500;
const SEGMENT_SIZE_INPUT_WIDTH: Pixels = px(160.0);

/// Parses a typed segment/commit-size value, rejecting non-numeric text and
/// non-positive values — the engine commits at least one row per segment, so
/// zero (and anything that does not parse as a whole number) is invalid.
pub fn parse_segment_size(text: &str) -> Option<u32> {
    match text.trim().parse::<u32>() {
        Ok(0) | Err(_) => None,
        Ok(value) => Some(value),
    }
}

/// Whether a disable-referential-integrity request is actually honored: only
/// when the target driver advertises `DriverCapabilities::DISABLE_FK_CHECKS`
/// (R7). The wizard never asks the engine to disable a toggle the target
/// cannot support — the gating happens here, before `MigrationOptions` is
/// built, not as a silent no-op inside `run_migration`.
pub fn resolved_disable_referential_integrity(
    requested: bool,
    target_supports_disable_fk_checks: bool,
) -> bool {
    requested && target_supports_disable_fk_checks
}

/// The control of the step the keyboard cursor is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionsField {
    SegmentSize,
    DisableReferentialIntegrity,
}

/// Emitted whenever the segment size or the disable-RI request changes, so
/// the host can re-evaluate any downstream state that depends on this
/// phase's values.
#[derive(Debug, Clone)]
pub struct OptionsChanged;

pub struct OptionsPhase {
    focus_handle: FocusHandle,
    segment_size: u32,
    segment_size_input: Entity<InputState>,
    segment_size_invalid: bool,
    supports_disable_ri: bool,
    disable_referential_integrity_requested: bool,
    cursor: OptionsField,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<OptionsChanged> for OptionsPhase {}

impl OptionsPhase {
    /// `supports_disable_ri` reflects the chosen target's
    /// `DriverCapabilities::DISABLE_FK_CHECKS` and is fixed for the phase's
    /// lifetime — the target container is chosen earlier, in Source & Target.
    pub fn new(supports_disable_ri: bool, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let segment_size_input = cx.new(|cx| {
            InputState::new(window, cx)
                .default_value(DEFAULT_SEGMENT_SIZE.to_string())
                .placeholder(dbflux_i18n::t!(
                    "document.migrate_wizard.options.segment_size_placeholder"
                ))
        });

        let mut phase = Self {
            focus_handle: cx.focus_handle(),
            segment_size: DEFAULT_SEGMENT_SIZE,
            segment_size_input,
            segment_size_invalid: false,
            supports_disable_ri,
            disable_referential_integrity_requested: false,
            cursor: OptionsField::SegmentSize,
            _subscriptions: Vec::new(),
        };

        let subscription = cx.subscribe_in(
            &phase.segment_size_input,
            window,
            |this, _entity, event: &InputEvent, window, cx| match event {
                InputEvent::Change => this.on_segment_size_changed(window, cx),
                InputEvent::PressEnter { .. } => this.focus_handle.focus(window, cx),
                _ => {}
            },
        );
        phase._subscriptions.push(subscription);

        phase
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus_handle
    }

    /// The last valid segment/commit size, feeding
    /// `MigrationOptions::segment_size` directly — an in-progress invalid
    /// edit never overwrites it, so the run always has a usable value.
    pub fn segment_size(&self) -> u32 {
        self.segment_size
    }

    /// Feeds `MigrationOptions::disable_referential_integrity` directly,
    /// already gated on the target's capability.
    pub fn disable_referential_integrity(&self) -> bool {
        resolved_disable_referential_integrity(
            self.disable_referential_integrity_requested,
            self.supports_disable_ri,
        )
    }

    fn on_segment_size_changed(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let typed = self.segment_size_input.read(cx).value().to_string();

        match parse_segment_size(&typed) {
            Some(value) => {
                self.segment_size = value;
                self.segment_size_invalid = false;
            }
            None => {
                self.segment_size_invalid = true;
            }
        }

        cx.emit(OptionsChanged);
        cx.notify();
    }

    /// Runs a keymap command on the step: J / K move between the segment
    /// size and the referential-integrity switch, Enter or I types in the
    /// size (Enter or Escape hand the keyboard back), and Enter or Space
    /// flips the switch. Returns whether the command applied.
    pub fn handle_command(
        &mut self,
        command: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let input_focused = self
            .segment_size_input
            .read(cx)
            .focus_handle(cx)
            .is_focused(window);

        if input_focused {
            if command == Command::Cancel {
                self.focus_handle.focus(window, cx);
                cx.notify();
                return true;
            }
            return false;
        }

        match command {
            Command::SelectNext if self.supports_disable_ri => {
                self.cursor = OptionsField::DisableReferentialIntegrity;
                cx.notify();
                true
            }
            Command::SelectPrev => {
                self.cursor = OptionsField::SegmentSize;
                cx.notify();
                true
            }
            Command::Execute | Command::ExpandCollapse => {
                match self.cursor {
                    OptionsField::SegmentSize if command == Command::Execute => {
                        self.segment_size_input
                            .update(cx, |state, cx| state.focus(window, cx));
                    }
                    OptionsField::SegmentSize => {}
                    OptionsField::DisableReferentialIntegrity => {
                        let requested = !self.disable_referential_integrity_requested;
                        self.on_disable_ri_toggled(requested, cx);
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Whether the segment size field has the keyboard, for keyboard tests.
    #[cfg(test)]
    pub(crate) fn segment_size_focused(&self, window: &Window, cx: &App) -> bool {
        self.segment_size_input
            .read(cx)
            .focus_handle(cx)
            .is_focused(window)
    }

    fn on_disable_ri_toggled(&mut self, checked: bool, cx: &mut Context<Self>) {
        self.disable_referential_integrity_requested = checked;
        cx.emit(OptionsChanged);
        cx.notify();
    }
}

impl Render for OptionsPhase {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let keyboard = self.focus_handle.is_focused(window);
        let segment_cursor = keyboard && self.cursor == OptionsField::SegmentSize;
        let ri_cursor = keyboard && self.cursor == OptionsField::DisableReferentialIntegrity;

        div()
            .track_focus(&self.focus_handle)
            .key_context("MigrateOptions")
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .size_full()
            .child(self.render_segment_size(segment_cursor, cx))
            .when(self.supports_disable_ri, |parent| {
                parent.child(self.render_disable_ri(ri_cursor, cx))
            })
    }
}

impl OptionsPhase {
    fn render_segment_size(&self, cursor: bool, cx: &App) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body(dbflux_i18n::t!(
                "document.migrate_wizard.options.segment_size_label"
            )))
            .child(
                focus_ring(
                    cursor,
                    FocusShape::Chamfer(ChamferCut::CONTROL),
                    None,
                    Input::new(&self.segment_size_input).small().w_full(),
                    cx,
                )
                .w(SEGMENT_SIZE_INPUT_WIDTH),
            )
            .when(self.segment_size_invalid, |parent| {
                parent.child(
                    Text::caption(dbflux_i18n::t!(
                        "document.migrate_wizard.options.segment_size_invalid"
                    ))
                    .danger(),
                )
            })
    }

    fn render_disable_ri(&self, cursor: bool, cx: &mut Context<Self>) -> impl IntoElement {
        let checkbox = Checkbox::new("migrate-options-disable-ri")
            .checked(self.disable_referential_integrity_requested)
            .label(dbflux_i18n::t!(
                "document.migrate_wizard.options.disable_referential_integrity"
            ))
            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                this.on_disable_ri_toggled(*checked, cx);
            }));

        focus_ring(
            cursor,
            FocusShape::Chamfer(ChamferCut::CONTROL),
            None,
            checkbox,
            cx,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_segment_size, resolved_disable_referential_integrity};
    use crate::migrate_wizard::build_migration_options;
    use dbflux_core::TableRef;

    #[test]
    fn parse_segment_size_accepts_positive_whole_numbers() {
        assert_eq!(parse_segment_size("500"), Some(500));
        assert_eq!(parse_segment_size("1"), Some(1));
        assert_eq!(parse_segment_size("  250  "), Some(250));
    }

    #[test]
    fn parse_segment_size_rejects_zero_and_non_numeric_input() {
        assert_eq!(parse_segment_size("0"), None);
        assert_eq!(parse_segment_size(""), None);
        assert_eq!(parse_segment_size("abc"), None);
        assert_eq!(parse_segment_size("-5"), None);
        assert_eq!(parse_segment_size("12.5"), None);
    }

    #[test]
    fn resolved_disable_referential_integrity_requires_both_request_and_capability() {
        assert!(!resolved_disable_referential_integrity(false, false));
        assert!(!resolved_disable_referential_integrity(false, true));
        assert!(!resolved_disable_referential_integrity(true, false));
        assert!(resolved_disable_referential_integrity(true, true));
    }

    #[test]
    fn options_phase_values_feed_migration_options_verbatim() {
        let segment_size = parse_segment_size("750").expect("valid segment size");
        let disable_referential_integrity = resolved_disable_referential_integrity(true, true);

        let options = build_migration_options(
            segment_size,
            "source_db".to_string(),
            "target_db".to_string(),
            false,
            disable_referential_integrity,
            Some(vec![TableRef::new("a")]),
        );

        assert_eq!(options.segment_size, 750);
        assert!(options.disable_referential_integrity);
    }
}
