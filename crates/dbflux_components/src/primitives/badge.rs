//! Connection-environment extensions over bolt's generic [`Badge`].
//!
//! The generic badge primitive lives in `bolt`; the environment semantics —
//! the `ConnectionEnvironment` type and the translated labels — are product
//! concerns and stay here.

use dbflux_core::ConnectionEnvironment;
use gpui::prelude::*;
use gpui::{App, FontWeight, SharedString, Window, div};
use gpui_component::ActiveTheme;

use crate::tokens::{ChamferCut, Feedback};
use bolt::primitives::Chamfer;

// The generic badge moved to `bolt`; re-exported at this path so call sites
// keep compiling.
pub use bolt::primitives::{Badge, BadgeTone};

/// Tone of a connection environment: Production reads as danger, Staging
/// as warning and Development as info.
pub fn environment_badge_tone(environment: ConnectionEnvironment) -> BadgeTone {
    match environment {
        ConnectionEnvironment::Production => BadgeTone::Danger,
        ConnectionEnvironment::Staging => BadgeTone::Warning,
        ConnectionEnvironment::Development => BadgeTone::Info,
    }
}

/// Environment tag (PROD, STAGING, ...) shown next to a connection name.
///
/// Bold 10 px caps on a 4 px chamfer; Danger by default, the tone used for
/// production.
#[derive(IntoElement)]
pub struct EnvTag {
    tone: BadgeTone,
    label: SharedString,
}

impl EnvTag {
    pub fn new(label: impl Into<SharedString>) -> Self {
        Self {
            tone: BadgeTone::Danger,
            label: label.into(),
        }
    }

    /// The tag of a connection environment: its short label (PROD, STAGING,
    /// DEV) in the environment's tone.
    pub fn for_environment(environment: ConnectionEnvironment) -> Self {
        Self::new(environment_short_label(environment)).tone(environment_badge_tone(environment))
    }

    pub fn tone(mut self, tone: BadgeTone) -> Self {
        self.tone = tone;
        self
    }
}

/// Short tag text of a connection environment (PROD, STAGING, DEV).
pub fn environment_short_label(environment: ConnectionEnvironment) -> String {
    match environment {
        ConnectionEnvironment::Production => dbflux_i18n::t!("components.env_tag.production"),
        ConnectionEnvironment::Staging => dbflux_i18n::t!("components.env_tag.staging"),
        ConnectionEnvironment::Development => dbflux_i18n::t!("components.env_tag.development"),
    }
}

/// Full name of a connection environment (Production, Staging, Development).
pub fn environment_label(environment: ConnectionEnvironment) -> String {
    match environment {
        ConnectionEnvironment::Production => dbflux_i18n::t!("components.environment.production"),
        ConnectionEnvironment::Staging => dbflux_i18n::t!("components.environment.staging"),
        ConnectionEnvironment::Development => {
            dbflux_i18n::t!("components.environment.development")
        }
    }
}

impl RenderOnce for EnvTag {
    fn render(self, window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let fill = self.tone.fill(theme, Feedback::ENV_TAG_FILL_ALPHA);
        let text_color = self.tone.text_color(theme);
        let label: SharedString = self.label.to_uppercase().into();

        div()
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .px(Feedback::ENV_TAG_PADDING_X)
            .py(Feedback::ENV_TAG_PADDING_Y)
            .child(Chamfer::new(ChamferCut::KEYCAP).fill(fill))
            .child(
                div()
                    .whitespace_nowrap()
                    .text_size(Feedback::ENV_TAG_FONT)
                    .font_weight(FontWeight::BOLD)
                    .letter_spacing(
                        Feedback::ENV_TAG_FONT.to_pixels(window.rem_size())
                            * Feedback::ENV_TAG_TRACKING_EM,
                    )
                    .text_color(text_color)
                    .child(label),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environments_map_to_their_tones() {
        assert_eq!(
            environment_badge_tone(ConnectionEnvironment::Production),
            BadgeTone::Danger
        );
        assert_eq!(
            environment_badge_tone(ConnectionEnvironment::Staging),
            BadgeTone::Warning
        );
        assert_eq!(
            environment_badge_tone(ConnectionEnvironment::Development),
            BadgeTone::Info
        );
        assert_eq!(
            EnvTag::for_environment(ConnectionEnvironment::Staging).tone,
            BadgeTone::Warning
        );
    }

    #[test]
    fn env_tag_defaults_to_danger() {
        assert_eq!(EnvTag::new("prod").tone, BadgeTone::Danger);
        assert_eq!(
            EnvTag::new("staging").tone(BadgeTone::Warning).tone,
            BadgeTone::Warning
        );
    }
}
