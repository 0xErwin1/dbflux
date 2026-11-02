//! Annotations: labelled overlays that follow semantic nodes from frame to frame.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{MAX_ID_BYTES, Rect};

/// Maximum annotations one bridge holds at once, from every source together.
pub const MAX_ANNOTATIONS: usize = 128;
/// Maximum length of an annotation identifier or group name.
pub const MAX_ANNOTATION_ID_BYTES: usize = 128;
/// Maximum length of an annotation label.
pub const MAX_ANNOTATION_LABEL_BYTES: usize = 128;
/// Longest lifetime an annotation may request: one hour.
pub const MAX_ANNOTATION_TTL_MS: u64 = 60 * 60 * 1000;
/// Color used when an annotation does not name one.
pub const DEFAULT_ANNOTATION_COLOR: &str = "#00A8FFFF";

/// What an annotation is attached to.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnnotationTarget {
    /// A semantic node. Its bounds are resolved again on every frame, so the
    /// annotation follows layout changes, scrolling and zoom, and is hidden
    /// while the node is absent, hidden, or scrolled out of view.
    Node {
        /// Stable semantic node identifier.
        node_id: String,
    },
    /// A fixed window-relative rectangle in logical pixels.
    Rect {
        /// Rectangle to mark.
        rect: Rect,
    },
}

/// How an annotation is drawn.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AnnotationStyle {
    /// A two-pixel outline in the annotation color.
    #[default]
    Outline,
    /// A fill in the annotation color; pick a translucent color.
    Fill,
    /// An outline plus a fill at a quarter of the color's alpha.
    OutlineFill,
}

/// Who last created or changed an annotation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AnnotationSource {
    /// The connected MCP agent.
    Agent,
    /// The application itself.
    App,
    /// The bridge, for example when annotations expire.
    Bridge,
}

/// A requested annotation, as an agent or the application submits it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AnnotationSpec {
    /// Caller-chosen identifier. Submitting an existing identifier replaces that
    /// annotation; omit it to have the bridge assign one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// What the annotation marks.
    pub target: AnnotationTarget,
    /// Short text drawn as a tag at the annotation's top-left corner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Eight-digit `#RRGGBBAA` color. Defaults to [`DEFAULT_ANNOTATION_COLOR`].
    #[serde(default = "default_color")]
    pub color: String,
    /// How the annotation is drawn.
    #[serde(default)]
    pub style: AnnotationStyle,
    /// Optional group, so related annotations can be cleared together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Remove the annotation automatically this many milliseconds after the
    /// first frame that draws it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

impl AnnotationSpec {
    /// Annotate a semantic node with the default color and an outline.
    #[must_use]
    pub fn node(node_id: impl Into<String>) -> Self {
        Self::new(AnnotationTarget::Node {
            node_id: node_id.into(),
        })
    }

    /// Annotate a fixed window-relative rectangle.
    #[must_use]
    pub fn rect(rect: Rect) -> Self {
        Self::new(AnnotationTarget::Rect { rect })
    }

    fn new(target: AnnotationTarget) -> Self {
        Self {
            id: None,
            target,
            label: None,
            color: default_color(),
            style: AnnotationStyle::default(),
            group: None,
            ttl_ms: None,
        }
    }

    /// Set the identifier.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the label.
    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the `#RRGGBBAA` color.
    #[must_use]
    pub fn with_color(mut self, color: impl Into<String>) -> Self {
        self.color = color.into();
        self
    }

    /// Set the drawing style.
    #[must_use]
    pub fn with_style(mut self, style: AnnotationStyle) -> Self {
        self.style = style;
        self
    }

    /// Set the group.
    #[must_use]
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Expire the annotation after `ttl_ms` milliseconds.
    #[must_use]
    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = Some(ttl_ms);
        self
    }

    /// Check every bound the bridge enforces.
    ///
    /// # Errors
    ///
    /// Returns a static explanation of the first violated bound.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self
            .id
            .as_deref()
            .is_some_and(|id| !is_valid_annotation_name(id))
        {
            return Err("annotation id is invalid");
        }
        if self
            .group
            .as_deref()
            .is_some_and(|group| !is_valid_annotation_name(group))
        {
            return Err("annotation group is invalid");
        }
        match &self.target {
            AnnotationTarget::Node { node_id } => {
                if node_id.is_empty()
                    || node_id.len() > MAX_ID_BYTES
                    || node_id.chars().any(char::is_control)
                {
                    return Err("annotation node id is invalid");
                }
            }
            AnnotationTarget::Rect { rect } => {
                if !rect.is_valid() {
                    return Err("annotation rectangle is invalid");
                }
            }
        }
        if parse_rgba(&self.color).is_none() {
            return Err("annotation color must use #RRGGBBAA");
        }
        if self.label.as_ref().is_some_and(|label| {
            label.len() > MAX_ANNOTATION_LABEL_BYTES || label.chars().any(char::is_control)
        }) {
            return Err("annotation label must be at most 128 bytes without control characters");
        }
        if self
            .ttl_ms
            .is_some_and(|ttl| ttl == 0 || ttl > MAX_ANNOTATION_TTL_MS)
        {
            return Err("annotation ttl_ms must be between 1 and 3600000");
        }
        Ok(())
    }
}

/// An annotation the bridge currently holds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Annotation {
    /// Unique identifier.
    pub id: String,
    /// What the annotation marks.
    pub target: AnnotationTarget,
    /// Label drawn as a tag, if any.
    pub label: Option<String>,
    /// `#RRGGBBAA` color.
    pub color: String,
    /// Drawing style.
    pub style: AnnotationStyle,
    /// Group, if any.
    pub group: Option<String>,
    /// Who last created or changed it.
    pub source: AnnotationSource,
    /// Milliseconds since the Unix epoch when it was last set.
    pub updated_ms: u64,
    /// Approximate milliseconds since the Unix epoch when it expires, if it has
    /// a lifetime. The lifetime starts at the first frame that draws it.
    pub expires_ms: Option<u64>,
    /// The visible window-relative bounds drawn in the most recent frame, or
    /// `None` when the target was missing, hidden, or scrolled out of view.
    pub resolved: Option<Rect>,
}

/// Parse an eight-digit `#RRGGBBAA` color into `0xRRGGBBAA`.
#[must_use]
pub fn parse_rgba(color: &str) -> Option<u32> {
    let hex = color.strip_prefix('#')?;
    if hex.len() != 8 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(hex, 16).ok()
}

/// Whether `name` is a valid annotation identifier or group name: 1-128
/// bytes without control characters.
#[must_use]
pub fn is_valid_annotation_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_ANNOTATION_ID_BYTES && !name.chars().any(char::is_control)
}

fn default_color() -> String {
    DEFAULT_ANNOTATION_COLOR.to_owned()
}

#[cfg(test)]
mod tests {
    use super::{AnnotationSpec, AnnotationStyle, AnnotationTarget, parse_rgba};
    use crate::Rect;

    #[test]
    fn specs_default_their_color_and_style() -> Result<(), serde_json::Error> {
        let spec: AnnotationSpec = serde_json::from_value(serde_json::json!({
            "target": { "kind": "node", "node_id": "save" }
        }))?;
        assert_eq!(spec, AnnotationSpec::node("save"));
        assert_eq!(spec.style, AnnotationStyle::Outline);
        assert_eq!(spec.validate(), Ok(()));
        Ok(())
    }

    #[test]
    fn validation_rejects_each_bound() {
        assert!(
            AnnotationSpec::node("a")
                .with_color("#fff")
                .validate()
                .is_err()
        );
        assert!(
            AnnotationSpec::node("a")
                .with_label("x".repeat(129))
                .validate()
                .is_err()
        );
        assert!(AnnotationSpec::node("a").with_ttl_ms(0).validate().is_err());
        assert!(AnnotationSpec::node("").validate().is_err());
        assert!(AnnotationSpec::node("a").with_id("").validate().is_err());
        assert!(
            AnnotationSpec::rect(Rect {
                x: 0.0,
                y: 0.0,
                width: -1.0,
                height: 1.0,
            })
            .validate()
            .is_err()
        );
        assert!(matches!(
            AnnotationSpec::node("a").target,
            AnnotationTarget::Node { .. }
        ));
    }

    #[test]
    fn colors_parse_as_rgba() {
        assert_eq!(parse_rgba("#00A8FFFF"), Some(0x00A8_FFFF));
        assert_eq!(parse_rgba("00A8FFFF"), None);
        assert_eq!(parse_rgba("#00A8FF"), None);
    }
}
