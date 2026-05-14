use std::fmt;

/// Represents keyboard modifiers in a platform-agnostic way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Modifiers {
    pub ctrl: bool,
    pub alt: bool,
    pub shift: bool,
    pub platform: bool,
}

impl Modifiers {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn ctrl() -> Self {
        Self {
            ctrl: true,
            ..Default::default()
        }
    }

    pub fn shift() -> Self {
        Self {
            shift: true,
            ..Default::default()
        }
    }

    pub fn alt() -> Self {
        Self {
            alt: true,
            ..Default::default()
        }
    }

    pub fn ctrl_shift() -> Self {
        Self {
            ctrl: true,
            shift: true,
            ..Default::default()
        }
    }

    /// Platform-aware "primary" modifier: Cmd on macOS, Ctrl elsewhere.
    ///
    /// Use this for application-level commands (palette, save, copy, new tab,
    /// run query, …) where the convention is Cmd on macOS but Ctrl everywhere
    /// else. For vim-style navigation (Ctrl+hjkl, Ctrl+u/d) and bindings that
    /// would clash with macOS system shortcuts (Cmd+M, Cmd+Shift+3/4), keep
    /// using [`Modifiers::ctrl`] / [`Modifiers::ctrl_shift`] instead.
    pub fn primary() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self {
                platform: true,
                ..Default::default()
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            Self::ctrl()
        }
    }

    /// Primary + Shift: Cmd+Shift on macOS, Ctrl+Shift elsewhere.
    pub fn primary_shift() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self {
                platform: true,
                shift: true,
                ..Default::default()
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            Self::ctrl_shift()
        }
    }

    #[allow(dead_code)]
    pub fn has_any(&self) -> bool {
        self.ctrl || self.alt || self.shift || self.platform
    }
}

/// A normalized key chord (key + modifiers) for keybinding matching.
///
/// Key names are normalized to lowercase, and platform-specific differences
/// (Cmd vs Ctrl) are abstracted away.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyChord {
    pub key: String,
    pub modifiers: Modifiers,
}

impl KeyChord {
    pub fn new(key: impl Into<String>, modifiers: Modifiers) -> Self {
        Self {
            key: Self::normalize_key(&key.into()),
            modifiers,
        }
    }

    /// Parses a key chord from a string like "Ctrl+Shift+P" or "j".
    #[allow(dead_code)]
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let mut modifiers = Modifiers::default();
        let parts: Vec<&str> = s.split('+').collect();

        if parts.is_empty() {
            return Err(ParseError::Empty);
        }

        let key_part = parts.last().ok_or(ParseError::Empty)?;

        for part in &parts[..parts.len() - 1] {
            match part.to_lowercase().as_str() {
                "ctrl" | "control" => modifiers.ctrl = true,
                "alt" => modifiers.alt = true,
                "shift" => modifiers.shift = true,
                "cmd" | "command" | "platform" | "super" => modifiers.platform = true,
                _ => return Err(ParseError::InvalidModifier(part.to_string())),
            }
        }

        let key = Self::normalize_key(key_part);
        if key.is_empty() {
            return Err(ParseError::Empty);
        }

        Ok(Self { key, modifiers })
    }

    fn normalize_key(key: &str) -> String {
        let lower = key.to_lowercase();

        match lower.as_str() {
            "arrowdown" | "down" => "down".to_string(),
            "arrowup" | "up" => "up".to_string(),
            "arrowleft" | "left" => "left".to_string(),
            "arrowright" | "right" => "right".to_string(),
            "enter" | "return" => "enter".to_string(),
            "escape" | "esc" => "escape".to_string(),
            "backspace" => "backspace".to_string(),
            "delete" | "del" => "delete".to_string(),
            "tab" => "tab".to_string(),
            "space" | " " => "space".to_string(),
            "home" => "home".to_string(),
            "end" => "end".to_string(),
            "pageup" => "pageup".to_string(),
            "pagedown" => "pagedown".to_string(),
            _ => lower,
        }
    }

    /// Returns true if this chord has the Ctrl or Platform (Cmd) modifier.
    #[allow(dead_code)]
    pub fn has_ctrl_or_cmd(&self) -> bool {
        self.modifiers.ctrl || self.modifiers.platform
    }
}

impl fmt::Display for KeyChord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Order: Cmd, Ctrl, Alt, Shift — matches the convention used in
        // user-facing docs (e.g. "Cmd+Shift+P", "Ctrl+Shift+P").
        let mut parts = Vec::new();

        if self.modifiers.platform {
            parts.push("Cmd");
        }
        if self.modifiers.ctrl {
            parts.push("Ctrl");
        }
        if self.modifiers.alt {
            parts.push("Alt");
        }
        if self.modifiers.shift {
            parts.push("Shift");
        }

        let key_display = match self.key.as_str() {
            "down" => "Down",
            "up" => "Up",
            "left" => "Left",
            "right" => "Right",
            "enter" => "Enter",
            "escape" => "Escape",
            "backspace" => "Backspace",
            "delete" => "Delete",
            "tab" => "Tab",
            "space" => "Space",
            "home" => "Home",
            "end" => "End",
            "pageup" => "PageUp",
            "pagedown" => "PageDown",
            _ => &self.key,
        };

        parts.push(key_display);
        write!(f, "{}", parts.join("+"))
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    InvalidModifier(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Empty => write!(f, "empty key chord"),
            ParseError::InvalidModifier(m) => write!(f, "invalid modifier: {}", m),
        }
    }
}

impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_key() {
        let chord = KeyChord::parse("j").unwrap();
        assert_eq!(chord.key, "j");
        assert!(!chord.modifiers.has_any());
    }

    #[test]
    fn test_parse_with_modifiers() {
        let chord = KeyChord::parse("Ctrl+Shift+P").unwrap();
        assert_eq!(chord.key, "p");
        assert!(chord.modifiers.ctrl);
        assert!(chord.modifiers.shift);
        assert!(!chord.modifiers.alt);
    }

    #[test]
    fn test_normalize_arrow_keys() {
        let chord1 = KeyChord::parse("ArrowDown").unwrap();
        let chord2 = KeyChord::parse("down").unwrap();
        assert_eq!(chord1.key, chord2.key);
    }

    #[test]
    fn test_display() {
        let chord = KeyChord::new("p", Modifiers::ctrl_shift());
        assert_eq!(chord.to_string(), "Ctrl+Shift+p");
    }

    #[test]
    fn test_display_cmd_first() {
        let chord = KeyChord {
            key: "p".to_string(),
            modifiers: Modifiers {
                platform: true,
                shift: true,
                ..Modifiers::none()
            },
        };
        assert_eq!(chord.to_string(), "Cmd+Shift+p");
    }

    #[test]
    fn test_primary_modifier_per_platform() {
        let m = Modifiers::primary();
        #[cfg(target_os = "macos")]
        {
            assert!(m.platform);
            assert!(!m.ctrl);
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert!(m.ctrl);
            assert!(!m.platform);
        }
    }
}
