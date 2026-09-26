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

    /// Text form used to persist a chord: the lowercase modifier names in a
    /// fixed order, each followed by `+`, then the key (`ctrl+shift+p`).
    ///
    /// The key is always the last segment, so keys such as `+` or `-`
    /// survive the round trip through [`KeyChord::from_storage_string`].
    pub fn to_storage_string(&self) -> String {
        let mut text = String::new();

        for (enabled, name) in [
            (self.modifiers.platform, "cmd"),
            (self.modifiers.ctrl, "ctrl"),
            (self.modifiers.alt, "alt"),
            (self.modifiers.shift, "shift"),
        ] {
            if enabled {
                text.push_str(name);
                text.push('+');
            }
        }

        text.push_str(&self.key);
        text
    }

    /// Parses the text produced by [`KeyChord::to_storage_string`].
    pub fn from_storage_string(text: &str) -> Result<Self, ParseError> {
        let mut modifiers = Modifiers::default();
        let mut rest = text;

        loop {
            let (flag, remainder) = if let Some(remainder) = rest.strip_prefix("cmd+") {
                (&mut modifiers.platform, remainder)
            } else if let Some(remainder) = rest.strip_prefix("ctrl+") {
                (&mut modifiers.ctrl, remainder)
            } else if let Some(remainder) = rest.strip_prefix("alt+") {
                (&mut modifiers.alt, remainder)
            } else if let Some(remainder) = rest.strip_prefix("shift+") {
                (&mut modifiers.shift, remainder)
            } else {
                break;
            };

            if remainder.is_empty() {
                break;
            }

            *flag = true;
            rest = remainder;
        }

        if rest.is_empty() {
            return Err(ParseError::Empty);
        }

        Ok(Self::new(rest, modifiers))
    }

    /// Text form of a key sequence: the storage form of each chord, separated
    /// by single spaces (`ctrl+k ctrl+s`). A chord's storage form never holds
    /// a space, because the space key is named `space`.
    pub fn sequence_to_storage_string(chords: &[KeyChord]) -> String {
        chords
            .iter()
            .map(KeyChord::to_storage_string)
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Parses the text produced by [`KeyChord::sequence_to_storage_string`].
    pub fn sequence_from_storage_string(text: &str) -> Result<Vec<Self>, ParseError> {
        if text.is_empty() {
            return Err(ParseError::Empty);
        }

        text.split(' ').map(Self::from_storage_string).collect()
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

/// The longest key sequence a binding may hold.
pub const MAX_SEQUENCE_LENGTH: usize = 4;

/// The keys of one binding: one chord, or several pressed one after the
/// other (`g g`, `Ctrl+K Ctrl+S`). Never empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeySequence(Vec<KeyChord>);

impl KeySequence {
    /// A sequence of `chords`, or `None` when there are none.
    pub fn new(chords: Vec<KeyChord>) -> Option<Self> {
        (!chords.is_empty()).then_some(Self(chords))
    }

    pub fn chords(&self) -> &[KeyChord] {
        &self.0
    }

    /// The first chord, the one the sequence starts with.
    pub fn first(&self) -> &KeyChord {
        &self.0[0]
    }

    /// Number of chords; a sequence always has at least one.
    pub fn chord_count(&self) -> usize {
        self.0.len()
    }

    /// Whether the sequence is a single chord.
    pub fn is_single(&self) -> bool {
        self.0.len() == 1
    }

    /// Whether `self` is a strict prefix of `other`: pressing `self` leaves
    /// the keyboard waiting to see whether `other` follows.
    pub fn is_prefix_of(&self, other: &KeySequence) -> bool {
        self.0.len() < other.0.len() && other.0.starts_with(&self.0)
    }

    /// Parses chords in [`KeyChord::parse`] form separated by whitespace
    /// (`Ctrl+K Ctrl+S`, `g g`).
    pub fn parse(text: &str) -> Result<Self, ParseError> {
        let chords = text
            .split_whitespace()
            .map(KeyChord::parse)
            .collect::<Result<Vec<_>, _>>()?;

        Self::new(chords).ok_or(ParseError::Empty)
    }

    /// Text form used to persist the sequence, see
    /// [`KeyChord::sequence_to_storage_string`].
    pub fn to_storage_string(&self) -> String {
        KeyChord::sequence_to_storage_string(&self.0)
    }

    /// Parses the text produced by [`KeySequence::to_storage_string`].
    pub fn from_storage_string(text: &str) -> Result<Self, ParseError> {
        let chords = KeyChord::sequence_from_storage_string(text)?;
        Self::new(chords).ok_or(ParseError::Empty)
    }
}

impl From<KeyChord> for KeySequence {
    fn from(chord: KeyChord) -> Self {
        Self(vec![chord])
    }
}

impl fmt::Display for KeySequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.0.iter().map(KeyChord::to_string).collect();
        write!(f, "{}", parts.join(" "))
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
    fn storage_string_round_trips_modifiers_and_symbol_keys() {
        let chords = [
            KeyChord::new("p", Modifiers::ctrl_shift()),
            KeyChord::new("+", Modifiers::ctrl()),
            KeyChord::new("-", Modifiers::none()),
            KeyChord::new(
                "enter",
                Modifiers {
                    platform: true,
                    alt: true,
                    ..Modifiers::none()
                },
            ),
        ];

        for chord in chords {
            let text = chord.to_storage_string();
            assert_eq!(KeyChord::from_storage_string(&text), Ok(chord));
        }

        assert_eq!(
            KeyChord::new("p", Modifiers::ctrl_shift()).to_storage_string(),
            "ctrl+shift+p"
        );
        assert_eq!(KeyChord::from_storage_string(""), Err(ParseError::Empty));
    }

    #[test]
    fn storage_sequences_round_trip() {
        let sequence = vec![
            KeyChord::new("k", Modifiers::ctrl()),
            KeyChord::new("space", Modifiers::none()),
            KeyChord::new("+", Modifiers::shift()),
        ];

        let text = KeyChord::sequence_to_storage_string(&sequence);
        assert_eq!(text, "ctrl+k space shift++");
        assert_eq!(KeyChord::sequence_from_storage_string(&text), Ok(sequence));

        assert_eq!(
            KeyChord::sequence_from_storage_string("g"),
            Ok(vec![KeyChord::new("g", Modifiers::none())])
        );
        assert_eq!(
            KeyChord::sequence_from_storage_string(""),
            Err(ParseError::Empty)
        );
        assert_eq!(
            KeyChord::sequence_from_storage_string("ctrl+k  g"),
            Err(ParseError::Empty)
        );
    }

    #[test]
    fn key_sequences_parse_display_and_round_trip() {
        let sequence = KeySequence::parse("Ctrl+K  ctrl+s").expect("valid sequence");
        assert_eq!(sequence.chord_count(), 2);
        assert_eq!(sequence.to_string(), "Ctrl+k Ctrl+s");
        assert_eq!(sequence.to_storage_string(), "ctrl+k ctrl+s");
        assert_eq!(
            KeySequence::from_storage_string("ctrl+k ctrl+s"),
            Ok(sequence.clone())
        );

        let single = KeySequence::from(KeyChord::new("g", Modifiers::none()));
        assert!(single.is_single());
        assert_eq!(KeySequence::parse(""), Err(ParseError::Empty));
        assert_eq!(KeySequence::new(Vec::new()), None);
    }

    #[test]
    fn prefix_is_strict() {
        let g = KeySequence::parse("g").expect("valid");
        let g_g = KeySequence::parse("g g").expect("valid");
        let g_h = KeySequence::parse("g h").expect("valid");

        assert!(g.is_prefix_of(&g_g));
        assert!(!g_g.is_prefix_of(&g_g));
        assert!(!g_g.is_prefix_of(&g));
        assert!(!g_h.is_prefix_of(&g_g));
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
