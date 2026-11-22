//! A user-chosen accent color, as stored in the general settings.

use serde::{Deserialize, Serialize};

/// An opaque `#RRGGBB` color. The only way to build one from text is
/// [`AccentColor::parse`], so a stored value is always a valid color.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AccentColor(u32);

impl AccentColor {
    /// The color `0xRRGGBB`.
    pub fn from_rgb(rgb: u32) -> Self {
        Self(rgb & 0xFF_FFFF)
    }

    /// The color as `0xRRGGBB`.
    pub fn rgb(self) -> u32 {
        self.0
    }

    /// Parses exactly `#RRGGBB` (case-insensitive), surrounding whitespace
    /// aside. Shorthand (`#RGB`), a missing `#` and alpha channels are rejected.
    pub fn parse(text: &str) -> Option<Self> {
        let digits = text.trim().strip_prefix('#')?;

        if digits.len() != 6 || !digits.chars().all(|digit| digit.is_ascii_hexdigit()) {
            return None;
        }

        u32::from_str_radix(digits, 16).ok().map(Self)
    }

    /// The canonical text form: `#` and six uppercase hex digits.
    pub fn to_hex(self) -> String {
        format!("#{:06X}", self.0)
    }
}

impl From<AccentColor> for String {
    fn from(color: AccentColor) -> Self {
        color.to_hex()
    }
}

impl TryFrom<String> for AccentColor {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text).ok_or_else(|| format!("`{text}` is not a color as #RRGGBB"))
    }
}

#[cfg(test)]
mod tests {
    use super::AccentColor;

    #[test]
    fn parses_only_a_full_hash_prefixed_hex_color() {
        assert_eq!(
            AccentColor::parse("#2F7D6D"),
            Some(AccentColor::from_rgb(0x2F7D6D))
        );
        assert_eq!(
            AccentColor::parse(" #2f7d6d "),
            Some(AccentColor::from_rgb(0x2F7D6D))
        );

        for rejected in [
            "",
            "#",
            "2F7D6D",
            "#2F7D6",
            "#2F7D6DD",
            "#FFF",
            "#GG7D6D",
            "#2F7D6D80",
            "##2F7D6D",
            "#+F7D6D",
        ] {
            assert_eq!(AccentColor::parse(rejected), None, "{rejected:?}");
        }
    }

    #[test]
    fn the_canonical_form_is_uppercase_and_round_trips() {
        let color = AccentColor::parse("#0a0b0c").expect("valid");

        assert_eq!(color.to_hex(), "#0A0B0C");
        assert_eq!(AccentColor::parse(&color.to_hex()), Some(color));
        assert_eq!(AccentColor::from_rgb(0x000001).to_hex(), "#000001");
    }

    #[test]
    fn serde_uses_the_hex_text_and_rejects_invalid_text() {
        let color = AccentColor::from_rgb(0x702963);

        assert_eq!(
            serde_json::to_string(&color).expect("serializes"),
            "\"#702963\""
        );
        assert_eq!(
            serde_json::from_str::<AccentColor>("\"#702963\"").expect("deserializes"),
            color
        );
        assert!(serde_json::from_str::<AccentColor>("\"purple\"").is_err());
    }
}
