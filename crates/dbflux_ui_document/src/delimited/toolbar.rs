//! The dialect controls of `DelimitedDocument`: the values each one offers,
//! the selects that show them, and the pane actions that reach them from the
//! keyboard.

use dbflux_components::controls::{Dropdown, DropdownItem};
use dbflux_components::icons::AppIcon;
use dbflux_delimited::{Dialect, Encoding};
use gpui::*;

use super::document::DelimitedDocument;
use crate::pane::PaneAction;

/// The delimiters the delimiter select offers, which are the ones detection
/// chooses between.
pub(super) const DELIMITERS: [u8; 4] = [b',', b'\t', b';', b'|'];

/// The quotes the quote select offers. `None` is a file without quoting.
pub(super) const QUOTES: [Option<u8>; 3] = [Some(b'"'), Some(b'\''), None];

/// The labels of the encodings the encoding select always offers.
/// `encoding_rs` reads ISO-8859-1 as windows-1252, as the WHATWG Encoding
/// Standard does, so those two labels are one entry.
const COMMON_ENCODING_LABELS: [&str; 6] = [
    "utf-8",
    "utf-16le",
    "utf-16be",
    "windows-1252",
    "iso-8859-1",
    "shift_jis",
];

/// The encodings the encoding select offers for a file detected as
/// `detected`: the common ones, then `detected` when it is not one of them.
pub(super) fn encoding_choices(detected: &'static Encoding) -> Vec<&'static Encoding> {
    let mut encodings: Vec<&'static Encoding> = Vec::new();

    let common = COMMON_ENCODING_LABELS
        .iter()
        .filter_map(|label| Encoding::for_label(label.as_bytes()));

    for encoding in common.chain(std::iter::once(detected)) {
        if !encodings.contains(&encoding) {
            encodings.push(encoding);
        }
    }

    encodings
}

/// The three selects of the dialect toolbar. The header flag is a checkbox
/// the document draws from its own state.
pub(super) struct DialectControls {
    pub(super) delimiter: Entity<Dropdown>,
    pub(super) quote: Entity<Dropdown>,
    pub(super) encoding: Entity<Dropdown>,

    /// The encodings of the encoding select, in the order of its items.
    encodings: Vec<&'static Encoding>,
}

impl DialectControls {
    /// Builds the selects of a file detected as `detected`, each showing the
    /// detected value. The item of a detected value says so, which is how a
    /// select tells a detected value from an overridden one.
    pub(super) fn new(detected: &Dialect, cx: &mut App) -> Self {
        let encodings = encoding_choices(detected.encoding);

        let delimiter_items = DELIMITERS
            .iter()
            .map(|delimiter| {
                item(
                    crate::labels::delimited_byte_name(*delimiter),
                    *delimiter == detected.delimiter,
                )
            })
            .collect();

        let quote_items = QUOTES
            .iter()
            .map(|quote| {
                item(
                    crate::labels::delimited_quote_name(*quote),
                    *quote == detected.quote,
                )
            })
            .collect();

        let encoding_items = encodings
            .iter()
            .map(|encoding| item(encoding.name().to_string(), *encoding == detected.encoding))
            .collect();

        let controls = Self {
            delimiter: cx.new(|_cx| Dropdown::new("delimited-delimiter").items(delimiter_items)),
            quote: cx.new(|_cx| Dropdown::new("delimited-quote").items(quote_items)),
            encoding: cx.new(|_cx| Dropdown::new("delimited-encoding").items(encoding_items)),
            encodings,
        };

        controls.show(detected, cx);
        controls
    }

    /// Makes every select show its value of `dialect`. A value the select
    /// does not offer leaves it without a selection.
    pub(super) fn show(&self, dialect: &Dialect, cx: &mut App) {
        let delimiter = DELIMITERS
            .iter()
            .position(|delimiter| *delimiter == dialect.delimiter);
        let quote = QUOTES.iter().position(|quote| *quote == dialect.quote);
        let encoding = self
            .encodings
            .iter()
            .position(|encoding| *encoding == dialect.encoding);

        for (dropdown, index) in [
            (&self.delimiter, delimiter),
            (&self.quote, quote),
            (&self.encoding, encoding),
        ] {
            dropdown.update(cx, |dropdown, cx| dropdown.set_selected_index(index, cx));
        }
    }

    /// The encoding of item `index` of the encoding select.
    pub(super) fn encoding_at(&self, index: usize) -> Option<&'static Encoding> {
        self.encodings.get(index).copied()
    }
}

fn item(name: String, is_detected: bool) -> DropdownItem {
    if is_detected {
        DropdownItem::new(crate::labels::delimited_detected_label(&name))
    } else {
        DropdownItem::new(name)
    }
}

impl DelimitedDocument {
    /// The label of the header checkbox, which says so when the flag is the
    /// one detection resolved.
    pub(super) fn header_label(&self) -> String {
        let label = dbflux_i18n::t!("document.delimited.toolbar.header");

        let is_detected = match (self.requested_dialect(), self.detected_dialect()) {
            (Some(requested), Some(detected)) => requested.has_header == detected.has_header,
            _ => false,
        };

        if is_detected {
            crate::labels::delimited_detected_label(&label)
        } else {
            label
        }
    }

    /// The dialect controls of a loaded file, as entries of the pane actions
    /// menu: each select entry opens its list with the keyboard on it, the
    /// header entry switches the flag, and the reset entry is enabled while
    /// there is an override to drop. The edit controls follow them. Empty
    /// until the file is loaded.
    pub(crate) fn pane_actions(&self, this: &Entity<Self>) -> Vec<PaneAction> {
        let Some(controls) = self.dialect_controls() else {
            return Vec::new();
        };

        let open_select = |id: &'static str, label: String, dropdown: &Entity<Dropdown>| {
            let dropdown = dropdown.downgrade();

            PaneAction::callback(id, label, move |window, cx| {
                if let Some(dropdown) = dropdown.upgrade() {
                    dropdown.update(cx, |dropdown, cx| dropdown.focus_and_open(window, cx));
                }
            })
        };

        let run =
            |id: &'static str,
             label: String,
             run: fn(&mut DelimitedDocument, &mut Context<DelimitedDocument>)| {
                let target = this.downgrade();

                PaneAction::callback(id, label, move |_window, cx| {
                    if let Some(document) = target.upgrade() {
                        document.update(cx, run);
                    }
                })
            };

        let mut actions = vec![
            open_select(
                "delimited-delimiter",
                dbflux_i18n::t!("document.delimited.toolbar.delimiter"),
                &controls.delimiter,
            ),
            open_select(
                "delimited-quote",
                dbflux_i18n::t!("document.delimited.toolbar.quote"),
                &controls.quote,
            ),
            run("delimited-header", self.header_label(), Self::toggle_header),
            open_select(
                "delimited-encoding",
                dbflux_i18n::t!("document.delimited.toolbar.encoding"),
                &controls.encoding,
            ),
            run(
                "delimited-dialect-reset",
                dbflux_i18n::t!("document.delimited.toolbar.reset"),
                Self::reset_dialect,
            )
            .icon(AppIcon::RotateCcw)
            .enabled(self.has_dialect_overrides()),
        ];

        actions.extend(self.edit_pane_actions(this));
        actions
    }
}
