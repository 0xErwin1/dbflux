//! Pure decision logic for the key-value size gate and the payload decoder
//! override (GitHub issue #354).
//!
//! Kept free of GPUI so the byte-limit formatting, the decode trigger, and
//! the "may I edit this?" predicate stay unit-testable without a window.

use dbflux_core::{
    DecodeOutcome, DecodedPayload, Encoding, KeyGetResult, KeyLoadState, KeyType, ValueRepr,
};
use dbflux_ui_base::AsyncUpdateResultExt;
use gpui::Context;

/// Values at or under this size are decoded on the same task that already
/// fetched them; anything larger is decoded on a fresh background-executor
/// task so a slow decompression never blocks the foreground update.
pub(super) const INLINE_DECODE_THRESHOLD_BYTES: usize = 256 * 1024;

/// The user's choice of how to interpret a `Binary`-repr value's bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(super) enum KvEncodingChoice {
    /// Auto-detect via [`dbflux_core::decode`].
    #[default]
    Auto,
    /// Show the untouched raw bytes; never decoded.
    Raw,
    /// Decode as a specific encoding via [`dbflux_core::decode_as`],
    /// bypassing detection.
    Manual(Encoding),
}

const MANUAL_ENCODINGS: [Encoding; 5] = [
    Encoding::Gzip,
    Encoding::Zstd,
    Encoding::SnappyFrame,
    Encoding::Lz4Frame,
    Encoding::MessagePack,
];

/// Dropdown items for the encoding override control, in index order:
/// `Auto`, `Raw`, then one entry per manual encoding.
pub(super) fn encoding_choice_labels() -> Vec<&'static str> {
    let mut labels = vec!["Auto", "Raw"];
    labels.extend(
        MANUAL_ENCODINGS
            .iter()
            .map(|encoding| encoding_name(*encoding)),
    );
    labels
}

pub(super) fn choice_for_index(index: usize) -> KvEncodingChoice {
    match index {
        1 => KvEncodingChoice::Raw,
        n if n >= 2 && n - 2 < MANUAL_ENCODINGS.len() => {
            KvEncodingChoice::Manual(MANUAL_ENCODINGS[n - 2])
        }
        _ => KvEncodingChoice::Auto,
    }
}

pub(super) fn index_for_choice(choice: KvEncodingChoice) -> usize {
    match choice {
        KvEncodingChoice::Auto => 0,
        KvEncodingChoice::Raw => 1,
        KvEncodingChoice::Manual(encoding) => MANUAL_ENCODINGS
            .iter()
            .position(|candidate| *candidate == encoding)
            .map(|position| position + 2)
            .unwrap_or(0),
    }
}

/// Whether a fetched value is eligible for encoding detection at all.
/// Decoding never runs on a partially-transferred value: a truncated
/// compressed stream would decompress into garbage, not a smaller result.
pub(super) fn should_attempt_decode(repr: ValueRepr, load_state: KeyLoadState) -> bool {
    repr == ValueRepr::Binary && matches!(load_state, KeyLoadState::Loaded)
}

/// Runs the chosen decode strategy against `bytes`, bounding the decoded
/// output at `cap_bytes`. Returns `None` for [`KvEncodingChoice::Raw`],
/// since Raw never decodes.
pub(super) fn compute_decode_outcome(
    bytes: &[u8],
    choice: KvEncodingChoice,
    cap_bytes: usize,
) -> Option<DecodeOutcome> {
    match choice {
        KvEncodingChoice::Raw => None,
        KvEncodingChoice::Auto => Some(dbflux_core::decode(bytes, cap_bytes)),
        KvEncodingChoice::Manual(encoding) => {
            Some(dbflux_core::decode_as(bytes, encoding, cap_bytes))
        }
    }
}

/// Whether the string editor may open for this value.
///
/// The raw `Vec<u8>` in `KeyGetResult::value` is the only thing ever written
/// back on save, so editing must only be allowed when what is on screen is
/// exactly those raw bytes — never a decoded re-interpretation of them, which
/// could silently discard the encoding on save if the detection was wrong.
pub(super) fn may_edit_value(
    key_type: KeyType,
    repr: ValueRepr,
    choice: KvEncodingChoice,
    decode_outcome: Option<&DecodeOutcome>,
) -> bool {
    if !matches!(key_type, KeyType::String | KeyType::Json) {
        return false;
    }

    if repr != ValueRepr::Binary {
        return true;
    }

    match choice {
        KvEncodingChoice::Raw => true,
        KvEncodingChoice::Manual(_) => false,
        KvEncodingChoice::Auto => !matches!(decode_outcome, Some(DecodeOutcome::Decoded(_))),
    }
}

pub(super) fn should_decode_inline(byte_len: usize) -> bool {
    byte_len <= INLINE_DECODE_THRESHOLD_BYTES
}

pub(super) fn encoding_name(encoding: Encoding) -> &'static str {
    match encoding {
        Encoding::Gzip => "gzip",
        Encoding::Zstd => "zstd",
        Encoding::SnappyFrame => "snappy",
        Encoding::Lz4Frame => "lz4",
        Encoding::MessagePack => "msgpack",
        Encoding::Png => "png",
        Encoding::Jpeg => "jpeg",
        Encoding::Gif => "gif",
        Encoding::WebP => "webp",
        Encoding::Bmp => "bmp",
    }
}

/// Label describing what a successful decode produced, for the "gzip → text"
/// style summary shown next to a decoded value.
pub(super) fn decoded_kind_label(payload: &DecodedPayload) -> String {
    match payload {
        DecodedPayload::Text(_) => dbflux_i18n::t!("document.key_value.render.decode.kind.text"),
        DecodedPayload::Bytes(_) => dbflux_i18n::t!("document.key_value.render.decode.kind.bytes"),
        DecodedPayload::PassThrough => {
            dbflux_i18n::t!("document.key_value.render.decode.kind.image")
        }
    }
}

/// Summary label for the current decode outcome, or `None` when nothing was
/// detected (the value is shown as raw bytes/text either way).
pub(super) fn encoding_summary_label(outcome: &DecodeOutcome) -> Option<String> {
    match outcome {
        DecodeOutcome::Decoded(value) => Some(format!(
            "{} → {}",
            encoding_name(value.encoding),
            decoded_kind_label(&value.payload)
        )),
        DecodeOutcome::DetectedButFailed { encoding, .. } => Some(format!(
            "{} ({})",
            encoding_name(*encoding),
            dbflux_i18n::t!("document.key_value.render.decode.failed")
        )),
        DecodeOutcome::TooLarge { encoding, .. } => Some(format!(
            "{} ({})",
            encoding_name(*encoding),
            dbflux_i18n::t!("document.key_value.render.decode.too_large")
        )),
        DecodeOutcome::Undetected => None,
    }
}

/// Preview text for the scalar value panel, honoring the current encoding
/// choice: `Raw` (or a non-`Binary` repr) always shows the untouched value,
/// exactly like [`super::parsing::render_value_preview`]; a successful
/// decode shows the decoded payload instead, per its kind.
pub(super) fn render_value_preview_with_decode(
    value: &KeyGetResult,
    choice: KvEncodingChoice,
    outcome: Option<&DecodeOutcome>,
) -> String {
    if value.repr != ValueRepr::Binary || matches!(choice, KvEncodingChoice::Raw) {
        return super::parsing::render_value_preview(value);
    }

    match outcome {
        Some(DecodeOutcome::Decoded(decoded)) => match &decoded.payload {
            DecodedPayload::Text(text) => super::parsing::truncate_preview_text(text),
            DecodedPayload::Bytes(bytes) => format!("{} bytes (binary)", bytes.len()),
            DecodedPayload::PassThrough => format!(
                "{} bytes (image: {})",
                value.value.len(),
                encoding_name(decoded.encoding)
            ),
        },
        _ => super::parsing::render_value_preview(value),
    }
}

impl super::KeyValueDocument {
    /// Byte cap applied both to the value fetch (the size gate) and to the
    /// decoded output (the decompression bomb guard): the single "key-value
    /// preview size limit" setting serves both purposes.
    pub(super) fn kv_size_limit_bytes(&self, cx: &gpui::App) -> u64 {
        self.app_state
            .read(cx)
            .general_settings()
            .key_value_size_limit_bytes()
    }

    /// Requests the currently selected key again, bypassing the size limit
    /// for this one fetch. One-shot: selecting a different key, or another
    /// plain refresh, goes back to the configured limit.
    pub(super) fn load_selected_value_without_limit(&mut self, cx: &mut Context<Self>) {
        self.kv_load_anyway = true;
        self.reload_selected_value(cx);
    }

    pub(super) fn set_kv_encoding_choice(
        &mut self,
        choice: KvEncodingChoice,
        cx: &mut Context<Self>,
    ) {
        if self.kv_encoding_choice == choice {
            return;
        }

        self.kv_encoding_choice = choice;
        self.kv_encoding_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(index_for_choice(choice)), cx);
        });
        self.recompute_kv_decode_outcome(cx);
        cx.notify();
    }

    /// Resets the encoding choice and decode outcome for a newly-loaded
    /// value, then (re)computes the decode outcome for it.
    pub(super) fn reset_kv_decode_state_for_new_value(&mut self, cx: &mut Context<Self>) {
        self.kv_encoding_choice = KvEncodingChoice::default();
        self.kv_encoding_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(index_for_choice(KvEncodingChoice::default())), cx);
        });
        self.recompute_kv_decode_outcome(cx);
    }

    pub(super) fn recompute_kv_decode_outcome(&mut self, cx: &mut Context<Self>) {
        self.kv_decode_generation = self.kv_decode_generation.wrapping_add(1);
        let generation = self.kv_decode_generation;

        let Some(value) = &self.selected_value else {
            self.kv_decode_outcome = None;
            return;
        };

        if !should_attempt_decode(value.repr, value.load_state) {
            self.kv_decode_outcome = None;
            return;
        }

        let choice = self.kv_encoding_choice;
        if matches!(choice, KvEncodingChoice::Raw) {
            self.kv_decode_outcome = None;
            return;
        }

        let bytes = value.value.clone();
        let cap_bytes = self.kv_size_limit_bytes(cx).min(usize::MAX as u64) as usize;

        if should_decode_inline(bytes.len()) {
            self.kv_decode_outcome = compute_decode_outcome(&bytes, choice, cap_bytes);
            return;
        }

        self.kv_decode_outcome = None;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let outcome = cx
                .background_executor()
                .spawn(async move { compute_decode_outcome(&bytes, choice, cap_bytes) })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if this.kv_decode_generation != generation {
                        return;
                    }
                    this.kv_decode_outcome = outcome;
                    cx.notify();
                });
            })
            .log_if_dropped();
        })
        .detach();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_attempt_decode_only_for_fully_loaded_binary_values() {
        assert!(should_attempt_decode(
            ValueRepr::Binary,
            KeyLoadState::Loaded
        ));
        assert!(!should_attempt_decode(
            ValueRepr::Text,
            KeyLoadState::Loaded
        ));
        assert!(!should_attempt_decode(
            ValueRepr::Json,
            KeyLoadState::Loaded
        ));
        assert!(!should_attempt_decode(
            ValueRepr::Structured,
            KeyLoadState::Loaded
        ));
        assert!(!should_attempt_decode(
            ValueRepr::Binary,
            KeyLoadState::Truncated {
                returned_bytes: 10,
                total_bytes: Some(20)
            }
        ));
        assert!(!should_attempt_decode(
            ValueRepr::Binary,
            KeyLoadState::TooLarge {
                size_bytes: 100,
                limit_bytes: 10
            }
        ));
    }

    #[test]
    fn should_decode_inline_below_or_at_threshold_only() {
        assert!(should_decode_inline(0));
        assert!(should_decode_inline(INLINE_DECODE_THRESHOLD_BYTES));
        assert!(!should_decode_inline(INLINE_DECODE_THRESHOLD_BYTES + 1));
    }

    #[test]
    fn encoding_choice_index_round_trips_every_variant() {
        let choices = [
            KvEncodingChoice::Auto,
            KvEncodingChoice::Raw,
            KvEncodingChoice::Manual(Encoding::Gzip),
            KvEncodingChoice::Manual(Encoding::Zstd),
            KvEncodingChoice::Manual(Encoding::SnappyFrame),
            KvEncodingChoice::Manual(Encoding::Lz4Frame),
            KvEncodingChoice::Manual(Encoding::MessagePack),
        ];

        for choice in choices {
            let index = index_for_choice(choice);
            assert_eq!(choice_for_index(index), choice);
        }
    }

    #[test]
    fn encoding_choice_labels_match_index_count() {
        assert_eq!(encoding_choice_labels().len(), 2 + MANUAL_ENCODINGS.len());
    }

    #[test]
    fn out_of_range_index_falls_back_to_auto() {
        assert_eq!(choice_for_index(999), KvEncodingChoice::Auto);
    }

    #[test]
    fn compute_decode_outcome_raw_never_decodes() {
        let bytes = b"anything";
        assert_eq!(
            compute_decode_outcome(bytes, KvEncodingChoice::Raw, 1024),
            None
        );
    }

    #[test]
    fn compute_decode_outcome_auto_detects_undetected_plain_text() {
        let bytes = b"just plain ordinary text, nothing special about it at all";
        let outcome = compute_decode_outcome(bytes, KvEncodingChoice::Auto, 1024);
        assert_eq!(outcome, Some(DecodeOutcome::Undetected));
    }

    #[test]
    fn compute_decode_outcome_manual_bypasses_detection() {
        // Manual MessagePack against plain text bytes: detection would say
        // Undetected, but decode_as forces the attempt and it fails cleanly.
        let bytes = b"just plain ordinary text, nothing special about it at all";
        let outcome =
            compute_decode_outcome(bytes, KvEncodingChoice::Manual(Encoding::MessagePack), 1024);
        assert!(matches!(
            outcome,
            Some(DecodeOutcome::DetectedButFailed {
                encoding: Encoding::MessagePack,
                ..
            })
        ));
    }

    #[test]
    fn may_edit_value_blocks_non_string_json_key_types() {
        assert!(!may_edit_value(
            KeyType::Hash,
            ValueRepr::Text,
            KvEncodingChoice::Auto,
            None
        ));
        assert!(!may_edit_value(
            KeyType::Bytes,
            ValueRepr::Binary,
            KvEncodingChoice::Raw,
            None
        ));
    }

    #[test]
    fn may_edit_value_allows_string_and_json_when_repr_is_not_binary() {
        assert!(may_edit_value(
            KeyType::String,
            ValueRepr::Text,
            KvEncodingChoice::Auto,
            None
        ));
        assert!(may_edit_value(
            KeyType::Json,
            ValueRepr::Json,
            KvEncodingChoice::Auto,
            None
        ));
    }

    #[test]
    fn may_edit_value_binary_raw_view_is_always_editable() {
        assert!(may_edit_value(
            KeyType::String,
            ValueRepr::Binary,
            KvEncodingChoice::Raw,
            Some(&DecodeOutcome::Decoded(dbflux_core::DecodedValue {
                encoding: Encoding::Gzip,
                payload: DecodedPayload::Text("hello".to_string()),
            }))
        ));
    }

    #[test]
    fn may_edit_value_binary_manual_override_is_never_editable() {
        assert!(!may_edit_value(
            KeyType::String,
            ValueRepr::Binary,
            KvEncodingChoice::Manual(Encoding::Gzip),
            Some(&DecodeOutcome::Undetected)
        ));
    }

    #[test]
    fn may_edit_value_binary_auto_blocks_only_when_actually_decoded() {
        assert!(!may_edit_value(
            KeyType::String,
            ValueRepr::Binary,
            KvEncodingChoice::Auto,
            Some(&DecodeOutcome::Decoded(dbflux_core::DecodedValue {
                encoding: Encoding::Gzip,
                payload: DecodedPayload::Bytes(vec![1, 2, 3]),
            }))
        ));

        assert!(may_edit_value(
            KeyType::String,
            ValueRepr::Binary,
            KvEncodingChoice::Auto,
            Some(&DecodeOutcome::Undetected)
        ));

        assert!(may_edit_value(
            KeyType::String,
            ValueRepr::Binary,
            KvEncodingChoice::Auto,
            Some(&DecodeOutcome::DetectedButFailed {
                encoding: Encoding::Gzip,
                reason: "corrupt".to_string(),
            })
        ));

        assert!(may_edit_value(
            KeyType::String,
            ValueRepr::Binary,
            KvEncodingChoice::Auto,
            None
        ));
    }

    #[test]
    fn encoding_summary_label_is_none_when_undetected() {
        assert_eq!(encoding_summary_label(&DecodeOutcome::Undetected), None);
    }

    fn binary_result(value: Vec<u8>, load_state: KeyLoadState) -> KeyGetResult {
        KeyGetResult {
            entry: dbflux_core::KeyEntry::new("k"),
            value,
            repr: ValueRepr::Binary,
            load_state,
        }
    }

    #[test]
    fn render_value_preview_with_decode_raw_choice_shows_raw_binary_size() {
        let result = binary_result(vec![0u8; 42], KeyLoadState::Loaded);
        let outcome = DecodeOutcome::Decoded(dbflux_core::DecodedValue {
            encoding: Encoding::Gzip,
            payload: DecodedPayload::Text("hello".to_string()),
        });

        assert_eq!(
            render_value_preview_with_decode(&result, KvEncodingChoice::Raw, Some(&outcome)),
            "42 bytes (binary)"
        );
    }

    #[test]
    fn render_value_preview_with_decode_shows_decoded_text() {
        let result = binary_result(vec![0u8; 5], KeyLoadState::Loaded);
        let outcome = DecodeOutcome::Decoded(dbflux_core::DecodedValue {
            encoding: Encoding::Gzip,
            payload: DecodedPayload::Text("hello world".to_string()),
        });

        assert_eq!(
            render_value_preview_with_decode(&result, KvEncodingChoice::Auto, Some(&outcome)),
            "hello world"
        );
    }

    #[test]
    fn render_value_preview_with_decode_falls_back_to_raw_when_undetected() {
        let bytes = vec![1u8, 2, 3];
        let result = binary_result(bytes, KeyLoadState::Loaded);

        assert_eq!(
            render_value_preview_with_decode(
                &result,
                KvEncodingChoice::Auto,
                Some(&DecodeOutcome::Undetected)
            ),
            "3 bytes (binary)"
        );
    }

    #[test]
    fn encoding_summary_label_describes_decoded_text() {
        let outcome = DecodeOutcome::Decoded(dbflux_core::DecodedValue {
            encoding: Encoding::Gzip,
            payload: DecodedPayload::Text("hello".to_string()),
        });

        let label = encoding_summary_label(&outcome).expect("should have a label");
        assert!(label.contains("gzip"));
    }
}
