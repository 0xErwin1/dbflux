use std::io::Read;

use serde::{Deserialize, Serialize};

/// A payload encoding that [`detect`] and [`decode`] can recognize.
///
/// Compression formats are recognized by magic bytes; [`Encoding::MessagePack`]
/// has no magic and is recognized structurally (see [`probe_message_pack`]);
/// image formats are recognized by magic bytes but are never decoded, since
/// their raw bytes are already the display-ready representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Encoding {
    Gzip,
    Zstd,
    SnappyFrame,
    Lz4Frame,
    MessagePack,
    Png,
    Jpeg,
    Gif,
    WebP,
    Bmp,
}

impl Encoding {
    /// Whether this encoding is a raster image format.
    ///
    /// Image payloads are never decompressed: their raw bytes are the
    /// display-ready representation, so [`decode`] and [`decode_as`] return
    /// [`DecodedPayload::PassThrough`] for them instead of attempting a
    /// raster decode.
    pub fn is_image(self) -> bool {
        matches!(
            self,
            Encoding::Png | Encoding::Jpeg | Encoding::Gif | Encoding::WebP | Encoding::Bmp
        )
    }
}

/// The decoded representation of a payload, kept separate from the raw input
/// bytes so a caller can never lose the original by acting on this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedPayload {
    /// Decompressed bytes (gzip, zstd, snappy, lz4).
    Bytes(Vec<u8>),
    /// A human-readable text rendering (MessagePack, pretty-printed as JSON).
    Text(String),
    /// The encoding was identified but carries no separate decoded form;
    /// the raw input bytes already are the display-ready representation
    /// (raster image formats).
    PassThrough,
}

/// A successfully identified and, where applicable, decoded payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedValue {
    pub encoding: Encoding,
    pub payload: DecodedPayload,
}

/// Outcome of attempting to detect and decode a raw byte payload.
///
/// This type never carries or mutates the caller's original bytes: it only
/// ever adds a detected encoding and, on success, a decoded representation.
/// A wrong guess can therefore never cause data loss on save-back, since the
/// caller keeps and saves back its own untouched input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeOutcome {
    /// An encoding was identified and decoded within the output bound.
    Decoded(DecodedValue),
    /// An encoding was identified (by magic bytes) but decoding it failed,
    /// for example because the bytes after the magic are corrupt.
    DetectedButFailed { encoding: Encoding, reason: String },
    /// An encoding was identified but its decoded size exceeds
    /// `max_output_bytes`; decoding was aborted before allocating it.
    TooLarge {
        encoding: Encoding,
        limit_bytes: usize,
    },
    /// No supported encoding was recognized; treat the payload as raw bytes.
    Undetected,
}

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4d, 0x18];
const SNAPPY_FRAME_MAGIC: [u8; 10] = [0xff, 0x06, 0x00, 0x00, b's', b'N', b'a', b'P', b'p', b'Y'];
const PNG_MAGIC: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
const JPEG_MAGIC: [u8; 3] = [0xff, 0xd8, 0xff];
const BMP_MAGIC: [u8; 2] = [b'B', b'M'];
const GIF87A_MAGIC: [u8; 6] = *b"GIF87a";
const GIF89A_MAGIC: [u8; 6] = *b"GIF89a";

/// The shortest buffer detection ever attempts to classify.
///
/// Below this length, single bytes are almost always valid one-byte
/// MessagePack values (fixints, nil, booleans), which would make the
/// structural probe a false-positive magnet on trivially short payloads.
const MIN_DETECTABLE_LEN: usize = 4;

/// Detects the encoding of `bytes` by magic bytes first, then by the
/// MessagePack structural probe. Returns `None` when nothing matches, in
/// which case the caller should treat the payload as raw/undetected.
pub fn detect(bytes: &[u8]) -> Option<Encoding> {
    if bytes.len() < MIN_DETECTABLE_LEN {
        return None;
    }

    if bytes.starts_with(&GZIP_MAGIC) {
        return Some(Encoding::Gzip);
    }

    if bytes.starts_with(&ZSTD_MAGIC) {
        return Some(Encoding::Zstd);
    }

    if bytes.starts_with(&SNAPPY_FRAME_MAGIC) {
        return Some(Encoding::SnappyFrame);
    }

    if bytes.starts_with(&LZ4_FRAME_MAGIC) {
        return Some(Encoding::Lz4Frame);
    }

    if bytes.starts_with(&PNG_MAGIC) {
        return Some(Encoding::Png);
    }

    if bytes.starts_with(&JPEG_MAGIC) {
        return Some(Encoding::Jpeg);
    }

    if bytes.starts_with(&GIF87A_MAGIC) || bytes.starts_with(&GIF89A_MAGIC) {
        return Some(Encoding::Gif);
    }

    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some(Encoding::WebP);
    }

    if bytes.starts_with(&BMP_MAGIC) {
        return Some(Encoding::Bmp);
    }

    if probe_message_pack(bytes) {
        return Some(Encoding::MessagePack);
    }

    None
}

/// Detects the encoding of `bytes` and decodes it, bounding the decoded
/// output at `max_output_bytes`.
///
/// Detection order is magic bytes, then the MessagePack structural probe,
/// then [`DecodeOutcome::Undetected`]. The input `bytes` are never
/// consumed or mutated; only a detected/decoded view of them is returned.
pub fn decode(bytes: &[u8], max_output_bytes: usize) -> DecodeOutcome {
    match detect(bytes) {
        Some(encoding) => decode_as(bytes, encoding, max_output_bytes),
        None => DecodeOutcome::Undetected,
    }
}

/// Decodes `bytes` as the caller-chosen `encoding`, bypassing detection.
///
/// Used when a caller (for example the UI, on the user's explicit request)
/// overrides the auto-detected encoding. Decoding still never mutates or
/// consumes the original `bytes`, and still honors `max_output_bytes`.
pub fn decode_as(bytes: &[u8], encoding: Encoding, max_output_bytes: usize) -> DecodeOutcome {
    if encoding.is_image() {
        return DecodeOutcome::Decoded(DecodedValue {
            encoding,
            payload: DecodedPayload::PassThrough,
        });
    }

    match encoding {
        Encoding::Gzip => decode_bounded(bytes, max_output_bytes, encoding, |reader, limit| {
            decompress_gzip(reader, limit)
        }),
        Encoding::Zstd => decode_bounded(bytes, max_output_bytes, encoding, |reader, limit| {
            decompress_zstd(reader, limit)
        }),
        Encoding::SnappyFrame => {
            decode_bounded(bytes, max_output_bytes, encoding, |reader, limit| {
                decompress_snappy_frame(reader, limit)
            })
        }
        Encoding::Lz4Frame => decode_bounded(bytes, max_output_bytes, encoding, |reader, limit| {
            decompress_lz4_frame(reader, limit)
        }),
        Encoding::MessagePack => decode_message_pack(bytes, max_output_bytes),
        Encoding::Png | Encoding::Jpeg | Encoding::Gif | Encoding::WebP | Encoding::Bmp => {
            unreachable!("image encodings are handled above via is_image()")
        }
    }
}

/// Runs a bounded decompressor and converts its outcome into a
/// [`DecodeOutcome`], keeping the size-limit and error-mapping logic in one
/// place for every compression format.
fn decode_bounded(
    bytes: &[u8],
    max_output_bytes: usize,
    encoding: Encoding,
    decompress: impl FnOnce(&[u8], usize) -> Result<Vec<u8>, BoundedDecompressError>,
) -> DecodeOutcome {
    match decompress(bytes, max_output_bytes) {
        Ok(decoded) => DecodeOutcome::Decoded(DecodedValue {
            encoding,
            payload: DecodedPayload::Bytes(decoded),
        }),
        Err(BoundedDecompressError::TooLarge) => DecodeOutcome::TooLarge {
            encoding,
            limit_bytes: max_output_bytes,
        },
        Err(BoundedDecompressError::Corrupt(reason)) => {
            DecodeOutcome::DetectedButFailed { encoding, reason }
        }
    }
}

enum BoundedDecompressError {
    TooLarge,
    Corrupt(String),
}

/// Reads at most `limit + 1` bytes from `reader` into a `Vec`, reporting
/// [`BoundedDecompressError::TooLarge`] as soon as the extra byte is seen
/// instead of allocating the full (possibly zip-bomb) decompressed size.
fn read_bounded(mut reader: impl Read, limit: usize) -> Result<Vec<u8>, BoundedDecompressError> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 64 * 1024];

    loop {
        let read = reader
            .read(&mut chunk)
            .map_err(|err| BoundedDecompressError::Corrupt(err.to_string()))?;

        if read == 0 {
            break;
        }

        buffer.extend_from_slice(&chunk[..read]);

        if buffer.len() > limit {
            return Err(BoundedDecompressError::TooLarge);
        }
    }

    Ok(buffer)
}

fn decompress_gzip(bytes: &[u8], limit: usize) -> Result<Vec<u8>, BoundedDecompressError> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    read_bounded(decoder, limit)
}

fn decompress_zstd(bytes: &[u8], limit: usize) -> Result<Vec<u8>, BoundedDecompressError> {
    let decoder = zstd::stream::read::Decoder::new(bytes)
        .map_err(|err| BoundedDecompressError::Corrupt(err.to_string()))?;
    read_bounded(decoder, limit)
}

fn decompress_snappy_frame(bytes: &[u8], limit: usize) -> Result<Vec<u8>, BoundedDecompressError> {
    let reader = snap::read::FrameDecoder::new(bytes);
    read_bounded(reader, limit)
}

fn decompress_lz4_frame(bytes: &[u8], limit: usize) -> Result<Vec<u8>, BoundedDecompressError> {
    let reader = lz4_flex::frame::FrameDecoder::new(bytes);
    read_bounded(reader, limit)
}

/// Attempts a bounded MessagePack parse of the entire buffer, accepting the
/// probe only when a single value is decoded and no bytes are left over.
/// This full-consumption requirement is what keeps the structural probe
/// from producing false positives on arbitrary binary data, since
/// MessagePack has no magic bytes of its own.
pub fn probe_message_pack(bytes: &[u8]) -> bool {
    parse_message_pack(bytes).is_some()
}

fn parse_message_pack(bytes: &[u8]) -> Option<serde_json::Value> {
    if bytes.is_empty() {
        return None;
    }

    let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(bytes));
    let value: serde_json::Value = Deserialize::deserialize(&mut deserializer).ok()?;

    if deserializer.position() as usize != bytes.len() {
        return None;
    }

    Some(value)
}

fn decode_message_pack(bytes: &[u8], max_output_bytes: usize) -> DecodeOutcome {
    let Some(value) = parse_message_pack(bytes) else {
        return DecodeOutcome::DetectedButFailed {
            encoding: Encoding::MessagePack,
            reason: "MessagePack buffer did not parse as a single, fully-consumed value"
                .to_string(),
        };
    };

    match serde_json::to_string_pretty(&value) {
        Ok(text) if text.len() > max_output_bytes => DecodeOutcome::TooLarge {
            encoding: Encoding::MessagePack,
            limit_bytes: max_output_bytes,
        },
        Ok(text) => DecodeOutcome::Decoded(DecodedValue {
            encoding: Encoding::MessagePack,
            payload: DecodedPayload::Text(text),
        }),
        Err(err) => DecodeOutcome::DetectedButFailed {
            encoding: Encoding::MessagePack,
            reason: err.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    fn zstd_compress(data: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(data, 0).expect("zstd encode")
    }

    fn snappy_frame_compress(data: &[u8]) -> Vec<u8> {
        use std::io::Write;

        let mut writer = snap::write::FrameEncoder::new(Vec::new());
        writer.write_all(data).expect("snappy write");
        writer.into_inner().expect("snappy finish")
    }

    fn lz4_frame_compress(data: &[u8]) -> Vec<u8> {
        use std::io::Write;

        let mut writer = lz4_flex::frame::FrameEncoder::new(Vec::new());
        writer.write_all(data).expect("lz4 write");
        writer.finish().expect("lz4 finish")
    }

    fn msgpack_encode(value: &serde_json::Value) -> Vec<u8> {
        let mut buffer = Vec::new();
        value
            .serialize(&mut rmp_serde::Serializer::new(&mut buffer))
            .expect("msgpack encode");
        buffer
    }

    const PAYLOAD: &[u8] = b"the quick brown fox jumps over the lazy dog, repeated for compressibility, the quick brown fox jumps over the lazy dog";

    #[test]
    fn gzip_round_trips() {
        let compressed = gzip_compress(PAYLOAD);

        assert_eq!(detect(&compressed), Some(Encoding::Gzip));

        match decode(&compressed, 1024 * 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::Gzip,
                payload: DecodedPayload::Bytes(decoded),
            }) => assert_eq!(decoded, PAYLOAD),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn zstd_round_trips() {
        let compressed = zstd_compress(PAYLOAD);

        assert_eq!(detect(&compressed), Some(Encoding::Zstd));

        match decode(&compressed, 1024 * 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::Zstd,
                payload: DecodedPayload::Bytes(decoded),
            }) => assert_eq!(decoded, PAYLOAD),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn snappy_frame_round_trips() {
        let compressed = snappy_frame_compress(PAYLOAD);

        assert_eq!(detect(&compressed), Some(Encoding::SnappyFrame));

        match decode(&compressed, 1024 * 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::SnappyFrame,
                payload: DecodedPayload::Bytes(decoded),
            }) => assert_eq!(decoded, PAYLOAD),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn lz4_frame_round_trips() {
        let compressed = lz4_frame_compress(PAYLOAD);

        assert_eq!(detect(&compressed), Some(Encoding::Lz4Frame));

        match decode(&compressed, 1024 * 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::Lz4Frame,
                payload: DecodedPayload::Bytes(decoded),
            }) => assert_eq!(decoded, PAYLOAD),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn messagepack_probe_accepts_real_buffer_and_decodes_to_json() {
        let value = serde_json::json!({ "name": "dbflux", "count": 3, "tags": ["a", "b"] });
        let encoded = msgpack_encode(&value);

        assert_eq!(detect(&encoded), Some(Encoding::MessagePack));

        match decode(&encoded, 1024 * 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::MessagePack,
                payload: DecodedPayload::Text(text),
            }) => {
                let round_tripped: serde_json::Value =
                    serde_json::from_str(&text).expect("valid JSON output");
                assert_eq!(round_tripped, value);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn messagepack_probe_rejects_plain_utf8_text() {
        let text = b"just some ordinary utf-8 text, nothing binary about it at all";

        assert!(!probe_message_pack(text));
        assert_eq!(detect(text), None);
    }

    #[test]
    fn messagepack_probe_rejects_random_binary() {
        // 0x00 alone is a valid one-byte msgpack fixint, so use bytes whose
        // leading byte demands trailing length bytes that are not present,
        // which reliably fails a full-consumption parse.
        let random = [0xc1_u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

        assert!(!probe_message_pack(&random));
        assert_eq!(detect(&random), None);
    }

    #[test]
    fn max_output_bytes_caps_decompression() {
        let compressed = gzip_compress(PAYLOAD);

        match decode(&compressed, 4) {
            DecodeOutcome::TooLarge {
                encoding: Encoding::Gzip,
                limit_bytes: 4,
            } => {}
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn corrupt_data_after_valid_magic_is_detected_but_failed_not_a_panic() {
        let mut corrupt = gzip_compress(PAYLOAD);
        // Keep the two-byte gzip magic intact but corrupt the header/body
        // that follows it, so detection succeeds but decoding must not.
        for byte in corrupt.iter_mut().skip(2) {
            *byte ^= 0xff;
        }

        assert_eq!(detect(&corrupt), Some(Encoding::Gzip));

        match decode(&corrupt, 1024 * 1024) {
            DecodeOutcome::DetectedButFailed {
                encoding: Encoding::Gzip,
                ..
            } => {}
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn image_magics_detect_without_decoding() {
        let mut png = PNG_MAGIC.to_vec();
        png.extend_from_slice(b"not a real png body, but that is fine, we never decode it");

        assert_eq!(detect(&png), Some(Encoding::Png));

        match decode(&png, 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::Png,
                payload: DecodedPayload::PassThrough,
            }) => {}
            other => panic!("unexpected outcome: {other:?}"),
        }

        let mut jpeg = JPEG_MAGIC.to_vec();
        jpeg.extend_from_slice(&[0x00, 0x01, 0x02]);
        assert_eq!(detect(&jpeg), Some(Encoding::Jpeg));

        let mut gif = GIF89A_MAGIC.to_vec();
        gif.extend_from_slice(&[0x00, 0x01, 0x02]);
        assert_eq!(detect(&gif), Some(Encoding::Gif));

        let mut webp = Vec::new();
        webp.extend_from_slice(b"RIFF");
        webp.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        webp.extend_from_slice(b"WEBP");
        assert_eq!(detect(&webp), Some(Encoding::WebP));

        let mut bmp = BMP_MAGIC.to_vec();
        bmp.extend_from_slice(&[0x00, 0x01, 0x02, 0x03]);
        assert_eq!(detect(&bmp), Some(Encoding::Bmp));
    }

    #[test]
    fn empty_and_short_input_are_undetected_raw() {
        assert_eq!(detect(&[]), None);
        assert!(matches!(decode(&[], 1024), DecodeOutcome::Undetected));

        let short = [0x1f_u8, 0x8b_u8][..1].to_vec();
        assert_eq!(detect(&short), None);
        assert!(matches!(decode(&short, 1024), DecodeOutcome::Undetected));
    }

    #[test]
    fn ambiguous_plain_text_falls_through_to_raw() {
        let text = b"just a plain ordinary string value stored in the database, nothing more";

        assert_eq!(detect(text), None);
        assert!(matches!(decode(text, 1024), DecodeOutcome::Undetected));
    }

    #[test]
    fn decode_as_overrides_detection() {
        let compressed = gzip_compress(PAYLOAD);

        match decode_as(&compressed, Encoding::Gzip, 1024 * 1024) {
            DecodeOutcome::Decoded(DecodedValue {
                encoding: Encoding::Gzip,
                payload: DecodedPayload::Bytes(decoded),
            }) => assert_eq!(decoded, PAYLOAD),
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
}
