use super::*;
use std::io::Cursor;

fn encode(text: &str, encoding: TextEncoding) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut writer = EncodingWriter::new(&mut output, encoding);
    writer.write_all(text.as_bytes())?;
    writer.flush()?;
    drop(writer);
    Ok(output)
}

#[test]
fn utf8_is_written_unchanged_without_a_preamble() {
    assert_eq!(encode("é", TextEncoding::Utf8).unwrap(), "é".as_bytes());
}

#[test]
fn utf8_bom_starts_with_the_byte_order_mark() {
    assert_eq!(
        encode("a", TextEncoding::Utf8Bom).unwrap(),
        vec![0xEF, 0xBB, 0xBF, b'a']
    );
}

#[test]
fn utf16le_starts_with_bom_and_is_little_endian() {
    assert_eq!(
        encode("aé", TextEncoding::Utf16Le).unwrap(),
        vec![0xFF, 0xFE, b'a', 0x00, 0xE9, 0x00]
    );
}

#[test]
fn windows_1252_encodes_one_byte_per_character() {
    assert_eq!(
        encode("é€", TextEncoding::Windows1252).unwrap(),
        vec![0xE9, 0x80]
    );
}

#[test]
fn windows_1252_rejects_an_unencodable_character_instead_of_replacing_it() {
    let error = encode("ok ε", TextEncoding::Windows1252).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("'ε'"), "{message}");
    assert!(message.contains("U+03B5"), "{message}");
    assert!(message.contains("Windows-1252"), "{message}");
}

#[test]
fn an_empty_file_still_gets_its_byte_order_mark() {
    assert_eq!(encode("", TextEncoding::Utf16Le).unwrap(), vec![0xFF, 0xFE]);
}

#[test]
fn a_character_split_across_writes_is_encoded_whole() {
    let bytes = "é".as_bytes();
    let mut output = Vec::new();
    let mut writer = EncodingWriter::new(&mut output, TextEncoding::Windows1252);
    writer.write_all(&bytes[..1]).unwrap();
    writer.write_all(&bytes[1..]).unwrap();
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(output, vec![0xE9]);
}

fn decode(bytes: &[u8], encoding: TextEncoding) -> io::Result<String> {
    let mut reader = DecodingReader::new(Cursor::new(bytes.to_vec()), encoding)
        .ok()
        .unwrap();
    let mut text = String::new();
    reader.read_to_string(&mut text)?;
    Ok(text)
}

#[test]
fn decodes_windows_1252() {
    assert_eq!(
        decode(&[b'a', 0xE9, 0x80], TextEncoding::Windows1252).unwrap(),
        "aé€"
    );
}

#[test]
fn decodes_utf16le_and_removes_the_byte_order_mark() {
    assert_eq!(
        decode(&[0xFF, 0xFE, b'a', 0x00, 0xE9, 0x00], TextEncoding::Utf16Le).unwrap(),
        "aé"
    );
}

#[test]
fn malformed_utf16_is_an_error() {
    assert!(decode(&[0x00, 0xD8], TextEncoding::Utf16Le).is_err());
}

#[test]
fn decodes_large_input_across_buffer_refills() {
    let text = "é".repeat(20_000);
    let encoded = encode(&text, TextEncoding::Utf16Le).unwrap();
    assert_eq!(decode(&encoded, TextEncoding::Utf16Le).unwrap(), text);
}

#[test]
fn utf8_encodings_need_no_decoder() {
    assert!(DecodingReader::new(Cursor::new(Vec::new()), TextEncoding::Utf8).is_err());
    assert!(DecodingReader::new(Cursor::new(Vec::new()), TextEncoding::Utf8Bom).is_err());
    let mut text = String::new();
    decode_to_utf8(Cursor::new("é".as_bytes().to_vec()), TextEncoding::Utf8)
        .read_to_string(&mut text)
        .unwrap();
    assert_eq!(text, "é");
}
