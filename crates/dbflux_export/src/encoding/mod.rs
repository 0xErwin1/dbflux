//! Streaming transcoding between UTF-8 and the CSV encodings: a writer that
//! encodes the UTF-8 the CSV writer produces, and a reader that decodes a file
//! back to UTF-8 for the CSV reader.

use std::io::{self, Read, Write};

use encoding_rs::{CoderResult, Decoder, UTF_16LE, WINDOWS_1252};

use crate::options::TextEncoding;

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";
const UTF16LE_BOM: &[u8] = b"\xFF\xFE";

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Encodes UTF-8 written to it. A character the target encoding cannot
/// represent is an error: replacing it would change the data silently.
pub(crate) struct EncodingWriter<W: Write> {
    inner: W,
    encoding: TextEncoding,
    /// Bytes of a UTF-8 character split across two writes.
    pending: Vec<u8>,
    preamble_written: bool,
}

impl<W: Write> EncodingWriter<W> {
    pub(crate) fn new(inner: W, encoding: TextEncoding) -> Self {
        Self {
            inner,
            encoding,
            pending: Vec::new(),
            preamble_written: false,
        }
    }

    fn write_preamble(&mut self) -> io::Result<()> {
        if self.preamble_written {
            return Ok(());
        }
        self.preamble_written = true;
        match self.encoding {
            TextEncoding::Utf8Bom => self.inner.write_all(UTF8_BOM),
            TextEncoding::Utf16Le => self.inner.write_all(UTF16LE_BOM),
            TextEncoding::Utf8 | TextEncoding::Windows1252 => Ok(()),
        }
    }

    fn transcode_pending(&mut self) -> io::Result<()> {
        let valid_length = match std::str::from_utf8(&self.pending) {
            Ok(_) => self.pending.len(),
            Err(error) if error.error_len().is_none() => error.valid_up_to(),
            Err(error) => return Err(invalid_data(format!("output is not valid UTF-8: {error}"))),
        };
        let complete = self
            .pending
            .get(..valid_length)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .unwrap_or_default();

        let encoded = encode_text(complete, self.encoding)?;
        self.inner.write_all(&encoded)?;
        self.pending.drain(..valid_length);
        Ok(())
    }
}

fn encode_text(text: &str, encoding: TextEncoding) -> io::Result<Vec<u8>> {
    match encoding {
        TextEncoding::Utf8 | TextEncoding::Utf8Bom => Ok(text.as_bytes().to_vec()),
        TextEncoding::Utf16Le => Ok(text.encode_utf16().flat_map(u16::to_le_bytes).collect()),
        TextEncoding::Windows1252 => {
            let (bytes, _, unmappable) = WINDOWS_1252.encode(text);
            if unmappable {
                return Err(unmappable_character_error(text));
            }
            Ok(bytes.into_owned())
        }
    }
}

fn unmappable_character_error(text: &str) -> io::Error {
    let offender = text.chars().find(|character| {
        let mut buffer = [0u8; 4];
        let (_, _, unmappable) = WINDOWS_1252.encode(character.encode_utf8(&mut buffer));
        unmappable
    });
    match offender {
        Some(character) => invalid_data(format!(
            "character '{character}' (U+{:04X}) cannot be encoded as Windows-1252; export as UTF-8 or UTF-16LE instead",
            u32::from(character)
        )),
        None => invalid_data("text cannot be encoded as Windows-1252".to_string()),
    }
}

impl<W: Write> Write for EncodingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.write_preamble()?;
        match self.encoding {
            TextEncoding::Utf8 | TextEncoding::Utf8Bom => self.inner.write(buffer),
            TextEncoding::Utf16Le | TextEncoding::Windows1252 => {
                self.pending.extend_from_slice(buffer);
                self.transcode_pending()?;
                Ok(buffer.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write_preamble()?;
        self.inner.flush()
    }
}

/// Decodes a UTF-16LE or Windows-1252 byte stream to UTF-8 in bounded memory.
/// Malformed input is an error, never a replacement character.
pub struct DecodingReader<R: Read> {
    inner: R,
    decoder: Decoder,
    input: Vec<u8>,
    input_start: usize,
    input_end: usize,
    input_exhausted: bool,
    output: Vec<u8>,
    output_start: usize,
    output_end: usize,
    finished: bool,
}

impl<R: Read> DecodingReader<R> {
    /// Gives `inner` back for the UTF-8 encodings: those files go to the CSV
    /// reader as they are, which also removes a byte order mark.
    pub fn new(inner: R, encoding: TextEncoding) -> Result<Self, R> {
        let decoder = match encoding {
            TextEncoding::Utf8 | TextEncoding::Utf8Bom => return Err(inner),
            TextEncoding::Utf16Le => UTF_16LE.new_decoder_with_bom_removal(),
            TextEncoding::Windows1252 => WINDOWS_1252.new_decoder_without_bom_handling(),
        };
        Ok(Self {
            inner,
            decoder,
            input: vec![0; 8192],
            input_start: 0,
            input_end: 0,
            input_exhausted: false,
            output: vec![0; 16384],
            output_start: 0,
            output_end: 0,
            finished: false,
        })
    }

    fn refill_output(&mut self) -> io::Result<()> {
        while self.output_start == self.output_end && !self.finished {
            if self.input_start == self.input_end && !self.input_exhausted {
                let read = self.inner.read(&mut self.input)?;
                self.input_start = 0;
                self.input_end = read;
                self.input_exhausted = read == 0;
            }

            let source = self
                .input
                .get(self.input_start..self.input_end)
                .unwrap_or_default();
            let (result, read, written, had_errors) =
                self.decoder
                    .decode_to_utf8(source, &mut self.output, self.input_exhausted);
            if had_errors {
                return Err(invalid_data(
                    "the file is not valid in its encoding".to_string(),
                ));
            }
            self.input_start += read;
            self.output_start = 0;
            self.output_end = written;
            if self.input_exhausted && result == CoderResult::InputEmpty {
                self.finished = true;
            }
        }
        Ok(())
    }
}

/// `reader` as UTF-8 text: decoded from UTF-16LE or Windows-1252, and passed
/// through for the UTF-8 encodings.
pub fn decode_to_utf8<R: Read + Send + 'static>(
    reader: R,
    encoding: TextEncoding,
) -> Box<dyn Read + Send> {
    match DecodingReader::new(reader, encoding) {
        Ok(decoder) => Box::new(decoder),
        Err(reader) => Box::new(reader),
    }
}

impl<R: Read> Read for DecodingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.refill_output()?;
        let available = self
            .output
            .get(self.output_start..self.output_end)
            .unwrap_or_default();
        let count = available.len().min(buffer.len());
        if let (Some(target), Some(source)) = (buffer.get_mut(..count), available.get(..count)) {
            target.copy_from_slice(source);
        }
        self.output_start += count;
        Ok(count)
    }
}

#[cfg(test)]
mod tests;
