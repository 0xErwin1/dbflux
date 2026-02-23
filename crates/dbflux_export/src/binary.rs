use crate::ExportError;
use dbflux_core::{QueryResult, QueryResultShape};
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum BinaryExportMode {
    Raw,
    Hex,
    Base64,
}

pub struct BinaryExporter {
    pub mode: BinaryExportMode,
}

impl BinaryExporter {
    pub fn export(&self, result: &QueryResult, writer: &mut dyn Write) -> Result<(), ExportError> {
        let bytes = match (&result.shape, &result.raw_bytes) {
            (QueryResultShape::Binary, Some(data)) => data,
            (QueryResultShape::Binary, None) => {
                return Err(ExportError::Failed("No binary data in result".to_string()));
            }
            _ => {
                return Err(ExportError::Failed(
                    "Cannot export non-binary result as binary".to_string(),
                ));
            }
        };

        match self.mode {
            BinaryExportMode::Raw => {
                writer.write_all(bytes)?;
            }
            BinaryExportMode::Hex => {
                writer.write_all(hex::encode(bytes).as_bytes())?;
            }
            BinaryExportMode::Base64 => {
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                writer.write_all(encoded.as_bytes())?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn binary_result(data: Vec<u8>) -> QueryResult {
        QueryResult::binary(data, Duration::from_millis(1))
    }

    #[test]
    fn exports_raw_bytes() {
        let result = binary_result(vec![0xDE, 0xAD, 0xBE, 0xEF]);

        let mut buf = Vec::new();
        BinaryExporter {
            mode: BinaryExportMode::Raw,
        }
        .export(&result, &mut buf)
        .unwrap();

        assert_eq!(buf, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn exports_hex() {
        let result = binary_result(vec![0xDE, 0xAD, 0xBE, 0xEF]);

        let mut buf = Vec::new();
        BinaryExporter {
            mode: BinaryExportMode::Hex,
        }
        .export(&result, &mut buf)
        .unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), "deadbeef");
    }

    #[test]
    fn exports_base64() {
        let result = binary_result(vec![0xDE, 0xAD, 0xBE, 0xEF]);

        let mut buf = Vec::new();
        BinaryExporter {
            mode: BinaryExportMode::Base64,
        }
        .export(&result, &mut buf)
        .unwrap();

        let output = String::from_utf8(buf).unwrap();
        // Verify it decodes back
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&output)
            .unwrap();
        assert_eq!(decoded, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn rejects_non_binary_shape() {
        let result = QueryResult::text("hello".to_string(), Duration::from_millis(1));

        let mut buf = Vec::new();
        let err = BinaryExporter {
            mode: BinaryExportMode::Raw,
        }
        .export(&result, &mut buf);

        assert!(err.is_err());
    }

    #[test]
    fn handles_empty_binary() {
        let result = binary_result(vec![]);

        let mut buf = Vec::new();
        BinaryExporter {
            mode: BinaryExportMode::Hex,
        }
        .export(&result, &mut buf)
        .unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), "");
    }
}
