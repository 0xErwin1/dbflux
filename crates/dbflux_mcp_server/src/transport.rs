use std::io::{self, BufRead, BufReader, Write};

/// Reads one newline-terminated JSON-RPC message from stdin.
///
/// Returns `None` on clean EOF (client closed the pipe).
/// Returns `Err` on I/O or parse error.
pub fn read_message(reader: &mut impl BufRead) -> io::Result<Option<serde_json::Value>> {
    let mut line = String::new();

    match reader.read_line(&mut line) {
        Ok(0) => return Ok(None),
        Ok(_) => {}
        Err(e) => return Err(e),
    }

    let trimmed = line.trim();

    if trimmed.is_empty() {
        return Ok(None);
    }

    let value =
        serde_json::from_str(trimmed).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(value))
}

/// Writes one JSON-RPC message to stdout, followed by a newline.
pub fn write_message(writer: &mut impl Write, message: &serde_json::Value) -> io::Result<()> {
    let serialized = serde_json::to_string(message)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    writeln!(writer, "{serialized}")?;
    writer.flush()
}

/// Builds a JSON-RPC 2.0 success response.
pub fn success_response(id: &serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

/// Builds a JSON-RPC 2.0 error response.
pub fn error_response(
    id: &serde_json::Value,
    code: i64,
    message: &str,
    data: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut error = serde_json::json!({
        "code": code,
        "message": message,
    });

    if let Some(data) = data {
        error["data"] = data;
    }

    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error,
    })
}

/// Standard JSON-RPC error codes.
pub mod codes {
    pub const PARSE_ERROR: i64 = -32700;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL_ERROR: i64 = -32603;

    /// Application-level denial (authorization rejected the request).
    pub const AUTHORIZATION_DENIED: i64 = -32000;
}

/// Creates a `BufReader` wrapping stdin.
pub fn stdin_reader() -> BufReader<io::Stdin> {
    BufReader::new(io::stdin())
}
