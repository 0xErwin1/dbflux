/// Subprocess test: verifies that no non-JSON bytes are written to stdout by
/// the `dbflux mcp` subcommand.
///
/// This test is annotated `#[ignore]` because it requires a pre-built `dbflux`
/// binary and a real trusted client entry in `dbflux.db`.  To run manually:
///
/// ```bash
/// cargo build -p dbflux --features sqlite,postgres
/// cargo nextest run -p dbflux_mcp_server --test stdio_no_pollution -- --ignored
/// ```
///
/// The test spawns `dbflux mcp --client-id test-client`, sends a `tools/list`
/// JSON-RPC request on stdin, reads stdout for 2 s, and asserts every non-empty
/// line is valid JSON.
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires a pre-built dbflux binary and a seeded dbflux.db"]
fn mcp_stdout_contains_only_json_lines() {
    let binary = std::env::var("DBFLUX_BIN").unwrap_or_else(|_| "target/debug/dbflux".to_owned());

    let mut child = Command::new(binary)
        .args(["mcp", "--client-id", "test-client"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn dbflux binary");

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    });

    let mut stdin = child.stdin.take().expect("stdin not captured");
    let request_bytes = serde_json::to_vec(&request).unwrap();
    stdin.write_all(&request_bytes).unwrap();
    stdin.write_all(b"\n").unwrap();
    drop(stdin);

    let stdout = child.stdout.take().expect("stdout not captured");
    let reader = BufReader::new(stdout);

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut valid_responses = 0usize;
    let mut invalid_lines: Vec<String> = Vec::new();

    for line in reader.lines() {
        if Instant::now() > deadline {
            break;
        }
        match line {
            Ok(l) if l.trim().is_empty() => continue,
            Ok(l) => {
                if serde_json::from_str::<serde_json::Value>(&l).is_ok() {
                    valid_responses += 1;
                } else {
                    invalid_lines.push(l);
                }
            }
            Err(_) => break,
        }
    }

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        invalid_lines.is_empty(),
        "stdout contained non-JSON lines (logging leak): {invalid_lines:?}"
    );
    assert!(
        valid_responses >= 1,
        "expected at least one valid JSON-RPC response on stdout"
    );
}
