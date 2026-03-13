/// CLI process management for `aws ssm start-session`.
///
/// Spawns the AWS CLI as a child process with the SSM port-forwarding
/// document and waits for the tunnel to become ready by probing the
/// local port. The child is killed on drop (RAII).
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use dbflux_core::DbError;

/// Default timeout waiting for the SSM tunnel to accept connections.
const READINESS_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between TCP probe attempts during readiness check.
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

/// Maximum number of stderr lines to retain for error reporting.
const MAX_STDERR_LINES: usize = 20;

/// An active SSM port-forwarding tunnel backed by an `aws ssm start-session`
/// child process.
///
/// The tunnel forwards `127.0.0.1:{local_port}` to `{remote_host}:{remote_port}`
/// through the target EC2 instance. The child process is killed when this
/// struct is dropped.
pub struct SsmTunnel {
    child: Child,
    local_port: u16,
}

impl SsmTunnel {
    /// Start a new SSM port-forwarding tunnel.
    ///
    /// 1. Allocates an ephemeral local port.
    /// 2. Spawns `aws ssm start-session` with the port-forwarding document.
    /// 3. Waits until the local port accepts connections (or times out).
    pub fn start(
        instance_id: &str,
        region: &str,
        remote_host: &str,
        remote_port: u16,
        aws_profile: Option<&str>,
    ) -> Result<Self, DbError> {
        let local_port = find_available_port()?;

        log::info!(
            "[SSM] Starting tunnel: instance={}, region={}, remote_host={}, remote_port={}, local_port={}",
            instance_id,
            region,
            remote_host,
            remote_port,
            local_port,
        );

        let parameters = format!(
            r#"{{"host":["{}"],"portNumber":["{}"],"localPortNumber":["{}"]}}"#,
            remote_host, remote_port, local_port,
        );

        let mut cmd = Command::new("aws");
        cmd.args([
            "ssm",
            "start-session",
            "--target",
            instance_id,
            "--document-name",
            "AWS-StartPortForwardingSessionToRemoteHost",
            "--parameters",
            &parameters,
            "--region",
            region,
        ]);
        cmd.env("AWS_REGION", region);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        if let Some(profile) = aws_profile {
            cmd.env("AWS_PROFILE", profile);
        }

        let mut child = cmd.spawn().map_err(|err| {
            DbError::connection_failed(format!(
                "Failed to start SSM session: {}. Is the AWS CLI installed and the Session Manager plugin available?",
                err,
            ))
        })?;

        // Spawn a background thread to collect stderr for error reporting.
        let stderr_lines = collect_stderr(&mut child);

        // Wait for the tunnel to become ready.
        if let Err(err) = wait_for_port(local_port, READINESS_TIMEOUT) {
            let _ = child.kill();
            let _ = child.wait();

            let stderr_context = read_collected_stderr(stderr_lines);
            let login_hint = sso_login_hint(&stderr_context, aws_profile);
            let detail = if stderr_context.is_empty() {
                match login_hint {
                    Some(hint) => format!("{}\n\n{}", err, hint),
                    None => err.to_string(),
                }
            } else {
                match login_hint {
                    Some(hint) => {
                        format!("{}\n\n{}\n\nSSM stderr:\n{}", err, hint, stderr_context)
                    }
                    None => format!("{}\n\nSSM stderr:\n{}", err, stderr_context),
                }
            };

            return Err(DbError::connection_failed(detail));
        }

        log::info!("[SSM] Tunnel ready on 127.0.0.1:{}", local_port);

        Ok(Self { child, local_port })
    }

    pub fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for SsmTunnel {
    fn drop(&mut self) {
        log::info!("[SSM] Shutting down tunnel on port {}", self.local_port);
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Port allocation
// ---------------------------------------------------------------------------

/// Find an available ephemeral port by binding to port 0 and reading
/// the OS-assigned port. The listener is dropped immediately, freeing
/// the port for use by the SSM child process.
fn find_available_port() -> Result<u16, DbError> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| {
        DbError::connection_failed(format!(
            "Failed to allocate local port for SSM tunnel: {}",
            err
        ))
    })?;

    let port = listener
        .local_addr()
        .map_err(|err| {
            DbError::connection_failed(format!("Failed to read allocated port address: {}", err))
        })?
        .port();

    drop(listener);
    Ok(port)
}

// ---------------------------------------------------------------------------
// Readiness check
// ---------------------------------------------------------------------------

/// Probe the local port until a TCP connection succeeds or `timeout` elapses.
fn wait_for_port(port: u16, timeout: Duration) -> Result<(), DbError> {
    let deadline = Instant::now() + timeout;

    loop {
        match std::net::TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(500),
        ) {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(_) if Instant::now() >= deadline => {
                return Err(DbError::connection_failed(format!(
                    "SSM tunnel readiness timeout: port {} did not become available within {}s",
                    port,
                    timeout.as_secs(),
                )));
            }
            Err(_) => {
                std::thread::sleep(PROBE_INTERVAL);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stderr collection
// ---------------------------------------------------------------------------

/// Spawn a background thread that reads stderr line-by-line and stores the
/// last `MAX_STDERR_LINES` for error reporting. Returns a handle to retrieve
/// the collected lines.
fn collect_stderr(child: &mut Child) -> Option<std::thread::JoinHandle<Vec<String>>> {
    let stderr = child.stderr.take()?;

    let handle = std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        let mut lines = Vec::new();

        for line in reader.lines() {
            match line {
                Ok(text) => {
                    log::debug!("[SSM stderr] {}", text);
                    if lines.len() >= MAX_STDERR_LINES {
                        lines.remove(0);
                    }
                    lines.push(text);
                }
                Err(_) => break,
            }
        }

        lines
    });

    Some(handle)
}

/// Read collected stderr lines, joining the thread if it hasn't finished.
fn read_collected_stderr(handle: Option<std::thread::JoinHandle<Vec<String>>>) -> String {
    match handle {
        Some(h) => {
            // Give the thread a moment to finish reading.
            std::thread::sleep(Duration::from_millis(100));
            match h.join() {
                Ok(lines) => lines.join("\n"),
                Err(_) => String::new(),
            }
        }
        None => String::new(),
    }
}

fn sso_login_hint(stderr_context: &str, aws_profile: Option<&str>) -> Option<String> {
    let lower = stderr_context.to_ascii_lowercase();
    let appears_to_be_sso = lower.contains("sso")
        || lower.contains("aws sso login")
        || lower.contains("token")
        || lower.contains("expired")
        || lower.contains("not logged in");

    if !appears_to_be_sso {
        return None;
    }

    let command = match aws_profile {
        Some(profile) => format!("aws sso login --profile {}", profile),
        None => "aws sso login --profile <profile-name>".to_string(),
    };

    Some(format!(
        "AWS SSO session appears missing or expired. Log in first with: `{}`",
        command
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_available_port_returns_nonzero() {
        let port = find_available_port().unwrap();
        assert!(port > 0, "Allocated port should be nonzero");
    }

    #[test]
    fn find_available_port_returns_unique_ports() {
        let port1 = find_available_port().unwrap();
        let port2 = find_available_port().unwrap();
        // With high probability these are different; on busy systems they
        // *could* collide, but it's extremely unlikely in a test.
        assert_ne!(port1, port2, "Two consecutive allocations should differ");
    }

    #[test]
    fn wait_for_port_succeeds_with_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let result = wait_for_port(port, Duration::from_secs(2));
        assert!(result.is_ok(), "Should succeed when a listener is active");
    }

    #[test]
    fn wait_for_port_times_out_without_listener() {
        // Use a port that is very unlikely to have a listener.
        // Allocate and immediately close so we know nothing is listening.
        let port = find_available_port().unwrap();

        let result = wait_for_port(port, Duration::from_millis(500));
        assert!(
            result.is_err(),
            "Should time out when no listener is active"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("readiness timeout"),
            "Error should mention timeout"
        );
    }

    #[test]
    fn ssm_tunnel_factory_passes_profile() {
        let factory = crate::SsmTunnelFactory::new(Some("my-profile".to_string()));
        assert!(factory.aws_profile.as_deref() == Some("my-profile"));
    }

    #[test]
    fn ssm_tunnel_factory_no_profile() {
        let factory = crate::SsmTunnelFactory::new(None);
        assert!(factory.aws_profile.is_none());
    }
}
