use super::{PostgresConnectParams, connect_postgres};
use crate::tls::PgTlsFiles;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const SSL_REQUEST_CODE: u32 = 80_877_103;

/// How the fake server answers an `SSLRequest`.
#[derive(Clone, Copy)]
enum SslAnswer {
    /// Refuses TLS, so the client continues in plaintext on the same socket.
    Refuse,
    /// Accepts TLS, then sends bytes that are not a TLS record.
    AcceptThenBreakHandshake,
}

/// Serves PostgreSQL connections that fail authentication and returns the
/// port and the number of connections accepted.
fn spawn_server(answer: SslAnswer) -> (u16, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
    let port = listener.local_addr().expect("local addr").port();
    let connections = Arc::new(AtomicUsize::new(0));

    std::thread::spawn({
        let connections = connections.clone();
        move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                connections.fetch_add(1, Ordering::SeqCst);
                serve(stream, answer).ok();
            }
        }
    });

    (port, connections)
}

fn serve(mut stream: TcpStream, answer: SslAnswer) -> std::io::Result<()> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header)?;
    let length = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let code = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

    if code == SSL_REQUEST_CODE {
        match answer {
            SslAnswer::AcceptThenBreakHandshake => {
                stream.write_all(b"S")?;
                stream.write_all(b"this is not a TLS record\n")?;
                return Ok(());
            }
            SslAnswer::Refuse => {
                stream.write_all(b"N")?;
                let mut length_bytes = [0u8; 4];
                stream.read_exact(&mut length_bytes)?;
                let mut rest = vec![0u8; u32::from_be_bytes(length_bytes) as usize - 4];
                stream.read_exact(&mut rest)?;
            }
        }
    } else {
        let mut rest = vec![0u8; length.saturating_sub(8)];
        stream.read_exact(&mut rest)?;
    }

    stream.write_all(&error_response("28P01", "password authentication failed"))
}

fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut fields = Vec::new();
    for (tag, value) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', code),
        (b'M', message),
    ] {
        fields.push(tag);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0);

    let mut packet = vec![b'E'];
    packet.extend_from_slice(&(fields.len() as u32 + 4).to_be_bytes());
    packet.extend_from_slice(&fields);
    packet
}

fn connect_with_prefer(port: u16) -> String {
    let tls_files = PgTlsFiles::default();
    let params = PostgresConnectParams {
        host: "127.0.0.1",
        port,
        user: "dbflux",
        password: Some("wrong"),
        database: "postgres",
        ssl_mode: "prefer",
        tls_files: &tls_files,
    };

    match connect_postgres(&params) {
        Ok(_) => panic!("the fake server never accepts a login"),
        Err(error) => format!("{error:?}"),
    }
}

#[test]
fn prefer_reports_a_server_error_without_a_plaintext_retry() {
    let (port, connections) = spawn_server(SslAnswer::Refuse);

    let error = connect_with_prefer(port);

    assert!(
        error.contains("Authentication failed"),
        "unexpected error: {error}"
    );
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

#[test]
fn prefer_retries_in_plaintext_when_the_tls_handshake_fails() {
    let (port, connections) = spawn_server(SslAnswer::AcceptThenBreakHandshake);

    let error = connect_with_prefer(port);

    assert!(
        error.contains("Authentication failed"),
        "unexpected error: {error}"
    );
    assert_eq!(connections.load(Ordering::SeqCst), 2);
}
