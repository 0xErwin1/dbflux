//! Integration tests for ProxyTunnel using real Docker containers.
//!
//! These tests require a running Docker daemon and are marked `#[ignore]`.
//! Run with: `cargo test -p dbflux_proxy -- --ignored`

use std::net::TcpStream;
use std::time::{Duration, Instant};

use dbflux_proxy::{ProxyCredentials, ProxyProtocol, ProxyTunnel, ProxyTunnelConfig};
use testcontainers::clients::Cli;
use testcontainers::core::WaitFor;
use testcontainers::{GenericImage, RunnableImage};

/// Docker bridge gateway IP — routes from inside containers back to the host.
const DOCKER_GATEWAY: &str = "172.17.0.1";

fn start_postgres(docker: &Cli) -> (testcontainers::Container<'_, GenericImage>, u16) {
    let image = GenericImage::new("postgres", "16")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_exposed_port(5432)
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(5432);
    (container, port)
}

fn start_socks5_proxy(docker: &Cli) -> (testcontainers::Container<'_, GenericImage>, u16) {
    let image = GenericImage::new("serjs/go-socks5-proxy", "latest")
        .with_env_var("REQUIRE_AUTH", "false")
        .with_exposed_port(1080)
        .with_wait_for(WaitFor::message_on_stderr(
            "Start listening proxy service on",
        ));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(1080);
    (container, port)
}

fn start_socks5_proxy_with_auth(
    docker: &Cli,
) -> (testcontainers::Container<'_, GenericImage>, u16) {
    let image = GenericImage::new("serjs/go-socks5-proxy", "latest")
        .with_env_var("PROXY_USER", "testuser")
        .with_env_var("PROXY_PASSWORD", "testpass")
        .with_exposed_port(1080)
        .with_wait_for(WaitFor::message_on_stderr(
            "Start listening proxy service on",
        ));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(1080);
    (container, port)
}

/// Wait for PostgreSQL to be reachable via a tunnel.
fn wait_for_postgres(tunnel_port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;

    loop {
        match postgres::Client::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                tunnel_port,
            ),
            postgres::NoTls,
        ) {
            Ok(mut client) => {
                let _ = client.simple_query("SELECT 1");
                return;
            }
            Err(_) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => panic!("PostgreSQL not reachable through tunnel after timeout: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// SOCKS5 tests
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn socks5_tunnel_connects_to_postgres() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_pg_container, pg_port) = start_postgres(&docker);
    let (_proxy_container, proxy_port) = start_socks5_proxy(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::Socks5,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: None,
    };

    // The proxy container needs to reach PostgreSQL. From inside the proxy
    // container, the Docker gateway routes back to the host where the
    // PostgreSQL port is mapped.
    let tunnel = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), pg_port)
        .expect("tunnel should start");

    let local_port = tunnel.local_port();
    assert_ne!(local_port, 0);

    wait_for_postgres(local_port, Duration::from_secs(30));

    let mut client = postgres::Client::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            local_port,
        ),
        postgres::NoTls,
    )
    .expect("should connect through tunnel");

    let rows = client
        .query("SELECT 1 + 1 AS result", &[])
        .expect("query should succeed");

    assert_eq!(rows.len(), 1);
    let value: i32 = rows[0].get("result");
    assert_eq!(value, 2);
}

#[test]
#[ignore = "requires Docker daemon"]
fn socks5_tunnel_with_auth_connects_to_postgres() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_pg_container, pg_port) = start_postgres(&docker);
    let (_proxy_container, proxy_port) = start_socks5_proxy_with_auth(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::Socks5,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: Some(ProxyCredentials {
            username: "testuser".to_string(),
            password: "testpass".to_string(),
        }),
    };

    let tunnel = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), pg_port)
        .expect("tunnel should start with auth");

    wait_for_postgres(tunnel.local_port(), Duration::from_secs(30));

    let mut client = postgres::Client::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            tunnel.local_port(),
        ),
        postgres::NoTls,
    )
    .expect("should connect through authenticated tunnel");

    let rows = client
        .query("SELECT 'hello' AS greeting", &[])
        .expect("query should succeed");

    assert_eq!(rows.len(), 1);
    let greeting: &str = rows[0].get("greeting");
    assert_eq!(greeting, "hello");
}

#[test]
#[ignore = "requires Docker daemon"]
fn socks5_tunnel_wrong_auth_fails() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_pg_container, pg_port) = start_postgres(&docker);
    let (_proxy_container, proxy_port) = start_socks5_proxy_with_auth(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::Socks5,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: Some(ProxyCredentials {
            username: "wrong".to_string(),
            password: "creds".to_string(),
        }),
    };

    let result = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), pg_port);
    assert!(result.is_err(), "should fail with wrong credentials");
}

#[test]
#[ignore = "requires Docker daemon"]
fn socks5_tunnel_unreachable_target_fails() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_proxy_container, proxy_port) = start_socks5_proxy(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::Socks5,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: None,
    };

    // Port 1 is unlikely to have anything listening
    let result = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), 1);
    assert!(result.is_err(), "should fail for unreachable target");
}

#[test]
#[ignore = "requires Docker daemon"]
fn socks5_tunnel_multiple_connections() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_pg_container, pg_port) = start_postgres(&docker);
    let (_proxy_container, proxy_port) = start_socks5_proxy(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::Socks5,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: None,
    };

    let tunnel = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), pg_port)
        .expect("tunnel should start");

    wait_for_postgres(tunnel.local_port(), Duration::from_secs(30));

    // Open multiple concurrent connections through the same tunnel
    let mut clients: Vec<postgres::Client> = (0..3)
        .map(|_| {
            postgres::Client::connect(
                &format!(
                    "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                    tunnel.local_port(),
                ),
                postgres::NoTls,
            )
            .expect("concurrent connection should succeed")
        })
        .collect();

    for (i, client) in clients.iter_mut().enumerate() {
        let rows = client
            .query("SELECT $1::int AS idx", &[&(i as i32)])
            .expect("query on concurrent connection should succeed");
        let idx: i32 = rows[0].get("idx");
        assert_eq!(idx, i as i32);
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn socks5_tunnel_drops_cleanly() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_pg_container, pg_port) = start_postgres(&docker);
    let (_proxy_container, proxy_port) = start_socks5_proxy(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::Socks5,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: None,
    };

    let tunnel = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), pg_port)
        .expect("tunnel should start");

    let local_port = tunnel.local_port();
    wait_for_postgres(local_port, Duration::from_secs(30));

    // Drop the tunnel — the background thread should shut down
    drop(tunnel);

    // Give the background thread time to stop
    std::thread::sleep(Duration::from_millis(100));

    // New connections to the now-closed port should fail
    let result = TcpStream::connect(format!("127.0.0.1:{local_port}"));
    assert!(
        result.is_err(),
        "connecting to dropped tunnel port should fail"
    );
}

// ---------------------------------------------------------------------------
// HTTP CONNECT tests
// ---------------------------------------------------------------------------

fn start_tinyproxy(docker: &Cli) -> (testcontainers::Container<'_, GenericImage>, u16) {
    let image = GenericImage::new("monokal/tinyproxy", "latest")
        .with_exposed_port(8888)
        .with_wait_for(WaitFor::message_on_stdout("Accepting connections"));

    let args = vec!["ANY".to_string()];
    let container = docker.run(RunnableImage::from((image, args)));
    let port = container.get_host_port_ipv4(8888);
    (container, port)
}

#[test]
#[ignore = "requires Docker daemon"]
fn http_connect_tunnel_to_postgres() {
    let _ = env_logger::try_init();

    let docker = Cli::default();
    let (_pg_container, pg_port) = start_postgres(&docker);
    let (_proxy_container, proxy_port) = start_tinyproxy(&docker);

    let config = ProxyTunnelConfig {
        protocol: ProxyProtocol::HttpConnect,
        proxy_host: "127.0.0.1".to_string(),
        proxy_port: proxy_port,
        credentials: None,
    };

    let tunnel = ProxyTunnel::start(config, DOCKER_GATEWAY.to_string(), pg_port)
        .expect("HTTP CONNECT tunnel should start");

    wait_for_postgres(tunnel.local_port(), Duration::from_secs(30));

    let mut client = postgres::Client::connect(
        &format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            tunnel.local_port(),
        ),
        postgres::NoTls,
    )
    .expect("should connect through HTTP CONNECT tunnel");

    let rows = client
        .query("SELECT 2 + 2 AS result", &[])
        .expect("query should succeed");

    assert_eq!(rows.len(), 1);
    let value: i32 = rows[0].get("result");
    assert_eq!(value, 4);
}

// ---------------------------------------------------------------------------
// ProxyTunnelConfig::from_profile tests
// ---------------------------------------------------------------------------

#[test]
fn from_profile_socks5_no_auth() {
    use dbflux_core::{ProxyAuth, ProxyKind, ProxyProfile};

    let profile = ProxyProfile {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        kind: ProxyKind::Socks5,
        host: "proxy.local".to_string(),
        port: 1080,
        auth: ProxyAuth::None,
        no_proxy: None,
        enabled: true,
        save_secret: false,
    };

    let config = ProxyTunnelConfig::from_profile(&profile, None);

    assert!(matches!(config.protocol, ProxyProtocol::Socks5));
    assert_eq!(config.proxy_host, "proxy.local");
    assert_eq!(config.proxy_port, 1080);
    assert!(config.credentials.is_none());
}

#[test]
fn from_profile_http_with_auth() {
    use dbflux_core::{ProxyAuth, ProxyKind, ProxyProfile};

    let profile = ProxyProfile {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        kind: ProxyKind::Http,
        host: "proxy.local".to_string(),
        port: 8080,
        auth: ProxyAuth::Basic {
            username: "admin".to_string(),
        },
        no_proxy: None,
        enabled: true,
        save_secret: false,
    };

    let config = ProxyTunnelConfig::from_profile(&profile, Some("s3cret"));

    assert!(matches!(config.protocol, ProxyProtocol::HttpConnect));
    assert_eq!(config.proxy_host, "proxy.local");
    assert_eq!(config.proxy_port, 8080);

    let creds = config.credentials.expect("should have credentials");
    assert_eq!(creds.username, "admin");
    assert_eq!(creds.password, "s3cret");
}

#[test]
fn from_profile_https_maps_to_https_connect() {
    use dbflux_core::{ProxyAuth, ProxyKind, ProxyProfile};

    let profile = ProxyProfile {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        kind: ProxyKind::Https,
        host: "proxy.local".to_string(),
        port: 3128,
        auth: ProxyAuth::None,
        no_proxy: None,
        enabled: true,
        save_secret: false,
    };

    let config = ProxyTunnelConfig::from_profile(&profile, None);
    assert!(matches!(config.protocol, ProxyProtocol::HttpsConnect));
}
