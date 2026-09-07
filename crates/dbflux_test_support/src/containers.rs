use std::time::{Duration, Instant};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

pub fn with_postgres_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("postgres", "16")
        .with_exposed_port(ContainerPort::Tcp(5432))
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres");

    let container = image.start().expect("failed to start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .expect("failed to get postgres host port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    run(url)
}

pub fn with_pgvector_postgres_16_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("pgvector/pgvector", "0.8.0-pg16")
        .with_exposed_port(ContainerPort::Tcp(5432))
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres");

    let container = image
        .start()
        .expect("failed to start pgvector postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .expect("failed to get pgvector postgres host port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    run(url)
}

pub fn with_mysql_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("mysql", "8.4")
        .with_exposed_port(ContainerPort::Tcp(3306))
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "root")
        .with_env_var("MYSQL_DATABASE", "testdb");

    let container = image.start().expect("failed to start mysql container");
    let port = container
        .get_host_port_ipv4(3306)
        .expect("failed to get mysql host port");
    let url = format!("mysql://root:root@127.0.0.1:{port}/testdb");

    run(url)
}

pub fn with_mongodb_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("mongo", "7")
        .with_exposed_port(ContainerPort::Tcp(27017))
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"));

    let container = image.start().expect("failed to start mongo container");
    let port = container
        .get_host_port_ipv4(27017)
        .expect("failed to get mongo host port");
    let url = format!("mongodb://127.0.0.1:{port}/testdb");

    run(url)
}

pub fn with_redis_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("redis", "7")
        .with_exposed_port(ContainerPort::Tcp(6379))
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));

    let container = image.start().expect("failed to start redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .expect("failed to get redis host port");
    let url = format!("redis://127.0.0.1:{port}/0");

    run(url)
}

/// Like [`with_redis_url`], but also hands `run` a reference to the running
/// container so it can `exec` into it (e.g. to `cat` a file written inside
/// the container's filesystem).
///
/// Prefer this over a host bind mount for reading files Redis writes (such
/// as `dump.rdb`): the official `redis` image declares `/data` as an
/// anonymous `VOLUME`, and a rootless container runtime maps that volume's
/// ownership to a subordinate UID/GID range that the host user does not
/// necessarily have configured, causing bind-mount and direct-volume access
/// to fail with a "potentially insufficient UIDs or GIDs" error. `docker
/// exec`/`podman exec` runs as a process inside the container's own user
/// namespace and has no such requirement.
pub fn with_redis_container<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String, &Container<GenericImage>) -> Result<T, E>,
{
    let image = GenericImage::new("redis", "7")
        .with_exposed_port(ContainerPort::Tcp(6379))
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));

    let container = image.start().expect("failed to start redis container");
    let port = container
        .get_host_port_ipv4(6379)
        .expect("failed to get redis host port");
    let url = format!("redis://127.0.0.1:{port}/0");

    run(url, &container)
}

/// Host TCP ports of the six nodes (3 masters + 3 replicas) started by the
/// `grokzen/redis-cluster` all-in-one image.
pub const REDIS_CLUSTER_PORTS: [u16; 6] = [7000, 7001, 7002, 7003, 7004, 7005];

/// Spin up a six-node Redis Cluster via the `grokzen/redis-cluster`
/// all-in-one image and pass the per-node connection URLs
/// (`redis://127.0.0.1:<port>/0`, ordered by `REDIS_CLUSTER_PORTS`) to `run`.
///
/// # Port mapping
///
/// A Redis Cluster client does not stay pinned to the node it dialed: it
/// reads `CLUSTER SLOTS`/`CLUSTER SHARDS` and routes every command to
/// whatever address each master advertises there. Under testcontainers'
/// usual random host-port mapping, that reply would carry the container's
/// *internal* ports, which are unreachable from the host the moment the
/// client starts following slot ranges instead of the seed address the test
/// dialed in on.
///
/// This is worked around the way the image's own docs recommend: `IP=0.0.0.0`
/// makes every node advertise `0.0.0.0` (which the OS resolves back to
/// loopback on connect), and each container port is mapped 1:1 onto the same
/// host port via `with_mapped_port` so a client dialing `0.0.0.0:<port>`
/// lands on the right node through the identical published mapping. This
/// means ports 7000-7005 must be free on the host — acceptable on a CI
/// runner, but it can collide with a Redis Cluster already running locally.
pub fn with_redis_cluster_urls<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(Vec<String>) -> Result<T, E>,
{
    let mut image = GenericImage::new("grokzen/redis-cluster", "7.0.10")
        .with_wait_for(WaitFor::seconds(5))
        .with_env_var("IP", "0.0.0.0");

    for port in REDIS_CLUSTER_PORTS {
        image = image.with_mapped_port(port, ContainerPort::Tcp(port));
    }

    let _container = image
        .start()
        .expect("failed to start redis cluster container");

    let urls: Vec<String> = REDIS_CLUSTER_PORTS
        .iter()
        .map(|port| format!("redis://127.0.0.1:{port}/0"))
        .collect();

    run(urls)
}

/// Connection parameters for a ClickHouse test container.
pub struct ClickHouseConfig {
    pub endpoint: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Spin up a ClickHouse 25.8 LTS container and wait for its HTTP API.
pub fn with_clickhouse<T, E, F>(run: F) -> Result<T, E>
where
    E: From<dbflux_core::DbError>,
    F: FnOnce(ClickHouseConfig) -> Result<T, E>,
{
    let user = "dbflux";
    let password = "dbflux";
    let database = "dbflux_test";
    let image = GenericImage::new("clickhouse/clickhouse-server", "25.8.30.16")
        .with_exposed_port(ContainerPort::Tcp(8123))
        .with_wait_for(WaitFor::seconds(1))
        .with_env_var("CLICKHOUSE_USER", user)
        .with_env_var("CLICKHOUSE_PASSWORD", password)
        .with_env_var("CLICKHOUSE_DB", database)
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");

    let container = image.start().expect("failed to start clickhouse container");
    let port = container
        .get_host_port_ipv4(8123)
        .expect("failed to get clickhouse HTTP host port");
    let endpoint = format!("http://127.0.0.1:{port}");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| dbflux_core::DbError::connection_failed(error.to_string()))
        .map_err(E::from)?;

    retry_db_operation(Duration::from_secs(60), || {
        client
            .get(format!("{endpoint}/ping"))
            .basic_auth(user, Some(password))
            .send()
            .map_err(|error| dbflux_core::DbError::connection_failed(error.to_string()))
            .map_err(E::from)
            .and_then(|response| {
                if response.status().is_success() {
                    Ok(())
                } else {
                    Err(E::from(dbflux_core::DbError::connection_failed(format!(
                        "ClickHouse ping returned {}",
                        response.status()
                    ))))
                }
            })
    })?;

    run(ClickHouseConfig {
        endpoint,
        user: user.to_string(),
        password: password.to_string(),
        database: database.to_string(),
    })
}

/// Password used when launching the SQL Server test container.
///
/// SQL Server requires a "strong" SA password: at least 8 characters with
/// uppercase, lowercase, digit, and special-character classes represented.
/// The same constant is reused inside test URIs.
pub const MSSQL_TEST_PASSWORD: &str = "Strong!Passw0rd";

pub fn with_mssql_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    // The official `mcr.microsoft.com/mssql/server` image takes the EULA via
    // `ACCEPT_EULA=Y` and the SA password via `MSSQL_SA_PASSWORD`. The image
    // is amd64-only; on arm64 hosts, run via emulation or substitute the
    // Azure SQL Edge image manually.
    let image = GenericImage::new("mcr.microsoft.com/mssql/server", "2022-latest")
        .with_exposed_port(ContainerPort::Tcp(1433))
        .with_wait_for(WaitFor::message_on_stdout(
            "SQL Server is now ready for client connections",
        ))
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", MSSQL_TEST_PASSWORD)
        .with_env_var("MSSQL_PID", "Developer");

    let container = image.start().expect("failed to start mssql container");
    let port = container
        .get_host_port_ipv4(1433)
        .expect("failed to get mssql host port");
    // Connect to `master` by default; tests that need a clean database
    // create `dbflux_test` themselves and `USE` it.
    let url = format!(
        "sqlserver://sa:{password}@127.0.0.1:{port}/master",
        password = MSSQL_TEST_PASSWORD
    );

    run(url)
}

pub fn with_dynamodb_endpoint<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("amazon/dynamodb-local", "latest")
        .with_exposed_port(ContainerPort::Tcp(8000))
        .with_wait_for(WaitFor::message_on_stdout("Initializing DynamoDB Local"));

    let container = image.start().expect("failed to start dynamodb container");
    let port = container
        .get_host_port_ipv4(8000)
        .expect("failed to get dynamodb host port");
    let endpoint = format!("http://127.0.0.1:{port}");

    run(endpoint)
}

/// Container parameters for an InfluxDB v2 instance.
pub struct InfluxV2Config {
    pub endpoint: String,
    pub token: String,
    pub org: String,
    pub bucket: String,
}

/// Container parameters for an InfluxDB v1 instance.
pub struct InfluxV1Config {
    pub endpoint: String,
}

/// Spin up an InfluxDB 2.7 container and pass its endpoint + credentials to `run`.
///
/// Waits until the `/health` endpoint returns a 2xx response before calling `run`.
pub fn with_influxdb_v2<T, E, F>(run: F) -> Result<T, E>
where
    E: From<dbflux_core::DbError>,
    F: FnOnce(InfluxV2Config) -> Result<T, E>,
{
    let token = "dbflux-test-token";
    let org = "dbflux-test-org";
    let bucket = "dbflux-test-bucket";

    // InfluxDB v2 logs to stdout; the "Listening" message signals HTTP readiness.
    let image = GenericImage::new("influxdb", "2.7")
        .with_exposed_port(ContainerPort::Tcp(8086))
        .with_wait_for(WaitFor::message_on_stdout("Listening"))
        .with_env_var("DOCKER_INFLUXDB_INIT_MODE", "setup")
        .with_env_var("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
        .with_env_var("DOCKER_INFLUXDB_INIT_PASSWORD", "adminpassword")
        .with_env_var("DOCKER_INFLUXDB_INIT_ORG", org)
        .with_env_var("DOCKER_INFLUXDB_INIT_BUCKET", bucket)
        .with_env_var("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", token);

    let container = image
        .start()
        .expect("failed to start influxdb v2 container");
    let port = container
        .get_host_port_ipv4(8086)
        .expect("failed to get influxdb v2 host port");
    let endpoint = format!("http://127.0.0.1:{port}");

    // Wait until the InfluxDB HTTP API is ready.
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| dbflux_core::DbError::connection_failed(e.to_string()))
        .map_err(E::from)?;

    retry_db_operation(Duration::from_secs(30), || {
        let url = format!("{endpoint}/health");
        client
            .get(&url)
            .send()
            .map_err(|e| dbflux_core::DbError::connection_failed(e.to_string()))
            .map_err(E::from)
            .and_then(|resp| {
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(E::from(dbflux_core::DbError::connection_failed(format!(
                        "health check returned {}",
                        resp.status()
                    ))))
                }
            })
    })?;

    run(InfluxV2Config {
        endpoint,
        token: token.to_string(),
        org: org.to_string(),
        bucket: bucket.to_string(),
    })
}

/// Spin up an InfluxDB 1.8 container and pass its endpoint to `run`.
pub fn with_influxdb_v1<T, E, F>(run: F) -> Result<T, E>
where
    E: From<dbflux_core::DbError>,
    F: FnOnce(InfluxV1Config) -> Result<T, E>,
{
    // InfluxDB v1 logs to stderr; the "Listening on HTTP" message signals readiness.
    let image = GenericImage::new("influxdb", "1.8")
        .with_exposed_port(ContainerPort::Tcp(8086))
        .with_wait_for(WaitFor::message_on_stderr("Listening on HTTP"));

    let container = image
        .start()
        .expect("failed to start influxdb v1 container");
    let port = container
        .get_host_port_ipv4(8086)
        .expect("failed to get influxdb v1 host port");
    let endpoint = format!("http://127.0.0.1:{port}");

    // Wait until HTTP is ready.
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| dbflux_core::DbError::connection_failed(e.to_string()))
        .map_err(E::from)?;

    retry_db_operation(Duration::from_secs(30), || {
        client
            .get(format!("{endpoint}/ping"))
            .send()
            .map_err(|e| dbflux_core::DbError::connection_failed(e.to_string()))
            .map_err(E::from)
            .and_then(|resp| {
                if resp.status().as_u16() < 300 {
                    Ok(())
                } else {
                    Err(E::from(dbflux_core::DbError::connection_failed(format!(
                        "ping returned {}",
                        resp.status()
                    ))))
                }
            })
    })?;

    run(InfluxV1Config { endpoint })
}

/// Static credentials and connection parameters for a MinIO test container.
///
/// `region` is a fixed placeholder (MinIO ignores it, but the AWS SDK
/// requires a non-empty region string to build a client).
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

/// Spin up a MinIO container and pass its endpoint + static credentials to `run`.
///
/// Readiness relies solely on polling the `/minio/health/live` endpoint:
/// MinIO has moved its startup banner between stdout and stderr across
/// releases, so a log-line wait times out depending on the image version.
/// The image tag is pinned for the same reason.
pub fn with_minio_endpoint<T, E, F>(run: F) -> Result<T, E>
where
    E: From<dbflux_core::DbError>,
    F: FnOnce(MinioConfig) -> Result<T, E>,
{
    let access_key_id = "minioadmin";
    let secret_access_key = "minioadmin";

    let image = GenericImage::new("minio/minio", "RELEASE.2025-09-07T16-13-09Z")
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_wait_for(WaitFor::seconds(1))
        .with_env_var("MINIO_ROOT_USER", access_key_id)
        .with_env_var("MINIO_ROOT_PASSWORD", secret_access_key)
        .with_cmd(vec!["server".to_string(), "/data".to_string()]);

    let container = image.start().expect("failed to start minio container");
    let port = container
        .get_host_port_ipv4(9000)
        .expect("failed to get minio host port");
    let endpoint = format!("http://127.0.0.1:{port}");

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| dbflux_core::DbError::connection_failed(e.to_string()))
        .map_err(E::from)?;

    retry_db_operation(Duration::from_secs(30), || {
        client
            .get(format!("{endpoint}/minio/health/live"))
            .send()
            .map_err(|e| dbflux_core::DbError::connection_failed(e.to_string()))
            .map_err(E::from)
            .and_then(|resp| {
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(E::from(dbflux_core::DbError::connection_failed(format!(
                        "health check returned {}",
                        resp.status()
                    ))))
                }
            })
    })?;

    run(MinioConfig {
        endpoint,
        access_key_id: access_key_id.to_string(),
        secret_access_key: secret_access_key.to_string(),
        region: "us-east-1".to_string(),
    })
}

/// Spin up a LocalStack community container scoped to the CloudWatch Logs and
/// CloudWatch (metrics/dashboards) APIs, and pass its endpoint to `run`.
///
/// `SERVICES=logs,cloudwatch` limits which backends LocalStack boots, keeping
/// startup fast. Readiness combines LocalStack's documented "Ready." stdout
/// marker with a poll of `/_localstack/health` so the test does not race a
/// container that has printed the marker but not yet finished wiring up the
/// scoped services.
pub fn with_localstack_cloudwatch_endpoint<T, E, F>(run: F) -> Result<T, E>
where
    E: From<dbflux_core::DbError>,
    F: FnOnce(String) -> Result<T, E>,
{
    let image = GenericImage::new("localstack/localstack", "3")
        .with_exposed_port(ContainerPort::Tcp(4566))
        .with_wait_for(WaitFor::message_on_stdout("Ready."))
        .with_env_var("SERVICES", "logs,cloudwatch");

    let container = image.start().expect("failed to start localstack container");
    let port = container
        .get_host_port_ipv4(4566)
        .expect("failed to get localstack host port");
    let endpoint = format!("http://127.0.0.1:{port}");

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| dbflux_core::DbError::connection_failed(error.to_string()))
        .map_err(E::from)?;

    retry_db_operation(Duration::from_secs(60), || {
        client
            .get(format!("{endpoint}/_localstack/health"))
            .send()
            .map_err(|error| dbflux_core::DbError::connection_failed(error.to_string()))
            .map_err(E::from)
            .and_then(|response| {
                if response.status().is_success() {
                    Ok(())
                } else {
                    Err(E::from(dbflux_core::DbError::connection_failed(format!(
                        "LocalStack health check returned {}",
                        response.status()
                    ))))
                }
            })
    })?;

    run(endpoint)
}

pub fn retry_db_operation<T, E, F>(timeout: Duration, mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
{
    let deadline = Instant::now() + timeout;

    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(error);
                }
            }
        }

        std::thread::sleep(Duration::from_millis(250));
    }
}
