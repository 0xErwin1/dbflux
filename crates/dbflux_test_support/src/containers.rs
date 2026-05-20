use std::time::{Duration, Instant};
use testcontainers::GenericImage;
use testcontainers::clients::Cli;
use testcontainers::core::WaitFor;

pub fn with_postgres_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let docker = Cli::default();
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
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    run(url)
}

pub fn with_mysql_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let docker = Cli::default();
    let image = GenericImage::new("mysql", "8.4")
        .with_env_var("MYSQL_ROOT_PASSWORD", "root")
        .with_env_var("MYSQL_DATABASE", "testdb")
        .with_exposed_port(3306)
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(3306);
    let url = format!("mysql://root:root@127.0.0.1:{port}/testdb");

    run(url)
}

pub fn with_mongodb_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let docker = Cli::default();
    let image = GenericImage::new("mongo", "7")
        .with_exposed_port(27017)
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(27017);
    let url = format!("mongodb://127.0.0.1:{port}/testdb");

    run(url)
}

pub fn with_redis_url<T, E, F>(run: F) -> Result<T, E>
where
    F: FnOnce(String) -> Result<T, E>,
{
    let docker = Cli::default();
    let image = GenericImage::new("redis", "7")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(6379);
    let url = format!("redis://127.0.0.1:{port}/0");

    run(url)
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
    let docker = Cli::default();
    let image = GenericImage::new("mcr.microsoft.com/mssql/server", "2022-latest")
        .with_env_var("ACCEPT_EULA", "Y")
        .with_env_var("MSSQL_SA_PASSWORD", MSSQL_TEST_PASSWORD)
        .with_env_var("MSSQL_PID", "Developer")
        .with_exposed_port(1433)
        .with_wait_for(WaitFor::message_on_stdout(
            "SQL Server is now ready for client connections",
        ));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(1433);
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
    let docker = Cli::default();
    let image = GenericImage::new("amazon/dynamodb-local", "latest")
        .with_exposed_port(8000)
        .with_wait_for(WaitFor::message_on_stdout("Initializing DynamoDB Local"));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(8000);
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
    let docker = Cli::default();
    let token = "dbflux-test-token";
    let org = "dbflux-test-org";
    let bucket = "dbflux-test-bucket";

    // InfluxDB v2 logs to stdout; the "Listening" message signals HTTP readiness.
    let image = GenericImage::new("influxdb", "2.7")
        .with_env_var("DOCKER_INFLUXDB_INIT_MODE", "setup")
        .with_env_var("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
        .with_env_var("DOCKER_INFLUXDB_INIT_PASSWORD", "adminpassword")
        .with_env_var("DOCKER_INFLUXDB_INIT_ORG", org)
        .with_env_var("DOCKER_INFLUXDB_INIT_BUCKET", bucket)
        .with_env_var("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", token)
        .with_exposed_port(8086)
        .with_wait_for(WaitFor::message_on_stdout("Listening"));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(8086);
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
    let docker = Cli::default();
    // InfluxDB v1 logs to stderr; the "Listening on HTTP" message signals readiness.
    let image = GenericImage::new("influxdb", "1.8")
        .with_exposed_port(8086)
        .with_wait_for(WaitFor::message_on_stderr("Listening on HTTP"));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(8086);
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
