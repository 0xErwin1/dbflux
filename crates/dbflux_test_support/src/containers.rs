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

pub fn retry_db_operation<T, F>(
    timeout: Duration,
    mut operation: F,
) -> Result<T, dbflux_core::DbError>
where
    F: FnMut() -> Result<T, dbflux_core::DbError>,
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
