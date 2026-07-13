use dbflux_core::{
    ConnectionErrorFormatter, DbError, ErrorLocation, FormattedError, QueryErrorFormatter,
};

/// Formats `postgres::Error` values raised by the Redshift wire client into
/// structured, human-readable messages.
///
/// Mirrors `dbflux_driver_postgres::PostgresErrorFormatter` (same underlying
/// wire client and SQLSTATE conventions), with messaging adapted to Redshift's
/// managed-cluster deployment model (VPC reachability, security groups)
/// instead of a self-hosted server.
pub struct RedshiftErrorFormatter;

impl RedshiftErrorFormatter {
    fn format_redshift_error(e: &postgres::Error) -> FormattedError {
        if let Some(db_error) = e.as_db_error() {
            let mut formatted = FormattedError::new(db_error.message());

            if let Some(detail) = db_error.detail() {
                formatted = formatted.with_detail(detail);
            }

            if let Some(hint) = db_error.hint() {
                formatted = formatted.with_hint(hint);
            }

            formatted = formatted.with_code(db_error.code().code());

            let has_location = db_error.table().is_some()
                || db_error.column().is_some()
                || db_error.constraint().is_some()
                || db_error.schema().is_some();

            if has_location {
                let mut location = ErrorLocation::new();

                if let Some(schema) = db_error.schema() {
                    location = location.with_schema(schema);
                }
                if let Some(table) = db_error.table() {
                    location = location.with_table(table);
                }
                if let Some(column) = db_error.column() {
                    location = location.with_column(column);
                }
                if let Some(constraint) = db_error.constraint() {
                    location = location.with_constraint(constraint);
                }

                formatted = formatted.with_location(location);
            }

            formatted
        } else {
            FormattedError::new(e.to_string())
        }
    }

    fn format_connection_message(source: &str, host: &str, port: u16) -> String {
        if source.contains("timed out") {
            format!(
                "Connection to {host}:{port} timed out. Check that the cluster endpoint is reachable and the port is open."
            )
        } else if source.contains("Connection refused") {
            format!(
                "Connection refused at {host}:{port}. Verify the Redshift cluster is available and reachable from this network (VPC security group / public accessibility)."
            )
        } else if source.contains("password authentication failed") {
            "Authentication failed. Check your username and password.".to_string()
        } else if source.contains("does not exist") {
            format!("Database or user does not exist: {source}")
        } else if source.contains("no pg_hba.conf entry") {
            format!(
                "The cluster rejected the connection from this host. Check the cluster's security group inbound rules for {host}."
            )
        } else if source.contains("error connecting to server")
            || source.contains("could not connect")
        {
            format!(
                "Could not connect to {host}:{port}. The cluster may be unreachable, behind a firewall, or requires an SSH tunnel."
            )
        } else if source.contains("Name or service not known")
            || source.contains("nodename nor servname")
        {
            format!("Could not resolve cluster hostname: {host}")
        } else {
            format!("Connection error: {source}")
        }
    }
}

impl QueryErrorFormatter for RedshiftErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        match error.downcast_ref::<postgres::Error>() {
            Some(pg_error) => Self::format_redshift_error(pg_error),
            None => FormattedError::new(error.to_string()),
        }
    }
}

impl ConnectionErrorFormatter for RedshiftErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        host: &str,
        port: u16,
    ) -> FormattedError {
        let source = error.to_string();
        let message = Self::format_connection_message(&source, host, port);
        FormattedError::new(message)
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        let source = error.to_string();

        let message = if source.contains("password authentication failed") {
            "Authentication failed. Check your username and password in the URI.".to_string()
        } else if source.contains("does not exist") {
            format!("Database or user does not exist: {source}")
        } else if source.contains("invalid connection string") {
            format!("Invalid connection URI format: {sanitized_uri}")
        } else {
            format!("Connection error with URI {sanitized_uri}: {source}")
        };

        FormattedError::new(message)
    }
}

pub(crate) static REDSHIFT_ERROR_FORMATTER: RedshiftErrorFormatter = RedshiftErrorFormatter;

pub(crate) fn format_redshift_connection_error(
    e: &postgres::Error,
    host: &str,
    port: u16,
) -> DbError {
    let formatted = REDSHIFT_ERROR_FORMATTER.format_connection_error(e, host, port);
    log::error!("Redshift connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

pub(crate) fn format_redshift_query_error(e: &postgres::Error) -> DbError {
    let formatted = RedshiftErrorFormatter::format_redshift_error(e);
    log::error!("Redshift query failed: {}", formatted.to_display_string());
    formatted.into_query_error()
}

pub(crate) fn format_redshift_uri_error(e: &postgres::Error, uri: &str) -> DbError {
    let sanitized = dbflux_core::sanitize_uri(uri);
    let formatted = REDSHIFT_ERROR_FORMATTER.format_uri_error(e, &sanitized);
    log::error!("Redshift URI connection failed: {}", formatted.message);
    formatted.into_connection_error()
}

#[cfg(test)]
mod tests {
    use super::{RedshiftErrorFormatter, format_redshift_connection_error};
    use dbflux_core::{DbError, QueryErrorFormatter};

    #[test]
    fn connection_refused_maps_to_a_clear_message() {
        let message = RedshiftErrorFormatter::format_connection_message(
            "Connection refused",
            "cluster.example.com",
            5439,
        );
        assert!(message.contains("cluster.example.com:5439"));
        assert!(message.contains("Redshift cluster"));
    }

    #[test]
    fn timed_out_maps_to_a_clear_message() {
        let message = RedshiftErrorFormatter::format_connection_message(
            "timed out",
            "cluster.example.com",
            5439,
        );
        assert!(message.contains("timed out"));
        assert!(message.contains("cluster.example.com:5439"));
    }

    #[test]
    fn password_auth_failure_maps_to_a_clear_message() {
        let message = RedshiftErrorFormatter::format_connection_message(
            "password authentication failed for user \"awsuser\"",
            "cluster.example.com",
            5439,
        );
        assert_eq!(
            message,
            "Authentication failed. Check your username and password."
        );
    }

    #[test]
    fn unrecognized_source_falls_back_to_a_generic_but_non_empty_message() {
        let message =
            RedshiftErrorFormatter::format_connection_message("some odd io failure", "host", 5439);
        assert_eq!(message, "Connection error: some odd io failure");
        assert!(!message.is_empty());
    }

    /// A genuine `postgres::Error` (client-side statement-timeout kind) with no
    /// `DbError` cause: confirms `format_query_error` falls back to the plain
    /// `Display` message rather than a raw `{:?}` Debug dump.
    #[test]
    fn query_error_without_db_cause_uses_display_not_debug() {
        let formatter = RedshiftErrorFormatter;
        let error = postgres::Error::__private_api_timeout();

        let formatted = formatter.format_query_error(&error);

        assert_eq!(formatted.message, error.to_string());
        assert!(!formatted.message.contains("ErrorInner"));
        assert!(!formatted.message.contains("Kind::"));
    }

    #[test]
    fn connection_error_helper_wraps_into_connection_failed_db_error() {
        let error = postgres::Error::__private_api_timeout();
        let db_error = format_redshift_connection_error(&error, "cluster.example.com", 5439);
        assert!(matches!(db_error, DbError::ConnectionFailed(_)));
    }
}
