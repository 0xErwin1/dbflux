#![allow(clippy::result_large_err)]

//! SSM port-forwarding tunnel for DBFlux.
//!
//! Spawns `aws ssm start-session` as a child process to create a local TCP
//! tunnel through AWS Systems Manager. The tunnel forwards a local port to
//! a remote host/port via an EC2 instance running the SSM agent.

mod process;

use dbflux_core::DbError;

pub use process::SsmTunnel;

/// Factory that creates SSM tunnels with an optional AWS profile override.
///
/// The `aws_profile` is set as the `AWS_PROFILE` environment variable on
/// the child process, allowing the tunnel to use credentials from a
/// specific named profile in `~/.aws/config`.
pub struct SsmTunnelFactory {
    aws_profile: Option<String>,
}

impl SsmTunnelFactory {
    pub fn new(aws_profile: Option<String>) -> Self {
        Self { aws_profile }
    }

    /// Start an SSM port-forwarding tunnel to the given remote host/port
    /// through the specified EC2 instance.
    pub fn start(
        &self,
        instance_id: &str,
        region: &str,
        remote_host: &str,
        remote_port: u16,
    ) -> Result<SsmTunnel, DbError> {
        SsmTunnel::start(
            instance_id,
            region,
            remote_host,
            remote_port,
            self.aws_profile.as_deref(),
        )
    }
}
