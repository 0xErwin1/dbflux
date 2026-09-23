use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use gpui_mcp_protocol::{
    AppId, BridgeResult, Capability, EndpointDescriptor, LocalEndpoint, MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES, Operation, PROTOCOL_VERSION, ProcessId, RequestId, WireRequest,
    WireResponse,
};
use interprocess::local_socket::{
    GenericFilePath, GenericNamespaced, Name, ToFsName as _, ToNsName as _,
    tokio::{Stream as LocalSocketStream, prelude::*},
};
use serde::Serialize;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DESCRIPTOR_BYTES: u64 = 16 * 1024;
const MAX_IDLE_CONNECTIONS: usize = 4;
const MAX_DISCOVERY_CANDIDATES: usize = 128;
const DISCOVERY_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

struct ConnectionPool {
    idle: Mutex<Vec<LocalSocketStream>>,
    permits: Semaphore,
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            idle: Mutex::new(Vec::with_capacity(MAX_IDLE_CONNECTIONS)),
            permits: Semaphore::new(MAX_IDLE_CONNECTIONS),
        }
    }

    async fn take(&self, endpoint: &LocalEndpoint) -> Result<LocalSocketStream, String> {
        if let Some(stream) = self.idle.lock().await.pop() {
            return Ok(stream);
        }
        connect(endpoint).await
    }

    async fn put(&self, stream: LocalSocketStream) {
        let mut idle = self.idle.lock().await;
        if idle.len() < MAX_IDLE_CONNECTIONS {
            idle.push(stream);
        }
    }
}

#[derive(Clone)]
pub(crate) struct BridgeClient {
    descriptor: Arc<EndpointDescriptor>,
    next_request_id: Arc<AtomicU64>,
    pool: Arc<ConnectionPool>,
}

impl BridgeClient {
    fn from_path(path: &Path, app_id: Option<&AppId>) -> Result<Self> {
        let descriptor = read_descriptor(path)?;
        if let Some(expected) = app_id
            && descriptor.app_id != *expected
        {
            bail!(
                "endpoint application id is {:?}, not {:?}",
                descriptor.app_id,
                expected
            );
        }
        validate_descriptor(&descriptor, path)?;
        Ok(Self {
            descriptor: Arc::new(descriptor),
            next_request_id: Arc::new(AtomicU64::new(1)),
            pool: Arc::new(ConnectionPool::new()),
        })
    }

    pub(crate) fn descriptor(&self) -> &EndpointDescriptor {
        &self.descriptor
    }

    pub(crate) fn target_id(&self) -> TargetId {
        TargetId::from_descriptor(&self.descriptor)
    }

    pub(crate) async fn call(&self, operation: Operation) -> Result<BridgeResult, String> {
        let response_timeout = match &operation {
            Operation::WaitForTree { timeout_ms, .. }
            | Operation::WaitForFrame { timeout_ms, .. } => {
                Duration::from_millis(*timeout_ms).saturating_add(IO_TIMEOUT)
            }
            _ => IO_TIMEOUT,
        };
        let request_id =
            self.next_request_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(1)
                });
        let request_id = request_id
            .ok()
            .and_then(RequestId::new)
            .ok_or_else(|| "bridge request identifier space is exhausted".to_owned())?;
        let request = WireRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            token: self.descriptor.token.clone(),
            operation,
        };
        let payload = serde_json::to_vec(&request)
            .map_err(|_| "could not encode the bridge request".to_owned())?;
        if payload.len() > MAX_REQUEST_BYTES {
            return Err("request exceeds the 1 MiB safety bound".to_owned());
        }

        let _permit = self
            .pool
            .permits
            .acquire()
            .await
            .map_err(|_| "bridge connection pool stopped".to_owned())?;
        let mut stream = self.pool.take(&self.descriptor.endpoint).await?;
        let response = exchange(&mut stream, &payload, response_timeout).await;
        if response.is_ok() {
            self.pool.put(stream).await;
        }
        let response = response?;

        if response.protocol_version != PROTOCOL_VERSION {
            return Err("the bridge returned an incompatible protocol version".to_owned());
        }
        if response.request_id != request_id {
            return Err("the bridge returned a mismatched request id".to_owned());
        }
        match (response.result, response.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(format!("{:?}: {}", error.code, error.message)),
            _ => Err("the bridge response has an invalid outcome".to_owned()),
        }
    }
}

/// Opaque, validated selection identity for one bridge instance.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(crate) struct TargetId(String);

impl TargetId {
    fn from_descriptor(descriptor: &EndpointDescriptor) -> Self {
        Self(format!(
            "{}-{:016x}",
            descriptor.app_id, descriptor.instance_id
        ))
    }

    fn parse(value: &str) -> Result<Self, String> {
        let Some((app_id, instance)) = value.rsplit_once('-') else {
            return Err("target ID must end with a 16-digit instance identifier".to_owned());
        };
        AppId::new(app_id).map_err(|error| error.to_string())?;
        if instance.len() != 16
            || !instance.bytes().all(|byte| byte.is_ascii_hexdigit())
            || u64::from_str_radix(instance, 16)
                .ok()
                .and_then(gpui_mcp_protocol::InstanceId::new)
                .is_none()
        {
            return Err(
                "target ID must end with a nonzero 16-digit instance identifier".to_owned(),
            );
        }
        Ok(Self(value.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Public, non-secret identity for one discoverable GPUI bridge.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AppInfo {
    pub(crate) target_id: TargetId,
    pub(crate) app_id: AppId,
    pub(crate) app_name: String,
    pub(crate) pid: ProcessId,
    pub(crate) selected: bool,
    pub(crate) capabilities: gpui_mcp_protocol::Capabilities,
}

impl AppInfo {
    fn from_client(client: &BridgeClient, selected: bool) -> Self {
        let descriptor = client.descriptor();
        Self {
            target_id: client.target_id(),
            app_id: descriptor.app_id.clone(),
            app_name: descriptor.app_name.clone(),
            pid: descriptor.pid,
            selected,
            capabilities: descriptor.capabilities.clone(),
        }
    }
}

/// Lazily discovers GPUI bridges for one long-lived MCP transport.
///
/// A single available bridge is selected automatically. When several are live,
/// callers select one by the opaque, non-secret target ID returned by
/// [`Self::list_apps`]. The optional application ID is an explicit discovery
/// filter; normal MCP configuration does not need one.
#[derive(Clone)]
pub(crate) struct BridgeRegistry {
    endpoint: Option<PathBuf>,
    app_id: Option<AppId>,
    endpoint_dir: Option<PathBuf>,
    selected: Arc<RwLock<Option<BridgeClient>>>,
}

impl BridgeRegistry {
    pub(crate) fn new(
        endpoint: Option<PathBuf>,
        app_id: Option<AppId>,
        endpoint_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            endpoint,
            app_id,
            endpoint_dir,
            selected: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) async fn list_apps(&self) -> Result<Vec<AppInfo>, String> {
        let clients = self
            .discover()
            .await
            .map_err(|error| format_discovery_error(&error))?;
        let mut selected = self.selected.write().await;
        if selected.as_ref().is_some_and(|selected| {
            clients
                .iter()
                .all(|client| client.target_id() != selected.target_id())
        }) {
            *selected = None;
        }
        if selected.is_none()
            && let [client] = clients.as_slice()
        {
            *selected = Some(client.clone());
        }
        let selected_id = selected.as_ref().map(BridgeClient::target_id);
        drop(selected);
        Ok(clients
            .iter()
            .map(|client| {
                AppInfo::from_client(
                    client,
                    selected_id
                        .as_ref()
                        .is_some_and(|selected| client.target_id() == *selected),
                )
            })
            .collect())
    }

    pub(crate) async fn select(&self, requested: &str) -> Result<AppInfo, String> {
        let requested = TargetId::parse(requested)?;
        let clients = self
            .discover()
            .await
            .map_err(|error| format_discovery_error(&error))?;
        let Some(client) = clients
            .into_iter()
            .find(|client| client.target_id() == requested)
        else {
            return Err(format!(
                "GPUI target {requested} is not live; call list_apps to refresh available targets"
            ));
        };
        *self.selected.write().await = Some(client.clone());
        Ok(AppInfo::from_client(&client, true))
    }

    pub(crate) async fn client(&self) -> Result<BridgeClient, String> {
        if let Some(client) = self.selected.read().await.clone() {
            return Ok(client);
        }
        let clients = self
            .discover()
            .await
            .map_err(|error| format_discovery_error(&error))?;
        match clients.as_slice() {
            [] => Err(
                "no live GPUI applications were found; start an instrumented app and retry"
                    .to_owned(),
            ),
            [client] => {
                let client = client.clone();
                *self.selected.write().await = Some(client.clone());
                Ok(client)
            }
            _ => {
                let listing = clients
                    .iter()
                    .map(|client| {
                        let descriptor = client.descriptor();
                        format!(
                            "{} ({}, pid {}, target {})",
                            descriptor.app_name,
                            descriptor.app_id,
                            descriptor.pid,
                            client.target_id()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(format!(
                    "multiple GPUI applications are live [{listing}]; call list_apps, then select_app"
                ))
            }
        }
    }

    async fn discover(&self) -> Result<Vec<BridgeClient>> {
        if let Some(endpoint) = &self.endpoint {
            let client = BridgeClient::from_path(endpoint, self.app_id.as_ref())?;
            if probe(&client).await {
                return Ok(vec![client]);
            }
            return Ok(Vec::new());
        }
        discover_clients(self.app_id.as_ref(), self.endpoint_dir.as_deref()).await
    }
}

fn format_discovery_error(error: &anyhow::Error) -> String {
    format!("GPUI bridge discovery failed: {error:#}")
}

async fn exchange(
    stream: &mut LocalSocketStream,
    payload: &[u8],
    response_timeout: Duration,
) -> Result<WireResponse, String> {
    let length = u32::try_from(payload.len()).map_err(|_| "request exceeds the wire limit")?;
    timeout(IO_TIMEOUT, async {
        stream.write_all(&length.to_be_bytes()).await?;
        stream.write_all(payload).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| "timed out writing the bridge request".to_owned())?
    .map_err(|_| "could not write the bridge request".to_owned())?;

    let mut length_bytes = [0_u8; 4];
    timeout(response_timeout, stream.read_exact(&mut length_bytes))
        .await
        .map_err(|_| "timed out waiting for the GPUI application".to_owned())?
        .map_err(|_| "the GPUI application closed the connection".to_owned())?;
    let response_length = u32::from_be_bytes(length_bytes) as usize;
    if response_length == 0 || response_length > MAX_RESPONSE_BYTES {
        return Err("bridge response size is invalid".to_owned());
    }
    let mut response_payload = vec![0_u8; response_length];
    timeout(IO_TIMEOUT, stream.read_exact(&mut response_payload))
        .await
        .map_err(|_| "timed out reading the bridge response".to_owned())?
        .map_err(|_| "the bridge response was truncated".to_owned())?;
    serde_json::from_slice(&response_payload)
        .map_err(|_| "the bridge returned invalid JSON".to_owned())
}

async fn connect(endpoint: &LocalEndpoint) -> Result<LocalSocketStream, String> {
    let name = endpoint_name(endpoint).map_err(|_| "the local endpoint name is invalid")?;
    timeout(CONNECT_TIMEOUT, LocalSocketStream::connect(name))
        .await
        .map_err(|_| "timed out connecting to the GPUI application".to_owned())?
        .map_err(|_| "could not connect to the GPUI application".to_owned())
}

fn endpoint_name(endpoint: &LocalEndpoint) -> std::io::Result<Name<'_>> {
    match endpoint {
        LocalEndpoint::Filesystem { path } => path.as_path().to_fs_name::<GenericFilePath>(),
        LocalEndpoint::Namespaced { name } => name.as_str().to_ns_name::<GenericNamespaced>(),
    }
}

async fn discover_clients(
    app_id: Option<&AppId>,
    endpoint_dir: Option<&Path>,
) -> Result<Vec<BridgeClient>> {
    let directory = match endpoint_dir {
        Some(directory) => directory.to_path_buf(),
        None => default_endpoint_dir()?,
    };
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("could not read endpoint directory {}", directory.display())
            });
        }
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("json"))
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths.truncate(MAX_DISCOVERY_CANDIDATES);
    let mut candidates = Vec::new();
    for path in paths {
        let Ok(descriptor) = read_descriptor(&path) else {
            continue;
        };
        candidates.push((path, descriptor));
    }
    let liveness = ProcessLiveness::query(candidates.iter().map(|(_, descriptor)| descriptor.pid));
    let mut probes = JoinSet::new();
    for (path, descriptor) in candidates {
        if liveness.is_stale(descriptor.pid, &path) {
            // The owning process is gone; drop its descriptor so stale files
            // never accumulate and future discovery stays unambiguous.
            let _ = fs::remove_file(&path);
            continue;
        }
        if app_id.is_none_or(|expected| descriptor.app_id == *expected)
            && validate_descriptor(&descriptor, &path).is_ok()
        {
            probes.spawn(async move {
                let client = BridgeClient {
                    descriptor: Arc::new(descriptor),
                    next_request_id: Arc::new(AtomicU64::new(1)),
                    pool: Arc::new(ConnectionPool::new()),
                };
                probe(&client).await.then_some(client)
            });
        }
    }
    let mut clients = Vec::new();
    while let Some(result) = probes.join_next().await {
        if let Ok(Some(client)) = result {
            clients.push(client);
        }
    }
    clients.sort_unstable_by_key(BridgeClient::target_id);
    Ok(clients)
}

async fn probe(client: &BridgeClient) -> bool {
    timeout(
        DISCOVERY_PROBE_TIMEOUT,
        connect(&client.descriptor().endpoint),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

/// Once a process that wrote a descriptor could plausibly have started this
/// long after the descriptor file's timestamp, treat the pid as reused.
const PID_REUSE_SLACK_SECS: u64 = 10;

/// Snapshot of which descriptor-owning processes are still running.
struct ProcessLiveness {
    system: sysinfo::System,
}

impl ProcessLiveness {
    fn query(pids: impl Iterator<Item = ProcessId>) -> Self {
        let mut pids: Vec<sysinfo::Pid> =
            pids.map(|pid| sysinfo::Pid::from_u32(pid.get())).collect();
        pids.sort_unstable();
        pids.dedup();
        let mut system = sysinfo::System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&pids), true);
        Self { system }
    }

    /// True when the descriptor's owning process no longer exists, or when its
    /// pid now belongs to a process started after the descriptor was written
    /// (the pid was reused, so the original owner is dead).
    fn is_stale(&self, pid: ProcessId, descriptor_path: &Path) -> bool {
        let Some(process) = self.system.process(sysinfo::Pid::from_u32(pid.get())) else {
            return true;
        };
        let started = process.start_time();
        if started == 0 {
            return false;
        }
        let written = fs::symlink_metadata(descriptor_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs());
        matches!(
            written,
            Some(written) if started > written.saturating_add(PID_REUSE_SLACK_SECS)
        )
    }
}

fn read_descriptor(path: &Path) -> Result<EndpointDescriptor> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("could not inspect endpoint descriptor {}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_DESCRIPTOR_BYTES
    {
        bail!("endpoint descriptor must be a regular, non-symlink file smaller than 16 KiB");
    }
    #[cfg(unix)]
    validate_descriptor_permissions(&metadata)?;
    let bytes = fs::read(path)
        .with_context(|| format!("could not read endpoint descriptor {}", path.display()))?;
    serde_json::from_slice(&bytes).context("endpoint descriptor contains invalid JSON")
}

fn validate_descriptor(descriptor: &EndpointDescriptor, descriptor_path: &Path) -> Result<()> {
    if descriptor.protocol_version != PROTOCOL_VERSION {
        bail!(
            "endpoint protocol is {}, but server requires {}",
            descriptor.protocol_version,
            PROTOCOL_VERSION
        );
    }
    validate_local_endpoint(&descriptor.endpoint, descriptor_path)?;
    if descriptor.token.len() != 64
        || !descriptor
            .token
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("endpoint authentication token is malformed");
    }
    if descriptor.capabilities.supports(Capability::Screenshot)
        && descriptor.native_window_id.is_none()
    {
        bail!("endpoint advertises screenshots without a native window identifier");
    }
    Ok(())
}

fn validate_local_endpoint(endpoint: &LocalEndpoint, descriptor_path: &Path) -> Result<()> {
    let _ = descriptor_path;
    match endpoint {
        LocalEndpoint::Filesystem { path } => {
            let _ = path;
            #[cfg(not(unix))]
            bail!("filesystem local endpoints are unsupported on this platform");
            #[cfg(unix)]
            {
                use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};

                if !path.is_absolute() {
                    bail!("filesystem local endpoint must be absolute");
                }
                let descriptor_parent = descriptor_path
                    .parent()
                    .ok_or_else(|| anyhow::Error::msg("descriptor has no parent directory"))?
                    .canonicalize()?;
                let endpoint_parent = path
                    .parent()
                    .ok_or_else(|| {
                        anyhow::Error::msg("filesystem local endpoint has no parent directory")
                    })?
                    .canonicalize()?;
                if endpoint_parent != descriptor_parent {
                    bail!("filesystem local endpoint must share the descriptor directory");
                }
                let socket_metadata = fs::symlink_metadata(path)?;
                if socket_metadata.file_type().is_symlink()
                    || !socket_metadata.file_type().is_socket()
                {
                    bail!("filesystem local endpoint is not a Unix-domain socket");
                }
                if socket_metadata.permissions().mode() & 0o077 != 0 {
                    bail!("filesystem local endpoint is accessible by group/other users");
                }
                let descriptor_metadata = fs::symlink_metadata(descriptor_path)?;
                if socket_metadata.uid() != descriptor_metadata.uid() {
                    bail!("descriptor and local endpoint have different owners");
                }
            }
        }
        LocalEndpoint::Namespaced { name } => {
            #[cfg(not(windows))]
            {
                let _ = name;
                bail!("namespaced local endpoints are unsupported on this platform");
            }
            #[cfg(windows)]
            if name.is_empty()
                || name.len() > 256
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            {
                bail!("namespaced local endpoint is malformed");
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn validate_descriptor_permissions(metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!("refusing endpoint descriptor readable or writable by group/other users");
    }
    Ok(())
}

pub(crate) fn default_runtime_root() -> Result<PathBuf> {
    gpui_mcp_protocol::runtime_root().context("resolve the GPUI MCP runtime directory")
}

fn default_endpoint_dir() -> Result<PathBuf> {
    default_runtime_root().map(|root| root.join("run"))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use anyhow::Result;
    use gpui_mcp_protocol::{
        AppId, BridgeResult, Capabilities, EndpointDescriptor, InstanceId, LocalEndpoint,
        NativeWindowId, Operation, PROTOCOL_VERSION, ProcessId, WireRequest, WireResponse,
    };
    use interprocess::local_socket::{
        GenericFilePath, GenericNamespaced, ListenerOptions, ToFsName as _, ToNsName as _,
        tokio::prelude::*,
    };
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::{BridgeClient, BridgeRegistry};

    #[tokio::test]
    async fn authenticated_persistent_connection_round_trips_twice() -> Result<()> {
        let directory = tempdir()?;
        let endpoint = test_endpoint(directory.path());
        let name = match &endpoint {
            LocalEndpoint::Filesystem { path } => path.as_path().to_fs_name::<GenericFilePath>()?,
            LocalEndpoint::Namespaced { name } => {
                name.as_str().to_ns_name::<GenericNamespaced>()?
            }
        };
        let listener = ListenerOptions::new().name(name).create_tokio()?;
        #[cfg(unix)]
        if let LocalEndpoint::Filesystem { path } = &endpoint {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        let token = "ab".repeat(32);
        let server_token = token.clone();
        let task = tokio::spawn(async move {
            let mut stream = listener.accept().await?;
            for _ in 0..2 {
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await?;
                let mut payload = vec![0_u8; u32::from_be_bytes(length) as usize];
                stream.read_exact(&mut payload).await?;
                let request: WireRequest = serde_json::from_slice(&payload)?;
                assert_eq!(request.token, server_token);
                let response = WireResponse::success(
                    request.request_id,
                    BridgeResult::Pong {
                        app_id: AppId::new("test")?,
                        pid: process_id(7)?,
                        protocol_version: PROTOCOL_VERSION,
                    },
                );
                let payload = serde_json::to_vec(&response)?;
                let response_length = u32::try_from(payload.len())?;
                stream.write_all(&response_length.to_be_bytes()).await?;
                stream.write_all(&payload).await?;
            }
            anyhow::Ok(())
        });

        let path = directory.path().join("endpoint.json");
        let descriptor = EndpointDescriptor {
            protocol_version: PROTOCOL_VERSION,
            app_id: AppId::new("test")?,
            app_name: "Test".to_owned(),
            instance_id: InstanceId::new(7)
                .ok_or_else(|| anyhow::anyhow!("invalid instance ID"))?,
            pid: process_id(7)?,
            endpoint,
            token,
            native_window_id: NativeWindowId::new(17),
            capabilities: Capabilities::default(),
        };
        fs::write(&path, serde_json::to_vec(&descriptor)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }

        let client = BridgeClient::from_path(&path, None)?;
        for _ in 0..2 {
            let result = client
                .call(Operation::Ping)
                .await
                .map_err(anyhow::Error::msg)?;
            assert!(matches!(result, BridgeResult::Pong { pid, .. } if pid.get() == 7));
        }
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_ignores_and_deletes_descriptors_of_dead_processes() -> Result<()> {
        let directory = tempdir()?;
        let live_endpoint = test_endpoint(directory.path());
        let name = match &live_endpoint {
            LocalEndpoint::Filesystem { path } => path.as_path().to_fs_name::<GenericFilePath>()?,
            LocalEndpoint::Namespaced { name } => {
                name.as_str().to_ns_name::<GenericNamespaced>()?
            }
        };
        let listener = ListenerOptions::new().name(name).create_tokio()?;
        #[cfg(unix)]
        if let LocalEndpoint::Filesystem { path } = &live_endpoint {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        let live_path = directory.path().join("live.json");
        write_test_descriptor(&live_path, std::process::id(), live_endpoint)?;

        #[cfg(unix)]
        let dead_endpoint = LocalEndpoint::Filesystem {
            path: directory.path().join("dead.sock"),
        };
        #[cfg(windows)]
        let dead_endpoint = LocalEndpoint::Namespaced {
            name: format!("gpui-mcp-test-dead-{}", std::process::id()),
        };
        let dead_path = directory.path().join("dead.json");
        write_test_descriptor(&dead_path, exited_process_id()?, dead_endpoint)?;

        let app_id = AppId::new("test-app")?;
        let discovered = super::discover_clients(Some(&app_id), Some(directory.path())).await?;
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].descriptor().app_id.as_str(), "test-app");
        assert!(live_path.exists());
        assert!(!dead_path.exists(), "stale descriptor should be deleted");
        drop(listener);
        Ok(())
    }

    #[tokio::test]
    async fn registry_starts_before_any_gpui_application() -> Result<()> {
        let directory = tempdir()?;
        let registry = BridgeRegistry::new(None, None, Some(directory.path().to_path_buf()));

        assert!(
            registry
                .list_apps()
                .await
                .map_err(anyhow::Error::msg)?
                .is_empty()
        );
        assert!(registry.client().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn registry_auto_selects_one_live_application() -> Result<()> {
        let directory = tempdir()?;
        let endpoint = test_endpoint_named(directory.path(), "single");
        let listener = create_test_listener(&endpoint)?;
        let listener_task = serve_test_listener(listener);
        let path = directory.path().join("single.json");
        write_named_test_descriptor(&path, std::process::id(), endpoint, "single-app", 0x101)?;
        let registry = BridgeRegistry::new(None, None, Some(directory.path().to_path_buf()));

        let client = registry.client().await.map_err(anyhow::Error::msg)?;
        assert_eq!(client.target_id().as_str(), "single-app-0000000000000101");
        let apps = registry.list_apps().await.map_err(anyhow::Error::msg)?;
        assert_eq!(apps.len(), 1);
        assert!(apps[0].selected);
        listener_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn registry_requires_explicit_selection_for_multiple_instances() -> Result<()> {
        let directory = tempdir()?;
        let first_endpoint = test_endpoint_named(directory.path(), "first");
        let second_endpoint = test_endpoint_named(directory.path(), "second");
        let first_listener = create_test_listener(&first_endpoint)?;
        let second_listener = create_test_listener(&second_endpoint)?;
        let first_listener_task = serve_test_listener(first_listener);
        let second_listener_task = serve_test_listener(second_listener);
        let first_path = directory.path().join("first.json");
        let second_path = directory.path().join("second.json");
        write_named_test_descriptor(
            &first_path,
            std::process::id(),
            first_endpoint,
            "shared-app",
            0x101,
        )?;
        write_named_test_descriptor(
            &second_path,
            std::process::id(),
            second_endpoint,
            "shared-app",
            0x202,
        )?;
        let registry = BridgeRegistry::new(None, None, Some(directory.path().to_path_buf()));

        let apps = registry.list_apps().await.map_err(anyhow::Error::msg)?;
        assert_eq!(apps.len(), 2);
        assert!(
            registry
                .client()
                .await
                .is_err_and(|error| error.contains("select_app"))
        );
        let selected = registry
            .select("shared-app-0000000000000202")
            .await
            .map_err(anyhow::Error::msg)?;
        assert_eq!(selected.target_id.as_str(), "shared-app-0000000000000202");
        assert_eq!(
            registry
                .client()
                .await
                .map_err(anyhow::Error::msg)?
                .target_id(),
            selected.target_id
        );
        first_listener_task.abort();
        second_listener_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn rust_application_id_can_filter_automatic_discovery() -> Result<()> {
        let directory = tempdir()?;
        let alpha_endpoint = test_endpoint_named(directory.path(), "alpha");
        let beta_endpoint = test_endpoint_named(directory.path(), "beta");
        let alpha_task = serve_test_listener(create_test_listener(&alpha_endpoint)?);
        let beta_task = serve_test_listener(create_test_listener(&beta_endpoint)?);
        write_named_test_descriptor(
            &directory.path().join("alpha.json"),
            std::process::id(),
            alpha_endpoint,
            "alpha",
            0xA,
        )?;
        write_named_test_descriptor(
            &directory.path().join("beta.json"),
            std::process::id(),
            beta_endpoint,
            "beta",
            0xB,
        )?;
        let registry = BridgeRegistry::new(
            None,
            Some(AppId::new("beta")?),
            Some(directory.path().to_path_buf()),
        );

        let apps = registry.list_apps().await.map_err(anyhow::Error::msg)?;
        assert_eq!(apps.len(), 1);
        assert_eq!(apps[0].app_id.as_str(), "beta");
        assert!(apps[0].selected);
        alpha_task.abort();
        beta_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn registry_refreshes_selection_after_application_restart() -> Result<()> {
        let directory = tempdir()?;
        let first_endpoint = test_endpoint_named(directory.path(), "before-restart");
        let first_task = serve_test_listener(create_test_listener(&first_endpoint)?);
        let first_path = directory.path().join("before-restart.json");
        write_named_test_descriptor(
            &first_path,
            std::process::id(),
            first_endpoint,
            "restartable",
            0x1,
        )?;
        let registry = BridgeRegistry::new(None, None, Some(directory.path().to_path_buf()));

        let before = registry.list_apps().await.map_err(anyhow::Error::msg)?;
        assert_eq!(before[0].target_id.as_str(), "restartable-0000000000000001");
        assert!(before[0].selected);

        first_task.abort();
        fs::remove_file(&first_path)?;
        assert!(
            registry
                .list_apps()
                .await
                .map_err(anyhow::Error::msg)?
                .is_empty()
        );

        let second_endpoint = test_endpoint_named(directory.path(), "after-restart");
        let second_task = serve_test_listener(create_test_listener(&second_endpoint)?);
        write_named_test_descriptor(
            &directory.path().join("after-restart.json"),
            std::process::id(),
            second_endpoint,
            "restartable",
            0x2,
        )?;

        let after = registry.list_apps().await.map_err(anyhow::Error::msg)?;
        assert_eq!(after[0].target_id.as_str(), "restartable-0000000000000002");
        assert!(after[0].selected);
        second_task.abort();
        Ok(())
    }

    #[test]
    fn liveness_distinguishes_running_and_exited_processes() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("descriptor.json");
        fs::write(&path, b"{}")?;
        let live_pid = std::process::id();
        let dead_pid = exited_process_id()?;
        let live_pid = process_id(live_pid)?;
        let dead_pid = process_id(dead_pid)?;
        let liveness = super::ProcessLiveness::query([live_pid, dead_pid].into_iter());
        assert!(!liveness.is_stale(live_pid, &path));
        assert!(liveness.is_stale(dead_pid, &path));
        Ok(())
    }

    #[test]
    fn liveness_treats_reused_pid_as_stale() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("descriptor.json");
        fs::write(&path, b"{}")?;
        let past = std::time::SystemTime::now() - std::time::Duration::from_hours(1);
        fs::File::options()
            .write(true)
            .open(&path)?
            .set_modified(past)?;
        let mut child = long_running_process()?;
        let pid = process_id(child.id())?;
        let liveness = super::ProcessLiveness::query(std::iter::once(pid));
        let stale = liveness.is_stale(pid, &path);
        child.kill()?;
        child.wait()?;
        assert!(
            stale,
            "a process started after the descriptor was written cannot own it"
        );
        Ok(())
    }

    fn write_test_descriptor(path: &Path, pid: u32, endpoint: LocalEndpoint) -> Result<()> {
        write_named_test_descriptor(path, pid, endpoint, "test-app", u64::from(pid))
    }

    fn write_named_test_descriptor(
        path: &Path,
        pid: u32,
        endpoint: LocalEndpoint,
        app_id: &str,
        instance_id: u64,
    ) -> Result<()> {
        let descriptor = EndpointDescriptor {
            protocol_version: PROTOCOL_VERSION,
            app_id: AppId::new(app_id)?,
            app_name: "Test".to_owned(),
            instance_id: InstanceId::new(instance_id)
                .ok_or_else(|| anyhow::anyhow!("invalid instance ID"))?,
            pid: process_id(pid)?,
            endpoint,
            token: "ab".repeat(32),
            native_window_id: None,
            capabilities: Capabilities::default(),
        };
        fs::write(path, serde_json::to_vec(&descriptor)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    fn process_id(value: u32) -> Result<ProcessId> {
        ProcessId::new(value).ok_or_else(|| anyhow::anyhow!("invalid process ID"))
    }

    fn exited_process_id() -> Result<u32> {
        #[cfg(windows)]
        let mut command = std::process::Command::new("cmd");
        #[cfg(windows)]
        command.args(["/C", "exit"]);
        #[cfg(unix)]
        let command = std::process::Command::new("true");
        #[cfg(unix)]
        let mut command = command;
        let mut child = command.spawn()?;
        let pid = child.id();
        child.wait()?;
        Ok(pid)
    }

    fn long_running_process() -> Result<std::process::Child> {
        #[cfg(windows)]
        let child = std::process::Command::new("ping")
            .args(["-n", "30", "127.0.0.1"])
            .stdout(std::process::Stdio::null())
            .spawn()?;
        #[cfg(unix)]
        let child = std::process::Command::new("sleep").arg("30").spawn()?;
        Ok(child)
    }

    fn test_endpoint(directory: &Path) -> LocalEndpoint {
        test_endpoint_named(directory, "test")
    }

    fn test_endpoint_named(directory: &Path, suffix: &str) -> LocalEndpoint {
        #[cfg(unix)]
        {
            LocalEndpoint::Filesystem {
                path: directory.join(format!("{suffix}.sock")),
            }
        }
        #[cfg(windows)]
        {
            let _ = directory;
            LocalEndpoint::Namespaced {
                name: format!(
                    "gpui-mcp-test-{suffix}-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |duration| duration.as_nanos())
                ),
            }
        }
    }

    fn create_test_listener(
        endpoint: &LocalEndpoint,
    ) -> Result<interprocess::local_socket::tokio::Listener> {
        let name = match endpoint {
            LocalEndpoint::Filesystem { path } => path.as_path().to_fs_name::<GenericFilePath>()?,
            LocalEndpoint::Namespaced { name } => {
                name.as_str().to_ns_name::<GenericNamespaced>()?
            }
        };
        let listener = ListenerOptions::new().name(name).create_tokio()?;
        #[cfg(unix)]
        if let LocalEndpoint::Filesystem { path } = endpoint {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(listener)
    }

    fn serve_test_listener(
        listener: interprocess::local_socket::tokio::Listener,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Ok(stream) = listener.accept().await {
                drop(stream);
            }
        })
    }
}
