# 驱动 RPC 协议规范

本文档定义 DBFlux 如何通过本地 IPC 发现、启动 RPC 服务并与之通信。

DBFlux 当前启用两类运行时服务族：

- `RpcServiceKind::Driver` → 运行时数据库驱动
- `RpcServiceKind::AuthProvider` → 应用与 MCP 服务器中的运行时认证提供程序注册表

## 权威来源

对于已启用的驱动服务，**服务本身**是以下内容的**权威来源**：

- 驱动类型（`DbKind`）
- 驱动元数据（`DriverMetadataDto`：名称、图标、类别、能力、查询语言等）
- 连接表单定义（`DriverFormDefDto`）

DBFlux 将启动配置存放在基于 SQLite 的服务配置中。RPC 服务在 **设置 → RPC 服务** 中创建与编辑。

## 集成模型

应用启动时，DBFlux 会从 `~/.local/share/dbflux/dbflux.db` 载入已配置的 RPC 服务，然后对每项服务依次执行以下操作：

1. 发现已持久化的服务描述符，其中包含 `RpcServiceKind`
2. 按 `kind` 分支处理
3. 确保服务正在运行（必要时启动它）
4. 执行对应族的 `Hello` 握手
5. 从服务读取运行时元数据
6. 将适配后的运行时服务注册到相应的内存注册表中

任一环节失败，该服务会被跳过，但不会中断启动。驱动失败不影响认证提供程序，认证提供程序失败也不影响驱动。

需注意的行为：

- 服务配置在启动时读取。修改 RPC 服务设置后需重启 DBFlux。
- `socket_id` 原样使用（DBFlux 不会重写它）。
- 内部注册表键为 `rpc:<socket_id>`。

## 传输层

DBFlux 通过 `interprocess` 使用本地 Socket：

- **Linux**：抽象命名空间 Unix Socket（`\0name`）
- **macOS**：位于 `/tmp/` 的 Unix Socket
- **Windows**：命名管道（`\\.\pipe\...`）

消息帧格式如下：

- 4 字节小端序长度（`u32`）
- bincode 载荷

最大消息大小：`16 MiB`。

进程退出或 Socket 被 drop 时会自动清理（由 `interprocess` 提供）。

## 运行时配置

主存储位置：`~/.local/share/dbflux/dbflux.db`（`cfg_services`、`cfg_service_args`、`cfg_service_env`）

设置界面：**设置 → RPC 服务**

说明：

- `socket_id` 为必填项。
- `kind` 支持 `driver` 与 `auth_provider`。
- `command` 为可选项。
  - 若省略 `command` 且 `args` 为空，DBFlux 会认为服务已在运行。
  - 对于 `driver`，若省略 `command` 而 `args` 非空，DBFlux 会启动 `dbflux-driver-host`。
  - 对于 `auth_provider`，由 DBFlux 托管启动需要显式指定 `command`；DBFlux 不会假定存在默认的宿主可执行文件。
- `args`、`env` 与 `startup_timeout_ms` 均为可选项。
- DBFlux 由此派生内部驱动注册表键 `rpc:<socket_id>`。
- 只有 `driver` 服务会注册为数据库驱动。
- `auth_provider` 服务只注册到认证提供程序注册表中，不会获得 `rpc:<socket_id>` 驱动标识。

## 握手契约

DBFlux 先建立连接并发送 `Hello`。

当前启用的驱动 RPC API 族是 `driver_rpc`。在现有的专用驱动 RPC 传输层中，该族隐含在协议本身里，`Hello` 期间不会在线路上传输。兼容性由驱动 RPC 端点加上所选协议主版本号共同保证；次版本号是追加式的，在同一主版本线内以确定方式协商得出。

客户端请求：

```rust
DriverRequestBody::Hello(DriverHelloRequest {
    client_name: "dbflux_driver_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 0),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 2),
        ProtocolVersion::new(1, 3),
    ],
    requested_capabilities: vec![
        DriverCapability::Cancellation,
        DriverCapability::ChunkedResults,
        DriverCapability::SchemaIntrospection,
        DriverCapability::MultiDatabase,
    ],
})
```

服务端响应必须包含：

- `selected_version`
- `capabilities`
- `driver_kind`
- `driver_metadata`
- `form_definition`

示例：

```rust
DriverResponseBody::Hello(DriverHelloResponse {
    server_name: "my-driver".to_string(),
    server_version: "1.0.0".to_string(),
    selected_version: DRIVER_RPC_VERSION,
    capabilities: vec![DriverCapability::SchemaIntrospection],
    driver_kind: DbKind::SQLite,
    driver_metadata: DriverMetadataDto {
        id: "my-driver".to_string(),
        display_name: "My Driver".to_string(),
        description: "External RPC driver".to_string(),
        category: DatabaseCategory::Relational,
        query_language: QueryLanguageDto::Sql,
        capabilities: DriverCapabilities::RELATIONAL_BASE.bits(),
        default_port: None,
        uri_scheme: "mydriver".to_string(),
        icon: Icon::Database,
    },
    form_definition: DriverFormDefDto {
        tabs: vec![
            // ...
        ],
    },
})
```

若多个兼容次版本存在重叠，宿主进程必须选择双方共有的最高次版本号。

若不存在兼容版本，则返回 `DriverRpcErrorCode::VersionMismatch`。

`Hello` 之后，所有请求与响应的消息信封（envelope）都必须使用协商确定的 `selected_version`。对端若收到握手后版本号不一致的信封，必须按版本不匹配予以拒绝。

当前的校验边界：

- DBFlux 会持久化每项服务的 API 族与版本元数据，用于服务发现以及日后的运行时接缝（扩展点）。
- 当前实际生效的驱动握手会校验协商得出的协议版本，但不会在线路上传输或另行重新校验 API 族字符串——因为驱动 RPC 传输层本身就是按族区分的。

### 驱动的审计事件上报（v1.2+）

声明了 `DriverCapability::AuditEmit` 的驱动（驱动 RPC ≥ 1.2）可在任意请求/响应周期中发送 `EmitAuditEvent` 中间帧（`done=false`），从而写入宿主审计日志。宿主应用在将事件持久化到 `aud_audit_events` 之前，会对每个事件做净化处理。

允许的类别：`Connection`（连接）、`Query`（查询）、`System`（系统）。其他类别会被静默丢弃。

宿主应用会覆盖身份字段（`actor_type` → `ExternalDriver`、`actor_id`、`source_id`、`driver_id`、`correlation_id`）以及来自 `AppState` 的连接上下文，并将 `details_json` 截断到配置的上限。限流与认证提供程序共用同一套策略：每个 `socket_id` 每 60 秒 100 个事件；超出部分会被丢弃，但不会导致会话出错。协商版本低于 v1.2 或未声明该能力的对端保持静默。完整的净化契约参见[审计 → 外部审计事件上报](AUDIT.md)。

### 键值读取大小限制（v1.3+）

`KeyGetRequest` 带有 `max_value_bytes: Option<u64>`，即一次 `KvGetKey` 调用可传输的值字节数的可选上限。`None` 表示不设上限，协商版本低于 v1.3 的对端同样是这一结果：该字段是追加式的，在线路载荷中缺失时默认为 `None`，因此较旧的驱动与宿主进程仍会获取完整值。

`KeyGetResult` 带有 `load_state: KeyLoadState`，用于表明 `value` 是否为完整载荷：

- `Loaded` — 已获取完整值。该字段在线路载荷中缺失时的默认值。
- `Truncated { returned_bytes, total_bytes }` — 仅获取了部分值（例如驱动端对集合类型的条目数设了上限）；驱动已知完整大小时，`total_bytes` 为完整大小。
- `TooLarge { size_bytes, limit_bytes }` — 因超过 `max_value_bytes` 而未获取该值；`value` 为空。

这两个字段都是既有请求/响应类型上的普通结构体字段（带 `#[serde(default)]`），并非新增的能力标志：`Hello` 协商不对其设限，忽略 `max_value_bytes` 的驱动只会始终返回 `Loaded`。

## 认证提供程序 RPC 契约

当前启用的认证提供程序 RPC API 族为 `auth_provider_rpc`，版本 `1.3`。

DBFlux 将持久化的 `api_family` / `api_major` 元数据用作启动前预检。随后，兼容的记录会在 `Hello` 期间协商双方共有的最高次版本号。

客户端请求：

```rust
AuthProviderRequestBody::Hello(AuthProviderHelloRequest {
    client_name: "dbflux_ipc".to_string(),
    client_version: "<version>".to_string(),
    supported_versions: vec![
        ProtocolVersion::new(1, 3),
        ProtocolVersion::new(1, 2),
        ProtocolVersion::new(1, 1),
        ProtocolVersion::new(1, 0),
    ],
    auth_token: Some("<token>".to_string()),
})
```

服务端响应必须包含：

- `selected_version`
- `provider_id`
- `display_name`
- `form_definition`

v1.2 的 `Hello` 响应还额外携带 `secret_dependency_opt_in`（`bool`），用于声明该提供程序是否选择接收依赖映射中的密钥字段值，以便进行动态选项查询。若为 `false`（默认值），DBFlux 在转发 `FetchDynamicOptions` 请求前会从依赖映射中剔除密钥值。

v1.3 的 `Hello` 响应还额外携带 `audit_emit_opt_in`（`bool`）。将其设为 `true` 即可启用审计事件上报（见下文）。默认值为 `false`。

支持的请求 / 响应流程：

| 请求 → 响应 | 用途 |
|---|---|
| `Hello` → `Hello` | 协议协商 + 提供程序身份 |
| `ValidateSession` → `SessionState` | 校验缓存的认证状态 |
| `Login` → `LoginUrlProgress?` + `LoginResult` | 可选的验证 URL + 最终登录结果 |
| `ResolveCredentials` → `Credentials` | 解析运行时凭据字段 |
| `FetchDynamicOptions` → `DynamicOptions` | 解析 `DynamicSelect` 表单字段的动态下拉选项（v1.2+） |
| （任意请求）→ `EmitAuditEvent`（中间帧） | 审计事件上报（v1.3+） |

说明：

- `Login` 在 `LoginResult` 之前可以发出零个或一个 `LoginUrlProgress` 事件。
- 若未发送任何进度事件，DBFlux 会将验证 URL 回调视为 `None`。
- `FetchDynamicOptions` 仅在协商版本不低于 `1.2` 时可用。协商版本低于 v1.2 的提供程序会直接从宿主应用得到永久性的「不支持」结果，而不发生 IPC 往返。
- `detect_importable_profiles`、认证配置文件回写 Hook，以及提供程序特有的值提供程序注册，本次改动中有意不纳入该 RPC 契约的范围。
- 认证提供程序的运行时失败会通过既有的 `DbError` 处理机制呈现，且不会中断启动。

### 认证提供程序的审计事件上报（v1.3+）

协商版本为 v1.3+ 且设置了 `audit_emit_opt_in: true` 的认证提供程序，可在任意请求/响应周期中发送 `EmitAuditEvent` 中间帧（`done=false`）。宿主应用会对其净化后写入 `aud_audit_events`。

允许的类别仅 `Connection`（连接）。其他类别会被静默丢弃。

`AuditEventEmitDto` 载荷的结构与驱动上报帧一致。宿主应用会覆盖身份字段（`actor_type`、`actor_id`、`source_id`、`driver_id`、`correlation_id`）。限流与驱动共用同一套策略：每个 `socket_id` 每 60 秒 100 个事件。

## 表单契约

DBFlux 中显示的连接表单由 `Hello` 返回的 `form_definition` 构建而成。

- 服务定义字段、标签页与分区。
- DBFlux 在界面中校验必填字段。
- 连接或保存时，DBFlux 通过 `OpenSession` 配置文件 JSON 中的 `DbConfig::External.values` 发送收集到的值。

若 `form_definition.tabs` 为空，连接表单将不显示任何驱动特有的输入项。

## 会话生命周期

1. `Hello`
2. `OpenSession`
3. 请求 / 响应操作
4. `CloseSession`

`OpenSession` 仍会返回带元数据的 `SessionOpened`。请使其与 `Hello` 中的元数据保持一致。

DBFlux 会将保存的配置文件 JSON 发送给 `OpenSession`。对于外部驱动，其配置文件结构如下：

```rust
DbConfig::External {
    kind: DbKind,
    values: HashMap<String, String>,
}
```

`values` 包含从你的 `form_definition` 收集到的字段值。

服务应解析 `profile_json`，预期其为 `DbConfig::External`，并在服务端再次校验必填字段。

## 请求 / 响应总览

| 请求 → 响应 | 用途 |
|---|---|
| `Hello` → `Hello` | 协议协商 + 驱动身份 |
| `OpenSession` → `SessionOpened` | 打开连接 / 会话 |
| `CloseSession` → `SessionClosed` | 关闭会话 |
| `Ping` → `Pong` | 存活探测 |
| `Execute` → `ExecuteResult` | 执行查询 |
| `Schema` → `Schema` | Schema 快照 |
| `ListDatabases` → `Databases` | 数据库列表 |

该协议还支持浏览、CRUD、键值与代码生成类操作。完整的枚举集合参见 `crates/dbflux_ipc/src/driver_protocol.rs`。

## 驱动的审计事件上报（v1.2+）

协商协议版本为 v1.2 或更高的驱动，可将审计事件作为中间响应帧（`done=false`）回传给宿主应用。宿主应用对其进行净化、限流后写入 `aud_audit_events`。

### 选择启用

在 `Hello` 响应的 `capabilities` 列表中加入 `DriverCapability::AuditEmit`。未声明该能力的驱动，其 `EmitAuditEvent` 帧会被宿主应用静默丢弃。

### 发送审计帧

在最终响应之前的任意时刻，发出一个 `DriverResponseEnvelope`，其 `done = false`、`body = DriverResponseBody::EmitAuditEvent(AuditEventEmitDto { .. })`：

```rust
DriverResponseEnvelope {
    protocol_version: negotiated_version,
    request_id: request.request_id,
    session_id: request.session_id,
    done: false,
    body: DriverResponseBody::EmitAuditEvent(AuditEventEmitDto {
        ts_ms: chrono::Utc::now().timestamp_millis(),
        level: EventSeverityDto::Info,
        category: EventCategoryDto::Connection,
        action: "session.open".to_string(),
        outcome: EventOutcomeDto::Success,
        summary: "Database session opened".to_string(),
        object_type: None,
        object_id: None,
        duration_ms: Some(42),
        error_code: None,
        error_message: None,
        details_json: None,
    }),
}
```

然后照常发送最终响应。

### 由宿主应用提供的字段

宿主应用始终会覆盖以下字段；请勿将其写入该 DTO（`AuditEventEmitDto` 中有意不包含这些字段）：

- `actor_type`、`actor_id`、`source_id`、`driver_id` — 一律设为 `ExternalDriver` / `rpc:<socket_id>`
- `connection_id`、`database_name` — 由当前活动会话上下文解析得出
- `correlation_id` — 每个会话一个，由宿主应用生成

### 允许的类别

驱动可上报 `Connection`、`Query` 与 `System` 事件。其他类别会被静默丢弃。

### 限流

每个 `socket_id` 每 60 秒 100 个事件。超出的帧会被丢弃，并计入 `AuditService::external_audit_dropped_count()`。

## 错误处理

通过 `DriverResponseBody::Error(DriverRpcError { ... })` 返回结构化错误。

常见错误码：

- `InvalidRequest`
- `UnsupportedMethod`
- `VersionMismatch`
- `SessionNotFound`
- `Timeout`
- `Cancelled`
- `Transport`
- `Driver`
- `Internal`

配置文件或表单值格式有误时使用 `InvalidRequest`；有意不实现的方法使用 `UnsupportedMethod`。认证提供程序 RPC 使用与之对应的 `AuthProviderRpcErrorCode` 错误码集，语义一致（`VersionMismatch`、`UnsupportedMethod`、`Timeout`、`Transport` 等）。

## 进程生命周期与清理

当 DBFlux 自行启动服务进程时（通过 `command` 或受支持的默认宿主命令），该进程会作为受管宿主进程被跟踪。

DBFlux 关闭时：

- 所有受跟踪的受管宿主进程都会被终止（`kill` + `wait`）
- 在 DBFlux 之外手动启动的宿主进程不会被跟踪，也不会被终止

这确保 DBFlux 只清理自己启动的进程。

若受管宿主进程提前退出，或在 Socket 就绪前超时，DBFlux 会报告该服务 ID 以及最近 stdout/stderr 的有限尾部输出，以便排查问题。

## 最小实现清单

你的服务应当：

1. 通过 `interprocess` 绑定 Socket
2. 处理 `Hello` 并返回元数据与 `kind`
3. 在 `Hello` 中返回表单定义
4. 处理 `OpenSession` / `CloseSession`
5. 实现至少一个可用的操作（如 `Execute`）
6. 对未实现的操作返回 `UnsupportedMethod`

建议：

7. 在 `OpenSession` 中校验 `DbConfig::External.values`
8. 对缺失或非法的表单值返回明确的 `InvalidRequest` 错误
9. 保持 `Hello` 元数据与 `SessionOpened` 元数据一致
10. 在每个 `Hello` 之后的信封中标明协商确定的版本号，而不是假定使用最新常量

## 本仓库中的可运行示例

可参考：

- `examples/custom_driver/src/main.rs`
- `examples/custom_driver/README.md`
- `examples/custom_auth_provider/src/main.rs`
- `examples/custom_auth_provider/README.md`

这些示例与当前启用的驱动服务集成模型兼容。

快速验证步骤：

1. 在 **设置 → RPC 服务** 中新建一个 **驱动** 服务
2. 将 `command` 指向你构建好的示例可执行文件
3. 将 `args` 设为 `--socket <your-socket-id>`
4. 重启 DBFlux
5. 通过服务暴露的界面表单，创建一个连接（驱动示例）或一个认证配置文件（认证提供程序示例）

## 参考资料

- `crates/dbflux_ipc/src/driver_protocol.rs`
- `crates/dbflux_driver_ipc/src/transport.rs`
- `crates/dbflux_driver_host/src/main.rs`
- `crates/dbflux/src/app.rs`
- `crates/dbflux_driver_ipc/src/driver.rs`
- `docs/RPC_SERVICES_CONFIG.md`
