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
- postcard 载荷

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

当前启用的驱动 RPC API 族是 `driver_rpc`。在现有的专用驱动 RPC 传输层中，该族隐含在协议本身里，`Hello` 期间不会在线路上传输。兼容性由驱动 RPC 端点加上所选协议主版本号共同保证；次版本号在同一主版本线内以确定方式协商得出。v1.7 及之前的次版本号是追加式的；v1.8 是一次线路格式修正，改变了部分现有载荷的编码方式，而之前能够解码的所有形态都逐字节相同（见下文的**线路格式修正**）。

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
        ProtocolVersion::new(1, 4),
        ProtocolVersion::new(1, 5),
        ProtocolVersion::new(1, 6),
        ProtocolVersion::new(1, 7),
        ProtocolVersion::new(1, 8),
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

### 批量读取模式列（v1.4+）

`SchemaColumns { database, schema }` 通过一次调用获取某个模式下所有关系的列，并返回 `SchemaColumns { columns: Vec<SchemaColumnInfo> }`，其中每条记录都在常规的 `ColumnInfo` 旁带有其 `table_name`。宿主进程将该请求分发到连接的 `schema_columns` 接缝，处理方式与 `SchemaIndexes` 和 `SchemaForeignKeys` 完全一致。

与上面的 v1.3 字段不同，该操作在线路上并非追加式变更：帧使用 postcard 编码，它以 varint 判别式索引而非名称来标记枚举变体。因此 `SchemaColumns` 被追加在请求与响应枚举中最后一个 v1.3 变体之后；若将其插入枚举中部，会使之后所有变体的索引整体后移，协商 v1.3 的对端会把移位后的变体解码成错误的变体，并使帧的剩余部分无法解码，导致流失去同步。客户端还在本地进行版本拦截：当协商的次版本低于 1.4 时，`IpcConnection::schema_columns` 检查 `Hello` 期间选定的版本，直接返回 `DbError::NotSupported` 而不发送任何内容，因此旧宿主进程根本不会收到新变体。消费者对该错误的处理与 trait 默认的 `NotSupported` 相同：回退到按表的 `table_details` 加载。

### 键数量（v1.5+）

`KvKeyCount { keyspace }` 请求某个键空间中的键数量（`None` 表示会话当前的键空间），并返回 `KvKeyCountResult { count }`。宿主进程将请求分派到连接的 `KeyValueApi::key_count` 接口；未实现它的驱动会以 trait 默认的 `NotSupported` 响应，客户端收到的是 `UnsupportedMethod`。

出于上文所述的线路索引原因，这两个变体都追加在各自枚举中最后一个 v1.4 变体之后。客户端同样在本地进行版本拦截：当协商的次版本低于 1.5 时，`IpcConnection::key_count` 直接返回 `DbError::NotSupported` 而不发送任何内容。键浏览器将该错误视为“无总数”，只显示本页键数。

### 请求行数上限（v1.6+）

`Execute` 和 `ExecuteWithHandle` 可以携带编辑器行数上限 `QueryRequestDto::limit`。由于 `QueryResultDto` 无处容纳该标志，宿主以 `ExecuteResultWithTruncation { result, rows_truncated }` 和 `ExecuteWithHandleResultWithTruncation { handle_id, result, rows_truncated }` 代替 `ExecuteResult` 和 `ExecuteWithHandleResult` 作答。仅当驱动为遵守上限而丢弃了行时，`rows_truncated` 才为 `true`。

出于上文所述的线路索引原因，这两个响应变体都追加在 `KvKeyCountResult` 之后。宿主只在协商结果恰为 v1.6 的会话中，对执行调用发送它们；v1.7 用下文的结果集响应取代它们，更早的会话继续使用 v1.5 的响应。哪些请求会携带上限，见下文的**受保护的执行**。

### 结果集（v1.7+）

一批语句或一个存储过程可能返回多个结果集，每个结果集都可能各自省略行。从 v1.7 起，宿主以全部结果集作答，而不只是第一个：

| 请求 | v1.7 响应 |
| --- | --- |
| `Execute` | `ExecuteResultSets { results }` |
| `ExecuteWithHandle` | `ExecuteWithHandleResultSets { handle_id, results }` |
| `BrowseTable`、`BrowseCollection`、`Explain`、`DescribeTable` | `BrowseResultSets { results }` |

`results` 是一个 `QueryResultSetsDto { primary, additional }`。每一项都是 `ResultSetDto { result, rows_truncated }`：`primary` 是第一个结果集，`additional` 按驱动返回的顺序保存其余结果集。只有当驱动从该结果集中丢弃了行时，`rows_truncated` 才为 `true`，因此从不限制行数的驱动总是发送 `false`。

出于上文所述的线路索引原因，这三个变体追加在 `ExecuteWithHandleResultWithTruncation` 之后。宿主只在协商为 v1.7 或更高版本的会话中发送它们。v1.6 会话继续使用 `ExecuteResultWithTruncation`、`ExecuteWithHandleResultWithTruncation` 和 `BrowseResult`，更早的会话继续使用 v1.5 的响应；在这些会话中只有主结果集会通过线路传输。无论协商的版本如何，客户端都接受所有变体，因此宿主无需知道自己在与哪个客户端构建通信。

### 线路格式修正（v1.8）

v1.8 没有新增任何请求或响应变体。它改变了部分现有载荷的编码方式，使协议定义的每一种载荷都能用 postcard 解码：

| 载荷 | v1.8 编码 |
| --- | --- |
| JSON 文档：`BrowseCollection`（`CollectionBrowseRequestDto`）的 `filter`、`projection` 和 `sort`，`CountCollection`（`CollectionCountRequestDto`）的 `filter`，`UpdateDocument`（`DocumentUpdateDto`）的过滤条件和更新内容，`InsertDocument`（`DocumentInsertDto`）的文档，`DeleteDocument`（`DocumentDeleteDto`）的过滤条件，以及 `CrudResult`（`CrudResultDto`）的 `metadata_extra` 值 | JSON 文本：一个存放序列化后 JSON 的 postcard 字符串，在协议边界处重新解析 |
| 过去带有 `skip_serializing_if` 的可选字段和列表字段，例如 browse 和 count 请求的 `semantic_filter` 与 `columns`、`ColumnInfo` 的 `enum_values`、`CrudResult` 的 `metadata_extra`、`ColumnAssignment` 的 `type_name`、`ExecutionContext` 的 `source` 以及 `FormSection` 的 `icon` | 始终编码，未设置时编码为 `None` 或空列表 |
| `IndexData` | 外部标记：先是变体索引，然后是索引列表 |

原始的 `serde_json::Value` 只能通过 `deserialize_any` 反序列化，而 postcard 不支持它，因此这类值能写入却永远读不回来。被跳过的字段会让帧变短，而解码器仍会读取该字段，导致其后的所有字段错位。内部标记的 `IndexData` 根本无法编码，因此任何带索引的表的 `TableDetails` 和 `GenerateCode` 都会失败。

该编码不受协商版本的控制。在 v1.8 之前能够解码的形态，在 v1.8 中逐字节相同；只有任何对端都无法解码的形态，其字节才会改变，因此使用更早次版本的对端仍可正常工作。

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
| `Execute` → `ExecuteResult`（自 v1.7 起为 `ExecuteResultSets`） | 执行查询 |
| `Schema` → `Schema` | Schema 快照 |
| `ListDatabases` → `Databases` | 数据库列表 |

该协议还支持浏览、CRUD、键值与代码生成类操作。完整的枚举集合参见 `crates/dbflux_ipc/src/driver_protocol.rs`。

**受保护的执行：**在协商为 v1.6 或更高版本的会话中，元数据声明了 `DriverCapabilities::REQUEST_ROW_LIMIT` 的驱动会收到 `QueryRequestDto::limit`。驱动必须在收集行时执行该上限（在所有结果集之间只计一次），并且仅在丢弃了行时设置 `rows_truncated`；否则必须在执行任何内容之前拒绝该请求。驱动未声明该能力时，宿主会话以 `UnsupportedMethod` 拒绝带上限的请求。低于 v1.6 时客户端从不发送上限：`IpcConnection` 不宣告 `REQUEST_ROW_LIMIT`，并在 `QueryRequest.limit` 为 `Some(n)`（包括 `Some(0)`）时，于发送 RPC 之前以 `NotSupported` 拒绝 `Execute` 和 `ExecuteWithHandle`。在所有版本中，客户端都以同样方式拒绝 `statement_timeout` 或只读强制，宿主也会拒绝 `statement_timeout`。低于 v1.7 时只有主结果集会通过线路传输，因此额外的结果集及其标志会被丢弃；从 v1.7 起，每个结果集都带着自己的标志通过线路（见上文的**结果集**）。客户端信任驱动的声明：声明了 `REQUEST_ROW_LIMIT` 却忽略上限的驱动，会返回未受限且标记为未截断的行。浏览和 CRUD 操作不受影响。

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
