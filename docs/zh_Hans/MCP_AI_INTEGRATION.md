# DBFlux AI + MCP 集成指南

本文档说明如何通过独立的 MCP 服务器二进制，把 AI 智能体与 DBFlux 集成起来。

它有意明确区分「当前已提供」与「仍待实现」的能力，以免集成方依赖尚未实现的行为。

## 1. 架构概览

DBFlux 通过 `dbflux mcp` 子命令提供 MCP 服务器功能，该子命令基于 stdio 使用 Model Context Protocol。AI 客户端（Claude Desktop、Cursor 等）以子进程方式启动这个二进制，并通过以换行分隔的 JSON-RPC 2.0 进行通信。

```
AI 客户端（Claude Desktop / Cursor / 任意 MCP 客户端）
        |  stdio  （JSON-RPC 2.0，以换行分隔）
        v
  dbflux mcp                    ← 已集成进 dbflux 主二进制
        |
        +--  dbflux_mcp          治理、授权、工具目录
        +--  dbflux_core         连接配置、配置、驱动程序 trait
        +--  dbflux_driver_*     真实的数据库驱动程序
        +--  dbflux_policy       策略引擎
        +--  dbflux_audit        审计追踪（SQLite）
```

MCP 服务器与 DBFlux 界面应用是两个相互独立的进程。它们共享同一个统一的 SQLite 数据库 `~/.local/share/dbflux/dbflux.db`（连接配置、治理、审计、历史记录、会话）。在界面中配置的治理规则（受信客户端、角色、策略、按连接的设置），由服务器在启动时从该数据库读取。`--config-dir` 参数仅为兼容命令行而保留，并不会改变统一数据库的位置；治理与审计始终从 `~/.local/share/dbflux/dbflux.db` 读取。

## 2. 运行 MCP 服务器

### 构建

```bash
# 全部驱动程序，含 MCP 支持（默认）
cargo build -p dbflux --release

# 仅 SQLite，含 MCP
cargo build -p dbflux --features sqlite,mcp --release

# 不含 MCP 支持（AI 集成已禁用）
cargo build -p dbflux --no-default-features --features sqlite,postgres,mysql,mongodb,redis,dynamodb,lua,aws --release
```

MCP 服务器已集成进 `dbflux` 主二进制。

### 用法

```
dbflux mcp --client-id <id> [--config-dir <path>]
```

| 参数 | 说明 |
|---|---|
| `--client-id <id>` | 该 AI 客户端的身份。必须与治理设置中已注册的受信客户端匹配。**必填。** |
| `--config-dir <path>` | 仅为兼容命令行而保留。治理/审计数据库始终解析到统一的 `~/.local/share/dbflux/dbflux.db`，该参数不会改变其位置。如需隔离的测试环境，请改为覆盖 `HOME` / `XDG_DATA_HOME`。 |

### Claude Desktop 配置

在 `~/Library/Application Support/Claude/claude_desktop_config.json`（macOS）或你所在平台的对应路径中添加：

```json
{
  "mcpServers": {
    "dbflux": {
      "command": "/path/to/dbflux",
      "args": ["mcp", "--client-id", "claude-desktop"]
    }
  }
}
```

`client-id` 的取值必须与你在 DBFlux 界面中 **设置 → MCP → 客户端** 下创建的受信客户端条目一致。

**注意**：如果你在构建 DBFlux 时未启用 `mcp` feature（`--no-default-features`），MCP 服务器将不可用。

## 3. 治理模型（核心概念）

每一个 AI 请求都会按顺序经过以下全部层面：

1. **受信客户端**：请求方身份必须已注册且处于启用状态。
2. **连接的 MCP 开关**：目标连接必须已启用 MCP。
3. **策略分配**：执行者必须在该连接上拥有一个带范围的分配。
4. **工具 + 执行类别白名单**：工具 ID 与其执行类别都必须被所分配的策略允许。
5. **审批流程**：写入/破坏性流程可以要求在执行前经人工审批。
6. **审计追踪**：每一个决策都会追加到统一 SQLite 数据库的 `aud_audit_events` 表中，可查询、可导出。完整的事件结构参见 `docs/AUDIT.md`。

这六层都会在每一个 `tools/call` 请求中，于服务器进程内依次执行；任何一层都无法从客户端绕过。

## 4. 标准工具清单（v1）

| 分组 | 工具 ID | 执行类别 | 作用 |
|---|---|---|---|
| 连接 | `list_connections` | metadata | 枚举所有已配置的数据库连接 |
| 连接 | `connect` | metadata | 针对某个已配置连接打开一个会话 |
| 连接 | `disconnect` | metadata | 关闭一个已打开的会话 |
| 连接 | `get_connection_info` | metadata | 获取驱动程序能力与连接元数据 |
| Schema | `list_databases` | metadata | 列出某个连接上可访问的所有数据库 |
| Schema | `list_schemas` | metadata | 列出数据库内的 Schema |
| Schema | `list_tables` | metadata | 列出 Schema 内的表与视图 |
| Schema | `list_collections` | metadata | 列出 MongoDB 的集合 |
| Schema | `describe_object` | metadata | 获取某个表的列/字段定义与索引 |
| 读取 | `select_data` | read | 对表或集合执行结构化的 SELECT。不支持的 `joins` 会被明确拒绝 |
| 读取 | `count_records` | read | 返回目标的行数/文档数 |
| 读取 | `aggregate_data` | read | 运行只读的聚合管道 |
| 读取 | `explain_query` | read | 显示查询执行计划，而不执行目标变更 |
| 读取 | `preview_mutation` | read | 返回写入查询的只读预览/计划。始终只读，变更绝不会被执行 |
| 写入 | `insert_record` | write | 插入单条记录 |
| 写入 | `update_records` | write | 更新符合筛选条件的记录 |
| 写入 | `upsert_record` | write | 按键插入或更新单条记录 |
| 写入 | `delete_records` | destructive | 删除符合筛选条件的记录 |
| 破坏性 | `truncate_table` | destructive | 清空表中的所有行 |
| DDL | `create_table` | admin | 创建表 |
| DDL | `alter_table` | admin_safe / admin / admin_destructive | 修改表；执行类别按变更类型逐项计算 |
| DDL | `create_index` | admin | 创建索引 |
| 破坏性 DDL | `drop_index` | admin_destructive | 删除索引 |
| DDL | `create_type` | admin | 创建用户自定义类型 |
| 破坏性 DDL | `drop_table` | admin_destructive | 删除表 |
| 破坏性 DDL | `drop_database` | admin_destructive | 删除数据库 |
| 脚本 | `list_scripts` | metadata | 列出脚本目录中的已保存脚本 |
| 脚本 | `get_script` | read | 获取某个已保存脚本的源码 |
| 脚本 | `create_script` | write | 将新脚本保存到脚本目录 |
| 脚本 | `update_script` | write | 覆盖已有的已保存脚本 |
| 脚本 | `delete_script` | admin | 永久删除脚本 |
| 脚本 | `execute_script` | computed | 对某个连接执行已保存的脚本。执行类别由脚本内容推导 |
| 审批 | `request_execution` | admin | 提交一项变更，执行前需人工审批 |
| 审批 | `list_pending_executions` | read | 查看所有等待审批的执行 |
| 审批 | `get_pending_execution` | read | 获取某个待审批执行的详情 |
| 审批 | `approve_execution` | admin | 批准一项待处理的变更（仅 admin） |
| 审批 | `reject_execution` | admin | 驳回并丢弃一项待处理的变更（仅 admin） |
| 审计 | `query_audit_logs` | read | 搜索并筛选审计追踪 |
| 审计 | `get_audit_entry` | read | 按 ID 获取单条审计日志 |
| 审计 | `export_audit_logs` | read | 以 CSV 或 JSON 下载审计日志条目 |

暂缓提供的工具（在 v1 中会在请求时明确拒绝）：

- `estimate_query_cost`
- `get_execution_status`

## 5. 执行类别

策略在两个层面把关工具：工具 ID 本身，以及执行分类。只有当两者都与策略的白名单匹配时，请求才会被放行。

| 执行类别 | 覆盖范围 |
|---|---|
| `metadata` | Schema 检查 —— 列出数据库、表，以及描述对象 |
| `read` | 运行只读查询、获取数据，以及只读预览 |
| `write` | 插入、更新，或运行会修改数据的脚本 |
| `destructive` | DELETE、DROP、TRUNCATE 及其他不可撤销的操作 |
| `admin_safe` | 安全的 DDL 操作，例如追加式 Schema 变更与创建索引 |
| `admin` | 有风险的 DDL 操作、审批、审计导出与特权动作 |
| `admin_destructive` | 不可逆的管理操作，例如删除或清空 Schema 对象 |

## 6. 内置策略与角色

三个策略与三个角色作为不可变的内置项随程序提供。无论磁盘上持久化了什么，它们始终存在，并且不能被删除或修改。

### 内置策略

| ID | 允许的执行类别 | 范围 |
|---|---|---|
| `builtin/read-only` | metadata, read | 所有发现与 Schema 工具；只读查询与预览工具；脚本列出/获取；审计读取工具 |
| `builtin/write` | metadata, read, write | 所有只读工具，加上具备写入能力的脚本，以及请求/审批提交流程 |
| `builtin/admin` | metadata, read, write, destructive, admin_safe, admin, admin_destructive | 本分支暴露的全部标准工具 |

### 内置角色

共有三个内置角色：`builtin/read-only`、`builtin/write` 和 `builtin/admin`。它们各自被分配同名的策略。

内置项在启动时注入，界面应用（`AppState`）与 MCP 服务器（通过 `dbflux_mcp_server::governance` 中的 `builtin_policies()` / `builtin_roles()` 循环）都是如此。它们从不写入磁盘。任何删除内置项的尝试都会返回错误。

对多数集成场景，建议先分配 `builtin/read-only`，只有在明确需要写入权限时，再升级到 `builtin/write` 或某个自定义策略。

## 7. 在 DBFlux 界面中完成运维配置

启动 MCP 服务器之前，请先在 DBFlux 界面中配置治理规则。

1. **设置 → MCP → 客户端标签页**
   - 把每个 AI 智能体注册为受信客户端（稳定的 `client_id`、便于阅读的名称、可选的颁发者）。
   - 将客户端标记为启用。未启用的客户端会在第一道授权关卡被拒绝。

2. **设置 → MCP → 角色标签页**
   - 内置角色（`Read Only`、`Write`、`Admin`）显示在顶部，且不能被删除。
   - 可以用多选下拉框组合多个策略，来创建自定义角色。

3. **设置 → MCP → 策略标签页**
   - 内置策略显示在顶部，且不能被修改。
   - 可以通过勾选工具与执行类别的复选框来创建自定义策略。

4. **连接管理器 → MCP 标签页**
   - 为目标连接启用 MCP。
   - 从已填充的下拉框中，为该连接选择执行者（受信客户端）、角色和/或策略。

5. **Workspace → 待审批项**
   - 审阅并批准/驳回触发了审批流程的写入/破坏性请求。

6. **Workspace → 审计**
   - 按执行者/工具/决策/时间范围筛选，并导出 CSV/JSON。

MCP 服务器在启动时从磁盘读取这些设置。如果你在服务器运行期间修改了界面中的治理设置，请重启服务器以加载新配置。

## 8. 持久化文件与路径

DBFlux 把所有状态持久化在一个统一的 SQLite 数据库和少数几个辅助目录中。路径由 `dirs` 解析（Linux 上遵循 `XDG_*`，macOS 上为 `~/Library`）。

Linux 上的典型默认值：

| 路径 | 内容 |
|---|---|
| `~/.local/share/dbflux/dbflux.db` | 统一数据库：连接配置、认证、SSH 隧道、治理、审计事件、历史记录、会话、界面状态 |
| `~/.local/share/dbflux/sessions/` | 用于自动保存与会话恢复的临时文件和影子文件 |
| `~/.local/share/dbflux/scripts/` | 用户编写的脚本目录 |

`dbflux.db` 数据库按前缀包含所有领域表：

- `cfg_*` —— 配置（连接配置、认证、治理、服务、Hook、驱动程序）
- `st_*` —— 状态（会话、查询历史、界面状态、已保存的查询）
- `aud_audit_events` —— 统一的审计日志（MCP 事件、查询事件、连接、Hook、脚本）
- `sys_*` —— 系统（迁移、旧版导入跟踪）

内置的策略与角色在启动时合成，从不写入磁盘。

测试时请注意：不要使用真实的用户目录。请给二进制传入 `--config-dir`，或把 `HOME` / `XDG_CONFIG_HOME` / `XDG_DATA_HOME` 指向临时路径，以获得隔离的运行环境。`dbflux_audit::temp_sqlite_path(name)` 这个辅助函数可以为审计测试生成隔离路径。

## 9. Rust 集成模式

### 进程内（界面应用，`AppState`）

```rust
// 注册一个受信客户端
state.upsert_mcp_trusted_client(TrustedClientDto {
    id: "agent-a".into(),
    name: "Agent A".into(),
    issuer: None,
    active: true,
})?;

// 为该智能体在某连接上分配一个内置角色
state.save_mcp_connection_policy_assignment(ConnectionPolicyAssignmentDto {
    connection_id: connection_id.to_string(),
    assignments: vec![ConnectionPolicyAssignment {
        actor_id: "agent-a".into(),
        role_ids: vec!["builtin/read-only".into()],
        policy_ids: vec![],
    }],
})?;
```

### 删除前检查是否为内置 ID

```rust
if dbflux_mcp::is_builtin(id) {
    // 内置项不可修改或删除
}
```

### 授权调用（MCP 服务器内部使用）

```rust
use dbflux_mcp::server::authorization::{AuthorizationRequest, authorize_request};

let outcome = authorize_request(
    &trusted_clients,
    &policy_engine,
    &audit_service,
    &AuthorizationRequest {
        identity: RequestIdentity { client_id: "agent-a".into(), issuer: None },
        connection_id: connection_id.to_string(),
        tool_id: "select_data".to_string(),
        classification: ExecutionClassification::Read,
        mcp_enabled_for_connection: true,
    },
    now_epoch_ms(),
)?;

if !outcome.allowed {
    // deny_code 与 deny_reason 说明了原因
}
```

## 10. 集成清单

在把 AI 客户端指向 MCP 服务器之前：

- [ ] `dbflux` 已带 MCP 支持构建（默认启用，或使用 `--features mcp`）
- [ ] 受信客户端已在 DBFlux 界面中注册并启用
- [ ] 传给二进制的 `--client-id` 与已注册的客户端一致
- [ ] 目标连接已启用 MCP
- [ ] 执行者在该连接上拥有策略分配
- [ ] 策略覆盖了智能体将要使用的工具
- [ ] 已了解任何写入/破坏性工具所涉及的审批工作流

## 11. 测试卫生

为避免测试污染开发机：

- 把 `--config-dir` 指向临时目录，或设置 `HOME` / `XDG_CONFIG_HOME` / `XDG_DATA_HOME`。
- 审计测试使用临时的 SQLite 路径。
- 测试代码中不要读写 `~/.config/dbflux` 或 `~/.local/share/dbflux`。
- 内置策略与角色开箱可用，无需任何设置 —— 不要在测试固件中手动插入它们。
- `dbflux_audit::temp_sqlite_path(name)` 这个辅助函数会为每个测试生成一个隔离路径。

## 12. 故障排查

### 服务器立即退出

- 缺少 `--client-id` 参数。
- 配置目录不可访问或无法创建。

### 请求因不受信被拒绝

- 确认该客户端在受信客户端列表中存在且处于启用状态。
- 确认 `--client-id` 与已注册的 `id` 完全一致（区分大小写）。

### 请求因连接未启用 MCP 被拒绝

- 在目标连接的治理设置中启用 MCP（连接管理器 → MCP 标签页）。
- 或者，如果希望所有连接都启用，可在配置中设置 `mcp_enabled_by_default: true`。

### 被策略拒绝

- 确认执行者在该连接范围内拥有分配。
- 确认工具 ID 在所属策略允许的工具之内。
- 确认执行类别在策略允许的执行类别之内。
- 如果使用 `builtin/read-only`，写入类工具（如 `create_script` 等）在设计上就被排除在外。

### 审批一直处于待处理

- 检查 DBFlux 工作区中的待审批队列，并明确地批准或驳回。
- `approve_execution` 需要 `admin` 执行类别 —— 请确认审批者的策略包含它。

### 审计导出缺少事件

- 确认筛选条件（`actor_id`、`tool_id`、时间范围、决策）没有过严。
- `export_audit_logs` 被归类为 `read` 执行类别。

### 无法删除策略或角色

- 内置 ID（`builtin/read-only`、`builtin/write`、`builtin/admin`）不能被删除。
- 如果需要可修改的变体，请用其他 ID 创建一个自定义策略。

### 界面中已修改设置，但服务器仍在使用旧值

- 重启 MCP 服务器进程。治理配置只在启动时从磁盘加载一次。
