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
4. **工具 + 按类别的决定**：工具 ID 必须被某个已分配的策略列出，并由该策略把调用的执行类别决定为 Allow、Ask 或 Deny（见第 5 节）。
5. **审批流程**：Ask 决定会把调用排入待审批执行队列。由人在 DBFlux 中批准或驳回；已批准的调用会在智能体以相同参数再次调用时执行一次。
6. **审计追踪**：每一个决策都会追加到统一 SQLite 数据库的 `aud_audit_events` 表中，可查询、可导出。完整的事件结构参见 `docs/AUDIT.md`。

这六层都会在每一个 `tools/call` 请求中，于服务器进程内依次执行；任何一层都无法从客户端绕过。

## 4. 标准工具清单（v1）

| 分组 | 工具 ID | 执行类别 | 作用 |
|---|---|---|---|
| 连接 | `list_connections` | metadata | 枚举所有已配置的数据库连接 |
| 连接 | `connect` | metadata | 针对某个已配置连接打开一个会话。当驱动有数据库概念时，响应会给出 `current_database` 和服务器上可用的 `databases`；其他工具通过 `database` 参数指向另一个数据库 |
| 连接 | `disconnect` | metadata | 关闭一个已打开的会话 |
| 连接 | `get_connection_info` | metadata | 获取驱动程序能力与连接元数据 |
| Schema | `list_databases` | metadata | 列出某个连接上可访问的所有数据库 |
| Schema | `list_schemas` | metadata | 列出数据库内的 Schema |
| Schema | `list_tables` | metadata | 列出 Schema 内的表与视图。传入 `names_only: true` 时，以字符串形式返回名称，而不是每项一个对象 |
| Schema | `list_collections` | metadata | 列出 MongoDB 的集合。与 `list_tables` 一样接受 `names_only` |
| Schema | `describe_object` | metadata | 获取某个表的列/字段定义与索引 |
| 读取 | `select_data` | read | 对表或集合执行结构化的 SELECT。与其他表的 `joins` 在声明支持 join 的驱动程序上执行；文档型、键值型以及其他未声明支持的驱动程序会返回明确的错误。`on` 条件只接受用 `AND` 连接的列比较。使用 join 时，在会从生成的查询中去掉 schema 的连接（SQLite、Turso）上使用带 schema 限定的表、在未声明支持 `$ilike` 的驱动上使用 `$ilike`，以及 `order_by` 方向不是 `asc` 或 `desc`，都会被拒绝。SQL Server 也能执行 join，其行数限制会生成为 `OFFSET … FETCH`；join 测试在 SQLite 上运行 |
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
| 脚本 | `list_scripts` | metadata | 列出脚本目录中的已保存脚本（不包含外部脚本文件夹） |
| 脚本 | `get_script` | read | 获取某个已保存脚本的源码 |
| 脚本 | `create_script` | write | 将新脚本保存到脚本目录 |
| 脚本 | `update_script` | write | 覆盖已有的已保存脚本 |
| 脚本 | `delete_script` | admin | 永久删除脚本 |
| 脚本 | `execute_script` | computed | 对某个连接执行已保存的脚本。执行类别由脚本内容推导 |
| 审批 | `request_execution` | admin | 把一次调用排队等待人工审批。批准后，以相同参数调用该工具本身即可执行一次 |
| 审批 | `list_pending_executions` | read | 查看所有等待审批的执行 |
| 审批 | `get_pending_execution` | read | 获取某个待审批执行的详情。被驳回的执行返回 `status: "rejected"` 以及审批人给出的原因 |
| 审批 | `approve_execution` | — | 通过 MCP 调用时始终被拒绝。由人在 DBFlux 中批准 |
| 审批 | `reject_execution` | — | 通过 MCP 调用时始终被拒绝。由人在 DBFlux 中驳回 |
| 审计 | `query_audit_logs` | read | 搜索并筛选审计追踪 |
| 审计 | `get_audit_entry` | read | 按 ID 获取单条审计日志 |
| 审计 | `export_audit_logs` | read | 以 CSV 或 JSON 下载审计日志条目 |

当驱动使 `select_data`、`count_records`、`aggregate_data` 或 `describe_object` 调用失败，且所查询的数据库或 schema 的元数据中没有列出该表或集合时，错误信息会说明在哪里查找了这个名称，并列出最接近的已列出名称。错误信息的措辞是 "is not listed"（未列出），因为该表可能不存在，也可能是该连接无权访问它。如果调用没有传入 `database`，而服务器列出了多个数据库，错误信息还会提示该表可能位于另一个数据库中。对于表（不包括集合），`count_records`、`aggregate_data`，以及在执行前不做检查的引擎上的 `select_data`（见下文），对 `where` 或 `order_by` 中引用的列做同样的处理。提示只包含客户端有权列出的名称：表名需要 `list_tables`，列名需要 `describe_object`，数据库信息需要 `list_databases`。驱动自身的错误文本保留在末尾。只有在驱动使调用失败之后才会执行这项查找，不提供这些元数据的驱动会返回原始错误。

`select_data` 则在会把未知的带引号标识符读成字符串的引擎上，在执行之前检查列：SQLite 和 Turso 就是这样的引擎，拼错的列不会报错，而是返回零行。对于关系型表，以及所有带 `joins` 的调用，`columns`、`where` 或 `order_by` 中未加限定或以所属表限定的每个名称，无论包含什么字符，都会与该表的列元数据进行比较，并且像 SQLite 一样只对 ASCII 字母不区分大小写。未列出的列会被拒绝，错误信息包含同样的提示，并附上 "The query was not run."，查询不会执行。表名和视图名按 ASCII 不区分大小写解析，生成列、虚拟表的隐藏列，以及引擎接受的 `rowid`、`oid` 和 `_rowid_` 都视为已列出。没有 `describe_object` 权限时，拒绝信息不会列出任何其他列名。当驱动没有该表的列元数据时，会跳过检查，调用照常执行；嵌套路径和以其他表限定的名称不做检查，因为引擎会拒绝它们。调用中每个被引用了列的表需要一次列元数据查询。PostgreSQL、MySQL、MariaDB、SQL Server、ClickHouse 和 Redshift 遇到未知列会报错，因此它们的调用不做检查直接执行，失败时附上上文所述的提示。

驱动为该表声明的伪列（例如 SQLite 的 `rowid`、MySQL 的 `_rowid`，或 PostgreSQL 的 `ctid` 和 `xmin`）视为已列出，且不会出现在建议中。不带 `joins` 的调用若在 `columns` 中引用伪列，会像 join 一样以生成的 SELECT 执行，从而返回该值：PostgreSQL 将 `ctid` 返回为 `(0,1)` 这样的文本，其他系统列返回为整数。这个 SELECT 接受的输入比普通调用少：`where` 只接受 join 所接受的运算符，名称只能是简单的 ASCII 标识符，每列只能出现一次，排序方向只能是 `asc` 或 `desc`。在执行前不做检查的引擎上，只有当 `columns` 中的某项不在调用读取的行中时，才会用一次列元数据查询来确认它是否为伪列。

暂缓提供的工具（在 v1 中会在请求时明确拒绝）：

- `estimate_query_cost`
- `get_execution_status`

## 5. 执行类别

策略在两个层面把关工具：工具 ID 本身，以及执行分类。策略列出它覆盖的工具，并为每个执行类别给出一个决定：

| 决定 | 该类别的调用会怎样 |
|---|---|
| Allow | 立即执行 |
| Ask | 等待人工处理：调用被排入待审批执行队列，批准后才会执行 |
| Deny | 被拒绝 |

| 执行类别 | 覆盖范围 |
|---|---|
| `metadata` | Schema 检查 —— 列出数据库、表，以及描述对象 |
| `read` | 运行只读查询、获取数据，以及只读预览 |
| `write` | 插入、更新，或运行会修改数据的脚本 |
| `destructive` | DELETE、DROP、TRUNCATE 及其他不可撤销的操作 |
| `admin_safe` | 安全的 DDL 操作，例如追加式 Schema 变更与创建索引 |
| `admin` | 有风险的 DDL 操作、审计导出与特权动作 |
| `admin_destructive` | 不可逆的管理操作，例如删除或清空 Schema 对象 |

`metadata` 与 `read` 只做读取。其余五个类别会修改数据或 Schema，下文称为变更类别。

### 策略如何组合

一个执行者在某个连接上可以拥有多个策略，既可直接分配，也可通过角色获得。只有列出所请求工具的策略参与判断，其中最宽松的决定生效：Allow 优先于 Ask，Ask 优先于 Deny。策略用于授予权限；Deny 表示没有授权，而不是否决。因此，某个策略对某一类别要求审批，并不会拦住另一个已分配策略已允许执行该类别的执行者。若要让某一类别等待审批，请确保分配给该执行者的其他策略都不允许它。

### 审批流程

1. 智能体调用的工具，其类别被策略决定为 Ask。服务器把该调用排入待审批执行队列，记录一条 outcome 为 `pending` 的 `mcp_authorize` 审计事件，并返回一个 JSON-RPC 错误，其数据为 `{"code": "approval_required", "status": "pending", "pending_id": "..."}`。
2. 由人在 DBFlux 中批准或驳回该调用（**Workspace → 待审批项**）。服务器与应用通过 `dbflux.db` 共享该队列，因此由 `dbflux mcp` 排队的调用会出现在应用中。
3. 智能体以相同参数再次调用同一工具。服务器找到与执行者、连接、工具及参数都匹配的批准记录，消耗它并执行该调用。该调用的 `mcp_authorize` 事件 outcome 为 `success`，并在 `details_json.pending_execution_id` 中注明所用的批准记录。

一次批准只执行一次调用。再次重复调用会排入新的请求，修改任何参数也是如此。被驳回的调用永远不会执行。批准在调用排队 24 小时后失效。

驳回之后，`get_pending_execution` 返回 `status: "rejected"` 以及 `reason` 字段，其中是审批人驳回时输入的文本（去除首尾空白，最多 500 个字符）；未填写时为 `null`。同一原因也会记录在 `mcp_reject_execution` 审计事件中。

`request_execution` 显式地把调用排队，效果与在 Ask 下直接调用该工具相同。`request_execution`、`list_pending_executions` 与 `get_pending_execution` 只创建或读取队列条目，因此在 Ask 下它们直接执行，自身不会被排队。

MCP 客户端永远不能批准或驳回：无论策略如何设置，`approve_execution` 与 `reject_execution` 通过 MCP 调用时都会被拒绝，错误代码为 `self_approval_forbidden`，且每次尝试都会被审计。只有人在 DBFlux 界面中处理待审批执行。

## 6. 内置策略与角色

三个策略与三个角色作为不可变的内置项随程序提供。无论磁盘上持久化了什么，它们始终存在，并且不能被删除或修改。

### 内置策略

读取默认允许；内置策略授予的每个变更类别都需要审批。

| ID | Allow | Ask | 范围 |
|---|---|---|---|
| `builtin/read-only` | metadata, read | — | 所有发现与 Schema 工具；只读查询与预览工具；脚本列出/获取；审计读取工具 |
| `builtin/write` | metadata, read | write | 所有只读工具，加上具备写入能力的脚本，以及请求/审批提交流程 |
| `builtin/admin` | metadata, read | write, destructive, admin_safe, admin, admin_destructive | 除 `approve_execution` 与 `reject_execution` 之外的全部标准工具 |

内置策略未列出的类别均被拒绝。

### 在 Ask 出现之前创建的策略

在引入 Ask 决定之前，策略只能允许某个类别，因此允许一个变更类别从来不是“无需审批即可执行”的明确选择。引入 Ask 的存储迁移（`034_cfg_tool_policy_approval_classes`）会据此重写已有的自定义策略：已允许的变更类别改为 Ask，已允许的 `metadata` 或 `read` 类别保持 Allow，未被允许的类别保持 Deny。若要让智能体重新无需审批地执行变更调用，请在该策略上选择 **全部允许，无需审批**。

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
   - 选择工具，并为每个执行类别选择 Allow、Ask 或 Deny，即可创建自定义策略。使用键盘时，在类别行上按 `enter` 会切换到下一个决定。
   - **全部允许，无需审批** 会把所有变更类别设为 Allow。此后智能体可以不经询问执行任何变更调用，包括 `DROP DATABASE`。

4. **连接管理器 → MCP 标签页**
   - 为目标连接启用 MCP。
   - 从已填充的下拉框中，为该连接选择执行者（受信客户端）、角色和/或策略。

5. **Workspace → 待审批项**
   - 审阅并批准或驳回被策略送去审批的调用。这是处理待审批执行的唯一位置。
   - 等待中的调用也会出现在标题栏铃铛下的通知中心里，有调用等待时铃铛显示强调色徽标。点击其所在行的**查看**会在此标签页中打开该调用；弹出面板本身从不批准或驳回。
   - `j` / `k` 在待处理的调用之间移动，`a` 批准所选调用，`r` 驳回它。已批准的调用会在智能体以相同参数再次调用时执行。每个决定都会写入审计日志。
   - 底部的原因输入框会在驳回时发回给智能体。输入框获得焦点时，`r` 与 `a` 输入文字而不是做出决定。每次决定后输入框会被清空。

6. **Workspace → 审计**
   - 按执行者/工具/决策/时间范围筛选，并导出 CSV/JSON。

MCP 服务器在启动时从磁盘读取这些设置。如果你在服务器运行期间修改了界面中的治理设置，请重启服务器以加载新配置。

## 8. 持久化文件与路径

DBFlux 把所有状态持久化在一个统一的 SQLite 数据库和少数几个辅助目录中。路径由 `dirs` 解析（Linux 上遵循 `XDG_*`，macOS 上为 `~/Library`）。

Linux 上的典型默认值：

| 路径 | 内容 |
|---|---|
| `~/.local/share/dbflux/dbflux.db` | 统一数据库：连接配置、认证、SSH 隧道、治理、审计事件、历史记录、会话、界面状态 |
| `~/.local/share/dbflux/sessions/` | 用于会话恢复与找回的临时文件和影子文件 |
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
        correlation_id: None,
    },
    now_epoch_ms(),
)?;

if !outcome.allowed {
    // deny_code 与 deny_reason 说明了原因
}
```

`authorize_request` 没有审批队列：Ask 决定会以不允许的结果返回，且 `deny_code == Some("approval_required")`。MCP 服务器改为调用 `McpRuntime::authorize_with_approval_mut`，它会传入调用参数，使 Ask 决定被排队，或消耗一条匹配的批准记录并放行该调用。

## 10. 集成清单

在把 AI 客户端指向 MCP 服务器之前：

- [ ] `dbflux` 已带 MCP 支持构建（默认启用，或使用 `--features mcp`）
- [ ] 受信客户端已在 DBFlux 界面中注册并启用
- [ ] 传给二进制的 `--client-id` 与已注册的客户端一致
- [ ] 目标连接已启用 MCP
- [ ] 执行者在该连接上拥有策略分配
- [ ] 策略覆盖了智能体将要使用的工具
- [ ] 需要审批的类别已设为 Ask，并且在智能体工作期间有人关注待审批项

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
- 确认策略把该执行类别决定为 Allow 或 Ask，而不是 Deny。
- 如果使用 `builtin/read-only`，写入类工具（如 `create_script` 等）在设计上就被排除在外。

### 调用返回 `approval_required`

- 策略把该调用的类别决定为 Ask。请在 **Workspace → 待审批项** 中批准 `pending_id` 所指的待审批执行，然后以相同参数重复该调用。
- 以不同参数重复调用会排入新的请求，而不会使用该批准。
- 智能体不能批准自己的调用：`approve_execution` 与 `reject_execution` 通过 MCP 调用时始终被拒绝（`self_approval_forbidden`）。

### 审计导出缺少事件

- 确认筛选条件（`actor_id`、`tool_id`、时间范围、决策）没有过严。
- `export_audit_logs` 被归类为 `read` 执行类别。

### 无法删除策略或角色

- 内置 ID（`builtin/read-only`、`builtin/write`、`builtin/admin`）不能被删除。
- 如果需要可修改的变体，请用其他 ID 创建一个自定义策略。

### 界面中已修改设置，但服务器仍在使用旧值

- 重启 MCP 服务器进程。治理配置只在启动时从磁盘加载一次。
