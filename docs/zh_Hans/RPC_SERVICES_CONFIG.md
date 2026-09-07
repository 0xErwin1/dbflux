# RPC 服务界面参考

本文件说明 DBFlux 中 RPC 服务的存储与管理方式。

DBFlux 现已通过 `RpcServiceKind` 持久化一套一等的 RPC 服务基础能力：

- `Driver` — 适配为运行时数据库驱动
- `AuthProvider` — 适配为应用与 MCP 服务器中的运行时认证提供程序注册表

## 存储

RPC 服务存储在 SQLite 数据库 `~/.local/share/dbflux/dbflux.db` 中，而非 JSON 文件。

**数据表：**

- `cfg_services` — 服务主记录（socket_id、service_kind、command、startup_timeout_ms、enabled）
- `cfg_services.api_family`、`cfg_services.api_major`、`cfg_services.api_minor` — 可选的 RPC API 契约元数据
- `cfg_service_args` — 按顺序排列的进程参数
- `cfg_service_env` — 环境变量

## 表结构

```sql
-- 基础表（迁移 001）。`service_kind` 由迁移 005 添加，
-- `api_family`/`api_major`/`api_minor` 由迁移 006 添加；此处一并
-- 列出仅供参考，它们不属于基础 DDL。
CREATE TABLE cfg_services (
    socket_id TEXT PRIMARY KEY,
    enabled INTEGER DEFAULT 1,
    command TEXT,
    startup_timeout_ms INTEGER,        -- SQL 层面未设默认值；5000 毫秒的
                                       -- 回退值（DEFAULT_STARTUP_TIMEOUT_MS）
                                       -- 在应用代码中生效
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    service_kind TEXT NOT NULL DEFAULT 'driver',  -- 由迁移 005 添加
    api_family TEXT,                              -- 由迁移 006 添加
    api_major INTEGER,                            -- 由迁移 006 添加
    api_minor INTEGER                             -- 由迁移 006 添加
);

CREATE TABLE cfg_service_args (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    position INTEGER NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE cfg_service_env (
    id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL REFERENCES cfg_services(socket_id),
    key TEXT NOT NULL,
    value TEXT NOT NULL
);
```

## 管理服务

服务通过**设置**界面中的 **RPC 服务**设置项管理，而非直接编辑文件。

添加或修改服务的步骤：
1. 打开设置 → RPC 服务
2. 新建服务，或选择已有服务
3. 选择服务类型（**驱动**或**认证提供程序**）
4. 配置 Socket ID、命令路径、参数、环境变量与超时时间
5. 保存更改

说明：

- **驱动**类型的服务在运行时生效，并沿用现有的 `rpc:<socket_id>` 驱动标识。
- **认证提供程序**类型的服务仅在运行时认证提供程序注册表中生效，不会作为驱动出现。
- DBFlux 保持驱动注册 ID 为 `rpc:<socket_id>` 的兼容性。
- 若已有驱动记录缺少 API 元数据，DBFlux 会将其默认设为当前 `driver_rpc` 契约的 `1.1` 版本。
- 若已有认证提供程序记录缺少 API 元数据，DBFlux 会将其默认设为当前 `auth_provider_rpc` 契约的 `1.2` 版本。
- 在 DBFlux 探测 Socket 之前，`api_family` / `api_major` 用作认证提供程序的启动前预检。

## 语义

- `socket_id` 原样用作 Socket 文件名
- DBFlux 在内部以 `rpc:<socket_id>` 标识每项服务
- DBFlux 在运行时适配之前，先按 `service_kind` 对每项服务分类
- 驱动的名称、图标、类别与表单来自服务的 `Hello` 响应（`driver_metadata`、`form_definition`），而非来自配置
- `service_kind='driver'` 的服务若在启动期间未能完成 RPC 握手（`Hello`），则不会被注册
- `service_kind='auth_provider'` 的服务在通过兼容性检查且探测成功后，会被载入认证提供程序注册表
- 驱动路径的协商会在 `Hello` 阶段选择双方均支持的最高兼容次版本号，此后要求每条后续消息信封（envelope）都使用这一协商确定的版本
- 认证提供程序的协商在 `auth_provider_rpc` 下遵循同样的 family/major/minor 规则；family 或 major 版本不兼容的服务会在注册前被跳过

## 字段

- `socket_id`（必填）：DBFlux 与服务共同使用的本地 Socket 名称。
  - 允许的字符：ASCII 字母、数字、`.`、`_`、`-`
  - 路径分隔符、空格及其他标点会被拒绝。
  - 该值会原样传递给平台的 Socket 命名空间，因此应保持简短且稳定。
- `command`（可选）：DBFlux 需要启动服务时运行的可执行文件。
  - 若省略该项且 `args` 也为空，DBFlux 会认为服务已在运行，不会启动任何进程。
  - 对于 `driver`，若省略该项而 `args` 非空，DBFlux 会启动 `dbflux-driver-host`。
  - 对于 `auth_provider`，若需由 DBFlux 启动服务，则必须显式设置 `command`。
- `args`（可选）：进程参数。
- `env`（可选）：传给所启动进程的环境变量。
- `startup_timeout_ms`（可选）：启动进程后等待 Socket 就绪的最长时间。
  - 默认值：`5000`

## 常见错误

- 服务配置与服务参数中的 Socket 名称不一致
- `command` 使用相对路径，在 DBFlux 进程环境下无法解析
- 直接编辑数据库，而未通过设置界面操作
- 服务未实现当前 RPC 协议版本要求的 `Hello` 字段
- 省略 `command` 却只提供部分 `args`；若希望 DBFlux 启动默认宿主进程，`args` 必须同时包含 `--driver` 与 `--socket`
- 为认证提供程序服务配置了 `args` 却未配置 `command`；DBFlux 会拒绝该启动配置，而不会假定使用驱动宿主进程
