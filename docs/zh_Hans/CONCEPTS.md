# 核心概念

本文档为贡献者和高级用户提供一份简短的心智模型，描述各子系统之间的契约；[架构](../ARCHITECTURE.md)仍然是 crate 边界与关键文件的权威、详尽地图。

## 心智模型

```text
界面文档
  -> 应用编排（连接配置、连接、策略、生命周期）
    -> dbflux_core 契约（元数据、能力、请求、值）
      -> 内置驱动程序或经 RPC 适配的驱动程序
        -> QueryResult -> 通用结果视图
        -> EventRecord -> 审计 Sink
```

关键的方向是向内的：呈现层与工作流依赖契约，而驱动程序特有的行为则留在契约之后。审计贯穿整个流程去观察所发生的工作，而不是另起一条独立的执行路径。

## 概念地图

| 概念 | 它是什么 | 为何重要 | 深入阅读 |
|---|---|---|---|
| 驱动程序 | `DbDriver` 与 `Connection` 契约的实现。 | 内置数据库与外部数据库通过同一边界进入应用。 | [核心 trait](../crates/dbflux_core/src/core/traits.rs)、[驱动程序开发](DRIVER_AUTHORING.md) |
| `DriverMetadata` | 驱动程序的声明式身份：类别、语言、表单，以及详细的功能描述符。 | 通用工作流无需识别具体是哪一个驱动程序，就能选择呈现方式与行为。 | [元数据定义](../crates/dbflux_core/src/driver/capabilities.rs) |
| `DriverCapabilities` | 由驱动程序声明、并由连接对外暴露的功能标志。 | 界面只启用确实受支持的操作，而不是根据数据库名称去猜。 | [能力标志](../crates/dbflux_core/src/driver/capabilities.rs) |
| 文档 | 以标签页形式管理的、经过类型擦除的面板，带有身份与事件契约。 | 新增文档类型时无需扩展一个封闭的文档枚举即可参与其中。 | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs)、[`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) |
| 查询结果 | 连接返回的结构化形状、列、行与值。 | 通用表格、树、文本视图、导出和图表共用同一个结果模型。 | [结果类型](../crates/dbflux_core/src/query/types.rs)、[`Value`](../crates/dbflux_core/src/core/value.rs) |
| MCP 治理 | 围绕 AI 工具施加的受信客户端、连接、分类、策略、审批与审计约束。 | 智能体的访问权限是显式的、有范围限定的、可审查且可观测的。 | [AI + MCP 集成](MCP_AI_INTEGRATION.md) |
| 审计 | 横切的 `EventRecord` / `EventSink`（事件接收端）可观测性接缝。 | 查询、生命周期工作、Hook、治理与外部服务共享一条互相关联的追踪记录。 | [审计参考](AUDIT.md)、[`EventSink`](../crates/dbflux_core/src/observability/source.rs) |
| Hooks | 绑定到连接生命周期各阶段的命令、脚本或 Lua。 | 环境的准备与清理留在驱动程序实现之外，并带有明确的失败行为。 | [Hook 契约](../crates/dbflux_core/src/connection/hook.rs)、[设置与 Hooks](SETTINGS.md#connection-hooks) |
| RPC 服务 | 持久化的描述符，在启动时被适配为驱动程序或认证提供程序。 | 进程外集成无需成为界面的特例，就能接入运行时。 | [RPC 配置](RPC_SERVICES_CONFIG.md)、[协议](DRIVER_RPC_PROTOCOL.md) |

## 驱动程序是契约，而非界面特例

`DbDriver` 负责创建并描述一个数据库集成；`Connection` 则对外暴露活动连接上的各项操作。它们当前的契约定义在 [`core/traits.rs`](../crates/dbflux_core/src/core/traits.rs) 中。内置 crate 直接实现这些契约，外部驱动程序则通过 RPC 适配而来。

解耦规则十分严格：界面与应用工作流代码只能通过通用元数据、能力和契约来适配，绝不能依赖具体的驱动程序 ID。如果某个功能需要在呈现层或工作流代码里写出 `if driver == "postgres"`，那说明缺失的抽象应当补在元数据、某项能力或某个核心契约上。

### 元数据与能力

[`DriverMetadata`](../crates/dbflux_core/src/driver/capabilities.rs) 描述一个驱动程序是什么：显示身份、`DatabaseCategory`、查询语言、语法与操作描述符、限制，以及其他通用的呈现输入。`DriverCapabilities` 则声明它支持哪些大类功能。连接会对外暴露同样的元数据与能力，因此调用方无需拿到它所属的驱动程序对象。

用元数据来选择通用模式，用能力来决定是否放行某项操作。不要根据驱动程序的键、图标、原生类型名字符串，或界面里维护的清单去推断支持情况。

## 文档是开放多态

[`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs) 是文档多态的接缝。它把每个具体的 GPUI 实体类型擦除到一组闭包之后，这些闭包分别负责渲染、焦点、命令、元数据、生命周期行为、去重和订阅。因此，工作区不会把文档建模成一个由具体文档类型组成的封闭枚举。

[`DocumentKey`](../crates/dbflux_ui_document/src/dedup.rs) 表达「已打开文档」的身份。每个面板自行判断是否匹配某个键，而 [`TabManager`](../crates/dbflux_ui_document/src/tab_manager.rs) 借助这一契约去聚焦已有的标签页，而不是再打开一个重复的。

文档会发出 [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs)。标签页管理器与工作区把这些事件翻译成跨文档的动作，而无需侵入某个具体文档的实现。要新增面板行为，请在这个接缝上做，而不是往工作区代码里添加对具体类型的匹配。

## 查询结果是结构化数据

当前的结果边界是 [`QueryResult`](../crates/dbflux_core/src/query/types.rs)：一个已声明的 `QueryResultShape`、`ColumnMeta` 条目、由核心 `Value` 组成的行、可选的文本或字节、执行耗时，以及可能的附加结果集。[`Value`](../crates/dbflux_core/src/core/value.rs) 保留了关系型与文档型的值，不会把一切都简化成 JSON 或显示字符串。

结构化的结果值与列元数据供给通用视图使用。特别是 `ColumnMeta.kind` 承载了语义类型信息，例如时间戳、浮点、整数、文本或未知。图表及其他使用方都是基于这一语义类型工作的；它们不得去嗅探 `ColumnMeta.type_name`，也不得依据驱动程序的身份做分支。

## MCP 治理包裹执行过程

MCP 进程通过受信客户端身份、按连接启用的 MCP 开关、执行分类、指派的角色与策略，以及必要时的审批来授权请求。决策与执行都会被记入审计。参见[治理模型](MCP_AI_INTEGRATION.md#3-governance-model-core-concepts)、[`dbflux_mcp` 授权](../crates/dbflux_mcp/src/server/authorization.rs)、[策略引擎](../crates/dbflux_policy/src/engine.rs)和[审批服务](../crates/dbflux_approval/src/service.rs)。

安全属性是这个边界的一部分：

- `preview_mutation` 受读取治理约束，只生成只读的执行计划，并不会真正执行该变更。实现会拒绝任何未被归类为元数据类或读取类的、由驱动程序生成的预览查询（[查询工具](../crates/dbflux_mcp_server/src/tools/query.rs)）。
- `select_data` 目前会直接拒绝请求中的 join，而不是静默地忽略它们（[读取工具](../crates/dbflux_mcp_server/src/tools/read.rs)）。
- 变更预览并不是 DDL 预览面。DDL 操作由各自独立的受治理工具承担；当前的[工具目录](../crates/dbflux_mcp/src/tool_catalog.rs)中没有暴露任何 DDL 预览工具。

分类、策略、审批与审计的决策都应留在治理边界上。某个处理函数不得因为底层驱动程序能执行该操作，就削弱这些决策。

## 审计是可观测性接缝

各服务通过 [`EventSink`](../crates/dbflux_core/src/observability/source.rs) 发出规范的 [`EventRecord`](../crates/dbflux_core/src/observability/types.rs)。该记录承载执行者、来源、类别、结果、目标上下文、详情与关联字段；Sink 自身负责校验与存储行为。

这让审计成为一项横切的能力：查询执行、连接生命周期、Hook、MCP 决策、配置以及外部 RPC 服务都可以被观测，而不必把各自的领域逻辑耦合到 SQLite 实现上。表结构、校验、脱敏、保留策略与 tracing 桥接的细节，参见[审计参考](AUDIT.md)。

## Hook 环绕连接生命周期

[`ConnectionHook`](../crates/dbflux_core/src/connection/hook.rs) 定义了**连接前**（`PreConnect`）、**连接后**（`PostConnect`）、**断开前**（`PreDisconnect`）和**断开后**（`PostDisconnect`）阶段要执行的命令、脚本或 Lua 工作。Hook 属于围绕连接的编排层，而不属于数据库驱动程序的查询契约。

失败策略是明确的：**断开连接**（`Disconnect`）会中止该阶段，**警告**（`Warn`）会继续执行但给出可见的警告，**忽略**（`Ignore`）则继续执行，仅把失败记入日志。执行方式可以是**阻塞**或**分离**，并受超时、环境变量与就绪信号的控制。配置与安全细节参见[设置与连接 Hooks](SETTINGS.md#connection-hooks)。

## RPC 服务是运行时描述符

RPC 服务是持久化的启动与兼容性描述符，分为 `Driver` 和 `AuthProvider` 两类。启动时，[`dbflux_app::rpc_services`](../crates/dbflux_app/src/rpc_services/) 会发现这些描述符、校验并探测相应的协议，然后把成功的服务适配进驱动程序或认证提供程序的注册表。某一族服务失败，并不会连带改变另一族。

外部驱动程序的注册表键仍然是 `rpc:<socket_id>`。认证提供程序使用自身的提供程序身份，永远不会作为数据库驱动程序出现。外部驱动程序的运行时元数据来自其握手过程，而不是界面里的条件判断。持久化细节参见 [RPC 服务配置](RPC_SERVICES_CONFIG.md)，传输、协商、生命周期与审计事件发出的细节参见 [驱动程序 RPC 协议](DRIVER_RPC_PROTOCOL.md)。

## 该在哪里改动

| 你要改动的是…… | 入口在…… | 判别准则 |
|---|---|---|
| 面向所有集成的数据库操作 | [`DbDriver` / `Connection`](../crates/dbflux_core/src/core/traits.rs) | 先定义通用契约，再实现各驱动程序。 |
| 某个通用功能是否显示或允许 | [元数据与能力](../crates/dbflux_core/src/driver/capabilities.rs) | 声明支持情况；绝不在界面/工作流代码中识别具体驱动程序。 |
| 结果渲染或语义类型行为 | [查询结果类型](../crates/dbflux_core/src/query/types.rs) | 依据形状、值与 `ColumnMeta.kind`。 |
| 新增一种工作区文档 | [`PaneHandle`](../crates/dbflux_ui_document/src/pane.rs) 与 [`DocumentEvent`](../crates/dbflux_ui_document/src/handle.rs) | 实现开放的面板接缝与去重键；不要扩展具体的文档联合类型。 |
| AI 工具或执行规则 | [MCP 集成](MCP_AI_INTEGRATION.md) 与 [授权](../crates/dbflux_mcp/src/server/authorization.rs) | 保持分类、策略、审批与审计的先后顺序。 |
| 一个需要被观测的领域动作 | [`EventRecord` 与 `EventSink`](../crates/dbflux_core/src/observability/types.rs) | 通过接缝发出事件；把存储细节留在领域代码之外。 |
| 连接建立或清理的自动化 | [Hook 契约](../crates/dbflux_core/src/connection/hook.rs) | 选定生命周期阶段与明确的失败模式。 |
| 进程外的驱动程序或认证提供程序 | [RPC 服务](RPC_SERVICES_CONFIG.md) | 持久化一个描述符，并通过 `dbflux_app::rpc_services` 完成适配。 |

想了解 crate 边界与关键文件的权威说明，请继续阅读[架构](../ARCHITECTURE.md)；若要实现内置或外部驱动程序，请参阅[驱动程序开发](DRIVER_AUTHORING.md)。
