# 仪表盘与已保存图表

DBFlux 把图表配置持久化为**已保存图表**，并把它们组合成**仪表盘**——即由图表面板（以及可选的分隔条）组成的网格，这些面板共享同一时间范围与刷新策略。

关于图表引擎的内部机制（渲染、坐标轴、降采样、配色），参见 [`CHARTS.md`](./CHARTS.md)；关于 SQLite 存储层，参见 [`ARCHITECTURE.md`](../ARCHITECTURE.md#storage--configuration)。

## 概述

- **SavedChart** 是图表配置的持久化形态：数据源绑定、序列、Y 轴绑定、刷新策略与时间范围预设。
- **仪表盘**是一个具名的面板网格。每个面板要么是 `Chart` 槽位（按 id 引用某个 `SavedChart`），要么是 `Divider`（分隔条）槽位（内联的 markdown 标题条——没有图表，也没有工具栏）。
- 仪表盘拥有一份共享的**时间范围**与**刷新策略**，通过订阅机制下发到每一个已加载的图表面板。
- 远程仪表盘（例如 CloudWatch）可以在侧边栏中**浏览**，并在驱动程序声明了相应能力时**导入**为本地仪表盘。

## 已保存图表

### 为查询结果绘图

1. 执行一条返回表格数据的查询。
2. 在结果网格中右键点击，选择**将此查询制作成图表**。

随后会打开一个图表文档，其中已填入你的查询，并自动运行。只有当结果带有可用的原始查询、且 DBFlux 能自动检测出可用于绘图的列时，该选项才会出现。支持的图表类型：折线图、柱状图、散点图、面积图、堆叠图、饼图。坐标轴的检测依据各列的类型（时间、数值、文本）；具体规则参见[图表](CHARTS.md)。

### 保存与重新打开

- 在图表文档中按**保存**并为其命名。再次保存同一图表会覆盖原图（不会产生重复项）。
- 在命令面板中用**打开图表…** 重新打开已保存的图表——它会列出当前连接的已保存图表。（若一个都没有，会显示「当前配置文件没有已保存的图表」。）
- 已保存的图表也会出现在侧边栏的**已保存图表**下，每个图表都有**打开 / 重命名… / 复制 / 删除** 操作。

图表按连接配置文件分别保存。

## 仪表盘

仪表盘是一个带名称的 12 列网格，由多个面板组成，这些面板共用同一个时间范围与同一套刷新策略。

### 新建仪表盘

1. 从命令面板运行**新建仪表盘…**（或在侧边栏的**仪表盘**文件夹上运行**新建仪表盘…**）。
2. 为其命名。新建的仪表盘以 12 列网格打开，且刷新处于关闭状态。

新建的仪表盘以**查看**模式打开。侧边栏的仪表盘文件夹会列出已保存的仪表盘，并提供**打开 / 重命名… / 复制 / 删除** 操作。

### 编辑与查看

在标题栏中切换**查看**与**编辑**：

- **编辑**模式下会显示拖拽手柄——拖动面板可调整顺序，拖动边缘或角可在 12 列网格内调整大小。
- **查看**模式为只读。

两种模式下整个仪表盘都可用键盘操作：`h`/`j`/`k`/`l` 或方向键选择面板，`Enter` 打开它（其图表或表格接收按键，直到按 `Escape`），`c` 打开配置浮层，`r` 或 `F2` 重命名，`x` 或 `Delete` 移除，`a` 添加面板，`m` 列出所有操作。在编辑模式下，`Shift` + `h`/`j`/`k`/`l` 移动所选面板，`Alt+Shift` + `h`/`j`/`k`/`l` 调整其大小。见[键盘快捷键](KEYBOARD.md#仪表盘)。

### 添加面板

点击 **+ 添加面板**。选择器最多有三个标签页：

| 标签页 | 生成方式 |
|-----|---------|
| **已保存图表** | 由一个或多个已有的已保存图表生成面板。 |
| **来自查询** | 由「名称 + 你输入的查询」生成一个新面板。 |
| **来自指标** | 由某个驱动指标生成面板（仅当该连接所属驱动提供指标目录时才显示）。 |

在编辑模式下，每个面板的设置按钮提供**配置… / 编辑标题… / 移除面板**。**配置**浮层可用于更改图表类型（折线图、柱状图、散点图、面积图、堆叠图、饼图）、调整轴绑定以及查看**统计**。

仪表盘还可以包含 **Divider**（分隔条）——即以 Markdown 标题形式呈现的条带，用于在视觉上对面板分组，点击后会折叠其下方的面板。

> **图表**面板引用一个已保存的图表。若删除该已保存图表，面板会变成一个占位符（「未找到图表 — 已保存的图表已被删除」），而不会消失。

### 时间范围与刷新

标题栏上有：

- **时间范围**预设：15m、1h、6h、24h、7d，或**自定义**（会展开一行日期与小时 / 分钟选择器）。该范围会同时应用于所有图表面板。
- 一个**刷新**拆分按钮：点击可立即刷新所有面板；下拉框用于设置自动刷新间隔（或关闭 / 打开时刷新）。

> **断开的连接会被妥善处理。** 当某个面板所属的连接已关闭时，其刷新节拍会被跳过——计时器保持存活，重新连接后自动恢复，无需重新打开仪表盘。

## 远程仪表盘（CloudWatch）

当驱动支持时（CloudWatch 是参考实现），DBFlux 可以**浏览**与**导入**上游仪表盘。

- **浏览**：上游仪表盘会显示在侧边栏中。打开某个仪表盘会拉取并将其渲染为一个**内存中的只读**仪表盘——不会向数据源回写任何内容，也不会在本地保存任何内容。**刷新**操作会重新拉取列表。该列表属于会话级作用域，重启后**不会**保留。
- **导入**：**导入仪表盘**命令（命令面板或侧边栏）会将上游定义解析为一个带有已导入图表的新**本地**仪表盘。该命令仅在当前连接所属驱动支持导入时可用——否则你会看到「当前连接不支持仪表盘导入。」

远程仪表盘只支持只读浏览与导入；DBFlux 从不修改数据源。

## 存储层

所有仪表盘与已保存图表的数据都存放在 `~/.local/share/dbflux/dbflux.db` 中，表名以 `viz_*` 为前缀：

| 表 | 用途 |
|---|---|
| `viz_dashboards` | 仪表盘记录（`profile_id` 可为空，`ON DELETE SET NULL`） |
| `viz_dashboard_panels` | 面板槽位：`panel_kind` 判别字段 + 可选的 `divider_markdown` |
| `viz_saved_charts` | 已保存图表的根记录（`SavedChartDto`） |
| `viz_saved_chart_series` | 各序列的设置 |
| `viz_saved_chart_binding_y` | Y 轴绑定 |
| `viz_saved_chart_source_metric_dimensions` | CloudWatch 指标维度 |
| `viz_saved_chart_source_metric_series` | CloudWatch 指标序列规格 |

存储库位于 `crates/dbflux_storage/src/repositories/viz_*.rs`，实现标准的 `Repository` trait（`all()`、`find_by_id()`、`upsert()`、`delete()`）。

## 内存管理器

两个管理器都在 SQLite 存储库之上包了一层内存缓存，用于同步读取。写入操作先落到存储库，缓存只在写入成功后才更新。

- **`DashboardManager`**（`crates/dbflux_ui_base/src/dashboard_manager.rs`）—— 领域类型包括 `Dashboard`、`DashboardPanel`、`DashboardPanelKind`（`Chart { saved_chart_id }` | `Divider { markdown }` | `Inspector { metric_id }`）和 `DashboardPanelDraft`。新建仪表盘时 `grid_columns = 12`；新建面板会追加到新的一行，取 `grid_column = 0`、`grid_width = 12`、`grid_height = 2`。
- **`SavedChartManager`**（`crates/dbflux_ui_base/src/saved_chart_manager.rs`）—— 负责 `SavedChart` 的整个生命周期，包括 `SavedChartRefreshPolicy`（`Off` / `Interval { every_secs }`）。
- **`RemoteDashboardCache`**（`crates/dbflux_app/src/remote_dashboard_cache.rs`）—— 上游仪表盘列表的会话级内存缓存，重启后不会保留。

## 文档系统集成

仪表盘以 `DashboardDocument` 的形式打开（`crates/dbflux_ui_document/src/dashboard/`）：

- **去重键**：`DocumentKey::Dashboard { dashboard_id }`（已持久化）或 `DocumentKey::InstanceOverview { profile_id }`（自动生成的只读文档）。
- **图表面板**：每个槽位包装一个 `ChartDocument` 实体（`Loaded`）；如果对应图表已被删除，则包装一个占位面板（`Orphan`）。
- **检查器面板**：每个槽位包装一个 `InspectorPanel` 实体，它承载一个 `DataGridPanel`，并按仪表盘的共享间隔刷新。由驱动程序提供的行操作（例如终止连接、取消查询）会出现在行的右键菜单中。
- **共享工具栏**：由单个 `TimeRangePanel` 通过订阅机制，把时间窗口的变化下发到所有已加载的面板。
- **并发控制**：面板的重新执行受 `PANEL_REEXEC_CAP` 限制，以免并发查询压垮连接。
- **网格**：采用 12 列的规范网格；通过 `dashboard/builder.rs` 中的 `DragReorderState` / `DragResizeState` 支持拖拽重排与拖拽调整尺寸。

独立的已保存图表以 `ChartDocument` 打开（`crates/dbflux_ui_document/src/chart_document/`），去重键为 `DocumentKey::Chart { saved_chart_id }`。`ChartDocument` 既可以独立渲染，也可以嵌入 `DashboardDocument` 的面板中。

## 实例概览与检查器

如果某个连接所用的驱动程序声明了 `INSTANCE_METRICS` 或 `INSTANCE_INSPECTOR` 能力，它就会提供一个只读的**实例概览**——一份由实时服务器指标和表格型检查器合成的仪表盘；在用户明确选择保留之前，它完全不会触碰存储。

对于支持的驱动——**PostgreSQL、MySQL/MariaDB、MongoDB、Redis、SQL Server、ClickHouse 与 InfluxDB（v2）**——已连接的配置文件会在侧边栏显示一个**实例概览**条目，位于**实例指标**与**实例检查器**文件夹之上。

- **实例概览**是根据驱动的默认布局合成的只读仪表盘（每个连接一个标签页）。你无法编辑它或添加面板，但可以按**另存为可编辑**，将其布局克隆为一个全新的、完全可编辑的仪表盘。
- **实例指标**是时序图表（例如连接数、吞吐量）。
- **实例检查器**是服务器实时状态的表格快照（例如 Postgres 的 `pg_stat_activity`、MySQL 的进程列表、MongoDB 的当前操作、Redis 的客户端列表），按共享间隔刷新。

实时实例图表只累积当前图表会话中观测到的内容：一个有上限的内存缓冲区（最近 120 个采样点），既不持久化，也不会回填历史。所选时间窗口决定显示的时间轴并过滤可用的采样点——相对范围会在每次刷新时向前滚动，而自定义范围应用后保持固定。七天窗口较早部分的空白只表示尚未采集到采样点，并不代表存在历史数据；**刷新**获取的是当前采样而不是重建过去，保存可编辑概览时保存的是布局与图表配置，而非已采集的指标历史。

### 打开实例概览

侧边栏会在已连接的配置下显示一个**实例概览**叶子节点，位于「实例指标」和「实例检查器」文件夹之上。点击它，或者从它的右键菜单中选择**打开**，即可打开该概览。

| 步骤 | 说明 |
|---|---|
| 数据来源 | 驱动程序的 `DefaultInstanceDashboard` 描述符（固定的 12 列布局），由 `InstanceCatalog::default_dashboard()` 返回 |
| 去重 | `DocumentKey::InstanceOverview { profile_id }` —— 每个连接只对应一个标签页；再次点击会聚焦已有的标签页 |
| 持久化 | 无。`DashboardDocument` 在打开时于内存中构建，不会写入任何 `viz_*` 行 |
| 模式 | 只读。编辑模式的切换、*+ 添加面板*以及编辑/查看开关都被隐藏，拖拽重排与拖拽调整尺寸也被禁用 |

### 另存为可编辑的仪表盘

只读概览上会显示一个**另存为可编辑**按钮（位于工具栏右侧分组，提示语为*「将此概览克隆为新的可编辑仪表盘」*）。它会把合成出来的布局——包括精确的面板位置——克隆为一个新的、由用户拥有的持久化 `Dashboard`：通过 `DashboardManager::append_panels` 逐个面板传入显式的 `DraftGridLayout`。克隆结果会作为一个普通的可编辑标签页打开，键为 `DocumentKey::Dashboard { dashboard_id }`。原来的概览仍然是只读的，并且每次打开时都会重新合成。

### 检查器面板

检查器是第三种仪表盘面板类型，与 `Chart` 和 `Divider` 并列：

| 面板类型 | 依托对象 | 说明 |
|---|---|---|
| `Chart` | `SavedChart` 引用（`saved_chart_id`） | 时序图表 |
| `Divider` | 内联 markdown | 标题条；没有工具栏 |
| `Inspector` | `DashboardPanelKind::Inspector { metric_id }` | 表格快照；按共享间隔刷新 |

`DashboardPanelKind::Inspector { metric_id }`（`crates/dbflux_ui_base/src/dashboard_manager.rs`）不携带任何图表引用——检查器仅由 `metric_id` 标识。每个检查器面板承载一个 `DataGridPanel`，显示 `InstanceCatalog::fetch_inspector_snapshot` 返回的当前快照（例如 PostgreSQL 的 `pg_stat_activity`、MySQL 的 `PROCESSLIST`、MongoDB 的 `currentOp`、Redis 的 `CLIENT LIST`）。

持久化方面：`Inspector` 取值存放在 `viz_dashboard_panels.panel_kind` 中，检查器的键存放在 `inspector_metric_id`。两者都由**迁移 014**（`014_viz_inspector_and_instance_metric`）引入；该迁移扩展了 `panel_kind` 的 CHECK 约束——这一约束最早由迁移 013 引入，当时只接受 `chart` / `divider`——使其也接受 `inspector`。（从侧边栏的「实例检查器」文件夹直接打开的独立检查器标签页，使用的是 `DocumentKey::InstanceInspector { profile_id, metric_id }`。）

### 检查器行操作

部分检查器提供逐行操作（例如**终止连接** / **终止会话**）。这些操作：

- **受权限控制** — 你没有权限执行的操作会被隐藏，因此你不会看到点了也必然失败的按钮。
- **破坏性操作会先确认** — 破坏性操作在运行前会弹出确认。
- **记入审计** — 每次尝试都会记录一条审计事件；失败时会弹出 Toast 提示，并附带指向对应审计行的链接。

检查器的各行可以提供由驱动程序定义的行操作（右键菜单，或在所选行上按 `m`），例如*终止连接* / *结束会话*。流程如下：

1. 驱动程序通过 `InstanceCatalog::row_actions(metric_id)` 返回一组 `InspectorRowAction`。可用性由各驱动程序的权限探测把关（见各驱动程序 README），因此权限不足的会话永远不会看到自己执行不了的操作。
2. 标记为 `is_destructive` 的操作在执行前会弹出确认对话框。
3. 确认后，连接会在**执行时**（而非点击时）重新解析，然后运行 `InstanceCatalog::execute_row_action(metric_id, action_id, row_values)`。
4. 每次尝试都会记录一条审计事件。失败会经由 `report_error_async`（`ErrorKind::Driver` 的 `UserFacingError`）上报，因此用户会看到一个带有关联 ID 的 Toast 提示，可据此跳转到对应的审计记录。

执行逻辑位于 `crates/dbflux_ui_document/src/instance_inspector/mod.rs`。

### 刷新行为

仪表盘、独立图表和检查器的刷新计时器，在每次触发前都会检查 `AppState::connections()` 中该面板所属的连接配置；连接已关闭时就跳过本次工作。计时器本身保持存活，因此重连后刷新会自动恢复，无需重新设置。

### 内置驱动程序的覆盖情况

| 驱动程序 | `INSTANCE_METRICS` | `INSTANCE_INSPECTOR` | 指标 / 检查器列表 |
|---|:---:|:---:|---|
| PostgreSQL | ✓ | ✓ | [README](../crates/dbflux_driver_postgres/README.md) |
| MySQL / MariaDB | ✓ | ✓ | [README](../crates/dbflux_driver_mysql/README.md) |
| MongoDB | ✓ | ✓ | [README](../crates/dbflux_driver_mongodb/README.md) |
| Redis | ✓ | ✓ | [README](../crates/dbflux_driver_redis/README.md) |
| SQL Server | ✓ | ✓ | [README](../crates/dbflux_driver_mssql/README.md) |
| ClickHouse | ✓ | ✓ | [README](../crates/dbflux_driver_clickhouse/README.md) |
| InfluxDB（仅 v2） | ✓ | ✓ | [README](../crates/dbflux_driver_influxdb/README.md) |

各驱动程序的 README 会列出它具体暴露了哪些指标、检查器和行操作，本文档不再重复。

## 驱动程序接缝

驱动程序通过通用的核心接缝接入仪表盘互操作能力——界面永远不会根据驱动程序 ID 做分支。

### 导入仪表盘（JSON → 本地仪表盘）

- **trait**：`DashboardImporter`（`crates/dbflux_core/src/connection/dashboard_import.rs`）
- **能力**：`DriverCapabilities::DASHBOARD_IMPORT`
- **值类型**：
  - `WidgetImportSpec` —— 解析后的部件规格
  - `MetricView::{TimeSeries, StackedArea, SingleValue}`
  - `ImportedMetricSeries` —— 序列与维度
  - `WidgetLayout` —— 透传到本地网格的原生布局坐标

驱动程序把仪表盘 JSON 解析成一组规范化的部件，界面再把这些部件作为 `SavedChart` 导入，并排布到一个新的 `Dashboard` 上。

### 浏览远程仪表盘（侧边栏）

- **trait**：`DashboardSource`（`crates/dbflux_core/src/connection/dashboard_source.rs`）
- **能力**：`DriverCapabilities::DASHBOARD_SYNC`
- **值类型**：`RemoteDashboard`、`DashboardRef`（可选的 `last_modified: ISO8601`）

侧边栏通过这一接缝列出上游仪表盘，结果缓存在 `RemoteDashboardCache` 中。选中某个远程仪表盘会触发 `DashboardImporter`，把它在本地实体化。

### 实例指标与检查器

- **trait**：`InstanceCatalog`（`crates/dbflux_core/src/connection/instance_catalog.rs`）
- **能力**：`DriverCapabilities::INSTANCE_METRICS`（时序）、`DriverCapabilities::INSTANCE_INSPECTOR`（表格快照）
- **值类型**：`InstanceMetricDef`、`InstanceInspectorDef`、`DefaultInstanceDashboard`、`InspectorRowAction`

驱动程序通过同一个目录暴露实时服务器指标（例如 `pg.tps`、`mysql.queries_per_sec`、`clickhouse.query`）和表格型检查器（例如 `pg.activity`、`mysql.processlist`、`mongo.currentop`、`redis.client_list`、`clickhouse.processes`）。每个驱动程序还会发布一个 `DefaultInstanceDashboard` 描述符，采用固定的 12 列布局——工作区会把这个描述符作为一个**只读的实例概览**仪表盘打开（去重键 `DocumentKey::InstanceOverview { profile_id }`）。「另存为可编辑」操作则会把该布局克隆为一个由用户拥有的持久化仪表盘。

检查器的各行可以声明 `InspectorRowAction`（例如*终止连接*）。操作的可用性由各驱动程序的权限探测把关（PostgreSQL 为 `pg_monitor` / `pg_signal_backend`，MySQL 为 `PROCESS` / `CONNECTION_ADMIN`，MongoDB 为 `killOp`，Redis 为 `CLIENT KILL`，SQL Server 为 `VIEW SERVER STATE` / `KILL`，ClickHouse 为 `KILL QUERY`），因此权限不足的会话永远不会看到自己执行不了的操作。

每一个刷新计时器（仪表盘 tick、独立图表 tick、检查器 tick）都会检查 `AppState::connections()` 中该面板所属的连接配置，并在连接已关闭时跳过本次工作；计时器本身保持存活，因此重连后刷新会自动恢复。

关于这一接缝的用户可见行为（实例概览如何打开、*另存为可编辑*、检查器面板与行操作），参见上文[实例概览与检查器](#实例概览与检查器)。

### CloudWatch 的实现

`crates/dbflux_driver_cloudwatch/src/` 提供：

- `CloudWatchDashboardSource` —— 通过 AWS SDK 列出 CloudWatch 仪表盘
- `CloudWatchDashboardImporter` —— 把 CloudWatch 仪表盘 JSON 解析为 `WidgetImportSpec`，其中包含指标序列、维度与统计聚合方式

这是**只读的浏览与导入**，并非同步功能。DBFlux 从不回写 CloudWatch 仪表盘。

## 能力矩阵

| 能力位 | 含义 |
|---|---|
| `DASHBOARD_IMPORT`（51） | 驱动程序可以把仪表盘 JSON 解析为部件规格 |
| `DASHBOARD_SYNC`（52） | 驱动程序可以列出上游仪表盘 |

两项能力相互独立：驱动程序可以只声明同步而不支持导入，反之亦然。

## 新增支持仪表盘的驱动程序

1. 在驱动程序的 `Connection` 上实现 `DashboardSource`，用于列出上游仪表盘，并把 `DASHBOARD_SYNC` 加入 `DriverMetadata.capabilities`。
2. 在驱动程序的 `Connection` 上实现 `DashboardImporter`，用于把仪表盘负载解析为一组 `WidgetImportSpec`，并加入 `DASHBOARD_IMPORT`。
3. 界面会自动在侧边栏呈现仪表盘树，并接管导入流程，无需任何针对具体驱动程序的分支。
