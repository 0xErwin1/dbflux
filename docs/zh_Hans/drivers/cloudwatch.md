# CloudWatch Logs

AWS CloudWatch Logs Insights 查询，源上下文由编辑器管理。

## 速览

- **类别** —— 日志流
- **查询语言** —— Logs Insights（SQL 编辑器模式）
- **URI 方案** —— `cloudwatch`

面向 DBFlux 的 AWS CloudWatch Logs 驱动程序，基于 [`aws-sdk-cloudwatchlogs`](https://crates.io/crates/aws-sdk-cloudwatchlogs) SDK 构建。

## 功能

- 归类为 `DatabaseCategory::LogStream` 的日志流驱动程序；`deployment_class` 为 `CloudManaged`。声明的能力为 `AUTHENTICATION` 与 `METRIC_SERIES`。
- 通过区域、具名配置文件以及可选的端点覆盖完成 AWS 连接配置，与 DynamoDB 的 AWS 连接流程保持一致。
- 查询通过 `StartQuery` + 轮询 `GetQueryResults` 执行（轮询间隔 500 毫秒，最多 120 次），源上下文由编辑器管理，提供目标日志组与时间范围。未请求额外限制的 Logs 查询使用 SDK 固定的 `StartQuery` 结果上限 1000。
- 可从源上下文的「Syntax」下拉框中选择三种查询语法：
  - CloudWatch Logs Insights QL（`cwli`，默认）—— `QueryLanguage::CloudWatchLogsInsightsQl`。
  - OpenSearch PPL（`ppl`）—— `QueryLanguage::OpenSearchPpl`。
  - OpenSearch SQL（`sql`）—— `QueryLanguage::OpenSearchSql`。
  它们分别映射到 SDK 的 `Cwli`、`Ppl` 与 `Sql` 查询语言取值。
- 源上下文规格（`SourceContextSpec`）暴露「日志组」目标选择器与开始/结束时间范围控件；CWLI 与 PPL 查询通过 `set_log_group_names` 把选中的日志组传给 `StartQuery`。
- Schema 发现通过 `fetch_log_groups` 枚举日志组，作为唯一的逻辑数据库（`SchemaLoadingStrategy::SingleDatabase`，默认数据库 `logs`）。
- 日志流以分页的集合子项形式呈现（`collection_children`，基于 `fetch_log_stream_page`），并作为事件流打开（`CollectionPresentation::EventStream`）。
- 事件流浏览（`browse_event_stream` / `EventStreamTarget`）由 `FilterLogEvents` 支撑，默认 24 小时的浏览窗口，支持筛选模式、流名前缀、显式流名，以及「最近优先」开关。
- Insights 的列名被归类为语义化的 `ColumnKind`（例如 `@timestamp`、`@ingestionTime` 被识别为时间戳），用于图表自动检测。
- 通过 `GetMetricData` 获取 CloudWatch 指标：每次请求执行一个 `MetricDataQuery`，把响应映射为按时间戳升序排列的两列（时间戳、值）`QueryResult`。来自 AWS 的时间戳（秒精度）会转换为毫秒。当返回多个 `MetricDataResult` 条目时，支持把多指标透视成宽格式。
- 可通过 `ListMetrics` 的分页浏览 CloudWatch 指标目录（命名空间，以及各命名空间下带维度组合的指标）。命名空间列表通过不带筛选条件地扫一遍 `ListMetrics` 并收集不同的命名空间字符串来合成。结果由 `MetricCatalogCache` 在会话内缓存。
- 指标目录可从连接的侧边栏树中浏览（指标 > 命名空间 > 指标）。点击某个指标叶子会打开一个已预填默认值的图表（平均值 / 5 分钟周期 / 跨所有维度聚合），并立即执行。图表文档中的选择器侧栏可用于细化维度、周期与统计方式。
- 客户端身份：每个请求都以 `dbflux-<version>` 作为 AWS SDK 应用名，可在 CloudTrail 的 `userAgent` 字段中看到。
- `CloudWatchLanguageService`（`language_service.rs`）如实反映了这三种查询面都是只读的：`detect_dangerous` 始终返回 `None`，`classify_execution` 始终报告 `Read`，而不会对 Logs Insights QL / PPL / OpenSearch SQL 文本套用 SQL 的危险查询启发式规则或 SQL 语法（这些方言都没有查询形状的变更或删除面；删除日志组/日志流属于管理 API 动作，而非查询）。

## 限制

- `execute` 在发送 Logs 或 Metrics 请求前，对任何指定的行数限制（包括零）或语句超时返回 `NotSupported`。Logs SDK 的固定上限并非请求指定的限制；没有默认超时，也不保证服务端工作量受到限制。
- `profile` 字段（AWS 具名配置文件）是一个 `AuthProfileRef` 表单字段。通用的可移植性接缝（`DbDriver::export_field_hint`）把所有 `AuthProfileRef` 字段映射为 `RequiredOnImport`，因此该字段值不会出现在任何导出的包中，接收方必须在导入时提供或创建匹配的认证配置文件。无需为此做针对特定驱动程序的覆盖。
- 未实现查询取消；`cancel()` 返回 `NotSupported`。
- OpenSearch SQL 模式不接收外部日志组：SQL 查询必须在其 SQL 文本中声明所查询的日志组，因为 CloudWatch API 不接受为 SQL 模式传入外部日志组参数（只有 CWLI 与 PPL 会走 `set_log_group_names`）。
- 编辑器的语法高亮仍是通用的（元数据层面把 `query_language` 报告为 `Sql`）；模式选择决定的是执行语义与补全关键字，而不是按模式区分的高亮。
- 只读：未声明变更、DDL、事务与分页能力（`query`、`mutation`、`ddl`、`transactions`、`limits` 均为 `None`）；`schema_features` 为空。
- 没有 SSL 表单（TLS 由 AWS SDK 的传输层处理）。
- 指标执行每次调用只支持一个 `MetricDataQuery`。
- 命名空间列表的合成（不带筛选条件地扫 `ListMetrics`）在指标很多的 AWS 大账号上可能较慢；一旦完成会在会话内缓存。该扫描上限为 50 页（约 25000 个指标），以约束超大账号下的最坏情况。触及上限时，命名空间列表会被静默截断并记录一条警告；后续会把上限替换为完整的超时 + 取消机制。
- 指标的真实集成测试（`live_execute_cloudwatch_metric`）需要真实的 AWS 凭据，默认以 `#[ignore]` 跳过。LocalStack Community 不支持 CloudWatch Metrics API。
- `tests/live_integration.rs` 在 CI 中针对 LocalStack Community 容器运行 Logs 数据面（日志组/日志流发现、事件浏览）。`DashboardImporter` 是纯 JSON 解析，也以同样方式验证。`DashboardSource`（依托 Metrics 家族的 `GetDashboard`/`ListDashboards` API）与 CloudWatch Logs Insights（`StartQuery`/`GetQueryResults`）也会对 LocalStack 尝试，但当社区版拒绝该调用时会记录一条消息并跳过；两者的完整端到端验证都需要真实的 AWS 账号（或 LocalStack Pro）。
- 没有写入权限探测：`Connection::probe_write_privilege` 有意保持 trait 默认值（`WritePrivilege::Unknown`），因为可靠的检查需要 `iam:SimulatePrincipalPolicy`，而连接所用的角色通常不具备该权限。
- 没有实例指标与实例检查器（未声明 `INSTANCE_METRICS`/`INSTANCE_INSPECTOR`）：CloudWatch Logs 自身的服务端指标属于 CloudWatch Metrics，因此再提供一个按驱动程序的 `InstanceCatalog` 只会重复这一层，而不是新增能力。
- 没有 `QueryGenerator`：`Connection::query_generator()` 保持 trait 默认值（`None`）。Logs Insights QL、PPL 与 OpenSearch SQL 都是只读查询面，没有可供预览的变更/DDL 形状。
