# InfluxDB

支持 InfluxQL 与 Flux 查询的 InfluxDB v1 / v2 时序数据库。

## 速览

- **类别** —— 时序
- **查询语言** —— InfluxQL / Flux
- **默认端口** —— 8086
- **URI 方案** —— `http`

面向 DBFlux 的 InfluxDB 驱动程序。

## 功能

- **时序类别** —— 归类为 `DatabaseCategory::TimeSeries`，默认编辑器语言为 `QueryLanguage::InfluxQuery`。声明的能力包括 `AUTHENTICATION`、`MULTIPLE_DATABASES`、`PAGINATION`、`EXPORT_CSV`、`EXPORT_JSON`、`CHART_AUTHORING`、`INSTANCE_METRICS` 与 `INSTANCE_INSPECTOR`。连接使用 `http` URI 方案、默认端口 8086，TLS 由基于 rustls 的 HTTP 客户端提供。
- **InfluxDB v1 与 v2** —— 两个 API 版本在同一个驱动 crate 中均受支持。
- **两个版本都支持 InfluxQL** —— v1 的查询语言在 v1 上可用，在 v2 上则经由 v2 的兼容端点使用。
- **v2 上的 Flux** —— 当连接配置为 v2 时，可以使用 Flux 查询。
- **可选的默认存储桶** —— 连接配置中的存储桶（v2）或数据库（v1）字段是可选的。v2 的 API 令牌可访问组织中的所有存储桶；v1 的用户可访问服务器上的所有数据库。该字段留空时，用户可在编辑器中通过源上下文下拉框按查询选择存储桶；填写它则只是预先选中该存储桶，并不限制访问其他存储桶。
- **按查询路由存储桶** —— 每条 InfluxQL 查询所使用的存储桶来自源上下文下拉框的选择，而不是连接配置。对于 Flux 查询，存储桶内嵌在查询文本本身中（`from(bucket: "...")`）。
- **无需存储桶的存活检查** —— 连接存活检查不要求指定存储桶：v1 针对内部数据库执行 `SHOW DATABASES`；v2 则请求 `/api/v2/buckets?limit=1`。
- **时间范围宏** —— InfluxQL 与 Flux 查询都支持兼容 Grafana 的宏标记，这些标记会在查询被送往驱动程序之前，替换成所绑定的时间范围窗口：

  | 标记 | 语言 | 展开结果 |
  |---|---|---|
  | `$timeFilter` | InfluxQL | `time >= 'RFC3339_start' AND time <= 'RFC3339_end'` |
  | `$__from` | InfluxQL | `'RFC3339_start'` |
  | `$__to` | InfluxQL | `'RFC3339_end'` |
  | `v.timeRangeStart` | Flux | `'RFC3339_start'` |
  | `v.timeRangeStop` | Flux | `'RFC3339_end'` |

  这些标记沿用 Grafana 的变量约定（InfluxQL 用 `$timeFilter`，Flux 用 `v.timeRangeStart`/`v.timeRangeStop`）。熟悉 Grafana 的用户会觉得这套语法很直观。

  RFC3339 格式：`YYYY-MM-DDTHH:MM:SSZ`（UTC，秒精度，带 `Z` 后缀）。

  **InfluxQL 示例** —— 使用 `$timeFilter`：

  ```influxql
  -- 输入：
  SELECT mean(usage_user) FROM cpu WHERE $timeFilter GROUP BY time(1m)

  -- 实际执行（窗口为 2026-05-20T00:00:00Z 至 2026-05-22T23:59:00Z）：
  SELECT mean(usage_user) FROM cpu WHERE time >= '2026-05-20T00:00:00Z' AND time <= '2026-05-22T23:59:00Z' GROUP BY time(1m)
  ```

  **Flux 示例** —— 使用 `v.timeRangeStart` / `v.timeRangeStop`：

  ```flux
  -- 输入：
  from(bucket: "telegraf")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "cpu")

  -- 实际执行（同一窗口）：
  from(bucket: "telegraf")
    |> range(start: '2026-05-20T00:00:00Z', stop: '2026-05-22T23:59:00Z')
    |> filter(fn: (r) => r._measurement == "cpu")
  ```

  **宏需要已绑定的窗口** —— 如果查询包含宏标记，但没有设置时间范围窗口（即源上下文面板没有选中项），这些宏会原样传给驱动程序而不做替换。由于 `$timeFilter` 等并非合法的 InfluxQL/Flux 语法，InfluxDB 会返回解析错误。

  **宏会抑制「缺失时注入」** —— 当查询包含任一已被识别的宏标记时，自动的时间窗口注入（见下文）会被抑制。宏替换被视为用户给出的权威时间边界。

  **v1 已知限制（朴素的子串替换）** —— 位于带引号字符串字面量或注释中的宏标记也会被替换。v1 没有转义语法。对于 Flux，任何名称恰好以 `v.timeRangeStart` 或 `v.timeRangeStop` 开头的变量（例如 `v.timeRangeStartCustom`）也会被替换。未来的版本计划实现真正的分词处理。

- **自动的时间窗口注入** —— 当通过源上下文面板设置了时间范围，且查询本身尚未包含时间谓词（InfluxQL 为 `time >=` 等，Flux 为 `|> range(`）时，驱动程序会自动注入边界。当查询含有显式的时间范围宏标记时，该行为被抑制。
- **结构化的错误消息** —— 服务端错误从 JSON 的 `{"error": "..."}` 字段解析，而不是把原始的 HTTP 状态码直接显示出来。
- **CSV 与 JSON 导出** —— 查询结果可通过标准的 DBFlux 导出管道导出。
- **审计发出** —— 所有查询都经由标准的 DBFlux 审计接收端（sink）跟踪。`bucket_or_database` 元数据字段记录的是每条查询实际使用的存储桶，而不是配置中的默认值。
- **多语句 InfluxQL** —— 当查询包含多个以 `;` 分隔的语句时（例如 `SHOW MEASUREMENTS; SHOW SERIES`），所有结果会被拼接成单个结果集，并在前面附加一个合成的 `statement_index` 整数列，用以区分来自不同语句的行。
- **「查询测量」右键菜单** —— 右键点击侧边栏中的某个测量会显示「查询测量」。该动作会打开一个新的代码文档，其中预填了模板查询（InfluxQL 为 `SELECT * FROM ...`，Flux 为 `from(bucket: ...) |> range(...)`）。
- **打开测量时以图表显示** —— 从侧边栏打开一个测量时，会针对该测量所属的存储桶或数据库，以 InfluxQL 运行 `SELECT * FROM "<measurement>" ORDER BY time DESC LIMIT <n> OFFSET <m>`（在 v2 上经由 InfluxQL 兼容端点）。`InfluxQueryGenerator::collection_browse_query` 返回的正是同一条语句，因此数据网格的工具栏和状态栏显示的就是实际运行的 InfluxQL 查询。结果包含一个时间戳列和带类型的字段列，因此测量会在 Chart 视图中打开，切换到 Data 视图即可查看各行。
- **存储桶上的「新建查询」右键菜单** —— 右键点击存储桶/数据库节点会显示「新建查询」，打开一个已激活该连接的空白代码文档。
- **读取模板生成** —— `InfluxQueryGenerator` 为 InfluxQL 与 Flux 生成「查询全部」以及按测量的读取模板（供右键菜单动作与「复制为查询」使用），并依据连接所配置的版本与默认存储桶感知版本差异。
- **客户端身份** —— 每个 HTTP 请求都以 `dbflux/<version>` 作为 `User-Agent` 头，可在服务器端的请求日志中看到。
- **感知方言的危险查询检测** —— `InfluxLanguageService` 会从查询文本判别是 InfluxQL 还是 Flux（Flux 呈管道形状，或以 `import` 开头），并在执行前标记出 InfluxQL 中的 `DROP DATABASE/MEASUREMENT/SERIES/RETENTION POLICY/SHARD`、不带 `WHERE` 子句的 `DELETE`，以及 Flux 中的 `delete()`/`influxdb.delete()` 调用。`classify_execution` 把 `SELECT`/`SHOW` 报告为读取，把 `DELETE`/`DROP` 与 Flux 的 `delete()` 报告为破坏性操作，因此治理策略看到的是准确的影响级别，而不是笼统的「写入」。`validate` 只是语法层面的健全性检查（括号与引号是否配平），并非完整的解析器。
- **写入权限探测（仅 v2）** —— 连接后请求 `GET /api/v2/authorizations`，检查连接所用令牌自身的权限：`buckets` 上有任何 `write` 权限即判定为可写，只有 `read` 权限则判定为只读；请求失败或被拒绝时（令牌通常不具备 `read:authorizations`），解析出的变更策略保持不变。v1 没有等价的、按令牌作用域的 API，因此从不做探测。
- **实例指标与检查器（仅 v2）** —— `InstanceCatalog` 的图表序列与表格快照来自服务器自身的 `GET /metrics` 端点（Prometheus 文本暴露格式），而不是 `_monitoring` 存储桶：`_monitoring` 存放的是由 Tasks 与 Checks 系统写入的告警检查/通知记录，在默认安装下是空的；`/metrics` 才是 InfluxDB 真正的遥测面。指标覆盖 Go 运行时内存、goroutine 与操作系统线程数、服务器运行时长与存储桶数量、BoltDB 的读写计数器，以及 HTTP API 的请求计数；跨标签组合拆分的 Prometheus 样本（例如按 handler/path/status 拆分的 `http_api_requests_total`）会被加总为每个指标的单一取值。对外提供两个检查器：`influx.health`（`GET /health` 的快照）与 `influx.metrics`（完整的 Prometheus 抓取结果，以名称/标签/取值构成的表格）。另有精选的 `DefaultInstanceDashboard` 把两者组合起来。

## 限制

- `execute()` 对任何请求的行数限制（包括零）或语句超时，在发送 HTTP 请求或分派实例上下文前返回 `NotSupported`。未请求这些保护的查询仍可执行；默认编辑器无法保证这些保护。

- **不支持查询取消** —— `cancel()` 返回 `NotSupported`；进行中的查询无法从界面中止（未声明 `QUERY_CANCELLATION`）。
- **不生成变更语句** —— `QueryGenerator::generate_mutation` 始终返回 `None`；只生成读取模板，这与只读的查询 API 一致。
- **v1 不支持 Flux** —— 对 v1 连接运行 Flux 查询会立即返回错误，不会发出 HTTP 请求。
- **不支持 INSERT/UPDATE/DELETE** —— InfluxDB 的查询 API 是只读的。数据写入使用的是 Line Protocol 写入 API，本驱动程序未暴露它。
- **不支持事务** —— InfluxDB 本身不支持事务。
- **InfluxQL 需要一个存储桶** —— InfluxQL 查询把存储桶嵌在 URL 中（`?db=<bucket>`）。如果源上下文下拉框与配置默认值都没有提供存储桶，执行会被拒绝，并给出要求用户选择一个存储桶的明确错误。
- **基于正则的时间谓词检测** —— 驱动程序使用正则表达式判断查询是否已包含时间谓词。当带引号的字符串字面量碰巧含有匹配 `time <`、`time >` 或 `|> range(` 的文本时，可能会误判。
- **多语句的列由第一条非空语句决定** —— 当多语句查询返回的结果形状不同（例如 `SHOW MEASUREMENTS; SHOW SERIES`）时，列的布局由第一条非空语句决定，后续语句的行会被映射到该布局上。形状不匹配时会产生列错位，而不是报错。
- **通过 Authorization 头做基本认证** —— v1 的用户名/密码凭据以 `Authorization: Basic <base64>` 头发送，而不是通过 URL 查询参数。这对日志卫生更友好，但与某些 InfluxDB 客户端库的做法不同。
- **向后兼容的序列化** —— 用旧的必填 `bucket_or_database` 字段保存的配置仍可正确加载。该字段通过一个 serde 别名反序列化为 `default_bucket`。此变更之后保存的配置使用 `default_bucket` 键。
- **实例指标与检查器仅支持 v2** —— 在 v1 连接上 `instance_catalog()` 返回 `None`。v1 确实也提供 `/metrics`，但其暴露的内容不含本目录所声明的 v2 指标名，而 `/health` 是 v2 的端点（v1 提供的是 `/ping`）。`execute()` 对 v1 连接上的实例查询会以 `NotSupported` 拒绝，而不是从一个本目录从未验证过的面去回答，这与上面提到的 v1 写入权限探测限制是一致的。
- **实例指标不支持行操作** —— `InstanceCatalog::row_actions` 使用 trait 默认实现（空列表）；`/metrics` 是一次只读的遥测抓取，没有任何可由某一行触发的服务端动作。
- **浏览测量时忽略过滤输入** —— `browse_collection` 与 `count_collection` 不读取集合过滤条件，因此在数据网格过滤框中输入的内容不会缩小结果行。
- **浏览测量没有时间窗口** —— 浏览会读取整个保留期内最新的行，并且图表的时间范围预设不会对测量提供。需要限定时间的查询请使用「查询测量」。
- **结果中不区分标签与字段** —— 两者都作为普通列返回。图表按第一个文本列分组，它通常是一个标签，但也可能是字符串字段。
