# ClickHouse

基于 HTTP 的列式分析型数据库。

## 速览

- **类别** —— 关系型
- **查询语言** —— SQL
- **默认端口** —— 8123
- **URI 方案** —— `http`

## 连接

该驱动程序使用 ClickHouse 的 HTTP 接口而非原生协议，因此端点是 URL，而不是主机/端口对。

| 字段 | 默认值 | 说明 |
|---|---|---|
| HTTP URL | `http://localhost:8123` | `https://` 端点经由 rustls 提供服务 |
| 数据库 | `default` | 限定 Schema 发现与未加限定的查询的作用范围 |
| 请求超时 | `30` 秒 | 必须大于零 |
| 用户 | `default` | |
| 密码 | —— | 保存在操作系统密钥环中，并以 HTTP Basic 认证方式发送 |

## 功能

- 基于 rustls 的阻塞式 HTTP(S) 传输，使用 HTTP Basic 认证。
- 支持任意单语句 SQL，响应被强制为 `JSONCompact`，因此列名与类型会随数据行一同返回。
- 每次响应都会把行宽与声明的列数进行核对，因此格式异常的响应会以明确的错误失败，而不会让取值在列之间错位。
- 从 `system.databases`、`system.tables` 与 `system.columns` 中发现 Schema：数据库、表、视图、列、引擎、排序键与分区键、磁盘占用大小，以及压缩方式。
- Schema 按数据库延迟加载，因此拥有大量数据库的服务器不会在连接时为所有数据库付出代价。
- 分页、排序与筛选由驱动程序以 `LIMIT`/`OFFSET` 的形式包在语句外层实现，这也是结果浏览无需游标即可工作的原因。
- 只读的可视化 SELECT 生成，遵循 ClickHouse 的标识符与字面量引用规则。
- 可从查询结果制作图表，并支持 CSV 与 JSON 导出。
- 每个 HTTP 请求都会以 `dbflux/<version>` 作为 `User-Agent` 头，可在服务器端的请求日志中看到。
- 危险查询检测使用共享的 `SqlLanguageService`（没有 ClickHouse 专属覆盖）：`ALTER TABLE ... DELETE WHERE ...` 与 `ALTER TABLE ... UPDATE ... WHERE ...` 已经会被识别为 `Alter`（任何以 `ALTER` 开头的语句都会被标记，无论其子命令是什么），`TRUNCATE`/`DROP` 也照常被识别。`KILL QUERY`/`KILL MUTATION` 与 `OPTIMIZE TABLE ... FINAL` 不会被标记 —— 它们既不删除行，也不改变表结构 —— 这与其他关系型驱动程序处理同类非破坏性管理语句的方式一致。
- 写入权限探测：连接后，会检查服务器设置 `readonly` 以及 `system.grants` 中当前用户及其活动会话角色，以识别只读会话，或缺少 `INSERT`/`ALTER UPDATE`/`ALTER DELETE` 授权的用户；当服务器本来就会拒绝写入时，会把解析出的变更策略收紧为只读（无副作用；只能通过角色之角色间接获得的授权不会被解析，策略保持原样）。
- 实例指标与检查器（`INSTANCE_METRICS`/`INSTANCE_INSPECTOR`）：从 `system.metrics`、`system.events` 与 `system.asynchronous_metrics` 精选的一组度量与计数器（活动查询、已跟踪内存、TCP/HTTP 连接数、已选取/已插入行数、操作系统可用内存），外加一个基于 `system.processes` 的运行中查询检查器，带「终止查询」行操作。该终止操作由构建目录时执行一次的 `KILL QUERY` 权限探测把关；除非探测成功，否则该操作保持隐藏 —— 因此无论探测因何失败，界面里都只是少了这个破坏性控件，而不会提供一个该会话执行不了的操作。

### 类型处理

取值会被递归解码，因此 `Map(String, Array(Nullable(Decimal256)))` 会以完整结构到达，而不是一段原始文本：

- 包装类型 —— `Nullable`、`LowCardinality`
- 容器类型 —— `Array`、`Tuple`、`Map`、`Nested`
- 数值 —— 最大到 `UInt256` 的整数、`Decimal256`、`BFloat16`、`Bool`
- 时间 —— `Date32`、`DateTime64`
- 其他 —— `Enum16`、`Nothing`

## 限制

- 不支持 SSH 隧道。
- 不支持事务、预编译语句与查询取消。正在执行的语句只受请求超时约束，别无其他；该驱动程序也不声明支持锁超时。
- 不支持结构化的 `INSERT`、`UPDATE`、`DELETE`、DDL 或数据传输。写入类 SQL 只有在编辑器中明确输入时才会执行，因此数据网格是只读的，该驱动程序也不能作为传输目标。
- 每个请求只允许一条 SQL 语句；多语句脚本不会被批量执行。
- HTTP 响应体上限为 128 MiB。
- 具名的 ClickHouse 时区不会在客户端解释。带偏移量的 ISO 时间戳会被准确处理；不带偏移量的时间戳则按 UTC 处理。
