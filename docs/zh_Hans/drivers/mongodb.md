# MongoDB

面向现代应用的文档数据库。

## 速览

- **类别** —— 文档型
- **查询语言** —— MongoDB 查询语法
- **默认端口** —— 27017
- **URI 方案** —— `mongodb`

面向 DBFlux 的 MongoDB 文档驱动程序。

## 功能

- 归类为 `DatabaseCategory::Document`、使用 `MongoQuery` 查询语言的文档型驱动程序；编辑器使用 MongoDB shell 语法，而非 SQL。
- 连接模式：手动（主机/端口/凭据/数据库）与 URI 模式。URI 模式接受 `mongodb://` 与 `mongodb+srv://` 连接串（会解析 SRV 记录以发现副本集）。
- 支持多个逻辑数据库（`MULTIPLE_DATABASES`），带集合浏览与文档计数。
- 支持身份认证（`AUTHENTICATION`）与 TLS/SSL（三种模式：`off`、`on`、`verify`），支持根证书与可选的客户端证书。
- 支持 SSH 隧道，以便通过堡垒机访问 MongoDB。
- 支持 `db.collection.method(...)` 与 `db.method(...)` 形式的 shell 风格查询解析，并保留 JSON 文档回退路径以向后兼容。支持的方法：`find`、`findOne`、`aggregate`、`count`/`countDocuments`、`insertOne`、`insertMany`、`updateOne`、`updateMany`、`deleteOne`、`deleteMany`。解析错误会携带字节偏移位置，供编辑器诊断使用。
- 聚合管道（`AGGREGATION`）；查询能力声明了 order-by、group-by、having、limit 与 offset。
- WHERE 运算符：`Eq`、`Ne`、`Gt`、`Gte`、`Lt`、`Lte`、`In`、`NotIn`，以及逻辑 `And`/`Or`/`Not`。
- 通过游标与分页令牌两种方式分页（`PaginationStyle::Cursor`、`PaginationStyle::PageToken`）。
- 面向文档的 Schema 元数据：集合字段与索引（`INDEXES`），嵌套文档与数组映射到文档树视图（`NESTED_DOCUMENTS`、`ARRAYS`）。
- 变更：插入、更新（含 upsert）与删除（`supports_upsert: true`）。`MongoShellGenerator` 会生成 `insertOne`/`insertMany`、`updateOne`/`updateMany`（带 `{ upsert: true }`）与 `deleteOne`/`deleteMany`，用于预览与「复制为查询」。
- DDL：删除数据库、删除集合、创建索引与删除索引。
- 结果的 JSON 导出（`EXPORT_JSON`）。
- 连接时把客户端身份上报为 `appName=dbflux/<version>`（可在服务器日志与 `db.currentOp()` 中看到），除非连接 URI 自己设置了 `appName`。
- 写入权限探测：连接后，通过检查 `connectionStatus`（`showPrivileges: true`）中授予写权限的特权/角色，把会话判定为可写、只读或未知；当直连到不可写节点（例如从节点）时，`hello` 会把结论覆盖为只读。

### 实例指标

基于 MongoDB `serverStatus` 命令精选的一组实时服务器指标。指标通过 BSON 点号路径遍历提取：

- `mongo.connections_current` —— 当前打开的连接数
- `mongo.connections_available` —— 可用的连接槽位数
- `mongo.opcounters_insert` —— 启动以来的插入操作数
- `mongo.opcounters_query` —— 启动以来的查询操作数
- `mongo.opcounters_update` —— 启动以来的更新操作数
- `mongo.opcounters_delete` —— 启动以来的删除操作数
- `mongo.opcounters_getmore` —— 启动以来的 getMore 操作数
- `mongo.mem_resident` —— 常驻内存（MB）
- `mongo.mem_virtual` —— 虚拟内存（MB）
- `mongo.network_bytes_in` —— 启动以来接收的字节数

每个指标以单行 `(timestamp_ms, value)` 返回，用于实时绘图。

### 实例检查器

以表格形式呈现运行中服务器状态的快照：

- `mongo.current_op` —— 来自 `$currentOp` 聚合管道的执行中操作（opid、type、ns、op、secs_running、wait_for_lock）

## 限制

- 带有行数限制（包括零）或语句超时的执行请求会在分派前被拒绝，因为 MongoDB 无法保证这些保护条件。不带保护条件的请求保持原有行为。

- 不支持 SQL；查询必须使用 MongoDB shell 风格语法（或 JSON 回退方式）。

- 实例指标每次调用只返回一个数据点（`serverStatus` 的当前快照），而非历史时序。操作计数器（例如 `mongo.opcounters_insert`）单调递增 —— 应将其理解为两次采样之间的增量，而不是绝对速率。

- `$currentOp` 需要 `inprog` 权限，在 Atlas 集群上则需要 `clusterMonitor` 角色。权限不足时，`fetch_inspector_snapshot("mongo.current_op")` 会返回空结果集。
- 不支持查询取消（未设置 `QUERY_CANCELLATION`）。
- 不支持 `RETURNING`；变更能力在能力层面也报告不支持批量插入、批量更新与批量删除（`supports_batch`、`supports_bulk_update`、`supports_bulk_delete` 均为 `false`），即便生成器能生成 `updateMany`/`deleteMany` 文本。
- 解析器的覆盖范围有意限定在上面列出的受支持方法集，而非完整的交互式 shell 语言；`distinct` 未作为查询能力呈现（`supports_distinct: false`）。
- 在查询能力层面不支持 join、子查询、union、CTE、窗口函数与 `EXPLAIN`。
- 事务在能力层面被声明为支持（`supports_transactions: true`），但不支持隔离级别、保存点、嵌套事务、只读事务与可延迟（deferrable）事务。
- DDL 非事务性（`transactional_ddl: false`）；不支持创建数据库、创建集合、alter、视图与触发器。
