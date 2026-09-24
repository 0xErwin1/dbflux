# SQL Server

Microsoft SQL Server 关系型数据库。

## 速览

- **类别** —— 关系型
- **查询语言** —— T-SQL
- **默认端口** —— 1433
- **URI 方案** —— `sqlserver`

面向 DBFlux 的 Microsoft SQL Server 驱动程序，基于 [`tiberius`](https://crates.io/crates/tiberius) TDS 客户端构建。

## 功能

- 面向 SQL Server / Azure SQL 的关系型驱动程序，支持 SQL 查询执行与 Schema 发现。
- 通过 SQL Server 登录账户（用户名 + 密码）认证；URI 模式接受 ADO、JDBC 以及 `sqlserver://user:pass@host:port/db` 形式的连接串。
- 把 `Application Name` 上报为 `dbflux/<version>`，除非连接串或 URI 已设置了该值 —— 此时始终以用户提供的值为准；`sqlserver://`/`mssql://` URL 方案可用 `applicationname` 查询参数来设置它。
- 经由 tiberius 的 `EncryptionLevel` 提供 TLS 加密模式（`off`、`on`、`required`）。表单只暴露一个 **SSL Mode** 下拉框，`TrustServerCertificate` 标志会自动推导：`off` —— 不加密（登录包仍由 TDS 加密）；`on` —— 加密，接受自签名证书，最适合使用其自动生成证书的本地/开发用 SQL Server；`required` —— 加密，校验证书链，适用于持有真实 CA 签名证书的服务器（如 Azure SQL 等）。在 URI 模式下，如果需要非常规组合（例如 `?encrypt=required&trust=true`），可用 `?trust=true|false` 显式覆盖推导出的值。
- 支持可选的 SQL Server 命名实例（`SQLEXPRESS`、`MSSQLSERVER2019` 等），在连接时通过查询 UDP 1434 上的 SQL Browser 解析（由 tiberius 的 `sql-browser-tokio` feature 启用）。表单的 Instance 字段、URI 模式下 SSMS 风格的 `host\instance` 写法，以及 `?instance=` URI 查询参数，设置的都是 tiberius 配置上的同一个 `instance_name`。
- 支持 SSH 隧道，以便通过堡垒机连接（仅转发 TCP 的隧道无法进行命名实例查找）。
- 通过 `USE [database]` 支持按标签页切换数据库；会话状态（`SET` 选项、临时表、事务）在同一连接上的多次 `execute()` 调用之间保持。
- 多结果集批处理：当一个批次产生多个结果集时（例如 `SELECT 1; SELECT 2;`，或含多个 `SELECT` 的存储过程），驱动程序会把**最后一个**非空结果集作为主 `QueryResult` 返回（沿用历史上「最后一条语句生效」的交互），并按批处理顺序把之前的所有非空结果集挂到 `QueryResult.additional_results` 上。纯粹的准备性批次（如 `SET LOCK_TIMEOUT 5000`）仍表现为单个空的主结果集。想遍历每一个结果集的调用方可以使用 `QueryResult::iter_result_sets()`。
- 有界执行：请求的行数上限（`QueryRequest::limit`）在收集阶段执行 —— tiberius 流被惰性读取，只有请求的行会被转换并保留。一个剩余行数预算在整个 execute 请求中共享：批次中的每个服务器结果集都按实际流顺序从同一预算中消耗（该上限是总量，而非按结果集的上限），并且每个结果集都会被排空到底，因此变更操作的效果会完整完成，预算耗尽后后续语句仍会执行，迟到的错误会被传播而不是被丢弃，每个结果集只有在确实省略了行时才报告 `rows_truncated`。主结果仍为最后一个结果集。上限为 `0` 时不保留任何行，但仍会执行并排空全部内容；不带上限的请求保持原来的无限制行为。
- 在任何执行之前、且不清除待处理的取消信号，拒绝无法安全限定的请求：请求的语句超时，以及针对驱动程序实例指标或检查器的带行数上限的请求。
- 数据传输引擎：原生的多行 `INSERT` 批量装载（`BULK_INSERT`，按 T-SQL `VALUES` 的行数限制，每条语句最多 1000 行，通过 `DriverLimits::max_bulk_insert_rows` 暴露），以及依据源表列生成的驱动程序原生 `CREATE TABLE` DDL（同时也支持 `TRUNCATE_TABLE`）。
- 写入权限探测：连接后，检查 `DATABASEPROPERTYEX(DB_NAME(), 'Updateability')` 判断是否为只读数据库（例如 Always On 的可读次要副本）；否则通过 `HAS_PERMS_BY_NAME` 判断该登录是否在任何可见的非系统基表上持有 `INSERT`/`UPDATE`/`DELETE` 权限；当服务器本来就会拒绝写入时，会把解析出的变更策略收紧为只读（无副作用；空数据库属于无结论，策略保持不变）。

### 实例指标

基于 `sys.dm_os_performance_counters` 精选的一组实时服务器指标：

- `mssql.batch_requests_per_sec` —— 每秒 T-SQL 批处理请求数
- `mssql.compilations_per_sec` —— 每秒 SQL 编译次数
- `mssql.recompilations_per_sec` —— 每秒 SQL 重编译次数
- `mssql.user_connections` —— 当前打开的用户连接数
- `mssql.lock_waits_per_sec` —— 每秒锁等待次数（`_Total` 实例）
- `mssql.page_reads_per_sec` —— 每秒缓冲池页读取次数
- `mssql.page_writes_per_sec` —— 每秒缓冲池页写入次数
- `mssql.buffer_cache_hit_ratio` —— 缓冲区缓存命中率（百分比）
- `mssql.server_memory_kb` —— 服务器总内存（KB）

每个指标以单行 `(timestamp_ms, value)` 返回，用于实时绘图。

需要 `VIEW SERVER STATE` 服务器权限。缺少该权限时，`list_metrics()` 返回空列表并记录一条警告。驱动程序在构建目录时探测该权限一次。

### 实例检查器

以表格形式呈现运行中服务器状态的快照：

- `mssql.active_sessions` —— 来自 `sys.dm_exec_sessions` 并与 `sys.dm_exec_requests` 连接后的用户会话（会话 ID、登录名、主机名、程序名、状态、CPU 时间、内存占用、命令、请求状态、等待类型、等待时间、阻塞会话 ID）

需要 `VIEW SERVER STATE` 权限。

### 查询取消

- 取消的实现方式是：从一条全新的旁路通道连接发出 `KILL <spid>`。tiberius 目前没有暴露 SSMS 所用的 TDS Attention 原语，因此次优的选择是请服务器终止正在运行该查询的会话。
- 连接时，驱动程序会捕获 `@@SPID`，并缓存一份 tiberius `Config` 的克隆（其中已包含登录信息）。取消句柄会按需打开第二条连接、执行 `KILL <spid>`，并把主连接标记为已污染。
- 取消之后，`cleanup_after_cancel()` 会重建主 tiberius 客户端、捕获新的 SPID，并重新发出之前的 `USE [db]`，使下一条查询仍在同一个数据库中运行。从界面的角度看，连接始终是连着的，只是底层的会话 ID 变了。
- 在被杀掉的会话上抛出的错误（错误码 596 / 233 / 6005）会被翻译成 `DbError::Cancelled`，因此界面显示的是「查询已取消」，而不是传输层的失败。
- 在现代 SQL Server 上，会话所有者无需 `ALTER ANY CONNECTION` 权限即可 `KILL` 自己的 SPID。在较旧或受限制的登录上，`KILL` 本身可能因权限错误而失败；驱动程序会把该错误呈现给用户。

### Schema 发现

- 数据库（`sys.databases`，隐藏系统数据库）。
- 每个数据库下的表与视图（`sys.tables`、`sys.views`）。
- 每张表的列 + 主键标志、索引、外键。
- 每张表的约束：CHECK 约束（含其定义）与 UNIQUE 约束（通过 `sys.indexes.is_unique_constraint`）。
- 供 Schema 浏览器侧边栏使用的全 schema 索引与外键。
- 用户自定义类型（`sys.types where is_user_defined = 1`），归类为 `Domain`（别名类型）或 `Composite`（表类型）。
- `view_details()` 会校验该视图确实存在于所请求的数据库中。
- **例程：**存储过程（`P`）、标量函数（`FN`）、内联表值函数（`IF`）、多语句表值函数（`TF`）以及 CLR 聚合函数（`AF`）按 schema 通过 `sys.objects` 列出。源码定义通过 `OBJECT_DEFINITION(object_id)` 获取。

### 忠实的 CREATE TABLE（schema diff）

- 列内省报告**精确的类型维度**：`nvarchar`/`nchar` 长度以字符计（UTF-16 字节数除以二，`-1` 渲染为 `MAX`），`varchar`/`char`/`binary`/`varbinary` 为字节长度，`decimal`/`numeric` 保留精度与标度，`datetime2`/`datetimeoffset`/`time` 保留标度，`float(n)` 保留精度。identity 有意**不**出现在 `type_name` 中——它通过结构化的创建元数据传递。
- `table_creation_metadata()` 将 identity 的种子与增量报告为**服务器转换后的精确文本**（`numeric(38,0)` 的 identity 值可能超出任何 64 位整数），按声明顺序报告主键列，提供指明未观测项的完整性报告，并为生成器无法表达的创建语义提供 blocker。
- `generate_code_with_creation_metadata("create_table", …)` 根据来自**引用侧**的元数据渲染忠实的 `CREATE TABLE`：转义后的方括号标识符、精确的 identity 文本、逐列的可空性与默认值，以及按声明顺序排列的主键。当没有元数据时——旧快照必须重新采集为深度快照——它会具名拒绝（`NotSupported`）；元数据不完整或带有 blocker 时同样拒绝。旧版 `generate_code("create_table")` 接缝在设计上会拒绝：它无法携带引用元数据。对于 **schema diff 整表重新生成**，支持完全依赖于元数据感知的 `generate_code_with_creation_metadata` 代码路径，这正是 `DdlCapabilities::supports_create_table` 针对此操作所描述的路径；在此重新生成之外，结构化 DDL 支持照常存在，而旧版 `generate_code("create_table")` 接缝在缺少引用元数据时总是会拒绝。
- schema diff 文档在表存在于引用侧、需要创建到目标侧时使用它；由目标连接生成，引用连接只提供元数据。

### 带 OUTPUT 的 CRUD

- 对某一行的 INSERT/UPDATE/DELETE 会使用 SQL Server 的 `OUTPUT INSERTED.*` / `OUTPUT DELETED.*` 子句，从而把变更后的行数据返回给调用方（`CrudResult::success(row)`），这与 Postgres 驱动程序使用 `RETURNING *` 的方式相同。
- `MutationCapabilities::supports_returning` 为 `true`。
- 行的标识必须是复合主键（对关系型驱动程序而言，这是唯一合理的 `RecordIdentity` 变体）。

### 查询计划

- `explain()` 在 `SET SHOWPLAN_XML ON` 下运行该查询，并以 XML 形式返回查询计划。驱动程序随后总会执行 `SET SHOWPLAN_XML OFF`，以免会话状态泄漏。
- `version_query()` 返回 `SELECT @@VERSION`。

### 方言

- `[方括号]` 形式的标识符引用，`]` 需要转义。
- `N'…'` 形式的 Unicode 字符串字面量；`0x…`（大写）形式的二进制字面量；布尔（`BIT`）值用 `1`/`0` 表示。
- `OFFSET … ROWS FETCH NEXT … ROWS ONLY` 形式的分页（并回退为 `ORDER BY 1`，使不带 ORDER BY 的 OFFSET 查询不会报错）。
- 不使用 `SELECT TOP N`；OFFSET/FETCH 是规范的分页写法。
- 有意不生成 `UPSERT`；SQL Server 的 `MERGE` 存在已知缺陷，应当手写。

### 错误上报

- tiberius 的 `Server` token 错误会通过 `FormattedError` 呈现其数字代码、严重性状态与来源行号。
- 常见的 MSSQL 错误号被映射为语义化的 `DbError` 变体，而不是笼统的 `QueryFailed`：

  | 代码 | DbError 变体 |
  | --- | --- |
  | 4060、18450、18452、18456、18486、18487、18488 | `AuthFailed` |
  | 229、230、262、297、916 | `PermissionDenied` |
  | 207、208、2812、4902 | `ObjectNotFound` |
  | 245、334、515、547、2601、2627、8152 | `ConstraintViolation` |
  | 102、156、8180 | `SyntaxError` |

- 约束冲突的消息会被解析，用以填充 `ErrorLocation`（schema、表、列、约束名），使界面能够高亮出问题的对象。

### 操作与限制

- 所有操作都声明 `transactional_ddl: true` 与 `supports_savepoints: true`。
- 支持的隔离级别：ReadUncommitted、ReadCommitted、RepeatableRead、Serializable、Snapshot。默认值为 ReadCommitted。

## DDL 行为

- **事务性 DDL。** SQL Server 中的大多数 DDL 都是事务性的。把 `CREATE`、`ALTER` 或 `DROP TABLE` 包在 `BEGIN TRAN … COMMIT` / `ROLLBACK` 中是可行的。例外：`CREATE DATABASE`、`DROP DATABASE`、`ALTER DATABASE`、`BACKUP`/`RESTORE` 以及 `CREATE FULLTEXT INDEX` 无法在显式事务中运行。
- **ALTER TABLE 的加锁。** `ALTER TABLE … ADD COLUMN <nullable>` 很快（仅涉及元数据）。添加带默认值的 NOT NULL 列会写入每一个数据页，并获取 Sch-M 锁。`ALTER TABLE … ALTER COLUMN` 可能会重写整张表，并在完成前阻塞读写。
- **在线索引操作**（企业版 / Azure SQL）：`CREATE INDEX … WITH (ONLINE = ON)` 与 `ALTER INDEX … REBUILD WITH (ONLINE = ON)` 允许并发 DML。不指定 `ONLINE = ON` 时，索引构建会获取 Sch-M 并阻塞写入（标准版/精简版只支持离线方式）。
- **TRUNCATE TABLE。** 仅涉及元数据、速度快、事务性，需要对该表拥有 `ALTER` 权限。不能被外键引用的表使用（请改用 `DELETE`，或先删除该外键）。
- **DROP TABLE / DROP VIEW。** 事务性。`IF EXISTS` 在 2016 及以上版本受支持。
- **约束。** 添加 `CHECK` / `UNIQUE` / `FOREIGN KEY` 约束默认会校验所有已存在的行（会短暂获取 Sch-M）。可用 `WITH NOCHECK` 在不扫描的情况下加上约束，之后再用 `WITH CHECK CHECK CONSTRAINT` 在合适的时候做校验 —— 这与 Postgres 的 `NOT VALID` + `VALIDATE CONSTRAINT` 是同一个模式。

## 限制

- 忠实的 `CREATE TABLE` 生成（schema diff）对于无法重现其创建语义的表会选择拒绝而不是扁平化：计算列、用户自定义/CLR（及别名）列类型、稀疏列、`FILESTREAM`、`ROWGUIDCOL`、内存优化表、系统版本 temporal 表、非聚集主键、行/页压缩，以及位于非默认文件组上的表。索引（主键除外）、外键和 CHECK/UNIQUE 约束不会随 `CREATE TABLE` 一起传递；需要单独应用。
- 生成的主键约束名由服务器命名：不会捕获原始约束名，因此 `PK_…` 与源不同。
- schema diff 从浅表列表中检测整表创建。引用创建元数据会**为每个活动引用表收集**（并从每个携带它的快照行读取），但仅在整表新增（`TableAdded`，即需要在目标侧生成新表）时被**消费**。已存在表的 identity 或主键变更既不会被收集进 diff，也不会被应用，需要手动处理。
- 在创建元数据支持（DBF-161 PR1）之前采集的深度快照不携带元数据；将其用作 diff 引用会拒绝创建，必须重新采集。会话数据库已知时，UI 仅在每张表的列或示例字段均已加载后，才在连接时采集深度快照，并纳入可用的创建元数据。采集出错不会断开连接，也不会保存不完整的深度快照；重新连接可获取新的有效快照。**活动连接**仍可通过通用 `table_creation_metadata` 接口提供元数据，无需已保存的快照。元数据缺失、不完整或被阻止时，仍拒绝生成 `CREATE TABLE`。
- 含有任何字符类型列（`char`、`varchar`、`nchar`、`nvarchar`、`text`、`ntext`）的表总是被拒绝：`sys.columns.collation_name` 对所有字符列都非空——即使该 collation 只是从源数据库默认值隐式继承——而生成时无法得知 target 数据库的默认 collation，重建的列可能会静默地以不同方式排序和比较。只有所有列均为无 collation 类型（整数、小数、日期、二进制、非类型化 `xml` 等）的表才能被忠实生成。
- 绑定到 XML 架构集合的类型化 `xml` 列会被拒绝：生成普通 `xml` 列会静默丢失集合绑定。非类型化 `xml` 列可以被忠实生成。

- 实例指标与检查器功能需要 `VIEW SERVER STATE` 服务器权限。缺少该权限时，`list_metrics()` 与 `list_inspectors()` 都返回空列表，而不是报错。

- 实例指标每次调用只返回一个数据点（`sys.dm_os_performance_counters` 的当前值），而非历史时序。速率计数器（例如 `mssql.batch_requests_per_sec`）表示 DMV 所报告的服务端滚动平均值，而不是由驱动程序计算出的增量。

- 最低支持的 SQL Server 版本：2016（13.0）。驱动程序使用 `DROP INDEX IF EXISTS … ON …` 语法，更旧的服务器会以语法错误（102）拒绝它。Azure SQL Database 与托管实例没有问题。
- 不支持对带有 `INSTEAD OF` 触发器的表（或可更新视图）做 CRUD。驱动程序通过不带 `INTO` 子句的 `OUTPUT INSERTED.*` / `OUTPUT DELETED.*` 返回变更后的行，而 SQL Server 会以错误 334 拒绝这种做法（「若语句包含不带 INTO 的 OUTPUT 子句，则目标表不能有任何已启用的触发器」）。该错误以 `ConstraintViolation` 呈现。
- 仅支持 SQL 的驱动程序；不提供文档型或键值 API。
- 取消会杀掉底层会话并透明地重连；它不是 SSMS 所用的那种精确的 TDS-Attention 取消（tiberius 目前未暴露该原语）。实践中唯一可见的差别是：任何会话局部状态（`SET` 选项、临时表、未提交的事务）都会被取消操作重置，当前数据库则会自动恢复。
- 取消的延迟取决于 SQL Server 的调度器：CPU 密集型的查询通常为几毫秒，处于锁等待中的查询则是立即生效。长时间的回滚（例如在事务中途取消一个大型 `DELETE`）可能让*服务端*的 SPID 在驱动程序已经切到新会话后，仍在 KILLED/ROLLBACK 状态停留一段时间。
- 不使用参数绑定 —— 语句通过 `simple_query` 下发。CRUD 辅助函数通过共享的 `SqlQueryBuilder` 与方言字面量格式化器把取值组合进 SQL 文本。较大的二进制或 Unicode 负载会以 `0x…` 或 `N'…'` 字面量内联。
- 有界执行是保留上限，而不是服务端限制：语句仍会在服务器上运行至完成，超过上限的所有行都会被接收后丢弃，并且不施加任何服务器工作量、时间或字节预算。该上限作用于保留的行，而不是传输的总字节数，单个超大行仍会被完整保留。行数上限下的变更操作会完成其全部效果。
- 行数上限适用于普通 SQL 执行，不适用于每一个 execute 上下文：针对驱动程序实例指标或检查器的带上限请求会在连接锁或任何分发之前被拒绝 —— 这些内部目录查询没有应用上限的位置 —— 而不带上限的请求对它们仍然可用。
- 请求的语句超时（`QueryRequest::statement_timeout`）不受支持，并在执行前被拒绝：驱动程序没有实现自己的截止时间机制 —— 没有 watchdog，没有基于 `KILL` 的截止时间，也没有会话级 `SET`。不带上限的普通查询仍可通过现有的 `KILL` 路径取消。
- 有界执行与拒绝行为仅针对 `mcr.microsoft.com/mssql/server:2022-latest` 容器镜像（SQL Server 2022）做过实机测试；未针对 Azure SQL Database 或托管实例进行测试。
- 流式处理：结果集被物化为 `Vec<Row>`。`Connection::execute` trait 返回的是一个已完全解析的 `QueryResult`，因此游标式的流式传输需要工作区层面的 API 改动，而不是仅靠驱动程序就能解决。
- 多语句批处理会通过 `QueryResult.additional_results` 呈现每一个非空结果集，但界面目前只渲染主（最后一个）结果集。在结果标签页系统读取 `additional_results` 之前，靠前的结果集虽被驱动程序捕获，在编辑器中却不可见。
- 批处理过程中发出的 `PRINT` 与信息性消息会被丢弃。要呈现它们，需要驱动 tiberius 更底层的 `TokenStream`，而不是 `QueryStream::into_results()`。
- 有意不生成 `UPSERT`。需要时请手写 `MERGE`。
- 直连时支持命名实例（tiberius 会在 UDP 1434 上查询 SQL Browser 服务），但经 SSH 隧道时不支持，因为 libssh2 只转发 TCP。标准变通做法是为该实例分配一个静态 TCP 端口，并直接连到该端口。
- Schema 探查使用 `sys.*` 目录视图；不具备默认 `VIEW DEFINITION` 权限的用户会看到不完整的元数据。此处适用 SQL Server 的元数据可见性规则。
- CLR 例程（CLR 标量函数、CLR 表值函数、CLR 存储过程）以及任何以 `ENCRYPTION` 创建的例程，`OBJECT_DEFINITION` 都会返回 `NULL`；这种情况下驱动程序呈现一条简短的回退提示，而不是错误。
- 例程的 `parameter_types` 不会被填充；本实现不查询 `sys.parameters`。
- SQL Server 在 `sys.objects.type` 分类中没有 `Window` 函数类型；该驱动程序从不产生 `RoutineKind::Window`。
- 数据传输引擎的迁移路径没有参照完整性开关（未设置 `DriverCapabilities::DISABLE_FK_CHECKS`；`Connection::set_referential_integrity` 返回 `NotSupported`）。SQL Server 是通过 `ALTER TABLE ... NOCHECK CONSTRAINT` 按表禁用外键检查的，这与引擎的单一全局开关不匹配；按表的变体可作为未来的扩展方向。
