# MySQL 与 MariaDB

广受欢迎的开源关系型数据库。

## 速览

- **类别** —— 关系型
- **查询语言** —— SQL
- **默认端口** —— 3306
- **URI 方案** —— `mysql`

## 功能

- 在同一个 crate 中提供 MySQL 与 MariaDB 的关系型驱动程序实现。
- 支持 SQL 执行、Schema 发现、索引、外键、CHECK 约束与唯一约束。
- 支持身份认证、SSH 隧道，以及 URI/手动两种连接模式。
- TLS 支持五种原生 SSL 模式（`DISABLED`、`PREFERRED`、`REQUIRED`、`VERIFY_CA`、`VERIFY_IDENTITY`）：`VERIFY_CA` 会校验证书链但跳过主机名校验，`VERIFY_IDENTITY` 则两者都校验。自定义根 CA 会在校验模式下取代系统信任库；客户端证书 + 私钥可启用双向 TLS。使用 `rustls`/`aws-lc-rs` 后端。
- 通过专用的取消路径（`KILL QUERY` 流程）支持查询取消。
- 包含面向 CRUD、索引、外键与表 DDL 操作的 SQL/代码生成。
- 例程发现：从 `information_schema.ROUTINES` 列出存储过程与用户自定义函数，包含参数类型与返回类型提示（仅函数）。
- 例程定义：通过 `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE` 获取完整的 `CREATE FUNCTION` 或 `CREATE PROCEDURE` 主体（只读；在查看器中定义不可编辑，也不可执行）。
- 多语句脚本（若干以 `;` 分隔的语句）会被切分并逐条执行，每条都走预编译路径，每个语句返回一个结果集。
- 数据传输引擎：原生的多行 `INSERT` 批量装载（`BULK_INSERT`）、依据源表列生成的驱动程序原生 `CREATE TABLE` DDL、`TRUNCATE TABLE` 支持，以及用于外键安全迁移的参照完整性开关（`SET FOREIGN_KEY_CHECKS`）。MySQL 与 MariaDB 共享这套能力。
- 以 `dbflux/<version>` 发送 `program_name` 连接属性，可在 `performance_schema.session_connect_attrs` 中看到。
- 写入权限探测：连接后，会检查 `@@read_only`/`@@super_read_only` 以及 `SHOW GRANTS` 中当前用户，以识别只读副本，或缺少 `INSERT`/`UPDATE`/`DELETE` 授权的角色；当服务器本来就会拒绝写入时，会把解析出的变更策略收紧为只读（无副作用；MariaDB 没有 `@@super_read_only`，此时回退为只检查 `@@read_only`）。

### 实例指标

从 `SHOW GLOBAL STATUS` 精选的一组实时服务器指标：

- `mysql.threads_connected` —— 当前打开的连接数
- `mysql.threads_running` —— 当前正在执行的查询数
- `mysql.queries_per_sec` —— 每秒查询数（累计计数器）
- `mysql.innodb_buffer_pool_hit_ratio` —— InnoDB 缓冲池读取效率
- `mysql.innodb_rows_read` —— 从 InnoDB 存储引擎读取的行数
- `mysql.innodb_rows_inserted` —— 插入 InnoDB 的行数
- `mysql.innodb_rows_updated` —— InnoDB 中更新的行数
- `mysql.innodb_rows_deleted` —— InnoDB 中删除的行数
- `mysql.slow_queries` —— 慢查询的累计条数
- `mysql.table_locks_waited` —— 表级锁争用计数器
- `mysql.bytes_sent` —— 已发送的网络字节数

每个指标以单行 `(timestamp_ms, value)` 返回，用于实时绘图。

### 实例检查器

以表格形式呈现运行中服务器状态的快照：

- `mysql.processlist` —— 来自 `information_schema.PROCESSLIST` 的活动会话（用户、主机、数据库、命令、时间、状态、信息）

## 限制

- 仅支持 SQL 的驱动程序；不提供文档型或键值 API。

- 实例指标每次调用只返回一个数据点（`SHOW GLOBAL STATUS` 的当前快照），而非历史时序。累计计数器（例如 `mysql.bytes_sent`）单调递增 —— 应将其理解为两次采样之间的增量，而不是绝对速率。

- `performance_schema` 的可用性探测只在构建目录时执行一次。当 `performance_schema` 缺失时，依赖于它的指标会从 `list_metrics()` 中省略。基于 `SHOW GLOBAL STATUS` 的静态指标集始终可用。

- 多语句脚本按顺序逐条执行，而不是作为一个服务端的原子批次；语句切分基于文本，可能会把内嵌 `;` 的存储程序主体（例如 `CREATE PROCEDURE ... BEGIN ... END`）切错。
- 取消取决于发出 `KILL QUERY` 时的服务器权限与连接状态。
- 代码生成只覆盖受支持的 MySQL/MariaDB 结构；不受支持的生成器 ID 会返回 `NotSupported`。
- 例程列表仅涵盖 FUNCTION 与 PROCEDURE 类型。MySQL 的聚合函数（通过 `CREATE AGGREGATE FUNCTION` UDF 插件注册）与窗口函数不会出现在 `information_schema.ROUTINES` 中，因此也不会被列出。
- `SHOW CREATE FUNCTION`/`SHOW CREATE PROCEDURE` 需要 `SHOW_ROUTINE` 权限（MySQL 8.0+）或该例程的所有权；权限不足时，定义列会返回 `NULL`，查看器会显示一条提示，而不是源码。

## DDL 能力

### 非事务性 DDL

**重要**：MySQL 的 DDL 操作**不是事务性的** —— 它们无法回滚：

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- DDL 会立即提交，ROLLBACK 不起作用！
ROLLBACK;  -- 为时已晚，列已经加上了
```

**例外**：`RENAME TABLE` 是原子的（可在事务中安全使用）。

### ALTER TABLE 的行为

**表重写**：
- 大多数 `ALTER TABLE` 操作会重写整张表（在此期间锁表）
- 使用 `ALGORITHM=INPLACE` 与 `LOCK=NONE` 进行在线 DDL（MySQL 5.6+）：
  ```sql
  ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL, ALGORITHM=INPLACE, LOCK=NONE;
  ```

**添加列**：
- 在**表末尾**添加列：快（仅涉及元数据）
- 在**表中间**添加列：表重写（锁表）
- 用 `AFTER column_name` 控制位置

**带默认值添加列**：
- 表重写（锁表）
- 默认值会写入所有已存在的行

**修改列类型**：
- 始终需要表重写（锁表）
- 数据转换在重写过程中完成

**删除列**：
- 表重写（锁表）
- 数据会立即删除

**重命名列**：
- 表重写（锁表）
- 可能破坏视图、触发器与应用代码

### 索引操作

**CREATE INDEX**：
- 对写操作锁表（允许读）
- 使用 `ALGORITHM=INPLACE, LOCK=NONE` 进行在线创建索引：
  ```sql
  CREATE INDEX idx_users_email ON users(email) ALGORITHM=INPLACE, LOCK=NONE;
  ```

**DROP INDEX**：
- 对写操作锁表（允许读）
- 使用 `ALGORITHM=INPLACE, LOCK=NONE` 进行在线删除索引

### 约束

**外键**：
- 添加外键会扫描两张表（两张表都锁）
- 尽可能使用 `ALGORITHM=INPLACE, LOCK=NONE`

**UNIQUE 约束**：
- 需要创建索引（锁表）

**CHECK 约束**（MySQL 8.0.16+）：
- 仅涉及元数据（快）
- 只在 INSERT/UPDATE 时校验

### 在线 DDL（MySQL 5.6+）

**ALGORITHM 选项**：
- `INPLACE` —— 就地修改表（不复制数据）
- `COPY` —— 创建新表并复制数据行（旧版 MySQL 的默认方式）
- `INSTANT` —— 仅涉及元数据（MySQL 8.0+，适用的操作有限）

**LOCK 选项**：
- `NONE` —— 允许并发读写
- `SHARED` —— 允许读，阻塞写
- `EXCLUSIVE` —— 阻塞读与写

**示例**：
```sql
ALTER TABLE users 
  ADD COLUMN phone VARCHAR(20) NULL,
  ALGORITHM=INPLACE,
  LOCK=NONE;
```

### 已知限制

- DDL 非事务性（无法回滚）
- 大多数 `ALTER TABLE` 操作会重写整张表（锁表）
- 在表中间添加列需要重写整张表
- 在线 DDL 的支持程度因 MySQL 版本而异
- 对大型表做零停机 DDL，请使用 `pt-online-schema-change`（Percona Toolkit）

### 最佳实践

1. **先在副本上测试** —— DDL 无法回滚
2. **使用在线 DDL** —— 在受支持的情况下加上 `ALGORITHM=INPLACE, LOCK=NONE`
3. **安排维护窗口** —— 在低峰时段执行 DDL
4. **关注表的大小** —— 大表重写耗时更长
5. **使用 pt-online-schema-change** —— 用于生产环境表的零停机 DDL
