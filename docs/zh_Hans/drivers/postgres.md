# PostgreSQL

功能强大的开源关系型数据库。

## 速览

- **类别** —— 关系型
- **查询语言** —— SQL
- **默认端口** —— 5432
- **URI 方案** —— `postgresql`

## 功能

- PostgreSQL 关系型驱动程序，支持 SQL 查询执行与 Schema 发现。
- 支持 schema、表、视图、索引、外键、CHECK 约束、唯一约束与自定义类型。
- 在 Schema 树中呈现存储例程（函数、存储过程、聚合函数、窗口函数），并提供只读的定义查看器。
- 支持身份认证、SSL、SSH 隧道，以及 URI/手动两种连接模式。
- 通过 PostgreSQL 的取消令牌支持查询取消。
- 包含 PostgreSQL 专属的 SQL/代码生成，面向 CRUD、索引、重建索引（reindex）、外键与类型操作。
- 多语句脚本（若干以 `;` 分隔的语句）通过简单查询协议作为一个批次执行，每个语句返回一个结果集。
- 数据传输引擎：原生的多行 `INSERT` 批量装载（`BULK_INSERT`）、依据源表列生成的驱动程序原生 `CREATE TABLE` DDL、`TRUNCATE TABLE` 支持，以及用于外键安全迁移的参照完整性开关（`SET session_replication_role`）。
- 以文本形式展示 `pgvector` 的 `vector`、`halfvec` 与 `sparsevec` 取值，包括经验证的一维数组。
- 以 PostgreSQL 的规范文本形式展示全文检索的 `tsvector` 与 `tsquery` 取值，包括一维数组。
- 把客户端身份以 `application_name=dbflux/<version>` 上报给服务器，除非连接串本身已设置 `application_name` —— 此时保留用户提供的值。
- 连接后探测写入权限：副本或只读事务模式无论授权如何都判定为只读；否则由已认证角色在可见基表上的 `INSERT`/`UPDATE`/`DELETE` 权限决定；空数据库或探测失败属于无结论，配置自身的变更策略保持不变。

### 实例指标

基于 PostgreSQL 系统视图精选的一组实时服务器指标：

- `pg.tps` —— 每秒事务数（来自 `pg_stat_database`）
- `pg.cache_hit_ratio` —— 缓冲区缓存命中率（来自 `pg_statio_user_tables`）
- `pg.active_connections` —— 处于 `'active'` 状态的连接数
- `pg.idle_connections` —— 处于 `'idle'` 状态的连接数
- `pg.blocks_read` —— 从磁盘读取的块数（来自 `pg_statio_user_tables`）
- `pg.stat_statements.mean_exec_ms` —— 每条查询的平均执行时间（需要 `pg_stat_statements` 扩展）

每个指标以单行 `(timestamp_ms, value)` 返回，用于实时绘图。

### 实例检查器

以表格形式呈现运行中服务器状态的快照：

- `pg.activity` —— 来自 `pg_stat_activity` 的当前会话（查询文本、状态、等待事件、持续时间）
- `pg.locks` —— 来自 `pg_locks` 并与 `pg_class` 连接后的活动锁

- 单语句行数限制只保留请求的行数，并报告是否有行被省略；执行仍会运行至完成。
- 带行数限制的多语句批次及请求的语句超时会在执行前被拒绝。

## 限制

- 行数限制只限制保留的数据，不限制服务器工作量、网络流量或执行时间；变更操作仍会完成所有效果。
- 实例指标和检查器不支持行数限制，请求会在分派前被拒绝。无限制批次保留原有行为。

- 批量（多语句）结果的列不携带类型元数据；取值以文本返回，并且对它们禁用图表自动检测。若需要完整带类型的列，请单独运行一条语句。

- `pg.stat_statements.mean_exec_ms` 只在安装并加载了 `pg_stat_statements` 扩展时可用。驱动程序会在构建目录时探测其是否存在；不存在时该指标会从 `list_metrics()` 中省略。

- 实例指标每次调用只返回一个数据点（当前快照），而非历史时序。界面会按配置的刷新间隔轮询，以构建实时图表。

- 仅支持 SQL 的驱动程序；不提供文档型或键值 API。
- 聚合函数与窗口函数的例程定义是由目录元数据合成的，因为 `pg_get_functiondef` 不支持它们。
- 不支持例程的编辑与执行；例程查看器是只读的。
- 取消是尽力而为的，取决于取消当时的服务器/会话状态。
- 代码生成只针对受支持的 PostgreSQL 结构；不受支持的生成器 ID 会返回 `NotSupported`。

## DDL 能力

### 事务性 DDL

PostgreSQL 支持**事务性 DDL** —— 所有 DDL 操作（除 `CREATE INDEX CONCURRENTLY` 外）都可以包在事务中并回滚：

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL;
-- 验证这项改动
ROLLBACK;  -- 出现问题时可以安全回滚
```

**例外**：`CREATE INDEX CONCURRENTLY` 与 `DROP INDEX CONCURRENTLY` 不能在事务内运行。

### ALTER TABLE 的行为

**带默认值添加列（PostgreSQL 11+）**：
- 快（仅涉及元数据的操作）
- 无需重写表
- 不会为读/写锁表

**不带默认值添加列**：
- 快（不重写）
- 已存在的行在该列上取 `NULL`

**修改列类型**：
- 可能需要重写表（锁表）
- 可用 `USING` 子句做自定义转换：`ALTER COLUMN age TYPE integer USING age::integer`

**删除列**：
- 快（把列标记为已删除，不重写）
- 数据不会立即回收（需要时使用 `VACUUM FULL`）

**重命名列**：
- 快（仅涉及元数据）
- 可能破坏视图、触发器与应用代码

### 索引操作

**CREATE INDEX**：
- 对写操作锁表（允许读）
- 使用 `CONCURRENTLY` 实现零停机创建索引：
  ```sql
  CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
  ```

**DROP INDEX**：
- 对写操作锁表（允许读）
- 使用 `CONCURRENTLY` 实现零停机删除索引：
  ```sql
  DROP INDEX CONCURRENTLY idx_users_email;
  ```

**REINDEX**：
- 对读与写都锁表
- 使用 `CONCURRENTLY`（PostgreSQL 12+）实现零停机重建索引

### 约束

**添加约束**：
- `CHECK` 与 `UNIQUE` 约束会扫描表（在大表上可能耗时）
- 用 `NOT VALID` 推迟校验：
  ```sql
  ALTER TABLE users ADD CONSTRAINT age_check CHECK (age >= 0) NOT VALID;
  -- 之后在不锁表的情况下校验：
  ALTER TABLE users VALIDATE CONSTRAINT age_check;
  ```

**外键**：
- 添加外键会扫描两张表
- 用 `NOT VALID` + `VALIDATE CONSTRAINT` 实现零停机创建外键

### 自定义类型

**CREATE TYPE（枚举）**：
- 快（仅涉及元数据）
- 用 `ALTER TYPE ... ADD VALUE` 增加枚举值：
  ```sql
  ALTER TYPE status_enum ADD VALUE 'archived';
  ```
  **注意**：该操作在事务内无法回滚（会立即提交）

**DROP TYPE**：
- 若该类型正被表使用则会失败
- 必须先删除依赖它的列

### 已知限制

- `CREATE INDEX CONCURRENTLY` 会短暂获取排他锁（在高流量表上可能阻塞）
- `ALTER TYPE ADD VALUE` 无法回滚
- 删除列不会立即回收磁盘空间（需要 `VACUUM FULL`）
