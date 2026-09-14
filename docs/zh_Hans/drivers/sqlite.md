# SQLite

嵌入式、基于文件的数据库。

## 速览

- **类别** —— 关系型
- **查询语言** —— SQL
- **URI 方案** —— `sqlite`

## 功能

- 嵌入式 SQLite 关系型驱动程序，使用基于文件的数据库路径。
- 支持 SQL 执行、Schema 发现、视图、索引、外键、CHECK 约束与唯一约束。
- 通过 SQLite 的中断句柄支持查询取消。
- 包含面向 CRUD、索引、重建索引（reindex）、创建表与删除表的 SQL/代码生成。
- 多语句脚本（若干以 `;` 分隔的语句）会被切分并逐条执行，每条都走预编译路径，每个语句返回一个结果集。（`rusqlite::prepare` 只会解析字符串中的第一条语句，因此脚本必须先切分。）
- 数据传输引擎：原生的多行 `INSERT` 批量装载（`BULK_INSERT`）、依据源表列生成的驱动程序原生 `CREATE TABLE` DDL，以及用于外键安全迁移的按连接参照完整性开关（`PRAGMA foreign_keys`）。

## 限制

- 仅支持本地文件；没有网络传输、SSH 隧道，也没有 TLS/SSL 模式。
- 仅支持 SQL 的驱动程序；不提供文档型或键值 API。
- SQLite 的 Schema 模型没有服务端的多 schema 命名空间与之对应。
- 没有 `TRUNCATE TABLE` 语句；数据传输引擎的 Truncate 装载选项对 SQLite 目标不可用（未设置 `DriverCapabilities::TRUNCATE_TABLE`）。

## DDL 能力

### 事务性 DDL

SQLite 支持**事务性 DDL** —— 所有 DDL 操作都可以包在事务中并回滚：

```sql
BEGIN;
ALTER TABLE users ADD COLUMN phone TEXT NULL;
-- 验证这项改动
ROLLBACK;  -- 出现问题时可以安全回滚
```

### ALTER TABLE 的限制

**重要**：SQLite 的 `ALTER TABLE` 支持**非常有限**：

**支持的操作**：
- `ADD COLUMN`（只能在表末尾）
- `RENAME COLUMN`（SQLite 3.25.0+）
- `RENAME TABLE`

**不支持的操作**：
- `DROP COLUMN`（需要重建表）
- `ALTER COLUMN`（修改类型需要重建表）
- 在表中间 `ADD COLUMN`（需要重建表）

### 重建表模式

对于不受支持的 `ALTER TABLE` 操作，请使用重建表模式：

```sql
BEGIN;

-- 1. 按期望的 Schema 创建新表
CREATE TABLE users_new (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  name TEXT,
  -- phone 列已删除，age 列已新增
  age INTEGER
);

-- 2. 从旧表复制数据
INSERT INTO users_new (id, email, name, age)
  SELECT id, email, name, NULL FROM users;

-- 3. 删除旧表
DROP TABLE users;

-- 4. 重命名新表
ALTER TABLE users_new RENAME TO users;

COMMIT;
```

**重要**：这种模式会丢失：
- 其他表指向它的外键引用
- 原表上的触发器
- 原表上的索引（必须重建）

### 索引操作

**CREATE INDEX**：
- 在此期间锁住数据库（阻塞写入）
- 没有并发选项（与 PostgreSQL 不同）

**DROP INDEX**：
- 快（仅涉及元数据）

**REINDEX**：
- 重建索引（锁住数据库）

### 约束

**添加约束**：
- SQLite 在 `INSERT`/`UPDATE` 时校验约束
- 无法为已有的表添加约束（需要重建表）

**外键**：
- 默认关闭（必须用 `PRAGMA foreign_keys = ON` 启用）
- 无法添加到已有的表（需要重建表）

### 已知限制

- 没有 `DROP COLUMN`（需要重建表）
- 没有 `ALTER COLUMN`（需要重建表）
- 无法为已有的表添加约束
- 不能并发创建索引（会锁住数据库）
- 动态类型（列类型只是建议性的）

### 最佳实践

1. **使用事务** —— DDL 是事务性的，始终用 `BEGIN`/`COMMIT` 包起来
2. **提前规划 Schema** —— 事后修改很困难
3. **使用重建表模式** —— 用于不受支持的 `ALTER TABLE` 操作
4. **重建索引与触发器** —— 在重建表之后
5. **先在副本上测试** —— 尤其是重建表模式
6. **启用外键** —— 修改 Schema 之前先执行 `PRAGMA foreign_keys = ON`
7. **使用 VACUUM** —— 在 `DROP TABLE` 或重建表之后回收磁盘空间
