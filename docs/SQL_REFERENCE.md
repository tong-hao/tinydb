# TinyDB SQL 语法参考手册

## 目录

1. [数据类型](#数据类型)
2. [DDL - 数据定义语言](#ddl---数据定义语言)
3. [DML - 数据操作语言](#dml---数据操作语言)
4. [事务控制](#事务控制)
5. [索引操作](#索引操作)
6. [查询工具](#查询工具)
7. [系统视图](#系统视图)
8. [表达式与运算符](#表达式与运算符)

---

## 数据类型

### 数值类型

| 类型 | 说明 | 存储大小 |
|------|------|----------|
| `INT` / `INTEGER` | 整数类型 | 4字节 |
| `BIGINT` | 大整数类型 | 8字节 |
| `SMALLINT` | 小整数类型 | 2字节 |
| `TINYINT` | 微整数类型 | 1字节 |
| `FLOAT` | 单精度浮点数 | 4字节 |
| `DOUBLE` | 双精度浮点数 | 8字节 |
| `REAL` | 浮点数别名 | 4字节 |
| `DECIMAL` | 精确小数 | 变长 |
| `NUMERIC` | 精确小数别名 | 变长 |

### 字符串类型

| 类型 | 说明 | 示例 |
|------|------|------|
| `CHAR(n)` | 定长字符串 | `CHAR(10)` |
| `VARCHAR(n)` | 变长字符串 | `VARCHAR(255)` |
| `TEXT` | 长文本类型 | `TEXT` |

### 日期时间类型

| 类型 | 说明 |
|------|------|
| `DATE` | 日期 |
| `TIME` | 时间 |
| `TIMESTAMP` | 时间戳 |
| `DATETIME` | 日期时间 |

### 布尔类型

| 类型 | 说明 |
|------|------|
| `BOOLEAN` | 布尔值 (TRUE/FALSE) |

---

## DDL - 数据定义语言

### CREATE TABLE - 创建表

```sql
CREATE TABLE table_name (
    column1 data_type,
    column2 data_type,
    ...
);
```

**示例：**
```sql
-- 创建用户表
CREATE TABLE users (
    id INT,
    name VARCHAR(32),
    age INT,
    email VARCHAR(100),
    created_at TIMESTAMP
);

-- 创建订单表
CREATE TABLE orders (
    id INT,
    user_id INT,
    amount DECIMAL,
    status VARCHAR(20)
);
```

### DROP TABLE - 删除表

```sql
-- 删除表
DROP TABLE table_name;

-- 如果存在则删除（安全删除）
DROP TABLE IF EXISTS table_name;
```

**示例：**
```sql
DROP TABLE users;
DROP TABLE IF EXISTS orders;
```

### ALTER TABLE - 修改表结构

#### 添加列
```sql
ALTER TABLE table_name ADD COLUMN column_name data_type;
-- 或简写
ALTER TABLE table_name ADD column_name data_type;
```

#### 删除列
```sql
ALTER TABLE table_name DROP COLUMN column_name;
-- 或简写
ALTER TABLE table_name DROP column_name;
```

#### 修改列
```sql
ALTER TABLE table_name MODIFY COLUMN column_name new_data_type;
-- 或简写
ALTER TABLE table_name MODIFY column_name new_data_type;
```

#### 重命名表
```sql
ALTER TABLE table_name RENAME TO new_table_name;
-- 或简写
ALTER TABLE table_name RENAME new_table_name;
```

**示例：**
```sql
-- 添加列
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- 删除列
ALTER TABLE users DROP COLUMN phone;

-- 修改列类型
ALTER TABLE users MODIFY COLUMN age BIGINT;

-- 重命名表
ALTER TABLE users RENAME TO customers;
```

---

## DML - 数据操作语言

### SELECT - 查询数据

```sql
-- 查询所有列
SELECT * FROM table_name;

-- 查询指定列
SELECT column1, column2 FROM table_name;

-- 带WHERE条件的查询
SELECT * FROM table_name WHERE condition;

-- 带表达式的查询
SELECT column1, column2 + 10 FROM table_name;
```

**示例：**
```sql
-- 查询所有用户
SELECT * FROM users;

-- 查询指定列
SELECT id, name FROM users;

-- 条件查询
SELECT * FROM users WHERE age > 25;

-- 多条件查询（AND/OR）
SELECT * FROM users WHERE age > 25 AND name = 'Alice';
SELECT * FROM users WHERE age < 20 OR age > 60;

-- 使用NOT
SELECT * FROM users WHERE NOT age = 25;

-- 算术表达式
SELECT id, age * 2 AS double_age FROM users;
```

### INSERT - 插入数据

```sql
-- 插入完整行
INSERT INTO table_name VALUES (value1, value2, ...);

-- 插入指定列
INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);
```

**示例：**
```sql
-- 插入完整行
INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com');

-- 插入指定列
INSERT INTO users (id, name) VALUES (2, 'Bob');

-- 插入多行（多次执行）
INSERT INTO users VALUES (3, 'Charlie', 30, 'charlie@example.com');
INSERT INTO users VALUES (4, 'David', 35, 'david@example.com');
```

### UPDATE - 更新数据

```sql
UPDATE table_name SET column1 = value1, column2 = value2, ... [WHERE condition];
```

**示例：**
```sql
-- 更新所有行
UPDATE users SET age = 26;

-- 条件更新
UPDATE users SET age = 26 WHERE id = 1;

-- 多列更新
UPDATE users SET name = 'Alice Smith', age = 27 WHERE id = 1;

-- 使用表达式更新
UPDATE users SET age = age + 1 WHERE id = 1;
```

### DELETE - 删除数据

```sql
DELETE FROM table_name [WHERE condition];
-- 或简写
DELETE table_name [WHERE condition];
```

**示例：**
```sql
-- 删除所有数据
DELETE FROM users;

-- 条件删除
DELETE FROM users WHERE id = 2;

-- 删除多行
DELETE FROM users WHERE age < 18;
```

---

## 事务控制

### BEGIN - 开始事务

```sql
BEGIN;
-- 或
BEGIN TRANSACTION;
```

### COMMIT - 提交事务

```sql
COMMIT;
-- 或
COMMIT TRANSACTION;
```

### ROLLBACK - 回滚事务

```sql
ROLLBACK;
-- 或
ROLLBACK TRANSACTION;
```

**示例：**
```sql
-- 事务示例
BEGIN;
INSERT INTO accounts (id, balance) VALUES (1, 1000);
INSERT INTO accounts (id, balance) VALUES (2, 2000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- 回滚示例
BEGIN;
INSERT INTO users VALUES (100, 'Test', 99);
-- 发现错误，回滚
ROLLBACK;
```

---

## 索引操作

### CREATE INDEX - 创建索引

```sql
-- 创建普通索引
CREATE INDEX index_name ON table_name (column_name);

-- 创建唯一索引
CREATE UNIQUE INDEX index_name ON table_name (column_name);
```

**示例：**
```sql
-- 创建普通索引
CREATE INDEX idx_users_name ON users(name);

-- 创建唯一索引
CREATE UNIQUE INDEX idx_users_email ON users(email);
```

### DROP INDEX - 删除索引

```sql
DROP INDEX index_name;
```

**示例：**
```sql
DROP INDEX idx_users_name;
```

---

## 查询工具

### ANALYZE - 收集统计信息

```sql
ANALYZE table_name;
```

**示例：**
```sql
-- 收集表的统计信息
ANALYZE users;
ANALYZE orders;
```

### EXPLAIN - 查看执行计划

```sql
EXPLAIN statement;
```

**示例：**
```sql
-- 查看SELECT的执行计划
EXPLAIN SELECT * FROM users WHERE id = 1;

-- 查看INSERT的执行计划
EXPLAIN INSERT INTO users VALUES (1, 'Test', 25);
```

---

## 系统视图

TinyDB 提供类似 PostgreSQL 的系统视图，用于监控和诊断。

### tn_stat_activity - 当前活动

显示当前数据库会话活动信息。

```sql
SELECT * FROM tn_stat_activity;
```

**列说明：**
- `pid` - 进程ID
- `username` - 用户名
- `database` - 数据库名
- `state` - 会话状态
- `query` - 当前执行的查询
- `query_start` - 查询开始时间
- `client_addr` - 客户端地址

### tn_stat_database - 数据库统计

显示数据库级别的统计信息。

```sql
SELECT * FROM tn_stat_database;
```

### tn_stat_user_tables - 表统计

显示用户表的统计信息。

```sql
SELECT * FROM tn_stat_user_tables;
```

**列说明：**
- `relid` - 表ID
- `relname` - 表名
- `seq_scan` - 顺序扫描次数
- `idx_scan` - 索引扫描次数
- `n_tup_ins` - 插入行数
- `n_tup_upd` - 更新行数
- `n_tup_del` - 删除行数
- `n_live_tup` - 活跃行数
- `n_dead_tup` - 死行数

### tn_stat_user_indexes - 索引统计

显示用户索引的使用统计。

```sql
SELECT * FROM tn_stat_user_indexes;
```

**列说明：**
- `relid` - 表ID
- `indexrelid` - 索引ID
- `indexrelname` - 索引名
- `idx_scan` - 索引扫描次数
- `idx_tup_read` - 通过索引读取的元组数
- `idx_tup_fetch` - 通过索引获取的元组数

### tn_locks - 锁信息

显示当前锁信息。

```sql
SELECT * FROM tn_locks;
SELECT * FROM tn_locks WHERE NOT granted;  -- 查看等待的锁
```

### tn_stat_replication - 复制状态

显示主从复制状态。

```sql
SELECT * FROM tn_stat_replication;
```

**列说明：**
- `pid` - 进程ID
- `client_addr` - 从节点地址
- `state` - 复制状态
- `sent_lsn` - 已发送LSN
- `write_lsn` - 已写入LSN
- `flush_lsn` - 已刷盘LSN
- `replay_lsn` - 已重放LSN

### tn_tables - 表列表

显示所有用户表的信息。

```sql
SELECT * FROM tn_tables;
```

**列说明：**
- `schemaname` - 模式名
- `tablename` - 表名
- `tableowner` - 表所有者

### tn_indexes - 索引列表

显示所有索引信息。

```sql
SELECT * FROM tn_indexes;
```

### tn_user - 用户列表

显示数据库用户。

```sql
SELECT * FROM tn_user;
```

**列说明：**
- `username` - 用户名
- `superuser` - 是否超级用户
- `createdb` - 能否创建数据库
- `createrole` - 能否创建角色

### tn_database - 数据库列表

显示数据库列表。

```sql
SELECT * FROM tn_database;
```

### tn_backup_list - 备份列表

显示备份信息。

```sql
SELECT * FROM tn_backup_list;
```

**列说明：**
- `backup_id` - 备份ID
- `backup_name` - 备份名称
- `backup_type` - 备份类型（FULL/INCREMENTAL）
- `start_time` - 开始时间
- `end_time` - 结束时间
- `status` - 状态

### tn_restore_points - 恢复点列表

显示恢复点信息。

```sql
SELECT * FROM tn_restore_points;
```

**示例：**
```sql
-- 创建恢复点（通过函数调用）
SELECT tn_create_restore_point('before_migration');

-- 查看恢复点
SELECT * FROM tn_restore_points;
```

---

## 表达式与运算符

### 比较运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `=` | 等于 | `age = 25` |
| `<>` 或 `!=` | 不等于 | `age <> 25` |
| `<` | 小于 | `age < 25` |
| `>` | 大于 | `age > 25` |
| `<=` | 小于等于 | `age <= 25` |
| `>=` | 大于等于 | `age >= 25` |

### 算术运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `+` | 加法 | `age + 1` |
| `-` | 减法 | `age - 1` |
| `*` | 乘法 | `age * 2` |
| `/` | 除法 | `amount / 2` |
| `%` | 取模 | `age % 10` |

### 逻辑运算符

| 运算符 | 说明 | 示例 |
|--------|------|------|
| `AND` | 逻辑与 | `age > 20 AND age < 30` |
| `OR` | 逻辑或 | `age < 18 OR age > 60` |
| `NOT` | 逻辑非 | `NOT age = 25` |

### 特殊值

| 值 | 说明 | 示例 |
|----|------|------|
| `NULL` | 空值 | `column IS NULL` |
| `TRUE` | 真 | `active = TRUE` |
| `FALSE` | 假 | `active = FALSE` |

---

## 完整示例

### 示例 1：基础CRUD操作

```sql
-- 创建表
CREATE TABLE employees (
    id INT,
    name VARCHAR(50),
    department VARCHAR(30),
    salary INT,
    hire_date DATE
);

-- 插入数据
INSERT INTO employees VALUES (1, '张三', '技术部', 8000, '2020-01-15');
INSERT INTO employees VALUES (2, '李四', '销售部', 6000, '2020-03-20');
INSERT INTO employees VALUES (3, '王五', '技术部', 9000, '2019-06-10');
INSERT INTO employees VALUES (4, '赵六', '财务部', 7000, '2021-02-01');

-- 查询所有员工
SELECT * FROM employees;

-- 查询技术部员工
SELECT * FROM employees WHERE department = '技术部';

-- 查询高薪员工
SELECT * FROM employees WHERE salary > 7000;

-- 更新薪资
UPDATE employees SET salary = salary + 500 WHERE department = '技术部';

-- 删除员工
DELETE FROM employees WHERE id = 4;

-- 查看表统计
SELECT * FROM tn_stat_user_tables WHERE relname = 'employees';
```

### 示例 2：使用索引优化查询

```sql
-- 创建表
CREATE TABLE products (
    id INT,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL,
    stock INT
);

-- 创建索引
CREATE INDEX idx_products_category ON products(category);
CREATE UNIQUE INDEX idx_products_name ON products(name);

-- 查看索引
SELECT * FROM tn_indexes WHERE tablename = 'products';

-- 使用索引的查询
SELECT * FROM products WHERE category = '电子产品';

-- 查看索引使用情况
SELECT * FROM tn_stat_user_indexes WHERE indexrelname = 'idx_products_category';
```

### 示例 3：事务处理

```sql
-- 创建账户表
CREATE TABLE accounts (
    id INT,
    name VARCHAR(50),
    balance DECIMAL
);

-- 插入初始数据
INSERT INTO accounts VALUES (1, 'Alice', 1000);
INSERT INTO accounts VALUES (2, 'Bob', 2000);

-- 转账事务
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- 检查余额
SELECT * FROM accounts;

-- 失败事务回滚示例
BEGIN;
UPDATE accounts SET balance = balance - 500 WHERE id = 1;
-- 假设这里出现错误，回滚事务
ROLLBACK;
```

### 示例 4：系统视图查询

```sql
-- 查看当前活动
SELECT * FROM tn_stat_activity;

-- 查看所有表
SELECT * FROM tn_tables;

-- 查看所有索引
SELECT * FROM tn_indexes;

-- 查看表统计
SELECT relname, seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del
FROM tn_stat_user_tables;

-- 查看锁等待
SELECT * FROM tn_locks WHERE NOT granted;

-- 查看备份列表
SELECT * FROM tn_backup_list;
```

---

## 注意事项

1. **SQL语句结束符**：每条SQL语句以分号 `;` 结尾（可选）
2. **标识符**：表名、列名不区分大小写，使用双引号可强制区分大小写
3. **字符串常量**：使用单引号 `'string'` 包围
4. **注释**：使用 `--` 表示单行注释

---

*文档版本：1.0*
*最后更新：2026-03-23*
