# TinyDB

一个轻量级 C/S 架构关系型数据库系统，采用 C++17 开发。

## 特性

- **SQL 支持**: 完整的 SQL 解析和执行
- **ACID 事务**: WAL 日志保证数据一致性
- **B+树索引**: 高性能索引加速查询
- **查询优化**: 基于代价的查询优化器
- **权限系统**: 用户、角色和细粒度权限控制
- **备份恢复**: 全量/增量备份，支持 PITR
- **主从复制**: WAL 流复制架构
- **监控诊断**: tn_stat_* 风格系统视图
- **命令行工具**: psql-like 交互客户端

## 快速开始

### 编译

```bash
mkdir build && cd build
cmake ..
make -j4
```

### 启动服务器

```bash
./bin/tinydb-server -p 5432
```

### 使用命令行客户端

```bash
# 交互模式
./bin/tinydb-cli -h localhost -p 5432

# 执行单条 SQL
./bin/tinydb-cli -c "SELECT * FROM users"

# 执行 SQL 脚本
./bin/tinydb-cli -f script.sql
```

### 示例 SQL

```sql
-- 创建表
CREATE TABLE users (id INT, name VARCHAR(32), age INT);

-- 插入数据
INSERT INTO users VALUES (1, 'Alice', 25);
INSERT INTO users VALUES (2, 'Bob', 30);

-- 创建索引
CREATE INDEX idx_users_id ON users(id);

-- 查询
SELECT * FROM users WHERE age > 25;

-- 更新
UPDATE users SET age = 26 WHERE id = 1;

-- 删除
DELETE FROM users WHERE id = 2;

-- 事务
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 35);
COMMIT;
```

## 系统视图

```sql
-- 查看当前活动
SELECT * FROM tn_stat_activity;

-- 查看表统计
SELECT * FROM tn_stat_user_tables;

-- 查看索引使用
SELECT * FROM tn_stat_user_indexes;

-- 查看锁信息
SELECT * FROM tn_locks;

-- 查看复制状态
SELECT * FROM tn_stat_replication;

-- 查看备份列表
SELECT * FROM tn_backup_list;
```

## 文档

- [Feature.md](Feature.md) - 5阶段开发计划
- [Phase3_Summary.md](Phase3_Summary.md) - 阶段三总结
- [Phase4_Summary.md](Phase4_Summary.md) - 阶段四总结
- [Phase5_Summary.md](Phase5_Summary.md) - 阶段五总结
- [docs/design.md](docs/design.md) - 架构设计文档
- [docs/protocol.md](docs/protocol.md) - 通信协议文档
- [docs/storage_design.md](docs/storage_design.md) - 存储引擎设计

## 目录结构

```
├── bin/                   # 可执行文件
│   ├── tinydb-server      # 数据库服务器
│   └── tinydb-cli         # 交互式客户端
├── src/                   # 源代码
│   ├── common/            # 公共组件
│   ├── network/           # 网络层
│   ├── sql/               # SQL解析
│   ├── storage/           # 存储引擎
│   ├── engine/            # 执行引擎
│   └── server/            # 服务器
├── client/                # 客户端 SDK
├── tests/                 # 测试代码
└── docs/                  # 文档
```

## 测试

```bash
# 运行所有测试
ctest

# 运行特定测试
./tests/test_phase5_integration
./tests/test_btree_index
./tests/test_statistics
```

## 技术栈

- **语言**: C++17
- **构建**: CMake 3.14+
- **测试**: Google Test
- **协议**: 自定义二进制协议
- **端口**: 5432

## 许可证

MIT License

## 致谢

设计参考 PostgreSQL 和 SQLite。
