# TinyDB 用户操作手册

## 目录

1. [简介](#简介)
2. [安装指南](#安装指南)
3. [快速开始](#快速开始)
4. [服务端操作](#服务端操作)
5. [客户端操作](#客户端操作)
6. [高级功能](#高级功能)
7. [故障排查](#故障排查)

---

## 简介

TinyDB 是一个轻量级 C/S 架构关系型数据库系统，采用 C++17 开发，支持完整的 SQL 语法、ACID 事务、B+树索引、查询优化等企业级特性。

### 主要特性

- **SQL 支持**: 完整的 SQL 解析和执行
- **ACID 事务**: WAL 日志保证数据一致性
- **B+树索引**: 高性能索引加速查询
- **查询优化**: 基于代价的查询优化器
- **权限系统**: 用户、角色和细粒度权限控制
- **备份恢复**: 全量/增量备份，支持 PITR（基于时间点的恢复）
- **主从复制**: WAL 流复制架构
- **监控诊断**: tn_stat_* 风格系统视图

---

## 安装指南

### 系统要求

- **操作系统**: Linux (Ubuntu 20.04+, CentOS 7+) / macOS 12+
- **编译器**: GCC 9+ 或 Clang 10+ (支持 C++17)
- **CMake**: 3.14 或更高版本
- **依赖库**:
  - Flex 和 Bison (SQL 解析器生成)
  - Google Test (可选，用于单元测试)
  - Readline (交互式命令行客户端)

### 安装依赖

#### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    libgtest-dev \
    flex \
    bison \
    libreadline-dev
```

#### CentOS/RHEL

```bash
sudo yum install -y \
    gcc-c++ \
    cmake3 \
    gtest-devel \
    flex \
    bison \
    readline-devel
```

#### macOS

```bash
brew install cmake
brew install googletest
brew install flex
brew install bison
brew install readline
```

### 编译安装

#### 1. 克隆仓库

```bash
git clone <repository-url>
cd tinydb
```

#### 2. 创建编译目录

```bash
mkdir build
cd build
```

#### 3. 配置 CMake

```bash
# Debug 模式（开发调试使用）
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Release 模式（生产环境使用）
cmake -DCMAKE_BUILD_TYPE=Release ..
```

#### 4. 编译

```bash
make -j$(nproc)
```

编译完成后，生成的可执行文件位于：
- `build/bin/tinydb-server` - 数据库服务器
- `build/bin/tinydb-cli` - 交互式命令行客户端

#### 5. 安装（可选）

```bash
sudo make install
```

默认安装路径为 `/usr/local/bin/`。

#### 6. 运行测试

```bash
# 运行所有测试
make test

# 或详细输出
ctest --output-on-failure

# 运行特定测试
./tests/test_phase5_integration
```

---

## 快速开始

### 1. 启动数据库服务器

```bash
# 默认端口 5432
./bin/tinydb-server

# 指定端口
./bin/tinydb-server -p 5432

# 查看帮助
./bin/tinydb-server --help
```

### 2. 使用交互式客户端连接

```bash
# 连接本地服务器
./bin/tinydb-cli -h localhost -p 5432

# 连接远程服务器
./bin/tinydb-cli -h 192.168.1.100 -p 5432
```

连接成功后，提示符变为 `tinydb=#`，即可输入 SQL 命令。

### 3. 执行 SQL 命令

```sql
-- 创建表
CREATE TABLE users (id INT, name VARCHAR(32), age INT);

-- 插入数据
INSERT INTO users VALUES (1, 'Alice', 25);
INSERT INTO users VALUES (2, 'Bob', 30);

-- 查询数据
SELECT * FROM users;
SELECT * FROM users WHERE age > 25;

-- 创建索引
CREATE INDEX idx_users_age ON users(age);

-- 更新数据
UPDATE users SET age = 26 WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE id = 2;

-- 删除表
DROP TABLE users;
```

### 4. 退出客户端

```sql
\q
```

或按 `Ctrl+D`。

---

## 服务端操作

### 启动参数

```
./bin/tinydb-server [选项]

选项:
  -p, --port <port>     监听端口 (默认: 5432)
  -d, --datadir <path>  数据目录 (默认: ./data)
  -c, --config <file>   配置文件路径
  -v, --verbose         详细日志输出
  -h, --help            显示帮助信息
```

### 示例

```bash
# 使用默认配置启动
./bin/tinydb-server

# 指定端口和数据目录
./bin/tinydb-server -p 5432 -d /var/lib/tinydb

# 详细日志模式（调试用）
./bin/tinydb-server -v
```

### 数据目录结构

```
data/
├── tn_class.dat          # 表元数据
├── tn_attribute.dat      # 列元数据
├── tn_index.dat          # 索引元数据
├── tn_user.dat           # 用户元数据
├── tn_statistic.dat      # 统计信息
├── wal/                  # WAL 日志目录
│   ├── 00000001.wal
│   └── 00000002.wal
├── tables/               # 表数据文件
│   ├── 1000.dat
│   └── 1001.dat
└── indexes/              # 索引数据文件
    ├── 2000.dat
    └── 2001.dat
```

---

## 客户端操作

### tinydb-cli 交互式客户端

#### 连接选项

```
./bin/tinydb-cli [选项]

选项:
  -h, --host <host>     服务器主机名 (默认: localhost)
  -p, --port <port>     服务器端口 (默认: 5432)
  -u, --user <user>     用户名
  -d, --database <db>   数据库名
  -c, --command <sql>   执行单条 SQL 命令
  -f, --file <file>     执行 SQL 脚本文件
  -v, --version         显示版本信息
  --help                显示帮助信息
```

#### 交互模式

```bash
# 进入交互模式
./bin/tinydb-cli

# 连接指定服务器
./bin/tinydb-cli -h localhost -p 5432
```

##### 交互命令

| 命令 | 说明 |
|------|------|
| `\q` | 退出客户端 |
| `\d` | 列出所有表 |
| `\d <表名>` | 查看表结构 |
| `\di` | 列出所有索引 |
| `\du` | 列出所有用户 |
| `\dt` | 列出所有表（同 `\d`） |
| `\?` | 显示帮助信息 |

#### 非交互模式

```bash
# 执行单条 SQL
./bin/tinydb-cli -c "SELECT * FROM users"

# 执行 SQL 脚本
./bin/tinydb-cli -f script.sql

# 从管道输入
cat script.sql | ./bin/tinydb-cli
```

---


## 高级功能

### 备份与恢复

#### 全量备份

```sql
-- 创建全量备份
BACKUP FULL;

-- 指定备份名称
BACKUP FULL TO 'backup_name';
```

#### 增量备份

```sql
-- 创建增量备份
BACKUP INCREMENTAL;
```

#### 基于时间点的恢复 (PITR)

```bash
# 使用恢复工具（需要停止服务器）
./bin/dbrecovery \
    --source=/path/to/backup \
    --target=/path/to/restore \
    --time="2024-01-15 14:30:00"
```

### 主从复制

#### 配置主库

编辑主库配置，启用 WAL 归档：

```bash
# 在主库上
./bin/tinydb-server -p 5432 -d /data/primary
```

#### 配置从库

```bash
# 使用主库备份初始化从库
./bin/tinydb-server -p 5433 -d /data/replica --replica-of=localhost:5432
```

#### 查看复制状态

```sql
-- 在主库上
SELECT * FROM tn_stat_replication;
```

### 数据导入导出

#### 导出数据

```bash
# 导出整个数据库
./bin/tinydb-cli -c "EXPORT DATABASE TO '/path/to/export.sql'"

# 导出单个表
./bin/tinydb-cli -c "EXPORT TABLE users TO '/path/to/users.csv' FORMAT CSV"

# 导出为 SQL
./bin/tinydb-cli -c "EXPORT TABLE users TO '/path/to/users.sql' FORMAT SQL"
```

#### 导入数据

```bash
# 从 SQL 文件导入
./bin/tinydb-cli -f /path/to/import.sql

# 从 CSV 导入
./bin/tinydb-cli -c "IMPORT TABLE users FROM '/path/to/users.csv' FORMAT CSV"
```

---

## 故障排查

### 常见问题

#### 1. 编译错误

**问题**: `Flex/Bison not found`

**解决**:
```bash
# Ubuntu/Debian
sudo apt-get install flex bison

# macOS
brew install flex bison
```

**问题**: `GTest not found`

**解决**:
```bash
# 跳过测试编译
cmake -DBUILD_TESTS=OFF ..

# 或安装 GTest
sudo apt-get install libgtest-dev
```

#### 2. 连接问题

**问题**: `Connection refused`

**排查步骤**:
1. 检查服务器是否启动: `ps aux | grep tinydb-server`
2. 检查端口是否正确: `netstat -tlnp | grep 5432`
3. 检查防火墙设置

#### 3. 数据损坏

**问题**: 崩溃后数据不一致

**解决**:
```bash
# 使用 WAL 恢复
./bin/tinydb-server -d /data/path --recovery
```

### 日志查看

```bash
# 查看服务器日志
tail -f data/server.log

# 详细日志模式
./bin/tinydb-server -v 2>&1 | tee server.log
```

### 性能调优

#### 缓冲区大小

在代码中修改 `buffer_pool.hpp`:
```cpp
static constexpr size_t POOL_SIZE = 1024;  // 增加缓冲区页数
```

#### 定期分析

```sql
-- 定期更新统计信息
ANALYZE;

-- 或在 crontab 中设置
0 2 * * * /path/to/tinydb-cli -c "ANALYZE"
```

---

## 附录

### 错误代码参考

| 错误代码 | 说明 |
|---------|------|
| 00000 | 成功 |
| 08XXX | 连接异常 |
| 22000 | 数据异常 |
| 26000 | 无效 SQL 语法 |
| 28000 | 无效授权 |
| 3D000 | 无效数据库名 |
| 42XXX | 语法错误或访问规则违规 |
| 55000 | 对象状态异常 |

### 配置文件示例

```ini
# tinydb.conf

[server]
port = 5432
data_dir = /var/lib/tinydb
max_connections = 100

[logging]
level = info
file = /var/log/tinydb/server.log

[buffer]
pool_size = 1024
page_size = 8192

[wal]
archive_mode = on
archive_command = 'cp %p /backup/wal/%f'
```

### 环境变量

| 变量名 | 说明 | 默认值 |
|-------|------|--------|
| `TINYDB_HOST` | 服务器主机 | localhost |
| `TINYDB_PORT` | 服务器端口 | 5432 |
| `TINYDB_USER` | 默认用户名 | current_user |
| `TINYDB_DATABASE` | 默认数据库 | tinydb |

---

## 技术支持

- **项目主页**: <repository-url>
- **问题反馈**: <issue-tracker-url>
- **文档更新**: 请参考最新版本的 CHANGELOG.md

---

*本文档适用于 TinyDB v0.1.0 版本*
