# Tinydb
​
## 构建和测试命令
### 编译
```bash
mkdir build
cd build

# Debug 模式（开发调试使用）
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Release 模式（生产环境使用）
cmake -DCMAKE_BUILD_TYPE=Release ..

make -j$(nproc)
```
## 运行

```bash
cd build

# 启动数据库服务器
bin/tinydb-server

# 启动交互式命令行客户端
bin/tinydb-cli

# 执行sql文件
bin/tinydb-cli -f xxx.sql
```
​
## 目录结构说明

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
├── client/                # 客户端
├── tests/                 # 测试代码
└── docs/                  # 文档
```
​
## 常见陷阱



