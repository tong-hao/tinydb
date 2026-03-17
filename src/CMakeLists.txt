# src/CMakeLists.txt

# 公共组件库
add_library(tinydb_common STATIC
    common/error.cpp
    common/logger.cpp
)

target_link_libraries(tinydb_common
    Threads::Threads
)

# 网络库
add_library(tinydb_network STATIC
    network/protocol.cpp
    network/connection.cpp
    network/server.cpp
)

target_link_libraries(tinydb_network
    tinydb_common
    Threads::Threads
)

# SQL 解析库 (使用手写 Lexer，暂时不使用 Flex/Bison)
add_library(tinydb_sql STATIC
    sql/lexer.cpp
    sql/ast.cpp
)

target_include_directories(tinydb_sql PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/sql
)

# 执行引擎库
add_library(tinydb_engine STATIC
    engine/executor.cpp
)

target_link_libraries(tinydb_engine
    tinydb_sql
    tinydb_common
)

# 服务端可执行文件
add_executable(dbserver
    server/main.cpp
)

target_link_libraries(dbserver
    tinydb_engine
    tinydb_network
    tinydb_sql
    tinydb_common
)

set_target_properties(dbserver PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
