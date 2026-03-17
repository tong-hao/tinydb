# src/CMakeLists.txt

# 公共组件库
add_library(common STATIC
    common/error.cpp
    common/logger.cpp
)

target_link_libraries(common
    Threads::Threads
)

# 存储引擎库
add_subdirectory(storage)

# 网络库
add_library(network STATIC
    network/protocol.cpp
    network/connection.cpp
    network/server.cpp
)

target_link_libraries(network
    common
    Threads::Threads
)

# SQL 解析库
add_library(sql STATIC
    sql/lexer.cpp
    sql/ast.cpp
)

target_include_directories(sql PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/sql
)

# 执行引擎库
add_library(engine STATIC
    engine/executor.cpp
)

target_link_libraries(engine
    sql
    storage
    common
)

# 服务端可执行文件
add_executable(dbserver
    server/main.cpp
)

target_link_libraries(dbserver
    engine
    network
    sql
    storage
    common
)

set_target_properties(dbserver PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
