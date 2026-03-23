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

# SQL 解析库 - 使用 Bison/Flex 生成
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# 生成 Flex 词法分析器
flex_target(LEXER sql/lexer.l ${CMAKE_CURRENT_BINARY_DIR}/sql/lexer_generated.cpp
    COMPILE_FLAGS "--header-file=${CMAKE_CURRENT_BINARY_DIR}/sql/lexer_generated.h"
)

# 生成 Bison 语法分析器
bison_target(PARSER sql/parser.y ${CMAKE_CURRENT_BINARY_DIR}/sql/parser_generated.cpp
    COMPILE_FLAGS "--defines=${CMAKE_CURRENT_BINARY_DIR}/sql/parser_generated.h"
)

add_flex_bison_dependency(LEXER PARSER)

# 解析器包装类
set(PARSER_WRAPPER ${CMAKE_CURRENT_SOURCE_DIR}/sql/parser.cpp)

add_library(sql STATIC
    ${FLEX_LEXER_OUTPUTS}
    ${BISON_PARSER_OUTPUTS}
    ${PARSER_WRAPPER}
    sql/ast.cpp
)

target_include_directories(sql PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/sql
    ${CMAKE_CURRENT_BINARY_DIR}/sql
)

# 设置编译选项以支持 C++17
set_target_properties(sql PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# 执行引擎库
add_library(engine STATIC
    engine/executor.cpp
    engine/optimizer.cpp
    engine/system_view_manager.cpp
)

target_link_libraries(engine
    sql
    storage
    common
)

# 服务端可执行文件
add_executable(tinydb-server
    server/main.cpp
)

target_link_libraries(tinydb-server
    engine
    network
    sql
    storage
    common
)

set_target_properties(tinydb-server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
