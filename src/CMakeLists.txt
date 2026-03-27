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

# 确保输出目录存在
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/sql/parser)

# 生成 Flex 词法分析器
flex_target(LEXER sql/parser/lexer.l ${CMAKE_CURRENT_BINARY_DIR}/sql/parser/lexer_generated.cpp
    COMPILE_FLAGS "--header-file=${CMAKE_CURRENT_BINARY_DIR}/sql/parser/lexer_generated.h"
)

# 生成 Bison 语法分析器
bison_target(PARSER sql/parser/parser.y ${CMAKE_CURRENT_BINARY_DIR}/sql/parser/parser_generated.cpp
    COMPILE_FLAGS "--defines=${CMAKE_CURRENT_BINARY_DIR}/sql/parser/parser_generated.h"
)

add_flex_bison_dependency(LEXER PARSER)

# 解析器包装类
set(PARSER_WRAPPER ${CMAKE_CURRENT_SOURCE_DIR}/sql/parser/parser.cpp)

add_library(sql_parser STATIC
    ${FLEX_LEXER_OUTPUTS}
    ${BISON_PARSER_OUTPUTS}
    ${PARSER_WRAPPER}
    sql/parser/ast.cpp
)

target_include_directories(sql_parser PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/sql/parser
    ${CMAKE_CURRENT_BINARY_DIR}/sql/parser
)

# 设置编译选项以支持 C++17
set_target_properties(sql_parser PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# 执行引擎库
add_library(sql_engine STATIC
    sql/executor/alter_table_executor.cpp
    sql/executor/create_table_executor.cpp
    sql/executor/delete_executor.cpp
    sql/executor/drop_table_executor.cpp
    sql/executor/executor.cpp
    sql/executor/executor_utils.cpp
    sql/executor/index_executor.cpp
    sql/executor/insert_executor.cpp
    sql/executor/select_executor.cpp
    sql/executor/transaction_executor.cpp
    sql/executor/update_executor.cpp
    sql/executor/view_executor.cpp
    sql/optimizer/optimizer.cpp
    sql/stmt/system_view_manager.cpp
)
target_link_libraries(sql_engine
    sql_parser
    storage
    common
)

# 服务端可执行文件
add_executable(tinydb-server
    main.cpp
)

target_link_libraries(tinydb-server
    sql_engine
    network
    sql_parser
    storage
    common
)

set_target_properties(tinydb-server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
