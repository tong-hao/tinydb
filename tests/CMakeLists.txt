# tests/CMakeLists.txt

# 单元测试
add_executable(test_lexer
    unit/test_lexer.cpp
)

target_link_libraries(test_lexer
    sql
    GTest::gtest_main
)

add_executable(test_protocol
    unit/test_protocol.cpp
)

target_link_libraries(test_protocol
    network
    GTest::gtest_main
)

add_executable(test_connection
    unit/test_connection.cpp
)

target_include_directories(test_connection PRIVATE
    ${CMAKE_SOURCE_DIR}/client/include
)

target_link_libraries(test_connection
    network
    tinydb_client
    GTest::gtest_main
)

# Phase 3: 事务管理单元测试
add_executable(test_transaction
    unit/test_transaction.cpp
)

target_link_libraries(test_transaction
    storage
    GTest::gtest_main
)

# Phase 3: 锁管理器单元测试
add_executable(test_lock_manager
    unit/test_lock_manager.cpp
)

target_link_libraries(test_lock_manager
    storage
    GTest::gtest_main
)

# Phase 3: UPDATE/DELETE语句单元测试
add_executable(test_update_delete
    unit/test_update_delete.cpp
)

target_link_libraries(test_update_delete
    engine
    sql
    storage
    GTest::gtest_main
)

# ALTER TABLE单元测试
add_executable(test_alter_table
    unit/test_alter_table.cpp
)

target_link_libraries(test_alter_table
    engine
    sql
    storage
    GTest::gtest_main
)

# CREATE TABLE 数据类型单元测试
add_executable(test_create_table_types
    unit/test_create_table_types.cpp
)

target_link_libraries(test_create_table_types
    engine
    sql
    storage
    GTest::gtest_main
)

# Phase 3: 表达式求值单元测试
add_executable(test_expression
    unit/test_expression.cpp
)

target_link_libraries(test_expression
    sql
    storage
    GTest::gtest_main
)

# Phase 3: 执行器单元测试
add_executable(test_executor
    unit/test_executor.cpp
)

target_link_libraries(test_executor
    engine
    sql
    storage
    GTest::gtest_main
)

# Phase 3: WAL日志单元测试
add_executable(test_wal
    unit/test_wal.cpp
)

target_link_libraries(test_wal
    storage
    GTest::gtest_main
)

# Phase 4: B+树索引单元测试
add_executable(test_btree_index
    unit/test_btree_index.cpp
)

target_link_libraries(test_btree_index
    storage
    GTest::gtest_main
)

# Phase 4: 索引DDL单元测试
add_executable(test_index_ddl
    unit/test_index_ddl.cpp
)

target_link_libraries(test_index_ddl
    storage
    GTest::gtest_main
)

# Phase 4: 统计信息单元测试
add_executable(test_statistics
    unit/test_statistics.cpp
)

target_link_libraries(test_statistics
    storage
    GTest::gtest_main
)

# Phase 4: 查询优化器单元测试
add_executable(test_optimizer
    unit/test_optimizer.cpp
)

target_link_libraries(test_optimizer
    engine
    storage
    GTest::gtest_main
)

# Phase 4: JOIN单元测试
add_executable(test_join
    unit/test_join.cpp
)

target_link_libraries(test_join
    engine
    storage
    GTest::gtest_main
)

# Phase 4: 集成测试
add_executable(test_phase4_integration
    unit/test_phase4_integration.cpp
)

target_link_libraries(test_phase4_integration
    engine
    storage
    GTest::gtest_main
)

# Phase 5: 企业级功能集成测试
add_executable(test_phase5_integration
    unit/test_phase5_integration.cpp
)

target_link_libraries(test_phase5_integration
    engine
    storage
    GTest::gtest_main
)

# 集成测试
add_executable(test_basic
    integration/test_basic.cpp
)

target_include_directories(test_basic PRIVATE
    ${CMAKE_SOURCE_DIR}/client/include
)

target_link_libraries(test_basic
    tinydb_client
    network
    engine
    GTest::gtest_main
)

# 添加测试
include(GoogleTest)
gtest_discover_tests(test_lexer)
gtest_discover_tests(test_protocol)
gtest_discover_tests(test_connection)
gtest_discover_tests(test_transaction)
gtest_discover_tests(test_lock_manager)
gtest_discover_tests(test_update_delete)
gtest_discover_tests(test_alter_table)
gtest_discover_tests(test_create_table_types)
gtest_discover_tests(test_expression)
gtest_discover_tests(test_executor)
gtest_discover_tests(test_wal)
gtest_discover_tests(test_btree_index)
gtest_discover_tests(test_index_ddl)
gtest_discover_tests(test_statistics)
gtest_discover_tests(test_optimizer)
gtest_discover_tests(test_join)
gtest_discover_tests(test_phase4_integration)
gtest_discover_tests(test_phase5_integration)
gtest_discover_tests(test_basic)
