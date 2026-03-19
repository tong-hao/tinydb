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

# Phase 3: 表达式求值单元测试
add_executable(test_expression
    unit/test_expression.cpp
)

target_link_libraries(test_expression
    sql
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
gtest_discover_tests(test_expression)
gtest_discover_tests(test_basic)
