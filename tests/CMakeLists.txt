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

target_link_libraries(test_connection
    network
    tinydb_client
    GTest::gtest_main
)

# 集成测试
add_executable(test_basic
    integration/test_basic.cpp
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
gtest_discover_tests(test_basic)
