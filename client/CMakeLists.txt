# client/CMakeLists.txt

# 客户端 SDK 库
add_library(tinydb_client STATIC
    src/client.cpp
)

target_link_libraries(tinydb_client
    tinydb_network
    tinydb_common
    Threads::Threads
)

# 客户端可执行文件
add_executable(dbclient
    main.cpp
)

target_link_libraries(dbclient
    tinydb_client
    tinydb_common
)

set_target_properties(dbclient PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
