# client/CMakeLists.txt

# 客户端 SDK 库
add_library(tinydb_client STATIC
    src/client.cpp
)

target_include_directories(tinydb_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(tinydb_client
    network
    common
    Threads::Threads
)

# 客户端可执行文件
add_executable(dbclient
    main.cpp
)

target_link_libraries(dbclient
    tinydb_client
    common
)

set_target_properties(dbclient PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
