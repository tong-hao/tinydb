# storage/CMakeLists.txt

set(STORAGE_SOURCES
    page.cpp
    disk_manager.cpp
    buffer_pool.cpp
    tuple.cpp
    table.cpp
    wal.cpp
    storage_engine.cpp
)

set(STORAGE_HEADERS
    page.h
    disk_manager.h
    buffer_pool.h
    tuple.h
    table.h
    wal.h
    storage_engine.h
)

# 创建存储引擎库
add_library(storage STATIC ${STORAGE_SOURCES} ${STORAGE_HEADERS})

target_include_directories(storage PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(storage PUBLIC
    common
)
