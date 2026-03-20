# storage/CMakeLists.txt

set(STORAGE_SOURCES
    page.cpp
    disk_manager.cpp
    buffer_pool.cpp
    tuple.cpp
    table.cpp
    wal.cpp
    transaction.cpp
    lock_manager.cpp
    storage_engine.cpp
    btree_index.cpp
    index_manager.cpp
    statistics.cpp
    permission_manager.cpp
    backup_manager.cpp
    replication_manager.cpp
    diagnostics_manager.cpp
    import_export_manager.cpp
)

set(STORAGE_HEADERS
    page.h
    disk_manager.h
    buffer_pool.h
    tuple.h
    table.h
    wal.h
    transaction.h
    lock_manager.h
    storage_engine.h
    btree_index.h
    index_manager.h
    statistics.h
    permission_manager.h
    backup_manager.h
    replication_manager.h
    diagnostics_manager.h
    import_export_manager.h
)

# 创建存储引擎库
add_library(storage STATIC ${STORAGE_SOURCES} ${STORAGE_HEADERS})

target_include_directories(storage PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(storage PUBLIC
    common
)
