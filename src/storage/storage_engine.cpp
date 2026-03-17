#include "storage_engine.h"
#include "common/logger.h"

namespace tinydb {
namespace storage {

StorageEngine::StorageEngine(const StorageConfig& config)
    : config_(config), initialized_(false) {}

StorageEngine::~StorageEngine() {
    shutdown();
}

bool StorageEngine::initialize() {
    if (initialized_) {
        return true;
    }

    LOG_INFO("Initializing storage engine...");

    // 创建磁盘管理器
    disk_manager_ = std::make_unique<DiskManager>(config_.db_file_path);

    // 创建缓冲池管理器
    buffer_pool_ = std::make_unique<BufferPoolManager>(config_.buffer_pool_size, disk_manager_.get());

    // 创建WAL管理器
    wal_manager_ = std::make_unique<WALManager>(config_.wal_file_path);
    if (!wal_manager_->initialize()) {
        LOG_ERROR("Failed to initialize WAL manager");
        return false;
    }

    // 创建表管理器
    table_manager_ = std::make_unique<TableManager>(buffer_pool_.get());

    initialized_ = true;
    LOG_INFO("Storage engine initialized successfully");
    return true;
}

bool StorageEngine::createTable(const std::string& table_name, const Schema& schema) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->createTable(table_name, schema);
}

bool StorageEngine::dropTable(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->dropTable(table_name);
}

std::shared_ptr<TableMeta> StorageEngine::getTable(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return nullptr;
    }
    return table_manager_->getTable(table_name);
}

bool StorageEngine::tableExists(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->tableExists(table_name);
}

std::vector<std::string> StorageEngine::getAllTableNames() {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return {};
    }
    return table_manager_->getAllTableNames();
}

TID StorageEngine::insert(const std::string& table_name, const Tuple& tuple) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return TID();
    }
    return table_manager_->insertTuple(table_name, tuple);
}

bool StorageEngine::remove(const std::string& table_name, const TID& tid) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->deleteTuple(table_name, tid);
}

Tuple StorageEngine::get(const std::string& table_name, const TID& tid) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return Tuple();
    }
    return table_manager_->getTuple(table_name, tid);
}

TableIterator StorageEngine::scan(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return TableIterator(nullptr, INVALID_PAGE_ID, nullptr);
    }
    return table_manager_->makeIterator(table_name);
}

bool StorageEngine::checkpoint() {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }

    LOG_INFO("Performing checkpoint...");

    // 刷新所有脏页
    buffer_pool_->flushAllPages();

    // 刷新WAL
    wal_manager_->flush();

    LOG_INFO("Checkpoint completed");
    return true;
}

void StorageEngine::shutdown() {
    if (!initialized_) {
        return;
    }

    LOG_INFO("Shutting down storage engine...");

    // 执行检查点
    checkpoint();

    // 关闭组件
    table_manager_.reset();
    wal_manager_.reset();
    buffer_pool_.reset();
    disk_manager_.reset();

    initialized_ = false;
    LOG_INFO("Storage engine shutdown complete");
}

} // namespace storage
} // namespace tinydb
