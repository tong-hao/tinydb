#include "index_manager.h"
#include "table.h"
#include <algorithm>

namespace tinydb {
namespace storage {

IndexManager::IndexManager(BufferPoolManager* buffer_pool)
    : buffer_pool_(buffer_pool), next_index_id_(1) {
    initializeSystemTables();
}

IndexManager::~IndexManager() = default;

bool IndexManager::initializeSystemTables() {
    // tn_index 系统表用于存储索引元数据
    tn_index_meta_ = std::make_shared<TableMeta>();
    tn_index_meta_->table_id = 3;  // tn_class=1, tn_attribute=2, tn_index=3
    tn_index_meta_->table_name = "tn_index";
    tn_index_meta_->schema_name = "tn_catalog";
    tn_index_meta_->first_page_id = INVALID_PAGE_ID;
    tn_index_meta_->tuple_count = 0;

    tn_index_meta_->schema = Schema();
    tn_index_meta_->schema.addColumn("index_id", DataType::INT32, sizeof(int32_t));
    tn_index_meta_->schema.addColumn("index_name", DataType::VARCHAR, 128);
    tn_index_meta_->schema.addColumn("table_id", DataType::INT32, sizeof(int32_t));
    tn_index_meta_->schema.addColumn("table_name", DataType::VARCHAR, 128);
    tn_index_meta_->schema.addColumn("column_name", DataType::VARCHAR, 128);
    tn_index_meta_->schema.addColumn("column_id", DataType::INT32, sizeof(int32_t));
    tn_index_meta_->schema.addColumn("root_page_id", DataType::INT32, sizeof(int32_t));
    tn_index_meta_->schema.addColumn("is_unique", DataType::INT32, sizeof(int32_t));
    tn_index_meta_->schema.addColumn("key_count", DataType::INT32, sizeof(int32_t));

    return true;
}

bool IndexManager::createIndex(const std::string& index_name, const std::string& table_name,
                               const std::string& column_name, bool is_unique) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (indexes_.find(index_name) != indexes_.end()) {
        return false;  // 索引已存在
    }

    // 创建索引元数据
    IndexMeta meta;
    meta.index_id = next_index_id_++;
    meta.index_name = index_name;
    meta.table_name = table_name;
    meta.column_name = column_name;
    meta.is_unique = is_unique;
    meta.key_count = 0;

    // 创建B+树索引
    auto index = std::make_shared<BTreeIndex>(buffer_pool_, meta);
    if (!index->initialize()) {
        return false;
    }

    // 保存索引
    indexes_[index_name] = index;

    // 保存到系统表
    saveIndexMeta(meta);

    return true;
}

bool IndexManager::dropIndex(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return false;  // 索引不存在
    }

    indexes_.erase(it);
    removeIndexMeta(index_name);

    return true;
}

std::shared_ptr<BTreeIndex> IndexManager::getIndex(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    return getIndexUnlocked(index_name);
}

std::shared_ptr<BTreeIndex> IndexManager::getIndexUnlocked(const std::string& index_name) {
    auto it = indexes_.find(index_name);
    if (it != indexes_.end()) {
        return it->second;
    }
    return nullptr;
}

std::vector<std::shared_ptr<BTreeIndex>> IndexManager::getTableIndexes(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<BTreeIndex>> result;

    for (const auto& [name, index] : indexes_) {
        if (!index) continue;
        if (index->getMeta().table_name == table_name) {
            result.push_back(index);
        }
    }

    return result;
}

std::shared_ptr<BTreeIndex> IndexManager::getColumnIndex(const std::string& table_name,
                                                          const std::string& column_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (const auto& [name, index] : indexes_) {
        if (!index) continue;
        const auto& meta = index->getMeta();
        if (meta.table_name == table_name && meta.column_name == column_name) {
            return index;
        }
    }

    return nullptr;
}

bool IndexManager::indexExists(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    return indexes_.find(index_name) != indexes_.end();
}

std::vector<std::string> IndexManager::getAllIndexNames() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;

    for (const auto& [name, index] : indexes_) {
        if (index) {
            result.push_back(name);
        }
    }

    return result;
}

std::vector<std::string> IndexManager::getIndexNamesForTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;

    for (const auto& [name, index] : indexes_) {
        if (!index) continue;
        if (index->getMeta().table_name == table_name) {
            result.push_back(name);
        }
    }

    return result;
}

// TODO: Insert语句执行的时候，这里应该被调用
bool IndexManager::insertEntry(const std::string& table_name, const std::string& column_name,
                               const IndexKey& key, const TID& tid) {
    auto index = getColumnIndex(table_name, column_name);
    if (!index) {
        return false;  // 没有索引，忽略
    }

    return index->insert(key, tid);
}

bool IndexManager::deleteEntry(const std::string& table_name, const std::string& column_name,
                               const IndexKey& key, const TID& tid) {
    auto index = getColumnIndex(table_name, column_name);
    if (!index) {
        return false;  // 没有索引，忽略
    }

    return index->remove(key, tid);
}

bool IndexManager::loadIndexes() {
    // 从系统表加载索引
    // TODO: 这里简化处理，实际应该从tn_index表读取
    return true;
}

bool IndexManager::saveIndexMeta(const IndexMeta& meta) {
    // 保存到tn_index系统表
    // TODO: 简化处理，实际应该插入到系统表
    return true;
}

bool IndexManager::removeIndexMeta(const std::string& index_name) {
    // 从tn_index系统表删除
    // TODO: 简化处理
    return true;
}

} // namespace storage
} // namespace tinydb
