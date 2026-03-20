#pragma once

#include "btree_index.h"
#include "table.h"
#include <unordered_map>
#include <mutex>

namespace tinydb {
namespace storage {

// 索引管理器
class IndexManager {
public:
    explicit IndexManager(BufferPoolManager* buffer_pool);
    ~IndexManager();

    // 创建索引
    bool createIndex(const std::string& index_name, const std::string& table_name,
                     const std::string& column_name, bool is_unique = false);

    // 删除索引
    bool dropIndex(const std::string& index_name);

    // 获取索引
    std::shared_ptr<BTreeIndex> getIndex(const std::string& index_name);

    // 获取表的索引列表
    std::vector<std::shared_ptr<BTreeIndex>> getTableIndexes(const std::string& table_name);

    // 获取列的索引
    std::shared_ptr<BTreeIndex> getColumnIndex(const std::string& table_name,
                                                const std::string& column_name);

    // 检查索引是否存在
    bool indexExists(const std::string& index_name);

    // 获取所有索引名
    std::vector<std::string> getAllIndexNames();

    // 获取表的索引名列表
    std::vector<std::string> getIndexNamesForTable(const std::string& table_name);

    // 插入索引条目（在表插入数据时调用）
    bool insertEntry(const std::string& table_name, const std::string& column_name,
                     const IndexKey& key, const TID& tid);

    // 删除索引条目（在表删除数据时调用）
    bool deleteEntry(const std::string& table_name, const std::string& column_name,
                     const IndexKey& key, const TID& tid);

    // 从系统表加载索引
    bool loadIndexes();

    // 保存索引元数据到系统表
    bool saveIndexMeta(const IndexMeta& meta);

    // 从系统表删除索引元数据
    bool removeIndexMeta(const std::string& index_name);

private:
    BufferPoolManager* buffer_pool_;
    std::unordered_map<std::string, std::shared_ptr<BTreeIndex>> indexes_;
    std::mutex mutex_;
    uint32_t next_index_id_;

    // 系统表元数据
    std::shared_ptr<TableMeta> pg_index_meta_;

    // 初始化系统表
    bool initializeSystemTables();

    // 从pg_index加载索引
    bool loadIndexFromPgIndex();

    // 获取索引元数据（内部使用）
    std::shared_ptr<BTreeIndex> getIndexUnlocked(const std::string& index_name);
};

} // namespace storage
} // namespace tinydb
