#pragma once

#include "tuple.h"
#include "page.h"
#include "buffer_pool.h"
#include <string>
#include <vector>
#include <memory>
#include <mutex>

namespace tinydb {
namespace storage {

// Table metadata (corresponds to tn_class)
struct TableMeta {
    uint32_t table_id;              // Table ID
    std::string table_name;         // Table name
    std::string schema_name;        // Schema name (default: public)
    PageId first_page_id;           // First data page
    PageId last_page_id;            // Last data page
    uint32_t tuple_count;           // Number of tuples
    uint32_t page_count;            // Number of pages
    uint32_t next_tuple_id;         // Next tuple ID
    Schema schema;                  // Table schema

    TableMeta() : table_id(0), first_page_id(INVALID_PAGE_ID), last_page_id(INVALID_PAGE_ID),
                  tuple_count(0), page_count(0), next_tuple_id(1) {}

    // Serialize
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);
};

// Column metadata (corresponds to tn_attribute)
struct ColumnMeta {
    uint32_t table_id;              // Parent table ID
    std::string column_name;        // Column name
    uint16_t column_id;             // Column number
    DataType data_type;             // Data type
    uint32_t type_length;           // Type length
    bool is_nullable;               // Whether nullable
    bool is_primary_key;            // Whether primary key
    int32_t default_value_offset;   // Default value offset (not supported yet)

    ColumnMeta() : table_id(0), column_id(0), data_type(DataType::INVALID),
                   type_length(0), is_nullable(true), is_primary_key(false),
                   default_value_offset(-1) {}

    // Construct from ColumnDef
    explicit ColumnMeta(const ColumnDef& def);

    // Serialize
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);
};

// Table iterator
class TableIterator {
public:
    TableIterator(BufferPoolManager* buffer_pool, PageId start_page, const Schema* schema);

    // Check if there is a next row
    bool hasNext();

    // Get current row
    Tuple getNext();

    // Get current TID
    TID getCurrentTID() const { return current_tid_; }

private:
    BufferPoolManager* buffer_pool_;
    const Schema* schema_;
    PageId current_page_id_;
    uint16_t current_slot_id_;
    TID current_tid_;
    bool has_next_;

    void advance();
    bool findNextValidTuple();
};

// 表管理器
class TableManager {
public:
    TableManager(BufferPoolManager* buffer_pool);
    ~TableManager();

    // 创建表
    bool createTable(const std::string& table_name, const Schema& schema);

    // 删除表
    bool dropTable(const std::string& table_name);

    // 获取表元数据
    std::shared_ptr<TableMeta> getTable(const std::string& table_name);

    // 检查表是否存在
    bool tableExists(const std::string& table_name);

    // 获取所有表名
    std::vector<std::string> getAllTableNames();

    // 插入元组
    TID insertTuple(const std::string& table_name, const Tuple& tuple);

    // 删除元组
    bool deleteTuple(const std::string& table_name, const TID& tid);

    // 获取元组
    Tuple getTuple(const std::string& table_name, const TID& tid);

    // 创建表迭代器
    TableIterator makeIterator(const std::string& table_name);

    // 加载系统表（启动时调用）
    bool loadSystemTables();

    // ALTER TABLE: 添加列
    bool addColumn(const std::string& table_name, const ColumnDef& column_def);

    // ALTER TABLE: 删除列
    bool dropColumn(const std::string& table_name, const std::string& column_name);

    // ALTER TABLE: 修改列
    bool modifyColumn(const std::string& table_name, const std::string& column_name, const ColumnDef& new_def);

    // ALTER TABLE: 重命名表
    bool renameTable(const std::string& old_name, const std::string& new_name);

    // ALTER TABLE: 重命名列
    bool renameColumn(const std::string& table_name, const std::string& old_col_name, const std::string& new_col_name);

private:
    BufferPoolManager* buffer_pool_;
    std::unordered_map<std::string, std::shared_ptr<TableMeta>> tables_;
    std::mutex mutex_;
    uint32_t next_table_id_;

    // 系统表元数据
    std::shared_ptr<TableMeta> tn_class_meta_;
    std::shared_ptr<TableMeta> tn_attribute_meta_;

    // 初始化系统表
    bool initializeSystemTables();

    // 从tn_class加载表
    bool loadTableFromPgClass();

    // 保存表元数据到tn_class
    bool saveTableMeta(const TableMeta& meta);

    // 删除表元数据从tn_class
    bool removeTableMeta(const std::string& table_name);

    // 保存列元数据到tn_attribute
    bool saveColumnMeta(const ColumnMeta& meta);

    // 删除列元数据从tn_attribute
    bool removeColumnMeta(uint32_t table_id);

    // 更新列元数据在tn_attribute
    bool updateColumnMeta(uint32_t table_id, const std::string& old_col_name, const ColumnDef& new_def);

    // 获取表的列元数据
    std::vector<ColumnMeta> loadColumnMeta(uint32_t table_id);

    // 查找或创建数据页以插入
    PageId findPageForInsert(uint32_t tuple_size);

    // 内部使用：插入元组到指定表（调用者必须已持有锁）
    TID insertTupleInternal(TableMeta* meta, const Tuple& tuple);

    // 内部使用：获取表元数据（调用者必须已持有锁）
    std::shared_ptr<TableMeta> getTableUnlocked(const std::string& table_name);
};

} // namespace storage
} // namespace tinydb
