#pragma once

#include "storage/tuple.h"
#include "storage/table.h"
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>

namespace tinydb {
namespace storage {

// 前向声明
class BufferPoolManager;
class TableManager;

// 列统计信息
struct ColumnStats {
    std::string column_name;
    DataType data_type;
    uint64_t null_count;           // NULL值数量
    uint64_t distinct_count;       // 不同值数量(NDV)
    uint64_t total_count;          // 总行数
    Field min_value;               // 最小值
    Field max_value;               // 最大值
    double avg_value;              // 平均值（数值类型）

    ColumnStats() : data_type(DataType::INVALID), null_count(0),
                    distinct_count(0), total_count(0), avg_value(0.0) {}

    // 序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);
};

// 表统计信息
struct TableStats {
    std::string table_name;
    uint64_t row_count;            // 行数
    uint64_t page_count;           // 页数
    uint64_t avg_row_length;       // 平均行长度
    std::unordered_map<std::string, ColumnStats> column_stats;

    TableStats() : row_count(0), page_count(0), avg_row_length(0) {}

    // 序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);
};

// 索引统计信息
struct IndexStats {
    std::string index_name;
    std::string table_name;
    std::string column_name;
    uint64_t key_count;            // 键数量
    uint64_t page_count;           // 索引页数量
    uint64_t tree_height;          // 树高度
    double avg_key_length;         // 平均键长度

    IndexStats() : key_count(0), page_count(0), tree_height(0), avg_key_length(0.0) {}

    // 序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);
};

// 统计信息管理器
class StatisticsManager {
public:
    explicit StatisticsManager(BufferPoolManager* buffer_pool);
    ~StatisticsManager();

    // 收集表统计信息
    bool analyzeTable(const std::string& table_name, TableManager* table_manager);

    // 获取表统计
    std::optional<TableStats> getTableStats(const std::string& table_name);

    // 获取列统计
    std::optional<ColumnStats> getColumnStats(const std::string& table_name,
                                               const std::string& column_name);

    // 获取索引统计
    std::optional<IndexStats> getIndexStats(const std::string& index_name);

    // 更新表行数
    void updateRowCount(const std::string& table_name, uint64_t row_count);

    // 更新页数
    void updatePageCount(const std::string& table_name, uint64_t page_count);

    // 估计选择率
    double estimateSelectivity(const std::string& table_name,
                               const std::string& column_name,
                               const std::string& op,
                               const Field& value);

    // 估计范围查询选择率
    double estimateRangeSelectivity(const std::string& table_name,
                                    const std::string& column_name,
                                    const Field& start_value,
                                    const Field& end_value);

    // 保存统计信息到系统表
    bool saveStats();

    // 从系统表加载统计信息
    bool loadStats();

private:
    BufferPoolManager* buffer_pool_;
    std::unordered_map<std::string, TableStats> table_stats_;
    std::unordered_map<std::string, IndexStats> index_stats_;

    // 从表扫描收集统计
    bool collectTableStats(const std::string& table_name, TableManager* table_manager);

    // 更新列统计
    void updateColumnStats(ColumnStats& stats, const Field& value);

    // 计算字段哈希（用于NDV估计）
    size_t hashField(const Field& value);
};

} // namespace storage
} // namespace tinydb
