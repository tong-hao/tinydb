#include "statistics.h"
#include "table.h"
#include <algorithm>
#include <cmath>
#include <set>
#include <optional>
#include <unordered_map>

namespace tinydb {
namespace storage {

// ==================== ColumnStats ====================

std::vector<uint8_t> ColumnStats::serialize() const {
    std::vector<uint8_t> result;

    // column_name
    uint32_t name_len = column_name.size();
    result.resize(result.size() + sizeof(name_len));
    memcpy(result.data() + result.size() - sizeof(name_len), &name_len, sizeof(name_len));
    result.insert(result.end(), column_name.begin(), column_name.end());

    // data_type
    result.push_back(static_cast<uint8_t>(data_type));

    // counts
    result.resize(result.size() + sizeof(null_count));
    memcpy(result.data() + result.size() - sizeof(null_count), &null_count, sizeof(null_count));

    result.resize(result.size() + sizeof(distinct_count));
    memcpy(result.data() + result.size() - sizeof(distinct_count), &distinct_count, sizeof(distinct_count));

    result.resize(result.size() + sizeof(total_count));
    memcpy(result.data() + result.size() - sizeof(total_count), &total_count, sizeof(total_count));

    // min/max values (serialized as Field)
    auto min_data = min_value.serialize();
    uint32_t min_len = min_data.size();
    result.resize(result.size() + sizeof(min_len));
    memcpy(result.data() + result.size() - sizeof(min_len), &min_len, sizeof(min_len));
    result.insert(result.end(), min_data.begin(), min_data.end());

    auto max_data = max_value.serialize();
    uint32_t max_len = max_data.size();
    result.resize(result.size() + sizeof(max_len));
    memcpy(result.data() + result.size() - sizeof(max_len), &max_len, sizeof(max_len));
    result.insert(result.end(), max_data.begin(), max_data.end());

    // avg_value
    result.resize(result.size() + sizeof(avg_value));
    memcpy(result.data() + result.size() - sizeof(avg_value), &avg_value, sizeof(avg_value));

    return result;
}

bool ColumnStats::deserialize(const uint8_t* data, size_t size) {
    size_t pos = 0;

    // column_name
    if (pos + sizeof(uint32_t) > size) return false;
    uint32_t name_len;
    memcpy(&name_len, data + pos, sizeof(name_len));
    pos += sizeof(name_len);

    if (pos + name_len > size) return false;
    column_name.assign(reinterpret_cast<const char*>(data + pos), name_len);
    pos += name_len;

    // data_type
    if (pos + 1 > size) return false;
    data_type = static_cast<DataType>(data[pos]);
    pos += 1;

    // counts
    if (pos + sizeof(null_count) > size) return false;
    memcpy(&null_count, data + pos, sizeof(null_count));
    pos += sizeof(null_count);

    if (pos + sizeof(distinct_count) > size) return false;
    memcpy(&distinct_count, data + pos, sizeof(distinct_count));
    pos += sizeof(distinct_count);

    if (pos + sizeof(total_count) > size) return false;
    memcpy(&total_count, data + pos, sizeof(total_count));
    pos += sizeof(total_count);

    // min_value - 使用Field反序列化
    if (pos + sizeof(uint32_t) > size) return false;
    uint32_t min_len;
    memcpy(&min_len, data + pos, sizeof(min_len));
    pos += sizeof(min_len);
    if (pos + min_len > size) return false;
    if (min_len > 0) {
        min_value = Field::deserialize(data + pos, min_len, data_type);
    }
    pos += min_len;

    // max_value - 使用Field反序列化
    if (pos + sizeof(uint32_t) > size) return false;
    uint32_t max_len;
    memcpy(&max_len, data + pos, sizeof(max_len));
    pos += sizeof(max_len);
    if (pos + max_len > size) return false;
    if (max_len > 0) {
        max_value = Field::deserialize(data + pos, max_len, data_type);
    }
    pos += max_len;

    // avg_value
    if (pos + sizeof(avg_value) > size) return false;
    memcpy(&avg_value, data + pos, sizeof(avg_value));

    return true;
}

// ==================== TableStats ====================

std::vector<uint8_t> TableStats::serialize() const {
    std::vector<uint8_t> result;

    // table_name
    uint32_t name_len = table_name.size();
    result.resize(result.size() + sizeof(name_len));
    memcpy(result.data() + result.size() - sizeof(name_len), &name_len, sizeof(name_len));
    result.insert(result.end(), table_name.begin(), table_name.end());

    // counts
    result.resize(result.size() + sizeof(row_count));
    memcpy(result.data() + result.size() - sizeof(row_count), &row_count, sizeof(row_count));

    result.resize(result.size() + sizeof(page_count));
    memcpy(result.data() + result.size() - sizeof(page_count), &page_count, sizeof(page_count));

    result.resize(result.size() + sizeof(avg_row_length));
    memcpy(result.data() + result.size() - sizeof(avg_row_length), &avg_row_length, sizeof(avg_row_length));

    // column_stats count
    uint32_t col_count = column_stats.size();
    result.resize(result.size() + sizeof(col_count));
    memcpy(result.data() + result.size() - sizeof(col_count), &col_count, sizeof(col_count));

    // each column stat
    for (const auto& [name, stats] : column_stats) {
        auto col_data = stats.serialize();
        uint32_t col_len = col_data.size();
        result.resize(result.size() + sizeof(col_len));
        memcpy(result.data() + result.size() - sizeof(col_len), &col_len, sizeof(col_len));
        result.insert(result.end(), col_data.begin(), col_data.end());
    }

    return result;
}

bool TableStats::deserialize(const uint8_t* data, size_t size) {
    size_t pos = 0;

    // table_name
    if (pos + sizeof(uint32_t) > size) return false;
    uint32_t name_len;
    memcpy(&name_len, data + pos, sizeof(name_len));
    pos += sizeof(name_len);

    if (pos + name_len > size) return false;
    table_name.assign(reinterpret_cast<const char*>(data + pos), name_len);
    pos += name_len;

    // counts
    if (pos + sizeof(row_count) > size) return false;
    memcpy(&row_count, data + pos, sizeof(row_count));
    pos += sizeof(row_count);

    if (pos + sizeof(page_count) > size) return false;
    memcpy(&page_count, data + pos, sizeof(page_count));
    pos += sizeof(page_count);

    if (pos + sizeof(avg_row_length) > size) return false;
    memcpy(&avg_row_length, data + pos, sizeof(avg_row_length));
    pos += sizeof(avg_row_length);

    // column_stats
    if (pos + sizeof(uint32_t) > size) return false;
    uint32_t col_count;
    memcpy(&col_count, data + pos, sizeof(col_count));
    pos += sizeof(col_count);

    column_stats.clear();
    for (uint32_t i = 0; i < col_count; ++i) {
        if (pos + sizeof(uint32_t) > size) return false;
        uint32_t col_len;
        memcpy(&col_len, data + pos, sizeof(col_len));
        pos += sizeof(col_len);

        if (pos + col_len > size) return false;
        ColumnStats stats;
        if (!stats.deserialize(data + pos, col_len)) return false;
        pos += col_len;

        column_stats[stats.column_name] = stats;
    }

    return true;
}

// ==================== StatisticsManager ====================

StatisticsManager::StatisticsManager(BufferPoolManager* buffer_pool)
    : buffer_pool_(buffer_pool) {}

StatisticsManager::~StatisticsManager() = default;

bool StatisticsManager::analyzeTable(const std::string& table_name, TableManager* table_manager) {
    return collectTableStats(table_name, table_manager);
}

bool StatisticsManager::collectTableStats(const std::string& table_name, TableManager* table_manager) {
    TableStats stats;
    stats.table_name = table_name;

    // 获取表元数据
    auto table_meta = table_manager->getTable(table_name);
    if (!table_meta) {
        return false;
    }

    // 初始化列统计
    for (const auto& col : table_meta->schema.getColumns()) {
        ColumnStats col_stats;
        col_stats.column_name = col.name;
        col_stats.data_type = col.type;
        stats.column_stats[col.name] = col_stats;
    }

    // 扫描表收集统计
    auto it = table_manager->makeIterator(table_name);
    // 为每列维护一个独立的哈希集合来估计NDV
    std::unordered_map<std::string, std::set<size_t>> column_distinct_hashes;

    uint64_t total_row_size = 0;
    while (it.hasNext()) {
        Tuple tuple = it.getNext();
        stats.row_count++;
        total_row_size += tuple.serialize().size();

        // 更新每列的统计
        for (size_t i = 0; i < tuple.getFieldCount() && i < table_meta->schema.getColumnCount(); ++i) {
            const auto& field = tuple.getField(i);
            const auto& col_name = table_meta->schema.getColumn(i).name;
            auto& col_stats = stats.column_stats[col_name];

            col_stats.total_count++;

            if (field.isNull()) {
                col_stats.null_count++;
                continue;
            }

            // 更新min/max
            updateColumnStats(col_stats, field);

            // 更新NDV（使用近似方法）- 每列独立计算
            size_t h = hashField(field);
            column_distinct_hashes[col_name].insert(h);
        }
    }

    // 估算页数（假设8KB页，填充率70%）
    if (stats.row_count > 0) {
        stats.avg_row_length = total_row_size / stats.row_count;
        stats.page_count = static_cast<uint64_t>((total_row_size + PAGE_SIZE * 0.7 - 1) / (PAGE_SIZE * 0.7));

        // 估算每列NDV
        for (auto& [name, col_stats] : stats.column_stats) {
            auto it_hashes = column_distinct_hashes.find(name);
            if (it_hashes != column_distinct_hashes.end()) {
                col_stats.distinct_count = std::min(
                    static_cast<uint64_t>(it_hashes->second.size()),
                    col_stats.total_count
                );
            } else {
                col_stats.distinct_count = 0;
            }
        }
    }

    table_stats_[table_name] = stats;
    return true;
}

void StatisticsManager::updateColumnStats(ColumnStats& stats, const Field& value) {
    // 注意：total_count 已经在主循环中递增

    if (value.isNull()) {
        stats.null_count++;
        return;
    }

    // 更新min/max
    if (stats.min_value.isNull() || value.toString() < stats.min_value.toString()) {
        stats.min_value = value;
    }
    if (stats.max_value.isNull() || stats.max_value.toString() < value.toString()) {
        stats.max_value = value;
    }
}

size_t StatisticsManager::hashField(const Field& value) {
    // 简单的哈希函数
    if (value.isNull()) return 0;

    switch (value.getType()) {
        case DataType::INT32:
            return std::hash<int32_t>{}(value.getInt32());
        case DataType::INT64:
            return std::hash<int64_t>{}(value.getInt64());
        case DataType::VARCHAR:
        case DataType::CHAR:
            return std::hash<std::string>{}(value.getString());
        default:
            return 0;
    }
}

std::optional<TableStats> StatisticsManager::getTableStats(const std::string& table_name) {
    auto it = table_stats_.find(table_name);
    if (it != table_stats_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<ColumnStats> StatisticsManager::getColumnStats(const std::string& table_name,
                                                             const std::string& column_name) {
    auto it = table_stats_.find(table_name);
    if (it != table_stats_.end()) {
        auto col_it = it->second.column_stats.find(column_name);
        if (col_it != it->second.column_stats.end()) {
            return col_it->second;
        }
    }
    return std::nullopt;
}

void StatisticsManager::updateRowCount(const std::string& table_name, uint64_t row_count) {
    table_stats_[table_name].row_count = row_count;
}

void StatisticsManager::updatePageCount(const std::string& table_name, uint64_t page_count) {
    table_stats_[table_name].page_count = page_count;
}

double StatisticsManager::estimateSelectivity(const std::string& table_name,
                                              const std::string& column_name,
                                              const std::string& op,
                                              const Field& value) {
    auto col_stats_opt = getColumnStats(table_name, column_name);
    if (!col_stats_opt.has_value()) {
        // 无统计信息，返回默认选择率
        return 0.1;
    }

    const auto& stats = col_stats_opt.value();
    if (stats.total_count == 0) {
        return 0.0;
    }

    // 使用启发式方法估计选择率
    if (op == "=") {
        // 等值选择率 = 1 / NDV
        if (stats.distinct_count > 0) {
            return 1.0 / stats.distinct_count;
        }
        return 0.1;
    } else if (op == "<>" || op == "!=") {
        // 不等选择率 = 1 - 等值选择率
        return 1.0 - estimateSelectivity(table_name, column_name, "=", value);
    } else if (op == "<" || op == ">" || op == "<=" || op == ">=") {
        // 范围选择率，假设数据均匀分布
        return 0.3;
    } else if (op == "IS NULL") {
        return static_cast<double>(stats.null_count) / stats.total_count;
    } else if (op == "IS NOT NULL") {
        return 1.0 - static_cast<double>(stats.null_count) / stats.total_count;
    }

    return 0.1;  // 默认选择率
}

double StatisticsManager::estimateRangeSelectivity(const std::string& table_name,
                                                   const std::string& column_name,
                                                   const Field& start_value,
                                                   const Field& end_value) {
    auto col_stats_opt = getColumnStats(table_name, column_name);
    if (!col_stats_opt.has_value()) {
        return 0.3;  // 默认选择率
    }

    const auto& stats = col_stats_opt.value();
    if (stats.total_count == 0) {
        return 0.0;
    }

    // 估算范围选择率
    // 假设数据均匀分布
    // 实际应该使用直方图，这里简化处理 TODO: 
    return 0.3;
}

bool StatisticsManager::saveStats() {
    // 保存到系统表（简化处理） TODO: 
    return true;
}

bool StatisticsManager::loadStats() {
    // 从系统表加载（简化处理）TODO: 
    return true;
}

} // namespace storage
} // namespace tinydb
