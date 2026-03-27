#pragma once

#include "page.h"
#include <cstdint>
#include <vector>
#include <string>
#include <memory>

namespace tinydb {
namespace storage {

// 数据类型枚举
enum class DataType : uint8_t {
    INVALID = 0,
    INT32 = 1,      // 4字节整数
    INT64 = 2,      // 8字节整数
    FLOAT = 3,      // 4字节浮点
    DOUBLE = 4,     // 8字节浮点
    BOOL = 5,       // 1字节布尔
    CHAR = 6,       // 定长字符串
    VARCHAR = 7,    // 变长字符串
    NULL_TYPE = 8,  // NULL值
    DECIMAL = 9,     // DECIMAL类型（存储为DOUBLE）
    DATE = 10,
    TIME = 11,
    DATETIME = 12
};

// 获取数据类型大小（定长类型）
size_t getTypeSize(DataType type);

// 检查类型是否为定长
bool isFixedLength(DataType type);

// 列定义
struct ColumnDef {
    std::string name;
    DataType type;
    uint32_t length;        // 对于CHAR/VARCHAR是最大长度
    bool is_nullable;
    bool is_primary_key;

    ColumnDef() : type(DataType::INVALID), length(0), is_nullable(true), is_primary_key(false) {}

    ColumnDef(const std::string& n, DataType t, uint32_t len = 0, bool nullable = true, bool pk = false)
        : name(n), type(t), length(len), is_nullable(nullable), is_primary_key(pk) {}

    size_t getStorageSize() const {
        if (isFixedLength(type)) {
            return getTypeSize(type);
        }
        // 变长类型存储为：4字节长度 + 实际数据
        return sizeof(uint32_t) + length;
    }
};

// 表模式定义
class Schema {
public:
    Schema() = default;

    void addColumn(const ColumnDef& col) { columns_.push_back(col); }
    void addColumn(const std::string& name, DataType type, uint32_t length = 0) {
        columns_.emplace_back(name, type, length);
    }

    const ColumnDef& getColumn(size_t idx) const { return columns_[idx]; }
    const std::vector<ColumnDef>& getColumns() const { return columns_; }

    size_t getColumnCount() const { return columns_.size(); }

    // 根据列名查找索引
    int findColumnIndex(const std::string& name) const;

    // 获取列索引
    const ColumnDef* getColumn(const std::string& name) const;

    // 检查列是否存在
    bool columnExists(const std::string& name) const;

    // 删除列
    bool removeColumn(const std::string& name);

    // 修改列名
    bool renameColumn(const std::string& old_name, const std::string& new_name);

    // 计算行的最大存储大小（所有列都非空且为最大长度）
    size_t getRowMaxSize() const;

    // 序列化/反序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);

private:
    std::vector<ColumnDef> columns_;
};

// 字段值
class Field {
public:
    Field() : type_(DataType::NULL_TYPE), is_null_(true) {}
    explicit Field(int32_t val) : type_(DataType::INT32), is_null_(false) { data_.assign(reinterpret_cast<const char*>(&val), sizeof(val)); }
    explicit Field(int64_t val) : type_(DataType::INT64), is_null_(false) { data_.assign(reinterpret_cast<const char*>(&val), sizeof(val)); }
    explicit Field(float val) : type_(DataType::FLOAT), is_null_(false) { data_.assign(reinterpret_cast<const char*>(&val), sizeof(val)); }
    explicit Field(double val) : type_(DataType::DOUBLE), is_null_(false) { data_.assign(reinterpret_cast<const char*>(&val), sizeof(val)); }
    explicit Field(bool val) : type_(DataType::BOOL), is_null_(false) { data_.assign(reinterpret_cast<const char*>(&val), sizeof(val)); }
    explicit Field(const std::string& val, DataType type = DataType::VARCHAR);

    DataType getType() const { return type_; }
    bool isNull() const { return is_null_; }

    int32_t getInt32() const;
    int64_t getInt64() const;
    float getFloat() const;
    double getDouble() const;
    bool getBool() const;
    std::string getString() const;

    // 序列化为字节数组
    std::vector<uint8_t> serialize() const;

    // 从字节数组反序列化
    static Field deserialize(const uint8_t* data, size_t size, DataType type);

    std::string toString() const;

private:
    DataType type_;
    std::string data_;
    bool is_null_;
};

// 元组（行）
class Tuple {
public:
    Tuple() = default;
    explicit Tuple(const Schema* schema) : schema_(schema) {}

    void setSchema(const Schema* schema) { schema_ = schema; }
    const Schema* getSchema() const { return schema_; }

    // 添加字段值
    void addField(const Field& field) { fields_.push_back(field); }
    void setField(size_t idx, const Field& field);

    // 获取字段
    const Field& getField(size_t idx) const { return fields_[idx]; }
    size_t getFieldCount() const { return fields_.size(); }

    // 序列化为字节数组（用于存储到页中）
    std::vector<uint8_t> serialize() const;

    // 从字节数组反序列化
    bool deserialize(const uint8_t* data, size_t size);

    std::string toString() const;

private:
    const Schema* schema_ = nullptr;
    std::vector<Field> fields_;
};

// 元组ID (TID) - 用于定位行
struct TID {
    PageId page_id;
    uint16_t slot_id;

    TID() : page_id(INVALID_PAGE_ID), slot_id(0xFFFF) {}
    TID(PageId pid, uint16_t sid) : page_id(pid), slot_id(sid) {}

    bool isValid() const { return page_id != INVALID_PAGE_ID && slot_id != 0xFFFF; }

    bool operator==(const TID& other) const {
        return page_id == other.page_id && slot_id == other.slot_id;
    }

    bool operator!=(const TID& other) const {
        return !(*this == other);
    }

    std::string toString() const {
        return "(" + std::to_string(page_id) + ", " + std::to_string(slot_id) + ")";
    }
};

} // namespace storage
} // namespace tinydb
