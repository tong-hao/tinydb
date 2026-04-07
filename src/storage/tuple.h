#pragma once

#include "page.h"
#include <cstdint>
#include <vector>
#include <string>
#include <memory>

namespace tinydb {
namespace storage {

// Data type enumeration
enum class DataType : uint8_t {
    INVALID = 0,
    INT32 = 1,      // 4-byte integer
    INT64 = 2,      // 8-byte integer
    FLOAT = 3,      // 4-byte float
    DOUBLE = 4,     // 8-byte double
    BOOL = 5,       // 1-byte boolean
    CHAR = 6,       // Fixed-length string
    VARCHAR = 7,    // Variable-length string
    NULL_TYPE = 8,  // NULL value
    DECIMAL = 9,     // DECIMAL type (stored as DOUBLE)
    DATE = 10,
    TIME = 11,
    DATETIME = 12
};

// Get data type size (fixed-length types)
size_t getTypeSize(DataType type);

// Check if type is fixed-length
bool isFixedLength(DataType type);

// Column definition
struct ColumnDef {
    std::string name;
    DataType type;
    uint32_t length;        // Maximum length for CHAR/VARCHAR
    bool is_nullable;
    bool is_primary_key;

    ColumnDef() : type(DataType::INVALID), length(0), is_nullable(true), is_primary_key(false) {}

    ColumnDef(const std::string& n, DataType t, uint32_t len = 0, bool nullable = true, bool pk = false)
        : name(n), type(t), length(len), is_nullable(nullable), is_primary_key(pk) {}

    size_t getStorageSize() const {
        if (isFixedLength(type)) {
            return getTypeSize(type);
        }
        // Variable-length types stored as: 4-byte length + actual data
        return sizeof(uint32_t) + length;
    }
};

// Table schema definition
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

    // Find column index by name
    int findColumnIndex(const std::string& name) const;

    // Get column by name
    const ColumnDef* getColumn(const std::string& name) const;

    // Check if column exists
    bool columnExists(const std::string& name) const;

    // Remove column
    bool removeColumn(const std::string& name);

    // Rename column
    bool renameColumn(const std::string& old_name, const std::string& new_name);

    // Calculate maximum row storage size (all columns non-null and at max length)
    size_t getRowMaxSize() const;

    // Serialize/deserialize
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);

private:
    std::vector<ColumnDef> columns_;
};

// Field value
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

    // Serialize to byte array
    std::vector<uint8_t> serialize() const;

    // Deserialize from byte array
    static Field deserialize(const uint8_t* data, size_t size, DataType type);

    std::string toString() const;

private:
    DataType type_;
    std::string data_;
    bool is_null_;
};

// Tuple (row)
class Tuple {
public:
    Tuple() = default;
    explicit Tuple(const Schema* schema) : schema_(schema) {}

    void setSchema(const Schema* schema) { schema_ = schema; }
    const Schema* getSchema() const { return schema_; }

    // Add field value
    void addField(const Field& field) { fields_.push_back(field); }
    void setField(size_t idx, const Field& field);

    // Get field
    const Field& getField(size_t idx) const { return fields_[idx]; }
    size_t getFieldCount() const { return fields_.size(); }

    // Serialize to byte array (for storage in page)
    std::vector<uint8_t> serialize() const;

    // Deserialize from byte array
    bool deserialize(const uint8_t* data, size_t size);

    std::string toString() const;

private:
    const Schema* schema_ = nullptr;
    std::vector<Field> fields_;
};

// Tuple ID (TID) - used to locate rows
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
