#include "tuple.h"
#include <cstring>
#include <algorithm>

namespace tinydb {
namespace storage {

// 获取数据类型大小
size_t getTypeSize(DataType type) {
    switch (type) {
        case DataType::INT32: return 4;
        case DataType::INT64: return 8;
        case DataType::FLOAT: return 4;
        case DataType::DOUBLE: return 8;
        case DataType::BOOL: return 1;
        case DataType::NULL_TYPE: return 0;
        default: return 0;
    }
}

// 检查类型是否为定长
bool isFixedLength(DataType type) {
    return type == DataType::INT32 ||
           type == DataType::INT64 ||
           type == DataType::FLOAT ||
           type == DataType::DOUBLE ||
           type == DataType::BOOL;
}

// Helper function for case-insensitive string comparison
static bool caseInsensitiveCompare(const std::string& a, const std::string& b) {
    if (a.length() != b.length()) {
        return false;
    }
    for (size_t i = 0; i < a.length(); ++i) {
        if (std::tolower(a[i]) != std::tolower(b[i])) {
            return false;
        }
    }
    return true;
}

// Schema实现
int Schema::findColumnIndex(const std::string& name) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (caseInsensitiveCompare(columns_[i].name, name)) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

const ColumnDef* Schema::getColumn(const std::string& name) const {
    int idx = findColumnIndex(name);
    if (idx >= 0) {
        return &columns_[idx];
    }
    return nullptr;
}

// 检查列是否存在
bool Schema::columnExists(const std::string& name) const {
    return findColumnIndex(name) >= 0;
}

// 删除列
bool Schema::removeColumn(const std::string& name) {
    int idx = findColumnIndex(name);
    if (idx < 0) {
        return false;
    }
    columns_.erase(columns_.begin() + idx);
    return true;
}

bool Schema::renameColumn(const std::string& old_name, const std::string& new_name) {
    int idx = findColumnIndex(old_name);
    if (idx < 0) {
        return false;
    }
    // Check if new name already exists
    if (findColumnIndex(new_name) >= 0) {
        return false;
    }
    columns_[idx].name = new_name;
    return true;
}

size_t Schema::getRowMaxSize() const {
    size_t size = 0;
    for (const auto& col : columns_) {
        size += col.getStorageSize();
    }
    // 加上NULL位图（每列1位，向上取整到字节）
    size += (columns_.size() + 7) / 8;
    return size;
}

std::vector<uint8_t> Schema::serialize() const {
    std::vector<uint8_t> result;
    // 列数
    uint32_t col_count = columns_.size();
    result.resize(sizeof(col_count));
    std::memcpy(result.data(), &col_count, sizeof(col_count));

    for (const auto& col : columns_) {
        // 列名长度
        uint32_t name_len = col.name.size();
        size_t old_size = result.size();
        result.resize(old_size + sizeof(name_len) + name_len + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint8_t) * 2);

        uint8_t* ptr = result.data() + old_size;
        std::memcpy(ptr, &name_len, sizeof(name_len));
        ptr += sizeof(name_len);
        std::memcpy(ptr, col.name.data(), name_len);
        ptr += name_len;

        uint8_t type_val = static_cast<uint8_t>(col.type);
        std::memcpy(ptr, &type_val, sizeof(type_val));
        ptr += sizeof(type_val);

        std::memcpy(ptr, &col.length, sizeof(col.length));
        ptr += sizeof(col.length);

        uint8_t nullable = col.is_nullable ? 1 : 0;
        std::memcpy(ptr, &nullable, sizeof(nullable));
        ptr += sizeof(nullable);

        uint8_t pk = col.is_primary_key ? 1 : 0;
        std::memcpy(ptr, &pk, sizeof(pk));
    }

    return result;
}

bool Schema::deserialize(const uint8_t* data, size_t size) {
    if (size < sizeof(uint32_t)) return false;

    const uint8_t* ptr = data;
    uint32_t col_count;
    std::memcpy(&col_count, ptr, sizeof(col_count));
    ptr += sizeof(col_count);

    columns_.clear();
    for (uint32_t i = 0; i < col_count; ++i) {
        ColumnDef col;

        uint32_t name_len;
        std::memcpy(&name_len, ptr, sizeof(name_len));
        ptr += sizeof(name_len);

        col.name.assign(reinterpret_cast<const char*>(ptr), name_len);
        ptr += name_len;

        uint8_t type_val;
        std::memcpy(&type_val, ptr, sizeof(type_val));
        col.type = static_cast<DataType>(type_val);
        ptr += sizeof(type_val);

        std::memcpy(&col.length, ptr, sizeof(col.length));
        ptr += sizeof(col.length);

        uint8_t nullable;
        std::memcpy(&nullable, ptr, sizeof(nullable));
        col.is_nullable = (nullable != 0);
        ptr += sizeof(nullable);

        uint8_t pk;
        std::memcpy(&pk, ptr, sizeof(pk));
        col.is_primary_key = (pk != 0);
        ptr += sizeof(pk);

        columns_.push_back(col);
    }

    return true;
}

// Field实现
Field::Field(const std::string& val, DataType type) : type_(type), is_null_(false) {
    if (type == DataType::CHAR || type == DataType::VARCHAR) {
        data_ = val;
    }
}

int32_t Field::getInt32() const {
    if (type_ != DataType::INT32 || is_null_ || data_.size() != 4) {
        return 0;
    }
    int32_t val;
    std::memcpy(&val, data_.data(), sizeof(val));
    return val;
}

int64_t Field::getInt64() const {
    if (type_ != DataType::INT64 || is_null_ || data_.size() != 8) {
        return 0;
    }
    int64_t val;
    std::memcpy(&val, data_.data(), sizeof(val));
    return val;
}

float Field::getFloat() const {
    if (type_ != DataType::FLOAT || is_null_ || data_.size() != 4) {
        return 0.0f;
    }
    float val;
    std::memcpy(&val, data_.data(), sizeof(val));
    return val;
}

double Field::getDouble() const {
    if (type_ != DataType::DOUBLE || is_null_ || data_.size() != 8) {
        return 0.0;
    }
    double val;
    std::memcpy(&val, data_.data(), sizeof(val));
    return val;
}

bool Field::getBool() const {
    if (type_ != DataType::BOOL || is_null_ || data_.size() != 1) {
        return false;
    }
    return data_[0] != 0;
}

std::string Field::getString() const {
    if (is_null_) return "NULL";
    if (type_ == DataType::CHAR || type_ == DataType::VARCHAR) {
        return data_;
    }
    return "";
}

std::vector<uint8_t> Field::serialize() const {
    std::vector<uint8_t> result;

    // 类型
    result.push_back(static_cast<uint8_t>(type_));

    // NULL标志
    result.push_back(is_null_ ? 1 : 0);

    if (!is_null_) {
        // 数据长度
        uint32_t len = data_.size();
        result.resize(result.size() + sizeof(len));
        std::memcpy(result.data() + 2, &len, sizeof(len));

        // 数据
        size_t old_size = result.size();
        result.resize(old_size + len);
        std::memcpy(result.data() + old_size, data_.data(), len);
    }

    return result;
}

Field Field::deserialize(const uint8_t* data, size_t size, DataType expected_type) {
    if (size < 2) return Field();

    const uint8_t* ptr = data;
    DataType type = static_cast<DataType>(*ptr++);
    bool is_null = (*ptr++ != 0);

    Field field;
    field.type_ = type;
    field.is_null_ = is_null;

    if (!is_null && size >= 6) {
        uint32_t len;
        std::memcpy(&len, ptr, sizeof(len));
        ptr += sizeof(len);
        field.data_.assign(reinterpret_cast<const char*>(ptr), len);
    }

    return field;
}

std::string Field::toString() const {
    if (is_null_) return "NULL";

    switch (type_) {
        case DataType::INT32: return std::to_string(getInt32());
        case DataType::INT64: return std::to_string(getInt64());
        case DataType::FLOAT: return std::to_string(getFloat());
        case DataType::DOUBLE: return std::to_string(getDouble());
        case DataType::BOOL: return getBool() ? "true" : "false";
        case DataType::CHAR:
        case DataType::VARCHAR: return "'" + getString() + "'";
        default: return "?";
    }
}

// Tuple实现
void Tuple::setField(size_t idx, const Field& field) {
    if (idx >= fields_.size()) {
        fields_.resize(idx + 1);
    }
    fields_[idx] = field;
}

std::vector<uint8_t> Tuple::serialize() const {
    std::vector<uint8_t> result;

    if (!schema_ || fields_.empty()) {
        return result;
    }

    size_t col_count = schema_->getColumnCount();

    // NULL位图
    size_t null_bitmap_size = (col_count + 7) / 8;
    std::vector<uint8_t> null_bitmap(null_bitmap_size, 0);

    for (size_t i = 0; i < fields_.size() && i < col_count; ++i) {
        if (fields_[i].isNull()) {
            null_bitmap[i / 8] |= (1 << (i % 8));
        }
    }

    // 写入NULL位图
    result.insert(result.end(), null_bitmap.begin(), null_bitmap.end());

    // 写入字段数据
    for (size_t i = 0; i < fields_.size() && i < col_count; ++i) {
        if (!fields_[i].isNull()) {
            auto field_data = fields_[i].serialize();
            // 跳过类型和NULL标志（前2字节），只写长度和数据
            if (field_data.size() > 6) {
                result.insert(result.end(), field_data.begin() + 2, field_data.end());
            }
        } else {
            // NULL值只写类型和NULL标志
            result.push_back(static_cast<uint8_t>(schema_->getColumn(i).type));
            result.push_back(1);
        }
    }

    return result;
}

bool Tuple::deserialize(const uint8_t* data, size_t size) {
    if (!schema_ || size == 0) return false;

    const uint8_t* ptr = data;
    size_t col_count = schema_->getColumnCount();
    size_t null_bitmap_size = (col_count + 7) / 8;

    if (size < null_bitmap_size) return false;

    // 读取NULL位图
    std::vector<uint8_t> null_bitmap(ptr, ptr + null_bitmap_size);
    ptr += null_bitmap_size;

    fields_.clear();

    for (size_t i = 0; i < col_count; ++i) {
        bool is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;
        DataType type = schema_->getColumn(i).type;

        if (is_null) {
            fields_.emplace_back();
        } else {
            // 读取字段：长度(4字节) + 数据
            if (static_cast<size_t>(ptr - data) + 4 > size) return false;

            uint32_t len;
            std::memcpy(&len, ptr, sizeof(len));
            ptr += sizeof(len);

            if (static_cast<size_t>(ptr - data) + len > size) return false;

            // 构造临时序列化数据用于反序列化
            std::vector<uint8_t> field_data;
            field_data.push_back(static_cast<uint8_t>(type));
            field_data.push_back(0);  // not null
            field_data.resize(2 + sizeof(len) + len);
            std::memcpy(field_data.data() + 2, &len, sizeof(len));
            std::memcpy(field_data.data() + 6, ptr, len);
            ptr += len;

            fields_.push_back(Field::deserialize(field_data.data(), field_data.size(), type));
        }
    }

    return true;
}

std::string Tuple::toString() const {
    std::string result = "(";
    for (size_t i = 0; i < fields_.size(); ++i) {
        if (i > 0) result += ", ";
        result += fields_[i].toString();
    }
    result += ")";
    return result;
}

} // namespace storage
} // namespace tinydb
