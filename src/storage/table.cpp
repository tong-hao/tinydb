#include "table.h"
#include "common/logger.h"
#include <cstring>

namespace tinydb {
namespace storage {

// TableMeta序列化
std::vector<uint8_t> TableMeta::serialize() const {
    std::vector<uint8_t> result;

    // table_id (4)
    result.resize(4);
    std::memcpy(result.data(), &table_id, 4);

    // table_name长度 + table_name
    uint32_t name_len = table_name.size();
    result.resize(8);
    std::memcpy(result.data() + 4, &name_len, 4);
    result.insert(result.end(), table_name.begin(), table_name.end());

    // schema_name长度 + schema_name
    uint32_t schema_len = schema_name.size();
    size_t offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &schema_len, 4);
    result.insert(result.end(), schema_name.begin(), schema_name.end());

    // first_page_id (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &first_page_id, 4);

    // last_page_id (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &last_page_id, 4);

    // tuple_count (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &tuple_count, 4);

    // page_count (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &page_count, 4);

    // next_tuple_id (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &next_tuple_id, 4);

    // schema
    auto schema_data = schema.serialize();
    uint32_t schema_data_len = schema_data.size();
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &schema_data_len, 4);
    result.insert(result.end(), schema_data.begin(), schema_data.end());

    return result;
}

bool TableMeta::deserialize(const uint8_t* data, size_t size) {
    if (size < 4) return false;
    const uint8_t* ptr = data;

    // table_id
    std::memcpy(&table_id, ptr, 4);
    ptr += 4;

    // table_name
    uint32_t name_len;
    std::memcpy(&name_len, ptr, 4);
    ptr += 4;
    if (static_cast<size_t>(ptr - data) + name_len > size) return false;
    table_name.assign(reinterpret_cast<const char*>(ptr), name_len);
    ptr += name_len;

    // schema_name
    uint32_t schema_len;
    std::memcpy(&schema_len, ptr, 4);
    ptr += 4;
    if (static_cast<size_t>(ptr - data) + schema_len > size) return false;
    schema_name.assign(reinterpret_cast<const char*>(ptr), schema_len);
    ptr += schema_len;

    // first_page_id
    std::memcpy(&first_page_id, ptr, 4);
    ptr += 4;

    // last_page_id
    std::memcpy(&last_page_id, ptr, 4);
    ptr += 4;

    // tuple_count
    std::memcpy(&tuple_count, ptr, 4);
    ptr += 4;

    // page_count
    std::memcpy(&page_count, ptr, 4);
    ptr += 4;

    // next_tuple_id
    std::memcpy(&next_tuple_id, ptr, 4);
    ptr += 4;

    // schema
    uint32_t schema_data_len;
    std::memcpy(&schema_data_len, ptr, 4);
    ptr += 4;
    if (static_cast<size_t>(ptr - data) + schema_data_len > size) return false;
    schema.deserialize(ptr, schema_data_len);

    return true;
}

// ColumnMeta实现
ColumnMeta::ColumnMeta(const ColumnDef& def)
    : column_name(def.name), data_type(def.type), type_length(def.length),
      is_nullable(def.is_nullable), is_primary_key(def.is_primary_key) {}

std::vector<uint8_t> ColumnMeta::serialize() const {
    std::vector<uint8_t> result;

    // table_id (4)
    result.resize(4);
    std::memcpy(result.data(), &table_id, 4);

    // column_name长度 + column_name
    uint32_t name_len = column_name.size();
    result.resize(8);
    std::memcpy(result.data() + 4, &name_len, 4);
    result.insert(result.end(), column_name.begin(), column_name.end());

    // column_id (2)
    size_t offset = result.size();
    result.resize(offset + 2);
    std::memcpy(result.data() + offset, &column_id, 2);

    // data_type (1)
    result.push_back(static_cast<uint8_t>(data_type));

    // type_length (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &type_length, 4);

    // is_nullable (1)
    result.push_back(is_nullable ? 1 : 0);

    // is_primary_key (1)
    result.push_back(is_primary_key ? 1 : 0);

    // default_value_offset (4)
    offset = result.size();
    result.resize(offset + 4);
    std::memcpy(result.data() + offset, &default_value_offset, 4);

    return result;
}

bool ColumnMeta::deserialize(const uint8_t* data, size_t size) {
    if (size < 4) return false;
    const uint8_t* ptr = data;

    // table_id
    std::memcpy(&table_id, ptr, 4);
    ptr += 4;

    // column_name
    uint32_t name_len;
    std::memcpy(&name_len, ptr, 4);
    ptr += 4;
    if (static_cast<size_t>(ptr - data) + name_len > size) return false;
    column_name.assign(reinterpret_cast<const char*>(ptr), name_len);
    ptr += name_len;

    // column_id
    std::memcpy(&column_id, ptr, 2);
    ptr += 2;

    // data_type
    data_type = static_cast<DataType>(*ptr++);

    // type_length
    std::memcpy(&type_length, ptr, 4);
    ptr += 4;

    // is_nullable
    is_nullable = (*ptr++ != 0);

    // is_primary_key
    is_primary_key = (*ptr++ != 0);

    // default_value_offset
    std::memcpy(&default_value_offset, ptr, 4);

    return true;
}

// TableIterator实现
TableIterator::TableIterator(BufferPoolManager* buffer_pool, PageId start_page, const Schema* schema)
    : buffer_pool_(buffer_pool), schema_(schema), current_page_id_(start_page),
      current_slot_id_(0), has_next_(false) {
    findNextValidTuple();
}

bool TableIterator::hasNext() {
    return has_next_;
}

Tuple TableIterator::getNext() {
    if (!has_next_) {
        return Tuple();
    }

    TID tid(current_page_id_, current_slot_id_);
    current_tid_ = tid;

    // 获取当前元组
    BufferFrame* frame = buffer_pool_->fetchPage(current_page_id_);
    if (!frame) {
        has_next_ = false;
        return Tuple();
    }

    const char* tuple_data = frame->getPage().getTupleData(current_slot_id_);
    uint16_t tuple_len = frame->getPage().getTupleLength(current_slot_id_);

    Tuple tuple(schema_);
    if (tuple_data && tuple_len > 0) {
        tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len);
    }

    buffer_pool_->unpinPage(current_page_id_, false);

    // 移动到下一个
    advance();
    findNextValidTuple();

    return tuple;
}

void TableIterator::advance() {
    current_slot_id_++;
}

bool TableIterator::findNextValidTuple() {
    while (current_page_id_ != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(current_page_id_);
        if (!frame) {
            has_next_ = false;
            return false;
        }

        const Page& page = frame->getPage();
        uint16_t slot_count = page.getSlotCount();

        // 在当前页中查找有效元组
        while (current_slot_id_ < slot_count) {
            const SlotEntry* slot = page.getSlot(current_slot_id_);
            if (slot && slot->isInUse() && !slot->isDeleted()) {
                buffer_pool_->unpinPage(current_page_id_, false);
                has_next_ = true;
                return true;
            }
            current_slot_id_++;
        }

        // 当前页没有更多元组，移动到下一页
        PageId next_page = page.header().next_page_id;
        buffer_pool_->unpinPage(current_page_id_, false);

        current_page_id_ = next_page;
        current_slot_id_ = 0;
    }

    has_next_ = false;
    return false;
}

// TableManager实现
TableManager::TableManager(BufferPoolManager* buffer_pool)
    : buffer_pool_(buffer_pool), next_table_id_(1) {
    loadSystemTables();
}

TableManager::~TableManager() = default;

bool TableManager::loadSystemTables() {
    // 尝试从pg_class加载表
    // 如果pg_class不存在，则初始化系统表

    // 首先检查pg_class是否存在（通过读取第0页的特殊标记）
    // 简化实现：直接尝试初始化
    return initializeSystemTables();
}

bool TableManager::initializeSystemTables() {
    std::lock_guard<std::mutex> lock(mutex_);

    // 创建pg_class的schema
    Schema pg_class_schema;
    pg_class_schema.addColumn("table_id", DataType::INT32);
    pg_class_schema.addColumn("table_name", DataType::VARCHAR, 64);
    pg_class_schema.addColumn("schema_name", DataType::VARCHAR, 64);
    pg_class_schema.addColumn("first_page_id", DataType::INT32);
    pg_class_schema.addColumn("last_page_id", DataType::INT32);
    pg_class_schema.addColumn("tuple_count", DataType::INT32);
    pg_class_schema.addColumn("page_count", DataType::INT32);
    pg_class_schema.addColumn("next_tuple_id", DataType::INT32);
    pg_class_schema.addColumn("schema_data", DataType::VARCHAR, 4096);

    // 创建pg_attribute的schema
    Schema pg_attribute_schema;
    pg_attribute_schema.addColumn("table_id", DataType::INT32);
    pg_attribute_schema.addColumn("column_name", DataType::VARCHAR, 64);
    pg_attribute_schema.addColumn("column_id", DataType::INT32);
    pg_attribute_schema.addColumn("data_type", DataType::INT32);
    pg_attribute_schema.addColumn("type_length", DataType::INT32);
    pg_attribute_schema.addColumn("is_nullable", DataType::BOOL);
    pg_attribute_schema.addColumn("is_primary_key", DataType::BOOL);
    pg_attribute_schema.addColumn("default_value_offset", DataType::INT32);

    // 分配pg_class和pg_attribute的表ID
    pg_class_meta_ = std::make_shared<TableMeta>();
    pg_class_meta_->table_id = 0;  // 系统表ID从0开始
    pg_class_meta_->table_name = "pg_class";
    pg_class_meta_->schema_name = "pg_catalog";
    pg_class_meta_->schema = pg_class_schema;

    pg_attribute_meta_ = std::make_shared<TableMeta>();
    pg_attribute_meta_->table_id = 1;
    pg_attribute_meta_->table_name = "pg_attribute";
    pg_attribute_meta_->schema_name = "pg_catalog";
    pg_attribute_meta_->schema = pg_attribute_schema;

    // 为系统表分配数据页
    PageId pg_class_page;
    BufferFrame* frame = buffer_pool_->newPage(&pg_class_page);
    if (!frame) {
        LOG_ERROR("Failed to allocate page for pg_class");
        return false;
    }
    pg_class_meta_->first_page_id = pg_class_page;
    pg_class_meta_->last_page_id = pg_class_page;
    pg_class_meta_->page_count = 1;
    buffer_pool_->unpinPage(pg_class_page, true);

    PageId pg_attr_page;
    frame = buffer_pool_->newPage(&pg_attr_page);
    if (!frame) {
        LOG_ERROR("Failed to allocate page for pg_attribute");
        return false;
    }
    pg_attribute_meta_->first_page_id = pg_attr_page;
    pg_attribute_meta_->last_page_id = pg_attr_page;
    pg_attribute_meta_->page_count = 1;
    buffer_pool_->unpinPage(pg_attr_page, true);

    // 保存系统表元数据
    tables_["pg_class"] = pg_class_meta_;
    tables_["pg_attribute"] = pg_attribute_meta_;

    next_table_id_ = 2;  // 下一个用户表ID从2开始

    LOG_INFO("System tables initialized");
    return true;
}

bool TableManager::createTable(const std::string& table_name, const Schema& schema) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (tables_.find(table_name) != tables_.end()) {
        LOG_ERROR("Table already exists: " << table_name);
        return false;
    }

    // 分配新页
    PageId first_page;
    BufferFrame* frame = buffer_pool_->newPage(&first_page);
    if (!frame) {
        LOG_ERROR("Failed to allocate page for table: " << table_name);
        return false;
    }
    buffer_pool_->unpinPage(first_page, true);

    // 创建表元数据
    auto meta = std::make_shared<TableMeta>();
    meta->table_id = next_table_id_++;
    meta->table_name = table_name;
    meta->schema_name = "public";
    meta->first_page_id = first_page;
    meta->last_page_id = first_page;
    meta->page_count = 1;
    meta->schema = schema;

    // 保存到pg_class
    if (!saveTableMeta(*meta)) {
        LOG_ERROR("Failed to save table meta to pg_class");
        return false;
    }

    // 保存列信息到pg_attribute
    for (size_t i = 0; i < schema.getColumnCount(); ++i) {
        ColumnMeta col_meta(schema.getColumn(i));
        col_meta.table_id = meta->table_id;
        col_meta.column_id = static_cast<uint16_t>(i);
        if (!saveColumnMeta(col_meta)) {
            LOG_ERROR("Failed to save column meta to pg_attribute");
            return false;
        }
    }

    tables_[table_name] = meta;
    LOG_INFO("Table created: " << table_name << " (id=" << meta->table_id << ")");
    return true;
}

bool TableManager::dropTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        LOG_ERROR("Table not found: " << table_name);
        return false;
    }

    auto meta = it->second;

    // 从pg_class删除
    if (!removeTableMeta(table_name)) {
        LOG_ERROR("Failed to remove table meta from pg_class");
        return false;
    }

    // 从pg_attribute删除列信息
    if (!removeColumnMeta(meta->table_id)) {
        LOG_ERROR("Failed to remove column meta from pg_attribute");
        return false;
    }

    // 释放所有数据页
    PageId page_id = meta->first_page_id;
    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) break;

        PageId next_page = frame->getPage().header().next_page_id;
        buffer_pool_->unpinPage(page_id, false);
        buffer_pool_->deletePage(page_id);

        page_id = next_page;
    }

    tables_.erase(it);
    LOG_INFO("Table dropped: " << table_name);
    return true;
}

std::shared_ptr<TableMeta> TableManager::getTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return it->second;
    }
    return nullptr;
}

bool TableManager::tableExists(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    return tables_.find(table_name) != tables_.end();
}

std::vector<std::string> TableManager::getAllTableNames() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> names;
    for (const auto& pair : tables_) {
        // 只返回用户表，不包括系统表
        if (pair.second->schema_name != "pg_catalog") {
            names.push_back(pair.first);
        }
    }
    return names;
}

TID TableManager::insertTuple(const std::string& table_name, const Tuple& tuple) {
    auto meta = getTable(table_name);
    if (!meta) {
        LOG_ERROR("Table not found: " << table_name);
        return TID();
    }

    // 序列化元组
    auto tuple_data = tuple.serialize();
    if (tuple_data.empty()) {
        LOG_ERROR("Failed to serialize tuple");
        return TID();
    }

    // 查找或创建数据页
    PageId page_id = meta->last_page_id;
    BufferFrame* frame = buffer_pool_->fetchPage(page_id);

    if (!frame) {
        LOG_ERROR("Failed to fetch page " << page_id);
        return TID();
    }

    // 检查页是否有足够空间
    if (!frame->getPage().hasEnoughSpace(static_cast<uint16_t>(tuple_data.size()))) {
        // 当前页已满，分配新页
        buffer_pool_->unpinPage(page_id, false);

        PageId new_page_id;
        frame = buffer_pool_->newPage(&new_page_id);
        if (!frame) {
            LOG_ERROR("Failed to allocate new page");
            return TID();
        }

        // 链接新页
        BufferFrame* last_frame = buffer_pool_->fetchPage(page_id);
        if (last_frame) {
            last_frame->getPage().header().next_page_id = new_page_id;
            buffer_pool_->unpinPage(page_id, true);
        }

        meta->last_page_id = new_page_id;
        meta->page_count++;
        page_id = new_page_id;
    }

    // 插入元组
    uint16_t slot_id = frame->getPage().insertTuple(
        reinterpret_cast<const char*>(tuple_data.data()),
        static_cast<uint16_t>(tuple_data.size())
    );

    buffer_pool_->unpinPage(page_id, true);

    meta->tuple_count++;

    LOG_DEBUG("Inserted tuple at (" << page_id << ", " << slot_id << ")");
    return TID(page_id, slot_id);
}

bool TableManager::deleteTuple(const std::string& table_name, const TID& tid) {
    auto meta = getTable(table_name);
    if (!meta) {
        return false;
    }

    BufferFrame* frame = buffer_pool_->fetchPage(tid.page_id);
    if (!frame) {
        return false;
    }

    bool result = frame->getPage().deleteTuple(tid.slot_id);
    buffer_pool_->unpinPage(tid.page_id, result);

    if (result) {
        meta->tuple_count--;
    }

    return result;
}

Tuple TableManager::getTuple(const std::string& table_name, const TID& tid) {
    auto meta = getTable(table_name);
    if (!meta) {
        return Tuple();
    }

    BufferFrame* frame = buffer_pool_->fetchPage(tid.page_id);
    if (!frame) {
        return Tuple();
    }

    const char* tuple_data = frame->getPage().getTupleData(tid.slot_id);
    uint16_t tuple_len = frame->getPage().getTupleLength(tid.slot_id);

    Tuple tuple(&meta->schema);
    if (tuple_data && tuple_len > 0) {
        tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len);
    }

    buffer_pool_->unpinPage(tid.page_id, false);
    return tuple;
}

TableIterator TableManager::makeIterator(const std::string& table_name) {
    auto meta = getTable(table_name);
    if (!meta) {
        return TableIterator(nullptr, INVALID_PAGE_ID, nullptr);
    }

    return TableIterator(buffer_pool_, meta->first_page_id, &meta->schema);
}

bool TableManager::saveTableMeta(const TableMeta& meta) {
    // 插入到pg_class
    Tuple tuple(&pg_class_meta_->schema);
    auto data = meta.serialize();

    // 构建元组字段
    tuple.addField(Field(static_cast<int32_t>(meta.table_id)));
    tuple.addField(Field(meta.table_name, DataType::VARCHAR));
    tuple.addField(Field(meta.schema_name, DataType::VARCHAR));
    tuple.addField(Field(static_cast<int32_t>(meta.first_page_id)));
    tuple.addField(Field(static_cast<int32_t>(meta.last_page_id)));
    tuple.addField(Field(static_cast<int32_t>(meta.tuple_count)));
    tuple.addField(Field(static_cast<int32_t>(meta.page_count)));
    tuple.addField(Field(static_cast<int32_t>(meta.next_tuple_id)));
    tuple.addField(Field(std::string(reinterpret_cast<const char*>(data.data()), data.size()), DataType::VARCHAR));

    TID tid = insertTuple("pg_class", tuple);
    return tid.isValid();
}

bool TableManager::removeTableMeta(const std::string& table_name) {
    // 遍历pg_class找到并删除对应记录
    TableIterator iter = makeIterator("pg_class");
    while (iter.hasNext()) {
        Tuple tuple = iter.getNext();
        if (tuple.getFieldCount() >= 2) {
            std::string name = tuple.getField(1).getString();
            if (name == table_name) {
                return deleteTuple("pg_class", iter.getCurrentTID());
            }
        }
    }
    return true;
}

bool TableManager::saveColumnMeta(const ColumnMeta& meta) {
    Tuple tuple(&pg_attribute_meta_->schema);

    tuple.addField(Field(static_cast<int32_t>(meta.table_id)));
    tuple.addField(Field(meta.column_name, DataType::VARCHAR));
    tuple.addField(Field(static_cast<int32_t>(meta.column_id)));
    tuple.addField(Field(static_cast<int32_t>(meta.data_type)));
    tuple.addField(Field(static_cast<int32_t>(meta.type_length)));
    tuple.addField(Field(meta.is_nullable));
    tuple.addField(Field(meta.is_primary_key));
    tuple.addField(Field(meta.default_value_offset));

    TID tid = insertTuple("pg_attribute", tuple);
    return tid.isValid();
}

bool TableManager::removeColumnMeta(uint32_t table_id) {
    // 遍历pg_attribute找到并删除对应记录
    TableIterator iter = makeIterator("pg_attribute");
    while (iter.hasNext()) {
        Tuple tuple = iter.getNext();
        if (tuple.getFieldCount() >= 1) {
            int32_t tid = tuple.getField(0).getInt32();
            if (static_cast<uint32_t>(tid) == table_id) {
                deleteTuple("pg_attribute", iter.getCurrentTID());
            }
        }
    }
    return true;
}

std::vector<ColumnMeta> TableManager::loadColumnMeta(uint32_t table_id) {
    std::vector<ColumnMeta> columns;
    TableIterator iter = makeIterator("pg_attribute");

    while (iter.hasNext()) {
        Tuple tuple = iter.getNext();
        if (tuple.getFieldCount() >= 1) {
            int32_t tid = tuple.getField(0).getInt32();
            if (static_cast<uint32_t>(tid) == table_id) {
                ColumnMeta meta;
                meta.table_id = table_id;
                meta.column_name = tuple.getField(1).getString();
                meta.column_id = static_cast<uint16_t>(tuple.getField(2).getInt32());
                meta.data_type = static_cast<DataType>(tuple.getField(3).getInt32());
                meta.type_length = tuple.getField(4).getInt32();
                meta.is_nullable = tuple.getField(5).getBool();
                meta.is_primary_key = tuple.getField(6).getBool();
                meta.default_value_offset = tuple.getField(7).getInt32();
                columns.push_back(meta);
            }
        }
    }

    return columns;
}

} // namespace storage
} // namespace tinydb
