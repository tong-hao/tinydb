#include "table.h"
#include "common/logger.h"
#include "common/string_utils.h"
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
    // 尝试从tn_class加载表
    // 如果tn_class不存在，则初始化系统表

    // 首先检查tn_class是否存在（通过读取第0页的特殊标记）
    // 简化实现：直接尝试初始化
    return initializeSystemTables();
}

bool TableManager::initializeSystemTables() {
    std::lock_guard<std::mutex> lock(mutex_);

    // 创建tn_class的schema
    Schema tn_class_schema;
    tn_class_schema.addColumn("table_id", DataType::INT32);
    tn_class_schema.addColumn("table_name", DataType::VARCHAR, 64);
    tn_class_schema.addColumn("schema_name", DataType::VARCHAR, 64);
    tn_class_schema.addColumn("first_page_id", DataType::INT32);
    tn_class_schema.addColumn("last_page_id", DataType::INT32);
    tn_class_schema.addColumn("tuple_count", DataType::INT32);
    tn_class_schema.addColumn("page_count", DataType::INT32);
    tn_class_schema.addColumn("next_tuple_id", DataType::INT32);
    tn_class_schema.addColumn("schema_data", DataType::VARCHAR, 4096);

    // 创建tn_attribute的schema
    Schema tn_attribute_schema;
    tn_attribute_schema.addColumn("table_id", DataType::INT32);
    tn_attribute_schema.addColumn("column_name", DataType::VARCHAR, 64);
    tn_attribute_schema.addColumn("column_id", DataType::INT32);
    tn_attribute_schema.addColumn("data_type", DataType::INT32);
    tn_attribute_schema.addColumn("type_length", DataType::INT32);
    tn_attribute_schema.addColumn("is_nullable", DataType::BOOL);
    tn_attribute_schema.addColumn("is_primary_key", DataType::BOOL);
    tn_attribute_schema.addColumn("default_value_offset", DataType::INT32);

    // 分配tn_class和tn_attribute的表ID
    tn_class_meta_ = std::make_shared<TableMeta>();
    tn_class_meta_->table_id = 0;  // 系统表ID从0开始
    tn_class_meta_->table_name = common::toLower("tn_class");
    tn_class_meta_->schema_name = "tn_catalog";
    tn_class_meta_->schema = tn_class_schema;

    tn_attribute_meta_ = std::make_shared<TableMeta>();
    tn_attribute_meta_->table_id = 1;
    tn_attribute_meta_->table_name = common::toLower("tn_attribute");
    tn_attribute_meta_->schema_name = "tn_catalog";
    tn_attribute_meta_->schema = tn_attribute_schema;

    // 为系统表分配数据页
    PageId tn_class_page;
    BufferFrame* frame = buffer_pool_->newPage(&tn_class_page);
    if (!frame) {
        LOG_ERROR("Failed to allocate page for tn_class");
        return false;
    }
    tn_class_meta_->first_page_id = tn_class_page;
    tn_class_meta_->last_page_id = tn_class_page;
    tn_class_meta_->page_count = 1;
    buffer_pool_->unpinPage(tn_class_page, true);

    PageId tn_attr_page;
    frame = buffer_pool_->newPage(&tn_attr_page);
    if (!frame) {
        LOG_ERROR("Failed to allocate page for tn_attribute");
        return false;
    }
    tn_attribute_meta_->first_page_id = tn_attr_page;
    tn_attribute_meta_->last_page_id = tn_attr_page;
    tn_attribute_meta_->page_count = 1;
    buffer_pool_->unpinPage(tn_attr_page, true);

    // 保存系统表元数据
    tables_["tn_class"] = tn_class_meta_;
    tables_["tn_attribute"] = tn_attribute_meta_;

    next_table_id_ = 2;  // 下一个用户表ID从2开始

    LOG_INFO("System tables initialized");
    return true;
}

bool TableManager::createTable(const std::string& table_name, const Schema& schema) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (tables_.find(common::toLower(table_name)) != tables_.end()) {
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
    meta->table_name = common::toLower(table_name);
    meta->schema_name = "public";
    meta->first_page_id = first_page;
    meta->last_page_id = first_page;
    meta->page_count = 1;
    meta->schema = schema;

    // 保存到tn_class
    if (!saveTableMeta(*meta)) {
        LOG_ERROR("Failed to save table meta to tn_class");
        return false;
    }

    // 保存列信息到tn_attribute
    for (size_t i = 0; i < schema.getColumnCount(); ++i) {
        ColumnMeta col_meta(schema.getColumn(i));
        col_meta.table_id = meta->table_id;
        col_meta.column_id = static_cast<uint16_t>(i);
        if (!saveColumnMeta(col_meta)) {
            LOG_ERROR("Failed to save column meta to tn_attribute");
            return false;
        }
    }

    tables_[common::toLower(table_name)] = meta;
    LOG_INFO("Table created: " << table_name << " (id=" << meta->table_id << ")");
    return true;
}

bool TableManager::dropTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tables_.find(common::toLower(table_name));
    if (it == tables_.end()) {
        LOG_ERROR("Table not found: " << table_name);
        return false;
    }

    auto meta = it->second;

    // 从tn_class删除
    if (!removeTableMeta(table_name)) {
        LOG_ERROR("Failed to remove table meta from tn_class");
        return false;
    }

    // 从tn_attribute删除列信息
    if (!removeColumnMeta(meta->table_id)) {
        LOG_ERROR("Failed to remove column meta from tn_attribute");
        return false;
    }

    // 释放所有数据页
    PageId page_id = meta->first_page_id;
    int page_count = 0;
    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) {
            LOG_ERROR("Failed to fetch page " << page_id << " during drop table");
            break;
        }

        PageId next_page = frame->getPage().header().next_page_id;
        buffer_pool_->unpinPage(page_id, false);
        buffer_pool_->deletePage(page_id);

        page_id = next_page;
        page_count++;
        if (page_count > 1000) {
            LOG_ERROR("Too many pages during drop table, possible infinite loop");
            break;
        }
    }

    tables_.erase(it);
    LOG_INFO("Table dropped: " << table_name);
    return true;
}

std::shared_ptr<TableMeta> TableManager::getTable(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tables_.find(common::toLower(table_name));
    if (it != tables_.end()) {
        return it->second;
    }
    return nullptr;
}

std::shared_ptr<TableMeta> TableManager::getTableUnlocked(const std::string& table_name) {
    auto it = tables_.find(common::toLower(table_name));
    if (it != tables_.end()) {
        return it->second;
    }
    return nullptr;
}

bool TableManager::tableExists(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    return tables_.find(common::toLower(table_name)) != tables_.end();
}

std::vector<std::string> TableManager::getAllTableNames() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> names;
    for (const auto& pair : tables_) {
        // 只返回用户表，不包括系统表
        if (pair.second->schema_name != "tn_catalog") {
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
    // 直接操作 tn_class 而不调用 insertTuple 以避免死锁
    // 序列化元组
    Tuple tuple(&tn_class_meta_->schema);
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

    auto tuple_data = tuple.serialize();
    if (tuple_data.empty()) {
        LOG_ERROR("Failed to serialize table meta tuple");
        return false;
    }

    // 查找或创建数据页
    PageId page_id = tn_class_meta_->last_page_id;
    BufferFrame* frame = buffer_pool_->fetchPage(page_id);

    if (!frame) {
        LOG_ERROR("Failed to fetch page " << page_id);
        return false;
    }

    // 检查页是否有足够空间
    if (!frame->getPage().hasEnoughSpace(static_cast<uint16_t>(tuple_data.size()))) {
        // 当前页已满，分配新页
        buffer_pool_->unpinPage(page_id, false);

        PageId new_page_id;
        frame = buffer_pool_->newPage(&new_page_id);
        if (!frame) {
            LOG_ERROR("Failed to allocate new page");
            return false;
        }

        // 链接新页
        BufferFrame* last_frame = buffer_pool_->fetchPage(page_id);
        if (last_frame) {
            last_frame->getPage().header().next_page_id = new_page_id;
            buffer_pool_->unpinPage(page_id, true);
        }

        tn_class_meta_->last_page_id = new_page_id;
        tn_class_meta_->page_count++;
        page_id = new_page_id;
    }

    // 插入元组
    uint16_t slot_id = frame->getPage().insertTuple(
        reinterpret_cast<const char*>(tuple_data.data()),
        static_cast<uint16_t>(tuple_data.size())
    );

    buffer_pool_->unpinPage(page_id, true);

    tn_class_meta_->tuple_count++;

    LOG_DEBUG("Saved table meta at (" << page_id << ", " << slot_id << ")");
    return true;
}

bool TableManager::removeTableMeta(const std::string& table_name) {
    // 直接遍历 tn_class 找到并删除对应记录，不调用 makeIterator/deleteTuple 以避免死锁
    PageId page_id = tn_class_meta_->first_page_id;
    std::string target_name = common::toLower(table_name);

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) break;

        Page& page = frame->getPage();
        uint16_t slot_count = page.getSlotCount();

        for (uint16_t slot_id = 0; slot_id < slot_count; ++slot_id) {
            // 获取元组数据
            const char* tuple_data = page.getTupleData(slot_id);
            uint16_t tuple_len = page.getTupleLength(slot_id);

            if (tuple_data && tuple_len > 0) {
                // 反序列化元组获取表名（第2个字段，索引1）
                Tuple tuple(&tn_class_meta_->schema);
                if (tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len)) {
                    if (tuple.getFieldCount() >= 2) {
                        std::string name = tuple.getField(1).getString();
                        if (common::toLower(name) == target_name) {
                            // 删除此元组
                            page.deleteTuple(slot_id);
                            buffer_pool_->unpinPage(page_id, true);
                            tn_class_meta_->tuple_count--;
                            LOG_DEBUG("Removed table meta for: " << table_name);
                            return true;
                        }
                    }
                }
            }
        }

        PageId next_page = page.header().next_page_id;
        buffer_pool_->unpinPage(page_id, false);
        page_id = next_page;
    }

    return true;  // 表可能不存在，不报错
}

bool TableManager::saveColumnMeta(const ColumnMeta& meta) {
    // 直接操作 tn_attribute 而不调用 insertTuple 以避免死锁
    Tuple tuple(&tn_attribute_meta_->schema);

    tuple.addField(Field(static_cast<int32_t>(meta.table_id)));
    tuple.addField(Field(meta.column_name, DataType::VARCHAR));
    tuple.addField(Field(static_cast<int32_t>(meta.column_id)));
    tuple.addField(Field(static_cast<int32_t>(meta.data_type)));
    tuple.addField(Field(static_cast<int32_t>(meta.type_length)));
    tuple.addField(Field(meta.is_nullable));
    tuple.addField(Field(meta.is_primary_key));
    tuple.addField(Field(meta.default_value_offset));

    auto tuple_data = tuple.serialize();
    if (tuple_data.empty()) {
        LOG_ERROR("Failed to serialize column meta tuple");
        return false;
    }

    // 查找或创建数据页
    PageId page_id = tn_attribute_meta_->last_page_id;
    BufferFrame* frame = buffer_pool_->fetchPage(page_id);

    if (!frame) {
        LOG_ERROR("Failed to fetch page " << page_id);
        return false;
    }

    // 检查页是否有足够空间
    if (!frame->getPage().hasEnoughSpace(static_cast<uint16_t>(tuple_data.size()))) {
        // 当前页已满，分配新页
        buffer_pool_->unpinPage(page_id, false);

        PageId new_page_id;
        frame = buffer_pool_->newPage(&new_page_id);
        if (!frame) {
            LOG_ERROR("Failed to allocate new page");
            return false;
        }

        // 链接新页
        BufferFrame* last_frame = buffer_pool_->fetchPage(page_id);
        if (last_frame) {
            last_frame->getPage().header().next_page_id = new_page_id;
            buffer_pool_->unpinPage(page_id, true);
        }

        tn_attribute_meta_->last_page_id = new_page_id;
        tn_attribute_meta_->page_count++;
        page_id = new_page_id;
    }

    // 插入元组
    uint16_t slot_id = frame->getPage().insertTuple(
        reinterpret_cast<const char*>(tuple_data.data()),
        static_cast<uint16_t>(tuple_data.size())
    );

    buffer_pool_->unpinPage(page_id, true);

    tn_attribute_meta_->tuple_count++;

    LOG_DEBUG("Saved column meta at (" << page_id << ", " << slot_id << ")");
    return true;
}

bool TableManager::removeColumnMeta(uint32_t table_id) {
    // 直接遍历 tn_attribute 找到并删除对应记录，不调用 makeIterator/deleteTuple 以避免死锁
    PageId page_id = tn_attribute_meta_->first_page_id;

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) break;

        Page& page = frame->getPage();
        uint16_t slot_count = page.getSlotCount();
        bool page_modified = false;

        // 从后向前遍历，这样删除不会影响未遍历的索引
        for (int16_t slot_id = static_cast<int16_t>(slot_count) - 1; slot_id >= 0; --slot_id) {
            // 获取元组数据
            const char* tuple_data = page.getTupleData(static_cast<uint16_t>(slot_id));
            uint16_t tuple_len = page.getTupleLength(static_cast<uint16_t>(slot_id));

            if (tuple_data && tuple_len > 0) {
                // 反序列化元组获取 table_id（第1个字段，索引0）
                Tuple tuple(&tn_attribute_meta_->schema);
                if (tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len)) {
                    if (tuple.getFieldCount() >= 1) {
                        int32_t tid = tuple.getField(0).getInt32();
                        if (static_cast<uint32_t>(tid) == table_id) {
                            // 删除此元组
                            page.deleteTuple(static_cast<uint16_t>(slot_id));
                            page_modified = true;
                            tn_attribute_meta_->tuple_count--;
                        }
                    }
                }
            }
        }

        PageId next_page = page.header().next_page_id;
        buffer_pool_->unpinPage(page_id, page_modified);
        page_id = next_page;
    }

    return true;
}

bool TableManager::updateColumnMeta(uint32_t table_id, const std::string& old_col_name, const ColumnDef& new_def) {
    // 直接遍历 tn_attribute 找到并更新对应记录
    PageId page_id = tn_attribute_meta_->first_page_id;

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) break;

        Page& page = frame->getPage();
        uint16_t slot_count = page.getSlotCount();
        bool page_modified = false;

        for (uint16_t slot_id = 0; slot_id < slot_count; ++slot_id) {
            // 获取元组数据
            const char* tuple_data = page.getTupleData(slot_id);
            uint16_t tuple_len = page.getTupleLength(slot_id);

            if (tuple_data && tuple_len > 0) {
                // 反序列化元组
                Tuple tuple(&tn_attribute_meta_->schema);
                if (tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len)) {
                    if (tuple.getFieldCount() >= 2) {
                        int32_t tid = tuple.getField(0).getInt32();
                        std::string col_name = tuple.getField(1).getString();
                        if (static_cast<uint32_t>(tid) == table_id && col_name == old_col_name) {
                            // 找到记录，更新列名
                            Tuple new_tuple(&tn_attribute_meta_->schema);
                            new_tuple.addField(Field(static_cast<int32_t>(table_id)));
                            new_tuple.addField(Field(new_def.name, DataType::VARCHAR));
                            new_tuple.addField(tuple.getField(2));  // column_id
                            new_tuple.addField(tuple.getField(3));  // data_type
                            new_tuple.addField(tuple.getField(4));  // type_length
                            new_tuple.addField(tuple.getField(5));  // is_nullable
                            new_tuple.addField(tuple.getField(6));  // is_primary_key
                            new_tuple.addField(tuple.getField(7));  // default_value_offset

                            auto new_tuple_data = new_tuple.serialize();
                            if (!new_tuple_data.empty()) {
                                // 删除旧记录
                                page.deleteTuple(slot_id);
                                // 插入新记录
                                page.insertTuple(
                                    reinterpret_cast<const char*>(new_tuple_data.data()),
                                    static_cast<uint16_t>(new_tuple_data.size())
                                );
                                page_modified = true;
                            }
                            buffer_pool_->unpinPage(page_id, page_modified);
                            return true;
                        }
                    }
                }
            }
        }

        PageId next_page = page.header().next_page_id;
        buffer_pool_->unpinPage(page_id, page_modified);
        page_id = next_page;
    }

    return false;
}

std::vector<ColumnMeta> TableManager::loadColumnMeta(uint32_t table_id) {
    std::vector<ColumnMeta> columns;
    TableIterator iter = makeIterator("tn_attribute");

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

// ALTER TABLE: 添加列
bool TableManager::addColumn(const std::string& table_name, const ColumnDef& column_def) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto table_meta = getTableUnlocked(table_name);
    if (!table_meta) {
        LOG_ERROR("Table does not exist: " << table_name);
        return false;
    }

    // 检查列是否已存在
    if (table_meta->schema.columnExists(column_def.name)) {
        LOG_ERROR("Column already exists: " << column_def.name);
        return false;
    }

    // 获取新的列ID
    uint16_t new_column_id = table_meta->schema.getColumnCount();

    // 添加列到schema
    table_meta->schema.addColumn(column_def.name, column_def.type, column_def.length);

    // 保存列元数据到tn_attribute
    ColumnMeta col_meta;
    col_meta.table_id = table_meta->table_id;
    col_meta.column_name = column_def.name;
    col_meta.column_id = new_column_id;
    col_meta.data_type = column_def.type;
    col_meta.type_length = column_def.length;
    col_meta.is_nullable = true;
    col_meta.is_primary_key = false;
    col_meta.default_value_offset = -1;

    if (!saveColumnMeta(col_meta)) {
        LOG_ERROR("Failed to save column metadata for: " << column_def.name);
        return false;
    }

    // 更新表的元数据（schema已改变，需要保存）
    if (!saveTableMeta(*table_meta)) {
        LOG_ERROR("Failed to save table metadata after adding column");
        return false;
    }

    LOG_INFO("Added column " << column_def.name << " to table " << table_name);
    return true;
}

// ALTER TABLE: 删除列
bool TableManager::dropColumn(const std::string& table_name, const std::string& column_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto table_meta = getTableUnlocked(table_name);
    if (!table_meta) {
        LOG_ERROR("Table does not exist: " << table_name);
        return false;
    }

    // 检查列是否存在
    int col_idx = table_meta->schema.findColumnIndex(column_name);
    if (col_idx < 0) {
        LOG_ERROR("Column does not exist: " << column_name);
        return false;
    }

    // 检查是否只有一个列（不允许删除最后一个列）
    if (table_meta->schema.getColumnCount() <= 1) {
        LOG_ERROR("Cannot drop the only column in table");
        return false;
    }

    // 从schema中删除列
    table_meta->schema.removeColumn(column_name);

    // 重新编号剩余的列
    // 删除tn_attribute中的列元数据并重新保存
    if (!removeColumnMeta(table_meta->table_id)) {
        LOG_ERROR("Failed to remove column metadata");
        return false;
    }

    // 重新保存所有列的元数据
    for (size_t i = 0; i < table_meta->schema.getColumnCount(); ++i) {
        const auto& col = table_meta->schema.getColumn(i);
        ColumnMeta col_meta;
        col_meta.table_id = table_meta->table_id;
        col_meta.column_name = col.name;
        col_meta.column_id = static_cast<uint16_t>(i);
        col_meta.data_type = col.type;
        col_meta.type_length = col.length;
        col_meta.is_nullable = true;
        col_meta.is_primary_key = false;
        col_meta.default_value_offset = -1;

        if (!saveColumnMeta(col_meta)) {
            LOG_ERROR("Failed to save column metadata for: " << col.name);
            return false;
        }
    }

    // 更新表的元数据
    if (!saveTableMeta(*table_meta)) {
        LOG_ERROR("Failed to save table metadata after dropping column");
        return false;
    }

    LOG_INFO("Dropped column " << column_name << " from table " << table_name);
    return true;
}

// ALTER TABLE: 修改列
bool TableManager::modifyColumn(const std::string& table_name, const std::string& column_name, const ColumnDef& new_def) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto table_meta = getTableUnlocked(table_name);
    if (!table_meta) {
        LOG_ERROR("Table does not exist: " << table_name);
        return false;
    }

    // 检查列是否存在
    int col_idx = table_meta->schema.findColumnIndex(column_name);
    if (col_idx < 0) {
        LOG_ERROR("Column does not exist: " << column_name);
        return false;
    }

    // 修改列定义（保留列名，修改类型和长度）
    // 注意：这里我们修改的是 schema 中的列定义
    // 由于 schema 使用 vector 存储，我们可以直接修改
    // 但是为了保持接口一致性，我们采用删除后添加的方式

    // 获取原列的属性
    const auto& old_col = table_meta->schema.getColumn(col_idx);

    // 删除旧的列元数据从 tn_attribute
    if (!removeColumnMeta(table_meta->table_id)) {
        LOG_ERROR("Failed to remove old column metadata");
        return false;
    }

    // 修改 schema 中的列定义
    // 由于 ColumnDef 没有直接的修改方法，我们需要重新构建
    // 这里我们采用一种简化的方式：删除后插入新定义

    // 首先收集所有列定义
    std::vector<ColumnDef> new_columns;
    for (size_t i = 0; i < table_meta->schema.getColumnCount(); ++i) {
        const auto& col = table_meta->schema.getColumn(i);
        if (static_cast<int>(i) == col_idx) {
            // 这是要修改的列，使用新定义
            new_columns.emplace_back(column_name, new_def.type, new_def.length, new_def.is_nullable, new_def.is_primary_key);
        } else {
            // 其他列保持不变
            new_columns.emplace_back(col.name, col.type, col.length, col.is_nullable, col.is_primary_key);
        }
    }

    // 清除 schema 并重新添加所有列
    // 由于 Schema 没有 clear 方法，我们需要逐个删除后重新添加
    // 这里我们使用一个技巧：创建新的 Schema 然后替换
    Schema new_schema;
    for (const auto& col : new_columns) {
        new_schema.addColumn(col);
    }
    table_meta->schema = std::move(new_schema);

    // 重新保存所有列的元数据到 tn_attribute
    for (size_t i = 0; i < table_meta->schema.getColumnCount(); ++i) {
        const auto& col = table_meta->schema.getColumn(i);
        ColumnMeta col_meta;
        col_meta.table_id = table_meta->table_id;
        col_meta.column_name = col.name;
        col_meta.column_id = static_cast<uint16_t>(i);
        col_meta.data_type = col.type;
        col_meta.type_length = col.length;
        col_meta.is_nullable = col.is_nullable;
        col_meta.is_primary_key = col.is_primary_key;
        col_meta.default_value_offset = -1;

        if (!saveColumnMeta(col_meta)) {
            LOG_ERROR("Failed to save column metadata for: " << col.name);
            return false;
        }
    }

    // 更新表的元数据
    if (!saveTableMeta(*table_meta)) {
        LOG_ERROR("Failed to save table metadata after modifying column");
        return false;
    }

    LOG_INFO("Modified column " << column_name << " in table " << table_name);
    return true;
}

// ALTER TABLE: 重命名表
bool TableManager::renameTable(const std::string& old_name, const std::string& new_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    // 检查旧表是否存在
    auto table_meta = getTableUnlocked(old_name);
    if (!table_meta) {
        LOG_ERROR("Table does not exist: " << old_name);
        return false;
    }

    // 检查新表名是否已被使用
    if (getTableUnlocked(new_name)) {
        LOG_ERROR("Table already exists: " << new_name);
        return false;
    }

    // 更新表元数据中的表名
    table_meta->table_name = new_name;

    // 更新系统表 tn_class 中的表名
    // 首先删除旧的记录
    if (!removeTableMeta(old_name)) {
        LOG_ERROR("Failed to remove old table metadata");
        return false;
    }

    // 然后添加新的记录
    if (!saveTableMeta(*table_meta)) {
        LOG_ERROR("Failed to save new table metadata");
        return false;
    }

    // 更新内存中的映射
    tables_.erase(common::toLower(old_name));
    tables_[common::toLower(new_name)] = table_meta;

    LOG_INFO("Renamed table " << old_name << " to " << new_name);
    return true;
}

bool TableManager::renameColumn(const std::string& table_name, const std::string& old_col_name, const std::string& new_col_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    // 检查表是否存在
    auto table_meta = getTableUnlocked(table_name);
    if (!table_meta) {
        LOG_ERROR("Table does not exist: " << table_name);
        return false;
    }

    // 检查旧列是否存在
    int col_idx = table_meta->schema.findColumnIndex(old_col_name);
    if (col_idx < 0) {
        LOG_ERROR("Column does not exist: " << old_col_name);
        return false;
    }

    // 检查新列名是否已被使用
    if (table_meta->schema.columnExists(new_col_name)) {
        LOG_ERROR("Column already exists: " << new_col_name);
        return false;
    }

    // 重命名列
    if (!table_meta->schema.renameColumn(old_col_name, new_col_name)) {
        LOG_ERROR("Failed to rename column in schema");
        return false;
    }

    // 获取新列定义
    auto col_def = table_meta->schema.getColumn(new_col_name);
    if (!col_def) {
        LOG_ERROR("Failed to get column definition after rename");
        return false;
    }

    // 更新系统表中的列元数据
    ColumnDef new_def(*col_def);
    if (!updateColumnMeta(table_meta->table_id, old_col_name, new_def)) {
        LOG_ERROR("Failed to update column metadata");
        return false;
    }

    LOG_INFO("Renamed column " << old_col_name << " to " << new_col_name << " in table " << table_name);
    return true;
}

} // namespace storage
} // namespace tinydb
