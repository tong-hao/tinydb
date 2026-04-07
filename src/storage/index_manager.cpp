#include "index_manager.h"
#include "table.h"
#include "common/logger.h"
#include "common/string_utils.h"
#include <algorithm>
#include <cstring>

namespace tinydb {
namespace storage {

IndexManager::IndexManager(BufferPoolManager* buffer_pool)
    : buffer_pool_(buffer_pool), next_index_id_(1) {
    initializeSystemTables();
    loadIndexes();
}

IndexManager::~IndexManager() = default;

bool IndexManager::initializeSystemTables() {
    // tn_index 系统表用于存储索引元数据
    tn_index_meta_ = std::make_shared<TableMeta>();
    tn_index_meta_->table_id = 3;  // tn_class=1, tn_attribute=2, tn_index=3
    tn_index_meta_->table_name = "tn_index";
    tn_index_meta_->schema_name = "tn_catalog";
    tn_index_meta_->first_page_id = INVALID_PAGE_ID;
    tn_index_meta_->last_page_id = INVALID_PAGE_ID;
    tn_index_meta_->page_count = 0;
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

    // 为tn_index分配数据页
    PageId first_page;
    BufferFrame* frame = buffer_pool_->newPage(&first_page);
    if (!frame) {
        LOG_ERROR("Failed to allocate page for tn_index");
        return false;
    }
    tn_index_meta_->first_page_id = first_page;
    tn_index_meta_->last_page_id = first_page;
    tn_index_meta_->page_count = 1;
    buffer_pool_->unpinPage(first_page, true);

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

// Insert语句执行时，这里会在InsertExecutor中被调用
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
    std::lock_guard<std::mutex> lock(mutex_);

    // 遍历 tn_index 系统表加载所有索引
    PageId page_id = tn_index_meta_->first_page_id;
    uint32_t max_index_id = 0;

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) {
            LOG_ERROR("Failed to fetch page " << page_id << " for tn_index");
            break;
        }

        Page& page = frame->getPage();
        uint16_t slot_count = page.getSlotCount();

        for (uint16_t slot_id = 0; slot_id < slot_count; ++slot_id) {
            const SlotEntry* slot = page.getSlot(slot_id);
            if (!slot || !slot->isInUse() || slot->isDeleted()) {
                continue;
            }

            const char* tuple_data = page.getTupleData(slot_id);
            uint16_t tuple_len = page.getTupleLength(slot_id);

            // 反序列化元组
            Tuple tuple(&tn_index_meta_->schema);
            if (!tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len)) {
                LOG_WARN("Failed to deserialize index tuple at (" << page_id << ", " << slot_id << ")");
                continue;
            }

            if (tuple.getFieldCount() < 9) {
                LOG_WARN("Invalid index tuple, field count: " << tuple.getFieldCount());
                continue;
            }

            // 提取索引元数据
            IndexMeta meta;
            meta.index_id = static_cast<uint32_t>(tuple.getField(0).getInt32());
            meta.index_name = tuple.getField(1).getString();
            meta.table_id = static_cast<uint32_t>(tuple.getField(2).getInt32());
            meta.table_name = tuple.getField(3).getString();
            meta.column_name = tuple.getField(4).getString();
            meta.column_id = static_cast<uint32_t>(tuple.getField(5).getInt32());
            meta.root_page_id = static_cast<PageId>(tuple.getField(6).getInt32());
            meta.is_unique = tuple.getField(7).getInt32() != 0;
            meta.key_count = static_cast<uint32_t>(tuple.getField(8).getInt32());

            // 创建B+树索引对象
            auto index = std::make_shared<BTreeIndex>(buffer_pool_, meta);
            if (!index->loadFromMeta()) {
                LOG_ERROR("Failed to load index from meta: " << meta.index_name);
                continue;
            }

            indexes_[meta.index_name] = index;

            // 更新最大索引ID
            if (meta.index_id > max_index_id) {
                max_index_id = meta.index_id;
            }

            LOG_DEBUG("Loaded index: " << meta.index_name << " (id=" << meta.index_id << ")");
        }

        PageId next_page = page.header().next_page_id;
        buffer_pool_->unpinPage(page_id, false);
        page_id = next_page;
    }

    // 设置下一个索引ID
    next_index_id_ = max_index_id + 1;

    LOG_INFO("Loaded " << indexes_.size() << " indexes from tn_index");
    return true;
}

bool IndexManager::saveIndexMeta(const IndexMeta& meta) {
    // 直接操作 tn_index，构建元组并插入
    Tuple tuple(&tn_index_meta_->schema);

    tuple.addField(Field(static_cast<int32_t>(meta.index_id)));
    tuple.addField(Field(meta.index_name, DataType::VARCHAR));
    tuple.addField(Field(static_cast<int32_t>(meta.table_id)));
    tuple.addField(Field(meta.table_name, DataType::VARCHAR));
    tuple.addField(Field(meta.column_name, DataType::VARCHAR));
    tuple.addField(Field(static_cast<int32_t>(meta.column_id)));
    tuple.addField(Field(static_cast<int32_t>(meta.root_page_id)));
    tuple.addField(Field(static_cast<int32_t>(meta.is_unique ? 1 : 0)));
    tuple.addField(Field(static_cast<int32_t>(meta.key_count)));

    auto tuple_data = tuple.serialize();
    if (tuple_data.empty()) {
        LOG_ERROR("Failed to serialize index meta tuple");
        return false;
    }

    // 查找或创建数据页
    PageId page_id = tn_index_meta_->last_page_id;
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
            LOG_ERROR("Failed to allocate new page for tn_index");
            return false;
        }

        // 链接新页
        BufferFrame* last_frame = buffer_pool_->fetchPage(page_id);
        if (last_frame) {
            last_frame->getPage().header().next_page_id = new_page_id;
            buffer_pool_->unpinPage(page_id, true);
        }

        tn_index_meta_->last_page_id = new_page_id;
        tn_index_meta_->page_count++;
        page_id = new_page_id;
    }

    // 插入元组
    uint16_t slot_id = frame->getPage().insertTuple(
        reinterpret_cast<const char*>(tuple_data.data()),
        static_cast<uint16_t>(tuple_data.size())
    );

    buffer_pool_->unpinPage(page_id, true);

    tn_index_meta_->tuple_count++;

    LOG_DEBUG("Saved index meta at (" << page_id << ", " << slot_id << "): " << meta.index_name);
    return true;
}

bool IndexManager::removeIndexMeta(const std::string& index_name) {
    // 直接遍历 tn_index 找到并删除对应记录
    PageId page_id = tn_index_meta_->first_page_id;
    std::string target_name = common::toLower(index_name);

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) break;

        Page& page = frame->getPage();
        uint16_t slot_count = page.getSlotCount();

        for (uint16_t slot_id = 0; slot_id < slot_count; ++slot_id) {
            const char* tuple_data = page.getTupleData(slot_id);
            uint16_t tuple_len = page.getTupleLength(slot_id);

            if (tuple_data && tuple_len > 0) {
                // 反序列化元组获取索引名（第2个字段，索引1）
                Tuple tuple(&tn_index_meta_->schema);
                if (tuple.deserialize(reinterpret_cast<const uint8_t*>(tuple_data), tuple_len)) {
                    if (tuple.getFieldCount() >= 2) {
                        std::string name = tuple.getField(1).getString();
                        if (common::toLower(name) == target_name) {
                            // 删除此元组
                            page.deleteTuple(slot_id);
                            buffer_pool_->unpinPage(page_id, true);
                            tn_index_meta_->tuple_count--;
                            LOG_DEBUG("Removed index meta for: " << index_name);
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

    return true;  // 索引可能不存在，不报错
}

} // namespace storage
} // namespace tinydb
