#include "btree_index.h"
#include "buffer_pool.h"
#include <cstring>
#include <algorithm>
#include <queue>

namespace tinydb {
namespace storage {

// ==================== IndexKey ====================

bool IndexKey::operator<(const IndexKey& other) const {
    if (type != other.type) {
        return static_cast<int>(type) < static_cast<int>(other.type);
    }
    switch (type) {
        case Type::INT32:
            return int32_val < other.int32_val;
        case Type::INT64:
            return int64_val < other.int64_val;
        case Type::STRING:
            return string_val < other.string_val;
        case Type::NULL_KEY:
            return false;
    }
    return false;
}

bool IndexKey::operator==(const IndexKey& other) const {
    if (type != other.type) return false;
    switch (type) {
        case Type::INT32:
            return int32_val == other.int32_val;
        case Type::INT64:
            return int64_val == other.int64_val;
        case Type::STRING:
            return string_val == other.string_val;
        case Type::NULL_KEY:
            return true;
    }
    return false;
}

std::vector<uint8_t> IndexKey::serialize() const {
    std::vector<uint8_t> result;
    result.push_back(static_cast<uint8_t>(type));

    switch (type) {
        case Type::INT32:
            result.resize(1 + sizeof(int32_t));
            memcpy(result.data() + 1, &int32_val, sizeof(int32_val));
            break;
        case Type::INT64:
            result.resize(1 + sizeof(int64_t));
            memcpy(result.data() + 1, &int64_val, sizeof(int64_val));
            break;
        case Type::STRING: {
            uint32_t len = string_val.size();
            result.resize(1 + sizeof(len) + len);
            memcpy(result.data() + 1, &len, sizeof(len));
            memcpy(result.data() + 1 + sizeof(len), string_val.data(), len);
            break;
        }
        case Type::NULL_KEY:
            break;
    }
    return result;
}

bool IndexKey::deserialize(const uint8_t* data, size_t size) {
    if (size < 1) return false;
    type = static_cast<Type>(data[0]);

    switch (type) {
        case Type::INT32:
            if (size < 1 + sizeof(int32_t)) return false;
            memcpy(&int32_val, data + 1, sizeof(int32_val));
            break;
        case Type::INT64:
            if (size < 1 + sizeof(int64_t)) return false;
            memcpy(&int64_val, data + 1, sizeof(int64_val));
            break;
        case Type::STRING: {
            if (size < 1 + sizeof(uint32_t)) return false;
            uint32_t len;
            memcpy(&len, data + 1, sizeof(len));
            if (size < 1 + sizeof(len) + len) return false;
            string_val.assign(reinterpret_cast<const char*>(data + 1 + sizeof(len)), len);
            break;
        }
        case Type::NULL_KEY:
            break;
    }
    return true;
}

size_t IndexKey::getSerializedSize() const {
    switch (type) {
        case Type::INT32:
            return 1 + sizeof(int32_t);
        case Type::INT64:
            return 1 + sizeof(int64_t);
        case Type::STRING:
            return 1 + sizeof(uint32_t) + string_val.size();
        case Type::NULL_KEY:
            return 1;
    }
    return 1;
}

IndexKey IndexKey::fromField(const Field& field) {
    switch (field.getType()) {
        case DataType::INT32:
            return IndexKey(field.getInt32());
        case DataType::INT64:
            return IndexKey(field.getInt64());
        case DataType::VARCHAR:
        case DataType::CHAR:
            return IndexKey(field.getString());
        default:
            return IndexKey();
    }
}

Field IndexKey::toField(DataType type) const {
    switch (type) {
        case DataType::INT32:
            return Field((this->type == Type::INT32) ? int32_val : static_cast<int32_t>(int64_val));
        case DataType::INT64:
            return Field((this->type == Type::INT64) ? int64_val : int32_val);
        case DataType::VARCHAR:
        case DataType::CHAR:
            return Field(string_val, type);
        default:
            return Field();
    }
}

// ==================== BTreePage ====================

BTreePage::BTreePage(Page* page) : page_(page) {}

void BTreePage::init(PageId id, BTreePageType type) {
    header().init(id, type);
}

BTreePageHeader& BTreePage::header() {
    return *reinterpret_cast<BTreePageHeader*>(page_->getData());
}

const BTreePageHeader& BTreePage::header() const {
    return *reinterpret_cast<const BTreePageHeader*>(page_->getData());
}

char* BTreePage::getEntryData() {
    return page_->getData() + sizeof(BTreePageHeader);
}

const char* BTreePage::getEntryData() const {
    return page_->getData() + sizeof(BTreePageHeader);
}

PageId BTreePage::getChildAt(int index) const {
    if (!isInternal() || index < 0 || index > header().key_count) {
        return INVALID_PAGE_ID;
    }

    if (index == 0) {
        PageId child_id;
        memcpy(&child_id, getEntryData(), sizeof(PageId));
        return child_id;
    }

    const char* ptr = getEntryData() + sizeof(PageId);
    for (int i = 1; i <= index; ++i) {
        uint32_t key_size;
        memcpy(&key_size, ptr, sizeof(key_size));
        ptr += sizeof(key_size) + key_size;

        PageId child_id;
        memcpy(&child_id, ptr, sizeof(PageId));
        if (i == index) {
            return child_id;
        }
        ptr += sizeof(PageId);
    }
    return INVALID_PAGE_ID;
}

IndexKey BTreePage::getKeyAt(int index) const {
    IndexKey key;
    if (index < 0 || index >= static_cast<int>(header().key_count)) {
        return key;
    }

    const char* ptr = getEntryData();
    if (isInternal()) {
        ptr += sizeof(PageId);
        for (int i = 0; i < index; ++i) {
            uint32_t key_size;
            memcpy(&key_size, ptr, sizeof(key_size));
            ptr += sizeof(key_size) + key_size;
            ptr += sizeof(PageId);
        }
        uint32_t key_size;
        memcpy(&key_size, ptr, sizeof(key_size));
        key.deserialize(reinterpret_cast<const uint8_t*>(ptr + sizeof(key_size)), key_size);
    } else {
        for (int i = 0; i < index; ++i) {
            uint32_t key_size;
            memcpy(&key_size, ptr, sizeof(key_size));
            ptr += sizeof(key_size) + key_size;
            ptr += sizeof(TID);
        }
        uint32_t key_size;
        memcpy(&key_size, ptr, sizeof(key_size));
        key.deserialize(reinterpret_cast<const uint8_t*>(ptr + sizeof(key_size)), key_size);
    }
    return key;
}

TID BTreePage::getTIDAt(int index) const {
    TID tid;
    if (!isLeaf() || index < 0 || index >= static_cast<int>(header().key_count)) {
        return tid;
    }

    const char* ptr = getEntryData();
    for (int i = 0; i < index; ++i) {
        uint32_t key_size;
        memcpy(&key_size, ptr, sizeof(key_size));
        ptr += sizeof(key_size) + key_size;
        ptr += sizeof(TID);
    }
    uint32_t key_size;
    memcpy(&key_size, ptr, sizeof(key_size));
    ptr += sizeof(key_size) + key_size;
    memcpy(&tid, ptr, sizeof(TID));
    return tid;
}

void BTreePage::insertInternalEntry(int index, const InternalEntry& entry) {
    auto key_data = entry.key.serialize();
    uint32_t key_size = key_data.size();
    size_t entry_size = sizeof(key_size) + key_size + sizeof(PageId);

    char* ptr = getEntryData();
    if (header().key_count == 0) {
        memcpy(ptr, &entry.child_page_id, sizeof(PageId));
        ptr += sizeof(PageId);
        memcpy(ptr, &key_size, sizeof(key_size));
        memcpy(ptr + sizeof(key_size), key_data.data(), key_size);
    } else {
        char* insert_pos = ptr + sizeof(PageId);
        for (int i = 0; i < index; ++i) {
            uint32_t ks;
            memcpy(&ks, insert_pos, sizeof(ks));
            insert_pos += sizeof(ks) + ks + sizeof(PageId);
        }

        size_t move_size = header().free_space - (insert_pos - page_->getData());
        memmove(insert_pos + entry_size, insert_pos, move_size);

        memcpy(insert_pos, &key_size, sizeof(key_size));
        memcpy(insert_pos + sizeof(key_size), key_data.data(), key_size);
        memcpy(insert_pos + sizeof(key_size) + key_size, &entry.child_page_id, sizeof(PageId));
    }

    header().key_count++;
    header().free_space += entry_size;
    header().flags |= BTreePageHeader::FLAG_DIRTY;
}

void BTreePage::insertLeafEntry(int index, const LeafEntry& entry) {
    auto key_data = entry.key.serialize();
    uint32_t key_size = key_data.size();
    size_t entry_size = sizeof(key_size) + key_size + sizeof(TID);

    char* ptr = getEntryData();
    char* insert_pos = ptr;
    for (int i = 0; i < index; ++i) {
        uint32_t ks;
        memcpy(&ks, insert_pos, sizeof(ks));
        insert_pos += sizeof(ks) + ks + sizeof(TID);
    }

    size_t move_size = header().free_space - (insert_pos - page_->getData());
    memmove(insert_pos + entry_size, insert_pos, move_size);

    memcpy(insert_pos, &key_size, sizeof(key_size));
    memcpy(insert_pos + sizeof(key_size), key_data.data(), key_size);
    memcpy(insert_pos + sizeof(key_size) + key_size, &entry.tid, sizeof(TID));

    header().key_count++;
    header().free_space += entry_size;
    header().flags |= BTreePageHeader::FLAG_DIRTY;
}

void BTreePage::removeInternalEntry(int index) {
    if (index < 0 || index >= header().key_count) return;

    char* ptr = getEntryData();
    char* remove_pos = ptr + sizeof(PageId);
    for (int i = 0; i < index; ++i) {
        uint32_t ks;
        memcpy(&ks, remove_pos, sizeof(ks));
        remove_pos += sizeof(ks) + ks + sizeof(PageId);
    }

    uint32_t key_size;
    memcpy(&key_size, remove_pos, sizeof(key_size));
    size_t entry_size = sizeof(key_size) + key_size + sizeof(PageId);

    char* next_pos = remove_pos + entry_size;
    size_t move_size = header().free_space - (next_pos - page_->getData());
    memmove(remove_pos, next_pos, move_size);

    header().key_count--;
    header().free_space -= entry_size;
    header().flags |= BTreePageHeader::FLAG_DIRTY;
}

void BTreePage::removeLeafEntry(int index) {
    if (index < 0 || index >= header().key_count) return;

    char* ptr = getEntryData();
    char* remove_pos = ptr;
    for (int i = 0; i < index; ++i) {
        uint32_t ks;
        memcpy(&ks, remove_pos, sizeof(ks));
        remove_pos += sizeof(ks) + ks + sizeof(TID);
    }

    uint32_t key_size;
    memcpy(&key_size, remove_pos, sizeof(key_size));
    size_t entry_size = sizeof(key_size) + key_size + sizeof(TID);

    char* next_pos = remove_pos + entry_size;
    size_t move_size = header().free_space - (next_pos - page_->getData());
    memmove(remove_pos, next_pos, move_size);

    header().key_count--;
    header().free_space -= entry_size;
    header().flags |= BTreePageHeader::FLAG_DIRTY;
}

bool BTreePage::isFull(size_t key_size) const {
    size_t entry_size = sizeof(uint32_t) + key_size + sizeof(TID);
    return header().free_space + entry_size > PAGE_SIZE;
}

bool BTreePage::canBorrow() const {
    return header().key_count > 2;
}

int BTreePage::findKey(const IndexKey& key) const {
    int left = 0;
    int right = header().key_count - 1;

    while (left <= right) {
        int mid = left + (right - left) / 2;
        IndexKey mid_key = getKeyAt(mid);
        if (mid_key == key) {
            return mid;
        } else if (mid_key < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return -1;
}

int BTreePage::findInsertPosition(const IndexKey& key) const {
    int left = 0;
    int right = header().key_count;

    while (left < right) {
        int mid = left + (right - left) / 2;
        IndexKey mid_key = getKeyAt(mid);
        if (mid_key < key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

InternalEntry BTreePage::split(BTreePage& new_page) {
    int mid = header().key_count / 2;
    IndexKey split_key = getKeyAt(mid);

    if (isLeaf()) {
        // 注意：new_page 已经在调用者处初始化，不需要再次 init
        // 只需要复制数据并更新链表指针
        for (int i = mid; i < header().key_count; ++i) {
            IndexKey key = getKeyAt(i);
            TID tid = getTIDAt(i);
            new_page.insertLeafEntry(i - mid, LeafEntry(key, tid));
        }
        // 更新链表：原页面 -> 新页面 -> 原next_page
        new_page.header().next_page_id = header().next_page_id;
        header().next_page_id = new_page.header().page_id;
        header().key_count = mid;
    } else {
        // 注意：new_page 已经在调用者处初始化，不需要再次 init
        // 内部节点分裂：键mid被提升到父节点，键mid+1及之后移到新节点
        // 对于内部节点，有key_count+1个子节点
        // 新节点的第一个子节点是原节点的第mid+1个子节点
        PageId first_child = getChildAt(mid + 1);

        // 手动设置新节点的第一个子节点
        // 注意：new_page是全新的空节点，key_count=0
        // 我们需要模拟insertInternalEntry的行为，但手动控制写入
        new_page.header().free_space = sizeof(BTreePageHeader);
        memcpy(new_page.getEntryData(), &first_child, sizeof(PageId));

        // 从mid+1开始复制键值对
        // 对于每个键i，其对应的右子节点是i+1
        // 第一个键（mid+1）的右子节点是mid+2，这个应该作为第二个子节点
        for (int i = mid + 1; i < header().key_count; ++i) {
            IndexKey key = getKeyAt(i);
            PageId right_child = getChildAt(i + 1);

            // 手动构建entry并追加到free_space位置
            auto key_data = key.serialize();
            uint32_t key_size = key_data.size();
            size_t entry_size = sizeof(key_size) + key_size + sizeof(PageId);

            char* ptr = new_page.getEntryData() + new_page.header().free_space;
            memcpy(ptr, &key_size, sizeof(key_size));
            memcpy(ptr + sizeof(key_size), key_data.data(), key_size);
            memcpy(ptr + sizeof(key_size) + key_size, &right_child, sizeof(PageId));

            new_page.header().key_count++;
            new_page.header().free_space += entry_size;
        }
        header().key_count = mid;
    }

    header().flags |= BTreePageHeader::FLAG_DIRTY;
    new_page.header().flags |= BTreePageHeader::FLAG_DIRTY;

    return InternalEntry(split_key, new_page.header().page_id);
}

void BTreePage::merge(BTreePage& sibling) {
    if (isLeaf()) {
        for (int i = 0; i < sibling.header().key_count; ++i) {
            IndexKey key = sibling.getKeyAt(i);
            TID tid = sibling.getTIDAt(i);
            insertLeafEntry(header().key_count, LeafEntry(key, tid));
        }
        header().next_page_id = sibling.header().next_page_id;
    } else {
        for (int i = 0; i < sibling.header().key_count; ++i) {
            IndexKey key = sibling.getKeyAt(i);
            PageId child = sibling.getChildAt(i + 1);
            insertInternalEntry(header().key_count, InternalEntry(key, child));
        }
    }
    header().flags |= BTreePageHeader::FLAG_DIRTY;
}

InternalEntry BTreePage::borrowFromEnd() {
    int last_idx = header().key_count - 1;
    IndexKey key = getKeyAt(last_idx);
    PageId child = getChildAt(last_idx + 1);
    removeInternalEntry(last_idx);
    return InternalEntry(key, child);
}

InternalEntry BTreePage::borrowFromFront() {
    IndexKey key = getKeyAt(0);
    PageId child = getChildAt(1);
    removeInternalEntry(0);
    return InternalEntry(key, child);
}

size_t BTreePage::calculateEntrySize(const IndexKey& key) const {
    return sizeof(uint32_t) + key.getSerializedSize() + (isLeaf() ? sizeof(TID) : sizeof(PageId));
}

// ==================== IndexMeta ====================

std::vector<uint8_t> IndexMeta::serialize() const {
    std::vector<uint8_t> result;

    result.resize(result.size() + sizeof(index_id));
    memcpy(result.data() + result.size() - sizeof(index_id), &index_id, sizeof(index_id));

    uint32_t name_len = index_name.size();
    result.resize(result.size() + sizeof(name_len));
    memcpy(result.data() + result.size() - sizeof(name_len), &name_len, sizeof(name_len));
    result.insert(result.end(), index_name.begin(), index_name.end());

    result.resize(result.size() + sizeof(table_id));
    memcpy(result.data() + result.size() - sizeof(table_id), &table_id, sizeof(table_id));

    name_len = table_name.size();
    result.resize(result.size() + sizeof(name_len));
    memcpy(result.data() + result.size() - sizeof(name_len), &name_len, sizeof(name_len));
    result.insert(result.end(), table_name.begin(), table_name.end());

    name_len = column_name.size();
    result.resize(result.size() + sizeof(name_len));
    memcpy(result.data() + result.size() - sizeof(name_len), &name_len, sizeof(name_len));
    result.insert(result.end(), column_name.begin(), column_name.end());

    result.resize(result.size() + sizeof(column_id));
    memcpy(result.data() + result.size() - sizeof(column_id), &column_id, sizeof(column_id));

    result.resize(result.size() + sizeof(root_page_id));
    memcpy(result.data() + result.size() - sizeof(root_page_id), &root_page_id, sizeof(root_page_id));

    result.push_back(is_unique ? 1 : 0);

    result.resize(result.size() + sizeof(key_count));
    memcpy(result.data() + result.size() - sizeof(key_count), &key_count, sizeof(key_count));

    return result;
}

bool IndexMeta::deserialize(const uint8_t* data, size_t size) {
    size_t pos = 0;

    if (pos + sizeof(index_id) > size) return false;
    memcpy(&index_id, data + pos, sizeof(index_id));
    pos += sizeof(index_id);

    if (pos + sizeof(uint32_t) > size) return false;
    uint32_t name_len;
    memcpy(&name_len, data + pos, sizeof(name_len));
    pos += sizeof(name_len);
    if (pos + name_len > size) return false;
    index_name.assign(reinterpret_cast<const char*>(data + pos), name_len);
    pos += name_len;

    if (pos + sizeof(table_id) > size) return false;
    memcpy(&table_id, data + pos, sizeof(table_id));
    pos += sizeof(table_id);

    if (pos + sizeof(uint32_t) > size) return false;
    memcpy(&name_len, data + pos, sizeof(name_len));
    pos += sizeof(name_len);
    if (pos + name_len > size) return false;
    table_name.assign(reinterpret_cast<const char*>(data + pos), name_len);
    pos += name_len;

    if (pos + sizeof(uint32_t) > size) return false;
    memcpy(&name_len, data + pos, sizeof(name_len));
    pos += sizeof(name_len);
    if (pos + name_len > size) return false;
    column_name.assign(reinterpret_cast<const char*>(data + pos), name_len);
    pos += name_len;

    if (pos + sizeof(column_id) > size) return false;
    memcpy(&column_id, data + pos, sizeof(column_id));
    pos += sizeof(column_id);

    if (pos + sizeof(root_page_id) > size) return false;
    memcpy(&root_page_id, data + pos, sizeof(root_page_id));
    pos += sizeof(root_page_id);

    if (pos + 1 > size) return false;
    is_unique = (data[pos] != 0);
    pos += 1;

    if (pos + sizeof(key_count) > size) return false;
    memcpy(&key_count, data + pos, sizeof(key_count));

    return true;
}

// ==================== BTreeIndex ====================

BTreeIndex::BTreeIndex(BufferPoolManager* buffer_pool, const IndexMeta& meta)
    : buffer_pool_(buffer_pool), meta_(meta) {}

BTreeIndex::~BTreeIndex() = default;

bool BTreeIndex::initialize() {
    if (!buffer_pool_) return false;

    PageId page_id;
    BufferFrame* frame = buffer_pool_->newPage(&page_id);
    if (!frame) return false;

    Page& page = frame->getPage();
    BTreePage btree_page(&page);
    btree_page.init(page_id, BTreePageType::LEAF_NODE);
    btree_page.setRoot(true);

    meta_.root_page_id = page_id;
    buffer_pool_->unpinPage(page_id, true);
    return true;
}

bool BTreeIndex::loadFromMeta() {
    if (!buffer_pool_ || meta_.root_page_id == INVALID_PAGE_ID) {
        return false;
    }

    BufferFrame* frame = buffer_pool_->fetchPage(meta_.root_page_id);
    if (!frame) return false;

    buffer_pool_->unpinPage(meta_.root_page_id, false);
    return true;
}

bool BTreeIndex::insert(const IndexKey& key, const TID& tid) {
    if (!buffer_pool_ || meta_.root_page_id == INVALID_PAGE_ID) {
        return false;
    }

    IndexKey split_key;
    PageId split_page_id;
    bool need_split = false;

    bool success = insertInternal(meta_.root_page_id, key, tid,
                                   split_key, split_page_id, need_split);

    if (need_split) {
        splitRoot(split_key, split_page_id);
    }

    return success;
}

bool BTreeIndex::insertInternal(PageId page_id, const IndexKey& key, const TID& tid,
                                 IndexKey& split_key, PageId& split_page_id, bool& need_split) {
    BufferFrame* frame = buffer_pool_->fetchPage(page_id);
    if (!frame) return false;

    Page& page = frame->getPage();
    BTreePage btree_page(&page);

    if (btree_page.isLeaf()) {
        if (meta_.is_unique) {
            int existing_index = btree_page.findKey(key);
            if (existing_index >= 0) {
                buffer_pool_->unpinPage(page_id, false);
                return false;
            }
        }

        if (btree_page.isFull(key.getSerializedSize())) {
            PageId new_page_id;
            BufferFrame* new_frame = buffer_pool_->newPage(&new_page_id);
            if (!new_frame) {
                buffer_pool_->unpinPage(page_id, false);
                return false;
            }

            Page& new_page = new_frame->getPage();
            BTreePage new_btree_page(&new_page);
            new_btree_page.init(new_page_id, BTreePageType::LEAF_NODE);
            new_btree_page.setRoot(false);

            InternalEntry split_entry = btree_page.split(new_btree_page);
            split_key = split_entry.key;
            split_page_id = split_entry.child_page_id;
            need_split = true;

            if (key < split_key) {
                int index = btree_page.findInsertPosition(key);
                btree_page.insertLeafEntry(index, LeafEntry(key, tid));
            } else {
                int index = new_btree_page.findInsertPosition(key);
                new_btree_page.insertLeafEntry(index, LeafEntry(key, tid));
            }

            buffer_pool_->unpinPage(page_id, true);
            buffer_pool_->unpinPage(split_page_id, true);
        } else {
            int index = btree_page.findInsertPosition(key);
            btree_page.insertLeafEntry(index, LeafEntry(key, tid));
            meta_.key_count++;
            buffer_pool_->unpinPage(page_id, true);
        }
    } else {
        int child_index = btree_page.findInsertPosition(key);
        PageId child_id = btree_page.getChildAt(child_index);

        buffer_pool_->unpinPage(page_id, false);

        IndexKey child_split_key;
        PageId child_split_page_id;
        bool child_need_split = false;

        bool success = insertInternal(child_id, key, tid, child_split_key, child_split_page_id, child_need_split);

        if (child_need_split) {
            frame = buffer_pool_->fetchPage(page_id);
            if (!frame) return false;
            Page& page = frame->getPage();
            BTreePage btree_page(&page);

            if (btree_page.isFull(child_split_key.getSerializedSize())) {
                PageId new_page_id;
                BufferFrame* new_frame = buffer_pool_->newPage(&new_page_id);
                if (!new_frame) {
                    buffer_pool_->unpinPage(page_id, false);
                    return false;
                }

                Page& new_page = new_frame->getPage();
                BTreePage new_btree_page(&new_page);
                new_btree_page.init(new_page_id, BTreePageType::INTERNAL_NODE);
                new_btree_page.setRoot(false);

                InternalEntry split_entry = btree_page.split(new_btree_page);
                split_key = split_entry.key;
                split_page_id = split_entry.child_page_id;
                need_split = true;

                buffer_pool_->unpinPage(page_id, true);
                buffer_pool_->unpinPage(split_page_id, true);
            } else {
                int insert_pos = btree_page.findInsertPosition(child_split_key);
                btree_page.insertInternalEntry(insert_pos, InternalEntry(child_split_key, child_split_page_id));
                buffer_pool_->unpinPage(page_id, true);
            }
        }
    }

    return true;
}

void BTreeIndex::splitRoot(const IndexKey& split_key, PageId split_page_id) {
    PageId new_root_id;
    BufferFrame* new_root_frame = buffer_pool_->newPage(&new_root_id);
    if (!new_root_frame) return;

    Page& new_root_page = new_root_frame->getPage();
    BTreePage new_root(&new_root_page);
    new_root.init(new_root_id, BTreePageType::INTERNAL_NODE);
    new_root.setRoot(true);

    // Mark old root as no longer root
    BufferFrame* old_root_frame = buffer_pool_->fetchPage(meta_.root_page_id);
    if (old_root_frame) {
        Page& old_root_page = old_root_frame->getPage();
        BTreePage old_root(&old_root_page);
        old_root.setRoot(false);
        buffer_pool_->unpinPage(meta_.root_page_id, true);
    }

    // Build the new root manually:
    // [old_root_page_id][key_size][split_key][split_page_id]
    char* ptr = new_root.getEntryData();

    // First child: old root
    memcpy(ptr, &meta_.root_page_id, sizeof(PageId));
    ptr += sizeof(PageId);

    // First key: split_key, followed by second child: split_page_id
    auto key_data = split_key.serialize();
    uint32_t key_size = key_data.size();
    memcpy(ptr, &key_size, sizeof(key_size));
    memcpy(ptr + sizeof(key_size), key_data.data(), key_size);
    memcpy(ptr + sizeof(key_size) + key_size, &split_page_id, sizeof(PageId));

    // Update header
    new_root.header().key_count = 1;
    new_root.header().free_space = sizeof(BTreePageHeader) + sizeof(PageId) + sizeof(key_size) + key_size + sizeof(PageId);
    new_root.header().flags |= BTreePageHeader::FLAG_DIRTY;

    meta_.root_page_id = new_root_id;
    buffer_pool_->unpinPage(new_root_id, true);
}

PageId BTreeIndex::findLeafPage(const IndexKey& key) {
    PageId page_id = meta_.root_page_id;

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) return INVALID_PAGE_ID;

        Page& page = frame->getPage();
        BTreePage btree_page(&page);

        if (btree_page.isLeaf()) {
            buffer_pool_->unpinPage(page_id, false);
            return page_id;
        }

        int index = btree_page.findInsertPosition(key);
        PageId next_page = btree_page.getChildAt(index);

        buffer_pool_->unpinPage(page_id, false);
        page_id = next_page;
    }

    return INVALID_PAGE_ID;
}

bool BTreeIndex::remove(const IndexKey& key, const TID& tid) {
    if (!buffer_pool_ || meta_.root_page_id == INVALID_PAGE_ID) {
        return false;
    }

    PageId leaf_id = findLeafPage(key);
    if (leaf_id == INVALID_PAGE_ID) return false;

    BufferFrame* frame = buffer_pool_->fetchPage(leaf_id);
    if (!frame) return false;

    Page& page = frame->getPage();
    BTreePage btree_page(&page);

    int index = btree_page.findKey(key);
    if (index < 0) {
        buffer_pool_->unpinPage(leaf_id, false);
        return false;
    }

    btree_page.removeLeafEntry(index);
    meta_.key_count--;
    buffer_pool_->unpinPage(leaf_id, true);
    return true;
}

std::vector<TID> BTreeIndex::lookup(const IndexKey& key) {
    std::vector<TID> result;

    if (!buffer_pool_ || meta_.root_page_id == INVALID_PAGE_ID) {
        return result;
    }

    PageId leaf_id = findLeafPage(key);
    if (leaf_id == INVALID_PAGE_ID) return result;

    BufferFrame* frame = buffer_pool_->fetchPage(leaf_id);
    if (!frame) return result;

    Page& page = frame->getPage();
    BTreePage btree_page(&page);

    // 找到键的位置
    int index = btree_page.findKey(key);
    if (index >= 0) {
        // 收集所有匹配的键（处理重复键）
        // 先向前扫描
        int i = index;
        while (i >= 0 && btree_page.getKeyAt(i) == key) {
            result.push_back(btree_page.getTIDAt(i));
            i--;
        }
        // 再向后扫描
        i = index + 1;
        while (i < btree_page.getKeyCount() && btree_page.getKeyAt(i) == key) {
            result.push_back(btree_page.getTIDAt(i));
            i++;
        }
    }

    buffer_pool_->unpinPage(leaf_id, false);
    return result;
}

std::vector<TID> BTreeIndex::rangeQuery(const IndexKey& start_key, const IndexKey& end_key) {
    std::vector<TID> result;

    if (!buffer_pool_ || meta_.root_page_id == INVALID_PAGE_ID) {
        return result;
    }

    PageId page_id = findLeafPage(start_key);

    while (page_id != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(page_id);
        if (!frame) break;

        Page& page = frame->getPage();
        BTreePage btree_page(&page);

        for (int i = 0; i < btree_page.getKeyCount(); ++i) {
            IndexKey key = btree_page.getKeyAt(i);
            if (key < start_key) continue;
            if (end_key < key) {
                buffer_pool_->unpinPage(page_id, false);
                return result;
            }
            result.push_back(btree_page.getTIDAt(i));
        }

        PageId next_page = btree_page.header().next_page_id;
        buffer_pool_->unpinPage(page_id, false);
        page_id = next_page;
    }

    return result;
}

size_t BTreeIndex::getKeySize() const {
    return 0;
}

// ==================== IndexScanIterator ====================

IndexScanIterator::IndexScanIterator(BufferPoolManager* buffer_pool, PageId start_page,
                                      const IndexKey* start_key, const IndexKey* end_key)
    : buffer_pool_(buffer_pool), current_page_id_(start_page),
      current_entry_index_(-1), has_next_(false) {
    if (start_key) start_key_ = *start_key;
    if (end_key) end_key_ = *end_key;
    advance();
}

bool IndexScanIterator::hasNext() {
    return has_next_;
}

TID IndexScanIterator::next() {
    if (!has_next_) return TID();

    BufferFrame* frame = buffer_pool_->fetchPage(current_page_id_);
    if (!frame) return TID();

    Page& page = frame->getPage();
    BTreePage btree_page(&page);
    TID result = btree_page.getTIDAt(current_entry_index_);
    buffer_pool_->unpinPage(current_page_id_, false);

    advance();
    return result;
}

void IndexScanIterator::advance() {
    has_next_ = false;

    while (current_page_id_ != INVALID_PAGE_ID) {
        BufferFrame* frame = buffer_pool_->fetchPage(current_page_id_);
        if (!frame) return;

        Page& page = frame->getPage();
        BTreePage btree_page(&page);

        current_entry_index_++;

        if (current_entry_index_ < btree_page.getKeyCount()) {
            IndexKey key = btree_page.getKeyAt(current_entry_index_);

            if (end_key_.has_value() && end_key_.value() < key) {
                buffer_pool_->unpinPage(current_page_id_, false);
                has_next_ = false;
                return;
            }

            if (!start_key_.has_value() || !(key < start_key_.value())) {
                has_next_ = true;
                buffer_pool_->unpinPage(current_page_id_, false);
                return;
            }
        } else {
            PageId next_page = btree_page.header().next_page_id;
            buffer_pool_->unpinPage(current_page_id_, false);
            current_page_id_ = next_page;
            current_entry_index_ = -1;
        }
    }
}

} // namespace storage
} // namespace tinydb
