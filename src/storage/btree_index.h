#pragma once

#include "page.h"
#include "tuple.h"
#include <string>
#include <vector>
#include <memory>
#include <optional>

namespace tinydb {
namespace storage {

// 前向声明
class BufferPoolManager;

// 索引键类型（支持多种数据类型）
struct IndexKey {
    enum class Type {
        INT32,
        INT64,
        STRING,
        NULL_KEY
    } type;

    union {
        int32_t int32_val;
        int64_t int64_val;
    };
    std::string string_val;

    IndexKey() : type(Type::NULL_KEY) {}
    explicit IndexKey(int32_t val) : type(Type::INT32) { int32_val = val; }
    explicit IndexKey(int64_t val) : type(Type::INT64) { int64_val = val; }
    explicit IndexKey(const std::string& val) : type(Type::STRING), string_val(val) {}

    // 比较操作符
    bool operator<(const IndexKey& other) const;
    bool operator==(const IndexKey& other) const;
    bool operator!=(const IndexKey& other) const { return !(*this == other); }
    bool operator<=(const IndexKey& other) const { return !(other < *this); }
    bool operator>(const IndexKey& other) const { return other < *this; }
    bool operator>=(const IndexKey& other) const { return !(*this < other); }

    // 序列化/反序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);

    // 获取序列化大小
    size_t getSerializedSize() const;

    // 从Field构造
    static IndexKey fromField(const Field& field);
    Field toField(DataType type) const;
};

// 索引条目：键 -> TID
struct IndexEntry {
    IndexKey key;
    TID tid;

    IndexEntry() = default;
    IndexEntry(IndexKey k, TID t) : key(std::move(k)), tid(t) {}
};

// B+树页类型
enum class BTreePageType {
    INTERNAL_NODE = 0,
    LEAF_NODE = 1
};

// B+树页头（位于索引页的前部）
struct BTreePageHeader {
    uint32_t page_id;
    uint32_t parent_page_id;     // 父节点页号
    uint32_t next_page_id;       // 下一页号（用于叶子节点链表）
    uint16_t key_count;          // 键数量
    uint16_t free_space;         // 空闲空间偏移
    uint8_t page_type;           // 页类型：0=内部节点, 1=叶子节点
    uint8_t is_root;             // 是否是根节点
    uint16_t flags;

    static constexpr uint16_t FLAG_DIRTY = 0x0001;

    void init(PageId id, BTreePageType type) {
        page_id = id;
        parent_page_id = INVALID_PAGE_ID;
        next_page_id = INVALID_PAGE_ID;
        key_count = 0;
        free_space = sizeof(BTreePageHeader);
        page_type = static_cast<uint8_t>(type);
        is_root = 0;
        flags = 0;
    }

    bool isInternal() const { return page_type == 0; }
    bool isLeaf() const { return page_type == 1; }
};

// B+树内部节点条目：键 -> 子页号
struct InternalEntry {
    IndexKey key;
    PageId child_page_id;

    InternalEntry() = default;
    InternalEntry(IndexKey k, PageId p) : key(std::move(k)), child_page_id(p) {}
};

// B+树叶子节点条目：键 -> TID
struct LeafEntry {
    IndexKey key;
    TID tid;

    LeafEntry() = default;
    LeafEntry(IndexKey k, TID t) : key(std::move(k)), tid(t) {}
};

// B+树页（使用Page的data存储）
class BTreePage {
public:
    explicit BTreePage(Page* page);

    // 初始化新页
    void init(PageId id, BTreePageType type);

    // 获取页头
    BTreePageHeader& header();
    const BTreePageHeader& header() const;

    // 页类型
    bool isLeaf() const { return header().isLeaf(); }
    bool isInternal() const { return header().isInternal(); }
    bool isRoot() const { return header().is_root != 0; }
    void setRoot(bool is_root) { header().is_root = is_root ? 1 : 0; }

    // 内部节点操作
    PageId getChildAt(int index) const;
    IndexKey getKeyAt(int index) const;
    void insertInternalEntry(int index, const InternalEntry& entry);
    void removeInternalEntry(int index);

    // 叶子节点操作
    TID getTIDAt(int index) const;
    void insertLeafEntry(int index, const LeafEntry& entry);
    void removeLeafEntry(int index);

    // 通用操作
    int getKeyCount() const { return header().key_count; }
    bool isEmpty() const { return header().key_count == 0; }
    bool isFull(size_t key_size) const;
    bool canBorrow() const;

    int findKey(const IndexKey& key) const;  // 精确查找
    int findInsertPosition(const IndexKey& key) const;  // 查找插入位置

    // 分裂（返回新页和新页的第一个键）
    InternalEntry split(BTreePage& new_page);

    // 合并
    void merge(BTreePage& sibling);

    // 借用（返回借用的键和条目）
    InternalEntry borrowFromEnd();
    InternalEntry borrowFromFront();

    // 获取条目数组起始位置
    char* getEntryData();
    const char* getEntryData() const;

private:
    Page* page_ = nullptr;

    // 计算条目大小
    size_t calculateEntrySize(const IndexKey& key) const;
};

// 索引元数据
struct IndexMeta {
    uint32_t index_id;              // 索引ID
    std::string index_name;         // 索引名
    uint32_t table_id;              // 所属表ID
    std::string table_name;         // 表名
    std::string column_name;        // 列名
    uint32_t column_id;             // 列ID
    PageId root_page_id;            // 根页号
    bool is_unique;                 // 是否唯一索引
    uint32_t key_count;             // 键数量

    IndexMeta() : index_id(0), table_id(0), column_id(0),
                  root_page_id(INVALID_PAGE_ID), is_unique(false), key_count(0) {}

    // 序列化/反序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);
};

// B+树索引类
class BTreeIndex {
public:
    BTreeIndex() = default;
    BTreeIndex(BufferPoolManager* buffer_pool, const IndexMeta& meta);
    ~BTreeIndex();

    // 初始化（创建根节点）
    bool initialize();

    // 从元数据加载
    bool loadFromMeta();

    // 插入键值对
    bool insert(const IndexKey& key, const TID& tid);

    // 删除键值对
    bool remove(const IndexKey& key, const TID& tid);

    // 查找（返回所有匹配的TID）
    std::vector<TID> lookup(const IndexKey& key);

    // 范围查询 [start_key, end_key]
    std::vector<TID> rangeQuery(const IndexKey& start_key, const IndexKey& end_key);

    // 获取元数据
    const IndexMeta& getMeta() const { return meta_; }
    void setMeta(const IndexMeta& meta) { meta_ = meta; }

    // 获取索引统计
    uint32_t getKeyCount() const { return meta_.key_count; }

private:
    BufferPoolManager* buffer_pool_ = nullptr;
    IndexMeta meta_;

    // 内部插入（递归）
    bool insertInternal(PageId page_id, const IndexKey& key, const TID& tid,
                        IndexKey& split_key, PageId& split_page_id, bool& need_split);

    // 查找叶子节点
    PageId findLeafPage(const IndexKey& key);

    // 处理根节点分裂
    void splitRoot(const IndexKey& split_key, PageId split_page_id);

    // 获取键的序列化大小
    size_t getKeySize() const;
};

// 索引扫描迭代器
class IndexScanIterator {
public:
    IndexScanIterator() = default;
    IndexScanIterator(BufferPoolManager* buffer_pool, PageId start_page,
                      const IndexKey* start_key, const IndexKey* end_key);

    // 是否还有下一个
    bool hasNext();

    // 获取下一个TID
    TID next();

private:
    BufferPoolManager* buffer_pool_ = nullptr;
    PageId current_page_id_;
    int current_entry_index_;
    std::optional<IndexKey> start_key_;
    std::optional<IndexKey> end_key_;
    bool has_next_;

    void advance();
};

} // namespace storage
} // namespace tinydb
