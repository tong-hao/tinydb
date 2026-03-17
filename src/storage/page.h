#pragma once

#include <cstdint>
#include <cstring>
#include <array>
#include <stdexcept>

namespace tinydb {
namespace storage {

// 页大小常量
constexpr uint32_t PAGE_SIZE = 8192;  // 8KB
constexpr uint32_t MAX_PAGE_COUNT = 0xFFFFFFFF;

// 页号类型
using PageId = uint32_t;
constexpr PageId INVALID_PAGE_ID = 0xFFFFFFFF;

// 页头结构（位于每页的前部）
struct PageHeader {
    uint32_t page_id;           // 页号
    uint32_t next_page_id;      // 下一页号（用于链表）
    uint16_t free_space;        // 空闲空间偏移量
    uint16_t free_space_size;   // 空闲空间大小
    uint16_t tuple_count;       // 元组数量
    uint16_t flags;             // 页标志
    uint32_t checksum;          // 校验和
    uint32_t lsn;               // 日志序列号

    static constexpr uint16_t FLAG_DIRTY = 0x0001;
    static constexpr uint16_t FLAG_WAL_LOGGED = 0x0002;

    void init(PageId id) {
        page_id = id;
        next_page_id = INVALID_PAGE_ID;
        free_space = sizeof(PageHeader);
        free_space_size = PAGE_SIZE - sizeof(PageHeader);
        tuple_count = 0;
        flags = 0;
        checksum = 0;
        lsn = 0;
    }
};

// 页内槽位目录项（从页尾向前增长）
struct SlotEntry {
    uint16_t offset;    // 元组偏移量
    uint16_t length;    // 元组长度
    uint16_t flags;     // 标志位

    static constexpr uint16_t FLAG_IN_USE = 0x0001;
    static constexpr uint16_t FLAG_DELETED = 0x0002;

    bool isInUse() const { return (flags & FLAG_IN_USE) != 0; }
    bool isDeleted() const { return (flags & FLAG_DELETED) != 0; }
    void setInUse(bool in_use) {
        if (in_use) flags |= FLAG_IN_USE;
        else flags &= ~FLAG_IN_USE;
    }
    void setDeleted(bool deleted) {
        if (deleted) flags |= FLAG_DELETED;
        else flags &= ~FLAG_DELETED;
    }
};

// 数据页类
class Page {
public:
    Page() = default;
    explicit Page(PageId id) { header().init(id); }

    // 获取页头
    PageHeader& header() { return *reinterpret_cast<PageHeader*>(data_.data()); }
    const PageHeader& header() const { return *reinterpret_cast<const PageHeader*>(data_.data()); }

    // 获取页号
    PageId getPageId() const { return header().page_id; }

    // 获取原始数据指针
    char* getData() { return data_.data(); }
    const char* getData() const { return data_.data(); }

    // 获取页大小
    static constexpr size_t getSize() { return PAGE_SIZE; }

    // 槽位操作
    SlotEntry* getSlot(uint16_t slot_id);
    const SlotEntry* getSlot(uint16_t slot_id) const;
    uint16_t getSlotCount() const { return header().tuple_count; }

    // 插入元组，返回槽位号
    uint16_t insertTuple(const char* data, uint16_t length);

    // 获取元组数据
    char* getTupleData(uint16_t slot_id);
    const char* getTupleData(uint16_t slot_id) const;
    uint16_t getTupleLength(uint16_t slot_id) const;

    // 删除元组（标记删除）
    bool deleteTuple(uint16_t slot_id);

    // 检查是否有足够空间
    bool hasEnoughSpace(uint16_t length) const;

    // 计算校验和
    uint32_t calculateChecksum() const;
    void updateChecksum() { header().checksum = calculateChecksum(); }
    bool verifyChecksum() const { return header().checksum == calculateChecksum(); }

    // 序列化/反序列化
    void serialize(char* buffer) const { std::memcpy(buffer, data_.data(), PAGE_SIZE); }
    void deserialize(const char* buffer) { std::memcpy(data_.data(), buffer, PAGE_SIZE); }

private:
    std::array<char, PAGE_SIZE> data_;

    // 获取槽位数组起始位置（从页尾向前）
    SlotEntry* getSlotArray();
    const SlotEntry* getSlotArray() const;

    // 计算槽位数组占用的空间
    size_t getSlotArraySize() const { return header().tuple_count * sizeof(SlotEntry); }
};

} // namespace storage
} // namespace tinydb
