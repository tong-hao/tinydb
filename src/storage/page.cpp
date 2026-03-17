#include "page.h"

namespace tinydb {
namespace storage {

// 获取槽位数组起始位置（从页尾向前）
SlotEntry* Page::getSlotArray() {
    return reinterpret_cast<SlotEntry*>(data_.data() + PAGE_SIZE - getSlotArraySize());
}

const SlotEntry* Page::getSlotArray() const {
    return reinterpret_cast<const SlotEntry*>(data_.data() + PAGE_SIZE - getSlotArraySize());
}

// 获取指定槽位
SlotEntry* Page::getSlot(uint16_t slot_id) {
    if (slot_id >= header().tuple_count) {
        return nullptr;
    }
    return getSlotArray() + slot_id;
}

const SlotEntry* Page::getSlot(uint16_t slot_id) const {
    if (slot_id >= header().tuple_count) {
        return nullptr;
    }
    return getSlotArray() + slot_id;
}

// 检查是否有足够空间
bool Page::hasEnoughSpace(uint16_t length) const {
    // 需要空间：元组数据 + 一个新的槽位
    size_t required = length + sizeof(SlotEntry);
    return header().free_space_size >= required;
}

// 插入元组
uint16_t Page::insertTuple(const char* data, uint16_t length) {
    if (!hasEnoughSpace(length)) {
        throw std::runtime_error("Not enough space in page");
    }

    // 获取当前槽位号
    uint16_t slot_id = header().tuple_count;

    // 写入元组数据
    char* tuple_addr = data_.data() + header().free_space;
    std::memcpy(tuple_addr, data, length);

    // 添加槽位目录项
    SlotEntry* slot_array = getSlotArray();
    SlotEntry* new_slot = reinterpret_cast<SlotEntry*>(
        reinterpret_cast<char*>(slot_array) - sizeof(SlotEntry));

    new_slot->offset = header().free_space;
    new_slot->length = length;
    new_slot->flags = SlotEntry::FLAG_IN_USE;

    // 更新页头
    header().free_space += length;
    header().free_space_size -= (length + sizeof(SlotEntry));
    header().tuple_count++;

    return slot_id;
}

// 获取元组数据
char* Page::getTupleData(uint16_t slot_id) {
    const SlotEntry* slot = getSlot(slot_id);
    if (!slot || !slot->isInUse() || slot->isDeleted()) {
        return nullptr;
    }
    return data_.data() + slot->offset;
}

const char* Page::getTupleData(uint16_t slot_id) const {
    const SlotEntry* slot = getSlot(slot_id);
    if (!slot || !slot->isInUse() || slot->isDeleted()) {
        return nullptr;
    }
    return data_.data() + slot->offset;
}

// 获取元组长度
uint16_t Page::getTupleLength(uint16_t slot_id) const {
    const SlotEntry* slot = getSlot(slot_id);
    if (!slot) {
        return 0;
    }
    return slot->length;
}

// 删除元组（标记删除）
bool Page::deleteTuple(uint16_t slot_id) {
    SlotEntry* slot = getSlot(slot_id);
    if (!slot || !slot->isInUse() || slot->isDeleted()) {
        return false;
    }
    slot->setDeleted(true);
    return true;
}

// 计算校验和（简单的CRC32-like算法）
uint32_t Page::calculateChecksum() const {
    uint32_t checksum = 0;
    // 跳过校验和字段本身
    const char* data = data_.data();
    size_t checksum_offset = offsetof(PageHeader, checksum);

    // 计算校验和字段之前的部分
    for (size_t i = 0; i < checksum_offset; ++i) {
        checksum = (checksum << 1) | (checksum >> 31);
        checksum ^= static_cast<uint8_t>(data[i]);
    }

    // 跳过校验和字段（4字节）
    size_t after_checksum = checksum_offset + sizeof(uint32_t);

    // 计算校验和字段之后的部分
    for (size_t i = after_checksum; i < PAGE_SIZE; ++i) {
        checksum = (checksum << 1) | (checksum >> 31);
        checksum ^= static_cast<uint8_t>(data[i]);
    }

    return checksum;
}

} // namespace storage
} // namespace tinydb
