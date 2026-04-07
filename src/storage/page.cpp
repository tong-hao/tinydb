#include "page.h"

namespace tinydb {
namespace storage {

// Get slot array starting position (from page end backward)
SlotEntry* Page::getSlotArray() {
    return reinterpret_cast<SlotEntry*>(data_.data() + PAGE_SIZE - getSlotArraySize());
}

const SlotEntry* Page::getSlotArray() const {
    return reinterpret_cast<const SlotEntry*>(data_.data() + PAGE_SIZE - getSlotArraySize());
}

// Get slot at specified index
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

// Check if enough space available
bool Page::hasEnoughSpace(uint16_t length) const {
    // Required space: tuple data + one new slot entry
    size_t required = length + sizeof(SlotEntry);
    return header().free_space_size >= required;
}

// Insert tuple
uint16_t Page::insertTuple(const char* data, uint16_t length) {
    if (!hasEnoughSpace(length)) {
        throw std::runtime_error("Not enough space in page");
    }

    // Get current slot ID
    uint16_t slot_id = header().tuple_count;

    // Write tuple data
    char* tuple_addr = data_.data() + header().free_space;
    std::memcpy(tuple_addr, data, length);

    // Add slot directory entry
    SlotEntry* slot_array = getSlotArray();
    SlotEntry* new_slot = reinterpret_cast<SlotEntry*>(
        reinterpret_cast<char*>(slot_array) - sizeof(SlotEntry));

    new_slot->offset = header().free_space;
    new_slot->length = length;
    new_slot->flags = SlotEntry::FLAG_IN_USE;

    // Update page header
    header().free_space += length;
    header().free_space_size -= (length + sizeof(SlotEntry));
    header().tuple_count++;

    return slot_id;
}

// Get tuple data
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

// Get tuple length
uint16_t Page::getTupleLength(uint16_t slot_id) const {
    const SlotEntry* slot = getSlot(slot_id);
    if (!slot) {
        return 0;
    }
    return slot->length;
}

// Delete tuple (mark as deleted)
bool Page::deleteTuple(uint16_t slot_id) {
    SlotEntry* slot = getSlot(slot_id);
    if (!slot || !slot->isInUse() || slot->isDeleted()) {
        return false;
    }
    slot->setDeleted(true);
    return true;
}

// Calculate checksum (simple CRC32-like algorithm)
uint32_t Page::calculateChecksum() const {
    uint32_t checksum = 0;
    // Skip checksum field itself
    const char* data = data_.data();
    size_t checksum_offset = offsetof(PageHeader, checksum);

    // Calculate checksum for data before checksum field
    for (size_t i = 0; i < checksum_offset; ++i) {
        checksum = (checksum << 1) | (checksum >> 31);
        checksum ^= static_cast<uint8_t>(data[i]);
    }

    // Skip checksum field (4 bytes)
    size_t after_checksum = checksum_offset + sizeof(uint32_t);

    // Calculate checksum for data after checksum field
    for (size_t i = after_checksum; i < PAGE_SIZE; ++i) {
        checksum = (checksum << 1) | (checksum >> 31);
        checksum ^= static_cast<uint8_t>(data[i]);
    }

    return checksum;
}

} // namespace storage
} // namespace tinydb
