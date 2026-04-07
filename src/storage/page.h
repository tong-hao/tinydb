#pragma once

#include <cstdint>
#include <cstring>
#include <array>
#include <stdexcept>

namespace tinydb {
namespace storage {

// Page size constant
constexpr uint32_t PAGE_SIZE = 8192;  // 8KB
constexpr uint32_t MAX_PAGE_COUNT = 0xFFFFFFFF;

// Page ID type
using PageId = uint32_t;
constexpr PageId INVALID_PAGE_ID = 0xFFFFFFFF;

// Page header structure (located at the beginning of each page)
struct PageHeader {
    uint32_t page_id;           // Page ID
    uint32_t next_page_id;      // Next page ID (for linked list)
    uint16_t free_space;        // Free space offset
    uint16_t free_space_size;   // Free space size
    uint16_t tuple_count;       // Number of tuples
    uint16_t flags;             // Page flags
    uint32_t checksum;          // Checksum
    uint32_t lsn;               // Log sequence number

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

// Slot directory entry within a page (grows backward from page end)
struct SlotEntry {
    uint16_t offset;    // Tuple offset
    uint16_t length;    // Tuple length
    uint16_t flags;     // Flag bits

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

// Data page class
class Page {
public:
    Page() = default;
    explicit Page(PageId id) { header().init(id); }

    // Get page header
    PageHeader& header() { return *reinterpret_cast<PageHeader*>(data_.data()); }
    const PageHeader& header() const { return *reinterpret_cast<const PageHeader*>(data_.data()); }

    // Get page ID
    PageId getPageId() const { return header().page_id; }

    // Get raw data pointer
    char* getData() { return data_.data(); }
    const char* getData() const { return data_.data(); }

    // Get page size
    static constexpr size_t getSize() { return PAGE_SIZE; }

    // Slot operations
    SlotEntry* getSlot(uint16_t slot_id);
    const SlotEntry* getSlot(uint16_t slot_id) const;
    uint16_t getSlotCount() const { return header().tuple_count; }

    // Insert tuple, returns slot ID
    uint16_t insertTuple(const char* data, uint16_t length);

    // Get tuple data
    char* getTupleData(uint16_t slot_id);
    const char* getTupleData(uint16_t slot_id) const;
    uint16_t getTupleLength(uint16_t slot_id) const;

    // Delete tuple (mark as deleted)
    bool deleteTuple(uint16_t slot_id);

    // Check if enough space available
    bool hasEnoughSpace(uint16_t length) const;

    // Calculate checksum
    uint32_t calculateChecksum() const;
    void updateChecksum() { header().checksum = calculateChecksum(); }
    bool verifyChecksum() const { return header().checksum == calculateChecksum(); }

    // Serialize/deserialize
    void serialize(char* buffer) const { std::memcpy(buffer, data_.data(), PAGE_SIZE); }
    void deserialize(const char* buffer) { std::memcpy(data_.data(), buffer, PAGE_SIZE); }

private:
    std::array<char, PAGE_SIZE> data_;

    // Get slot array starting position (from page end backward)
    SlotEntry* getSlotArray();
    const SlotEntry* getSlotArray() const;

    // Calculate slot array occupied space
    size_t getSlotArraySize() const { return header().tuple_count * sizeof(SlotEntry); }
};

} // namespace storage
} // namespace tinydb
