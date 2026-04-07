#pragma once

#include "page.h"
#include "disk_manager.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <atomic>
#include <memory>
#include <condition_variable>

namespace tinydb {
namespace storage {

// Buffer page frame
class BufferFrame {
public:
    BufferFrame() : page_id_(INVALID_PAGE_ID), pin_count_(0), is_dirty_(false) {}

    PageId getPageId() const { return page_id_; }
    void setPageId(PageId id) { page_id_ = id; }

    Page& getPage() { return page_; }
    const Page& getPage() const { return page_; }

    int getPinCount() const { return pin_count_; }
    void pin() { ++pin_count_; }
    bool unpin() {
        if (pin_count_ > 0) {
            --pin_count_;
            return true;
        }
        return false;
    }

    bool isDirty() const { return is_dirty_; }
    void setDirty(bool dirty) { is_dirty_ = dirty; }

    bool isPinned() const { return pin_count_ > 0; }

private:
    PageId page_id_;        // Page ID
    Page page_;             // Page data
    std::atomic<int> pin_count_;    // Pin count (reference count)
    std::atomic<bool> is_dirty_;    // Dirty page flag
};

// Buffer pool manager
class BufferPoolManager {
public:
    explicit BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();

    // Disable copy
    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;

    // Fetch page (from buffer pool or load from disk)
    BufferFrame* fetchPage(PageId page_id);

    // Create new page
    BufferFrame* newPage(PageId* page_id);

    // Release page (decrease pin count)
    bool unpinPage(PageId page_id, bool is_dirty);

    // Flush page to disk
    bool flushPage(PageId page_id);

    // Flush all dirty pages
    void flushAllPages();

    // Delete page
    bool deletePage(PageId page_id);

    // Get buffer pool size
    size_t getPoolSize() const { return pool_size_; }

    // Get current number of buffered pages
    size_t getPageCount() const;

    // Get number of dirty pages
    size_t getDirtyPageCount() const;

private:
    size_t pool_size_;                          // Buffer pool size
    DiskManager* disk_manager_;                 // Disk manager

    // Buffer frame array
    std::unique_ptr<BufferFrame[]> frames_;

    // Mapping from page ID to frame index
    std::unordered_map<PageId, size_t> page_table_;

    // LRU list (stores frame indices)
    std::list<size_t> lru_list_;

    // Position mapping in LRU list (for O(1) removal)
    std::unordered_map<size_t, std::list<size_t>::iterator> lru_map_;

    // Mutex
    mutable std::mutex mutex_;

    // Find replaceable frame (unpinned page)
    size_t findReplaceableFrame();

    // Remove from LRU list
    void removeFromLRU(size_t frame_id);

    // Add to LRU list head (most recently used)
    void addToLRUHead(size_t frame_id);

    // Add to LRU list tail (least recently used)
    void addToLRUTail(size_t frame_id);
};

} // namespace storage
} // namespace tinydb
