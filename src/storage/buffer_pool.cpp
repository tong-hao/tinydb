#include "buffer_pool.h"
#include "common/logger.h"

namespace tinydb {
namespace storage {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    frames_ = std::make_unique<BufferFrame[]>(pool_size);

    // Initialize all frames to the tail of LRU list (free)
    for (size_t i = 0; i < pool_size; ++i) {
        lru_list_.push_back(i);
        lru_map_[i] = --lru_list_.end();
    }

    LOG_INFO("Buffer pool initialized with " << pool_size << " frames");
}

BufferPoolManager::~BufferPoolManager() {
    flushAllPages();
}

BufferFrame* BufferPoolManager::fetchPage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        BufferFrame* frame = &frames_[frame_id];
        frame->pin();

        // Move to LRU head (most recently used)
        removeFromLRU(frame_id);
        addToLRUHead(frame_id);

        return frame;
    }

    // Page not in buffer pool, needs to be loaded
    size_t frame_id = findReplaceableFrame();
    if (frame_id >= pool_size_) {
        LOG_ERROR("No replaceable frame found for page " << page_id);
        return nullptr;
    }

    BufferFrame* frame = &frames_[frame_id];

    // If frame is dirty, flush to disk first
    if (frame->isDirty() && frame->getPageId() != INVALID_PAGE_ID) {
        if (!disk_manager_->writePage(frame->getPageId(), &frame->getPage())) {
            LOG_ERROR("Failed to flush dirty page " << frame->getPageId());
            return nullptr;
        }
        frame->setDirty(false);
    }

    // Remove old page from page table
    if (frame->getPageId() != INVALID_PAGE_ID) {
        page_table_.erase(frame->getPageId());
    }

    // Read page from disk
    if (!disk_manager_->readPage(page_id, &frame->getPage())) {
        LOG_ERROR("Failed to read page " << page_id << " from disk");
        return nullptr;
    }

    // Set frame info
    frame->setPageId(page_id);
    frame->pin();

    // Update page table and LRU
    page_table_[page_id] = frame_id;
    removeFromLRU(frame_id);
    addToLRUHead(frame_id);

    return frame;
}

BufferFrame* BufferPoolManager::newPage(PageId* page_id) {
    if (!page_id) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Allocate new page from disk manager
    PageId new_page_id = disk_manager_->allocatePage();
    if (new_page_id == INVALID_PAGE_ID) {
        LOG_ERROR("Failed to allocate new page from disk manager");
        return nullptr;
    }

    // Find replaceable frame
    size_t frame_id = findReplaceableFrame();
    if (frame_id >= pool_size_) {
        LOG_ERROR("No replaceable frame found for new page");
        disk_manager_->deallocatePage(new_page_id);
        return nullptr;
    }

    BufferFrame* frame = &frames_[frame_id];

    // If frame is dirty, flush to disk first
    if (frame->isDirty() && frame->getPageId() != INVALID_PAGE_ID) {
        if (!disk_manager_->writePage(frame->getPageId(), &frame->getPage())) {
            LOG_ERROR("Failed to flush dirty page " << frame->getPageId());
            disk_manager_->deallocatePage(new_page_id);
            return nullptr;
        }
        frame->setDirty(false);
    }

    // Remove old page from page table
    if (frame->getPageId() != INVALID_PAGE_ID) {
        page_table_.erase(frame->getPageId());
    }

    // Initialize new page
    frame->getPage().header().init(new_page_id);
    frame->setPageId(new_page_id);
    frame->pin();
    frame->setDirty(true);

    // Update page table and LRU
    page_table_[new_page_id] = frame_id;
    removeFromLRU(frame_id);
    addToLRUHead(frame_id);

    *page_id = new_page_id;
    return frame;
}

bool BufferPoolManager::unpinPage(PageId page_id, bool is_dirty) {
    if (page_id == INVALID_PAGE_ID) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    size_t frame_id = it->second;
    BufferFrame* frame = &frames_[frame_id];

    if (!frame->unpin()) {
        return false;
    }

    if (is_dirty) {
        frame->setDirty(true);
    }

    // If no longer pinned, move to LRU tail
    if (!frame->isPinned()) {
        removeFromLRU(frame_id);
        addToLRUTail(frame_id);
    }

    return true;
}

bool BufferPoolManager::flushPage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool, may already be up-to-date on disk
        return true;
    }

    size_t frame_id = it->second;
    BufferFrame* frame = &frames_[frame_id];

    if (!frame->isDirty()) {
        return true;
    }

    if (!disk_manager_->writePage(page_id, &frame->getPage())) {
        LOG_ERROR("Failed to flush page " << page_id);
        return false;
    }

    frame->setDirty(false);
    return true;
}

void BufferPoolManager::flushAllPages() {
    std::lock_guard<std::mutex> lock(mutex_);

    for (size_t i = 0; i < pool_size_; ++i) {
        BufferFrame* frame = &frames_[i];
        if (frame->isDirty() && frame->getPageId() != INVALID_PAGE_ID) {
            if (!disk_manager_->writePage(frame->getPageId(), &frame->getPage())) {
                LOG_ERROR("Failed to flush page " << frame->getPageId());
            } else {
                frame->setDirty(false);
            }
        }
    }
}

bool BufferPoolManager::deletePage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        BufferFrame* frame = &frames_[frame_id];

        // Cannot delete pinned page
        if (frame->isPinned()) {
            LOG_ERROR("Cannot delete pinned page " << page_id);
            return false;
        }

        // Reset frame
        frame->setPageId(INVALID_PAGE_ID);
        frame->setDirty(false);

        // Remove from page table
        page_table_.erase(it);

        // Move to LRU tail
        removeFromLRU(frame_id);
        addToLRUTail(frame_id);
    }

    // Release page from disk manager
    disk_manager_->deallocatePage(page_id);
    return true;
}

size_t BufferPoolManager::getPageCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return page_table_.size();
}

size_t BufferPoolManager::getDirtyPageCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (size_t i = 0; i < pool_size_; ++i) {
        if (frames_[i].isDirty()) {
            ++count;
        }
    }
    return count;
}

size_t BufferPoolManager::findReplaceableFrame() {
    // Search from the tail of LRU list (least recently used)
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        size_t frame_id = *it;
        if (!frames_[frame_id].isPinned()) {
            return frame_id;
        }
    }
    return pool_size_;  // Not found
}

void BufferPoolManager::removeFromLRU(size_t frame_id) {
    auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        lru_list_.erase(it->second);
        lru_map_.erase(it);
    }
}

void BufferPoolManager::addToLRUHead(size_t frame_id) {
    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
}

void BufferPoolManager::addToLRUTail(size_t frame_id) {
    lru_list_.push_back(frame_id);
    lru_map_[frame_id] = --lru_list_.end();
}

} // namespace storage
} // namespace tinydb
