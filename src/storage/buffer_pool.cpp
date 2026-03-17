#include "buffer_pool.h"
#include "common/logger.h"

namespace tinydb {
namespace storage {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    frames_ = std::make_unique<BufferFrame[]>(pool_size);

    // 初始化所有帧到LRU列表尾部（空闲）
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

    // 检查页是否已在缓冲池中
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        BufferFrame* frame = &frames_[frame_id];
        frame->pin();

        // 移动到LRU头部（最近使用）
        removeFromLRU(frame_id);
        addToLRUHead(frame_id);

        return frame;
    }

    // 页不在缓冲池中，需要加载
    size_t frame_id = findReplaceableFrame();
    if (frame_id >= pool_size_) {
        LOG_ERROR("No replaceable frame found for page " << page_id);
        return nullptr;
    }

    BufferFrame* frame = &frames_[frame_id];

    // 如果帧是脏的，先写回磁盘
    if (frame->isDirty() && frame->getPageId() != INVALID_PAGE_ID) {
        if (!disk_manager_->writePage(frame->getPageId(), &frame->getPage())) {
            LOG_ERROR("Failed to flush dirty page " << frame->getPageId());
            return nullptr;
        }
        frame->setDirty(false);
    }

    // 从页表中移除旧页
    if (frame->getPageId() != INVALID_PAGE_ID) {
        page_table_.erase(frame->getPageId());
    }

    // 从磁盘读取页
    if (!disk_manager_->readPage(page_id, &frame->getPage())) {
        LOG_ERROR("Failed to read page " << page_id << " from disk");
        return nullptr;
    }

    // 设置帧信息
    frame->setPageId(page_id);
    frame->pin();

    // 更新页表和LRU
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

    // 从磁盘管理器分配新页
    PageId new_page_id = disk_manager_->allocatePage();
    if (new_page_id == INVALID_PAGE_ID) {
        LOG_ERROR("Failed to allocate new page from disk manager");
        return nullptr;
    }

    // 找到可替换的帧
    size_t frame_id = findReplaceableFrame();
    if (frame_id >= pool_size_) {
        LOG_ERROR("No replaceable frame found for new page");
        disk_manager_->deallocatePage(new_page_id);
        return nullptr;
    }

    BufferFrame* frame = &frames_[frame_id];

    // 如果帧是脏的，先写回磁盘
    if (frame->isDirty() && frame->getPageId() != INVALID_PAGE_ID) {
        if (!disk_manager_->writePage(frame->getPageId(), &frame->getPage())) {
            LOG_ERROR("Failed to flush dirty page " << frame->getPageId());
            disk_manager_->deallocatePage(new_page_id);
            return nullptr;
        }
        frame->setDirty(false);
    }

    // 从页表中移除旧页
    if (frame->getPageId() != INVALID_PAGE_ID) {
        page_table_.erase(frame->getPageId());
    }

    // 初始化新页
    frame->getPage().header().init(new_page_id);
    frame->setPageId(new_page_id);
    frame->pin();
    frame->setDirty(true);

    // 更新页表和LRU
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

    // 如果不再被固定，移动到LRU尾部
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
        // 页不在缓冲池中，可能已经在磁盘上是最新的
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

        // 如果页被固定，不能删除
        if (frame->isPinned()) {
            LOG_ERROR("Cannot delete pinned page " << page_id);
            return false;
        }

        // 重置帧
        frame->setPageId(INVALID_PAGE_ID);
        frame->setDirty(false);

        // 从页表中移除
        page_table_.erase(it);

        // 移动到LRU尾部
        removeFromLRU(frame_id);
        addToLRUTail(frame_id);
    }

    // 从磁盘管理器释放页
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
    // 从LRU列表尾部开始找（最久未使用）
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        size_t frame_id = *it;
        if (!frames_[frame_id].isPinned()) {
            return frame_id;
        }
    }
    return pool_size_;  // 未找到
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
