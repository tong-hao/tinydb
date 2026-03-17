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

// 缓冲页帧
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
    PageId page_id_;        // 页号
    Page page_;             // 页数据
    std::atomic<int> pin_count_;    // 引用计数
    std::atomic<bool> is_dirty_;    // 脏页标志
};

// 缓冲池管理器
class BufferPoolManager {
public:
    explicit BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
    ~BufferPoolManager();

    // 禁止拷贝
    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;

    // 获取页（从缓冲池或磁盘加载）
    BufferFrame* fetchPage(PageId page_id);

    // 创建新页
    BufferFrame* newPage(PageId* page_id);

    // 释放页（减少引用计数）
    bool unpinPage(PageId page_id, bool is_dirty);

    // 刷新页到磁盘
    bool flushPage(PageId page_id);

    // 刷新所有脏页
    void flushAllPages();

    // 删除页
    bool deletePage(PageId page_id);

    // 获取缓冲池大小
    size_t getPoolSize() const { return pool_size_; }

    // 获取当前缓冲的页数
    size_t getPageCount() const;

    // 获取脏页数量
    size_t getDirtyPageCount() const;

private:
    size_t pool_size_;                          // 缓冲池大小
    DiskManager* disk_manager_;                 // 磁盘管理器

    // 缓冲帧数组
    std::unique_ptr<BufferFrame[]> frames_;

    // 页号到帧索引的映射
    std::unordered_map<PageId, size_t> page_table_;

    // LRU列表（存储帧索引）
    std::list<size_t> lru_list_;

    // LRU列表中的位置映射（用于O(1)删除）
    std::unordered_map<size_t, std::list<size_t>::iterator> lru_map_;

    // 互斥锁
    mutable std::mutex mutex_;

    // 找到可替换的帧（未固定的页）
    size_t findReplaceableFrame();

    // 从LRU列表中移除
    void removeFromLRU(size_t frame_id);

    // 添加到LRU列表头部（最近使用）
    void addToLRUHead(size_t frame_id);

    // 添加到LRU列表尾部（最久未使用）
    void addToLRUTail(size_t frame_id);
};

} // namespace storage
} // namespace tinydb
