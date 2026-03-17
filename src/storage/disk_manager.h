#pragma once

#include "page.h"
#include <string>
#include <fstream>
#include <mutex>
#include <atomic>

namespace tinydb {
namespace storage {

// 数据库文件头（位于文件开头）
struct FileHeader {
    char magic[8];          // 魔数 "TINYDB\0\0"
    uint32_t version;       // 文件格式版本
    uint32_t page_size;     // 页大小
    uint32_t page_count;    // 总页数
    uint32_t free_page;     // 空闲页链表头
    uint32_t reserved[8];   // 保留字段

    void init() {
        std::memcpy(magic, "TINYDB\0\0", 8);
        version = 1;
        page_size = PAGE_SIZE;
        page_count = 1;  // 第0页是文件头页
        free_page = INVALID_PAGE_ID;
        std::fill(std::begin(reserved), std::end(reserved), 0);
    }

    bool isValid() const {
        return std::memcmp(magic, "TINYDB\0\0", 8) == 0 &&
               version == 1 &&
               page_size == PAGE_SIZE;
    }
};

// 磁盘管理器 - 负责页的物理读写
class DiskManager {
public:
    explicit DiskManager(const std::string& db_file_path);
    ~DiskManager();

    // 禁止拷贝
    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    // 读取页
    bool readPage(PageId page_id, Page* page);

    // 写入页
    bool writePage(PageId page_id, const Page* page);

    // 分配新页
    PageId allocatePage();

    // 释放页
    void deallocatePage(PageId page_id);

    // 获取当前页数
    uint32_t getPageCount() const { return file_header_.page_count; }

    // 刷新文件头到磁盘
    bool flushFileHeader();

    // 关闭文件
    void close();

    // 获取文件路径
    const std::string& getFilePath() const { return file_path_; }

private:
    std::string file_path_;
    std::fstream file_;
    FileHeader file_header_;
    mutable std::mutex mutex_;

    // 初始化新数据库文件
    bool initializeNewFile();

    // 打开已有数据库文件
    bool openExistingFile();

    // 扩展文件以容纳新页
    bool extendFile();

    // 计算页在文件中的偏移量
    size_t pageOffset(PageId page_id) const {
        return static_cast<size_t>(page_id) * PAGE_SIZE;
    }
};

} // namespace storage
} // namespace tinydb
