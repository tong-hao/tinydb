#include "disk_manager.h"
#include "common/logger.h"
#include <cstring>
#include <algorithm>

namespace tinydb {
namespace storage {

DiskManager::DiskManager(const std::string& db_file_path)
    : file_path_(db_file_path) {
    // 检查文件是否存在
    std::ifstream test_file(file_path_, std::ios::binary);
    bool file_exists = test_file.good();
    test_file.close();

    if (file_exists) {
        if (!openExistingFile()) {
            LOG_ERROR("Failed to open existing database file: " << file_path_);
            throw std::runtime_error("Failed to open database file");
        }
        LOG_INFO("Opened existing database: " << file_path_ << " with " << file_header_.page_count << " pages");
    } else {
        if (!initializeNewFile()) {
            LOG_ERROR("Failed to create new database file: " << file_path_);
            throw std::runtime_error("Failed to create database file");
        }
        LOG_INFO("Created new database: " << file_path_);
    }
}

DiskManager::~DiskManager() {
    close();
}

bool DiskManager::initializeNewFile() {
    std::lock_guard<std::mutex> lock(mutex_);

    // 以读写模式打开文件
    file_.open(file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        return false;
    }

    // 初始化文件头
    file_header_.init();

    // 写入文件头
    file_.seekp(0);
    file_.write(reinterpret_cast<const char*>(&file_header_), sizeof(FileHeader));

    // 扩展文件到第一页大小
    file_.seekp(PAGE_SIZE - 1);
    file_.put(0);

    file_.flush();
    return file_.good();
}

bool DiskManager::openExistingFile() {
    std::lock_guard<std::mutex> lock(mutex_);

    // 以读写模式打开文件
    file_.open(file_path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        return false;
    }

    // 读取文件头
    file_.seekg(0);
    file_.read(reinterpret_cast<char*>(&file_header_), sizeof(FileHeader));

    if (!file_header_.isValid()) {
        file_.close();
        return false;
    }

    return true;
}

bool DiskManager::readPage(PageId page_id, Page* page) {
    if (!page || page_id == INVALID_PAGE_ID) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id >= file_header_.page_count) {
        LOG_ERROR("Invalid page id: " << page_id);
        return false;
    }

    file_.seekg(pageOffset(page_id));
    if (!file_.good()) {
        return false;
    }

    char buffer[PAGE_SIZE];
    file_.read(buffer, PAGE_SIZE);

    if (file_.gcount() != PAGE_SIZE) {
        LOG_ERROR("Failed to read complete page " << page_id);
        return false;
    }

    page->deserialize(buffer);
    return true;
}

bool DiskManager::writePage(PageId page_id, const Page* page) {
    if (!page || page_id == INVALID_PAGE_ID) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id >= file_header_.page_count) {
        LOG_ERROR("Invalid page id: " << page_id);
        return false;
    }

    file_.seekp(pageOffset(page_id));
    if (!file_.good()) {
        return false;
    }

    char buffer[PAGE_SIZE];
    page->serialize(buffer);

    file_.write(buffer, PAGE_SIZE);
    file_.flush();

    return file_.good();
}

PageId DiskManager::allocatePage() {
    std::lock_guard<std::mutex> lock(mutex_);

    // 首先检查空闲页链表
    if (file_header_.free_page != INVALID_PAGE_ID) {
        PageId page_id = file_header_.free_page;

        // 读取空闲页，获取下一个空闲页
        Page temp_page;
        file_.seekg(pageOffset(page_id));
        char buffer[PAGE_SIZE];
        file_.read(buffer, PAGE_SIZE);
        temp_page.deserialize(buffer);

        // 更新空闲页链表头
        file_header_.free_page = temp_page.header().next_page_id;

        // 刷新文件头
        flushFileHeader();

        // 初始化新页
        temp_page.header().init(page_id);
        file_.seekp(pageOffset(page_id));
        temp_page.serialize(buffer);
        file_.write(buffer, PAGE_SIZE);
        file_.flush();

        return page_id;
    }

    // 没有空闲页，分配新页
    PageId new_page_id = file_header_.page_count++;

    // 刷新文件头
    flushFileHeader();

    // 扩展文件
    if (!extendFile()) {
        file_header_.page_count--;
        return INVALID_PAGE_ID;
    }

    // 初始化新页
    Page new_page(new_page_id);
    char buffer[PAGE_SIZE];
    new_page.serialize(buffer);

    file_.seekp(pageOffset(new_page_id));
    file_.write(buffer, PAGE_SIZE);
    file_.flush();

    return new_page_id;
}

void DiskManager::deallocatePage(PageId page_id) {
    if (page_id == INVALID_PAGE_ID) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (page_id >= file_header_.page_count) {
        return;
    }

    // 读取要释放的页
    Page page;
    char buffer[PAGE_SIZE];
    file_.seekg(pageOffset(page_id));
    file_.read(buffer, PAGE_SIZE);
    page.deserialize(buffer);

    // 将页加入空闲链表
    page.header().next_page_id = file_header_.free_page;
    page.header().page_id = page_id;
    page.header().flags = 0;

    // 写回
    file_.seekp(pageOffset(page_id));
    page.serialize(buffer);
    file_.write(buffer, PAGE_SIZE);

    // 更新空闲链表头
    file_header_.free_page = page_id;
    flushFileHeader();
}

bool DiskManager::flushFileHeader() {
    file_.seekp(0);
    file_.write(reinterpret_cast<const char*>(&file_header_), sizeof(FileHeader));
    file_.flush();
    return file_.good();
}

bool DiskManager::extendFile() {
    // 获取当前文件大小
    file_.seekp(0, std::ios::end);
    auto current_size = file_.tellp();
    auto required_size = static_cast<std::streamoff>(pageOffset(file_header_.page_count));

    if (current_size < required_size) {
        file_.seekp(required_size - 1);
        file_.put(0);
        file_.flush();
    }

    return file_.good();
}

void DiskManager::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_.is_open()) {
        flushFileHeader();
        file_.close();
    }
}

} // namespace storage
} // namespace tinydb
