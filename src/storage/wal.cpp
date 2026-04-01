#include "wal.h"
#include "common/logger.h"
#include <cstring>
#include <algorithm>

namespace tinydb {
namespace storage {

// LogRecord实现
LogRecord::LogRecord() {
    header_.size = LogRecordHeader::HEADER_SIZE;
    header_.type = LogRecordType::INVALID;
    header_.txn_id = INVALID_TXN_ID;
    header_.lsn = INVALID_LSN;
    header_.prev_lsn = INVALID_LSN;
    header_.checksum = 0;
}

LogRecord::LogRecord(LogRecordType type, TransactionId txn_id, LSN prev_lsn) {
    header_.size = LogRecordHeader::HEADER_SIZE;
    header_.type = type;
    header_.txn_id = txn_id;
    header_.lsn = INVALID_LSN;  // 由WALManager设置
    header_.prev_lsn = prev_lsn;
    header_.checksum = 0;
}

std::vector<uint8_t> LogRecord::serialize() const {
    std::vector<uint8_t> result;
    result.resize(header_.size);

    uint8_t* ptr = result.data();

    // 序列化头部
    std::memcpy(ptr, &header_.size, sizeof(header_.size));
    ptr += sizeof(header_.size);

    *ptr++ = static_cast<uint8_t>(header_.type);

    std::memcpy(ptr, &header_.txn_id, sizeof(header_.txn_id));
    ptr += sizeof(header_.txn_id);

    std::memcpy(ptr, &header_.lsn, sizeof(header_.lsn));
    ptr += sizeof(header_.lsn);

    std::memcpy(ptr, &header_.prev_lsn, sizeof(header_.prev_lsn));
    ptr += sizeof(header_.prev_lsn);

    std::memcpy(ptr, &header_.checksum, sizeof(header_.checksum));
    ptr += sizeof(header_.checksum);

    // 序列化数据
    if (!data_.empty()) {
        std::memcpy(ptr, data_.data(), data_.size());
    }

    return result;
}

bool LogRecord::deserialize(const uint8_t* data, size_t size) {
    if (size < LogRecordHeader::HEADER_SIZE) {
        return false;
    }

    const uint8_t* ptr = data;

    // 反序列化头部
    std::memcpy(&header_.size, ptr, sizeof(header_.size));
    ptr += sizeof(header_.size);

    if (size < header_.size) {
        return false;
    }

    header_.type = static_cast<LogRecordType>(*ptr++);

    std::memcpy(&header_.txn_id, ptr, sizeof(header_.txn_id));
    ptr += sizeof(header_.txn_id);

    std::memcpy(&header_.lsn, ptr, sizeof(header_.lsn));
    ptr += sizeof(header_.lsn);

    std::memcpy(&header_.prev_lsn, ptr, sizeof(header_.prev_lsn));
    ptr += sizeof(header_.prev_lsn);

    std::memcpy(&header_.checksum, ptr, sizeof(header_.checksum));
    ptr += sizeof(header_.checksum);

    // 反序列化数据
    size_t data_size = header_.size - LogRecordHeader::HEADER_SIZE;
    if (data_size > 0) {
        data_.assign(ptr, ptr + data_size);
    } else {
        data_.clear();
    }

    return true;
}

uint32_t LogRecord::calculateChecksum() const {
    uint32_t checksum = 0;

    // 计算头部（除checksum字段外）的校验和
    checksum ^= header_.size;
    checksum ^= static_cast<uint32_t>(header_.type);
    checksum ^= header_.txn_id;
    checksum ^= static_cast<uint32_t>(header_.lsn);
    checksum ^= static_cast<uint32_t>(header_.prev_lsn);

    // 计算数据的校验和
    for (uint8_t byte : data_) {
        checksum = (checksum << 1) | (checksum >> 31);
        checksum ^= byte;
    }

    return checksum;
}

// WALManager实现
WALManager::WALManager(const std::string& log_file_path)
    : log_file_path_(log_file_path) {}

WALManager::~WALManager() {
    close();
}

bool WALManager::initialize() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    // 检查文件是否存在
    std::ifstream test_file(log_file_path_, std::ios::binary);
    bool file_exists = test_file.good();
    test_file.close();

    if (file_exists) {
        // 打开已有日志文件
        file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            LOG_ERROR("Failed to open WAL file: " << log_file_path_);
            return false;
        }

        // 读取最后一条日志的LSN
        file_.seekg(0, std::ios::end);
        auto file_size = file_.tellg();

        if (file_size > 0) {
            // 简单起见，从文件大小估算LSN
            // 实际应该遍历所有日志记录
            current_lsn_.store(static_cast<LSN>(file_size));
            flushed_lsn_.store(current_lsn_.load());
        }

        LOG_INFO("Opened existing WAL file: " << log_file_path_);
    } else {
        // 创建新日志文件
        file_.open(log_file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
        if (!file_.is_open()) {
            LOG_ERROR("Failed to create WAL file: " << log_file_path_);
            return false;
        }

        current_lsn_.store(1);  // LSN从1开始
        flushed_lsn_.store(0);

        LOG_INFO("Created new WAL file: " << log_file_path_);
    }

    return true;
}

LSN WALManager::appendLog(LogRecordType type, TransactionId txn_id, const uint8_t* data, size_t size) {
    LogRecord record(type, txn_id, getAndUpdateTxnLSN(txn_id, INVALID_LSN));

    if (data && size > 0) {
        record.setData(data, size);
    }

    // 分配LSN
    LSN lsn = current_lsn_.fetch_add(record.getSize());
    if (lsn == INVALID_LSN) {
        lsn = current_lsn_.fetch_add(record.getSize());
    }

    // 这里简化处理，实际LSN应该基于文件偏移 TODO: 
    // 为了简化，我们使用递增的LSN
    record.setLSN(lsn);
    record.setChecksum(record.calculateChecksum());

    if (!writeLogRecord(record)) {
        return INVALID_LSN;
    }

    // 更新事务LSN链
    getAndUpdateTxnLSN(txn_id, lsn);

    return lsn;
}

LSN WALManager::logBegin(TransactionId txn_id) {
    return appendLog(LogRecordType::BEGIN, txn_id);
}

LSN WALManager::logCommit(TransactionId txn_id) {
    return appendLog(LogRecordType::COMMIT, txn_id);
}

LSN WALManager::logRollback(TransactionId txn_id) {
    return appendLog(LogRecordType::ROLLBACK, txn_id);
}

LSN WALManager::logInsert(TransactionId txn_id, uint32_t table_id, uint32_t page_id,
                          uint16_t slot_id, const uint8_t* tuple_data, size_t tuple_size) {
    // 构建插入日志数据: table_id(4) + page_id(4) + slot_id(2) + tuple_size(4) + tuple_data
    std::vector<uint8_t> data;
    data.reserve(14 + tuple_size);

    data.resize(4);
    std::memcpy(data.data(), &table_id, 4);

    data.resize(8);
    std::memcpy(data.data() + 4, &page_id, 4);

    data.resize(10);
    std::memcpy(data.data() + 8, &slot_id, 2);

    uint32_t size = static_cast<uint32_t>(tuple_size);
    data.resize(14);
    std::memcpy(data.data() + 10, &size, 4);

    if (tuple_data && tuple_size > 0) {
        data.insert(data.end(), tuple_data, tuple_data + tuple_size);
    }

    return appendLog(LogRecordType::INSERT, txn_id, data.data(), data.size());
}

LSN WALManager::logDelete(TransactionId txn_id, uint32_t table_id, uint32_t page_id, uint16_t slot_id) {
    // 构建删除日志数据: table_id(4) + page_id(4) + slot_id(2)
    uint8_t data[10];
    std::memcpy(data, &table_id, 4);
    std::memcpy(data + 4, &page_id, 4);
    std::memcpy(data + 8, &slot_id, 2);

    return appendLog(LogRecordType::DELETE, txn_id, data, 10);
}

LSN WALManager::logUpdate(TransactionId txn_id, uint32_t table_id, uint32_t page_id, uint16_t slot_id,
                          const uint8_t* old_data, size_t old_size,
                          const uint8_t* new_data, size_t new_size) {
    // 构建更新日志数据
    std::vector<uint8_t> data;
    data.reserve(18 + old_size + new_size);

    // table_id(4) + page_id(4) + slot_id(2) + old_size(4) + old_data + new_size(4) + new_data
    data.resize(4);
    std::memcpy(data.data(), &table_id, 4);

    data.resize(8);
    std::memcpy(data.data() + 4, &page_id, 4);

    data.resize(10);
    std::memcpy(data.data() + 8, &slot_id, 2);

    uint32_t osize = static_cast<uint32_t>(old_size);
    data.resize(14);
    std::memcpy(data.data() + 10, &osize, 4);

    if (old_data && old_size > 0) {
        data.insert(data.end(), old_data, old_data + old_size);
    }

    uint32_t nsize = static_cast<uint32_t>(new_size);
    size_t offset = 14 + old_size;
    data.resize(offset + 4);
    std::memcpy(data.data() + offset, &nsize, 4);

    if (new_data && new_size > 0) {
        data.insert(data.end(), new_data, new_data + new_size);
    }

    return appendLog(LogRecordType::UPDATE, txn_id, data.data(), data.size());
}

LSN WALManager::logNewPage(TransactionId txn_id, uint32_t page_id) {
    return appendLog(LogRecordType::NEW_PAGE, txn_id, reinterpret_cast<const uint8_t*>(&page_id), 4);
}

bool WALManager::flush() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    if (!file_.is_open()) {
        return false;
    }

    file_.flush();

    // 更新已刷盘的LSN
    flushed_lsn_.store(current_lsn_.load() - 1);

    return file_.good();
}

void WALManager::close() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    if (file_.is_open()) {
        flush();
        file_.close();
    }
}

bool WALManager::writeLogRecord(const LogRecord& record) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);

    if (!file_.is_open()) {
        return false;
    }

    // 移动到文件末尾
    file_.seekp(0, std::ios::end);

    // 序列化并写入
    auto data = record.serialize();
    file_.write(reinterpret_cast<const char*>(data.data()), data.size());

    return file_.good();
}

LSN WALManager::getAndUpdateTxnLSN(TransactionId txn_id, LSN new_lsn) {
    std::lock_guard<std::mutex> lock(txn_mutex_);

    auto it = txn_last_lsn_.find(txn_id);
    LSN prev_lsn = (it != txn_last_lsn_.end()) ? it->second : INVALID_LSN;

    if (new_lsn != INVALID_LSN) {
        txn_last_lsn_[txn_id] = new_lsn;
    }

    return prev_lsn;
}

std::vector<LogRecord> WALManager::readAllLogs() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<LogRecord> records;

    if (!file_.is_open()) {
        return records;
    }

    // 保存当前位置
    auto current_pos = file_.tellg();

    // 从头开始读取
    file_.seekg(0, std::ios::beg);

    while (file_.good() && !file_.eof()) {
        // 读取记录大小
        uint32_t record_size;
        file_.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));

        if (file_.gcount() != sizeof(record_size) || record_size < LogRecordHeader::HEADER_SIZE) {
            break;
        }

        // 读取完整记录
        std::vector<uint8_t> buffer(record_size);
        std::memcpy(buffer.data(), &record_size, sizeof(record_size));

        file_.read(reinterpret_cast<char*>(buffer.data()) + sizeof(record_size),
                   record_size - sizeof(record_size));

        if (static_cast<size_t>(file_.gcount()) != record_size - sizeof(record_size)) {
            break;
        }

        LogRecord record;
        if (record.deserialize(buffer.data(), buffer.size())) {
            records.push_back(record);
        }
    }

    // 恢复位置
    file_.seekg(current_pos);

    return records;
}

std::vector<LogRecord> WALManager::readLogsFrom(LSN start_lsn) {
    // 简化实现：读取所有日志，然后过滤
    auto all_records = readAllLogs();
    std::vector<LogRecord> result;

    for (const auto& record : all_records) {
        if (record.getLSN() >= start_lsn) {
            result.push_back(record);
        }
    }

    return result;
}

} // namespace storage
} // namespace tinydb
