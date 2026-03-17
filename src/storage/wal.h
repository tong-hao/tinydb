#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <mutex>
#include <atomic>
#include <unordered_map>

namespace tinydb {
namespace storage {

// 日志记录类型
enum class LogRecordType : uint8_t {
    INVALID = 0,
    INSERT = 1,         // 插入记录
    UPDATE = 2,         // 更新记录
    DELETE = 3,         // 删除记录
    COMMIT = 4,         // 事务提交
    ROLLBACK = 5,       // 事务回滚
    BEGIN = 6,          // 事务开始
    CHECKPOINT = 7,     // 检查点
    NEW_PAGE = 8        // 新页分配
};

// 日志序列号
using LSN = uint64_t;
constexpr LSN INVALID_LSN = 0;

// 事务ID
using TransactionId = uint32_t;
constexpr TransactionId INVALID_TXN_ID = 0;

// 日志记录头
struct LogRecordHeader {
    uint32_t size;              // 记录总大小
    LogRecordType type;         // 记录类型
    TransactionId txn_id;       // 事务ID
    LSN lsn;                    // 日志序列号
    LSN prev_lsn;               // 同一事务的前一条日志LSN
    uint32_t checksum;          // 校验和

    static constexpr size_t HEADER_SIZE = 28;
};

// 日志记录
class LogRecord {
public:
    LogRecord();
    LogRecord(LogRecordType type, TransactionId txn_id, LSN prev_lsn);

    // 设置记录数据
    void setData(const std::vector<uint8_t>& data) { data_ = data; updateSize(); }
    void setData(const uint8_t* data, size_t size) { data_.assign(data, data + size); updateSize(); }

    // 设置LSN
    void setLSN(LSN lsn) { header_.lsn = lsn; }

    // 设置校验和
    void setChecksum(uint32_t checksum) { header_.checksum = checksum; }

    // 获取记录信息
    LogRecordType getType() const { return header_.type; }
    TransactionId getTxnId() const { return header_.txn_id; }
    LSN getLSN() const { return header_.lsn; }
    LSN getPrevLSN() const { return header_.prev_lsn; }
    const std::vector<uint8_t>& getData() const { return data_; }

    // 序列化/反序列化
    std::vector<uint8_t> serialize() const;
    bool deserialize(const uint8_t* data, size_t size);

    // 计算校验和
    uint32_t calculateChecksum() const;
    bool verifyChecksum() const { return header_.checksum == calculateChecksum(); }

    // 获取记录大小
    uint32_t getSize() const { return header_.size; }

private:
    LogRecordHeader header_;
    std::vector<uint8_t> data_;

    void updateSize() {
        header_.size = static_cast<uint32_t>(LogRecordHeader::HEADER_SIZE + data_.size());
    }

    // 友元类，允许WALManager访问私有成员
    friend class WALManager;
};

// WAL日志管理器
class WALManager {
public:
    explicit WALManager(const std::string& log_file_path);
    ~WALManager();

    // 禁止拷贝
    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;

    // 初始化（打开或创建日志文件）
    bool initialize();

    // 追加日志记录
    LSN appendLog(LogRecordType type, TransactionId txn_id, const uint8_t* data = nullptr, size_t size = 0);

    // 事务相关日志
    LSN logBegin(TransactionId txn_id);
    LSN logCommit(TransactionId txn_id);
    LSN logRollback(TransactionId txn_id);
    LSN logInsert(TransactionId txn_id, uint32_t table_id, uint32_t page_id, uint16_t slot_id, const uint8_t* tuple_data, size_t tuple_size);
    LSN logDelete(TransactionId txn_id, uint32_t table_id, uint32_t page_id, uint16_t slot_id);
    LSN logUpdate(TransactionId txn_id, uint32_t table_id, uint32_t page_id, uint16_t slot_id, const uint8_t* old_data, size_t old_size, const uint8_t* new_data, size_t new_size);
    LSN logNewPage(TransactionId txn_id, uint32_t page_id);

    // 强制刷盘（fsync）
    bool flush();

    // 获取当前LSN
    LSN getCurrentLSN() const { return current_lsn_.load(); }

    // 获取已刷盘的LSN
    LSN getFlushedLSN() const { return flushed_lsn_.load(); }

    // 关闭日志文件
    void close();

    // 恢复相关（阶段二仅提供接口）
    // 读取所有日志记录
    std::vector<LogRecord> readAllLogs();

    // 从指定LSN开始读取
    std::vector<LogRecord> readLogsFrom(LSN start_lsn);

private:
    std::string log_file_path_;
    std::fstream file_;
    std::mutex mutex_;
    std::atomic<LSN> current_lsn_{INVALID_LSN};
    std::atomic<LSN> flushed_lsn_{INVALID_LSN};

    // 事务的最后LSN映射（用于prev_lsn链）
    std::unordered_map<TransactionId, LSN> txn_last_lsn_;
    std::mutex txn_mutex_;

    // 内部写入方法
    bool writeLogRecord(const LogRecord& record);

    // 更新事务LSN链
    LSN getAndUpdateTxnLSN(TransactionId txn_id, LSN new_lsn);
};

} // namespace storage
} // namespace tinydb
