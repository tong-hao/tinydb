#include <gtest/gtest.h>
#include "storage/wal.h"
#include "storage/tuple.h"
#include <cstdio>

using namespace tinydb;
using namespace tinydb::storage;

class WALTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_wal_file_ = "test_wal.log";
        // 确保文件不存在
        std::remove(test_wal_file_.c_str());

        wal_manager_ = std::make_unique<WALManager>(test_wal_file_);
        ASSERT_TRUE(wal_manager_->initialize());
    }

    void TearDown() override {
        wal_manager_->close();
        wal_manager_.reset();
        std::remove(test_wal_file_.c_str());
    }

    std::string test_wal_file_;
    std::unique_ptr<WALManager> wal_manager_;
};

// 测试WAL初始化
TEST_F(WALTest, Initialization) {
    EXPECT_NE(wal_manager_.get(), nullptr);
    EXPECT_GE(wal_manager_->getCurrentLSN(), 1);
}

// 测试日志记录序列化和反序列化
TEST_F(WALTest, LogRecordSerializeDeserialize) {
    LogRecord record(LogRecordType::INSERT, 1, INVALID_LSN);
    record.setLSN(100);

    // 设置一些数据
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    record.setData(data);

    // 序列化
    auto serialized = record.serialize();
    EXPECT_EQ(serialized.size(), record.getSize());

    // 反序列化
    LogRecord deserialized;
    EXPECT_TRUE(deserialized.deserialize(serialized.data(), serialized.size()));

    // 验证
    EXPECT_EQ(deserialized.getType(), LogRecordType::INSERT);
    EXPECT_EQ(deserialized.getTxnId(), 1);
    EXPECT_EQ(deserialized.getLSN(), 100);
    EXPECT_EQ(deserialized.getData(), data);
}

// 测试BEGIN日志
TEST_F(WALTest, LogBegin) {
    TransactionId txn_id = 1;
    LSN lsn = wal_manager_->logBegin(txn_id);

    EXPECT_NE(lsn, INVALID_LSN);
    EXPECT_GE(lsn, 1);
}

// 测试COMMIT日志
TEST_F(WALTest, LogCommit) {
    TransactionId txn_id = 1;
    wal_manager_->logBegin(txn_id);
    LSN lsn = wal_manager_->logCommit(txn_id);

    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试ROLLBACK日志
TEST_F(WALTest, LogRollback) {
    TransactionId txn_id = 1;
    wal_manager_->logBegin(txn_id);
    LSN lsn = wal_manager_->logRollback(txn_id);

    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试INSERT日志
TEST_F(WALTest, LogInsert) {
    TransactionId txn_id = 1;
    uint32_t table_id = 10;
    uint32_t page_id = 5;
    uint16_t slot_id = 3;

    std::vector<uint8_t> tuple_data = {0x01, 0x02, 0x03, 0x04};

    LSN lsn = wal_manager_->logInsert(txn_id, table_id, page_id, slot_id,
                                       tuple_data.data(), tuple_data.size());

    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试DELETE日志
TEST_F(WALTest, LogDelete) {
    TransactionId txn_id = 1;
    uint32_t table_id = 10;
    uint32_t page_id = 5;
    uint16_t slot_id = 3;

    LSN lsn = wal_manager_->logDelete(txn_id, table_id, page_id, slot_id);

    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试UPDATE日志
TEST_F(WALTest, LogUpdate) {
    TransactionId txn_id = 1;
    uint32_t table_id = 10;
    uint32_t page_id = 5;
    uint16_t slot_id = 3;

    std::vector<uint8_t> old_data = {0x01, 0x02};
    std::vector<uint8_t> new_data = {0x03, 0x04, 0x05};

    LSN lsn = wal_manager_->logUpdate(txn_id, table_id, page_id, slot_id,
                                       old_data.data(), old_data.size(),
                                       new_data.data(), new_data.size());

    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试NEW_PAGE日志
TEST_F(WALTest, LogNewPage) {
    TransactionId txn_id = 1;
    uint32_t page_id = 100;

    LSN lsn = wal_manager_->logNewPage(txn_id, page_id);

    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试Flush
TEST_F(WALTest, Flush) {
    wal_manager_->logBegin(1);
    EXPECT_TRUE(wal_manager_->flush());

    EXPECT_GE(wal_manager_->getFlushedLSN(), 0);
}

// 测试读取所有日志
TEST_F(WALTest, ReadAllLogs) {
    // 写入多条日志
    wal_manager_->logBegin(1);
    wal_manager_->logInsert(1, 10, 1, 0, nullptr, 0);
    wal_manager_->logCommit(1);

    wal_manager_->logBegin(2);
    wal_manager_->logDelete(2, 10, 1, 0);
    wal_manager_->logRollback(2);

    // 读取所有日志
    auto records = wal_manager_->readAllLogs();
    EXPECT_EQ(records.size(), 6);

    // 验证第一条是BEGIN
    EXPECT_EQ(records[0].getType(), LogRecordType::BEGIN);
    EXPECT_EQ(records[0].getTxnId(), 1);

    // 验证第二条是INSERT
    EXPECT_EQ(records[1].getType(), LogRecordType::INSERT);
}

// 测试从指定LSN读取
TEST_F(WALTest, ReadLogsFromLSN) {
    // 写入多条日志
    wal_manager_->logBegin(1);
    LSN commit_lsn = wal_manager_->logCommit(1);

    wal_manager_->logBegin(2);
    wal_manager_->logRollback(2);

    // 从commit_lsn开始读取
    auto records = wal_manager_->readLogsFrom(commit_lsn);
    EXPECT_GE(records.size(), 3); // commit(1), begin(2), rollback(2)
}

// 测试LSN递增
TEST_F(WALTest, LSNIncrement) {
    LSN lsn1 = wal_manager_->logBegin(1);
    LSN lsn2 = wal_manager_->logInsert(1, 10, 1, 0, nullptr, 0);
    LSN lsn3 = wal_manager_->logCommit(1);

    EXPECT_LT(lsn1, lsn2);
    EXPECT_LT(lsn2, lsn3);
}

// 测试校验和计算
TEST_F(WALTest, ChecksumCalculation) {
    LogRecord record(LogRecordType::INSERT, 1, INVALID_LSN);
    record.setData(std::vector<uint8_t>{1, 2, 3, 4});

    uint32_t checksum = record.calculateChecksum();
    EXPECT_NE(checksum, 0);

    // 设置校验和并验证
    record.setChecksum(checksum);
    EXPECT_TRUE(record.verifyChecksum());

    // 修改数据后校验和应该不匹配
    record.setData(std::vector<uint8_t>{1, 2, 3, 5});
    EXPECT_FALSE(record.verifyChecksum());
}

// 测试多事务日志交错
TEST_F(WALTest, MultipleInterleavedTransactions) {
    // 事务1开始
    LSN txn1_begin = wal_manager_->logBegin(1);

    // 事务2开始
    LSN txn2_begin = wal_manager_->logBegin(2);

    // 事务1操作
    wal_manager_->logInsert(1, 10, 1, 0, nullptr, 0);

    // 事务2操作
    wal_manager_->logInsert(2, 10, 2, 0, nullptr, 0);

    // 事务1提交
    LSN txn1_commit = wal_manager_->logCommit(1);

    // 事务2回滚
    LSN txn2_rollback = wal_manager_->logRollback(2);

    // 读取所有日志
    auto records = wal_manager_->readAllLogs();
    EXPECT_EQ(records.size(), 6);

    // 验证LSN顺序
    EXPECT_LT(txn1_begin, txn2_begin);
    EXPECT_LT(txn2_begin, txn1_commit);
    EXPECT_LT(txn1_commit, txn2_rollback);
}

// 测试空数据日志
TEST_F(WALTest, EmptyDataLog) {
    LSN lsn = wal_manager_->appendLog(LogRecordType::CHECKPOINT, 0, nullptr, 0);
    EXPECT_NE(lsn, INVALID_LSN);
}

// 测试大日志数据
TEST_F(WALTest, LargeDataLog) {
    std::vector<uint8_t> large_data(1024 * 10); // 10KB数据
    for (size_t i = 0; i < large_data.size(); i++) {
        large_data[i] = static_cast<uint8_t>(i % 256);
    }

    LSN lsn = wal_manager_->logInsert(1, 10, 1, 0,
                                       large_data.data(), large_data.size());
    EXPECT_NE(lsn, INVALID_LSN);

    // 读取并验证
    auto records = wal_manager_->readAllLogs();
    EXPECT_EQ(records.size(), 1);
    // logInsert会在数据前添加元数据(14字节: table_id+page_id+slot_id+size)
    // 所以实际数据大小 = 元数据(14字节) + 原始数据(10240字节) = 10254字节
    EXPECT_EQ(records[0].getData().size(), 14 + large_data.size());
}

// 测试PrevLSN链
TEST_F(WALTest, PrevLSNChain) {
    TransactionId txn_id = 1;

    // 第一条日志
    LogRecord record1(LogRecordType::BEGIN, txn_id, INVALID_LSN);
    EXPECT_EQ(record1.getPrevLSN(), INVALID_LSN);

    // 第二条日志，链接到第一条
    LSN lsn1 = 100;
    LogRecord record2(LogRecordType::INSERT, txn_id, lsn1);
    EXPECT_EQ(record2.getPrevLSN(), lsn1);
}

// 测试日志记录大小
TEST_F(WALTest, LogRecordSize) {
    LogRecord record(LogRecordType::INSERT, 1, INVALID_LSN);

    // 空数据时大小应该是头部大小
    EXPECT_EQ(record.getSize(), LogRecordHeader::HEADER_SIZE);

    // 添加数据后
    std::vector<uint8_t> data(100);
    record.setData(data);
    EXPECT_EQ(record.getSize(), LogRecordHeader::HEADER_SIZE + 100);
}

// 测试无效的反序列化
TEST_F(WALTest, InvalidDeserialization) {
    LogRecord record;

    // 数据太小
    std::vector<uint8_t> small_data(5);
    EXPECT_FALSE(record.deserialize(small_data.data(), small_data.size()));

    // 大小字段不匹配
    std::vector<uint8_t> data(LogRecordHeader::HEADER_SIZE);
    uint32_t wrong_size = 1000; // 错误的大小
    std::memcpy(data.data(), &wrong_size, sizeof(wrong_size));
    EXPECT_FALSE(record.deserialize(data.data(), data.size()));
}
