#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <chrono>
#include <unordered_map>
#include <mutex>

namespace tinydb {
namespace storage {

// 表级统计信息
struct TableStatsView {
    std::string schema_name;        // 模式名
    std::string table_name;         // 表名
    int64_t seq_scan_count;         // 顺序扫描次数
    int64_t index_scan_count;       // 索引扫描次数
    int64_t row_insert_count;       // 插入行数
    int64_t row_update_count;       // 更新行数
    int64_t row_delete_count;       // 删除行数
    int64_t row_live_count;         // 活跃行数
    int64_t row_dead_count;         // 死行数
    int64_t last_vacuum_time;       // 上次VACUUM时间
    int64_t last_analyze_time;      // 上次ANALYZE时间
    double cache_hit_ratio;         // 缓存命中率

    TableStatsView()
        : seq_scan_count(0)
        , index_scan_count(0)
        , row_insert_count(0)
        , row_update_count(0)
        , row_delete_count(0)
        , row_live_count(0)
        , row_dead_count(0)
        , last_vacuum_time(0)
        , last_analyze_time(0)
        , cache_hit_ratio(0.0) {}
};

// 索引级统计信息
struct IndexStatsView {
    std::string schema_name;        // 模式名
    std::string table_name;         // 表名
    std::string index_name;         // 索引名
    int64_t index_scan_count;       // 索引扫描次数
    int64_t index_read_count;       // 通过索引读取的行数
    int64_t index_fetch_count;      // 通过索引获取的行数
    int64_t index_insert_count;     // 索引插入次数
    int64_t index_delete_count;     // 索引删除次数
    int64_t index_size_bytes;       // 索引大小（字节）
    double cache_hit_ratio;         // 缓存命中率

    IndexStatsView()
        : index_scan_count(0)
        , index_read_count(0)
        , index_fetch_count(0)
        , index_insert_count(0)
        , index_delete_count(0)
        , index_size_bytes(0)
        , cache_hit_ratio(0.0) {}
};

// 数据库活动统计
struct ActivityStatsView {
    int32_t process_id;             // 进程ID
    std::string username;           // 用户名
    std::string application_name;   // 应用名
    std::string client_addr;        // 客户端地址
    int32_t client_port;            // 客户端端口
    std::string database_name;      // 数据库名
    std::string query;              // 当前查询
    std::string state;              // 状态（active, idle, idle in transaction）
    int64_t query_start_time;       // 查询开始时间
    int64_t transaction_start_time; // 事务开始时间
    int64_t backend_start_time;     // 后端启动时间
    int64_t wait_event_type;        // 等待事件类型
    std::string wait_event;         // 等待事件

    ActivityStatsView()
        : process_id(0)
        , client_port(0)
        , query_start_time(0)
        , transaction_start_time(0)
        , backend_start_time(0)
        , wait_event_type(0) {}
};

// 锁统计信息
struct LockStatsView {
    std::string lock_type;          // 锁类型
    std::string database_name;      // 数据库名
    std::string relation_name;      // 关系名（表名）
    int32_t process_id;             // 持有锁的进程ID
    std::string mode;               // 锁模式（ShareLock, ExclusiveLock等）
    bool granted;                   // 是否已授予
    int64_t granted_time;           // 授予时间

    LockStatsView()
        : process_id(0)
        , granted(false)
        , granted_time(0) {}
};

// 缓冲区统计
struct BufferStatsView {
    std::string database_name;      // 数据库名
    std::string table_name;         // 表名
    int64_t blocks_read;            // 从磁盘读取的块数
    int64_t blocks_hit;             // 从缓存命中的块数
    double hit_ratio;               // 命中率

    BufferStatsView()
        : blocks_read(0)
        , blocks_hit(0)
        , hit_ratio(0.0) {}
};

// WAL统计
struct WalStatsView {
    int64_t wal_records;            // WAL记录数
    int64_t wal_fpi;                // 全页写次数
    int64_t wal_bytes;              // WAL字节数
    int64_t wal_buffers_full;       // WAL缓冲区满次数
    int64_t wal_write_count;        // WAL写入次数
    int64_t wal_sync_count;         // WAL同步次数
    double wal_write_time;          // WAL写入时间（毫秒）
    double wal_sync_time;           // WAL同步时间（毫秒）

    WalStatsView()
        : wal_records(0)
        , wal_fpi(0)
        , wal_bytes(0)
        , wal_buffers_full(0)
        , wal_write_count(0)
        , wal_sync_count(0)
        , wal_write_time(0.0)
        , wal_sync_time(0.0) {}
};

// 复制统计
struct ReplicationStatsView {
    std::string client_addr;        // 客户端地址
    std::string state;              // 状态
    int64_t sent_lsn;               // 已发送LSN
    int64_t write_lsn;              // 已写入LSN
    int64_t flush_lsn;              // 已刷盘LSN
    int64_t replay_lsn;             // 已重放LSN
    double write_lag;               // 写入延迟（字节）
    double flush_lag;               // 刷盘延迟（字节）
    double replay_lag;              // 重放延迟（字节）
    int64_t sync_priority;          // 同步优先级
    std::string sync_state;         // 同步状态

    ReplicationStatsView()
        : sent_lsn(0)
        , write_lsn(0)
        , flush_lsn(0)
        , replay_lsn(0)
        , write_lag(0.0)
        , flush_lag(0.0)
        , replay_lag(0.0)
        , sync_priority(0) {}
};

// 系统级统计
struct SystemStatsView {
    int64_t db_size_bytes;          // 数据库大小
    int64_t free_space_bytes;       // 空闲空间
    int64_t transaction_count;      // 事务总数
    int64_t commit_count;           // 提交数
    int64_t rollback_count;         // 回滚数
    int64_t block_read_count;       // 块读取数
    int64_t block_write_count;      // 块写入数
    int64_t connection_count;       // 当前连接数
    int64_t max_connections;        // 最大连接数
    double cpu_usage_percent;       // CPU使用率
    double memory_usage_percent;    // 内存使用率

    SystemStatsView()
        : db_size_bytes(0)
        , free_space_bytes(0)
        , transaction_count(0)
        , commit_count(0)
        , rollback_count(0)
        , block_read_count(0)
        , block_write_count(0)
        , connection_count(0)
        , max_connections(100)
        , cpu_usage_percent(0.0)
        , memory_usage_percent(0.0) {}
};

// 诊断管理器（类似PostgreSQL的统计收集器）
class DiagnosticsManager {
public:
    DiagnosticsManager();
    ~DiagnosticsManager();

    // 禁止拷贝
    DiagnosticsManager(const DiagnosticsManager&) = delete;
    DiagnosticsManager& operator=(const DiagnosticsManager&) = delete;

    // ========== 表统计 ==========
    // 记录顺序扫描
    void recordSeqScan(const std::string& table_name);

    // 记录索引扫描
    void recordIndexScan(const std::string& table_name, const std::string& index_name);

    // 记录插入
    void recordInsert(const std::string& table_name);

    // 记录更新
    void recordUpdate(const std::string& table_name);

    // 记录删除
    void recordDelete(const std::string& table_name);

    // 记录VACUUM
    void recordVacuum(const std::string& table_name);

    // 记录ANALYZE
    void recordAnalyze(const std::string& table_name);

    // 获取表统计
    TableStatsView getTableStats(const std::string& table_name) const;

    // 获取所有表统计
    std::vector<TableStatsView> getAllTableStats() const;

    // ========== 索引统计 ==========
    // 记录索引使用
    void recordIndexUsage(const std::string& table_name,
                          const std::string& index_name,
                          int64_t rows_read);

    // 获取索引统计
    IndexStatsView getIndexStats(const std::string& index_name) const;

    // 获取所有索引统计
    std::vector<IndexStatsView> getAllIndexStats() const;

    // ========== 活动统计 ==========
    // 开始跟踪查询
    void startQuery(int32_t pid, const std::string& username,
                    const std::string& query);

    // 结束查询
    void endQuery(int32_t pid);

    // 设置查询状态
    void setQueryState(int32_t pid, const std::string& state);

    // 获取当前活动
    std::vector<ActivityStatsView> getCurrentActivity() const;

    // 获取当前活动（特定用户）
    std::vector<ActivityStatsView> getUserActivity(const std::string& username) const;

    // ========== 锁统计 ==========
    // 记录锁获取
    void recordLockAcquire(const std::string& lock_type,
                           const std::string& relation,
                           int32_t pid,
                           const std::string& mode,
                           bool granted);

    // 记录锁释放
    void recordLockRelease(int32_t pid);

    // 获取当前锁
    std::vector<LockStatsView> getCurrentLocks() const;

    // ========== 缓冲区统计 ==========
    // 记录缓冲区命中
    void recordBufferHit(const std::string& table_name);

    // 记录缓冲区读取
    void recordBufferRead(const std::string& table_name);

    // 获取缓冲区统计
    std::vector<BufferStatsView> getBufferStats() const;

    // ========== WAL统计 ==========
    // 记录WAL写入
    void recordWalWrite(int64_t bytes, double write_time);

    // 记录WAL同步
    void recordWalSync(double sync_time);

    // 获取WAL统计
    WalStatsView getWalStats() const;

    // ========== 复制统计 ==========
    // 更新复制统计
    void updateReplicationStats(const ReplicationStatsView& stats);

    // 获取复制统计
    std::vector<ReplicationStatsView> getReplicationStats() const;

    // ========== 系统统计 ==========
    // 更新系统统计
    void updateSystemStats(const SystemStatsView& stats);

    // 获取系统统计
    SystemStatsView getSystemStats() const;

    // ========== 诊断工具 ==========
    // 获取慢查询（执行时间超过阈值）
    std::vector<ActivityStatsView> getSlowQueries(double threshold_ms) const;

    // 获取阻塞查询
    std::vector<ActivityStatsView> getBlockedQueries() const;

    // 获取锁等待信息
    std::vector<LockStatsView> getLockWaits() const;

    // 获取数据库大小
    int64_t getDatabaseSize() const;

    // 获取表大小
    int64_t getTableSize(const std::string& table_name) const;

    // 获取索引大小
    int64_t getIndexSize(const std::string& index_name) const;

    // ========== 重置统计 ==========
    // 重置所有统计
    void resetAllStats();

    // 重置表统计
    void resetTableStats(const std::string& table_name);

    // 重置索引统计
    void resetIndexStats(const std::string& index_name);

private:
    mutable std::mutex mutex_;

    // 表统计
    std::unordered_map<std::string, TableStatsView> table_stats_;

    // 索引统计
    std::unordered_map<std::string, IndexStatsView> index_stats_;

    // 活动统计
    std::unordered_map<int32_t, ActivityStatsView> activity_stats_;

    // 锁统计
    std::vector<LockStatsView> lock_stats_;

    // 缓冲区统计
    std::unordered_map<std::string, BufferStatsView> buffer_stats_;

    // WAL统计
    WalStatsView wal_stats_;

    // 复制统计
    std::vector<ReplicationStatsView> replication_stats_;

    // 系统统计
    SystemStatsView system_stats_;

    // 获取当前时间戳
    int64_t getCurrentTimestamp() const;
};

// 全局诊断管理器
extern DiagnosticsManager* g_diagnostics_manager;

} // namespace storage
} // namespace tinydb
