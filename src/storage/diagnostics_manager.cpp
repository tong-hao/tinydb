#include "diagnostics_manager.h"
#include "common/logger.h"
#include <chrono>
#include <algorithm>

namespace tinydb {
namespace storage {

// 全局诊断管理器实例
DiagnosticsManager* g_diagnostics_manager = nullptr;

// 获取当前时间戳（毫秒）
int64_t DiagnosticsManager::getCurrentTimestamp() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

DiagnosticsManager::DiagnosticsManager() {
    LOG_INFO("DiagnosticsManager initialized");
}

DiagnosticsManager::~DiagnosticsManager() {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_.clear();
    index_stats_.clear();
    activity_stats_.clear();
    lock_stats_.clear();
    buffer_stats_.clear();
}

// ========== 表统计 ==========

void DiagnosticsManager::recordSeqScan(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].seq_scan_count++;
}

void DiagnosticsManager::recordIndexScan(const std::string& table_name,
                                         const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].index_scan_count++;
}

void DiagnosticsManager::recordInsert(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].row_insert_count++;
    table_stats_[table_name].row_live_count++;
}

void DiagnosticsManager::recordUpdate(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].row_update_count++;
    // 更新会产生死行（简化处理）
    table_stats_[table_name].row_dead_count++;
}

void DiagnosticsManager::recordDelete(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].row_delete_count++;
    table_stats_[table_name].row_live_count--;
    table_stats_[table_name].row_dead_count++;
}

void DiagnosticsManager::recordVacuum(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].last_vacuum_time = getCurrentTimestamp();
    // VACUUM清理死行
    table_stats_[table_name].row_dead_count = 0;
}

void DiagnosticsManager::recordAnalyze(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_[table_name].last_analyze_time = getCurrentTimestamp();
}

TableStatsView DiagnosticsManager::getTableStats(const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = table_stats_.find(table_name);
    if (it != table_stats_.end()) {
        return it->second;
    }
    return TableStatsView();
}

std::vector<TableStatsView> DiagnosticsManager::getAllTableStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<TableStatsView> result;
    for (const auto& pair : table_stats_) {
        result.push_back(pair.second);
    }
    return result;
}

// ========== 索引统计 ==========

void DiagnosticsManager::recordIndexUsage(const std::string& table_name,
                                          const std::string& index_name,
                                          int64_t rows_read) {
    std::lock_guard<std::mutex> lock(mutex_);
    IndexStatsView& stats = index_stats_[index_name];
    stats.table_name = table_name;
    stats.index_name = index_name;
    stats.index_scan_count++;
    stats.index_read_count += rows_read;
    stats.index_fetch_count += rows_read;
}

IndexStatsView DiagnosticsManager::getIndexStats(const std::string& index_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = index_stats_.find(index_name);
    if (it != index_stats_.end()) {
        return it->second;
    }
    return IndexStatsView();
}

std::vector<IndexStatsView> DiagnosticsManager::getAllIndexStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<IndexStatsView> result;
    for (const auto& pair : index_stats_) {
        result.push_back(pair.second);
    }
    return result;
}

// ========== 活动统计 ==========

void DiagnosticsManager::startQuery(int32_t pid, const std::string& username,
                                    const std::string& query) {
    std::lock_guard<std::mutex> lock(mutex_);
    ActivityStatsView& stats = activity_stats_[pid];
    stats.process_id = pid;
    stats.username = username;
    stats.query = query;
    stats.state = "active";
    stats.query_start_time = getCurrentTimestamp();
    stats.backend_start_time = getCurrentTimestamp();
}

void DiagnosticsManager::endQuery(int32_t pid) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = activity_stats_.find(pid);
    if (it != activity_stats_.end()) {
        activity_stats_.erase(it);
    }
}

void DiagnosticsManager::setQueryState(int32_t pid, const std::string& state) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = activity_stats_.find(pid);
    if (it != activity_stats_.end()) {
        it->second.state = state;
    }
}

std::vector<ActivityStatsView> DiagnosticsManager::getCurrentActivity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ActivityStatsView> result;
    for (const auto& pair : activity_stats_) {
        result.push_back(pair.second);
    }
    return result;
}

std::vector<ActivityStatsView> DiagnosticsManager::getUserActivity(
    const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ActivityStatsView> result;
    for (const auto& pair : activity_stats_) {
        if (pair.second.username == username) {
            result.push_back(pair.second);
        }
    }
    return result;
}

// ========== 锁统计 ==========

void DiagnosticsManager::recordLockAcquire(const std::string& lock_type,
                                           const std::string& relation,
                                           int32_t pid,
                                           const std::string& mode,
                                           bool granted) {
    std::lock_guard<std::mutex> lock(mutex_);
    LockStatsView stats;
    stats.lock_type = lock_type;
    stats.relation_name = relation;
    stats.process_id = pid;
    stats.mode = mode;
    stats.granted = granted;
    stats.granted_time = getCurrentTimestamp();
    lock_stats_.push_back(stats);
}

void DiagnosticsManager::recordLockRelease(int32_t pid) {
    std::lock_guard<std::mutex> lock(mutex_);
    lock_stats_.erase(
        std::remove_if(lock_stats_.begin(), lock_stats_.end(),
            [pid](const LockStatsView& stats) {
                return stats.process_id == pid;
            }),
        lock_stats_.end()
    );
}

std::vector<LockStatsView> DiagnosticsManager::getCurrentLocks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lock_stats_;
}

// ========== 缓冲区统计 ==========

void DiagnosticsManager::recordBufferHit(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    BufferStatsView& stats = buffer_stats_[table_name];
    stats.table_name = table_name;
    stats.blocks_hit++;
    if (stats.blocks_hit + stats.blocks_read > 0) {
        stats.hit_ratio = static_cast<double>(stats.blocks_hit) /
                         (stats.blocks_hit + stats.blocks_read);
    }
}

void DiagnosticsManager::recordBufferRead(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    BufferStatsView& stats = buffer_stats_[table_name];
    stats.table_name = table_name;
    stats.blocks_read++;
    if (stats.blocks_hit + stats.blocks_read > 0) {
        stats.hit_ratio = static_cast<double>(stats.blocks_hit) /
                         (stats.blocks_hit + stats.blocks_read);
    }
}

std::vector<BufferStatsView> DiagnosticsManager::getBufferStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<BufferStatsView> result;
    for (const auto& pair : buffer_stats_) {
        result.push_back(pair.second);
    }
    return result;
}

// ========== WAL统计 ==========

void DiagnosticsManager::recordWalWrite(int64_t bytes, double write_time) {
    std::lock_guard<std::mutex> lock(mutex_);
    wal_stats_.wal_records++;
    wal_stats_.wal_bytes += bytes;
    wal_stats_.wal_write_count++;
    wal_stats_.wal_write_time += write_time;
}

void DiagnosticsManager::recordWalSync(double sync_time) {
    std::lock_guard<std::mutex> lock(mutex_);
    wal_stats_.wal_sync_count++;
    wal_stats_.wal_sync_time += sync_time;
}

WalStatsView DiagnosticsManager::getWalStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return wal_stats_;
}

// ========== 复制统计 ==========

void DiagnosticsManager::updateReplicationStats(const ReplicationStatsView& stats) {
    std::lock_guard<std::mutex> lock(mutex_);
    // 查找并更新或添加
    bool found = false;
    for (auto& existing : replication_stats_) {
        if (existing.client_addr == stats.client_addr) {
            existing = stats;
            found = true;
            break;
        }
    }
    if (!found) {
        replication_stats_.push_back(stats);
    }
}

std::vector<ReplicationStatsView> DiagnosticsManager::getReplicationStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return replication_stats_;
}

// ========== 系统统计 ==========

void DiagnosticsManager::updateSystemStats(const SystemStatsView& stats) {
    std::lock_guard<std::mutex> lock(mutex_);
    system_stats_ = stats;
}

SystemStatsView DiagnosticsManager::getSystemStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return system_stats_;
}

// ========== 诊断工具 ==========

std::vector<ActivityStatsView> DiagnosticsManager::getSlowQueries(
    double threshold_ms) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ActivityStatsView> result;
    int64_t now = getCurrentTimestamp();
    for (const auto& pair : activity_stats_) {
        double elapsed = now - pair.second.query_start_time;
        if (elapsed > threshold_ms) {
            result.push_back(pair.second);
        }
    }
    return result;
}

std::vector<ActivityStatsView> DiagnosticsManager::getBlockedQueries() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ActivityStatsView> result;
    for (const auto& pair : activity_stats_) {
        if (pair.second.state == "idle in transaction" ||
            pair.second.wait_event_type != 0) {
            result.push_back(pair.second);
        }
    }
    return result;
}

std::vector<LockStatsView> DiagnosticsManager::getLockWaits() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<LockStatsView> result;
    for (const auto& stats : lock_stats_) {
        if (!stats.granted) {
            result.push_back(stats);
        }
    }
    return result;
}

int64_t DiagnosticsManager::getDatabaseSize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return system_stats_.db_size_bytes;
}

int64_t DiagnosticsManager::getTableSize(const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = table_stats_.find(table_name);
    if (it != table_stats_.end()) {
        // 简化：返回估计大小
        return (it->second.row_live_count + it->second.row_dead_count) * 100;
    }
    return 0;
}

int64_t DiagnosticsManager::getIndexSize(const std::string& index_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = index_stats_.find(index_name);
    if (it != index_stats_.end()) {
        return it->second.index_size_bytes;
    }
    return 0;
}

// ========== 重置统计 ==========

void DiagnosticsManager::resetAllStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_.clear();
    index_stats_.clear();
    activity_stats_.clear();
    lock_stats_.clear();
    buffer_stats_.clear();
    wal_stats_ = WalStatsView();
    replication_stats_.clear();
    system_stats_ = SystemStatsView();
    LOG_INFO("All statistics reset");
}

void DiagnosticsManager::resetTableStats(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    table_stats_.erase(table_name);
}

void DiagnosticsManager::resetIndexStats(const std::string& index_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    index_stats_.erase(index_name);
}

} // namespace storage
} // namespace tinydb
