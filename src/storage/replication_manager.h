#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>

namespace tinydb {
namespace storage {

// 复制状态
enum class ReplicationState {
    DISCONNECTED,   // 未连接
    CONNECTING,     // 连接中
    SYNCING,        // 同步中
    STREAMING,      // 流复制中
    ERROR           // 错误状态
};

// 复制角色
enum class ReplicationRole {
    MASTER,         // 主节点
    SLAVE,          // 从节点
    STANDBY         // 热备节点（可读从节点）
};

// WAL 复制消息
struct WalReplicationMessage {
    uint64_t lsn;           // LSN位置
    std::string wal_data;   // WAL数据
    uint64_t timestamp;     // 时间戳
    uint32_t checksum;      // 校验和

    WalReplicationMessage() : lsn(0), timestamp(0), checksum(0) {}
};

// 复制配置
struct ReplicationConfig {
    ReplicationRole role;               // 复制角色
    std::string master_host;            // 主节点地址
    uint16_t master_port;               // 主节点端口
    std::string replication_slot;       // 复制槽名称
    uint64_t sync_interval_ms;          // 同步间隔（毫秒）
    uint32_t max_wal_size_mb;           // 最大WAL缓存大小（MB）
    bool synchronous_commit;            // 是否同步提交
    uint32_t connection_timeout_sec;    // 连接超时（秒）
    uint32_t keepalive_interval_sec;    // 保活间隔（秒）

    ReplicationConfig()
        : role(ReplicationRole::MASTER)
        , master_port(5433)
        , sync_interval_ms(1000)
        , max_wal_size_mb(16)
        , synchronous_commit(false)
        , connection_timeout_sec(10)
        , keepalive_interval_sec(60) {}
};

// 复制统计信息
struct ReplicationStats {
    uint64_t sent_lsn;          // 已发送LSN
    uint64_t write_lsn;         // 已写入LSN
    uint64_t flush_lsn;         // 已刷盘LSN
    uint64_t replay_lsn;        // 已重放LSN
    uint64_t lag_bytes;         // 复制延迟（字节）
    double lag_ms;              // 复制延迟（毫秒）
    uint64_t messages_sent;     // 发送消息数
    uint64_t messages_received; // 接收消息数
    uint64_t bytes_sent;        // 发送字节数
    uint64_t bytes_received;    // 接收字节数
    uint64_t last_msg_time;     // 最后消息时间

    ReplicationStats()
        : sent_lsn(0)
        , write_lsn(0)
        , flush_lsn(0)
        , replay_lsn(0)
        , lag_bytes(0)
        , lag_ms(0.0)
        , messages_sent(0)
        , messages_received(0)
        , bytes_sent(0)
        , bytes_received(0)
        , last_msg_time(0) {}
};

// 复制连接（用于网络通信）
class ReplicationConnection {
public:
    ReplicationConnection();
    ~ReplicationConnection();

    // 禁止拷贝
    ReplicationConnection(const ReplicationConnection&) = delete;
    ReplicationConnection& operator=(const ReplicationConnection&) = delete;

    // 连接到主节点（从节点使用）
    bool connect(const std::string& host, uint16_t port, uint32_t timeout_sec);

    // 接受从节点连接（主节点使用）
    bool accept(uint16_t port, uint32_t timeout_sec);

    // 断开连接
    void disconnect();

    // 检查是否连接
    bool isConnected() const;

    // 发送WAL消息
    bool sendWalMessage(const WalReplicationMessage& msg);

    // 接收WAL消息
    bool receiveWalMessage(WalReplicationMessage& msg, uint32_t timeout_ms);

    // 发送心跳
    bool sendHeartbeat(uint64_t lsn);

    // 接收心跳确认
    bool receiveHeartbeatAck(uint64_t& lsn, uint32_t timeout_ms);

    // 获取连接信息
    std::string getConnectionInfo() const;

private:
    int socket_fd_;
    std::string remote_addr_;
    std::atomic<bool> connected_;

    // 内部方法
    bool setSocketTimeout(uint32_t timeout_sec);
    bool sendAll(const char* data, size_t len);
    bool recvAll(char* data, size_t len, uint32_t timeout_ms);
};

// 复制槽（用于管理从节点）
struct ReplicationSlot {
    std::string slot_name;      // 槽名称
    std::string client_addr;    // 客户端地址
    uint64_t confirmed_lsn;     // 确认的LSN
    uint64_t restart_lsn;       // 重启LSN
    bool active;                // 是否活跃
    uint64_t create_time;       // 创建时间
    uint64_t last_msg_time;     // 最后消息时间

    ReplicationSlot()
        : confirmed_lsn(0)
        , restart_lsn(0)
        , active(false)
        , create_time(0)
        , last_msg_time(0) {}
};

// 复制管理器
class ReplicationManager {
public:
    ReplicationManager();
    ~ReplicationManager();

    // 禁止拷贝
    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    // ========== 初始化配置 ==========
    // 配置复制角色
    void configure(const ReplicationConfig& config);

    // 获取当前配置
    ReplicationConfig getConfig() const;

    // ========== 主节点操作 ==========
    // 启动主节点复制服务
    bool startMasterReplication();

    // 停止主节点复制服务
    void stopMasterReplication();

    // 创建复制槽
    bool createReplicationSlot(const std::string& slot_name);

    // 删除复制槽
    bool dropReplicationSlot(const std::string& slot_name);

    // 获取复制槽列表
    std::vector<ReplicationSlot> listReplicationSlots() const;

    // 广播WAL到所有从节点
    bool broadcastWal(const WalReplicationMessage& msg);

    // 等待从节点确认（同步复制）
    bool waitForSlaveAck(uint64_t lsn, uint32_t timeout_ms);

    // ========== 从节点操作 ==========
    // 启动从节点复制服务
    bool startSlaveReplication();

    // 停止从节点复制服务
    void stopSlaveReplication();

    // 连接到主节点
    bool connectToMaster(const std::string& host, uint16_t port);

    // 从主节点接收WAL并应用
    void receiveAndApplyWal();

    // 请求WAL重传
    bool requestWalResend(uint64_t start_lsn);

    // ========== 热备操作 ==========
    // 启动热备模式（可读从节点）
    bool startHotStandby();

    // 停止热备模式
    void stopHotStandby();

    // 提升从节点为主节点（故障转移）
    bool promoteToMaster();

    // ========== 状态查询 ==========
    // 获取复制状态
    ReplicationState getReplicationState() const;

    // 获取复制统计信息
    ReplicationStats getReplicationStats() const;

    // 获取复制延迟
    double getReplicationLag() const;

    // 检查是否为主节点
    bool isMaster() const;

    // 检查是否为从节点
    bool isSlave() const;

    // 检查是否为热备节点
    bool isHotStandby() const;

    // ========== 同步控制 ==========
    // 暂停复制
    void pauseReplication();

    // 恢复复制
    void resumeReplication();

    // 检查复制是否暂停
    bool isReplicationPaused() const;

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    ReplicationConfig config_;
    std::atomic<ReplicationState> state_;
    std::atomic<ReplicationRole> role_;
    std::atomic<bool> running_;
    std::atomic<bool> paused_;

    // 主节点相关
    std::unique_ptr<ReplicationConnection> master_conn_;
    std::thread master_thread_;
    std::vector<std::unique_ptr<ReplicationConnection>> slave_connections_;
    std::unordered_map<std::string, ReplicationSlot> replication_slots_;

    // 从节点相关
    std::unique_ptr<ReplicationConnection> slave_conn_;
    std::thread slave_thread_;
    uint64_t last_received_lsn_;
    uint64_t last_applied_lsn_;

    // WAL缓冲区
    std::queue<WalReplicationMessage> wal_buffer_;
    std::mutex wal_buffer_mutex_;
    std::condition_variable wal_buffer_cv_;
    size_t max_wal_buffer_size_;

    // 统计信息
    ReplicationStats stats_;
    mutable std::mutex stats_mutex_;

    // 内部方法
    void masterWorkerThread();
    void slaveWorkerThread();
    bool applyWalMessage(const WalReplicationMessage& msg);
    void updateStats(const ReplicationStats& new_stats);
    bool ensureWalDirectory();
    void cleanupOldWalFiles();
};

// 全局复制管理器
extern ReplicationManager* g_replication_manager;

} // namespace storage
} // namespace tinydb
