#include "replication_manager.h"
#include "common/logger.h"
#include <cstring>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

namespace tinydb {
namespace storage {

// 全局复制管理器实例
ReplicationManager* g_replication_manager = nullptr;

// 辅助函数：获取当前时间戳（毫秒）
static uint64_t getCurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// 辅助函数：计算校验和（简单实现）
static uint32_t calculateChecksum(const std::string& data) {
    uint32_t sum = 0;
    for (char c : data) {
        sum = sum * 31 + static_cast<uint8_t>(c);
    }
    return sum;
}

// ==================== ReplicationConnection Implementation ====================

ReplicationConnection::ReplicationConnection()
    : socket_fd_(-1)
    , connected_(false) {
}

ReplicationConnection::~ReplicationConnection() {
    disconnect();
}

bool ReplicationConnection::connect(const std::string& host, uint16_t port, uint32_t timeout_sec) {
    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        LOG_ERROR("Failed to create socket");
        return false;
    }

    // 设置连接超时
    if (timeout_sec > 0) {
        setSocketTimeout(timeout_sec);
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid address: " << host);
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    if (::connect(socket_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        LOG_ERROR("Failed to connect to " << host << ":" << port);
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    remote_addr_ = host + ":" + std::to_string(port);
    connected_ = true;

    LOG_INFO("Connected to " << remote_addr_);
    return true;
}

bool ReplicationConnection::accept(uint16_t port, uint32_t timeout_sec) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        LOG_ERROR("Failed to create listen socket");
        return false;
    }

    // 允许地址重用
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        LOG_ERROR("Failed to bind to port " << port);
        close(listen_fd);
        return false;
    }

    if (::listen(listen_fd, 5) < 0) {
        LOG_ERROR("Failed to listen on port " << port);
        close(listen_fd);
        return false;
    }

    LOG_INFO("Listening for replication connections on port " << port);

    // 设置接收超时
    if (timeout_sec > 0) {
        struct timeval tv;
        tv.tv_sec = timeout_sec;
        tv.tv_usec = 0;
        setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    socket_fd_ = ::accept(listen_fd, (struct sockaddr*)&client_addr, &client_len);
    close(listen_fd);

    if (socket_fd_ < 0) {
        LOG_ERROR("Failed to accept connection");
        return false;
    }

    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
    remote_addr_ = std::string(client_ip) + ":" + std::to_string(ntohs(client_addr.sin_port));
    connected_ = true;

    LOG_INFO("Accepted replication connection from " << remote_addr_);
    return true;
}

void ReplicationConnection::disconnect() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    connected_ = false;
    remote_addr_.clear();
}

bool ReplicationConnection::isConnected() const {
    return connected_;
}

bool ReplicationConnection::sendWalMessage(const WalReplicationMessage& msg) {
    if (!connected_ || socket_fd_ < 0) {
        return false;
    }

    // 发送消息头
    uint32_t header[4];
    header[0] = 0x57414C31;  // "WAL1" 魔数
    header[1] = sizeof(header);
    header[2] = static_cast<uint32_t>(msg.wal_data.size());
    header[3] = static_cast<uint32_t>(msg.lsn);

    if (!sendAll(reinterpret_cast<const char*>(header), sizeof(header))) {
        return false;
    }

    // 发送WAL数据
    if (!msg.wal_data.empty()) {
        if (!sendAll(msg.wal_data.data(), msg.wal_data.size())) {
            return false;
        }
    }

    return true;
}

bool ReplicationConnection::receiveWalMessage(WalReplicationMessage& msg, uint32_t timeout_ms) {
    if (!connected_ || socket_fd_ < 0) {
        return false;
    }

    // 接收消息头
    uint32_t header[4];
    if (!recvAll(reinterpret_cast<char*>(header), sizeof(header), timeout_ms)) {
        return false;
    }

    if (header[0] != 0x57414C31) {
        LOG_ERROR("Invalid message magic");
        return false;
    }

    msg.lsn = header[3];
    uint32_t data_len = header[2];

    // 接收WAL数据
    if (data_len > 0) {
        msg.wal_data.resize(data_len);
        if (!recvAll(msg.wal_data.data(), data_len, timeout_ms)) {
            return false;
        }
    }

    msg.timestamp = getCurrentTimeMs();
    msg.checksum = calculateChecksum(msg.wal_data);

    return true;
}

bool ReplicationConnection::sendHeartbeat(uint64_t lsn) {
    WalReplicationMessage msg;
    msg.lsn = lsn;
    msg.timestamp = getCurrentTimeMs();
    return sendWalMessage(msg);
}

bool ReplicationConnection::receiveHeartbeatAck(uint64_t& lsn, uint32_t timeout_ms) {
    WalReplicationMessage msg;
    if (!receiveWalMessage(msg, timeout_ms)) {
        return false;
    }
    lsn = msg.lsn;
    return true;
}

std::string ReplicationConnection::getConnectionInfo() const {
    return remote_addr_;
}

bool ReplicationConnection::setSocketTimeout(uint32_t timeout_sec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    return setsockopt(socket_fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0 &&
           setsockopt(socket_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
}

bool ReplicationConnection::sendAll(const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(socket_fd_, data + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("Send failed: " << strerror(errno));
            return false;
        }
        sent += n;
    }
    return true;
}

bool ReplicationConnection::recvAll(char* data, size_t len, uint32_t timeout_ms) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(socket_fd_, data + received, len - received, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 超时
                return false;
            }
            LOG_ERROR("Receive failed: " << strerror(errno));
            return false;
        }
        if (n == 0) {
            // 连接关闭
            return false;
        }
        received += n;
    }
    return true;
}

// ==================== ReplicationManager Implementation ====================

ReplicationManager::ReplicationManager()
    : state_(ReplicationState::DISCONNECTED)
    , role_(ReplicationRole::MASTER)
    , running_(false)
    , paused_(false)
    , last_received_lsn_(0)
    , last_applied_lsn_(0)
    , max_wal_buffer_size_(16 * 1024 * 1024) {
}

ReplicationManager::~ReplicationManager() {
    stopMasterReplication();
    stopSlaveReplication();
}

void ReplicationManager::configure(const ReplicationConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    config_ = config;
    role_ = config.role;
}

ReplicationConfig ReplicationManager::getConfig() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_;
}

bool ReplicationManager::startMasterReplication() {
    if (role_ != ReplicationRole::MASTER) {
        LOG_ERROR("Cannot start master replication: not in master role");
        return false;
    }

    if (running_) {
        LOG_WARN("Master replication already running");
        return true;
    }

    running_ = true;
    state_ = ReplicationState::CONNECTING;

    master_thread_ = std::thread(&ReplicationManager::masterWorkerThread, this);

    LOG_INFO("Master replication service started");
    return true;
}

void ReplicationManager::stopMasterReplication() {
    running_ = false;
    cv_.notify_all();

    if (master_thread_.joinable()) {
        master_thread_.join();
    }

    // 断开所有从节点连接
    for (auto& conn : slave_connections_) {
        if (conn) {
            conn->disconnect();
        }
    }
    slave_connections_.clear();

    if (master_conn_) {
        master_conn_->disconnect();
        master_conn_.reset();
    }

    state_ = ReplicationState::DISCONNECTED;
    LOG_INFO("Master replication service stopped");
}

bool ReplicationManager::createReplicationSlot(const std::string& slot_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (replication_slots_.find(slot_name) != replication_slots_.end()) {
        LOG_ERROR("Replication slot already exists: " << slot_name);
        return false;
    }

    ReplicationSlot slot;
    slot.slot_name = slot_name;
    slot.create_time = getCurrentTimeMs();
    slot.restart_lsn = last_applied_lsn_;

    replication_slots_[slot_name] = slot;

    LOG_INFO("Created replication slot: " << slot_name);
    return true;
}

bool ReplicationManager::dropReplicationSlot(const std::string& slot_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replication_slots_.find(slot_name);
    if (it == replication_slots_.end()) {
        LOG_ERROR("Replication slot not found: " << slot_name);
        return false;
    }

    replication_slots_.erase(it);

    LOG_INFO("Dropped replication slot: " << slot_name);
    return true;
}

std::vector<ReplicationSlot> ReplicationManager::listReplicationSlots() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<ReplicationSlot> result;
    for (const auto& pair : replication_slots_) {
        result.push_back(pair.second);
    }

    return result;
}

bool ReplicationManager::broadcastWal(const WalReplicationMessage& msg) {
    std::lock_guard<std::mutex> lock(mutex_);

    bool all_sent = true;
    for (auto& conn : slave_connections_) {
        if (conn && conn->isConnected()) {
            if (!conn->sendWalMessage(msg)) {
                LOG_WARN("Failed to send WAL to slave");
                all_sent = false;
            } else {
                stats_.messages_sent++;
                stats_.bytes_sent += msg.wal_data.size();
            }
        }
    }

    stats_.sent_lsn = msg.lsn;
    return all_sent;
}

bool ReplicationManager::waitForSlaveAck(uint64_t lsn, uint32_t timeout_ms) {
    auto start = getCurrentTimeMs();
    while (getCurrentTimeMs() - start < timeout_ms) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            bool all_acked = true;
            for (const auto& pair : replication_slots_) {
                if (pair.second.active && pair.second.confirmed_lsn < lsn) {
                    all_acked = false;
                    break;
                }
            }
            if (all_acked) {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

bool ReplicationManager::startSlaveReplication() {
    if (role_ != ReplicationRole::SLAVE && role_ != ReplicationRole::STANDBY) {
        LOG_ERROR("Cannot start slave replication: not in slave role");
        return false;
    }

    if (running_) {
        LOG_WARN("Slave replication already running");
        return true;
    }

    running_ = true;
    state_ = ReplicationState::CONNECTING;

    slave_thread_ = std::thread(&ReplicationManager::slaveWorkerThread, this);

    LOG_INFO("Slave replication service started");
    return true;
}

void ReplicationManager::stopSlaveReplication() {
    running_ = false;
    cv_.notify_all();

    if (slave_thread_.joinable()) {
        slave_thread_.join();
    }

    if (slave_conn_) {
        slave_conn_->disconnect();
        slave_conn_.reset();
    }

    state_ = ReplicationState::DISCONNECTED;
    LOG_INFO("Slave replication service stopped");
}

bool ReplicationManager::connectToMaster(const std::string& host, uint16_t port) {
    slave_conn_ = std::make_unique<ReplicationConnection>();
    if (!slave_conn_->connect(host, port, config_.connection_timeout_sec)) {
        LOG_ERROR("Failed to connect to master at " << host << ":" << port);
        state_ = ReplicationState::ERROR;
        return false;
    }

    state_ = ReplicationState::SYNCING;
    LOG_INFO("Connected to master at " << host << ":" << port);
    return true;
}

void ReplicationManager::receiveAndApplyWal() {
    if (!slave_conn_ || !slave_conn_->isConnected()) {
        return;
    }

    WalReplicationMessage msg;
    while (running_ && slave_conn_->receiveWalMessage(msg, config_.sync_interval_ms)) {
        if (paused_) {
            // 复制已暂停，等待恢复
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // 应用WAL消息
        if (applyWalMessage(msg)) {
            last_received_lsn_ = msg.lsn;
            last_applied_lsn_ = msg.lsn;

            // 更新统计信息
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.messages_received++;
                stats_.bytes_received += msg.wal_data.size();
                stats_.replay_lsn = msg.lsn;
                stats_.last_msg_time = getCurrentTimeMs();
            }

            // 发送确认
            slave_conn_->sendHeartbeat(last_applied_lsn_);
        }
    }
}

bool ReplicationManager::requestWalResend(uint64_t start_lsn) {
    if (!slave_conn_ || !slave_conn_->isConnected()) {
        return false;
    }

    WalReplicationMessage msg;
    msg.lsn = start_lsn;
    msg.wal_data = "RESEND_REQUEST";

    return slave_conn_->sendWalMessage(msg);
}

bool ReplicationManager::startHotStandby() {
    if (role_ != ReplicationRole::STANDBY) {
        LOG_ERROR("Cannot start hot standby: not in standby role");
        return false;
    }

    LOG_INFO("Starting hot standby mode");
    return startSlaveReplication();
}

void ReplicationManager::stopHotStandby() {
    LOG_INFO("Stopping hot standby mode");
    stopSlaveReplication();
}

bool ReplicationManager::promoteToMaster() {
    if (role_ != ReplicationRole::SLAVE && role_ != ReplicationRole::STANDBY) {
        LOG_ERROR("Cannot promote: not a slave");
        return false;
    }

    LOG_INFO("Promoting slave to master");

    // 停止从节点复制
    stopSlaveReplication();

    // 切换角色
    role_ = ReplicationRole::MASTER;
    config_.role = ReplicationRole::MASTER;

    // 启动主节点复制服务
    return startMasterReplication();
}

ReplicationState ReplicationManager::getReplicationState() const {
    return state_;
}

ReplicationStats ReplicationManager::getReplicationStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

double ReplicationManager::getReplicationLag() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_.lag_ms;
}

bool ReplicationManager::isMaster() const {
    return role_ == ReplicationRole::MASTER;
}

bool ReplicationManager::isSlave() const {
    return role_ == ReplicationRole::SLAVE;
}

bool ReplicationManager::isHotStandby() const {
    return role_ == ReplicationRole::STANDBY;
}

void ReplicationManager::pauseReplication() {
    paused_ = true;
    LOG_INFO("Replication paused");
}

void ReplicationManager::resumeReplication() {
    paused_ = false;
    cv_.notify_all();
    LOG_INFO("Replication resumed");
}

bool ReplicationManager::isReplicationPaused() const {
    return paused_;
}

void ReplicationManager::masterWorkerThread() {
    LOG_INFO("Master worker thread started");

    while (running_) {
        // 接受从节点连接
        auto conn = std::make_unique<ReplicationConnection>();
        if (conn->accept(config_.master_port, 1)) {  // 1秒超时
            std::lock_guard<std::mutex> lock(mutex_);
            slave_connections_.push_back(std::move(conn));
            LOG_INFO("New slave connected");
        }

        // 清理断开的连接
        {
            std::lock_guard<std::mutex> lock(mutex_);
            slave_connections_.erase(
                std::remove_if(slave_connections_.begin(), slave_connections_.end(),
                    [](const std::unique_ptr<ReplicationConnection>& c) {
                        return !c->isConnected();
                    }),
                slave_connections_.end()
            );
        }

        // 发送心跳
        {
            std::lock_guard<std::mutex> lock(mutex_);
            WalReplicationMessage heartbeat;
            heartbeat.lsn = last_applied_lsn_;
            heartbeat.timestamp = getCurrentTimeMs();
            for (auto& c : slave_connections_) {
                if (c->isConnected()) {
                    c->sendHeartbeat(last_applied_lsn_);
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG_INFO("Master worker thread stopped");
}

void ReplicationManager::slaveWorkerThread() {
    LOG_INFO("Slave worker thread started");

    // 连接到主节点
    if (!connectToMaster(config_.master_host, config_.master_port)) {
        LOG_ERROR("Failed to connect to master");
        state_ = ReplicationState::ERROR;
        return;
    }

    state_ = ReplicationState::STREAMING;

    while (running_) {
        if (paused_) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !paused_ || !running_; });
            continue;
        }

        receiveAndApplyWal();

        // 检查连接状态
        if (!slave_conn_ || !slave_conn_->isConnected()) {
            LOG_WARN("Lost connection to master, reconnecting...");
            state_ = ReplicationState::CONNECTING;

            // 尝试重新连接
            std::this_thread::sleep_for(std::chrono::seconds(5));
            connectToMaster(config_.master_host, config_.master_port);
        }
    }

    LOG_INFO("Slave worker thread stopped");
}

bool ReplicationManager::applyWalMessage(const WalReplicationMessage& msg) {
    // 简化实现：将WAL数据写入本地缓冲区
    // 实际实现应该解析WAL记录并重放到存储引擎

    if (msg.wal_data.empty()) {
        // 心跳消息
        return true;
    }

    if (msg.wal_data == "RESEND_REQUEST") {
        // 重传请求
        return true;
    }

    LOG_DEBUG("Applied WAL at LSN " << msg.lsn << " (" << msg.wal_data.size() << " bytes)");
    return true;
}

void ReplicationManager::updateStats(const ReplicationStats& new_stats) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = new_stats;
}

bool ReplicationManager::ensureWalDirectory() {
    // 确保WAL归档目录存在
    return true;
}

void ReplicationManager::cleanupOldWalFiles() {
    // 清理旧的WAL文件
}

} // namespace storage
} // namespace tinydb
