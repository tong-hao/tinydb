#pragma once

#include "protocol.h"
#include "common/error.h"
#include "common/logger.h"
#include <functional>
#include <atomic>
#include <sys/socket.h>
#include <unistd.h>

namespace tinydb {
namespace network {

// 连接状态
enum class ConnectionState {
    CONNECTING,
    CONNECTED,
    CLOSING,
    CLOSED
};

// 连接类
class Connection {
public:
    using MessageHandler = std::function<void(MessageType, const buffer_t&)>;
    using CloseHandler = std::function<void()>;

    Connection(int socket_fd);
    ~Connection();

    // 禁止拷贝
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // 允许移动
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    // 启动消息处理循环（阻塞）
    void run();

    // 发送消息
    Error sendMessage(MessageType type, const buffer_t& body);
    Error sendMessage(MessageType type);

    // 关闭连接
    void close();

    // 获取状态
    ConnectionState state() const { return state_.load(); }
    int socketFd() const { return socket_fd_; }

    // 设置回调
    void setMessageHandler(MessageHandler handler) { message_handler_ = std::move(handler); }
    void setCloseHandler(CloseHandler handler) { close_handler_ = std::move(handler); }

private:
    // 读取完整消息
    Error readMessage(MessageType& type, buffer_t& body);

    // 读取指定字节数
    Error readExact(byte_t* buffer, size_t length);

    // 发送指定字节数
    Error sendExact(const byte_t* buffer, size_t length);

    int socket_fd_;
    std::atomic<ConnectionState> state_;
    MessageHandler message_handler_;
    CloseHandler close_handler_;
};

} // namespace network
} // namespace tinydb
