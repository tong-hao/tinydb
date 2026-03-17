#pragma once

#include "connection.h"
#include "common/logger.h"
#include <thread>
#include <vector>
#include <atomic>
#include <netinet/in.h>

namespace tinydb {
namespace network {

// 请求处理器接口
class RequestHandler {
public:
    virtual ~RequestHandler() = default;
    virtual void handleRequest(Connection& conn, MessageType type, const buffer_t& body) = 0;
};

// TCP 服务器
class Server {
public:
    Server(uint16_t port, RequestHandler* handler);
    ~Server();

    // 禁止拷贝
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // 启动服务器（阻塞）
    void run();

    // 停止服务器
    void stop();

    // 获取状态
    bool isRunning() const { return running_.load(); }
    uint16_t port() const { return port_; }

private:
    // 接受连接循环
    void acceptLoop();

    // 处理客户端连接
    void handleClient(Connection conn);

    uint16_t port_;
    int listen_fd_;
    std::atomic<bool> running_;
    RequestHandler* request_handler_;
    std::vector<std::thread> client_threads_;
    std::mutex threads_mutex_;
};

} // namespace network
} // namespace tinydb
