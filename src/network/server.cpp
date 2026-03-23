#include "server.h"
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <sys/select.h>

namespace tinydb {
namespace network {

Server::Server(uint16_t port, RequestHandler* handler)
    : port_(port)
    , listen_fd_(-1)
    , running_(false)
    , request_handler_(handler) {
}

Server::~Server() {
    stop();
}

void Server::run() {
    // 创建 socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        LOG_ERROR("Failed to create socket: " << strerror(errno));
        return;
    }

    // 设置地址重用
    int reuse = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        LOG_ERROR("Failed to set SO_REUSEADDR: " << strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    // 绑定地址
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG_ERROR("Failed to bind to port " << port_ << ": " << strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    // 开始监听
    if (::listen(listen_fd_, 128) < 0) {
        LOG_ERROR("Failed to listen: " << strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    running_ = true;
    LOG_INFO("Server started on port " << port_);

    acceptLoop();
}

void Server::stop() {
    LOG_INFO("stop() called");
    if (!running_.exchange(false)) {
        LOG_INFO("stop() already called, returning");
        return;
    }

    LOG_INFO("Stopping server...");

    // 先关闭监听 socket，这样 acceptLoop 中的 select 会返回
    int fd = listen_fd_;
    listen_fd_ = -1;
    if (fd >= 0) {
        LOG_INFO("Closing listen_fd_: " << fd);
        ::close(fd);
    }

    // 等待acceptLoop结束
    LOG_INFO("Waiting for acceptLoop to finish...");
    // 短暂等待确保acceptLoop看到running_变化
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 等待所有客户端线程结束（带超时）
    std::lock_guard<std::mutex> lock(threads_mutex_);
    LOG_INFO("Joining " << client_threads_.size() << " client threads");
    for (auto& t : client_threads_) {
        if (t.joinable()) {
            // 短暂等待线程完成
            t.join();
        }
    }
    client_threads_.clear();

    LOG_INFO("Server stopped");
}

void Server::acceptLoop() {
    LOG_INFO("acceptLoop started");
    while (running_.load()) {
        // 使用 select 来检查是否有新的连接，这样可以在超时后检查 running_ 状态
        fd_set read_fds;
        FD_ZERO(&read_fds);

        int fd = listen_fd_;
        if (fd < 0) {
            LOG_INFO("listen_fd_ is closed, exiting acceptLoop");
            break;
        }
        FD_SET(fd, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 1;  // 1秒超时
        timeout.tv_usec = 0;

        int ret = ::select(fd + 1, &read_fds, nullptr, nullptr, &timeout);
        if (ret < 0) {
            if (errno == EINTR) {
                LOG_DEBUG("select() interrupted by signal");
                continue;
            }
            LOG_ERROR("select() failed: " << strerror(errno));
            break;
        }

        if (ret == 0) {
            // 超时，继续循环检查 running_
            continue;
        }

        if (!FD_ISSET(listen_fd_, &read_fds)) {
            continue;
        }

        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int client_fd = ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (!running_.load()) {
                break;
            }
            LOG_ERROR("Failed to accept connection: " << strerror(errno));
            continue;
        }

        LOG_INFO("New connection from " << inet_ntoa(client_addr.sin_addr) << ":" << ntohs(client_addr.sin_port));

        // 创建新线程处理客户端
        std::lock_guard<std::mutex> lock(threads_mutex_);
        client_threads_.emplace_back(&Server::handleClient, this, Connection(client_fd));

        // 清理已完成的线程
        for (auto it = client_threads_.begin(); it != client_threads_.end();) {
            if (it->joinable()) {
                ++it;
            } else {
                it = client_threads_.erase(it);
            }
        }
    }
}

void Server::handleClient(Connection conn) {
    int fd = conn.socketFd();

    conn.setMessageHandler([this, &conn](MessageType type, const buffer_t& body) {
        if (request_handler_) {
            request_handler_->handleRequest(conn, type, body);
        }
    });

    conn.setCloseHandler([fd]() {
        LOG_INFO("Client disconnected, fd=" << fd);
    });

    conn.run();
}

} // namespace network
} // namespace tinydb
