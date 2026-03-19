#include "connection.h"
#include <errno.h>
#include <string.h>
#include <sys/select.h>
#include <fcntl.h>

namespace tinydb {
namespace network {

Connection::Connection(int socket_fd)
    : socket_fd_(socket_fd)
    , state_(ConnectionState::CONNECTED) {
    LOG_DEBUG("Connection created, fd=" << socket_fd_);
    // 设置 socket 为非阻塞模式，以便可以检查 running_ 状态
    int flags = fcntl(socket_fd_, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK);
    }
}

Connection::~Connection() {
    close();
}

Connection::Connection(Connection&& other) noexcept
    : socket_fd_(other.socket_fd_)
    , state_(other.state_.load())
    , message_handler_(std::move(other.message_handler_))
    , close_handler_(std::move(other.close_handler_)) {
    other.socket_fd_ = -1;
    other.state_ = ConnectionState::CLOSED;
}

Connection& Connection::operator=(Connection&& other) noexcept {
    if (this != &other) {
        close();
        socket_fd_ = other.socket_fd_;
        state_ = other.state_.load();
        message_handler_ = std::move(other.message_handler_);
        close_handler_ = std::move(other.close_handler_);
        other.socket_fd_ = -1;
        other.state_ = ConnectionState::CLOSED;
    }
    return *this;
}

void Connection::run() {
    if (state_.load() != ConnectionState::CONNECTED) {
        LOG_ERROR("Connection not in CONNECTED state");
        return;
    }

    LOG_INFO("Connection started processing, fd=" << socket_fd_);

    while (state_.load() == ConnectionState::CONNECTED) {
        // 使用 select 检查是否有数据可读，设置超时以便可以检查 state_
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(socket_fd_, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 1;  // 1秒超时
        timeout.tv_usec = 0;

        int ret = ::select(socket_fd_ + 1, &read_fds, nullptr, nullptr, &timeout);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("select() failed: " << strerror(errno));
            break;
        }

        if (ret == 0) {
            // 超时，继续循环检查 state_
            continue;
        }

        if (!FD_ISSET(socket_fd_, &read_fds)) {
            continue;
        }

        MessageType type;
        buffer_t body;

        Error err = readMessage(type, body);
        if (err.failed()) {
            if (err.code() == ErrorCode::E_CONNECTION_CLOSED) {
                LOG_INFO("Connection closed by peer, fd=" << socket_fd_);
            } else if (err.code() == ErrorCode::E_NETWORK_ERROR &&
                       (err.toString().find("Resource temporarily unavailable") != std::string::npos ||
                        err.toString().find("EAGAIN") != std::string::npos)) {
                // 非阻塞模式下没有数据可读，继续循环
                continue;
            } else {
                LOG_ERROR("Read message error: " << err.toString());
            }
            break;
        }

        if (message_handler_) {
            message_handler_(type, body);
        }
    }

    if (close_handler_) {
        close_handler_();
    }

    close();
}

Error Connection::sendMessage(MessageType type, const buffer_t& body) {
    if (state_.load() != ConnectionState::CONNECTED) {
        return Error(ErrorCode::E_CONNECTION_CLOSED);
    }

    buffer_t message = buildMessage(type, body);
    return sendExact(message.data(), message.size());
}

Error Connection::sendMessage(MessageType type) {
    return sendMessage(type, buffer_t());
}

void Connection::close() {
    ConnectionState expected = ConnectionState::CONNECTED;
    if (state_.compare_exchange_strong(expected, ConnectionState::CLOSING)) {
        LOG_DEBUG("Closing connection, fd=" << socket_fd_);
        if (socket_fd_ >= 0) {
            ::close(socket_fd_);
            socket_fd_ = -1;
        }
        state_ = ConnectionState::CLOSED;
    }
}

Error Connection::readMessage(MessageType& type, buffer_t& body) {
    // 读取消息头
    byte_t header_buffer[PROTOCOL_HEADER_SIZE];
    Error err = readExact(header_buffer, PROTOCOL_HEADER_SIZE);
    if (err.failed()) {
        return err;
    }

    // 解析消息头
    MessageHeader header;
    header.deserialize(header_buffer);
    err = header.validate();
    if (err.failed()) {
        return err;
    }

    type = static_cast<MessageType>(header.type);

    // 读取消息体
    if (header.body_length > 0) {
        body.resize(header.body_length);
        err = readExact(body.data(), header.body_length);
        if (err.failed()) {
            return err;
        }
    }

    return Error::success();
}

Error Connection::readExact(byte_t* buffer, size_t length) {
    size_t total_read = 0;

    while (total_read < length) {
        ssize_t n = ::recv(socket_fd_, buffer + total_read, length - total_read, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 非阻塞模式下没有数据可读
                return Error(ErrorCode::E_NETWORK_ERROR, "EAGAIN");
            }
            return Error(ErrorCode::E_NETWORK_ERROR, strerror(errno));
        }
        if (n == 0) {
            return Error(ErrorCode::E_CONNECTION_CLOSED);
        }
        total_read += n;
    }

    return Error::success();
}

Error Connection::sendExact(const byte_t* buffer, size_t length) {
    size_t total_sent = 0;

    while (total_sent < length) {
        ssize_t n = ::send(socket_fd_, buffer + total_sent, length - total_sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EPIPE || errno == ECONNRESET) {
                return Error(ErrorCode::E_CONNECTION_CLOSED);
            }
            return Error(ErrorCode::E_NETWORK_ERROR, strerror(errno));
        }
        total_sent += n;
    }

    return Error::success();
}

} // namespace network
} // namespace tinydb
