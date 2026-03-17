#include "dbsdk.h"
#include "network/protocol.h"
#include "common/logger.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <atomic>

using namespace tinydb;
using namespace tinydb::network;

namespace tinydb {
namespace client {

class Client::Impl {
public:
    Impl() : socket_fd_(-1), state_(ConnectionState::DISCONNECTED) {}

    ~Impl() {
        disconnect();
    }

    bool connect(const std::string& host, uint16_t port) {
        if (state_ == ConnectionState::CONNECTED) {
            return true;
        }

        state_ = ConnectionState::CONNECTING;

        // 创建 socket
        socket_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ < 0) {
            last_error_ = "Failed to create socket: " + std::string(strerror(errno));
            state_ = ConnectionState::ERROR;
            return false;
        }

        // 设置服务器地址
        struct sockaddr_in server_addr;
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);

        if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
            last_error_ = "Invalid address: " + host;
            ::close(socket_fd_);
            socket_fd_ = -1;
            state_ = ConnectionState::ERROR;
            return false;
        }

        // 连接服务器
        if (::connect(socket_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
            last_error_ = "Connection failed: " + std::string(strerror(errno));
            ::close(socket_fd_);
            socket_fd_ = -1;
            state_ = ConnectionState::ERROR;
            return false;
        }

        state_ = ConnectionState::CONNECTED;
        return true;
    }

    void disconnect() {
        if (socket_fd_ >= 0) {
            ::close(socket_fd_);
            socket_fd_ = -1;
        }
        state_ = ConnectionState::DISCONNECTED;
    }

    bool isConnected() const {
        return state_ == ConnectionState::CONNECTED;
    }

    Result execute(const std::string& sql) {
        if (!isConnected()) {
            return Result::error("Not connected");
        }

        // 发送 SQL 命令
        SQLCommandMessage cmd(sql);
        buffer_t body = cmd.serialize();
        buffer_t message = buildMessage(MessageType::SQL_COMMAND, body);

        if (!sendAll(message.data(), message.size())) {
            return Result::error("Failed to send: " + last_error_);
        }

        // 接收响应
        MessageType resp_type;
        buffer_t resp_body;
        if (!receiveMessage(resp_type, resp_body)) {
            return Result::error("Failed to receive: " + last_error_);
        }

        if (resp_type == MessageType::SQL_RESPONSE) {
            SQLResponseMessage resp;
            if (!resp.deserialize(resp_body.data(), static_cast<uint32_t>(resp_body.size()))) {
                return Result::error("Failed to parse response");
            }
            if (resp.status == 0) {
                return Result::ok(resp.message);
            } else {
                return Result::error(resp.message);
            }
        } else if (resp_type == MessageType::ERROR) {
            ErrorMessage err;
            if (!err.deserialize(resp_body.data(), static_cast<uint32_t>(resp_body.size()))) {
                return Result::error("Failed to parse error");
            }
            return Result::error(err.message);
        }

        return Result::error("Unknown response type");
    }

    bool ping() {
        if (!isConnected()) {
            return false;
        }

        buffer_t message = buildMessage(MessageType::HEARTBEAT_REQ);
        if (!sendAll(message.data(), message.size())) {
            return false;
        }

        MessageType resp_type;
        buffer_t resp_body;
        if (!receiveMessage(resp_type, resp_body)) {
            return false;
        }

        return resp_type == MessageType::HEARTBEAT_RESP;
    }

    std::string lastError() const {
        return last_error_;
    }

private:
    bool sendAll(const byte_t* data, size_t length) {
        size_t sent = 0;
        while (sent < length) {
            ssize_t n = ::send(socket_fd_, data + sent, length - sent, MSG_NOSIGNAL);
            if (n < 0) {
                last_error_ = strerror(errno);
                state_ = ConnectionState::ERROR;
                return false;
            }
            sent += n;
        }
        return true;
    }

    bool receiveMessage(MessageType& type, buffer_t& body) {
        // 读取消息头
        byte_t header_buf[PROTOCOL_HEADER_SIZE];
        if (!receiveAll(header_buf, PROTOCOL_HEADER_SIZE)) {
            return false;
        }

        MessageHeader header;
        header.deserialize(header_buf);

        auto err = header.validate();
        if (err.failed()) {
            last_error_ = err.toString();
            return false;
        }

        type = static_cast<MessageType>(header.type);

        // 读取消息体
        if (header.body_length > 0) {
            body.resize(header.body_length);
            if (!receiveAll(body.data(), header.body_length)) {
                return false;
            }
        }

        return true;
    }

    bool receiveAll(byte_t* buffer, size_t length) {
        size_t received = 0;
        while (received < length) {
            ssize_t n = ::recv(socket_fd_, buffer + received, length - received, 0);
            if (n < 0) {
                last_error_ = strerror(errno);
                state_ = ConnectionState::ERROR;
                return false;
            }
            if (n == 0) {
                last_error_ = "Connection closed by server";
                state_ = ConnectionState::DISCONNECTED;
                return false;
            }
            received += n;
        }
        return true;
    }

    int socket_fd_;
    std::atomic<ConnectionState> state_;
    std::string last_error_;
};

// Client 公共接口实现
Client::Client() : impl_(std::make_unique<Impl>()) {}

Client::~Client() = default;

Client::Client(Client&&) noexcept = default;

Client& Client::operator=(Client&&) noexcept = default;

bool Client::connect(const std::string& host, uint16_t port) {
    return impl_->connect(host, port);
}

void Client::disconnect() {
    impl_->disconnect();
}

bool Client::isConnected() const {
    return impl_->isConnected();
}

Result Client::execute(const std::string& sql) {
    return impl_->execute(sql);
}

bool Client::ping() {
    return impl_->ping();
}

std::string Client::lastError() const {
    return impl_->lastError();
}

} // namespace client
} // namespace tinydb
