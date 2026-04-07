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

// Connection state
enum class ConnectionState {
    CONNECTING,
    CONNECTED,
    CLOSING,
    CLOSED
};

// Connection class
class Connection {
public:
    using MessageHandler = std::function<void(MessageType, const buffer_t&)>;
    using CloseHandler = std::function<void()>;

    Connection(int socket_fd);
    ~Connection();

    // Disable copy
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    // Allow move
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    // Start message processing loop (blocking)
    void run();

    // Send message
    Error sendMessage(MessageType type, const buffer_t& body);
    Error sendMessage(MessageType type);

    // Close connection
    void close();

    // Get status
    ConnectionState state() const { return state_.load(); }
    int socketFd() const { return socket_fd_; }

    // Set callbacks
    void setMessageHandler(MessageHandler handler) { message_handler_ = std::move(handler); }
    void setCloseHandler(CloseHandler handler) { close_handler_ = std::move(handler); }

private:
    // Read complete message
    Error readMessage(MessageType& type, buffer_t& body);

    // Read exact number of bytes
    Error readExact(byte_t* buffer, size_t length);

    // Send exact number of bytes
    Error sendExact(const byte_t* buffer, size_t length);

    int socket_fd_;
    std::atomic<ConnectionState> state_;
    MessageHandler message_handler_;
    CloseHandler close_handler_;
};

} // namespace network
} // namespace tinydb
