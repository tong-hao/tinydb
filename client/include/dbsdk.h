#pragma once

#include <string>
#include <cstdint>
#include <functional>
#include <memory>

namespace tinydb {
namespace client {

// 连接状态
enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    ERROR
};

// 执行结果
class Result {
public:
    Result() : success_(false) {}
    explicit Result(std::string message) : success_(true), message_(std::move(message)) {}

    static Result ok(const std::string& msg) {
        return Result(msg);
    }

    static Result error(const std::string& msg) {
        Result r;
        r.message_ = msg;
        return r;
    }

    bool success() const { return success_; }
    const std::string& message() const { return message_; }

private:
    bool success_;
    std::string message_;
};

// 数据库客户端
class Client {
public:
    Client();
    ~Client();

    // 禁止拷贝，允许移动
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) noexcept;
    Client& operator=(Client&&) noexcept;

    // 连接到服务器
    bool connect(const std::string& host, uint16_t port);

    // 断开连接
    void disconnect();

    // 检查连接状态
    bool isConnected() const;

    // 执行 SQL 命令
    Result execute(const std::string& sql);

    // 发送心跳
    bool ping();

    // 获取最后错误信息
    std::string lastError() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace client
} // namespace tinydb
