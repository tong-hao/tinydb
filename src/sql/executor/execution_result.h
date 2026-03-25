#pragma once

#include <string>

namespace tinydb {
namespace engine {

// 执行结果
class ExecutionResult {
public:
    ExecutionResult() : success_(true) {}
    explicit ExecutionResult(std::string message) : success_(true), message_(std::move(message)) {}

    static ExecutionResult ok(const std::string& msg = "OK") {
        return ExecutionResult(msg);
    }

    static ExecutionResult error(const std::string& msg) {
        ExecutionResult result;
        result.success_ = false;
        result.message_ = msg;
        return result;
    }

    bool success() const { return success_; }
    const std::string& message() const { return message_; }

private:
    bool success_;
    std::string message_;
};

} // namespace engine
} // namespace tinydb
