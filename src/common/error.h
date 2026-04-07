#pragma once

#include "types.h"
#include <string>
#include <system_error>

namespace tinydb {

class Error {
public:
    Error() : code_(ErrorCode::OK) {}
    explicit Error(ErrorCode code) : code_(code) {}
    Error(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

    bool ok() const { return code_ == ErrorCode::OK; }
    bool failed() const { return !ok(); }

    ErrorCode code() const { return code_; }
    const std::string& message() const { return message_; }

    std::string toString() const;

    static Error success() { return Error(); }

private:
    ErrorCode code_;
    std::string message_;
};

// Convert error code to string
std::string errorCodeToString(ErrorCode code);

} // namespace tinydb
