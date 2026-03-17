#include "error.h"

namespace tinydb {

std::string errorCodeToString(ErrorCode code) {
    switch (code) {
        case ErrorCode::OK:
            return "OK";
        case ErrorCode::E_INVALID_MAGIC:
            return "E_INVALID_MAGIC";
        case ErrorCode::E_INVALID_VERSION:
            return "E_INVALID_VERSION";
        case ErrorCode::E_INVALID_TYPE:
            return "E_INVALID_TYPE";
        case ErrorCode::E_SQL_PARSE_ERROR:
            return "E_SQL_PARSE_ERROR";
        case ErrorCode::E_INTERNAL_ERROR:
            return "E_INTERNAL_ERROR";
        case ErrorCode::E_CONNECTION_CLOSED:
            return "E_CONNECTION_CLOSED";
        case ErrorCode::E_NETWORK_ERROR:
            return "E_NETWORK_ERROR";
        case ErrorCode::E_TIMEOUT:
            return "E_TIMEOUT";
        default:
            return "UNKNOWN_ERROR";
    }
}

std::string Error::toString() const {
    if (message_.empty()) {
        return errorCodeToString(code_);
    }
    return errorCodeToString(code_) + ": " + message_;
}

} // namespace tinydb
