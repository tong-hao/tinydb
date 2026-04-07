#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

namespace tinydb {

using byte_t = uint8_t;
using buffer_t = std::vector<byte_t>;

// Message types
enum class MessageType : uint8_t {
    SQL_COMMAND = 0x01,
    SQL_RESPONSE = 0x02,
    HEARTBEAT_REQ = 0x03,
    HEARTBEAT_RESP = 0x04,
    ERROR = 0x05
};

// Protocol constants
constexpr uint8_t PROTOCOL_MAGIC = 0x54;      // 'T'
constexpr uint8_t PROTOCOL_VERSION = 0x01;
constexpr size_t PROTOCOL_HEADER_SIZE = 12;
constexpr size_t MAX_MESSAGE_SIZE = 16 * 1024 * 1024;  // 16MB
constexpr size_t MAX_SQL_LENGTH = 1024 * 1024;         // 1MB

// Error codes
enum class ErrorCode : uint32_t {
    OK = 0,
    E_INVALID_MAGIC = 0x00000001,
    E_INVALID_VERSION = 0x00000002,
    E_INVALID_TYPE = 0x00000003,
    E_SQL_PARSE_ERROR = 0x00000004,
    E_INTERNAL_ERROR = 0x00000005,
    E_CONNECTION_CLOSED = 0x00000006,
    E_NETWORK_ERROR = 0x00000007,
    E_TIMEOUT = 0x00000008
};

} // namespace tinydb
