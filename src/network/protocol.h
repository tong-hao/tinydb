#pragma once

#include "common/types.h"
#include "common/error.h"
#include <string>
#include <cstring>
#include <arpa/inet.h>

namespace tinydb {
namespace network {

// Protocol message header
struct MessageHeader {
    uint8_t magic;
    uint8_t version;
    uint8_t type;
    uint8_t flags;
    uint32_t reserved;
    uint32_t body_length;

    MessageHeader()
        : magic(PROTOCOL_MAGIC)
        , version(PROTOCOL_VERSION)
        , type(0)
        , flags(0)
        , reserved(0)
        , body_length(0) {}

    // Serialize to buffer
    void serialize(byte_t* buffer) const {
        buffer[0] = magic;
        buffer[1] = version;
        buffer[2] = type;
        buffer[3] = flags;
        *reinterpret_cast<uint32_t*>(buffer + 4) = htonl(reserved);
        *reinterpret_cast<uint32_t*>(buffer + 8) = htonl(body_length);
    }

    // Deserialize from buffer
    bool deserialize(const byte_t* buffer) {
        magic = buffer[0];
        version = buffer[1];
        type = buffer[2];
        flags = buffer[3];
        reserved = ntohl(*reinterpret_cast<const uint32_t*>(buffer + 4));
        body_length = ntohl(*reinterpret_cast<const uint32_t*>(buffer + 8));
        return true;
    }

    // Validate message header
    Error validate() const {
        if (magic != PROTOCOL_MAGIC) {
            return Error(ErrorCode::E_INVALID_MAGIC);
        }
        if (version != PROTOCOL_VERSION) {
            return Error(ErrorCode::E_INVALID_VERSION);
        }
        if (body_length > MAX_MESSAGE_SIZE) {
            return Error(ErrorCode::E_INTERNAL_ERROR, "Message too large");
        }
        return Error::success();
    }
};

// SQL command message
struct SQLCommandMessage {
    std::string sql;

    SQLCommandMessage() = default;
    explicit SQLCommandMessage(std::string sql_str) : sql(std::move(sql_str)) {}

    // Serialize
    buffer_t serialize() const {
        buffer_t buffer;
        uint32_t sql_len = htonl(static_cast<uint32_t>(sql.length()));
        buffer.resize(4 + sql.length());
        std::memcpy(buffer.data(), &sql_len, 4);
        std::memcpy(buffer.data() + 4, sql.data(), sql.length());
        return buffer;
    }

    // 反序列化
    bool deserialize(const byte_t* data, uint32_t length) {
        if (length < 4) return false;
        uint32_t sql_len = ntohl(*reinterpret_cast<const uint32_t*>(data));
        if (length < 4 + sql_len) return false;
        sql.assign(reinterpret_cast<const char*>(data + 4), sql_len);
        return true;
    }
};

// SQL response message
struct SQLResponseMessage {
    uint8_t status;  // 0=OK, 1=Error
    std::string message;

    SQLResponseMessage() : status(0) {}
    SQLResponseMessage(uint8_t s, std::string msg) : status(s), message(std::move(msg)) {}

    static SQLResponseMessage ok(const std::string& msg = "OK") {
        return SQLResponseMessage(0, msg);
    }

    static SQLResponseMessage error(const std::string& msg) {
        return SQLResponseMessage(1, msg);
    }

    // Serialize
    buffer_t serialize() const {
        buffer_t buffer;
        uint32_t msg_len = htonl(static_cast<uint32_t>(message.length()));
        buffer.resize(1 + 4 + message.length());
        buffer[0] = status;
        std::memcpy(buffer.data() + 1, &msg_len, 4);
        std::memcpy(buffer.data() + 5, message.data(), message.length());
        return buffer;
    }

    // Deserialize
    bool deserialize(const byte_t* data, uint32_t length) {
        if (length < 5) return false;
        status = data[0];
        uint32_t msg_len = ntohl(*reinterpret_cast<const uint32_t*>(data + 1));
        if (length < 5 + msg_len) return false;
        message.assign(reinterpret_cast<const char*>(data + 5), msg_len);
        return true;
    }
};

// Error message
struct ErrorMessage {
    uint32_t error_code;
    std::string message;

    ErrorMessage() : error_code(0) {}
    ErrorMessage(ErrorCode code, std::string msg)
        : error_code(static_cast<uint32_t>(code)), message(std::move(msg)) {}

    // Serialize
    buffer_t serialize() const {
        buffer_t buffer;
        uint32_t code_net = htonl(error_code);
        uint32_t msg_len = htonl(static_cast<uint32_t>(message.length()));
        buffer.resize(4 + 4 + message.length());
        std::memcpy(buffer.data(), &code_net, 4);
        std::memcpy(buffer.data() + 4, &msg_len, 4);
        std::memcpy(buffer.data() + 8, message.data(), message.length());
        return buffer;
    }

    // Deserialize
    bool deserialize(const byte_t* data, uint32_t length) {
        if (length < 8) return false;
        error_code = ntohl(*reinterpret_cast<const uint32_t*>(data));
        uint32_t msg_len = ntohl(*reinterpret_cast<const uint32_t*>(data + 4));
        if (length < 8 + msg_len) return false;
        message.assign(reinterpret_cast<const char*>(data + 8), msg_len);
        return true;
    }
};

// Build complete message
buffer_t buildMessage(MessageType type, const buffer_t& body);
buffer_t buildMessage(MessageType type);

} // namespace network
} // namespace tinydb
