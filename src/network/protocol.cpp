#include "protocol.h"

namespace tinydb {
namespace network {

buffer_t buildMessage(MessageType type, const buffer_t& body) {
    buffer_t message;
    message.resize(PROTOCOL_HEADER_SIZE + body.size());

    MessageHeader header;
    header.type = static_cast<uint8_t>(type);
    header.body_length = static_cast<uint32_t>(body.size());
    header.serialize(message.data());

    if (!body.empty()) {
        std::memcpy(message.data() + PROTOCOL_HEADER_SIZE, body.data(), body.size());
    }

    return message;
}

buffer_t buildMessage(MessageType type) {
    return buildMessage(type, buffer_t());
}

} // namespace network
} // namespace tinydb
