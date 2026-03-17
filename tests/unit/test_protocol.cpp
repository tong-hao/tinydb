#include <gtest/gtest.h>
#include "network/protocol.h"

using namespace tinydb;
using namespace tinydb::network;

TEST(ProtocolTest, MessageHeaderSerializeDeserialize) {
    MessageHeader header;
    header.magic = PROTOCOL_MAGIC;
    header.version = PROTOCOL_VERSION;
    header.type = static_cast<uint8_t>(MessageType::SQL_COMMAND);
    header.flags = 0;
    header.reserved = 0;
    header.body_length = 100;

    byte_t buffer[PROTOCOL_HEADER_SIZE];
    header.serialize(buffer);

    MessageHeader parsed;
    parsed.deserialize(buffer);

    EXPECT_EQ(parsed.magic, PROTOCOL_MAGIC);
    EXPECT_EQ(parsed.version, PROTOCOL_VERSION);
    EXPECT_EQ(parsed.type, static_cast<uint8_t>(MessageType::SQL_COMMAND));
    EXPECT_EQ(parsed.flags, 0);
    EXPECT_EQ(parsed.reserved, 0);
    EXPECT_EQ(parsed.body_length, 100);
}

TEST(ProtocolTest, MessageHeaderValidate) {
    MessageHeader header;
    header.magic = PROTOCOL_MAGIC;
    header.version = PROTOCOL_VERSION;
    header.type = 0;
    header.flags = 0;
    header.reserved = 0;
    header.body_length = 100;

    auto err = header.validate();
    EXPECT_TRUE(err.ok());

    header.magic = 0x00;
    err = header.validate();
    EXPECT_TRUE(err.failed());
    EXPECT_EQ(err.code(), ErrorCode::E_INVALID_MAGIC);

    header.magic = PROTOCOL_MAGIC;
    header.version = 0xFF;
    err = header.validate();
    EXPECT_TRUE(err.failed());
    EXPECT_EQ(err.code(), ErrorCode::E_INVALID_VERSION);
}

TEST(ProtocolTest, SQLCommandMessageSerializeDeserialize) {
    SQLCommandMessage cmd("SELECT * FROM users");
    buffer_t buffer = cmd.serialize();

    SQLCommandMessage parsed;
    bool success = parsed.deserialize(buffer.data(), static_cast<uint32_t>(buffer.size()));

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed.sql, "SELECT * FROM users");
}

TEST(ProtocolTest, SQLResponseMessageSerializeDeserialize) {
    SQLResponseMessage resp(0, "OK");
    buffer_t buffer = resp.serialize();

    SQLResponseMessage parsed;
    bool success = parsed.deserialize(buffer.data(), static_cast<uint32_t>(buffer.size()));

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed.status, 0);
    EXPECT_EQ(parsed.message, "OK");
}

TEST(ProtocolTest, SQLResponseMessageError) {
    SQLResponseMessage resp(1, "Table not found");
    buffer_t buffer = resp.serialize();

    SQLResponseMessage parsed;
    bool success = parsed.deserialize(buffer.data(), static_cast<uint32_t>(buffer.size()));

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed.status, 1);
    EXPECT_EQ(parsed.message, "Table not found");
}

TEST(ProtocolTest, ErrorMessageSerializeDeserialize) {
    ErrorMessage err(ErrorCode::E_SQL_PARSE_ERROR, "Syntax error");
    buffer_t buffer = err.serialize();

    ErrorMessage parsed;
    bool success = parsed.deserialize(buffer.data(), static_cast<uint32_t>(buffer.size()));

    EXPECT_TRUE(success);
    EXPECT_EQ(parsed.error_code, static_cast<uint32_t>(ErrorCode::E_SQL_PARSE_ERROR));
    EXPECT_EQ(parsed.message, "Syntax error");
}

TEST(ProtocolTest, BuildMessage) {
    SQLCommandMessage cmd("SELECT 1");
    buffer_t body = cmd.serialize();
    buffer_t message = buildMessage(MessageType::SQL_COMMAND, body);

    EXPECT_EQ(message.size(), PROTOCOL_HEADER_SIZE + body.size());

    // 验证消息头
    MessageHeader header;
    header.deserialize(message.data());
    EXPECT_EQ(header.magic, PROTOCOL_MAGIC);
    EXPECT_EQ(header.version, PROTOCOL_VERSION);
    EXPECT_EQ(header.type, static_cast<uint8_t>(MessageType::SQL_COMMAND));
    EXPECT_EQ(header.body_length, body.size());
}

TEST(ProtocolTest, BuildMessageWithoutBody) {
    buffer_t message = buildMessage(MessageType::HEARTBEAT_REQ);

    EXPECT_EQ(message.size(), PROTOCOL_HEADER_SIZE);

    MessageHeader header;
    header.deserialize(message.data());
    EXPECT_EQ(header.magic, PROTOCOL_MAGIC);
    EXPECT_EQ(header.version, PROTOCOL_VERSION);
    EXPECT_EQ(header.type, static_cast<uint8_t>(MessageType::HEARTBEAT_REQ));
    EXPECT_EQ(header.body_length, 0);
}
