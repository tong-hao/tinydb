#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "network/server.h"
#include "network/connection.h"
#include "dbsdk.h"

using namespace tinydb;
using namespace tinydb::network;
using namespace tinydb::client;

// 简单的请求处理器用于测试
class TestRequestHandler : public RequestHandler {
public:
    void handleRequest(Connection& conn, MessageType type, const buffer_t& body) override {
        if (type == MessageType::SQL_COMMAND) {
            SQLCommandMessage cmd;
            if (cmd.deserialize(body.data(), static_cast<uint32_t>(body.size()))) {
                SQLResponseMessage resp(0, "OK: " + cmd.sql);
                conn.sendMessage(MessageType::SQL_RESPONSE, resp.serialize());
            }
        } else if (type == MessageType::HEARTBEAT_REQ) {
            conn.sendMessage(MessageType::HEARTBEAT_RESP);
        }
    }
};

class ConnectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        handler_ = std::make_unique<TestRequestHandler>();
        server_ = std::make_unique<Server>(5433, handler_.get());  // 使用 5433 避免冲突

        // 在后台线程启动服务器
        server_thread_ = std::thread([this]() {
            server_->run();
        });

        // 等待服务器启动
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    std::unique_ptr<TestRequestHandler> handler_;
    std::unique_ptr<Server> server_;
    std::thread server_thread_;
};

TEST_F(ConnectionTest, ClientConnect) {
    Client client;
    EXPECT_TRUE(client.connect("localhost", 5433));
    EXPECT_TRUE(client.isConnected());
    client.disconnect();
    EXPECT_FALSE(client.isConnected());
}

TEST_F(ConnectionTest, ClientExecuteSQL) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5433));

    auto result = client.execute("SELECT 1");
    EXPECT_TRUE(result.success());
    EXPECT_NE(result.message().find("OK"), std::string::npos);

    client.disconnect();
}

TEST_F(ConnectionTest, ClientPing) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5433));

    EXPECT_TRUE(client.ping());

    client.disconnect();
}

TEST_F(ConnectionTest, MultipleClients) {
    const int num_clients = 5;
    std::vector<std::unique_ptr<Client>> clients;

    for (int i = 0; i < num_clients; ++i) {
        auto client = std::make_unique<Client>();
        ASSERT_TRUE(client->connect("localhost", 5433));
        clients.push_back(std::move(client));
    }

    for (auto& client : clients) {
        auto result = client->execute("SELECT 1");
        EXPECT_TRUE(result.success());
    }

    for (auto& client : clients) {
        client->disconnect();
    }
}

TEST_F(ConnectionTest, ClientReconnect) {
    Client client;

    // 第一次连接
    ASSERT_TRUE(client.connect("localhost", 5433));
    EXPECT_TRUE(client.isConnected());
    client.disconnect();
    EXPECT_FALSE(client.isConnected());

    // 重新连接
    ASSERT_TRUE(client.connect("localhost", 5433));
    EXPECT_TRUE(client.isConnected());

    auto result = client.execute("SELECT 1");
    EXPECT_TRUE(result.success());

    client.disconnect();
}
