#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <cstdlib>
#include "dbsdk.h"

using namespace tinydb::client;

class IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 启动服务器进程
        server_pid_ = fork();
        if (server_pid_ == 0) {
            // 子进程：启动服务器
            // Try multiple paths to find the server binary
            execl("../../bin/dbserver", "dbserver", "-p", "5434", nullptr);
            execl("../bin/dbserver", "dbserver", "-p", "5434", nullptr);
            execl("./bin/dbserver", "dbserver", "-p", "5434", nullptr);
            exit(1);
        }

        // 等待服务器启动
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    void TearDown() override {
        // 停止服务器
        if (server_pid_ > 0) {
            kill(server_pid_, SIGTERM);
            waitpid(server_pid_, nullptr, 0);
        }
    }

    pid_t server_pid_;
};

TEST_F(IntegrationTest, BasicConnection) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));
    EXPECT_TRUE(client.isConnected());
    client.disconnect();
}

TEST_F(IntegrationTest, ExecuteSelect) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));

    auto result = client.execute("SELECT 1");
    EXPECT_TRUE(result.success());
    EXPECT_NE(result.message().find("SELECT"), std::string::npos);

    client.disconnect();
}

TEST_F(IntegrationTest, ExecuteCreateTable) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));

    auto result = client.execute("CREATE TABLE users (id INT, name VARCHAR(32))");
    EXPECT_TRUE(result.success());
    EXPECT_NE(result.message().find("CREATE"), std::string::npos);

    client.disconnect();
}

TEST_F(IntegrationTest, ExecuteInsert) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));

    // First create the table
    auto result1 = client.execute("CREATE TABLE users (id INT, name VARCHAR(32))");
    EXPECT_TRUE(result1.success());

    // Then insert into it
    auto result = client.execute("INSERT INTO users VALUES (1, 'Alice')");
    EXPECT_TRUE(result.success());
    EXPECT_NE(result.message().find("INSERT"), std::string::npos);

    client.disconnect();
}

TEST_F(IntegrationTest, ExecuteDropTable) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));

    // First create the table
    auto result1 = client.execute("CREATE TABLE users (id INT, name VARCHAR(32))");
    EXPECT_TRUE(result1.success());

    // Then drop it
    auto result = client.execute("DROP TABLE users");
    EXPECT_TRUE(result.success());
    EXPECT_NE(result.message().find("DROP"), std::string::npos);

    client.disconnect();
}

TEST_F(IntegrationTest, MultipleCommands) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));

    // 创建表
    auto result1 = client.execute("CREATE TABLE test (id INT)");
    EXPECT_TRUE(result1.success());

    // 插入数据
    auto result2 = client.execute("INSERT INTO test VALUES (1)");
    EXPECT_TRUE(result2.success());

    // 查询
    auto result3 = client.execute("SELECT * FROM test");
    EXPECT_TRUE(result3.success());

    // 删除表
    auto result4 = client.execute("DROP TABLE test");
    EXPECT_TRUE(result4.success());

    client.disconnect();
}

TEST_F(IntegrationTest, Heartbeat) {
    Client client;
    ASSERT_TRUE(client.connect("localhost", 5434));

    // 发送多次心跳
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(client.ping());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // 心跳后仍然可以执行 SQL
    auto result = client.execute("SELECT 1");
    EXPECT_TRUE(result.success());

    client.disconnect();
}
