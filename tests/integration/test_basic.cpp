#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <cstdlib>
#include "dbsdk.h"

using namespace tinydb::client;

class IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建数据目录
        system("mkdir -p /tmp/tinydb_test_data");

        // 启动服务器进程
        server_pid_ = fork();
        if (server_pid_ == 0) {
            // 子进程：启动服务器
            // Try multiple paths to find the server binary
            execl("../../bin/dbserver", "dbserver", "-p", "5434", "-d", "/tmp/tinydb_test_data", nullptr);
            execl("../bin/dbserver", "dbserver", "-p", "5434", "-d", "/tmp/tinydb_test_data", nullptr);
            execl("./bin/dbserver", "dbserver", "-p", "5434", "-d", "/tmp/tinydb_test_data", nullptr);
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

        // 清理数据目录
        system("rm -rf /tmp/tinydb_test_data");
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

    // 创建多列表
    auto result1 = client.execute(
        "CREATE TABLE employees ("
        "id INT, "
        "name VARCHAR(32), "
        "email VARCHAR(64), "
        "age INT)"
    );
    EXPECT_TRUE(result1.success());
    EXPECT_NE(result1.message().find("CREATE"), std::string::npos);

    // 插入多条数据
    auto result2 = client.execute(
        "INSERT INTO employees VALUES (1, 'Alice', 'alice@example.com', 30)"
    );
    EXPECT_TRUE(result2.success());
    EXPECT_NE(result2.message().find("INSERT"), std::string::npos);

    auto result3 = client.execute(
        "INSERT INTO employees VALUES (2, 'Bob', 'bob@example.com', 25)"
    );
    EXPECT_TRUE(result3.success());

    auto result4 = client.execute(
        "INSERT INTO employees VALUES (3, 'Charlie', 'charlie@example.com', 35)"
    );
    EXPECT_TRUE(result4.success());

    // 查询所有数据并验证
    auto result5 = client.execute("SELECT * FROM employees");
    EXPECT_TRUE(result5.success());
    // 验证返回结果中包含插入的数据
    EXPECT_NE(result5.message().find("Alice"), std::string::npos);
    EXPECT_NE(result5.message().find("alice@example.com"), std::string::npos);
    EXPECT_NE(result5.message().find("Bob"), std::string::npos);
    EXPECT_NE(result5.message().find("bob@example.com"), std::string::npos);
    EXPECT_NE(result5.message().find("Charlie"), std::string::npos);
    EXPECT_NE(result5.message().find("charlie@example.com"), std::string::npos);

    // 条件查询 - 按年龄筛选
    auto result6 = client.execute("SELECT * FROM employees WHERE age > 25");
    EXPECT_TRUE(result6.success());
    EXPECT_NE(result6.message().find("Alice"), std::string::npos);
    EXPECT_NE(result6.message().find("Charlie"), std::string::npos);

    // 条件查询 - 按名字筛选
    auto result7 = client.execute("SELECT * FROM employees WHERE name = 'Bob'");
    EXPECT_TRUE(result7.success());
    EXPECT_NE(result7.message().find("Bob"), std::string::npos);
    EXPECT_NE(result7.message().find("bob@example.com"), std::string::npos);

    // 投影查询 - 只查指定列
    auto result8 = client.execute("SELECT name, age FROM employees");
    EXPECT_TRUE(result8.success());
    EXPECT_NE(result8.message().find("Alice"), std::string::npos);
    EXPECT_NE(result8.message().find("30"), std::string::npos);

    // 删除表
    auto result9 = client.execute("DROP TABLE employees");
    EXPECT_TRUE(result9.success());
    EXPECT_NE(result9.message().find("DROP"), std::string::npos);

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
