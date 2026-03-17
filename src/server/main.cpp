#include "network/server.h"
#include "network/connection.h"
#include "network/protocol.h"
#include "engine/executor.h"
#include "storage/storage_engine.h"
#include "common/logger.h"
#include <iostream>
#include <cstring>
#include <csignal>

using namespace tinydb;
using namespace tinydb::network;
using namespace tinydb::engine;
using namespace tinydb::storage;

// 全局实例，用于信号处理
Server* g_server = nullptr;
StorageEngine* g_storage = nullptr;

// 信号处理函数
void signalHandler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        LOG_INFO("Received signal " << sig << ", shutting down...");
        if (g_server) {
            g_server->stop();
        }
    }
}

// 请求处理器
class SQLRequestHandler : public RequestHandler {
public:
    SQLRequestHandler(StorageEngine* storage) : storage_(storage) {
        executor_ = std::make_unique<Executor>();
        executor_->initialize(storage);
    }

    void handleRequest(Connection& conn, MessageType type, const buffer_t& body) override {
        switch (type) {
            case MessageType::SQL_COMMAND: {
                SQLCommandMessage cmd;
                if (!cmd.deserialize(body.data(), static_cast<uint32_t>(body.size()))) {
                    sendErrorResponse(conn, ErrorCode::E_SQL_PARSE_ERROR, "Failed to parse SQL command");
                    return;
                }

                LOG_INFO("Received SQL: " << cmd.sql);

                // 执行 SQL
                auto result = executor_->execute(cmd.sql);

                // 发送响应
                if (result.success()) {
                    SQLResponseMessage resp(0, result.message());
                    conn.sendMessage(MessageType::SQL_RESPONSE, resp.serialize());
                } else {
                    sendErrorResponse(conn, ErrorCode::E_INTERNAL_ERROR, result.message());
                }
                break;
            }

            case MessageType::HEARTBEAT_REQ: {
                conn.sendMessage(MessageType::HEARTBEAT_RESP);
                LOG_DEBUG("Heartbeat response sent");
                break;
            }

            default: {
                sendErrorResponse(conn, ErrorCode::E_INVALID_TYPE, "Unknown message type");
                break;
            }
        }
    }

private:
    void sendErrorResponse(Connection& conn, ErrorCode code, const std::string& message) {
        ErrorMessage err(code, message);
        conn.sendMessage(MessageType::ERROR, err.serialize());
    }

    StorageEngine* storage_;
    std::unique_ptr<Executor> executor_;
};

void printUsage(const char* program) {
    std::cout << "Usage: " << program << " [options]\n"
              << "Options:\n"
              << "  -p, --port PORT    Listen port (default: 5432)\n"
              << "  -d, --data DIR     Data directory (default: ./data)\n"
              << "  --debug            Enable debug logging\n"
              << "  -h, --help         Show this help message\n";
}

int main(int argc, char* argv[]) {
    uint16_t port = 5432;
    std::string data_dir = "./data";
    bool debug = false;

    // 解析命令行参数
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) {
            if (i + 1 < argc) {
                port = static_cast<uint16_t>(atoi(argv[++i]));
            }
        } else if (strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--data") == 0) {
            if (i + 1 < argc) {
                data_dir = argv[++i];
            }
        } else if (strcmp(argv[i], "--debug") == 0) {
            debug = true;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            printUsage(argv[0]);
            return 0;
        }
    }

    // 设置日志级别
    if (debug) {
        Logger::instance().setLevel(LogLevel::DEBUG);
    }

    LOG_INFO("TinyDB Server starting...");
    LOG_INFO("Port: " << port);
    LOG_INFO("Data directory: " << data_dir);

    // 初始化存储引擎
    StorageConfig storage_config;
    storage_config.db_file_path = data_dir + "/tinydb.db";
    storage_config.wal_file_path = data_dir + "/tinydb.wal";
    storage_config.buffer_pool_size = 1024;

    StorageEngine storage(storage_config);
    if (!storage.initialize()) {
        LOG_ERROR("Failed to initialize storage engine");
        return 1;
    }
    g_storage = &storage;

    LOG_INFO("Storage engine initialized");
    LOG_INFO("Database file: " << storage_config.db_file_path);
    LOG_INFO("WAL file: " << storage_config.wal_file_path);

    // 设置信号处理
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    std::signal(SIGPIPE, SIG_IGN);

    // 创建请求处理器
    SQLRequestHandler handler(&storage);

    // 创建并启动服务器
    Server server(port, &handler);
    g_server = &server;

    server.run();

    // 关闭存储引擎
    storage.shutdown();

    LOG_INFO("TinyDB Server stopped");
    return 0;
}
