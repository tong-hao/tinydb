#include "dbsdk.h"
#include <iostream>
#include <cstring>
#include <cstdlib>

using namespace tinydb::client;

void printUsage(const char* program) {
    std::cout << "Usage: " << program << " [options] [SQL]\n"
              << "Options:\n"
              << "  -h, --host HOST    Server host (default: localhost)\n"
              << "  -p, --port PORT    Server port (default: 5432)\n"
              << "  --help             Show this help message\n"
              << "\nExamples:\n"
              << "  " << program << " -h localhost -p 5432 \"SELECT 1\"\n"
              << "  " << program << " \"CREATE TABLE users (id INT)\"\n";
}

int main(int argc, char* argv[]) {
    std::string host = "localhost";
    uint16_t port = 5432;
    std::string sql;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--host") == 0) {
            if (i + 1 < argc) {
                host = argv[++i];
            }
        } else if (strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) {
            if (i + 1 < argc) {
                port = static_cast<uint16_t>(atoi(argv[++i]));
            }
        } else if (strcmp(argv[i], "--help") == 0) {
            printUsage(argv[0]);
            return 0;
        } else if (argv[i][0] != '-') {
            sql = argv[i];
        }
    }

    if (sql.empty()) {
        std::cerr << "Error: No SQL command specified\n\n";
        printUsage(argv[0]);
        return 1;
    }

    // Create client
    Client client;

    // Connect to server
    std::cout << "Connecting to " << host << ":" << port << "..." << std::endl;
    if (!client.connect(host, port)) {
        std::cerr << "Failed to connect: " << client.lastError() << std::endl;
        return 1;
    }

    std::cout << "Connected. Executing: " << sql << std::endl;

    // Execute SQL
    auto result = client.execute(sql);

    if (result.success()) {
        std::cout << "Result: " << result.message() << std::endl;
    } else {
        std::cerr << "Error: " << result.message() << std::endl;
        client.disconnect();
        return 1;
    }

    // Disconnect
    client.disconnect();
    return 0;
}
