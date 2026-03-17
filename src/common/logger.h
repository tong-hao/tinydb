#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <mutex>
#include <chrono>
#include <iomanip>

namespace tinydb {

enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3
};

class Logger {
public:
    static Logger& instance();

    void setLevel(LogLevel level) { level_ = level; }
    LogLevel level() const { return level_; }

    void log(LogLevel level, const std::string& message);

private:
    Logger() : level_(LogLevel::INFO) {}
    ~Logger() = default;

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    std::string levelToString(LogLevel level);
    std::string currentTime();

    LogLevel level_;
    std::mutex mutex_;
};

// 日志宏
#define LOG_DEBUG(msg) do { \
    if (tinydb::Logger::instance().level() <= tinydb::LogLevel::DEBUG) { \
        std::ostringstream oss; \
        oss << msg; \
        tinydb::Logger::instance().log(tinydb::LogLevel::DEBUG, oss.str()); \
    } \
} while(0)

#define LOG_INFO(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    tinydb::Logger::instance().log(tinydb::LogLevel::INFO, oss.str()); \
} while(0)

#define LOG_WARN(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    tinydb::Logger::instance().log(tinydb::LogLevel::WARN, oss.str()); \
} while(0)

#define LOG_ERROR(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    tinydb::Logger::instance().log(tinydb::LogLevel::ERROR, oss.str()); \
} while(0)

} // namespace tinydb
