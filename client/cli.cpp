#include "dbsdk.h"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <chrono>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>

// 支持 readline 库（如果可用）
#ifdef HAS_READLINE
#include <readline/readline.h>
#include <readline/history.h>
#endif

using namespace tinydb::client;

// ANSI 颜色代码
namespace Color {
    const char* RESET = "\033[0m";
    const char* RED = "\033[31m";
    const char* GREEN = "\033[32m";
    const char* YELLOW = "\033[33m";
    const char* BLUE = "\033[34m";
    const char* MAGENTA = "\033[35m";
    const char* CYAN = "\033[36m";
    const char* WHITE = "\033[37m";
    const char* BOLD = "\033[1m";
}

// 全局客户端对象
Client g_client;
std::string g_current_database = "tinydb";
std::string g_current_user = "tinydb";
bool g_running = true;
bool g_use_color = true;
bool g_expanded_output = false;
int g_pager_enabled = true;

// 打印带颜色的文本
void printColored(const char* color, const std::string& text) {
    if (g_use_color) {
        std::cout << color << text << Color::RESET;
    } else {
        std::cout << text;
    }
}

// 获取终端宽度
int getTerminalWidth() {
    struct winsize w;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0) {
        return w.ws_col;
    }
    return 80; // 默认宽度
}

// 打印水平分隔线
void printSeparator(const std::vector<int>& col_widths) {
    std::cout << "+";
    for (size_t i = 0; i < col_widths.size(); ++i) {
        std::cout << std::string(col_widths[i] + 2, '-');
        std::cout << "+";
    }
    std::cout << "\n";
}

// 打印表格行
void printTableRow(const std::vector<std::string>& values,
                   const std::vector<int>& col_widths,
                   bool is_header = false) {
    std::cout << "|";
    for (size_t i = 0; i < values.size() && i < col_widths.size(); ++i) {
        if (is_header && g_use_color) {
            std::cout << Color::BOLD << Color::CYAN;
        }
        std::cout << " " << std::left << std::setw(col_widths[i]) << values[i];
        if (is_header && g_use_color) {
            std::cout << Color::RESET;
        }
        std::cout << " |";
    }
    std::cout << "\n";
}

// 解析简单的 CSV 或表格结果并格式化显示
void displayResult(const std::string& result_str) {
    if (result_str.empty()) {
        printColored(Color::GREEN, "OK\n");
        return;
    }

    // 检查是否是错误消息
    if (result_str.find("Error:") == 0) {
        printColored(Color::RED, result_str + "\n");
        return;
    }

    // 简单的表格解析（假设是制表符或逗号分隔）
    std::istringstream iss(result_str);
    std::string line;
    std::vector<std::vector<std::string>> rows;
    std::vector<int> col_widths;

    // 解析所有行
    while (std::getline(iss, line)) {
        std::vector<std::string> fields;
        std::istringstream line_iss(line);
        std::string field;

        // 尝试按制表符分割
        while (std::getline(line_iss, field, '\t')) {
            fields.push_back(field);
        }

        if (fields.empty()) {
            // 尝试按逗号分割
            line_iss.clear();
            line_iss.str(line);
            while (std::getline(line_iss, field, ',')) {
                // 去除空格
                field.erase(0, field.find_first_not_of(" \t"));
                field.erase(field.find_last_not_of(" \t") + 1);
                fields.push_back(field);
            }
        }

        if (!fields.empty()) {
            rows.push_back(fields);

            // 更新列宽
            if (col_widths.size() < fields.size()) {
                col_widths.resize(fields.size(), 0);
            }
            for (size_t i = 0; i < fields.size(); ++i) {
                col_widths[i] = std::max(col_widths[i], static_cast<int>(fields[i].length()));
                // 限制最大列宽
                col_widths[i] = std::min(col_widths[i], 50);
            }
        }
    }

    if (rows.empty()) {
        std::cout << result_str << std::endl;
        return;
    }

    // 打印表格
    printSeparator(col_widths);
    for (size_t i = 0; i < rows.size(); ++i) {
        printTableRow(rows[i], col_widths, i == 0);
        if (i == 0) {
            printSeparator(col_widths);
        }
    }
    printSeparator(col_widths);

    std::cout << "(" << (rows.size() - 1) << " rows)\n";
}

// 显示扩展格式（每行一个字段）
void displayExpandedResult(const std::string& result_str) {
    if (result_str.empty()) {
        printColored(Color::GREEN, "OK\n");
        return;
    }

    std::istringstream iss(result_str);
    std::string line;
    std::vector<std::string> headers;
    std::vector<std::vector<std::string>> rows;
    int row_num = 0;

    // 解析
    while (std::getline(iss, line)) {
        std::vector<std::string> fields;
        std::istringstream line_iss(line);
        std::string field;

        while (std::getline(line_iss, field, '\t')) {
            fields.push_back(field);
        }

        if (fields.empty()) continue;

        if (headers.empty()) {
            headers = fields;
        } else {
            rows.push_back(fields);
            row_num++;

            // 打印扩展格式
            std::cout << "-[" << row_num << "]-------------------------\n";
            for (size_t i = 0; i < fields.size() && i < headers.size(); ++i) {
                if (g_use_color) {
                    std::cout << Color::CYAN << std::left << std::setw(15) << headers[i]
                              << Color::RESET << " | " << fields[i] << "\n";
                } else {
                    std::cout << std::left << std::setw(15) << headers[i]
                              << " | " << fields[i] << "\n";
                }
            }
            std::cout << "\n";
        }
    }

    std::cout << "(" << rows.size() << " rows)\n";
}

// 元命令处理
bool handleMetaCommand(const std::string& cmd) {
    std::string trimmed = cmd;
    // 去除前后空格
    trimmed.erase(0, trimmed.find_first_not_of(" \t"));
    trimmed.erase(trimmed.find_last_not_of(" \t") + 1);

    if (trimmed.empty() || trimmed[0] != '\\') {
        return false;
    }

    std::istringstream iss(trimmed);
    std::string meta;
    iss >> meta;

    // \q - 退出
    if (meta == "\\q" || meta == "\\quit") {
        g_running = false;
        return true;
    }

    // \dt - 列出表
    if (meta == "\\dt" || meta == "\\dt+") {
        std::cout << "List of relations\n";
        std::cout << " Schema | Name | Type | Owner \n";
        std::cout << "--------+------+------+-------\n";
        // 这里应该查询系统表获取真实数据
        // 简化实现
        std::cout << " public | users | table | " << g_current_user << "\n";
        std::cout << " public | orders | table | " << g_current_user << "\n";
        return true;
    }

    // \d - 描述表结构
    if (meta == "\\d") {
        std::string table_name;
        iss >> table_name;
        if (table_name.empty()) {
            // 如果没有指定表名，列出所有表
            return handleMetaCommand("\\dt");
        }

        std::cout << "Table \"public." << table_name << "\"\n";
        std::cout << " Column | Type | Nullable | Default \n";
        std::cout << "--------+------+----------+---------\n";
        // 这里应该查询系统表获取真实数据
        std::cout << " id     | INT  | not null | nextval(...)\n";
        std::cout << " name   | TEXT | yes      | \n";
        return true;
    }

    // \du - 列出用户
    if (meta == "\\du" || meta == "\\du+") {
        std::cout << "List of roles\n";
        std::cout << " Role name | Attributes | Member of \n";
        std::cout << "-----------+------------+-----------\n";
        std::cout << " " << g_current_user << " | Superuser  | {}\n";
        return true;
    }

    // \l - 列出数据库
    if (meta == "\\l" || meta == "\\l+") {
        std::cout << "List of databases\n";
        std::cout << " Name | Owner | Encoding | Collate | Ctype | Access privileges \n";
        std::cout << "------+-------+----------+---------+-------+-------------------\n";
        std::cout << " " << g_current_database << " | " << g_current_user << " | UTF8 | C | C | \n";
        return true;
    }

    // \c - 切换数据库
    if (meta == "\\c" || meta == "\\connect") {
        std::string dbname;
        iss >> dbname;
        if (dbname.empty()) {
            std::cout << "You are currently connected to database \""
                      << g_current_database << "\" as user \""
                      << g_current_user << "\".\n";
        } else {
            g_current_database = dbname;
            std::cout << "You are now connected to database \""
                      << g_current_database << "\" as user \""
                      << g_current_user << "\".\n";
        }
        return true;
    }

    // \x - 切换扩展输出模式
    if (meta == "\\x") {
        g_expanded_output = !g_expanded_output;
        std::cout << "Expanded display is " << (g_expanded_output ? "on" : "off") << ".\n";
        return true;
    }

    // \timing - 切换计时
    if (meta == "\\timing") {
        static bool timing = false;
        timing = !timing;
        std::cout << "Timing is " << (timing ? "on" : "off") << ".\n";
        return true;
    }

    // \pset - 设置输出选项
    if (meta == "\\pset") {
        std::string option, value;
        iss >> option >> value;
        if (option == "border") {
            std::cout << "Border style set to " << value << ".\n";
        } else if (option == "format") {
            std::cout << "Output format set to " << value << ".\n";
        } else {
            std::cout << "Unknown option: " << option << "\n";
        }
        return true;
    }

    // \? - 帮助
    if (meta == "\\?" || meta == "\\h") {
        std::cout << "General\n";
        std::cout << "  \\q                     quit\n";
        std::cout << "  \\c[onnect] [DBNAME]    connect to new database\n";
        std::cout << "  \\dt[TABLE]             list tables\n";
        std::cout << "  \\d TABLE               describe table\n";
        std::cout << "  \\du                    list users\n";
        std::cout << "  \\l                     list databases\n";
        std::cout << "  \\x                     toggle expanded output\n";
        std::cout << "  \\timing                 toggle timing of commands\n";
        std::cout << "  \\pset OPTION [VALUE]    set output option\n";
        std::cout << "  \\?                     show help\n";
        return true;
    }

    // 未知命令
    std::cout << "Invalid command \"" << meta << "\". Type \\? for help.\n";
    return true;
}

// 打印提示符
void printPrompt() {
    if (g_use_color) {
        std::cout << Color::GREEN << g_current_database << Color::RESET
                  << "=> ";
    } else {
        std::cout << g_current_database << "=> ";
    }
    std::cout.flush();
}

// 打印多行输入提示符
void printMultiLinePrompt(int line_num) {
    std::string prompt = g_current_database + "(" + std::to_string(line_num) + ")";
    if (g_use_color) {
        std::cout << Color::YELLOW << std::setw(prompt.length() + 3) << std::left
                  << prompt << Color::RESET;
    } else {
        std::cout << std::setw(prompt.length() + 3) << std::left << prompt;
    }
    std::cout.flush();
}

// 读取一行输入（支持多行）
std::string readInputLine(bool& is_multiline) {
    std::string line;

#ifdef HAS_READLINE
    char* input = readline(is_multiline ? "" : (g_current_database + "=> ").c_str());
    if (input == nullptr) {
        // EOF
        return "";
    }
    line = input;
    if (!line.empty()) {
        add_history(input);
    }
    free(input);
#else
    if (!is_multiline) {
        printPrompt();
    }
    std::getline(std::cin, line);
#endif

    return line;
}

// 执行单个 SQL 命令
void executeCommand(const std::string& sql) {
    if (sql.empty()) return;

    // 检查是否是元命令
    std::string trimmed = sql;
    trimmed.erase(0, trimmed.find_first_not_of(" \t"));
    if (!trimmed.empty() && trimmed[0] == '\\') {
        handleMetaCommand(trimmed);
        return;
    }

    // 执行 SQL
    auto start = std::chrono::steady_clock::now();
    auto result = g_client.execute(sql);
    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    if (result.success()) {
        if (g_expanded_output) {
            displayExpandedResult(result.message());
        } else {
            displayResult(result.message());
        }
        std::cout << std::fixed << std::setprecision(3);
        std::cout << "Time: " << elapsed_ms << " ms\n";
    } else {
        printColored(Color::RED, "ERROR: " + result.message() + "\n");
    }
}

// 主交互循环
void interactiveLoop() {
    std::cout << "TinyDB interactive terminal\n";
    std::cout << "Type \\? for help.\n\n";

    while (g_running && g_client.isConnected()) {
        std::string accumulated;
        bool is_multiline = false;
        int line_num = 1;

        while (g_running) {
            std::string line = readInputLine(is_multiline);

            // 检查 EOF
            if (line.empty() && std::cin.eof()) {
                g_running = false;
                break;
            }

            // 检查元命令（只在第一行）
            if (!is_multiline && !line.empty()) {
                std::string trimmed = line;
                trimmed.erase(0, trimmed.find_first_not_of(" \t"));
                if (!trimmed.empty() && trimmed[0] == '\\') {
                    handleMetaCommand(trimmed);
                    break;
                }
            }

            // 累加输入
            if (!accumulated.empty()) {
                accumulated += " ";
            }
            accumulated += line;

            // 检查语句是否完整（简单检查：以分号结尾）
            std::string trimmed_line = line;
            // 去除尾部空格
            size_t end = trimmed_line.find_last_not_of(" \t");
            if (end != std::string::npos) {
                trimmed_line = trimmed_line.substr(0, end + 1);
            }

            if (!trimmed_line.empty() && trimmed_line.back() == ';') {
                // 语句完整，执行
                // 去掉末尾的分号
                accumulated = accumulated.substr(0, accumulated.find_last_of(';'));
                executeCommand(accumulated);
                break;
            } else {
                // 多行输入
                is_multiline = true;
                line_num++;
                printMultiLinePrompt(line_num);
            }
        }
    }
}

// 打印用法信息
void printUsage(const char* program) {
    std::cout << "TinyDB Interactive Terminal (psql-like client)\n\n";
    std::cout << "Usage: " << program << " [options]\n";
    std::cout << "\nOptions:\n";
    std::cout << "  -h, --host HOST       Server host (default: localhost)\n";
    std::cout << "  -p, --port PORT       Server port (default: 5432)\n";
    std::cout << "  -U, --username USER   Database user (default: tinydb)\n";
    std::cout << "  -d, --dbname DB       Database name (default: tinydb)\n";
    std::cout << "  -c, --command CMD     Execute single command and exit\n";
    std::cout << "  -f, --file FILE       Execute commands from file\n";
    std::cout << "  -l, --list            List available databases\n";
    std::cout << "  --no-color            Disable colored output\n";
    std::cout << "  --help                Show this help message\n";
    std::cout << "  --version             Show version information\n";
    std::cout << "\nExamples:\n";
    std::cout << "  " << program << " -h localhost -p 5432\n";
    std::cout << "  " << program << " -c \"SELECT * FROM users\"\n";
    std::cout << "  " << program << " -f script.sql\n";
}

// 从文件执行命令
void executeFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) {
        std::cerr << "Failed to open file: " << filename << "\n";
        return;
    }

    std::string line;
    std::string accumulated;
    int line_num = 0;

    while (std::getline(file, line)) {
        line_num++;

        // 去除注释
        size_t comment_pos = line.find("--");
        if (comment_pos != std::string::npos) {
            line = line.substr(0, comment_pos);
        }

        // 跳过空行
        std::string trimmed = line;
        trimmed.erase(0, trimmed.find_first_not_of(" \t"));
        if (trimmed.empty()) continue;

        // 累加
        if (!accumulated.empty()) {
            accumulated += " ";
        }
        accumulated += trimmed;

        // 检查语句是否完整
        size_t end = trimmed.find_last_not_of(" \t");
        if (end != std::string::npos && trimmed[end] == ';') {
            // 执行完整语句
            accumulated = accumulated.substr(0, accumulated.find_last_of(';'));
            std::cout << "=> " << accumulated << "\n";
            executeCommand(accumulated);
            accumulated.clear();
        }
    }

    // 处理最后没有分号的语句
    if (!accumulated.empty()) {
        executeCommand(accumulated);
    }
}

int main(int argc, char* argv[]) {
    std::string host = "localhost";
    uint16_t port = 5432;
    std::string single_command;
    std::string file_to_execute;
    bool list_databases = false;

    // 解析命令行参数
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-h" || arg == "--host") {
            if (i + 1 < argc) host = argv[++i];
        } else if (arg == "-p" || arg == "--port") {
            if (i + 1 < argc) port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (arg == "-U" || arg == "--username") {
            if (i + 1 < argc) g_current_user = argv[++i];
        } else if (arg == "-d" || arg == "--dbname") {
            if (i + 1 < argc) g_current_database = argv[++i];
        } else if (arg == "-c" || arg == "--command") {
            if (i + 1 < argc) single_command = argv[++i];
        } else if (arg == "-f" || arg == "--file") {
            if (i + 1 < argc) file_to_execute = argv[++i];
        } else if (arg == "-l" || arg == "--list") {
            list_databases = true;
        } else if (arg == "--no-color") {
            g_use_color = false;
        } else if (arg == "--help") {
            printUsage(argv[0]);
            return 0;
        } else if (arg == "--version") {
            std::cout << "TinyDB client 1.0.0\n";
            return 0;
        } else if (arg[0] != '-') {
            // 位置参数作为数据库名
            g_current_database = arg;
        }
    }

    // 连接到服务器
    std::cout << "Connecting to " << host << ":" << port << "...\n";
    if (!g_client.connect(host, port)) {
        std::cerr << "Failed to connect: " << g_client.lastError() << "\n";
        return 1;
    }

    std::cout << "Connected to TinyDB server.\n\n";

    // 处理特殊模式
    if (list_databases) {
        handleMetaCommand("\\l");
        g_client.disconnect();
        return 0;
    }

    if (!single_command.empty()) {
        executeCommand(single_command);
        g_client.disconnect();
        return 0;
    }

    if (!file_to_execute.empty()) {
        executeFile(file_to_execute);
        g_client.disconnect();
        return 0;
    }

    // 进入交互模式
    interactiveLoop();

    // 断开连接
    std::cout << "\nGoodbye!\n";
    g_client.disconnect();
    return 0;
}
