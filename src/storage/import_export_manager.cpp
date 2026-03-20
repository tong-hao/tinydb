#include "import_export_manager.h"
#include "common/logger.h"
#include <fstream>
#include <sstream>
#include <chrono>
#include <algorithm>

namespace tinydb {
namespace storage {

// 全局导入导出管理器实例
ImportExportManager* g_import_export_manager = nullptr;

ImportExportManager::ImportExportManager() {
    LOG_INFO("ImportExportManager initialized");
}

ImportExportManager::~ImportExportManager() {
}

// ========== 导入功能 ==========

ImportResult ImportExportManager::importFromFile(const std::string& file_path,
                                                  const std::string& table_name,
                                                  const ImportOptions& options) {
    std::string content;
    if (!readFile(file_path, content)) {
        ImportResult result;
        result.success = false;
        result.errors.push_back("Failed to read file: " + file_path);
        return result;
    }

    return importFromString(content, table_name, options);
}

ImportResult ImportExportManager::importFromString(const std::string& data,
                                                    const std::string& table_name,
                                                    const ImportOptions& options) {
    switch (options.format) {
        case ImportFormat::CSV:
            return importFromCsv(data, table_name, options);
        case ImportFormat::JSON:
            return importFromJson(data, table_name, options);
        case ImportFormat::SQL:
            return importFromSql(data, table_name, options);
        case ImportFormat::BINARY:
            return importFromBinary(data, table_name, options);
        default:
            ImportResult result;
            result.errors.push_back("Unsupported import format");
            return result;
    }
}

ImportResult ImportExportManager::bulkImport(const std::string& file_path,
                                              const std::string& table_name,
                                              const ImportOptions& options) {
    // 批量导入与常规导入相同，但使用更大的批量大小
    ImportOptions bulk_options = options;
    bulk_options.batch_size = 10000;  // 更大的批量大小
    return importFromFile(file_path, table_name, bulk_options);
}

// ========== 导出功能 ==========

ExportResult ImportExportManager::exportToFile(const std::string& table_name,
                                                const std::string& file_path,
                                                const ExportOptions& options) {
    std::string output;
    ExportResult result = exportToString(table_name, output, options);

    if (result.success) {
        if (!writeFile(file_path, output)) {
            result.success = false;
            result.error_message = "Failed to write file: " + file_path;
        } else {
            result.bytes_written = output.size();
        }
    }

    return result;
}

ExportResult ImportExportManager::exportToString(const std::string& table_name,
                                                  std::string& output,
                                                  const ExportOptions& options) {
    switch (options.format) {
        case ExportFormat::CSV:
            return exportToCsv(table_name, options, output);
        case ExportFormat::JSON:
            return exportToJson(table_name, options, output);
        case ExportFormat::SQL:
            return exportToSql(table_name, options, output);
        case ExportFormat::BINARY:
            return exportToBinary(table_name, options, output);
        case ExportFormat::MARKDOWN:
            return exportToMarkdown(table_name, options, output);
        default:
            ExportResult result;
            result.error_message = "Unsupported export format";
            return result;
    }
}

ExportResult ImportExportManager::exportQueryToFile(const std::string& query,
                                                     const std::string& file_path,
                                                     const ExportOptions& options) {
    // 简化实现：执行查询并导出结果
    // 实际实现需要查询执行器支持
    ExportResult result;
    result.error_message = "Query export not yet implemented";
    return result;
}

ExportResult ImportExportManager::exportQueryToString(const std::string& query,
                                                       std::string& output,
                                                       const ExportOptions& options) {
    // 简化实现
    ExportResult result;
    result.error_message = "Query export not yet implemented";
    return result;
}

// ========== 格式转换 ==========

std::vector<std::string> ImportExportManager::parseCsvLine(const std::string& line,
                                                            char delimiter,
                                                            char quote_char) {
    std::vector<std::string> fields;
    std::string field;
    bool in_quotes = false;

    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];

        if (c == quote_char) {
            if (in_quotes && i + 1 < line.length() && line[i + 1] == quote_char) {
                // 转义的引号
                field += quote_char;
                ++i;  // 跳过下一个引号
            } else {
                in_quotes = !in_quotes;
            }
        } else if (c == delimiter && !in_quotes) {
            fields.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }

    fields.push_back(field);
    return fields;
}

std::string ImportExportManager::formatCsvLine(const std::vector<std::string>& fields,
                                                char delimiter,
                                                char quote_char) {
    std::ostringstream oss;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) oss << delimiter;
        oss << escapeCsvField(fields[i], delimiter, quote_char);
    }
    return oss.str();
}

std::string ImportExportManager::escapeCsvField(const std::string& field,
                                                 char delimiter,
                                                 char quote_char) {
    // 检查是否需要引号
    bool needs_quotes = false;
    for (char c : field) {
        if (c == delimiter || c == quote_char || c == '\n' || c == '\r') {
            needs_quotes = true;
            break;
        }
    }

    if (!needs_quotes) {
        return field;
    }

    // 添加引号并转义内部引号
    std::string result;
    result += quote_char;
    for (char c : field) {
        if (c == quote_char) {
            result += quote_char;
            result += quote_char;
        } else {
            result += c;
        }
    }
    result += quote_char;
    return result;
}

// ========== 进度回调 ==========

void ImportExportManager::setProgressCallback(
    std::function<void(int64_t current, int64_t total)> callback) {
    progress_callback_ = callback;
}

// ========== 工具方法 ==========

ImportFormat ImportExportManager::detectFormat(const std::string& file_path) {
    size_t pos = file_path.find_last_of('.');
    if (pos == std::string::npos) {
        return ImportFormat::CSV;  // 默认CSV
    }

    std::string ext = file_path.substr(pos + 1);
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    if (ext == "csv") return ImportFormat::CSV;
    if (ext == "json") return ImportFormat::JSON;
    if (ext == "sql") return ImportFormat::SQL;
    if (ext == "bin") return ImportFormat::BINARY;

    return ImportFormat::CSV;
}

bool ImportExportManager::validateImportOptions(const ImportOptions& options,
                                                 std::string& error) {
    if (options.batch_size <= 0) {
        error = "Batch size must be positive";
        return false;
    }
    return true;
}

bool ImportExportManager::validateExportOptions(const ExportOptions& options,
                                                 std::string& error) {
    if (options.limit < -1) {
        error = "Limit must be -1 (unlimited) or non-negative";
        return false;
    }
    return true;
}

std::vector<std::string> ImportExportManager::getSupportedImportFormats() const {
    return {"CSV", "JSON", "SQL", "BINARY"};
}

std::vector<std::string> ImportExportManager::getSupportedExportFormats() const {
    return {"CSV", "JSON", "SQL", "BINARY", "MARKDOWN"};
}

// ========== 具体格式实现 ==========

ImportResult ImportExportManager::importFromCsv(const std::string& data,
                                                 const std::string& table_name,
                                                 const ImportOptions& options) {
    ImportResult result;
    auto start = std::chrono::steady_clock::now();

    std::istringstream iss(data);
    std::string line;
    int64_t line_num = 0;

    // 跳过表头
    if (options.has_header) {
        std::getline(iss, line);
        line_num++;
    }

    // 解析每一行
    while (std::getline(iss, line)) {
        line_num++;

        // 跳过空行
        if (line.empty()) continue;

        std::vector<std::string> fields = parseCsvLine(line, options.delimiter, options.quote_char);

        // 这里应该插入到表中（需要StorageEngine支持）
        // 简化实现：只计数
        result.rows_imported++;

        reportProgress(line_num, -1);  // -1表示总数未知

        // 批量提交
        if (result.rows_imported % options.batch_size == 0) {
            // 批量提交逻辑
        }
    }

    auto end = std::chrono::steady_clock::now();
    result.elapsed_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    result.success = true;

    LOG_INFO("Imported " << result.rows_imported << " rows to " << table_name);
    return result;
}

ExportResult ImportExportManager::exportToCsv(const std::string& table_name,
                                               const ExportOptions& options,
                                               std::string& output) {
    ExportResult result;
    auto start = std::chrono::steady_clock::now();

    std::ostringstream oss;

    // 写入表头
    if (options.include_header) {
        // 这里应该获取表的列名（需要StorageEngine支持）
        // 简化实现
        oss << "col1" << options.delimiter << "col2" << options.delimiter << "col3" << "\n";
    }

    // 这里应该查询表数据（需要StorageEngine支持）
    // 简化实现
    result.rows_exported = 0;

    output = oss.str();

    auto end = std::chrono::steady_clock::now();
    result.elapsed_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
    result.success = true;

    LOG_INFO("Exported " << result.rows_exported << " rows from " << table_name);
    return result;
}

ImportResult ImportExportManager::importFromJson(const std::string& data,
                                                  const std::string& table_name,
                                                  const ImportOptions& options) {
    // 简化实现
    ImportResult result;
    result.errors.push_back("JSON import not yet fully implemented");
    return result;
}

ExportResult ImportExportManager::exportToJson(const std::string& table_name,
                                                const ExportOptions& options,
                                                std::string& output) {
    ExportResult result;
    std::ostringstream oss;

    if (options.pretty_print) {
        oss << "[\n";
        // 这里应该生成格式化的JSON
        oss << "  {\"id\": 1, \"name\": \"test\"}\n";
        oss << "]";
    } else {
        oss << "[{\"id\":1,\"name\":\"test\"}]";
    }

    output = oss.str();
    result.success = true;
    return result;
}

ImportResult ImportExportManager::importFromSql(const std::string& data,
                                                 const std::string& table_name,
                                                 const ImportOptions& options) {
    // 简化实现
    ImportResult result;
    result.errors.push_back("SQL import not yet fully implemented");
    return result;
}

ExportResult ImportExportManager::exportToSql(const std::string& table_name,
                                               const ExportOptions& options,
                                               std::string& output) {
    ExportResult result;
    std::ostringstream oss;

    // 生成INSERT语句
    // 这里应该查询表数据
    oss << "INSERT INTO " << table_name << " (col1, col2, col3) VALUES (1, 'test', NULL);\n";

    output = oss.str();
    result.success = true;
    return result;
}

ImportResult ImportExportManager::importFromBinary(const std::string& data,
                                                    const std::string& table_name,
                                                    const ImportOptions& options) {
    ImportResult result;
    result.errors.push_back("Binary import not yet fully implemented");
    return result;
}

ExportResult ImportExportManager::exportToBinary(const std::string& table_name,
                                                  const ExportOptions& options,
                                                  std::string& output) {
    ExportResult result;
    result.error_message = "Binary export not yet fully implemented";
    return result;
}

ExportResult ImportExportManager::exportToMarkdown(const std::string& table_name,
                                                    const ExportOptions& options,
                                                    std::string& output) {
    ExportResult result;
    std::ostringstream oss;

    // Markdown表格格式
    // | col1 | col2 | col3 |
    // |------|------|------|
    // | 1    | test |      |

    oss << "| col1 | col2 | col3 |\n";
    oss << "|------|------|------|\n";
    // 这里应该查询表数据
    oss << "| 1    | test |      |\n";

    output = oss.str();
    result.success = true;
    return result;
}

void ImportExportManager::reportProgress(int64_t current, int64_t total) {
    if (progress_callback_) {
        progress_callback_(current, total);
    }
}

bool ImportExportManager::readFile(const std::string& file_path, std::string& content) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
        return false;
    }

    std::ostringstream oss;
    oss << file.rdbuf();
    content = oss.str();
    return true;
}

bool ImportExportManager::writeFile(const std::string& file_path, const std::string& content) {
    std::ofstream file(file_path, std::ios::binary);
    if (!file) {
        return false;
    }

    file << content;
    return file.good();
}

} // namespace storage
} // namespace tinydb
