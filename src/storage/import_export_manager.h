#pragma once

#include <string>
#include <vector>
#include <functional>
#include <unordered_map>

namespace tinydb {
namespace storage {

// 导入格式
enum class ImportFormat {
    CSV,        // CSV格式
    JSON,       // JSON格式
    SQL,        // SQL INSERT语句
    BINARY      // 二进制格式
};

// 导出格式
enum class ExportFormat {
    CSV,        // CSV格式
    JSON,       // JSON格式
    SQL,        // SQL INSERT语句
    BINARY,     // 二进制格式
    MARKDOWN    // Markdown表格
};

// 导入选项
struct ImportOptions {
    ImportFormat format;            // 导入格式
    bool has_header;                // 是否有表头（CSV）
    char delimiter;                 // 分隔符（CSV）
    char quote_char;                // 引号字符（CSV）
    char escape_char;               // 转义字符（CSV）
    std::string null_string;        // NULL值表示
    std::string encoding;           // 编码
    bool ignore_errors;             // 忽略错误继续导入
    bool replace_existing;          // 替换现有数据
    int batch_size;                 // 批量插入大小
    std::vector<std::string> columns;  // 指定列（空表示所有列）

    ImportOptions()
        : format(ImportFormat::CSV)
        , has_header(true)
        , delimiter(',')
        , quote_char('"')
        , escape_char('\\')
        , null_string("NULL")
        , encoding("UTF-8")
        , ignore_errors(false)
        , replace_existing(false)
        , batch_size(1000) {}
};

// 导出选项
struct ExportOptions {
    ExportFormat format;            // 导出格式
    bool include_header;            // 包含表头（CSV）
    char delimiter;                 // 分隔符（CSV）
    char quote_char;                // 引号字符（CSV）
    std::string null_string;        // NULL值表示
    std::string encoding;           // 编码
    bool pretty_print;              // 美化输出（JSON）
    int indent_size;                // 缩进大小（JSON）
    std::string where_clause;       // WHERE条件
    std::vector<std::string> columns;  // 指定列（空表示所有列）
    int limit;                      // 限制行数（-1表示无限制）

    ExportOptions()
        : format(ExportFormat::CSV)
        , include_header(true)
        , delimiter(',')
        , quote_char('"')
        , null_string("NULL")
        , encoding("UTF-8")
        , pretty_print(true)
        , indent_size(2)
        , limit(-1) {}
};

// 导入结果
struct ImportResult {
    bool success;                   // 是否成功
    int64_t rows_imported;          // 导入行数
    int64_t rows_skipped;           // 跳过行数
    int64_t rows_failed;            // 失败行数
    std::vector<std::string> errors;    // 错误信息
    double elapsed_time_ms;         // 耗时（毫秒）

    ImportResult()
        : success(false)
        , rows_imported(0)
        , rows_skipped(0)
        , rows_failed(0)
        , elapsed_time_ms(0.0) {}
};

// 导出结果
struct ExportResult {
    bool success;                   // 是否成功
    int64_t rows_exported;          // 导出行数
    int64_t bytes_written;          // 写入字节数
    std::string error_message;      // 错误信息
    double elapsed_time_ms;         // 耗时（毫秒）

    ExportResult()
        : success(false)
        , rows_exported(0)
        , bytes_written(0)
        , elapsed_time_ms(0.0) {}
};

// TODO：没有用起来（后续完善）
// 数据导入导出管理器
class ImportExportManager {
public:
    ImportExportManager();
    ~ImportExportManager();

    // 禁止拷贝
    ImportExportManager(const ImportExportManager&) = delete;
    ImportExportManager& operator=(const ImportExportManager&) = delete;

    // ========== 导入功能 ==========
    // 从文件导入到表
    ImportResult importFromFile(const std::string& file_path,
                                 const std::string& table_name,
                                 const ImportOptions& options);

    // 从字符串导入到表
    ImportResult importFromString(const std::string& data,
                                   const std::string& table_name,
                                   const ImportOptions& options);

    // 批量导入（高性能）
    ImportResult bulkImport(const std::string& file_path,
                            const std::string& table_name,
                            const ImportOptions& options);

    // ========== 导出功能 ==========
    // 从表导出到文件
    ExportResult exportToFile(const std::string& table_name,
                               const std::string& file_path,
                               const ExportOptions& options);

    // 从表导出到字符串
    ExportResult exportToString(const std::string& table_name,
                                 std::string& output,
                                 const ExportOptions& options);

    // 从查询结果导出
    ExportResult exportQueryToFile(const std::string& query,
                                    const std::string& file_path,
                                    const ExportOptions& options);

    ExportResult exportQueryToString(const std::string& query,
                                      std::string& output,
                                      const ExportOptions& options);

    // ========== 格式转换 ==========
    // 转换CSV行为字段数组
    std::vector<std::string> parseCsvLine(const std::string& line,
                                          char delimiter = ',',
                                          char quote_char = '"');

    // 将字段数组格式化为CSV行
    std::string formatCsvLine(const std::vector<std::string>& fields,
                              char delimiter = ',',
                              char quote_char = '"');

    // 转义CSV字段
    std::string escapeCsvField(const std::string& field,
                               char delimiter = ',',
                               char quote_char = '"');

    // ========== 进度回调 ==========
    // 设置进度回调
    void setProgressCallback(std::function<void(int64_t current, int64_t total)> callback);

    // ========== 工具方法 ==========
    // 检测文件格式
    ImportFormat detectFormat(const std::string& file_path);

    // 验证导入选项
    bool validateImportOptions(const ImportOptions& options, std::string& error);

    // 验证导出选项
    bool validateExportOptions(const ExportOptions& options, std::string& error);

    // 获取支持的导入格式列表
    std::vector<std::string> getSupportedImportFormats() const;

    // 获取支持的导出格式列表
    std::vector<std::string> getSupportedExportFormats() const;

private:
    std::function<void(int64_t, int64_t)> progress_callback_;

    // CSV导入导出
    ImportResult importFromCsv(const std::string& data,
                                const std::string& table_name,
                                const ImportOptions& options);
    ExportResult exportToCsv(const std::string& table_name,
                              const ExportOptions& options,
                              std::string& output);

    // JSON导入导出
    ImportResult importFromJson(const std::string& data,
                                 const std::string& table_name,
                                 const ImportOptions& options);
    ExportResult exportToJson(const std::string& table_name,
                               const ExportOptions& options,
                               std::string& output);

    // SQL导入导出
    ImportResult importFromSql(const std::string& data,
                                const std::string& table_name,
                                const ImportOptions& options);
    ExportResult exportToSql(const std::string& table_name,
                              const ExportOptions& options,
                              std::string& output);

    // 二进制导入导出（简化实现）
    ImportResult importFromBinary(const std::string& data,
                                   const std::string& table_name,
                                   const ImportOptions& options);
    ExportResult exportToBinary(const std::string& table_name,
                                 const ExportOptions& options,
                                 std::string& output);

    // Markdown导出
    ExportResult exportToMarkdown(const std::string& table_name,
                                   const ExportOptions& options,
                                   std::string& output);

    // 报告进度
    void reportProgress(int64_t current, int64_t total);

    // 读取文件
    bool readFile(const std::string& file_path, std::string& content);

    // 写入文件
    bool writeFile(const std::string& file_path, const std::string& content);
};

// 全局导入导出管理器
extern ImportExportManager* g_import_export_manager;

} // namespace storage
} // namespace tinydb
