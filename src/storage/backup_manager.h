#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <functional>

namespace tinydb {
namespace storage {

// 备份类型
enum class BackupType {
    FULL,       // 全量备份
    INCREMENTAL // 增量备份
};

// 备份元数据
struct BackupMetadata {
    std::string backup_id;           // 备份ID
    std::string backup_name;         // 备份名称
    BackupType type;                 // 备份类型
    std::chrono::system_clock::time_point start_time;  // 开始时间
    std::chrono::system_clock::time_point end_time;    // 结束时间
    uint64_t wal_start_lsn;          // 开始LSN
    uint64_t wal_end_lsn;            // 结束LSN
    size_t data_size;                // 数据大小
    size_t wal_size;                 // WAL大小
    std::string db_version;          // 数据库版本
    std::vector<std::string> tablespaces;  // 表空间列表
    bool is_valid;                   // 是否有效

    BackupMetadata() : type(BackupType::FULL), wal_start_lsn(0), wal_end_lsn(0),
                       data_size(0), wal_size(0), is_valid(false) {}
};

// 恢复点
struct RestorePoint {
    std::string name;                // 恢复点名称
    std::chrono::system_clock::time_point timestamp;  // 时间戳
    uint64_t wal_lsn;                // WAL LSN
    std::string backup_id;           // 关联的备份ID

    RestorePoint() : wal_lsn(0) {}
};

// 备份管理器
class BackupManager {
public:
    BackupManager(const std::string& data_dir);
    ~BackupManager();

    // 禁止拷贝
    BackupManager(const BackupManager&) = delete;
    BackupManager& operator=(const BackupManager&) = delete;

    // ========== 备份操作 ==========
    // 创建全量备份
    bool createFullBackup(const std::string& backup_name,
                          const std::string& destination,
                          std::function<void(int)> progress_callback = nullptr);

    // 创建增量备份
    bool createIncrementalBackup(const std::string& backup_name,
                                 const std::string& base_backup_id,
                                 const std::string& destination,
                                 std::function<void(int)> progress_callback = nullptr);

    // 列出所有备份
    std::vector<BackupMetadata> listBackups() const;

    // 获取备份信息
    bool getBackupInfo(const std::string& backup_id, BackupMetadata& info) const;

    // 删除备份
    bool deleteBackup(const std::string& backup_id);

    // 验证备份完整性
    bool verifyBackup(const std::string& backup_id);

    // ========== 恢复操作 ==========
    // 从备份恢复
    bool restoreFromBackup(const std::string& backup_id,
                           const std::string& target_dir,
                           std::function<void(int)> progress_callback = nullptr);

    // 恢复到指定时间点（PITR）
    bool restoreToPointInTime(const std::string& backup_id,
                              const std::chrono::system_clock::time_point& target_time,
                              const std::string& target_dir,
                              std::function<void(int)> progress_callback = nullptr);

    // 恢复到指定LSN
    bool restoreToLSN(const std::string& backup_id,
                      uint64_t target_lsn,
                      const std::string& target_dir,
                      std::function<void(int)> progress_callback = nullptr);

    // ========== 恢复点管理 ==========
    // 创建恢复点
    bool createRestorePoint(const std::string& point_name);

    // 列出所有恢复点
    std::vector<RestorePoint> listRestorePoints() const;

    // 删除恢复点
    bool deleteRestorePoint(const std::string& point_name);

    // ========== 归档管理 ==========
    // 启用WAL归档
    bool enableWalArchiving(const std::string& archive_dir);

    // 禁用WAL归档
    bool disableWalArchiving();

    // 归档WAL文件
    bool archiveWalFile(const std::string& wal_file);

    // 获取已归档的WAL文件列表
    std::vector<std::string> getArchivedWalFiles(uint64_t start_lsn, uint64_t end_lsn) const;

private:
    std::string data_dir_;           // 数据目录
    std::string backup_dir_;         // 备份目录
    std::string archive_dir_;        // WAL归档目录
    bool archiving_enabled_;         // 归档是否启用

    // 备份元数据文件路径
    std::string getBackupMetadataPath(const std::string& backup_id) const;

    // 保存备份元数据
    bool saveBackupMetadata(const BackupMetadata& metadata);

    // 加载备份元数据
    bool loadBackupMetadata(const std::string& backup_id, BackupMetadata& metadata) const;

    // 复制文件
    bool copyFile(const std::string& source, const std::string& dest);

    // 复制目录
    bool copyDirectory(const std::string& source, const std::string& dest,
                       std::function<void(int)> progress_callback = nullptr);

    // 计算目录大小
    size_t calculateDirectorySize(const std::string& path) const;

    // 应用WAL日志到目标目录
    bool applyWalLogs(const std::string& target_dir,
                      uint64_t start_lsn,
                      uint64_t end_lsn,
                      std::function<void(int)> progress_callback = nullptr);
};

} // namespace storage
} // namespace tinydb
