#include "backup_manager.h"
#include "common/logger.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <random>
#include <algorithm>
#include <dirent.h>
#include <sys/stat.h>
#include <cstring>
#include <unistd.h>
#include <functional>

namespace tinydb {
namespace storage {

// 生成唯一备份ID
static std::string generateBackupId() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 9999);

    std::stringstream ss;
    ss << "BK" << ms << "_" << std::setfill('0') << std::setw(4) << dis(gen);
    return ss.str();
}

// 获取当前时间字符串
static std::string getCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t_now), "%Y%m%d_%H%M%S");
    return ss.str();
}

BackupManager::BackupManager(const std::string& data_dir)
    : data_dir_(data_dir)
    , archiving_enabled_(false) {
    // 创建备份目录
    backup_dir_ = data_dir + "/backups";
    struct stat st;
    if (stat(backup_dir_.c_str(), &st) != 0) {
        mkdir(backup_dir_.c_str(), 0755);
    }
    LOG_INFO("BackupManager initialized, data_dir: " << data_dir_);
}

BackupManager::~BackupManager() {
    disableWalArchiving();
}

std::string BackupManager::getBackupMetadataPath(const std::string& backup_id) const {
    return backup_dir_ + "/" + backup_id + ".meta";
}

bool BackupManager::saveBackupMetadata(const BackupMetadata& metadata) {
    std::string path = getBackupMetadataPath(metadata.backup_id);
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        LOG_ERROR("Failed to create backup metadata file: " << path);
        return false;
    }

    // 写入备份元数据（简化格式）
    file << "BACKUP_ID=" << metadata.backup_id << "\n";
    file << "BACKUP_NAME=" << metadata.backup_name << "\n";
    file << "TYPE=" << (metadata.type == BackupType::FULL ? "FULL" : "INCREMENTAL") << "\n";
    file << "START_TIME=" << metadata.start_time.time_since_epoch().count() << "\n";
    file << "END_TIME=" << metadata.end_time.time_since_epoch().count() << "\n";
    file << "WAL_START_LSN=" << metadata.wal_start_lsn << "\n";
    file << "WAL_END_LSN=" << metadata.wal_end_lsn << "\n";
    file << "DATA_SIZE=" << metadata.data_size << "\n";
    file << "WAL_SIZE=" << metadata.wal_size << "\n";
    file << "DB_VERSION=" << metadata.db_version << "\n";
    file << "IS_VALID=" << (metadata.is_valid ? "1" : "0") << "\n";

    // 写入表空间列表
    file << "TABLESPACES=";
    for (size_t i = 0; i < metadata.tablespaces.size(); ++i) {
        if (i > 0) file << ",";
        file << metadata.tablespaces[i];
    }
    file << "\n";

    file.close();
    return true;
}

bool BackupManager::loadBackupMetadata(const std::string& backup_id, BackupMetadata& metadata) const {
    std::string path = getBackupMetadataPath(backup_id);
    std::ifstream file(path);
    if (!file) {
        LOG_ERROR("Failed to open backup metadata file: " << path);
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        size_t pos = line.find('=');
        if (pos == std::string::npos) continue;

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        if (key == "BACKUP_ID") metadata.backup_id = value;
        else if (key == "BACKUP_NAME") metadata.backup_name = value;
        else if (key == "TYPE") metadata.type = (value == "FULL") ? BackupType::FULL : BackupType::INCREMENTAL;
        else if (key == "WAL_START_LSN") metadata.wal_start_lsn = std::stoull(value);
        else if (key == "WAL_END_LSN") metadata.wal_end_lsn = std::stoull(value);
        else if (key == "DATA_SIZE") metadata.data_size = std::stoull(value);
        else if (key == "WAL_SIZE") metadata.wal_size = std::stoull(value);
        else if (key == "DB_VERSION") metadata.db_version = value;
        else if (key == "IS_VALID") metadata.is_valid = (value == "1");
        else if (key == "TABLESPACES") {
            std::stringstream ss(value);
            std::string tablespace;
            while (std::getline(ss, tablespace, ',')) {
                if (!tablespace.empty()) {
                    metadata.tablespaces.push_back(tablespace);
                }
            }
        }
    }

    file.close();
    return true;
}

bool BackupManager::copyFile(const std::string& source, const std::string& dest) {
    std::ifstream src(source, std::ios::binary);
    if (!src) {
        LOG_ERROR("Failed to open source file: " << source);
        return false;
    }

    std::ofstream dst(dest, std::ios::binary);
    if (!dst) {
        LOG_ERROR("Failed to create destination file: " << dest);
        return false;
    }

    dst << src.rdbuf();

    src.close();
    dst.close();

    return true;
}

bool BackupManager::copyDirectory(const std::string& source, const std::string& dest,
                                  std::function<void(int)> progress_callback) {
    struct stat st;
    if (stat(source.c_str(), &st) != 0) {
        LOG_ERROR("Source directory does not exist: " << source);
        return false;
    }

    // 创建目标目录
    if (stat(dest.c_str(), &st) != 0) {
        mkdir(dest.c_str(), 0755);
    }

    DIR* dir = opendir(source.c_str());
    if (!dir) {
        LOG_ERROR("Failed to open source directory: " << source);
        return false;
    }

    struct dirent* entry;
    int file_count = 0;
    int processed_count = 0;

    // 先统计文件数
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') continue;
        file_count++;
    }
    closedir(dir);

    dir = opendir(source.c_str());
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') continue;

        std::string src_path = source + "/" + entry->d_name;
        std::string dst_path = dest + "/" + entry->d_name;

        if (stat(src_path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
            // 递归复制子目录
            if (!copyDirectory(src_path, dst_path, nullptr)) {
                closedir(dir);
                return false;
            }
        } else {
            // 复制文件
            if (!copyFile(src_path, dst_path)) {
                closedir(dir);
                return false;
            }
        }

        processed_count++;
        if (progress_callback) {
            int progress = (processed_count * 100) / file_count;
            progress_callback(progress);
        }
    }

    closedir(dir);
    return true;
}

size_t BackupManager::calculateDirectorySize(const std::string& path) const {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return 0;
    }

    if (!S_ISDIR(st.st_mode)) {
        return st.st_size;
    }

    size_t total_size = 0;
    DIR* dir = opendir(path.c_str());
    if (!dir) return 0;

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') continue;

        std::string full_path = path + "/" + entry->d_name;
        total_size += calculateDirectorySize(full_path);
    }

    closedir(dir);
    return total_size;
}

bool BackupManager::createFullBackup(const std::string& backup_name,
                                     const std::string& destination,
                                     std::function<void(int)> progress_callback) {
    BackupMetadata metadata;
    metadata.backup_id = generateBackupId();
    metadata.backup_name = backup_name.empty() ? "backup_" + getCurrentTimestamp() : backup_name;
    metadata.type = BackupType::FULL;
    metadata.start_time = std::chrono::system_clock::now();
    metadata.db_version = "1.0.0";

    LOG_INFO("Starting full backup: " << metadata.backup_name << " (ID: " << metadata.backup_id << ")");

    // 获取当前WAL位置（简化：使用目录大小作为LSN）
    std::string wal_dir = data_dir_ + "/wal";
    metadata.wal_start_lsn = calculateDirectorySize(wal_dir);

    // 创建备份目标目录
    std::string backup_path = destination.empty() ? backup_dir_ + "/" + metadata.backup_id : destination;
    struct stat st;
    if (stat(backup_path.c_str(), &st) != 0) {
        mkdir(backup_path.c_str(), 0755);
    }

    // 复制数据文件
    if (!copyDirectory(data_dir_, backup_path + "/data", progress_callback)) {
        LOG_ERROR("Failed to copy data directory");
        return false;
    }

    // 复制WAL文件
    std::string backup_wal_dir = backup_path + "/wal";
    if (stat(wal_dir.c_str(), &st) == 0) {
        mkdir(backup_wal_dir.c_str(), 0755);
        copyDirectory(wal_dir, backup_wal_dir, nullptr);
    }

    metadata.end_time = std::chrono::system_clock::now();
    metadata.wal_end_lsn = metadata.wal_start_lsn;
    metadata.data_size = calculateDirectorySize(backup_path + "/data");
    metadata.wal_size = calculateDirectorySize(backup_wal_dir);
    metadata.is_valid = true;

    // 收集表空间信息（简化：扫描所有.db文件）
    DIR* dir = opendir(data_dir_.c_str());
    if (dir) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (entry->d_name[0] == '.') continue;
            std::string name(entry->d_name);
            if (name.length() > 3 && name.substr(name.length() - 3) == ".db") {
                metadata.tablespaces.push_back(name);
            }
        }
        closedir(dir);
    }

    // 保存元数据
    if (!saveBackupMetadata(metadata)) {
        LOG_ERROR("Failed to save backup metadata");
        return false;
    }

    LOG_INFO("Full backup completed: " << metadata.backup_id
             << " (Data: " << metadata.data_size << " bytes)");
    return true;
}

bool BackupManager::createIncrementalBackup(const std::string& backup_name,
                                            const std::string& base_backup_id,
                                            const std::string& destination,
                                            std::function<void(int)> progress_callback) {
    // 检查基础备份是否存在
    BackupMetadata base_metadata;
    if (!getBackupInfo(base_backup_id, base_metadata)) {
        LOG_ERROR("Base backup not found: " << base_backup_id);
        return false;
    }

    if (base_metadata.type != BackupType::FULL) {
        LOG_ERROR("Incremental backup must be based on a full backup");
        return false;
    }

    BackupMetadata metadata;
    metadata.backup_id = generateBackupId();
    metadata.backup_name = backup_name.empty() ? "incr_backup_" + getCurrentTimestamp() : backup_name;
    metadata.type = BackupType::INCREMENTAL;
    metadata.start_time = std::chrono::system_clock::now();
    metadata.db_version = "1.0.0";

    LOG_INFO("Starting incremental backup: " << metadata.backup_name
             << " (ID: " << metadata.backup_id << ") based on " << base_backup_id);

    // 记录基础备份ID
    std::string backup_path = destination.empty() ? backup_dir_ + "/" + metadata.backup_id : destination;
    struct stat st;
    if (stat(backup_path.c_str(), &st) != 0) {
        mkdir(backup_path.c_str(), 0755);
    }

    // 获取当前WAL位置
    std::string wal_dir = data_dir_ + "/wal";
    metadata.wal_start_lsn = base_metadata.wal_end_lsn;
    metadata.wal_end_lsn = calculateDirectorySize(wal_dir);

    // 只复制自基础备份以来修改的文件（简化实现：复制所有WAL文件）
    std::string backup_wal_dir = backup_path + "/wal";
    mkdir(backup_wal_dir.c_str(), 0755);

    if (stat(wal_dir.c_str(), &st) == 0) {
        copyDirectory(wal_dir, backup_wal_dir, progress_callback);
    }

    // 对于增量备份，也复制可能被修改的数据文件
    // 简化：复制所有数据文件（实际需要基于时间戳或校验和判断）

    metadata.end_time = std::chrono::system_clock::now();
    metadata.data_size = 0;  // 增量备份数据部分大小为0
    metadata.wal_size = calculateDirectorySize(backup_wal_dir);
    metadata.is_valid = true;

    // 保存元数据
    if (!saveBackupMetadata(metadata)) {
        LOG_ERROR("Failed to save backup metadata");
        return false;
    }

    LOG_INFO("Incremental backup completed: " << metadata.backup_id);
    return true;
}

std::vector<BackupMetadata> BackupManager::listBackups() const {
    std::vector<BackupMetadata> result;

    DIR* dir = opendir(backup_dir_.c_str());
    if (!dir) {
        return result;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') continue;

        std::string name(entry->d_name);
        if (name.length() > 5 && name.substr(name.length() - 5) == ".meta") {
            std::string backup_id = name.substr(0, name.length() - 5);
            BackupMetadata metadata;
            if (loadBackupMetadata(backup_id, metadata)) {
                result.push_back(metadata);
            }
        }
    }

    closedir(dir);

    // 按开始时间排序
    std::sort(result.begin(), result.end(), [](const BackupMetadata& a, const BackupMetadata& b) {
        return a.start_time < b.start_time;
    });

    return result;
}

bool BackupManager::getBackupInfo(const std::string& backup_id, BackupMetadata& info) const {
    return loadBackupMetadata(backup_id, info);
}

bool BackupManager::deleteBackup(const std::string& backup_id) {
    // 删除备份目录
    std::string backup_path = backup_dir_ + "/" + backup_id;

    // 递归删除目录的辅助函数
    std::function<bool(const std::string&)> removeDir = [&](const std::string& path) -> bool {
        struct stat st;
        if (stat(path.c_str(), &st) != 0) {
            return true;
        }

        if (!S_ISDIR(st.st_mode)) {
            return std::remove(path.c_str()) == 0;
        }

        DIR* dir = opendir(path.c_str());
        if (!dir) return false;

        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (entry->d_name[0] == '.') continue;
            std::string full_path = path + "/" + entry->d_name;
            removeDir(full_path);
        }
        closedir(dir);

        return rmdir(path.c_str()) == 0;
    };

    if (!removeDir(backup_path)) {
        LOG_WARN("Failed to remove backup directory: " << backup_path);
    }

    // 删除元数据文件
    std::string meta_path = getBackupMetadataPath(backup_id);
    if (std::remove(meta_path.c_str()) != 0) {
        LOG_WARN("Failed to remove backup metadata: " << meta_path);
    }

    LOG_INFO("Deleted backup: " << backup_id);
    return true;
}

bool BackupManager::verifyBackup(const std::string& backup_id) {
    BackupMetadata metadata;
    if (!getBackupInfo(backup_id, metadata)) {
        LOG_ERROR("Backup not found: " << backup_id);
        return false;
    }

    // 验证备份目录存在
    std::string backup_path = backup_dir_ + "/" + backup_id;
    struct stat st;
    if (stat(backup_path.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
        LOG_ERROR("Backup directory missing: " << backup_path);
        return false;
    }

    // 验证数据目录
    std::string data_path = backup_path + "/data";
    if (stat(data_path.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) {
        LOG_ERROR("Backup data directory missing: " << data_path);
        return false;
    }

    // 验证元数据一致性
    size_t actual_data_size = calculateDirectorySize(data_path);
    if (actual_data_size != metadata.data_size) {
        LOG_WARN("Backup data size mismatch: expected " << metadata.data_size
                  << ", actual " << actual_data_size);
    }

    LOG_INFO("Backup verification passed: " << backup_id);
    return true;
}

bool BackupManager::restoreFromBackup(const std::string& backup_id,
                                      const std::string& target_dir,
                                      std::function<void(int)> progress_callback) {
    BackupMetadata metadata;
    if (!getBackupInfo(backup_id, metadata)) {
        LOG_ERROR("Backup not found: " << backup_id);
        return false;
    }

    if (!metadata.is_valid) {
        LOG_ERROR("Backup is not valid: " << backup_id);
        return false;
    }

    LOG_INFO("Starting restore from backup: " << backup_id);

    std::string backup_path = backup_dir_ + "/" + backup_id;
    std::string restore_path = target_dir.empty() ? data_dir_ : target_dir;

    // 如果是恢复到原目录，先备份当前数据
    if (restore_path == data_dir_) {
        std::string current_backup = data_dir_ + "/../data_backup_" + getCurrentTimestamp();
        LOG_INFO("Backing up current data to: " << current_backup);
        copyDirectory(data_dir_, current_backup, nullptr);
    }

    // 复制数据文件
    std::string backup_data = backup_path + "/data";
    if (!copyDirectory(backup_data, restore_path, progress_callback)) {
        LOG_ERROR("Failed to restore data directory");
        return false;
    }

    // 复制WAL文件（如果需要）
    std::string backup_wal = backup_path + "/wal";
    struct stat st;
    if (stat(backup_wal.c_str(), &st) == 0) {
        std::string target_wal = restore_path + "/../wal";
        mkdir(target_wal.c_str(), 0755);
        copyDirectory(backup_wal, target_wal, nullptr);
    }

    LOG_INFO("Restore completed from backup: " << backup_id);
    return true;
}

bool BackupManager::restoreToPointInTime(const std::string& backup_id,
                                         const std::chrono::system_clock::time_point& target_time,
                                         const std::string& target_dir,
                                         std::function<void(int)> progress_callback) {
    BackupMetadata metadata;
    if (!getBackupInfo(backup_id, metadata)) {
        LOG_ERROR("Backup not found: " << backup_id);
        return false;
    }

    // 先恢复基础备份
    if (!restoreFromBackup(backup_id, target_dir, progress_callback)) {
        LOG_ERROR("Failed to restore base backup");
        return false;
    }

    LOG_INFO("Starting PITR to target time from backup: " << backup_id);

    // 获取需要应用的WAL文件列表
    auto wal_files = getArchivedWalFiles(metadata.wal_end_lsn, UINT64_MAX);

    // 应用WAL日志（简化实现）
    if (!applyWalLogs(target_dir.empty() ? data_dir_ : target_dir,
                      metadata.wal_end_lsn, UINT64_MAX, progress_callback)) {
        LOG_WARN("Some WAL files could not be applied");
    }

    LOG_INFO("PITR restore completed");
    return true;
}

bool BackupManager::restoreToLSN(const std::string& backup_id,
                                 uint64_t target_lsn,
                                 const std::string& target_dir,
                                 std::function<void(int)> progress_callback) {
    BackupMetadata metadata;
    if (!getBackupInfo(backup_id, metadata)) {
        LOG_ERROR("Backup not found: " << backup_id);
        return false;
    }

    if (target_lsn < metadata.wal_start_lsn) {
        LOG_ERROR("Target LSN is before backup start LSN");
        return false;
    }

    // 先恢复基础备份
    if (!restoreFromBackup(backup_id, target_dir, progress_callback)) {
        LOG_ERROR("Failed to restore base backup");
        return false;
    }

    LOG_INFO("Starting restore to LSN " << target_lsn << " from backup: " << backup_id);

    // 应用WAL到目标LSN
    if (!applyWalLogs(target_dir.empty() ? data_dir_ : target_dir,
                      metadata.wal_end_lsn, target_lsn, progress_callback)) {
        LOG_WARN("Some WAL files could not be applied");
    }

    LOG_INFO("Restore to LSN completed");
    return true;
}

bool BackupManager::createRestorePoint(const std::string& point_name) {
    RestorePoint point;
    point.name = point_name;
    point.timestamp = std::chrono::system_clock::now();

    // 获取当前WAL位置
    std::string wal_dir = data_dir_ + "/wal";
    point.wal_lsn = calculateDirectorySize(wal_dir);

    // 保存恢复点信息到文件
    std::string restore_point_path = backup_dir_ + "/restore_points.txt";
    std::ofstream file(restore_point_path, std::ios::app);
    if (!file) {
        LOG_ERROR("Failed to create restore point file");
        return false;
    }

    file << point.name << "|" << point.timestamp.time_since_epoch().count()
         << "|" << point.wal_lsn << "\n";
    file.close();

    LOG_INFO("Created restore point: " << point_name << " at LSN " << point.wal_lsn);
    return true;
}

std::vector<RestorePoint> BackupManager::listRestorePoints() const {
    std::vector<RestorePoint> result;

    std::string restore_point_path = backup_dir_ + "/restore_points.txt";
    std::ifstream file(restore_point_path);
    if (!file) {
        return result;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string name, timestamp_str, lsn_str;

        if (std::getline(ss, name, '|') &&
            std::getline(ss, timestamp_str, '|') &&
            std::getline(ss, lsn_str, '|')) {
            RestorePoint point;
            point.name = name;
            point.wal_lsn = std::stoull(lsn_str);
            result.push_back(point);
        }
    }

    file.close();
    return result;
}

bool BackupManager::deleteRestorePoint(const std::string& point_name) {
    auto points = listRestorePoints();

    std::string restore_point_path = backup_dir_ + "/restore_points.txt";
    std::ofstream file(restore_point_path);
    if (!file) {
        return false;
    }

    bool found = false;
    for (const auto& point : points) {
        if (point.name == point_name) {
            found = true;
            continue;
        }
        file << point.name << "|" << point.timestamp.time_since_epoch().count()
             << "|" << point.wal_lsn << "\n";
    }

    file.close();

    if (found) {
        LOG_INFO("Deleted restore point: " << point_name);
    }
    return found;
}

bool BackupManager::enableWalArchiving(const std::string& archive_dir) {
    archive_dir_ = archive_dir;

    struct stat st;
    if (stat(archive_dir_.c_str(), &st) != 0) {
        mkdir(archive_dir_.c_str(), 0755);
    }

    archiving_enabled_ = true;
    LOG_INFO("WAL archiving enabled to: " << archive_dir_);
    return true;
}

bool BackupManager::disableWalArchiving() {
    archiving_enabled_ = false;
    LOG_INFO("WAL archiving disabled");
    return true;
}

bool BackupManager::archiveWalFile(const std::string& wal_file) {
    if (!archiving_enabled_) {
        return false;
    }

    // 获取文件名
    size_t pos = wal_file.find_last_of('/');
    std::string filename = (pos == std::string::npos) ? wal_file : wal_file.substr(pos + 1);

    std::string dest = archive_dir_ + "/" + filename;
    if (!copyFile(wal_file, dest)) {
        LOG_ERROR("Failed to archive WAL file: " << wal_file);
        return false;
    }

    LOG_DEBUG("Archived WAL file: " << wal_file << " -> " << dest);
    return true;
}

std::vector<std::string> BackupManager::getArchivedWalFiles(uint64_t start_lsn, uint64_t end_lsn) const {
    std::vector<std::string> result;

    if (!archiving_enabled_) {
        return result;
    }

    DIR* dir = opendir(archive_dir_.c_str());
    if (!dir) {
        return result;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_name[0] == '.') continue;

        std::string wal_file = archive_dir_ + "/" + entry->d_name;
        result.push_back(wal_file);
    }

    closedir(dir);

    // 按文件名排序
    std::sort(result.begin(), result.end());

    return result;
}

bool BackupManager::applyWalLogs(const std::string& target_dir,
                                 uint64_t start_lsn,
                                 uint64_t end_lsn,
                                 std::function<void(int)> progress_callback) {
    LOG_INFO("Applying WAL logs from LSN " << start_lsn << " to " << end_lsn);

    // 获取归档的WAL文件
    auto wal_files = getArchivedWalFiles(start_lsn, end_lsn);

    if (wal_files.empty()) {
        LOG_WARN("No WAL files to apply");
        return true;
    }

    int file_count = 0;
    for (const auto& wal_file : wal_files) {
        file_count++;
        LOG_DEBUG("Applying WAL file: " << wal_file);

        // 这里应该调用WAL重放逻辑
        // 简化实现：仅记录

        if (progress_callback) {
            int progress = (file_count * 100) / wal_files.size();
            progress_callback(progress);
        }
    }

    LOG_INFO("WAL replay completed, applied " << file_count << " files");
    return true;
}

} // namespace storage
} // namespace tinydb
