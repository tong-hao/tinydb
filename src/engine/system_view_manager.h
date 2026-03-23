#pragma once

#include "storage/diagnostics_manager.h"
#include "storage/import_export_manager.h"
#include "storage/backup_manager.h"
#include <string>
#include <vector>
#include <memory>

namespace tinydb {
namespace engine {

// 系统视图列定义
struct SystemViewColumn {
    std::string name;
    std::string type;
    std::string description;
};

// 系统视图
struct SystemView {
    std::string name;
    std::string schema;
    std::vector<SystemViewColumn> columns;
    std::string description;
};

// 查询结果集
struct SystemViewResult {
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
    bool success;
    std::string error_message;

    SystemViewResult() : success(true) {}
};

// 系统视图管理器 - 提供类似PostgreSQL的tn_catalog视图
class SystemViewManager {
public:
    SystemViewManager();
    ~SystemViewManager();

    // 禁止拷贝
    SystemViewManager(const SystemViewManager&) = delete;
    SystemViewManager& operator=(const SystemViewManager&) = delete;

    // 初始化
    void initialize(storage::DiagnosticsManager* diag_mgr,
                   storage::ImportExportManager* import_export_mgr,
                   storage::BackupManager* backup_mgr);

    // 检查是否是系统视图查询
    bool isSystemViewQuery(const std::string& table_name) const;

    // 查询系统视图
    SystemViewResult querySystemView(const std::string& view_name,
                                     const std::vector<std::string>& columns = {});

    // 获取所有系统视图列表
    std::vector<SystemView> listSystemViews() const;

    // 获取特定视图定义
    bool getViewDefinition(const std::string& view_name, SystemView& view) const;

private:
    storage::DiagnosticsManager* diag_mgr_;
    storage::ImportExportManager* import_export_mgr_;
    storage::BackupManager* backup_mgr_;

    // 系统视图定义
    std::unordered_map<std::string, SystemView> views_;

    // 注册系统视图
    void registerSystemViews();

    // 查询 tn_stat_activity
    SystemViewResult queryPgStatActivity();

    // 查询 tn_stat_database
    SystemViewResult queryPgStatDatabase();

    // 查询 tn_stat_user_tables
    SystemViewResult queryPgStatUserTables();

    // 查询 tn_stat_user_indexes
    SystemViewResult queryPgStatUserIndexes();

    // 查询 tn_locks
    SystemViewResult queryPgLocks();

    // 查询 tn_stat_replication
    SystemViewResult queryPgStatReplication();

    // 查询 tn_tables
    SystemViewResult queryPgTables();

    // 查询 tn_indexes
    SystemViewResult queryPgIndexes();

    // 查询 tn_user
    SystemViewResult queryPgUser();

    // 查询 tn_database
    SystemViewResult queryPgDatabase();

    // 查询备份列表
    SystemViewResult queryBackupList();

    // 查询恢复点
    SystemViewResult queryRestorePoints();
};

// 全局系统视图管理器
extern SystemViewManager* g_system_view_manager;

} // namespace engine
} // namespace tinydb
