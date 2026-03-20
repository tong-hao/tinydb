#include "system_view_manager.h"
#include "common/logger.h"
#include <chrono>

namespace tinydb {
namespace engine {

// 获取当前时间戳（毫秒）
static int64_t getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// 全局系统视图管理器实例
SystemViewManager* g_system_view_manager = nullptr;

SystemViewManager::SystemViewManager()
    : diag_mgr_(nullptr)
    , import_export_mgr_(nullptr)
    , backup_mgr_(nullptr) {
    registerSystemViews();
}

SystemViewManager::~SystemViewManager() {
}

void SystemViewManager::initialize(storage::DiagnosticsManager* diag_mgr,
                                  storage::ImportExportManager* import_export_mgr,
                                  storage::BackupManager* backup_mgr) {
    diag_mgr_ = diag_mgr;
    import_export_mgr_ = import_export_mgr;
    backup_mgr_ = backup_mgr;
    LOG_INFO("SystemViewManager initialized");
}

void SystemViewManager::registerSystemViews() {
    // pg_stat_activity - 当前活动
    views_["pg_stat_activity"] = {
        "pg_stat_activity",
        "pg_catalog",
        {
            {"pid", "INT", "进程ID"},
            {"usename", "TEXT", "用户名"},
            {"application_name", "TEXT", "应用名"},
            {"client_addr", "TEXT", "客户端地址"},
            {"client_port", "INT", "客户端端口"},
            {"database_name", "TEXT", "数据库名"},
            {"query", "TEXT", "当前查询"},
            {"state", "TEXT", "状态"},
            {"query_start", "TIMESTAMP", "查询开始时间"},
            {"backend_start", "TIMESTAMP", "后端启动时间"}
        },
        "当前数据库活动会话"
    };

    // pg_stat_database - 数据库统计
    views_["pg_stat_database"] = {
        "pg_stat_database",
        "pg_catalog",
        {
            {"datname", "TEXT", "数据库名"},
            {"numbackends", "INT", "后端数"},
            {"xact_commit", "BIGINT", "提交数"},
            {"xact_rollback", "BIGINT", "回滚数"},
            {"blks_read", "BIGINT", "块读取数"},
            {"blks_hit", "BIGINT", "块命中数"},
            {"tup_returned", "BIGINT", "返回行数"},
            {"tup_fetched", "BIGINT", "获取行数"},
            {"tup_inserted", "BIGINT", "插入行数"},
            {"tup_updated", "BIGINT", "更新行数"},
            {"tup_deleted", "BIGINT", "删除行数"},
            {"conflicts", "BIGINT", "冲突数"},
            {"temp_files", "BIGINT", "临时文件数"},
            {"temp_bytes", "BIGINT", "临时文件大小"},
            {"deadlocks", "BIGINT", "死锁数"},
            {"blk_read_time", "DOUBLE", "读块时间"},
            {"blk_write_time", "DOUBLE", "写块时间"}
        },
        "数据库级统计信息"
    };

    // pg_stat_user_tables - 用户表统计
    views_["pg_stat_user_tables"] = {
        "pg_stat_user_tables",
        "pg_catalog",
        {
            {"relid", "INT", "表ID"},
            {"schemaname", "TEXT", "模式名"},
            {"relname", "TEXT", "表名"},
            {"seq_scan", "BIGINT", "顺序扫描次数"},
            {"seq_tup_read", "BIGINT", "顺序扫描读取行数"},
            {"idx_scan", "BIGINT", "索引扫描次数"},
            {"idx_tup_fetch", "BIGINT", "索引扫描获取行数"},
            {"n_tup_ins", "BIGINT", "插入行数"},
            {"n_tup_upd", "BIGINT", "更新行数"},
            {"n_tup_del", "BIGINT", "删除行数"},
            {"n_live_tup", "BIGINT", "活跃行数"},
            {"n_dead_tup", "BIGINT", "死行数"},
            {"last_vacuum", "TIMESTAMP", "上次VACUUM时间"},
            {"last_autovacuum", "TIMESTAMP", "上次自动VACUUM时间"},
            {"last_analyze", "TIMESTAMP", "上次ANALYZE时间"},
            {"last_autoanalyze", "TIMESTAMP", "上次自动ANALYZE时间"}
        },
        "用户表统计信息"
    };

    // pg_stat_user_indexes - 用户索引统计
    views_["pg_stat_user_indexes"] = {
        "pg_stat_user_indexes",
        "pg_catalog",
        {
            {"relid", "INT", "表ID"},
            {"indexrelid", "INT", "索引ID"},
            {"schemaname", "TEXT", "模式名"},
            {"relname", "TEXT", "表名"},
            {"indexrelname", "TEXT", "索引名"},
            {"idx_scan", "BIGINT", "索引扫描次数"},
            {"idx_tup_read", "BIGINT", "索引读取行数"},
            {"idx_tup_fetch", "BIGINT", "索引获取行数"}
        },
        "用户索引统计信息"
    };

    // pg_locks - 锁信息
    views_["pg_locks"] = {
        "pg_locks",
        "pg_catalog",
        {
            {"locktype", "TEXT", "锁类型"},
            {"database", "TEXT", "数据库名"},
            {"relation", "TEXT", "关系名"},
            {"page", "INT", "页号"},
            {"tuple", "INT", "元组号"},
            {"virtualxid", "TEXT", "虚拟事务ID"},
            {"transactionid", "BIGINT", "事务ID"},
            {"classid", "INT", "类ID"},
            {"objid", "INT", "对象ID"},
            {"objsubid", "INT", "对象子ID"},
            {"virtualtransaction", "TEXT", "虚拟事务"},
            {"pid", "INT", "进程ID"},
            {"mode", "TEXT", "锁模式"},
            {"granted", "BOOLEAN", "是否已授予"},
            {"fastpath", "BOOLEAN", "是否快速路径"}
        },
        "当前锁信息"
    };

    // pg_stat_replication - 复制统计
    views_["pg_stat_replication"] = {
        "pg_stat_replication",
        "pg_catalog",
        {
            {"pid", "INT", "进程ID"},
            {"usesysid", "INT", "用户系统ID"},
            {"usename", "TEXT", "用户名"},
            {"application_name", "TEXT", "应用名"},
            {"client_addr", "TEXT", "客户端地址"},
            {"client_hostname", "TEXT", "客户端主机名"},
            {"client_port", "INT", "客户端端口"},
            {"backend_start", "TIMESTAMP", "后端启动时间"},
            {"backend_xmin", "BIGINT", "后端XID最小值"},
            {"state", "TEXT", "状态"},
            {"sent_lsn", "TEXT", "已发送LSN"},
            {"write_lsn", "TEXT", "已写入LSN"},
            {"flush_lsn", "TEXT", "已刷盘LSN"},
            {"replay_lsn", "TEXT", "已重放LSN"},
            {"write_lag", "INTERVAL", "写入延迟"},
            {"flush_lag", "INTERVAL", "刷盘延迟"},
            {"replay_lag", "INTERVAL", "重放延迟"},
            {"sync_priority", "INT", "同步优先级"},
            {"sync_state", "TEXT", "同步状态"}
        },
        "流复制统计信息"
    };

    // pg_tables - 表列表
    views_["pg_tables"] = {
        "pg_tables",
        "pg_catalog",
        {
            {"schemaname", "TEXT", "模式名"},
            {"tablename", "TEXT", "表名"},
            {"tableowner", "TEXT", "表所有者"},
            {"tablespace", "TEXT", "表空间"},
            {"hasindexes", "BOOLEAN", "是否有索引"},
            {"hasrules", "BOOLEAN", "是否有规则"},
            {"hastriggers", "BOOLEAN", "是否有触发器"}
        },
        "表列表"
    };

    // pg_indexes - 索引列表
    views_["pg_indexes"] = {
        "pg_indexes",
        "pg_catalog",
        {
            {"schemaname", "TEXT", "模式名"},
            {"tablename", "TEXT", "表名"},
            {"indexname", "TEXT", "索引名"},
            {"tablespace", "TEXT", "表空间"},
            {"indexdef", "TEXT", "索引定义"}
        },
        "索引列表"
    };

    // pg_user - 用户列表
    views_["pg_user"] = {
        "pg_user",
        "pg_catalog",
        {
            {"usename", "TEXT", "用户名"},
            {"usesysid", "INT", "用户系统ID"},
            {"usecreatedb", "BOOLEAN", "是否可以创建数据库"},
            {"usesuper", "BOOLEAN", "是否是超级用户"},
            {"userepl", "BOOLEAN", "是否可以复制"},
            {"usebypassrls", "BOOLEAN", "是否可以绕过行级安全"},
            {"passwd", "TEXT", "密码"},
            {"valuntil", "TIMESTAMP", "密码过期时间"},
            {"useconfig", "TEXT[]", "用户配置"}
        },
        "用户列表"
    };

    // pg_database - 数据库列表
    views_["pg_database"] = {
        "pg_database",
        "pg_catalog",
        {
            {"datname", "TEXT", "数据库名"},
            {"datdba", "INT", "数据库所有者"},
            {"encoding", "TEXT", "编码"},
            {"datcollate", "TEXT", "排序规则"},
            {"datctype", "TEXT", "字符分类"},
            {"datistemplate", "BOOLEAN", "是否是模板"},
            {"datallowconn", "BOOLEAN", "是否允许连接"},
            {"datconnlimit", "INT", "连接限制"},
            {"datlastsysoid", "INT", "最后系统OID"},
            {"datfrozenxid", "BIGINT", "冻结XID"},
            {"dattablespace", "TEXT", "表空间"},
            {"datacl", "TEXT", "访问权限"}
        },
        "数据库列表"
    };

    // pg_backup_list - 备份列表
    views_["pg_backup_list"] = {
        "pg_backup_list",
        "pg_catalog",
        {
            {"backup_id", "TEXT", "备份ID"},
            {"backup_name", "TEXT", "备份名称"},
            {"backup_type", "TEXT", "备份类型"},
            {"start_time", "TIMESTAMP", "开始时间"},
            {"end_time", "TIMESTAMP", "结束时间"},
            {"wal_start_lsn", "TEXT", "开始LSN"},
            {"wal_end_lsn", "TEXT", "结束LSN"},
            {"data_size", "BIGINT", "数据大小"},
            {"wal_size", "BIGINT", "WAL大小"},
            {"db_version", "TEXT", "数据库版本"},
            {"is_valid", "BOOLEAN", "是否有效"}
        },
        "备份列表"
    };

    // pg_restore_points - 恢复点列表
    views_["pg_restore_points"] = {
        "pg_restore_points",
        "pg_catalog",
        {
            {"point_name", "TEXT", "恢复点名称"},
            {"timestamp", "TIMESTAMP", "时间戳"},
            {"wal_lsn", "TEXT", "WAL LSN"}
        },
        "恢复点列表"
    };
}

bool SystemViewManager::isSystemViewQuery(const std::string& table_name) const {
    return views_.find(table_name) != views_.end();
}

SystemViewResult SystemViewManager::querySystemView(const std::string& view_name,
                                                    const std::vector<std::string>& columns) {
    auto it = views_.find(view_name);
    if (it == views_.end()) {
        SystemViewResult result;
        result.success = false;
        result.error_message = "Unknown system view: " + view_name;
        return result;
    }

    // 根据视图名称路由到对应的查询方法
    if (view_name == "pg_stat_activity") {
        return queryPgStatActivity();
    } else if (view_name == "pg_stat_database") {
        return queryPgStatDatabase();
    } else if (view_name == "pg_stat_user_tables") {
        return queryPgStatUserTables();
    } else if (view_name == "pg_stat_user_indexes") {
        return queryPgStatUserIndexes();
    } else if (view_name == "pg_locks") {
        return queryPgLocks();
    } else if (view_name == "pg_stat_replication") {
        return queryPgStatReplication();
    } else if (view_name == "pg_tables") {
        return queryPgTables();
    } else if (view_name == "pg_indexes") {
        return queryPgIndexes();
    } else if (view_name == "pg_user") {
        return queryPgUser();
    } else if (view_name == "pg_database") {
        return queryPgDatabase();
    } else if (view_name == "pg_backup_list") {
        return queryBackupList();
    } else if (view_name == "pg_restore_points") {
        return queryRestorePoints();
    }

    SystemViewResult result;
    result.success = false;
    result.error_message = "View not yet implemented: " + view_name;
    return result;
}

SystemViewResult SystemViewManager::queryPgStatActivity() {
    SystemViewResult result;
    result.column_names = {"pid", "usename", "application_name", "client_addr",
                          "client_port", "database_name", "query", "state",
                          "query_start", "backend_start"};

    if (diag_mgr_) {
        auto activities = diag_mgr_->getCurrentActivity();
        for (const auto& activity : activities) {
            std::vector<std::string> row;
            row.push_back(std::to_string(activity.process_id));
            row.push_back(activity.username);
            row.push_back(activity.application_name);
            row.push_back(activity.client_addr);
            row.push_back(std::to_string(activity.client_port));
            row.push_back(activity.database_name);
            row.push_back(activity.query);
            row.push_back(activity.state);
            row.push_back(std::to_string(activity.query_start_time));
            row.push_back(std::to_string(activity.backend_start_time));
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgStatDatabase() {
    SystemViewResult result;
    result.column_names = {"datname", "numbackends", "xact_commit", "xact_rollback",
                          "blks_read", "blks_hit", "tup_returned", "tup_fetched",
                          "tup_inserted", "tup_updated", "tup_deleted", "conflicts",
                          "temp_files", "temp_bytes", "deadlocks", "blk_read_time",
                          "blk_write_time"};

    if (diag_mgr_) {
        auto stats = diag_mgr_->getSystemStats();
        std::vector<std::string> row;
        row.push_back("tinydb");  // datname
        row.push_back(std::to_string(stats.connection_count));  // numbackends
        row.push_back(std::to_string(stats.commit_count));  // xact_commit
        row.push_back(std::to_string(stats.rollback_count));  // xact_rollback
        row.push_back(std::to_string(stats.block_read_count));  // blks_read
        row.push_back(std::to_string(stats.block_write_count));  // blks_hit
        row.push_back("0");  // tup_returned
        row.push_back("0");  // tup_fetched
        row.push_back("0");  // tup_inserted
        row.push_back("0");  // tup_updated
        row.push_back("0");  // tup_deleted
        row.push_back("0");  // conflicts
        row.push_back("0");  // temp_files
        row.push_back("0");  // temp_bytes
        row.push_back("0");  // deadlocks
        row.push_back("0");  // blk_read_time
        row.push_back("0");  // blk_write_time
        result.rows.push_back(row);
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgStatUserTables() {
    SystemViewResult result;
    result.column_names = {"relid", "schemaname", "relname", "seq_scan", "seq_tup_read",
                          "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd",
                          "n_tup_del", "n_live_tup", "n_dead_tup", "last_vacuum",
                          "last_autovacuum", "last_analyze", "last_autoanalyze"};

    if (diag_mgr_) {
        auto table_stats = diag_mgr_->getAllTableStats();
        int relid = 1;
        for (const auto& stats : table_stats) {
            std::vector<std::string> row;
            row.push_back(std::to_string(relid++));
            row.push_back("public");  // schemaname
            row.push_back(stats.table_name);
            row.push_back(std::to_string(stats.seq_scan_count));
            row.push_back("0");  // seq_tup_read
            row.push_back(std::to_string(stats.index_scan_count));
            row.push_back("0");  // idx_tup_fetch
            row.push_back(std::to_string(stats.row_insert_count));
            row.push_back(std::to_string(stats.row_update_count));
            row.push_back(std::to_string(stats.row_delete_count));
            row.push_back(std::to_string(stats.row_live_count));
            row.push_back(std::to_string(stats.row_dead_count));
            row.push_back(std::to_string(stats.last_vacuum_time));
            row.push_back("0");  // last_autovacuum
            row.push_back(std::to_string(stats.last_analyze_time));
            row.push_back("0");  // last_autoanalyze
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgStatUserIndexes() {
    SystemViewResult result;
    result.column_names = {"relid", "indexrelid", "schemaname", "relname", "indexrelname",
                          "idx_scan", "idx_tup_read", "idx_tup_fetch"};

    if (diag_mgr_) {
        auto index_stats = diag_mgr_->getAllIndexStats();
        int relid = 1;
        for (const auto& stats : index_stats) {
            std::vector<std::string> row;
            row.push_back("1");  // relid
            row.push_back(std::to_string(relid++));  // indexrelid
            row.push_back("public");  // schemaname
            row.push_back(stats.table_name);
            row.push_back(stats.index_name);
            row.push_back(std::to_string(stats.index_scan_count));
            row.push_back(std::to_string(stats.index_read_count));
            row.push_back(std::to_string(stats.index_fetch_count));
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgLocks() {
    SystemViewResult result;
    result.column_names = {"locktype", "database", "relation", "page", "tuple",
                          "virtualxid", "transactionid", "classid", "objid",
                          "objsubid", "virtualtransaction", "pid", "mode",
                          "granted", "fastpath"};

    if (diag_mgr_) {
        auto locks = diag_mgr_->getCurrentLocks();
        for (const auto& lock : locks) {
            std::vector<std::string> row;
            row.push_back(lock.lock_type);
            row.push_back(lock.database_name);
            row.push_back(lock.relation_name);
            row.push_back("");  // page
            row.push_back("");  // tuple
            row.push_back("");  // virtualxid
            row.push_back("");  // transactionid
            row.push_back("");  // classid
            row.push_back("");  // objid
            row.push_back("");  // objsubid
            row.push_back("");  // virtualtransaction
            row.push_back(std::to_string(lock.process_id));
            row.push_back(lock.mode);
            row.push_back(lock.granted ? "t" : "f");
            row.push_back("t");  // fastpath
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgStatReplication() {
    SystemViewResult result;
    result.column_names = {"pid", "usesysid", "usename", "application_name", "client_addr",
                          "client_hostname", "client_port", "backend_start", "backend_xmin",
                          "state", "sent_lsn", "write_lsn", "flush_lsn", "replay_lsn",
                          "write_lag", "flush_lag", "replay_lag", "sync_priority", "sync_state"};

    if (diag_mgr_) {
        auto repl_stats = diag_mgr_->getReplicationStats();
        int pid = 1000;
        for (const auto& stats : repl_stats) {
            std::vector<std::string> row;
            row.push_back(std::to_string(pid++));
            row.push_back(std::to_string(pid));  // usesysid
            row.push_back("replica");  // usename
            row.push_back("walreceiver");  // application_name
            row.push_back(stats.client_addr);
            row.push_back("");  // client_hostname
            row.push_back("5433");  // client_port
            row.push_back(std::to_string(getCurrentTimestamp()));
            row.push_back("");  // backend_xmin
            row.push_back(stats.state);
            row.push_back(std::to_string(stats.sent_lsn));
            row.push_back(std::to_string(stats.write_lsn));
            row.push_back(std::to_string(stats.flush_lsn));
            row.push_back(std::to_string(stats.replay_lsn));
            row.push_back(std::to_string(stats.write_lag));
            row.push_back(std::to_string(stats.flush_lag));
            row.push_back(std::to_string(stats.replay_lag));
            row.push_back(std::to_string(stats.sync_priority));
            row.push_back(stats.sync_state);
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgTables() {
    SystemViewResult result;
    result.column_names = {"schemaname", "tablename", "tableowner", "tablespace",
                          "hasindexes", "hasrules", "hastriggers"};

    if (diag_mgr_) {
        auto table_stats = diag_mgr_->getAllTableStats();
        for (const auto& stats : table_stats) {
            std::vector<std::string> row;
            row.push_back("public");
            row.push_back(stats.table_name);
            row.push_back("tinydb");  // tableowner
            row.push_back("pg_default");  // tablespace
            row.push_back("t");  // hasindexes
            row.push_back("f");  // hasrules
            row.push_back("f");  // hastriggers
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgIndexes() {
    SystemViewResult result;
    result.column_names = {"schemaname", "tablename", "indexname", "tablespace", "indexdef"};

    if (diag_mgr_) {
        auto index_stats = diag_mgr_->getAllIndexStats();
        for (const auto& stats : index_stats) {
            std::vector<std::string> row;
            row.push_back("public");
            row.push_back(stats.table_name);
            row.push_back(stats.index_name);
            row.push_back("pg_default");
            row.push_back("CREATE INDEX " + stats.index_name + " ON " + stats.table_name);
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryPgUser() {
    SystemViewResult result;
    result.column_names = {"usename", "usesysid", "usecreatedb", "usesuper", "userepl",
                          "usebypassrls", "passwd", "valuntil", "useconfig"};

    // 默认用户
    std::vector<std::string> row1;
    row1.push_back("root");
    row1.push_back("1");
    row1.push_back("t");  // usecreatedb
    row1.push_back("t");  // usesuper
    row1.push_back("t");  // userepl
    row1.push_back("t");  // usebypassrls
    row1.push_back("********");
    row1.push_back("");
    row1.push_back("");
    result.rows.push_back(row1);

    std::vector<std::string> row2;
    row2.push_back("tinydb");
    row2.push_back("2");
    row2.push_back("f");
    row2.push_back("f");
    row2.push_back("f");
    row2.push_back("f");
    row2.push_back("********");
    row2.push_back("");
    row2.push_back("");
    result.rows.push_back(row2);

    return result;
}

SystemViewResult SystemViewManager::queryPgDatabase() {
    SystemViewResult result;
    result.column_names = {"datname", "datdba", "encoding", "datcollate", "datctype",
                          "datistemplate", "datallowconn", "datconnlimit", "datlastsysoid",
                          "datfrozenxid", "dattablespace", "datacl"};

    std::vector<std::string> row;
    row.push_back("tinydb");  // datname
    row.push_back("1");  // datdba
    row.push_back("UTF8");  // encoding
    row.push_back("en_US.UTF-8");  // datcollate
    row.push_back("en_US.UTF-8");  // datctype
    row.push_back("f");  // datistemplate
    row.push_back("t");  // datallowconn
    row.push_back("-1");  // datconnlimit
    row.push_back("10000");  // datlastsysoid
    row.push_back("3");  // datfrozenxid
    row.push_back("pg_default");  // dattablespace
    row.push_back("");  // datacl
    result.rows.push_back(row);

    return result;
}

SystemViewResult SystemViewManager::queryBackupList() {
    SystemViewResult result;
    result.column_names = {"backup_id", "backup_name", "backup_type", "start_time",
                          "end_time", "wal_start_lsn", "wal_end_lsn", "data_size",
                          "wal_size", "db_version", "is_valid"};

    if (backup_mgr_) {
        auto backups = backup_mgr_->listBackups();
        for (const auto& backup : backups) {
            std::vector<std::string> row;
            row.push_back(backup.backup_id);
            row.push_back(backup.backup_name);
            row.push_back(backup.type == storage::BackupType::FULL ? "FULL" : "INCREMENTAL");
            row.push_back(std::to_string(backup.start_time.time_since_epoch().count()));
            row.push_back(std::to_string(backup.end_time.time_since_epoch().count()));
            row.push_back(std::to_string(backup.wal_start_lsn));
            row.push_back(std::to_string(backup.wal_end_lsn));
            row.push_back(std::to_string(backup.data_size));
            row.push_back(std::to_string(backup.wal_size));
            row.push_back(backup.db_version);
            row.push_back(backup.is_valid ? "t" : "f");
            result.rows.push_back(row);
        }
    }

    return result;
}

SystemViewResult SystemViewManager::queryRestorePoints() {
    SystemViewResult result;
    result.column_names = {"point_name", "timestamp", "wal_lsn"};

    if (backup_mgr_) {
        auto points = backup_mgr_->listRestorePoints();
        for (const auto& point : points) {
            std::vector<std::string> row;
            row.push_back(point.name);
            row.push_back(std::to_string(point.timestamp.time_since_epoch().count()));
            row.push_back(std::to_string(point.wal_lsn));
            result.rows.push_back(row);
        }
    }

    return result;
}

std::vector<SystemView> SystemViewManager::listSystemViews() const {
    std::vector<SystemView> result;
    for (const auto& pair : views_) {
        result.push_back(pair.second);
    }
    return result;
}

bool SystemViewManager::getViewDefinition(const std::string& view_name, SystemView& view) const {
    auto it = views_.find(view_name);
    if (it != views_.end()) {
        view = it->second;
        return true;
    }
    return false;
}

} // namespace engine
} // namespace tinydb
