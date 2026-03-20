#include "permission_manager.h"
#include "common/logger.h"
#include <algorithm>

namespace tinydb {
namespace storage {

// 全局权限管理器实例
PermissionManager* g_permission_manager = nullptr;

// 将权限转换为字符串
std::string permissionToString(Permission perm) {
    switch (perm) {
        case Permission::SELECT: return "SELECT";
        case Permission::INSERT: return "INSERT";
        case Permission::UPDATE: return "UPDATE";
        case Permission::DELETE: return "DELETE";
        case Permission::CREATE: return "CREATE";
        case Permission::DROP: return "DROP";
        case Permission::ALL: return "ALL";
        default: return "UNKNOWN";
    }
}

// 将字符串转换为权限
Permission stringToPermission(const std::string& str) {
    if (str == "SELECT") return Permission::SELECT;
    if (str == "INSERT") return Permission::INSERT;
    if (str == "UPDATE") return Permission::UPDATE;
    if (str == "DELETE") return Permission::DELETE;
    if (str == "CREATE") return Permission::CREATE;
    if (str == "DROP") return Permission::DROP;
    if (str == "ALL") return Permission::ALL;
    return Permission::SELECT;  // 默认
}

// PermissionManager 实现

PermissionManager::PermissionManager() {
    initializeDefaultUsers();
    LOG_INFO("PermissionManager initialized");
}

PermissionManager::~PermissionManager() {
    std::lock_guard<std::mutex> lock(mutex_);
    users_.clear();
    roles_.clear();
    permissions_.clear();
}

void PermissionManager::initializeDefaultUsers() {
    // 创建默认超级用户
    auto root_user = std::make_shared<User>();
    root_user->username = "root";
    root_user->password_hash = "root";  // 简化：明文存储，实际应该哈希
    root_user->is_superuser = true;
    users_["root"] = root_user;

    // 创建默认普通用户
    auto default_user = std::make_shared<User>();
    default_user->username = "tinydb";
    default_user->password_hash = "tinydb";
    default_user->is_superuser = false;
    users_["tinydb"] = default_user;

    LOG_INFO("Default users created: root, tinydb");
}

// ========== 用户管理 ==========

bool PermissionManager::createUser(const std::string& username, const std::string& password, bool is_superuser) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (users_.find(username) != users_.end()) {
        LOG_ERROR("User already exists: " << username);
        return false;
    }

    auto user = std::make_shared<User>();
    user->username = username;
    user->password_hash = password;  // 简化：明文存储
    user->is_superuser = is_superuser;
    users_[username] = user;

    LOG_INFO("Created user: " << username << (is_superuser ? " (superuser)" : ""));
    return true;
}

bool PermissionManager::dropUser(const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = users_.find(username);
    if (it == users_.end()) {
        LOG_ERROR("User does not exist: " << username);
        return false;
    }

    // 不能删除最后一个超级用户
    if (it->second->is_superuser) {
        int superuser_count = 0;
        for (const auto& pair : users_) {
            if (pair.second->is_superuser) superuser_count++;
        }
        if (superuser_count <= 1) {
            LOG_ERROR("Cannot drop the last superuser");
            return false;
        }
    }

    users_.erase(it);

    // 清理该用户的权限
    permissions_.erase(
        std::remove_if(permissions_.begin(), permissions_.end(),
            [&username](const PermissionEntry& entry) {
                return entry.grantee == username;
            }),
        permissions_.end()
    );

    LOG_INFO("Dropped user: " << username);
    return true;
}

bool PermissionManager::authenticateUser(const std::string& username, const std::string& password) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = users_.find(username);
    if (it == users_.end()) {
        return false;
    }

    return it->second->password_hash == password;
}

bool PermissionManager::userExists(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return users_.find(username) != users_.end();
}

std::shared_ptr<User> PermissionManager::getUser(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(username);
    if (it != users_.end()) {
        return it->second;
    }
    return nullptr;
}

bool PermissionManager::alterUserPassword(const std::string& username, const std::string& new_password) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = users_.find(username);
    if (it == users_.end()) {
        LOG_ERROR("User does not exist: " << username);
        return false;
    }

    it->second->password_hash = new_password;
    LOG_INFO("Changed password for user: " << username);
    return true;
}

// ========== 角色管理 ==========

bool PermissionManager::createRole(const std::string& role_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (roles_.find(role_name) != roles_.end()) {
        LOG_ERROR("Role already exists: " << role_name);
        return false;
    }

    auto role = std::make_shared<Role>();
    role->role_name = role_name;
    roles_[role_name] = role;

    LOG_INFO("Created role: " << role_name);
    return true;
}

bool PermissionManager::dropRole(const std::string& role_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = roles_.find(role_name);
    if (it == roles_.end()) {
        LOG_ERROR("Role does not exist: " << role_name);
        return false;
    }

    // 从所有用户的角色列表中移除
    for (auto& user_pair : users_) {
        auto& roles = user_pair.second->roles;
        roles.erase(std::remove(roles.begin(), roles.end(), role_name), roles.end());
    }

    // 从所有角色的成员列表中移除
    for (auto& role_pair : roles_) {
        auto& member_roles = role_pair.second->member_roles;
        member_roles.erase(std::remove(member_roles.begin(), member_roles.end(), role_name), member_roles.end());
    }

    roles_.erase(it);

    // 清理该角色的权限
    permissions_.erase(
        std::remove_if(permissions_.begin(), permissions_.end(),
            [&role_name](const PermissionEntry& entry) {
                return entry.grantee == role_name;
            }),
        permissions_.end()
    );

    LOG_INFO("Dropped role: " << role_name);
    return true;
}

bool PermissionManager::grantRoleToUser(const std::string& role_name, const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto role_it = roles_.find(role_name);
    if (role_it == roles_.end()) {
        LOG_ERROR("Role does not exist: " << role_name);
        return false;
    }

    auto user_it = users_.find(username);
    if (user_it == users_.end()) {
        LOG_ERROR("User does not exist: " << username);
        return false;
    }

    // 检查是否已经是该角色
    auto& user_roles = user_it->second->roles;
    if (std::find(user_roles.begin(), user_roles.end(), role_name) != user_roles.end()) {
        LOG_WARN("User " << username << " already has role " << role_name);
        return true;  // 已经是该角色，视为成功
    }

    user_roles.push_back(role_name);
    role_it->second->member_users.push_back(username);

    LOG_INFO("Granted role " << role_name << " to user " << username);
    return true;
}

bool PermissionManager::revokeRoleFromUser(const std::string& role_name, const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto role_it = roles_.find(role_name);
    if (role_it == roles_.end()) {
        LOG_ERROR("Role does not exist: " << role_name);
        return false;
    }

    auto user_it = users_.find(username);
    if (user_it == users_.end()) {
        LOG_ERROR("User does not exist: " << username);
        return false;
    }

    // 从用户的角色列表中移除
    auto& user_roles = user_it->second->roles;
    user_roles.erase(std::remove(user_roles.begin(), user_roles.end(), role_name), user_roles.end());

    // 从角色的成员列表中移除
    auto& members = role_it->second->member_users;
    members.erase(std::remove(members.begin(), members.end(), username), members.end());

    LOG_INFO("Revoked role " << role_name << " from user " << username);
    return true;
}

bool PermissionManager::roleExists(const std::string& role_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return roles_.find(role_name) != roles_.end();
}

std::shared_ptr<Role> PermissionManager::getRole(const std::string& role_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = roles_.find(role_name);
    if (it != roles_.end()) {
        return it->second;
    }
    return nullptr;
}

// ========== 权限管理 ==========

bool PermissionManager::grantPermission(const std::string& grantee,
                                        const std::string& schema_name,
                                        const std::string& table_name,
                                        const std::string& column_name,
                                        Permission permission,
                                        bool with_grant_option,
                                        const std::string& granted_by) {
    std::lock_guard<std::mutex> lock(mutex_);

    // 检查被授予者是否存在
    if (users_.find(grantee) == users_.end() && roles_.find(grantee) == roles_.end()) {
        LOG_ERROR("Grantee does not exist: " << grantee);
        return false;
    }

    // 检查是否已有相同权限
    for (const auto& entry : permissions_) {
        if (entry.grantee == grantee &&
            entry.schema_name == schema_name &&
            entry.table_name == table_name &&
            entry.column_name == column_name &&
            entry.permission == permission) {
            LOG_WARN("Permission already granted");
            return true;
        }
    }

    PermissionEntry entry;
    entry.grantee = grantee;
    entry.schema_name = schema_name;
    entry.table_name = table_name;
    entry.column_name = column_name;
    entry.permission = permission;
    entry.with_grant_option = with_grant_option;
    entry.granted_by = granted_by;

    permissions_.push_back(entry);

    LOG_INFO("Granted " << permissionToString(permission) << " on "
              << schema_name << "." << table_name
              << (column_name.empty() ? "" : "." + column_name)
              << " to " << grantee);
    return true;
}

bool PermissionManager::revokePermission(const std::string& grantee,
                                         const std::string& schema_name,
                                         const std::string& table_name,
                                         const std::string& column_name,
                                         Permission permission) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = std::remove_if(permissions_.begin(), permissions_.end(),
        [&grantee, &schema_name, &table_name, &column_name, permission](const PermissionEntry& entry) {
            return entry.grantee == grantee &&
                   entry.schema_name == schema_name &&
                   entry.table_name == table_name &&
                   entry.column_name == column_name &&
                   entry.permission == permission;
        });

    if (it == permissions_.end()) {
        LOG_ERROR("Permission not found");
        return false;
    }

    permissions_.erase(it, permissions_.end());

    LOG_INFO("Revoked " << permissionToString(permission) << " on "
              << schema_name << "." << table_name
              << " from " << grantee);
    return true;
}

bool PermissionManager::hasPermission(const std::string& username,
                                      const std::string& schema_name,
                                      const std::string& table_name,
                                      const std::string& column_name,
                                      Permission permission) const {
    std::lock_guard<std::mutex> lock(mutex_);

    // 检查是否是超级用户
    auto user_it = users_.find(username);
    if (user_it != users_.end() && user_it->second->is_superuser) {
        return true;
    }

    // 获取用户的所有角色
    auto user_roles = getUserAllRoles(username);

    // 检查直接权限和角色权限
    for (const auto& entry : permissions_) {
        if (entry.grantee != username && user_roles.find(entry.grantee) == user_roles.end()) {
            continue;
        }

        if (permissionMatches(entry, schema_name, table_name, column_name, permission)) {
            return true;
        }
    }

    return false;
}

bool PermissionManager::hasTablePermission(const std::string& username,
                                           const std::string& schema_name,
                                           const std::string& table_name,
                                           Permission permission) const {
    return hasPermission(username, schema_name, table_name, "", permission);
}

bool PermissionManager::hasColumnPermission(const std::string& username,
                                            const std::string& schema_name,
                                            const std::string& table_name,
                                            const std::string& column_name,
                                            Permission permission) const {
    // 先检查列级权限
    if (hasPermission(username, schema_name, table_name, column_name, permission)) {
        return true;
    }
    // 再检查表级权限
    return hasTablePermission(username, schema_name, table_name, permission);
}

bool PermissionManager::isSuperUser(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(username);
    if (it != users_.end()) {
        return it->second->is_superuser;
    }
    return false;
}

// ========== 权限查询 ==========

std::vector<PermissionEntry> PermissionManager::getUserPermissions(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<PermissionEntry> result;
    auto user_roles = getUserAllRoles(username);

    for (const auto& entry : permissions_) {
        if (entry.grantee == username || user_roles.find(entry.grantee) != user_roles.end()) {
            result.push_back(entry);
        }
    }

    return result;
}

std::vector<PermissionEntry> PermissionManager::getTablePermissions(const std::string& schema_name,
                                                                    const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<PermissionEntry> result;
    for (const auto& entry : permissions_) {
        if (entry.schema_name == schema_name && entry.table_name == table_name) {
            result.push_back(entry);
        }
    }

    return result;
}

std::vector<std::string> PermissionManager::getRoleMembers(const std::string& role_name) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = roles_.find(role_name);
    if (it != roles_.end()) {
        return it->second->member_users;
    }

    return {};
}

// ========== 辅助方法 ==========

std::unordered_set<std::string> PermissionManager::getUserAllRoles(const std::string& username) const {
    std::unordered_set<std::string> result;

    auto user_it = users_.find(username);
    if (user_it == users_.end()) {
        return result;
    }

    // 递归获取角色
    std::vector<std::string> to_process = user_it->second->roles;
    while (!to_process.empty()) {
        std::string role_name = to_process.back();
        to_process.pop_back();

        if (result.insert(role_name).second) {  // 如果插入成功（新元素）
            auto role_it = roles_.find(role_name);
            if (role_it != roles_.end()) {
                // 添加角色继承的角色
                for (const auto& inherited_role : role_it->second->member_roles) {
                    to_process.push_back(inherited_role);
                }
            }
        }
    }

    return result;
}

bool PermissionManager::permissionMatches(const PermissionEntry& entry,
                                          const std::string& schema_name,
                                          const std::string& table_name,
                                          const std::string& column_name,
                                          Permission permission) const {
    // 检查权限类型
    if (entry.permission != permission && entry.permission != Permission::ALL) {
        return false;
    }

    // 检查schema匹配（空字符串表示通配）
    if (!entry.schema_name.empty() && entry.schema_name != schema_name) {
        return false;
    }

    // 检查表名匹配
    if (!entry.table_name.empty() && entry.table_name != table_name) {
        return false;
    }

    // 检查列名匹配
    if (!entry.column_name.empty() && entry.column_name != column_name) {
        return false;
    }

    return true;
}

} // namespace storage
} // namespace tinydb
