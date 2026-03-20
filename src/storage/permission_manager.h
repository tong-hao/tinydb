#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>

namespace tinydb {
namespace storage {

// 权限类型
enum class Permission {
    SELECT,      // 查询权限
    INSERT,      // 插入权限
    UPDATE,      // 更新权限
    DELETE,      // 删除权限
    CREATE,      // 创建权限（表、索引等）
    DROP,        // 删除权限（表、索引等）
    ALL          // 所有权限
};

// 将权限转换为字符串
std::string permissionToString(Permission perm);
// 将字符串转换为权限
Permission stringToPermission(const std::string& str);

// 用户/角色信息
struct User {
    std::string username;
    std::string password_hash;  // 密码哈希（简化实现）
    bool is_superuser;          // 是否是超级用户
    std::vector<std::string> roles;  // 所属角色

    User() : is_superuser(false) {}
};

// 权限条目
struct PermissionEntry {
    std::string grantee;        // 被授予者（用户或角色）
    std::string schema_name;    // 模式名（空字符串表示所有）
    std::string table_name;     // 表名（空字符串表示所有）
    std::string column_name;    // 列名（空字符串表示所有列）
    Permission permission;      // 权限类型
    bool with_grant_option;     // 是否可以授予他人
    std::string granted_by;     // 授予者

    PermissionEntry() : permission(Permission::SELECT), with_grant_option(false) {}
};

// 角色信息
struct Role {
    std::string role_name;
    std::vector<std::string> member_users;  // 成员用户
    std::vector<std::string> member_roles;  // 成员角色（角色继承）
};

// 权限管理器
class PermissionManager {
public:
    PermissionManager();
    ~PermissionManager();

    // 禁止拷贝
    PermissionManager(const PermissionManager&) = delete;
    PermissionManager& operator=(const PermissionManager&) = delete;

    // ========== 用户管理 ==========
    // 创建用户
    bool createUser(const std::string& username, const std::string& password, bool is_superuser = false);
    // 删除用户
    bool dropUser(const std::string& username);
    // 验证用户密码
    bool authenticateUser(const std::string& username, const std::string& password) const;
    // 检查用户是否存在
    bool userExists(const std::string& username) const;
    // 获取用户信息
    std::shared_ptr<User> getUser(const std::string& username) const;
    // 修改用户密码
    bool alterUserPassword(const std::string& username, const std::string& new_password);

    // ========== 角色管理 ==========
    // 创建角色
    bool createRole(const std::string& role_name);
    // 删除角色
    bool dropRole(const std::string& role_name);
    // 授予角色给用户
    bool grantRoleToUser(const std::string& role_name, const std::string& username);
    // 从用户回收角色
    bool revokeRoleFromUser(const std::string& role_name, const std::string& username);
    // 检查角色是否存在
    bool roleExists(const std::string& role_name) const;
    // 获取角色信息
    std::shared_ptr<Role> getRole(const std::string& role_name) const;

    // ========== 权限管理 ==========
    // 授予权限
    bool grantPermission(const std::string& grantee,
                         const std::string& schema_name,
                         const std::string& table_name,
                         const std::string& column_name,
                         Permission permission,
                         bool with_grant_option = false,
                         const std::string& granted_by = "");

    // 回收权限
    bool revokePermission(const std::string& grantee,
                          const std::string& schema_name,
                          const std::string& table_name,
                          const std::string& column_name,
                          Permission permission);

    // 检查是否有权限
    bool hasPermission(const std::string& username,
                       const std::string& schema_name,
                       const std::string& table_name,
                       const std::string& column_name,
                       Permission permission) const;

    // 检查是否有表级权限
    bool hasTablePermission(const std::string& username,
                            const std::string& schema_name,
                            const std::string& table_name,
                            Permission permission) const;

    // 检查是否有列级权限
    bool hasColumnPermission(const std::string& username,
                             const std::string& schema_name,
                             const std::string& table_name,
                             const std::string& column_name,
                             Permission permission) const;

    // 检查是否是超级用户
    bool isSuperUser(const std::string& username) const;

    // ========== 权限查询 ==========
    // 获取用户的所有权限
    std::vector<PermissionEntry> getUserPermissions(const std::string& username) const;
    // 获取表的所有权限
    std::vector<PermissionEntry> getTablePermissions(const std::string& schema_name,
                                                      const std::string& table_name) const;
    // 获取角色的所有成员
    std::vector<std::string> getRoleMembers(const std::string& role_name) const;

private:
    mutable std::mutex mutex_;

    // 用户名 -> 用户信息
    std::unordered_map<std::string, std::shared_ptr<User>> users_;

    // 角色名 -> 角色信息
    std::unordered_map<std::string, std::shared_ptr<Role>> roles_;

    // 权限列表
    std::vector<PermissionEntry> permissions_;

    // 初始化默认用户（如postgres/root）
    void initializeDefaultUsers();

    // 获取用户的所有角色（递归）
    std::unordered_set<std::string> getUserAllRoles(const std::string& username) const;

    // 检查权限是否匹配
    bool permissionMatches(const PermissionEntry& entry,
                           const std::string& schema_name,
                           const std::string& table_name,
                           const std::string& column_name,
                           Permission permission) const;
};

// 全局权限管理器
extern PermissionManager* g_permission_manager;

} // namespace storage
} // namespace tinydb
