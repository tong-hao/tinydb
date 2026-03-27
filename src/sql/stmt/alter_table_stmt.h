#pragma once

#include <string>
#include "statement.h"

namespace tinydb {
namespace sql {

class AlterTableStmt : public Statement {
public:
    StatementType type() const override { return StatementType::ALTER_TABLE; }
    enum class ActionType {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        RENAME_TABLE,
        RENAME_COLUMN,
        ALTER_COLUMN_TYPE
    };

    void setTable(const std::string& table) { table_ = table; }
    void setAction(ActionType action) { action_ = action; }
    void setNewTableName(const std::string& name) { new_table_name_ = name; }
    void setColumnName(const std::string& name) { column_name_ = name; }
    void setNewColumnName(const std::string& name) { new_column_name_ = name; }
    void setColumnType(const std::string& type) { column_type_ = type; }
    void setColumnDef(const std::string& name, const std::string& type) {
        column_name_ = name;
        column_type_ = type;
    }
    void setColumnRename(const std::string& old_name, const std::string& new_name) {
        column_name_ = old_name;
        new_column_name_ = new_name;
    }

    const std::string& table() const { return table_; }
    ActionType action() const { return action_; }
    const std::string& newTableName() const { return new_table_name_; }
    const std::string& columnName() const { return column_name_; }
    const std::string& newColumnName() const { return new_column_name_; }
    const std::string& columnType() const { return column_type_; }

    std::string toString() const override {
        std::string result = "ALTER TABLE " + table_ + " ";
        switch (action_) {
            case ActionType::ADD_COLUMN:
                result += "ADD COLUMN " + column_name_ + " " + column_type_;
                break;
            case ActionType::DROP_COLUMN:
                result += "DROP COLUMN " + column_name_;
                break;
            case ActionType::MODIFY_COLUMN:
                result += "MODIFY COLUMN " + column_name_ + " " + column_type_;
                break;
            case ActionType::RENAME_TABLE:
                result += "RENAME TO " + new_table_name_;
                break;
            case ActionType::RENAME_COLUMN:
                result += "RENAME COLUMN " + column_name_ + " TO " + new_column_name_;
                break;
            case ActionType::ALTER_COLUMN_TYPE:
                result += "ALTER COLUMN " + column_name_ + " TYPE " + column_type_;
                break;
        }
        return result;
    }

private:
    std::string table_;
    ActionType action_;
    std::string new_table_name_;
    std::string column_name_;
    std::string new_column_name_;
    std::string column_type_;
};

}
}
