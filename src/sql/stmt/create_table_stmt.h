#pragma once

#include <string>
#include <vector>
#include <memory>
#include "statement.h"
#include "expression.h"

namespace tinydb {
namespace sql {

class CreateTableStmt : public Statement {
public:
    StatementType type() const override { return StatementType::CREATE_TABLE; }
    struct ColumnDef {
        std::string name;
        std::string type;
        bool is_primary_key = false;
        bool is_not_null = false;
        bool is_unique = false;
        std::unique_ptr<Expression> default_value;
        std::string check_expr;
    };

    void setTable(const std::string& table) { table_ = table; }
    void setIfNotExists(bool if_not_exists) { if_not_exists_ = if_not_exists; }
    void addColumn(const std::string& name, const std::string& type) {
        columns_.push_back({name, type, false, false, false, nullptr, ""});
    }
    void addColumn(ColumnDef col) {
        columns_.push_back(std::move(col));
    }

    const std::string& table() const { return table_; }
    bool ifNotExists() const { return if_not_exists_; }
    const std::vector<ColumnDef>& columns() const { return columns_; }

    std::string toString() const override {
        std::string result = "CREATE TABLE ";
        if (if_not_exists_) result += "IF NOT EXISTS ";
        result += table_ + " (";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) result += ", ";
            result += columns_[i].name + " " + columns_[i].type;
            if (columns_[i].is_primary_key) result += " PRIMARY KEY";
            if (columns_[i].is_not_null) result += " NOT NULL";
            if (columns_[i].is_unique) result += " UNIQUE";
            if (columns_[i].default_value) {
                result += " DEFAULT " + columns_[i].default_value->toString();
            }
            if (!columns_[i].check_expr.empty()) {
                result += " CHECK (" + columns_[i].check_expr + ")";
            }
        }
        result += ")";
        return result;
    }

private:
    std::string table_;
    bool if_not_exists_ = false;
    std::vector<ColumnDef> columns_;
};

}
}
