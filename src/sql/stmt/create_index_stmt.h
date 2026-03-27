#pragma once

#include <string>
#include "statement.h"

namespace tinydb {
namespace sql {

class CreateIndexStmt : public Statement {
public:
    void setIndexName(const std::string& name) { index_name_ = name; }
    void setTableName(const std::string& name) { table_name_ = name; }
    void setColumnName(const std::string& name) { column_name_ = name; }
    void setUnique(bool unique) { is_unique_ = unique; }

    const std::string& indexName() const { return index_name_; }
    const std::string& tableName() const { return table_name_; }
    const std::string& columnName() const { return column_name_; }
    bool isUnique() const { return is_unique_; }

    std::string toString() const override {
        std::string result = "CREATE ";
        if (is_unique_) result += "UNIQUE ";
        result += "INDEX " + index_name_;
        result += " ON " + table_name_;
        result += " (" + column_name_ + ")";
        return result;
    }

private:
    std::string index_name_;
    std::string table_name_;
    std::string column_name_;
    bool is_unique_ = false;
};

}
}
