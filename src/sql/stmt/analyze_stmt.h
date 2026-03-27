#pragma once

#include <string>
#include "statement.h"

namespace tinydb {
namespace sql {

class AnalyzeStmt : public Statement {
public:
    StatementType type() const override { return StatementType::ANALYZE; }
    explicit AnalyzeStmt(std::string table_name)
        : table_name_(std::move(table_name)) {}

    const std::string& tableName() const { return table_name_; }

    std::string toString() const override {
        return "ANALYZE " + table_name_;
    }

private:
    std::string table_name_;
};

}
}
