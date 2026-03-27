#pragma once

#include <string>
#include <memory>
#include "statement.h"
#include "expression.h"

namespace tinydb {
namespace sql {

class DeleteStmt : public Statement {
public:
    StatementType type() const override { return StatementType::DELETE; }
    void setTable(const std::string& table) { table_ = table; }
    void setWhereCondition(std::unique_ptr<Expression> condition) { where_condition_ = std::move(condition); }

    const std::string& table() const { return table_; }
    Expression* whereCondition() const { return where_condition_.get(); }

    std::string toString() const override {
        std::string result = "DELETE FROM " + table_;
        if (where_condition_) {
            result += " WHERE " + where_condition_->toString();
        }
        return result;
    }

private:
    std::string table_;
    std::unique_ptr<Expression> where_condition_;
};

}
}
