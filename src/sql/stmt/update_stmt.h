#pragma once

#include <string>
#include <vector>
#include <memory>
#include "statement.h"
#include "expression.h"

namespace tinydb {
namespace sql {

class UpdateStmt : public Statement {
public:
    StatementType type() const override { return StatementType::UPDATE; }
    void setTable(const std::string& table) { table_ = table; }
    void addAssignment(const std::string& column, std::unique_ptr<Expression> value) {
        assignments_.push_back({column, std::move(value)});
    }
    void setWhereCondition(std::unique_ptr<Expression> condition) { where_condition_ = std::move(condition); }

    const std::string& table() const { return table_; }
    const std::vector<std::pair<std::string, std::unique_ptr<Expression>>>& assignments() const { return assignments_; }
    Expression* whereCondition() const { return where_condition_.get(); }

    std::string toString() const override {
        std::string result = "UPDATE " + table_ + " SET ";
        for (size_t i = 0; i < assignments_.size(); ++i) {
            if (i > 0) result += ", ";
            result += assignments_[i].first + " = " + assignments_[i].second->toString();
        }
        if (where_condition_) {
            result += " WHERE " + where_condition_->toString();
        }
        return result;
    }

private:
    std::string table_;
    std::vector<std::pair<std::string, std::unique_ptr<Expression>>> assignments_;
    std::unique_ptr<Expression> where_condition_;
};

}
}
