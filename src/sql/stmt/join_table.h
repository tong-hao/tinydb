#pragma once

#include <string>
#include <memory>
#include "expression.h"

namespace tinydb {
namespace sql {

enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL
};

class JoinTable {
public:
    JoinTable(std::string table, JoinType type)
        : table_name_(std::move(table)), join_type_(type) {}

    void setJoinCondition(std::unique_ptr<Expression> condition) {
        join_condition_ = std::move(condition);
    }

    const std::string& tableName() const { return table_name_; }
    JoinType joinType() const { return join_type_; }
    Expression* joinCondition() const { return join_condition_.get(); }

private:
    std::string table_name_;
    JoinType join_type_;
    std::unique_ptr<Expression> join_condition_;
};

}
}
