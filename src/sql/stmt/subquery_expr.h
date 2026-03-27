#pragma once

#include "select_stmt.h"
#include "expression.h"

namespace tinydb {
namespace sql {

// 子查询表达式 (标量子查询)
class SubqueryExpr : public Expression {
public:
    explicit SubqueryExpr(std::unique_ptr<SelectStmt> select)
        : select_(std::move(select)) {}

    ~SubqueryExpr() override = default;

    const SelectStmt* select() const { return select_.get(); }

    std::string toString() const override {
        return "(" + select_->toString() + ")";
    }

private:
    std::unique_ptr<SelectStmt> select_;
};

// EXISTS 子查询表达式
class ExistsExpr : public Expression {
public:
    explicit ExistsExpr(std::unique_ptr<SelectStmt> select)
        : select_(std::move(select)) {}

    ~ExistsExpr() override = default;

    const SelectStmt* select() const { return select_.get(); }

    std::string toString() const override {
        return "EXISTS (" + select_->toString() + ")";
    }

private:
    std::unique_ptr<SelectStmt> select_;
};

// InExpr with subquery support
class InSubqueryExpr : public Expression {
public:
    InSubqueryExpr(std::unique_ptr<Expression> left, std::unique_ptr<SelectStmt> subquery, bool is_not = false)
        : left_(std::move(left)), is_not_(is_not), subquery_(std::move(subquery)) {}

    ~InSubqueryExpr() override = default;

    const Expression* left() const { return left_.get(); }
    const SelectStmt* subquery() const { return subquery_.get(); }
    bool isNot() const { return is_not_; }

    std::string toString() const override {
        std::string result = left_->toString();
        if (is_not_) {
            result += " NOT IN (";
        } else {
            result += " IN (";
        }
        result += subquery_->toString() + ")";
        return result;
    }

private:
    std::unique_ptr<Expression> left_;
    bool is_not_;
    std::unique_ptr<SelectStmt> subquery_;
};

}
}
