
#pragma once

#include <string>
#include <vector>
#include <memory>
#include "ast_node.h"

namespace tinydb {
namespace sql {

class Expression : public ASTNode {
public:
    virtual ~Expression() = default;
};

// 列引用表达式
class ColumnRefExpr : public Expression {
public:
    explicit ColumnRefExpr(std::string name) : column_name_(std::move(name)) {}

    const std::string& columnName() const { return column_name_; }

    std::string toString() const override {
        return "ColumnRef(" + column_name_ + ")";
    }

private:
    std::string column_name_;
};

// 别名表达式 (expr AS alias)
class AliasExpr : public Expression {
public:
    AliasExpr(std::unique_ptr<Expression> expr, std::string alias)
        : expr_(std::move(expr)), alias_(std::move(alias)) {}

    const Expression* expression() const { return expr_.get(); }
    const std::string& alias() const { return alias_; }

    std::string toString() const override {
        return expr_->toString() + " AS " + alias_;
    }

private:
    std::unique_ptr<Expression> expr_;
    std::string alias_;
};

// 常量表达式
class LiteralExpr : public Expression {
public:
    explicit LiteralExpr(std::string value) : value_(std::move(value)) {}

    const std::string& value() const { return value_; }

    std::string toString() const override {
        return "Literal(" + value_ + ")";
    }

private:
    std::string value_;
};

// 操作符类型
enum class OpType {
    // 比较操作符
    EQ, NE, LT, LE, GT, GE,
    // 算术操作符
    ADD, SUB, MUL, DIV, MOD,
    // 逻辑操作符
    AND, OR, NOT
};

// 二元操作表达式
class BinaryOpExpr : public Expression {
public:
    BinaryOpExpr(OpType op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    OpType op() const { return op_; }
    const Expression* left() const { return left_.get(); }
    const Expression* right() const { return right_.get(); }

    std::string toString() const override {
        std::string op_str;
        switch (op_) {
            case OpType::ADD: op_str = "+"; break;
            case OpType::SUB: op_str = "-"; break;
            case OpType::MUL: op_str = "*"; break;
            case OpType::DIV: op_str = "/"; break;
            case OpType::MOD: op_str = "%"; break;
            default: op_str = "?"; break;
        }
        return "(" + left_->toString() + " " + op_str + " " + right_->toString() + ")";
    }

private:
    OpType op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

// 比较表达式
class ComparisonExpr : public Expression {
public:
    ComparisonExpr(OpType op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    OpType op() const { return op_; }
    const Expression* left() const { return left_.get(); }
    const Expression* right() const { return right_.get(); }

    std::string toString() const override {
        std::string op_str;
        switch (op_) {
            case OpType::EQ: op_str = "="; break;
            case OpType::NE: op_str = "<>"; break;
            case OpType::LT: op_str = "<"; break;
            case OpType::LE: op_str = "<="; break;
            case OpType::GT: op_str = ">"; break;
            case OpType::GE: op_str = ">="; break;
            default: op_str = "?"; break;
        }
        return "(" + left_->toString() + " " + op_str + " " + right_->toString() + ")";
    }

private:
    OpType op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

// 逻辑表达式
class LogicalExpr : public Expression {
public:
    LogicalExpr(OpType op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right = nullptr)
        : op_(op), left_(std::move(left)), right_(std::move(right)) {}

    OpType op() const { return op_; }
    const Expression* left() const { return left_.get(); }
    const Expression* right() const { return right_.get(); }

    std::string toString() const override {
        std::string op_str;
        switch (op_) {
            case OpType::AND: op_str = "AND"; break;
            case OpType::OR: op_str = "OR"; break;
            case OpType::NOT: op_str = "NOT"; break;
            default: op_str = "?"; break;
        }
        if (op_ == OpType::NOT) {
            return op_str + " " + left_->toString();
        }
        return "(" + left_->toString() + " " + op_str + " " + right_->toString() + ")";
    }

private:
    OpType op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

// IN 表达式
class InExpr : public Expression {
public:
    InExpr(std::unique_ptr<Expression> left, std::vector<std::unique_ptr<Expression>> values, bool is_not = false)
        : left_(std::move(left)), is_not_(is_not) {
        values_ = std::move(values);
    }

    const Expression* left() const { return left_.get(); }
    const std::vector<std::unique_ptr<Expression>>& values() const { return values_; }
    bool isNot() const { return is_not_; }

    std::string toString() const override {
        std::string result = left_->toString();
        if (is_not_) {
            result += " NOT IN (";
        } else {
            result += " IN (";
        }
        for (size_t i = 0; i < values_.size(); ++i) {
            if (i > 0) result += ", ";
            result += values_[i]->toString();
        }
        result += ")";
        return result;
    }

private:
    std::unique_ptr<Expression> left_;
    std::vector<std::unique_ptr<Expression>> values_;
    bool is_not_;
};

// BETWEEN 表达式
class BetweenExpr : public Expression {
public:
    BetweenExpr(std::unique_ptr<Expression> left, std::unique_ptr<Expression> lower, std::unique_ptr<Expression> upper, bool is_not = false)
        : left_(std::move(left)), lower_(std::move(lower)), upper_(std::move(upper)), is_not_(is_not) {}

    const Expression* left() const { return left_.get(); }
    const Expression* lower() const { return lower_.get(); }
    const Expression* upper() const { return upper_.get(); }
    bool isNot() const { return is_not_; }

    std::string toString() const override {
        std::string result = left_->toString();
        if (is_not_) {
            result += " NOT BETWEEN ";
        } else {
            result += " BETWEEN ";
        }
        result += lower_->toString() + " AND " + upper_->toString();
        return result;
    }

private:
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> lower_;
    std::unique_ptr<Expression> upper_;
    bool is_not_;
};

// LIKE 表达式
class LikeExpr : public Expression {
public:
    LikeExpr(std::unique_ptr<Expression> left, std::unique_ptr<Expression> pattern, bool is_not = false)
        : left_(std::move(left)), pattern_(std::move(pattern)), is_not_(is_not) {}

    const Expression* left() const { return left_.get(); }
    const Expression* pattern() const { return pattern_.get(); }
    bool isNot() const { return is_not_; }

    std::string toString() const override {
        std::string result = left_->toString();
        if (is_not_) {
            result += " NOT LIKE ";
        } else {
            result += " LIKE ";
        }
        result += pattern_->toString();
        return result;
    }

private:
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> pattern_;
    bool is_not_;
};

// IS NULL 表达式
class IsNullExpr : public Expression {
public:
    IsNullExpr(std::unique_ptr<Expression> left, bool is_not = false)
        : left_(std::move(left)), is_not_(is_not) {}

    const Expression* left() const { return left_.get(); }
    bool isNot() const { return is_not_; }

    std::string toString() const override {
        std::string result = left_->toString();
        if (is_not_) {
            result += " IS NOT NULL";
        } else {
            result += " IS NULL";
        }
        return result;
    }

private:
    std::unique_ptr<Expression> left_;
    bool is_not_;
};

// 聚合函数类型
enum class AggregateFunc {
    COUNT,
    SUM,
    AVG,
    MAX,
    MIN
};

// 聚合函数表达式
class AggregateExpr : public Expression {
public:
    AggregateExpr(AggregateFunc func, std::unique_ptr<Expression> arg, bool is_star = false)
        : func_(func), arg_(std::move(arg)), is_star_(is_star) {}

    AggregateFunc func() const { return func_; }
    const Expression* arg() const { return arg_.get(); }
    bool isStar() const { return is_star_; }

    std::string toString() const override {
        std::string func_name;
        switch (func_) {
            case AggregateFunc::COUNT: func_name = "COUNT"; break;
            case AggregateFunc::SUM: func_name = "SUM"; break;
            case AggregateFunc::AVG: func_name = "AVG"; break;
            case AggregateFunc::MAX: func_name = "MAX"; break;
            case AggregateFunc::MIN: func_name = "MIN"; break;
        }
        if (is_star_) {
            return func_name + "(*)";
        } else if (arg_) {
            return func_name + "(" + arg_->toString() + ")";
        } else {
            return func_name + "()";
        }
    }

private:
    AggregateFunc func_;
    std::unique_ptr<Expression> arg_;
    bool is_star_;
};

}
}
