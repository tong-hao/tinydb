#pragma once

#include <string>
#include <vector>
#include <memory>
#include "statement.h"
#include "expression.h"
#include "join_table.h"

namespace tinydb {
namespace sql {

struct OrderByItem {
    std::unique_ptr<Expression> expr;
    bool is_asc;

    OrderByItem(std::unique_ptr<Expression> e, bool asc = true)
        : expr(std::move(e)), is_asc(asc) {}
};

class SelectStmt : public Statement {
public:
    void setDistinct(bool distinct) { distinct_ = distinct; }
    bool isDistinct() const { return distinct_; }

    void addSelectItem(std::unique_ptr<Expression> expr) {
        select_list_.push_back(std::move(expr));
    }

    void addSelectColumn(const std::string& col) {
        select_list_.push_back(std::make_unique<ColumnRefExpr>(col));
    }

    void setFromTable(const std::string& table) { from_table_ = table; }
    void setWhereCondition(std::unique_ptr<Expression> condition) { where_condition_ = std::move(condition); }

    void addJoinTable(std::unique_ptr<JoinTable> join) {
        join_tables_.push_back(std::move(join));
    }

    void addGroupByItem(std::unique_ptr<Expression> expr) {
        group_by_list_.push_back(std::move(expr));
    }

    void setHavingCondition(std::unique_ptr<Expression> condition) {
        having_condition_ = std::move(condition);
    }

    void addOrderBy(std::unique_ptr<Expression> expr, bool is_asc = true) {
        order_by_.push_back(OrderByItem(std::move(expr), is_asc));
    }
    void setLimit(int64_t limit) { limit_ = limit; has_limit_ = true; }
    void setOffset(int64_t offset) { offset_ = offset; has_offset_ = true; }

    const std::vector<std::unique_ptr<Expression>>& selectList() const { return select_list_; }
    const std::string& fromTable() const { return from_table_; }
    Expression* whereCondition() const { return where_condition_.get(); }

    const std::vector<std::unique_ptr<JoinTable>>& joinTables() const { return join_tables_; }

    const std::vector<std::unique_ptr<Expression>>& groupByList() const { return group_by_list_; }
    Expression* havingCondition() const { return having_condition_.get(); }
    bool hasGroupBy() const { return !group_by_list_.empty(); }

    const std::vector<OrderByItem>& orderBy() const { return order_by_; }
    bool hasLimit() const { return has_limit_; }
    int64_t limit() const { return limit_; }
    bool hasOffset() const { return has_offset_; }
    int64_t offset() const { return offset_; }

    std::vector<std::string> getAllTables() const {
        std::vector<std::string> tables;
        if (!from_table_.empty()) {
            tables.push_back(from_table_);
        }
        for (const auto& join : join_tables_) {
            tables.push_back(join->tableName());
        }
        return tables;
    }

    std::string toString() const override {
        std::string result = distinct_ ? "SELECT DISTINCT " : "SELECT ";
        for (size_t i = 0; i < select_list_.size(); ++i) {
            if (i > 0) result += ", ";
            result += select_list_[i]->toString();
        }
        if (!from_table_.empty()) {
            result += " FROM " + from_table_;
        }
        for (const auto& join : join_tables_) {
            switch (join->joinType()) {
                case JoinType::INNER:
                    result += " JOIN " + join->tableName();
                    break;
                case JoinType::LEFT:
                    result += " LEFT JOIN " + join->tableName();
                    break;
                default:
                    result += " JOIN " + join->tableName();
            }
            if (join->joinCondition()) {
                result += " ON " + join->joinCondition()->toString();
            }
        }
        if (where_condition_) {
            result += " WHERE " + where_condition_->toString();
        }
        if (!group_by_list_.empty()) {
            result += " GROUP BY ";
            for (size_t i = 0; i < group_by_list_.size(); ++i) {
                if (i > 0) result += ", ";
                result += group_by_list_[i]->toString();
            }
        }
        if (having_condition_) {
            result += " HAVING " + having_condition_->toString();
        }
        if (!order_by_.empty()) {
            result += " ORDER BY ";
            for (size_t i = 0; i < order_by_.size(); ++i) {
                if (i > 0) result += ", ";
                result += order_by_[i].expr->toString();
                result += order_by_[i].is_asc ? " ASC" : " DESC";
            }
        }
        if (has_limit_) {
            result += " LIMIT " + std::to_string(limit_);
        }
        if (has_offset_) {
            result += " OFFSET " + std::to_string(offset_);
        }
        return result;
    }

private:
    bool distinct_ = false;
    std::vector<std::unique_ptr<Expression>> select_list_;
    std::string from_table_;
    std::unique_ptr<Expression> where_condition_;
    std::vector<std::unique_ptr<JoinTable>> join_tables_;
    std::vector<std::unique_ptr<Expression>> group_by_list_;
    std::unique_ptr<Expression> having_condition_;
    std::vector<OrderByItem> order_by_;
    int64_t limit_ = 0;
    int64_t offset_ = 0;
    bool has_limit_ = false;
    bool has_offset_ = false;
};

}
}

