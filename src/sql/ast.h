#pragma once

#include <string>
#include <vector>
#include <memory>

namespace tinydb {
namespace sql {

// 前向声明
class ASTNode;
class Statement;
class SelectStmt;
class InsertStmt;
class UpdateStmt;
class DeleteStmt;
class CreateTableStmt;
class DropTableStmt;
class AlterTableStmt;
class CreateIndexStmt;  // Phase 4
class DropIndexStmt;    // Phase 4
class JoinTable;        // Phase 4
class BeginStmt;
class CommitStmt;
class RollbackStmt;
class CreateViewStmt;   // CREATE VIEW
class DropViewStmt;     // DROP VIEW

// AST 节点基类
class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual std::string toString() const = 0;
};

// 表达式基类
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
        return column_name_;
    }

private:
    std::string column_name_;
};

// 常量表达式
class LiteralExpr : public Expression {
public:
    explicit LiteralExpr(std::string value) : value_(std::move(value)) {}

    const std::string& value() const { return value_; }

    std::string toString() const override {
        return value_;
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
        return left_->toString() + " " + op_str + " " + right_->toString();
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
        return left_->toString() + " " + op_str + " " + right_->toString();
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
        return left_->toString() + " " + op_str + " " + right_->toString();
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

// JOIN类型
enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL
};

// JOIN表（用于多表JOIN）
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

// Statement 基类
class Statement : public ASTNode {
public:
    virtual ~Statement() = default;
};

// ORDER BY 项
struct OrderByItem {
    std::unique_ptr<Expression> expr;
    bool is_asc;  // true for ASC, false for DESC

    OrderByItem(std::unique_ptr<Expression> e, bool asc = true)
        : expr(std::move(e)), is_asc(asc) {}
};

// SELECT 语句
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

    // Phase 4: 支持多表JOIN
    void addJoinTable(std::unique_ptr<JoinTable> join) {
        join_tables_.push_back(std::move(join));
    }

    // GROUP BY and HAVING support
    void addGroupByItem(std::unique_ptr<Expression> expr) {
        group_by_list_.push_back(std::move(expr));
    }

    void setHavingCondition(std::unique_ptr<Expression> condition) {
        having_condition_ = std::move(condition);
    }

    // ORDER BY, LIMIT, OFFSET
    void addOrderBy(std::unique_ptr<Expression> expr, bool is_asc = true) {
        order_by_.push_back(OrderByItem(std::move(expr), is_asc));
    }
    void setLimit(int64_t limit) { limit_ = limit; has_limit_ = true; }
    void setOffset(int64_t offset) { offset_ = offset; has_offset_ = true; }

    const std::vector<std::unique_ptr<Expression>>& selectList() const { return select_list_; }
    const std::string& fromTable() const { return from_table_; }
    Expression* whereCondition() const { return where_condition_.get(); }

    // Phase 4: 获取JOIN表
    const std::vector<std::unique_ptr<JoinTable>>& joinTables() const { return join_tables_; }

    // GROUP BY and HAVING getters
    const std::vector<std::unique_ptr<Expression>>& groupByList() const { return group_by_list_; }
    Expression* havingCondition() const { return having_condition_.get(); }
    bool hasGroupBy() const { return !group_by_list_.empty(); }

    // ORDER BY, LIMIT, OFFSET getters
    const std::vector<OrderByItem>& orderBy() const { return order_by_; }
    bool hasLimit() const { return has_limit_; }
    int64_t limit() const { return limit_; }
    bool hasOffset() const { return has_offset_; }
    int64_t offset() const { return offset_; }

    // Phase 4: 获取所有涉及的表
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
        // Phase 4: 输出JOIN信息
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
        // GROUP BY
        if (!group_by_list_.empty()) {
            result += " GROUP BY ";
            for (size_t i = 0; i < group_by_list_.size(); ++i) {
                if (i > 0) result += ", ";
                result += group_by_list_[i]->toString();
            }
        }
        // HAVING
        if (having_condition_) {
            result += " HAVING " + having_condition_->toString();
        }
        // ORDER BY
        if (!order_by_.empty()) {
            result += " ORDER BY ";
            for (size_t i = 0; i < order_by_.size(); ++i) {
                if (i > 0) result += ", ";
                result += order_by_[i].expr->toString();
                result += order_by_[i].is_asc ? " ASC" : " DESC";
            }
        }
        // LIMIT and OFFSET
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
    std::vector<std::unique_ptr<JoinTable>> join_tables_;  // Phase 4
    std::vector<std::unique_ptr<Expression>> group_by_list_;  // GROUP BY
    std::unique_ptr<Expression> having_condition_;  // HAVING
    std::vector<OrderByItem> order_by_;  // ORDER BY
    int64_t limit_ = 0;                   // LIMIT value
    int64_t offset_ = 0;                  // OFFSET value
    bool has_limit_ = false;              // has LIMIT clause
    bool has_offset_ = false;             // has OFFSET clause
};

// INSERT 语句
class InsertStmt : public Statement {
public:
    void setTable(const std::string& table) { table_ = table; }
    void addColumn(const std::string& col) { columns_.push_back(col); }
    void addValue(std::unique_ptr<Expression> val) { values_.push_back(std::move(val)); }

    const std::string& table() const { return table_; }
    const std::vector<std::string>& columns() const { return columns_; }
    const std::vector<std::unique_ptr<Expression>>& values() const { return values_; }

    std::string toString() const override {
        std::string result = "INSERT INTO " + table_;
        if (!columns_.empty()) {
            result += " (";
            for (size_t i = 0; i < columns_.size(); ++i) {
                if (i > 0) result += ", ";
                result += columns_[i];
            }
            result += ")";
        }
        result += " VALUES (";
        for (size_t i = 0; i < values_.size(); ++i) {
            if (i > 0) result += ", ";
            result += values_[i]->toString();
        }
        result += ")";
        return result;
    }

private:
    std::string table_;
    std::vector<std::string> columns_;
    std::vector<std::unique_ptr<Expression>> values_;
};

// CREATE TABLE 语句
class CreateTableStmt : public Statement {
public:
    struct ColumnDef {
        std::string name;
        std::string type;
        bool is_primary_key = false;
        bool is_not_null = false;
        bool is_unique = false;
        std::unique_ptr<Expression> default_value;
        std::string check_expr;  // CHECK constraint expression as string
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

// DROP TABLE 语句
class DropTableStmt : public Statement {
public:
    explicit DropTableStmt(std::string table) : table_(std::move(table)) {}

    const std::string& table() const { return table_; }

    std::string toString() const override {
        return "DROP TABLE " + table_;
    }

private:
    std::string table_;
};

// UPDATE 语句
class UpdateStmt : public Statement {
public:
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

// DELETE 语句
class DeleteStmt : public Statement {
public:
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

// ALTER TABLE 语句
class AlterTableStmt : public Statement {
public:
    enum class ActionType {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN,
        RENAME_TABLE,
        RENAME_COLUMN
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

// BEGIN 语句（阶段三新增）
class BeginStmt : public Statement {
public:
    std::string toString() const override {
        return "BEGIN";
    }
};

// COMMIT 语句（阶段三新增）
class CommitStmt : public Statement {
public:
    std::string toString() const override {
        return "COMMIT";
    }
};

// ROLLBACK 语句（阶段三新增）
class RollbackStmt : public Statement {
public:
    std::string toString() const override {
        return "ROLLBACK";
    }
};

// Phase 4: CREATE INDEX 语句
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

// CREATE VIEW 语句
class CreateViewStmt : public Statement {
public:
    void setViewName(const std::string& name) { view_name_ = name; }
    void setSelectStmt(std::unique_ptr<SelectStmt> select) { select_stmt_ = std::move(select); }

    const std::string& viewName() const { return view_name_; }
    SelectStmt* selectStmt() const { return select_stmt_.get(); }

    std::string toString() const override {
        return "CREATE VIEW " + view_name_ + " AS " + (select_stmt_ ? select_stmt_->toString() : "");
    }

private:
    std::string view_name_;
    std::unique_ptr<SelectStmt> select_stmt_;
};

// DROP VIEW 语句
class DropViewStmt : public Statement {
public:
    explicit DropViewStmt(std::string view_name, bool if_exists = false)
        : view_name_(std::move(view_name)), if_exists_(if_exists) {}

    const std::string& viewName() const { return view_name_; }
    bool ifExists() const { return if_exists_; }

    std::string toString() const override {
        if (if_exists_) {
            return "DROP VIEW IF EXISTS " + view_name_;
        }
        return "DROP VIEW " + view_name_;
    }

private:
    std::string view_name_;
    bool if_exists_;
};

// Phase 4: DROP INDEX 语句
class DropIndexStmt : public Statement {
public:
    explicit DropIndexStmt(std::string index_name)
        : index_name_(std::move(index_name)) {}

    const std::string& indexName() const { return index_name_; }

    std::string toString() const override {
        return "DROP INDEX " + index_name_;
    }

private:
    std::string index_name_;
};

// Phase 4: ANALYZE 语句
class AnalyzeStmt : public Statement {
public:
    explicit AnalyzeStmt(std::string table_name)
        : table_name_(std::move(table_name)) {}

    const std::string& tableName() const { return table_name_; }

    std::string toString() const override {
        return "ANALYZE " + table_name_;
    }

private:
    std::string table_name_;
};

// Phase 4: EXPLAIN 语句
class ExplainStmt : public Statement {
public:
    ExplainStmt() = default;
    explicit ExplainStmt(std::unique_ptr<Statement> stmt)
        : statement_(std::move(stmt)) {}

    Statement* innerStatement() const { return statement_.get(); }

    std::string toString() const override {
        return "EXPLAIN " + (statement_ ? statement_->toString() : "");
    }

private:
    std::unique_ptr<Statement> statement_;
};

// AST 根节点
class AST {
public:
    void setStatement(std::unique_ptr<Statement> stmt) { statement_ = std::move(stmt); }

    Statement* statement() const { return statement_.get(); }

    std::string toString() const {
        if (statement_) {
            return statement_->toString();
        }
        return "<empty>";
    }

private:
    std::unique_ptr<Statement> statement_;
};

} // namespace sql
} // namespace tinydb
