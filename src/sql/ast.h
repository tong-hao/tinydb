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
class BeginStmt;      // 新增
class CommitStmt;     // 新增
class RollbackStmt;   // 新增

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
        return "ColumnRef(" + column_name_ + ")";
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
            return "(" + op_str + " " + left_->toString() + ")";
        }
        return "(" + left_->toString() + " " + op_str + " " + right_->toString() + ")";
    }

private:
    OpType op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

// 语句基类
class Statement : public ASTNode {
public:
    virtual ~Statement() = default;
};

// SELECT 语句
class SelectStmt : public Statement {
public:
    void addSelectItem(std::unique_ptr<Expression> expr) {
        select_list_.push_back(std::move(expr));
    }

    void addSelectColumn(const std::string& col) {
        select_list_.push_back(std::make_unique<ColumnRefExpr>(col));
    }

    void setFromTable(const std::string& table) { from_table_ = table; }
    void setWhereCondition(std::unique_ptr<Expression> condition) { where_condition_ = std::move(condition); }

    const std::vector<std::unique_ptr<Expression>>& selectList() const { return select_list_; }
    const std::string& fromTable() const { return from_table_; }
    Expression* whereCondition() const { return where_condition_.get(); }

    std::string toString() const override {
        std::string result = "SELECT ";
        for (size_t i = 0; i < select_list_.size(); ++i) {
            if (i > 0) result += ", ";
            result += select_list_[i]->toString();
        }
        if (!from_table_.empty()) {
            result += " FROM " + from_table_;
        }
        if (where_condition_) {
            result += " WHERE " + where_condition_->toString();
        }
        return result;
    }

private:
    std::vector<std::unique_ptr<Expression>> select_list_;
    std::string from_table_;
    std::unique_ptr<Expression> where_condition_;
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
    };

    void setTable(const std::string& table) { table_ = table; }
    void addColumn(const std::string& name, const std::string& type) {
        columns_.push_back({name, type});
    }

    const std::string& table() const { return table_; }
    const std::vector<ColumnDef>& columns() const { return columns_; }

    std::string toString() const override {
        std::string result = "CREATE TABLE " + table_ + " (";
        for (size_t i = 0; i < columns_.size(); ++i) {
            if (i > 0) result += ", ";
            result += columns_[i].name + " " + columns_[i].type;
        }
        result += ")";
        return result;
    }

private:
    std::string table_;
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
        RENAME_TABLE
    };

    void setTable(const std::string& table) { table_ = table; }
    void setAction(ActionType action) { action_ = action; }
    void setNewTableName(const std::string& name) { new_table_name_ = name; }
    void setColumnDef(const std::string& name, const std::string& type) {
        column_name_ = name;
        column_type_ = type;
    }

    const std::string& table() const { return table_; }
    ActionType action() const { return action_; }
    const std::string& newTableName() const { return new_table_name_; }
    const std::string& columnName() const { return column_name_; }
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
        }
        return result;
    }

private:
    std::string table_;
    ActionType action_;
    std::string new_table_name_;
    std::string column_name_;
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
