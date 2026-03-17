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

    void setFromTable(const std::string& table) { from_table_ = table; }

    const std::vector<std::unique_ptr<Expression>>& selectList() const { return select_list_; }
    const std::string& fromTable() const { return from_table_; }

    std::string toString() const override {
        std::string result = "SELECT ";
        for (size_t i = 0; i < select_list_.size(); ++i) {
            if (i > 0) result += ", ";
            result += select_list_[i]->toString();
        }
        if (!from_table_.empty()) {
            result += " FROM " + from_table_;
        }
        return result;
    }

private:
    std::vector<std::unique_ptr<Expression>> select_list_;
    std::string from_table_;
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
