#pragma once

#include "ast.h"
#include "lexer.h"
#include <vector>
#include <memory>
#include <string>

namespace tinydb {
namespace sql {

// 递归下降解析器
class Parser {
public:
    explicit Parser(const std::vector<Token>& tokens);

    // 解析入口
    std::unique_ptr<AST> parse();

    // 获取错误信息
    const std::string& error() const { return error_; }

private:
    const std::vector<Token>& tokens_;
    size_t pos_;
    std::string error_;

    // 当前token
    const Token& current() const;
    // 向前看n个token
    const Token& peek(size_t n = 0) const;
    // 消费当前token
    Token consume();
    // 匹配并消费指定类型的token
    bool match(TokenType type);
    // 期望指定类型的token
    bool expect(TokenType type, const std::string& msg);
    // 检查是否到达末尾
    bool isAtEnd() const;

    // 解析语句
    std::unique_ptr<Statement> parseStatement();

    // 解析各种语句
    std::unique_ptr<SelectStmt> parseSelect();
    std::unique_ptr<InsertStmt> parseInsert();
    std::unique_ptr<UpdateStmt> parseUpdate();
    std::unique_ptr<DeleteStmt> parseDelete();
    std::unique_ptr<CreateTableStmt> parseCreateTable();
    std::unique_ptr<DropTableStmt> parseDropTable();
    std::unique_ptr<AlterTableStmt> parseAlterTable();

    // 解析表达式
    std::unique_ptr<Expression> parseExpression();
    std::unique_ptr<Expression> parseOrExpression();
    std::unique_ptr<Expression> parseAndExpression();
    std::unique_ptr<Expression> parseNotExpression();
    std::unique_ptr<Expression> parseComparisonExpression();
    std::unique_ptr<Expression> parseAdditiveExpression();
    std::unique_ptr<Expression> parseMultiplicativeExpression();
    std::unique_ptr<Expression> parseUnaryExpression();
    std::unique_ptr<Expression> parsePrimaryExpression();

    // 设置错误信息
    void setError(const std::string& msg);
};

} // namespace sql
} // namespace tinydb
