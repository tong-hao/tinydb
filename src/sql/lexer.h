#pragma once

#include "ast.h"
#include <string>
#include <vector>
#include <memory>

namespace tinydb {
namespace sql {

// Token 类型
enum class TokenType {
    // 关键字
    SELECT, INSERT, UPDATE, DELETE,
    FROM, WHERE, INTO, VALUES, SET,
    CREATE, DROP, TABLE, INDEX, ON,
    AND, OR, NOT, NULLX, TRUE, FALSE,
    PRIMARY, KEY, UNIQUE, FOREIGN, REFERENCES,
    AS, DISTINCT, ALL,
    ORDER, BY, ASC, DESC, LIMIT, OFFSET,
    GROUP, HAVING,
    JOIN, INNER, LEFT, RIGHT, OUTER, CROSS, NATURAL, USING,
    IN, EXISTS, BETWEEN, LIKE, IS,
    CASE, WHEN, THEN, ELSE, END,
    IF, CAST, DEFAULT,
    EXPLAIN, DESCRIBE, SHOW, DATABASES, TABLES,
    ALTER, ADD, COLUMN, MODIFY, RENAME,

    // 数据类型
    INT, BIGINT, SMALLINT, TINYINT,
    FLOAT, DOUBLE, REAL, DECIMAL, NUMERIC,
    CHAR, VARCHAR, TEXT,
    DATE, TIME, TIMESTAMP, DATETIME,
    BOOL, BOOLEAN,

    // 标识符和常量
    IDENTIFIER, NUMBER, STRING_LITERAL,

    // 运算符
    EQ, NE, LT, GT, LE, GE,
    PLUS, MINUS, STAR, SLASH, PERCENT, CONCAT,
    LPAREN, RPAREN, COMMA, SEMICOLON, DOT,

    // 特殊
    END_OF_FILE,
    INVALID
};

// Token 结构
struct Token {
    TokenType type;
    std::string value;
    int line;
    int column;

    Token(TokenType t = TokenType::INVALID, std::string v = "", int l = 0, int c = 0)
        : type(t), value(std::move(v)), line(l), column(c) {}
};

// 词法分析器
class Lexer {
public:
    explicit Lexer(const std::string& input);

    // 获取下一个 Token
    Token nextToken();

    // 获取所有 Token
    std::vector<Token> tokenize();

private:
    void skipWhitespace();
    void skipComment();
    Token parseIdentifier();
    Token parseNumber();
    Token parseString();
    Token parseOperator();

    char peek() const;
    char advance();
    bool match(char expected);

    TokenType getKeywordType(const std::string& word) const;

    const std::string& input_;
    size_t pos_;
    int line_;
    int column_;
};

} // namespace sql
} // namespace tinydb
