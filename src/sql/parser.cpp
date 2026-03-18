#include "parser.h"
#include <cctype>

namespace tinydb {
namespace sql {

Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens), pos_(0) {}

const Token& Parser::current() const {
    if (pos_ < tokens_.size()) {
        return tokens_[pos_];
    }
    static const Token eof_token{TokenType::END_OF_FILE, ""};
    return eof_token;
}

const Token& Parser::peek(size_t n) const {
    if (pos_ + n < tokens_.size()) {
        return tokens_[pos_ + n];
    }
    static const Token eof_token{TokenType::END_OF_FILE, ""};
        return eof_token;
}

Token Parser::consume() {
    if (pos_ < tokens_.size()) {
        return tokens_[pos_++];
    }
    return Token{TokenType::END_OF_FILE, ""};
}

bool Parser::match(TokenType type) {
    if (current().type == type) {
        consume();
        return true;
    }
    return false;
}

bool Parser::expect(TokenType type, const std::string& msg) {
    if (current().type == type) {
        consume();
        return true;
    }
    setError(msg + ", got: " + current().value);
    return false;
}

bool Parser::isAtEnd() const {
    return current().type == TokenType::END_OF_FILE;
}

void Parser::setError(const std::string& msg) {
    if (error_.empty()) {
        error_ = msg;
    }
}

std::unique_ptr<AST> Parser::parse() {
    auto ast = std::make_unique<AST>();
    auto stmt = parseStatement();
    if (stmt) {
        ast->setStatement(std::move(stmt));
    }
    return ast;
}

std::unique_ptr<Statement> Parser::parseStatement() {
    if (current().type == TokenType::SELECT) {
        return parseSelect();
    } else if (current().type == TokenType::INSERT) {
        return parseInsert();
    } else if (current().type == TokenType::UPDATE) {
        return parseUpdate();
    } else if (current().type == TokenType::DELETE) {
        return parseDelete();
    } else if (current().type == TokenType::CREATE) {
        return parseCreateTable();
    } else if (current().type == TokenType::DROP) {
        return parseDropTable();
    } else if (current().type == TokenType::ALTER) {
        return parseAlterTable();
    }
    setError("Unknown statement type: " + current().value);
    return nullptr;
}

std::unique_ptr<SelectStmt> Parser::parseSelect() {
    auto stmt = std::make_unique<SelectStmt>();

    // SELECT
    if (!match(TokenType::SELECT)) {
        setError("Expected SELECT");
        return nullptr;
    }

    // 选择列表
    if (current().type == TokenType::STAR) {
        consume();
        stmt->addSelectColumn("*");
    } else {
        while (true) {
            if (current().type == TokenType::IDENTIFIER) {
                std::string col_name = consume().value;
                stmt->addSelectColumn(col_name);
            } else {
                break;
            }

            if (!match(TokenType::COMMA)) {
                break;
            }
        }
    }

    // FROM
    if (!match(TokenType::FROM)) {
        setError("Expected FROM");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        stmt->setFromTable(consume().value);
    } else {
        setError("Expected table name");
        return nullptr;
    }

    // 可选的 WHERE
    if (match(TokenType::WHERE)) {
        auto where_expr = parseExpression();
        if (where_expr) {
            stmt->setWhereCondition(std::move(where_expr));
        }
    }

    return stmt;
}

std::unique_ptr<InsertStmt> Parser::parseInsert() {
    auto stmt = std::make_unique<InsertStmt>();

    // INSERT INTO
    if (!match(TokenType::INSERT)) {
        setError("Expected INSERT");
        return nullptr;
    }
    if (!match(TokenType::INTO)) {
        setError("Expected INTO");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        stmt->setTable(consume().value);
    } else {
        setError("Expected table name");
        return nullptr;
    }

    // 可选的列列表
    if (match(TokenType::LPAREN)) {
        while (true) {
            if (current().type == TokenType::IDENTIFIER) {
                stmt->addColumn(consume().value);
            } else {
                break;
            }

            if (!match(TokenType::COMMA)) {
                break;
            }
        }
        if (!match(TokenType::RPAREN)) {
            setError("Expected )");
            return nullptr;
        }
    }

    // VALUES
    if (!match(TokenType::VALUES)) {
        setError("Expected VALUES");
        return nullptr;
    }

    // 值列表
    if (!match(TokenType::LPAREN)) {
        setError("Expected (");
        return nullptr;
    }

    while (true) {
        auto expr = parsePrimaryExpression();
        if (expr) {
            stmt->addValue(std::move(expr));
        } else {
            break;
        }

        if (!match(TokenType::COMMA)) {
            break;
        }
    }

    if (!match(TokenType::RPAREN)) {
        setError("Expected )");
        return nullptr;
    }

    return stmt;
}

std::unique_ptr<UpdateStmt> Parser::parseUpdate() {
    auto stmt = std::make_unique<UpdateStmt>();

    // UPDATE
    if (!match(TokenType::UPDATE)) {
        setError("Expected UPDATE");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        stmt->setTable(consume().value);
    } else {
        setError("Expected table name");
        return nullptr;
    }

    // SET
    if (!match(TokenType::SET)) {
        setError("Expected SET");
        return nullptr;
    }

    // 赋值列表
    while (true) {
        if (current().type != TokenType::IDENTIFIER) {
            setError("Expected column name");
            return nullptr;
        }
        std::string col_name = consume().value;

        if (!match(TokenType::EQ)) {
            setError("Expected =");
            return nullptr;
        }

        auto value_expr = parsePrimaryExpression();
        if (!value_expr) {
            setError("Expected value");
            return nullptr;
        }

        stmt->addAssignment(col_name, std::move(value_expr));

        if (!match(TokenType::COMMA)) {
            break;
        }
    }

    // 可选的 WHERE
    if (match(TokenType::WHERE)) {
        auto where_expr = parseExpression();
        if (where_expr) {
            stmt->setWhereCondition(std::move(where_expr));
        }
    }

    return stmt;
}

std::unique_ptr<DeleteStmt> Parser::parseDelete() {
    auto stmt = std::make_unique<DeleteStmt>();

    // DELETE FROM
    if (!match(TokenType::DELETE)) {
        setError("Expected DELETE");
        return nullptr;
    }
    if (!match(TokenType::FROM)) {
        setError("Expected FROM");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        stmt->setTable(consume().value);
    } else {
        setError("Expected table name");
        return nullptr;
    }

    // 可选的 WHERE
    if (match(TokenType::WHERE)) {
        auto where_expr = parseExpression();
        if (where_expr) {
            stmt->setWhereCondition(std::move(where_expr));
        }
    }

    return stmt;
}

std::unique_ptr<CreateTableStmt> Parser::parseCreateTable() {
    auto stmt = std::make_unique<CreateTableStmt>();

    // CREATE TABLE
    if (!match(TokenType::CREATE)) {
        setError("Expected CREATE");
        return nullptr;
    }
    if (!match(TokenType::TABLE)) {
        setError("Expected TABLE");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        stmt->setTable(consume().value);
    } else {
        setError("Expected table name");
        return nullptr;
    }

    // 列定义列表
    if (!match(TokenType::LPAREN)) {
        setError("Expected (");
        return nullptr;
    }

    while (true) {
        if (current().type != TokenType::IDENTIFIER) {
            setError("Expected column name");
            return nullptr;
        }
        std::string col_name = consume().value;

        // 类型
        std::string type_str;
        if (current().type == TokenType::INT ||
            current().type == TokenType::BIGINT ||
            current().type == TokenType::FLOAT ||
            current().type == TokenType::DOUBLE ||
            current().type == TokenType::BOOL ||
            current().type == TokenType::CHAR ||
            current().type == TokenType::VARCHAR ||
            current().type == TokenType::IDENTIFIER) {
            type_str = consume().value;

            // 处理 VARCHAR(n) 等带长度的类型
            if (match(TokenType::LPAREN)) {
                if (current().type == TokenType::NUMBER) {
                    type_str += "(" + consume().value + ")";
                }
                if (!match(TokenType::RPAREN)) {
                    setError("Expected )");
                    return nullptr;
                }
            }
        } else {
            setError("Expected type name");
            return nullptr;
        }

        stmt->addColumn(col_name, type_str);

        if (!match(TokenType::COMMA)) {
            break;
        }
    }

    if (!match(TokenType::RPAREN)) {
        setError("Expected )");
        return nullptr;
    }

    return stmt;
}

std::unique_ptr<DropTableStmt> Parser::parseDropTable() {
    // DROP TABLE
    if (!match(TokenType::DROP)) {
        setError("Expected DROP");
        return nullptr;
    }
    if (!match(TokenType::TABLE)) {
        setError("Expected TABLE");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        std::string table_name = consume().value;
        return std::make_unique<DropTableStmt>(table_name);
    } else {
        setError("Expected table name");
        return nullptr;
    }
}

std::unique_ptr<AlterTableStmt> Parser::parseAlterTable() {
    auto stmt = std::make_unique<AlterTableStmt>();

    // ALTER TABLE
    if (!match(TokenType::ALTER)) {
        setError("Expected ALTER");
        return nullptr;
    }
    if (!match(TokenType::TABLE)) {
        setError("Expected TABLE");
        return nullptr;
    }

    // 表名
    if (current().type == TokenType::IDENTIFIER) {
        stmt->setTable(consume().value);
    } else {
        setError("Expected table name");
        return nullptr;
    }

    // ADD / DROP / MODIFY / RENAME
    if (match(TokenType::ADD)) {
        stmt->setAction(AlterTableStmt::ActionType::ADD_COLUMN);
        if (match(TokenType::COLUMN)) {
            // 可选的 COLUMN 关键字
        }
        if (current().type == TokenType::IDENTIFIER) {
            std::string col_name = consume().value;
            std::string type_str;
            if (current().type == TokenType::INT ||
                current().type == TokenType::VARCHAR ||
                current().type == TokenType::IDENTIFIER) {
                type_str = consume().value;
                if (match(TokenType::LPAREN)) {
                    if (current().type == TokenType::NUMBER) {
                        type_str += "(" + consume().value + ")";
                    }
                    match(TokenType::RPAREN);
                }
            }
            stmt->setColumnDef(col_name, type_str);
        }
    } else if (match(TokenType::DROP)) {
        stmt->setAction(AlterTableStmt::ActionType::DROP_COLUMN);
        if (match(TokenType::COLUMN)) {
            // 可选的 COLUMN 关键字
        }
        if (current().type == TokenType::IDENTIFIER) {
            std::string col_name = consume().value;
            stmt->setColumnDef(col_name, "");
        }
    }

    return stmt;
}

// 表达式解析 - 使用优先级 climbing
std::unique_ptr<Expression> Parser::parseExpression() {
    return parseOrExpression();
}

std::unique_ptr<Expression> Parser::parseOrExpression() {
    auto left = parseAndExpression();
    if (!left) return nullptr;

    while (match(TokenType::OR)) {
        auto right = parseAndExpression();
        if (!right) return nullptr;
        left = std::make_unique<LogicalExpr>(OpType::OR, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> Parser::parseAndExpression() {
    auto left = parseNotExpression();
    if (!left) return nullptr;

    while (match(TokenType::AND)) {
        auto right = parseNotExpression();
        if (!right) return nullptr;
        left = std::make_unique<LogicalExpr>(OpType::AND, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<Expression> Parser::parseNotExpression() {
    if (match(TokenType::NOT)) {
        auto expr = parseNotExpression();
        if (!expr) return nullptr;
        return std::make_unique<LogicalExpr>(OpType::NOT, std::move(expr));
    }
    return parseComparisonExpression();
}

std::unique_ptr<Expression> Parser::parseComparisonExpression() {
    auto left = parseAdditiveExpression();
    if (!left) return nullptr;

    OpType op;
    if (match(TokenType::EQ)) {
        op = OpType::EQ;
    } else if (match(TokenType::NE)) {
        op = OpType::NE;
    } else if (match(TokenType::LT)) {
        op = OpType::LT;
    } else if (match(TokenType::LE)) {
        op = OpType::LE;
    } else if (match(TokenType::GT)) {
        op = OpType::GT;
    } else if (match(TokenType::GE)) {
        op = OpType::GE;
    } else {
        return left;
    }

    auto right = parseAdditiveExpression();
    if (!right) return nullptr;

    return std::make_unique<ComparisonExpr>(op, std::move(left), std::move(right));
}

std::unique_ptr<Expression> Parser::parseAdditiveExpression() {
    auto left = parseMultiplicativeExpression();
    if (!left) return nullptr;

    while (true) {
        if (match(TokenType::PLUS)) {
            auto right = parseMultiplicativeExpression();
            if (!right) return nullptr;
            left = std::make_unique<BinaryOpExpr>(OpType::ADD, std::move(left), std::move(right));
        } else if (match(TokenType::MINUS)) {
            auto right = parseMultiplicativeExpression();
            if (!right) return nullptr;
            left = std::make_unique<BinaryOpExpr>(OpType::SUB, std::move(left), std::move(right));
        } else {
            break;
        }
    }

    return left;
}

std::unique_ptr<Expression> Parser::parseMultiplicativeExpression() {
    auto left = parseUnaryExpression();
    if (!left) return nullptr;

    while (true) {
        if (match(TokenType::STAR)) {
            auto right = parseUnaryExpression();
            if (!right) return nullptr;
            left = std::make_unique<BinaryOpExpr>(OpType::MUL, std::move(left), std::move(right));
        } else if (match(TokenType::SLASH)) {
            auto right = parseUnaryExpression();
            if (!right) return nullptr;
            left = std::make_unique<BinaryOpExpr>(OpType::DIV, std::move(left), std::move(right));
        } else {
            break;
        }
    }

    return left;
}

std::unique_ptr<Expression> Parser::parseUnaryExpression() {
    if (match(TokenType::MINUS)) {
        auto expr = parseUnaryExpression();
        if (!expr) return nullptr;
        // 创建 0 - expr
        auto zero = std::make_unique<LiteralExpr>("0");
        return std::make_unique<BinaryOpExpr>(OpType::SUB, std::move(zero), std::move(expr));
    }
    return parsePrimaryExpression();
}

std::unique_ptr<Expression> Parser::parsePrimaryExpression() {
    if (current().type == TokenType::NUMBER) {
        return std::make_unique<LiteralExpr>(consume().value);
    } else if (current().type == TokenType::STRING_LITERAL) {
        return std::make_unique<LiteralExpr>(consume().value);
    } else if (current().type == TokenType::IDENTIFIER) {
        return std::make_unique<ColumnRefExpr>(consume().value);
    } else if (match(TokenType::LPAREN)) {
        auto expr = parseExpression();
        if (!match(TokenType::RPAREN)) {
            setError("Expected )");
            return nullptr;
        }
        return expr;
    }

    setError("Unexpected token: " + current().value);
    return nullptr;
}

} // namespace sql
} // namespace tinydb
