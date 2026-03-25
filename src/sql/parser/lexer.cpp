#include "lexer.h"
#include <cctype>
#include <unordered_map>

namespace tinydb {
namespace sql {

Lexer::Lexer(const std::string& input)
    : input_(input), pos_(0), line_(1), column_(1) {
}

Token Lexer::nextToken() {
    skipWhitespace();

    if (pos_ >= input_.length()) {
        return Token(TokenType::END_OF_FILE, "", line_, column_);
    }

    char c = peek();
    int start_line = line_;
    int start_column = column_;

    // 标识符或关键字
    if (std::isalpha(c) || c == '_') {
        return parseIdentifier();
    }

    // 数字
    if (std::isdigit(c)) {
        return parseNumber();
    }

    // 字符串
    if (c == '\'' || c == '"') {
        return parseString();
    }

    // 运算符和标点符号
    return parseOperator();
}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    Token token;
    do {
        token = nextToken();
        tokens.push_back(token);
    } while (token.type != TokenType::END_OF_FILE);
    return tokens;
}

void Lexer::skipWhitespace() {
    while (pos_ < input_.length() && std::isspace(peek())) {
        if (peek() == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        pos_++;
    }
}

Token Lexer::parseIdentifier() {
    int start_line = line_;
    int start_column = column_;
    std::string value;

    while (pos_ < input_.length() && (std::isalnum(peek()) || peek() == '_')) {
        value += advance();
    }

    // 转换为小写以进行关键字匹配
    std::string lower_value = value;
    for (auto& c : lower_value) {
        c = std::tolower(c);
    }

    TokenType type = getKeywordType(lower_value);
    if (type == TokenType::INVALID) {
        type = TokenType::IDENTIFIER;
    }

    return Token(type, value, start_line, start_column);
}

Token Lexer::parseNumber() {
    int start_line = line_;
    int start_column = column_;
    std::string value;

    while (pos_ < input_.length() && std::isdigit(peek())) {
        value += advance();
    }

    // 小数部分
    if (peek() == '.' && pos_ + 1 < input_.length() && std::isdigit(input_[pos_ + 1])) {
        value += advance();  // '.'
        while (pos_ < input_.length() && std::isdigit(peek())) {
            value += advance();
        }
    }

    return Token(TokenType::NUMBER, value, start_line, start_column);
}

Token Lexer::parseString() {
    int start_line = line_;
    int start_column = column_;
    char quote = advance();  // 开头的引号
    std::string value;

    while (pos_ < input_.length() && peek() != quote) {
        if (peek() == '\\' && pos_ + 1 < input_.length()) {
            advance();  // 跳过反斜杠
            char escaped = advance();
            switch (escaped) {
                case 'n': value += '\n'; break;
                case 't': value += '\t'; break;
                case 'r': value += '\r'; break;
                case '\\': value += '\\'; break;
                case '\'': value += '\''; break;
                case '"': value += '"'; break;
                default: value += escaped; break;
            }
        } else {
            value += advance();
        }
    }

    if (peek() == quote) {
        advance();  // 结尾的引号
    }

    return Token(TokenType::STRING_LITERAL, value, start_line, start_column);
}

Token Lexer::parseOperator() {
    int start_line = line_;
    int start_column = column_;
    char c = advance();

    switch (c) {
        case '=': return Token(TokenType::EQ, "=", start_line, start_column);
        case '<':
            if (match('=')) return Token(TokenType::LE, "<=", start_line, start_column);
            if (match('>')) return Token(TokenType::NE, "<>", start_line, start_column);
            return Token(TokenType::LT, "<", start_line, start_column);
        case '>':
            if (match('=')) return Token(TokenType::GE, ">=", start_line, start_column);
            return Token(TokenType::GT, ">", start_line, start_column);
        case '!':
            if (match('=')) return Token(TokenType::NE, "!=", start_line, start_column);
            return Token(TokenType::INVALID, "!", start_line, start_column);
        case '+': return Token(TokenType::PLUS, "+", start_line, start_column);
        case '-': return Token(TokenType::MINUS, "-", start_line, start_column);
        case '*': return Token(TokenType::STAR, "*", start_line, start_column);
        case '/': return Token(TokenType::SLASH, "/", start_line, start_column);
        case '%': return Token(TokenType::PERCENT, "%", start_line, start_column);
        case '(': return Token(TokenType::LPAREN, "(", start_line, start_column);
        case ')': return Token(TokenType::RPAREN, ")", start_line, start_column);
        case ',': return Token(TokenType::COMMA, ",", start_line, start_column);
        case ';': return Token(TokenType::SEMICOLON, ";", start_line, start_column);
        case '.': return Token(TokenType::DOT, ".", start_line, start_column);
        case '|':
            if (match('|')) return Token(TokenType::CONCAT, "||", start_line, start_column);
            return Token(TokenType::INVALID, "|", start_line, start_column);
        default:
            return Token(TokenType::INVALID, std::string(1, c), start_line, start_column);
    }
}

char Lexer::peek() const {
    if (pos_ >= input_.length()) {
        return '\0';
    }
    return input_[pos_];
}

char Lexer::advance() {
    if (pos_ >= input_.length()) {
        return '\0';
    }
    char c = input_[pos_++];
    column_++;
    return c;
}

bool Lexer::match(char expected) {
    if (peek() != expected) {
        return false;
    }
    advance();
    return true;
}

TokenType Lexer::getKeywordType(const std::string& word) const {
    static const std::unordered_map<std::string, TokenType> keywords = {
        // 关键字
        {"select", TokenType::SELECT},
        {"insert", TokenType::INSERT},
        {"update", TokenType::UPDATE},
        {"delete", TokenType::DELETE},
        {"from", TokenType::FROM},
        {"where", TokenType::WHERE},
        {"into", TokenType::INTO},
        {"values", TokenType::VALUES},
        {"set", TokenType::SET},
        {"create", TokenType::CREATE},
        {"drop", TokenType::DROP},
        {"table", TokenType::TABLE},
        {"index", TokenType::INDEX},
        {"on", TokenType::ON},
        {"and", TokenType::AND},
        {"or", TokenType::OR},
        {"not", TokenType::NOT},
        {"null", TokenType::NULLX},
        {"true", TokenType::TRUE},
        {"false", TokenType::FALSE},
        {"primary", TokenType::PRIMARY},
        {"key", TokenType::KEY},
        {"unique", TokenType::UNIQUE},
        {"foreign", TokenType::FOREIGN},
        {"references", TokenType::REFERENCES},
        {"as", TokenType::AS},
        {"distinct", TokenType::DISTINCT},
        {"all", TokenType::ALL},
        {"order", TokenType::ORDER},
        {"by", TokenType::BY},
        {"asc", TokenType::ASC},
        {"desc", TokenType::DESC},
        {"limit", TokenType::LIMIT},
        {"offset", TokenType::OFFSET},
        {"group", TokenType::GROUP},
        {"having", TokenType::HAVING},
        {"join", TokenType::JOIN},
        {"inner", TokenType::INNER},
        {"left", TokenType::LEFT},
        {"right", TokenType::RIGHT},
        {"outer", TokenType::OUTER},
        {"cross", TokenType::CROSS},
        {"natural", TokenType::NATURAL},
        {"using", TokenType::USING},
        {"in", TokenType::IN},
        {"exists", TokenType::EXISTS},
        {"between", TokenType::BETWEEN},
        {"like", TokenType::LIKE},
        {"is", TokenType::IS},
        {"case", TokenType::CASE},
        {"when", TokenType::WHEN},
        {"then", TokenType::THEN},
        {"else", TokenType::ELSE},
        {"end", TokenType::END},
        {"if", TokenType::IF},
        {"cast", TokenType::CAST},
        {"default", TokenType::DEFAULT},
        {"explain", TokenType::EXPLAIN},
        {"describe", TokenType::DESCRIBE},
        {"show", TokenType::SHOW},
        {"databases", TokenType::DATABASES},
        {"tables", TokenType::TABLES},
        {"alter", TokenType::ALTER},
        {"add", TokenType::ADD},
        {"column", TokenType::COLUMN},
        {"modify", TokenType::MODIFY},
        {"rename", TokenType::RENAME},

        // 事务关键字（阶段三新增）
        {"begin", TokenType::BEGIN},
        {"commit", TokenType::COMMIT},
        {"rollback", TokenType::ROLLBACK},
        {"transaction", TokenType::TRANSACTION},
        {"savepoint", TokenType::SAVEPOINT},

        // 数据类型
        {"int", TokenType::INT},
        {"integer", TokenType::INT},
        {"bigint", TokenType::BIGINT},
        {"smallint", TokenType::SMALLINT},
        {"tinyint", TokenType::TINYINT},
        {"float", TokenType::FLOAT},
        {"double", TokenType::DOUBLE},
        {"real", TokenType::REAL},
        {"decimal", TokenType::DECIMAL},
        {"numeric", TokenType::NUMERIC},
        {"char", TokenType::CHAR},
        {"varchar", TokenType::VARCHAR},
        {"text", TokenType::TEXT},
        {"date", TokenType::DATE},
        {"time", TokenType::TIME},
        {"timestamp", TokenType::TIMESTAMP},
        {"datetime", TokenType::DATETIME},
        {"boolean", TokenType::BOOLEAN},
        {"bool", TokenType::BOOL},
    };

    auto it = keywords.find(word);
    if (it != keywords.end()) {
        return it->second;
    }
    return TokenType::INVALID;
}

} // namespace sql
} // namespace tinydb
