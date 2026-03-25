#include "parser.h"
#include "lexer_generated.h"
#include "parser_generated.h"
#include <cstring>
#include <sstream>

// 定义全局 AST 指针
namespace tinydb { namespace sql { class AST; } }
tinydb::sql::AST* g_ast = nullptr;

namespace tinydb {
namespace sql {

Parser::Parser(const std::string& sql) : sql_(sql) {}

std::unique_ptr<AST> Parser::parse() {
    // 创建新的 AST 对象
    auto ast = std::make_unique<AST>();
    g_ast = ast.get();

    // 设置输入字符串
    YY_BUFFER_STATE buffer = yy_scan_string(sql_.c_str());

    // 解析
    int result = yyparse();

    // 清理
    yy_delete_buffer(buffer);

    // 检查解析结果
    if (result != 0) {
        error_ = "Parse error";
        return nullptr;
    }

    // 检查是否有语句被解析
    if (ast->statement() == nullptr) {
        error_ = "Empty statement";
        return nullptr;
    }

    return ast;
}

} // namespace sql
} // namespace tinydb
