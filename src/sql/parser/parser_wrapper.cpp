#include "parser.h"
#include "ast.h"

// Forward declarations for bison-generated functions
extern int yyparse(void);
extern void yy_scan_string(const char* str);
extern void yylex_destroy(void);

// Global SQLParseTree pointer - defined here
tinydb::sql::SQLParseTree* g_ast = nullptr;

namespace tinydb {
namespace sql {

Parser::Parser(const std::string& sql)
    : sql_(sql) {
}

std::unique_ptr<SQLParseTree> Parser::parse() {
    // Reset error state
    error_.clear();

    // Reset global SQLParseTree
    ::g_ast = nullptr;

    // Set up the lexer
    yy_scan_string(sql_.c_str());

    // Parse
    int result = yyparse();

    // Clean up lexer
    yylex_destroy();

    if (result != 0) {
        // Parsing failed
        if (error_.empty()) {
            error_ = "Parse error";
        }
        delete ::g_ast;
        ::g_ast = nullptr;
        return nullptr;
    }

    return std::unique_ptr<SQLParseTree>(::g_ast);
}

} // namespace sql
} // namespace tinydb
