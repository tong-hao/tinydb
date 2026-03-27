#pragma once

#include "ast.h"
#include <string>
#include <memory>

namespace tinydb {
namespace sql {

// Bison/Flex 生成的解析器包装类
class Parser {
public:
    explicit Parser(const std::string& sql);

    // 解析入口
    std::unique_ptr<SQLParseTree> parse();

    // 获取错误信息
    const std::string& error() const { return error_; }

private:
    std::string sql_;
    std::string error_;
};

} // namespace sql
} // namespace tinydb
