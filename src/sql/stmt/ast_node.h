#pragma once
#include <string>

namespace tinydb {
namespace sql {
// SQLParseTree 节点基类
class ASTNode {
public:
    virtual ~ASTNode() = default;
    virtual std::string toString() const = 0;
};

}
}
