#pragma once

#include "ast_node.h"

namespace tinydb {
namespace sql {

class Statement : public ASTNode {
public:
    virtual ~Statement() = default;
};

}
}
