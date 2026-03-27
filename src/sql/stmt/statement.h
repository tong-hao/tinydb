#pragma once

#include "ast_node.h"

namespace tinydb {
namespace sql {

enum class StatementType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE_TABLE,
    DROP_TABLE,
    ALTER_TABLE,
    CREATE_INDEX,
    DROP_INDEX,
    ANALYZE,
    EXPLAIN,
    BEGIN,
    COMMIT,
    ROLLBACK,
    CREATE_VIEW,
    DROP_VIEW,
    UNKNOWN
};

class Statement : public ASTNode {
public:
    virtual ~Statement() = default;
    virtual StatementType type() const = 0;
};

}
}
