#pragma once

#include "statement.h"

namespace tinydb {
namespace sql {

class RollbackStmt : public Statement {
public:
    StatementType type() const override { return StatementType::ROLLBACK; }
    std::string toString() const override {
        return "ROLLBACK";
    }
};

}
}
