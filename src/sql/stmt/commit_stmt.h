#pragma once

#include "statement.h"

namespace tinydb {
namespace sql {

class CommitStmt : public Statement {
public:
    StatementType type() const override { return StatementType::COMMIT; }
    std::string toString() const override {
        return "COMMIT";
    }
};

}
}
