#pragma once

#include "statement.h"

namespace tinydb {
namespace sql {

class BeginStmt : public Statement {
public:
    StatementType type() const override { return StatementType::BEGIN; }
    std::string toString() const override {
        return "BEGIN";
    }
};

}
}
