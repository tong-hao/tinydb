#pragma once

#include "statement.h"

namespace tinydb {
namespace sql {

class RollbackStmt : public Statement {
public:
    std::string toString() const override {
        return "ROLLBACK";
    }
};

}
}
