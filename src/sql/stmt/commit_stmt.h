#pragma once

#include "statement.h"

namespace tinydb {
namespace sql {

class CommitStmt : public Statement {
public:
    std::string toString() const override {
        return "COMMIT";
    }
};

}
}
