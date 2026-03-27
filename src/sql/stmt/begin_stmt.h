#pragma once

#include "statement.h"

namespace tinydb {
namespace sql {

class BeginStmt : public Statement {
public:
    std::string toString() const override {
        return "BEGIN";
    }
};

}
}
