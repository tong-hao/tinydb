#pragma once

#include <string>
#include "statement.h"

namespace tinydb {
namespace sql {

class DropTableStmt : public Statement {
public:
    explicit DropTableStmt(std::string table) : table_(std::move(table)) {}

    const std::string& table() const { return table_; }

    std::string toString() const override {
        return "DROP TABLE " + table_;
    }

private:
    std::string table_;
};

}
}
