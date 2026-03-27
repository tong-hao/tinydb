#pragma once

#include <string>
#include "statement.h"

namespace tinydb {
namespace sql {

class DropIndexStmt : public Statement {
public:
    StatementType type() const override { return StatementType::DROP_INDEX; }
    explicit DropIndexStmt(std::string index_name)
        : index_name_(std::move(index_name)) {}

    const std::string& indexName() const { return index_name_; }

    std::string toString() const override {
        return "DROP INDEX " + index_name_;
    }

private:
    std::string index_name_;
};

}
}
