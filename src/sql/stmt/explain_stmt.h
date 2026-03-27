#pragma once

#include <string>
#include <memory>
#include "statement.h"

namespace tinydb {
namespace sql {

class ExplainStmt : public Statement {
public:
    StatementType type() const override { return StatementType::EXPLAIN; }
    ExplainStmt() = default;
    explicit ExplainStmt(std::unique_ptr<Statement> stmt)
        : statement_(std::move(stmt)) {}

    Statement* innerStatement() const { return statement_.get(); }

    std::string toString() const override {
        return "EXPLAIN " + (statement_ ? statement_->toString() : "");
    }

private:
    std::unique_ptr<Statement> statement_;
};

}
}
