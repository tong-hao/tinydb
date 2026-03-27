#pragma once

#include <string>
#include <memory>
#include "statement.h"

namespace tinydb {
namespace sql {

class SQLParseTree {
public:
    void setStatement(std::unique_ptr<Statement> stmt) { statement_ = std::move(stmt); }

    Statement* statement() const { return statement_.get(); }

    std::string toString() const {
        if (statement_) {
            return statement_->toString();
        }
        return "<empty>";
    }

private:
    std::unique_ptr<Statement> statement_;
};

}
}
