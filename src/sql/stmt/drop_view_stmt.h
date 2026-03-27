#pragma once

#include <string>
#include "statement.h"

namespace tinydb {
namespace sql {

class DropViewStmt : public Statement {
public:
    explicit DropViewStmt(std::string view_name, bool if_exists = false)
        : view_name_(std::move(view_name)), if_exists_(if_exists) {}

    const std::string& viewName() const { return view_name_; }
    bool ifExists() const { return if_exists_; }

    std::string toString() const override {
        if (if_exists_) {
            return "DROP VIEW IF EXISTS " + view_name_;
        }
        return "DROP VIEW " + view_name_;
    }

private:
    std::string view_name_;
    bool if_exists_;
};

}
}
