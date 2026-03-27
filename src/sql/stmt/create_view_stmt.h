#pragma once

#include <string>
#include <memory>
#include "statement.h"
#include "sql/stmt/select_stmt.h"

namespace tinydb {
namespace sql {

class CreateViewStmt : public Statement {
public:
    StatementType type() const override { return StatementType::CREATE_VIEW; }
    void setViewName(const std::string& name) { view_name_ = name; }
    void setSelectStmt(std::unique_ptr<SelectStmt> select) { select_stmt_ = std::move(select); }

    const std::string& viewName() const { return view_name_; }
    SelectStmt* selectStmt() const { return select_stmt_.get(); }

    std::string toString() const override {
        return "CREATE VIEW " + view_name_ + " AS " + (select_stmt_ ? select_stmt_->toString() : "");
    }

private:
    std::string view_name_;
    std::unique_ptr<SelectStmt> select_stmt_;
};

}
}
