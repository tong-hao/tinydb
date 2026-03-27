#pragma once

#include <string>
#include <vector>
#include <memory>
#include "statement.h"
#include "expression.h"

namespace tinydb {
namespace sql {

class InsertStmt : public Statement {
public:
    void setTable(const std::string& table) { table_ = table; }
    void addColumn(const std::string& col) { columns_.push_back(col); }
    void addValue(std::unique_ptr<Expression> val) {
        if (multi_row_values_.empty()) {
            multi_row_values_.push_back({});
        }
        multi_row_values_.back().push_back(std::move(val));
    }
    void addRow() { multi_row_values_.push_back({}); }

    const std::string& table() const { return table_; }
    const std::vector<std::string>& columns() const { return columns_; }
    const std::vector<std::unique_ptr<Expression>>& values() const {
        return multi_row_values_.empty() ? values_ : multi_row_values_[0];
    }
    const std::vector<std::vector<std::unique_ptr<Expression>>>& multiRowValues() const {
        return multi_row_values_;
    }
    bool isMultiRow() const { return multi_row_values_.size() > 1; }

    std::string toString() const override {
        std::string result = "INSERT INTO " + table_;
        if (!columns_.empty()) {
            result += " (";
            for (size_t i = 0; i < columns_.size(); ++i) {
                if (i > 0) result += ", ";
                result += columns_[i];
            }
            result += ")";
        }
        result += " VALUES ";
        for (size_t r = 0; r < multi_row_values_.size(); ++r) {
            if (r > 0) result += ", ";
            result += "(";
            const auto& row = multi_row_values_[r];
            for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0) result += ", ";
                result += row[i]->toString();
            }
            result += ")";
        }
        return result;
    }

private:
    std::string table_;
    std::vector<std::string> columns_;
    std::vector<std::unique_ptr<Expression>> values_;
    std::vector<std::vector<std::unique_ptr<Expression>>> multi_row_values_;
};

}
}
