#include "executor.h"
#include "common/logger.h"
#include "system_view_manager.h"
#include <sstream>
#include <algorithm>
#include <cctype>
#include "sql/parser.h"

namespace tinydb {
namespace engine {

Executor::Executor() {
    LOG_INFO("Executor initialized");
}

Executor::~Executor() {
}

void Executor::initialize(storage::StorageEngine* storage_engine) {
    storage_engine_ = storage_engine;
    LOG_INFO("Executor bound to storage engine");
}

ExecutionResult Executor::execute(const sql::AST& ast) {
    if (!ast.statement()) {
        return ExecutionResult::error("Empty statement");
    }

    const sql::Statement* stmt = ast.statement();

    // 根据语句类型分发执行
    if (auto select_stmt = dynamic_cast<const sql::SelectStmt*>(stmt)) {
        return executeSelect(select_stmt);
    } else if (auto insert_stmt = dynamic_cast<const sql::InsertStmt*>(stmt)) {
        return executeInsert(insert_stmt);
    }  else if (auto update_stmt = dynamic_cast<const sql::UpdateStmt*>(stmt)) {
        return executeUpdate(update_stmt);
    } else if (auto delete_stmt = dynamic_cast<const sql::DeleteStmt*>(stmt)) {
        return executeDelete(delete_stmt);
    } else if (auto create_stmt = dynamic_cast<const sql::CreateTableStmt*>(stmt)) {
        return executeCreateTable(create_stmt);
    } else if (auto drop_stmt = dynamic_cast<const sql::DropTableStmt*>(stmt)) {
        return executeDropTable(drop_stmt);
    } else if (auto alter_stmt = dynamic_cast<const sql::AlterTableStmt*>(stmt)) {
        return executeAlterTable(alter_stmt);
    } else if (auto begin_stmt = dynamic_cast<const sql::BeginStmt*>(stmt)) {
        return executeBegin(begin_stmt);
    } else if (auto commit_stmt = dynamic_cast<const sql::CommitStmt*>(stmt)) {
        return executeCommit(commit_stmt);
    } else if (auto rollback_stmt = dynamic_cast<const sql::RollbackStmt*>(stmt)) {
        return executeRollback(rollback_stmt);
    } else if (auto create_index_stmt = dynamic_cast<const sql::CreateIndexStmt*>(stmt)) {
        return executeCreateIndex(create_index_stmt);
    } else if (auto drop_index_stmt = dynamic_cast<const sql::DropIndexStmt*>(stmt)) {
        return executeDropIndex(drop_index_stmt);
    } else if (auto analyze_stmt = dynamic_cast<const sql::AnalyzeStmt*>(stmt)) {
        return executeAnalyze(analyze_stmt);
    } else if (auto explain_stmt = dynamic_cast<const sql::ExplainStmt*>(stmt)) {
        return executeExplain(explain_stmt);
    } else if (auto create_view_stmt = dynamic_cast<const sql::CreateViewStmt*>(stmt)) {
        return executeCreateView(create_view_stmt);
    } else if (auto drop_view_stmt = dynamic_cast<const sql::DropViewStmt*>(stmt)) {
        return executeDropView(drop_view_stmt);
    }

    return ExecutionResult::error("Unsupported statement type");
}

ExecutionResult Executor::execute(const std::string &sql)
{
    LOG_INFO("Executing SQL: " << sql);

    // 使用 Parser 直接解析 SQL (Bison/Flex 集成)
    ExecutionResult result;
    try
    {
        sql::Parser parser(sql);
        auto ast = parser.parse();

        if (ast && ast->statement())
        {
            // 通过 AST 执行
            LOG_INFO("SQL parsed successfully, executing via AST");
            result = execute(*ast);
        }
        else
        {
            // 解析失败，直接报错
            LOG_ERROR("SQL parse failed: " + parser.error());
            result = ExecutionResult::error("SQL parse error: " + parser.error());
        }
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("SQL execution error: " << e.what());
        result = ExecutionResult::error("SQL execution error: " + std::string(e.what()));
    }

    return result;
}

// 评估 WHERE 条件
bool Executor::evaluateWhereCondition(const storage::Tuple& tuple, const sql::Expression* condition, const storage::Schema* schema) {
    if (!condition) return true;

    // 处理比较表达式
    if (auto comp_expr = dynamic_cast<const sql::ComparisonExpr*>(condition)) {
        auto left = comp_expr->left();
        auto right = comp_expr->right();

        // 获取左值（列引用）
        std::string left_val;
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            // 使用 toString() 获取字段的字符串表示（包含整数等）
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号（如果是字符串类型）
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        } else if (auto literal = dynamic_cast<const sql::LiteralExpr*>(left)) {
            left_val = literal->value();
            // 去除引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 获取右值（字面量）
        std::string right_val;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(right)) {
            right_val = literal->value();
            // 去除引号
            if (right_val.size() >= 2 && right_val.front() == '\'' && right_val.back() == '\'') {
                right_val = right_val.substr(1, right_val.size() - 2);
            }
        } else if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(right)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            right_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (right_val.size() >= 2 && right_val.front() == '\'' && right_val.back() == '\'') {
                right_val = right_val.substr(1, right_val.size() - 2);
            }
        }

        // 比较操作
        switch (comp_expr->op()) {
            case sql::OpType::EQ:
                return left_val == right_val;
            case sql::OpType::NE:
                return left_val != right_val;
            case sql::OpType::LT:
                try {
                    return std::stod(left_val) < std::stod(right_val);
                } catch (...) {
                    return left_val < right_val;
                }
            case sql::OpType::LE:
                try {
                    return std::stod(left_val) <= std::stod(right_val);
                } catch (...) {
                    return left_val <= right_val;
                }
            case sql::OpType::GT:
                try {
                    return std::stod(left_val) > std::stod(right_val);
                } catch (...) {
                    return left_val > right_val;
                }
            case sql::OpType::GE:
                try {
                    return std::stod(left_val) >= std::stod(right_val);
                } catch (...) {
                    return left_val >= right_val;
                }
            default:
                return false;
        }
    }

    // 处理逻辑表达式
    if (auto logic_expr = dynamic_cast<const sql::LogicalExpr*>(condition)) {
        auto left_result = evaluateWhereCondition(tuple, logic_expr->left(), schema);

        if (logic_expr->op() == sql::OpType::AND) {
            if (!left_result) return false;
            return evaluateWhereCondition(tuple, logic_expr->right(), schema);
        } else if (logic_expr->op() == sql::OpType::OR) {
            if (left_result) return true;
            return evaluateWhereCondition(tuple, logic_expr->right(), schema);
        } else if (logic_expr->op() == sql::OpType::NOT) {
            return !left_result;
        }
    }

    // 处理 IN 表达式
    if (auto in_expr = dynamic_cast<const sql::InExpr*>(condition)) {
        // 获取左值（列引用）
        std::string left_val;
        auto left = in_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 检查是否在值列表中
        bool found = false;
        for (const auto& val_expr : in_expr->values()) {
            std::string val;
            if (auto literal = dynamic_cast<const sql::LiteralExpr*>(val_expr.get())) {
                val = literal->value();
                // 去除引号
                if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                    val = val.substr(1, val.size() - 2);
                }
            }
            if (left_val == val) {
                found = true;
                break;
            }
        }

        return in_expr->isNot() ? !found : found;
    }

    // 处理 BETWEEN 表达式
    if (auto between_expr = dynamic_cast<const sql::BetweenExpr*>(condition)) {
        // 获取左值（列引用）
        std::string left_val;
        auto left = between_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 获取下界值
        std::string lower_val;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(between_expr->lower())) {
            lower_val = literal->value();
            if (lower_val.size() >= 2 && lower_val.front() == '\'' && lower_val.back() == '\'') {
                lower_val = lower_val.substr(1, lower_val.size() - 2);
            }
        }

        // 获取上界值
        std::string upper_val;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(between_expr->upper())) {
            upper_val = literal->value();
            if (upper_val.size() >= 2 && upper_val.front() == '\'' && upper_val.back() == '\'') {
                upper_val = upper_val.substr(1, upper_val.size() - 2);
            }
        }

        // 比较：left >= lower AND left <= upper
        bool in_range = false;
        try {
            double left_num = std::stod(left_val);
            double lower_num = std::stod(lower_val);
            double upper_num = std::stod(upper_val);
            in_range = (left_num >= lower_num && left_num <= upper_num);
        } catch (...) {
            // 字符串比较
            in_range = (left_val >= lower_val && left_val <= upper_val);
        }

        return between_expr->isNot() ? !in_range : in_range;
    }

    // 处理 LIKE 表达式
    if (auto like_expr = dynamic_cast<const sql::LikeExpr*>(condition)) {
        // 获取左值（列引用）
        std::string left_val;
        auto left = like_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 获取模式值
        std::string pattern;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(like_expr->pattern())) {
            pattern = literal->value();
            if (pattern.size() >= 2 && pattern.front() == '\'' && pattern.back() == '\'') {
                pattern = pattern.substr(1, pattern.size() - 2);
            }
        }

        // 简单的 LIKE 模式匹配（支持 % 和 _）
        bool matches = matchLikePattern(left_val, pattern);
        return like_expr->isNot() ? !matches : matches;
    }

    // 处理 IS NULL 表达式
    if (auto is_null_expr = dynamic_cast<const sql::IsNullExpr*>(condition)) {
        // 获取左值（列引用）
        bool is_null = false;
        auto left = is_null_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            // 获取字段并检查是否为空
            const auto& field = tuple.getField(col_idx);
            is_null = field.isNull();
        }

        return is_null_expr->isNot() ? !is_null : is_null;
    }

    return true;
}

// 格式化元组为字符串，支持选择特定列
std::string Executor::formatTuple(const storage::Tuple& tuple, const std::vector<std::unique_ptr<sql::Expression>>& select_list, const storage::Schema* schema) {
    // 如果没有指定列或者 select_list 为空，返回所有列
    if (select_list.empty()) {
        return tuple.toString();
    }

    // 检查是否是 * (所有列)
    bool is_select_all = false;
    for (const auto& expr : select_list) {
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr.get())) {
            if (col_ref->columnName() == "*") {
                is_select_all = true;
                break;
            }
        }
    }

    if (is_select_all) {
        return tuple.toString();
    }

    // 构建指定列的结果
    std::string result = "(";
    for (size_t i = 0; i < select_list.size(); ++i) {
        if (i > 0) result += ", ";

        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(select_list[i].get())) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx >= 0 && col_idx < static_cast<int>(tuple.getFieldCount())) {
                result += tuple.getField(col_idx).toString();
            } else {
                result += "NULL";
            }
        } else if (auto literal = dynamic_cast<const sql::LiteralExpr*>(select_list[i].get())) {
            result += literal->value();
        } else if (auto alias_expr = dynamic_cast<const sql::AliasExpr*>(select_list[i].get())) {
            // 处理别名表达式 (expr AS alias)
            storage::Field field = evaluateExpression(alias_expr->expression(), tuple, schema);
            result += field.toString();
        } else {
            // 处理其他表达式类型（算术运算、比较等）
            storage::Field field = evaluateExpression(select_list[i].get(), tuple, schema);
            result += field.toString();
        }
    }
    result += ")";
    return result;
}

ExecutionResult Executor::executeSelect(const sql::SelectStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("SELECT executed (simulated - no storage engine)");
    }

    std::string table_name = stmt ? stmt->fromTable() : "";

    // Phase 4: 检查是否是多表JOIN查询
    if (stmt && !stmt->joinTables().empty()) {
        return executeJoin(stmt);
    }

    // Phase 4: 使用优化器生成执行计划
    if (stmt && !table_name.empty() && optimizer_) {
        ExecutionPlan plan = optimizer_->optimize(stmt);
        // 可以根据执行计划选择扫描方式
    }

    // Phase 4: 检查是否可以使用索引扫描
    if (stmt && stmt->whereCondition() && !table_name.empty()) {
        // 尝试找到可以使用索引的列
        auto comp_expr = dynamic_cast<const sql::ComparisonExpr*>(stmt->whereCondition());
        if (comp_expr && comp_expr->op() == sql::OpType::EQ) {
            if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(comp_expr->left())) {
                std::string col_name = col_ref->columnName();

                // 检查该列是否有索引
                auto index = storage_engine_->getIndexManager()->getColumnIndex(table_name, col_name);
                if (index && comp_expr->right()) {
                    // 获取查询值
                    if (auto literal = dynamic_cast<const sql::LiteralExpr*>(comp_expr->right())) {
                        std::string val = literal->value();
                        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                            val = val.substr(1, val.size() - 2);
                        }

                        // 构建索引键
                        storage::IndexKey key;
                        try {
                            key = storage::IndexKey(std::stoi(val));
                        } catch (...) {
                            key = storage::IndexKey(val);
                        }

                        // 使用索引查找
                        auto tids = storage_engine_->indexLookup(table_name, col_name, key);

                        std::string result = "Table: " + table_name + " (Index Scan on " + col_name + ")\n";
                        result += "--------------------\n";

                        auto table_meta = storage_engine_->getTable(table_name);
                        int count = 0;
                        for (const auto& tid : tids) {
                            auto tuple = storage_engine_->get(table_name, tid);
                            result += formatTuple(tuple, stmt->selectList(), &table_meta->schema) + "\n";
                            count++;
                        }

                        result += "--------------------\n";
                        result += "Total rows: " + std::to_string(count);

                        return ExecutionResult::ok(result);
                    }
                }
            }
        }
    }

    // 原有的SELECT逻辑...
    if (table_name == "tn_class" || table_name == "tn_attribute") {
        // 查询系统表
        std::string result = "Table: " + table_name + "\n";
        result += "--------------------\n";

        auto iter = storage_engine_->scan(table_name);
        int count = 0;
        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            result += tuple.toString() + "\n";
            count++;
        }

        result += "--------------------\n";
        result += "Total rows: " + std::to_string(count);

        return ExecutionResult::ok(result);
    }

    // 查询用户表
    if (!table_name.empty()) {
        // 首先检查是否是系统视图查询
        if (g_system_view_manager && g_system_view_manager->isSystemViewQuery(table_name)) {
            // 获取要查询的列
            std::vector<std::string> columns;
            if (stmt) {
                const auto& select_list = stmt->selectList();
                for (size_t i = 0; i < select_list.size(); ++i) {
                    if (auto* col_ref = dynamic_cast<const sql::ColumnRefExpr*>(select_list[i].get())) {
                        columns.push_back(col_ref->columnName());
                    }
                }
            }

            // 查询系统视图
            auto view_result = g_system_view_manager->querySystemView(table_name, columns);
            if (!view_result.success) {
                return ExecutionResult::error(view_result.error_message);
            }

            // 应用 WHERE 条件过滤
            const sql::Expression* where_condition = stmt ? stmt->whereCondition() : nullptr;

            // 格式化结果
            std::string result = "Table: " + table_name + "\n";
            result += "--------------------\n";

            // 打印列名
            for (size_t i = 0; i < view_result.column_names.size(); ++i) {
                if (i > 0) result += "\t";
                result += view_result.column_names[i];
            }
            result += "\n";

            // 打印数据行
            int count = 0;
            for (const auto& row : view_result.rows) {
                // 如果有过滤条件，这里需要简单处理
                // 注意：完整的 WHERE 评估需要更复杂的实现
                bool skip = false;
                if (where_condition) {
                    // 简化处理：检查字符串匹配
                    // 对于系统视图，我们暂时不过滤或仅做简单过滤
                }
                if (skip) continue;

                for (size_t i = 0; i < row.size(); ++i) {
                    if (i > 0) result += "\t";
                    result += row[i];
                }
                result += "\n";
                count++;
            }

            result += "--------------------\n";
            result += "Total rows: " + std::to_string(count);

            return ExecutionResult::ok(result);
        }

        // Check if this is a user-defined view
        auto view_it = views_.find(table_name);
        if (view_it != views_.end()) {
            // This is a view - parse and execute the underlying SELECT
            const ViewMeta& view_meta = view_it->second;

            // Parse the view's SQL definition
            sql::Parser view_parser(view_meta.sql_definition);
            auto view_ast = view_parser.parse();

            if (!view_ast || !view_ast->statement()) {
                return ExecutionResult::error("Failed to parse view definition");
            }

            auto view_select = dynamic_cast<sql::SelectStmt*>(view_ast->statement());
            if (!view_select) {
                return ExecutionResult::error("View definition is not a SELECT statement");
            }

            // Get the base table from the view's SELECT
            std::string view_base_table = view_select->fromTable();
            if (view_base_table.empty()) {
                return ExecutionResult::error("View has no base table");
            }

            // Check if base table exists
            if (!storage_engine_->tableExists(view_base_table)) {
                return ExecutionResult::error("View base table does not exist: " + view_base_table);
            }

            auto table_meta = storage_engine_->getTable(view_base_table);
            if (!table_meta) {
                return ExecutionResult::error("Failed to get view base table metadata: " + view_base_table);
            }

            std::string result = "Table: " + table_name + " (view on " + view_base_table + ")\n";
            result += "--------------------\n";

            auto iter = storage_engine_->scan(view_base_table);
            int count = 0;

            // Get WHERE conditions from both the view query and the current query
            const sql::Expression* view_where = view_select->whereCondition();
            const sql::Expression* current_where = stmt ? stmt->whereCondition() : nullptr;

            while (iter.hasNext()) {
                auto tuple = iter.getNext();

                // Apply view's WHERE condition
                if (view_where && !evaluateWhereCondition(tuple, view_where, &table_meta->schema)) {
                    continue;
                }

                // Apply current query's WHERE condition
                if (current_where && !evaluateWhereCondition(tuple, current_where, &table_meta->schema)) {
                    continue;
                }

                // For view, use the view's select list
                result += formatTuple(tuple, view_select->selectList(), &table_meta->schema) + "\n";
                count++;
            }

            result += "--------------------\n";
            result += "Total rows: " + std::to_string(count);

            return ExecutionResult::ok(result);
        }

        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        std::string result = "Table: " + table_name + "\n";
        result += "--------------------\n";

        auto iter = storage_engine_->scan(table_name);
        int count = 0;

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt ? stmt->whereCondition() : nullptr;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();

            // 应用 WHERE 条件过滤
            if (where_condition && !evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 格式化输出，支持选择特定列
            if (stmt) {
                result += formatTuple(tuple, stmt->selectList(), &table_meta->schema) + "\n";
            } else {
                result += tuple.toString() + "\n";
            }
            count++;
        }

        result += "--------------------\n";
        result += "Total rows: " + std::to_string(count);

        return ExecutionResult::ok(result);
    }

    return ExecutionResult::ok("SELECT executed");
}

ExecutionResult Executor::executeInsert(const sql::InsertStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("INSERT executed (simulated - no storage engine)");
    }

    // 阶段二：实际插入数据
    if (stmt) {
        std::string table_name = stmt->table();

        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取或开始事务
        storage::Transaction* txn = current_txn_;
        bool auto_txn = false;
        if (!txn) {
            txn = storage_engine_->beginTransaction();
            current_txn_ = txn;  // 设置当前事务
            auto_txn = true;
        }

        // 获取排他锁（写锁）
        if (!acquireTableLock(table_name, storage::LockType::EXCLUSIVE)) {
            if (auto_txn) {
                storage_engine_->abortTransaction(txn);
                current_txn_ = nullptr;
            }
            return ExecutionResult::error("Failed to acquire lock on table: " + table_name);
        }

        const auto& columns = stmt->columns();
        int64_t inserted_count = 0;

        // 处理多行插入
        if (stmt->isMultiRow()) {
            const auto& multi_values = stmt->multiRowValues();
            for (const auto& row_values : multi_values) {
                // 构建元组
                storage::Tuple tuple(&table_meta->schema);

                for (size_t i = 0; i < row_values.size(); ++i) {
                    const auto* literal = dynamic_cast<const sql::LiteralExpr*>(row_values[i].get());
                    if (literal) {
                        std::string val = literal->value();

                        // 确定列索引
                        size_t col_idx = i;
                        if (i < columns.size()) {
                            col_idx = table_meta->schema.findColumnIndex(columns[i]);
                            if (col_idx == static_cast<size_t>(-1)) {
                                releaseTableLock(table_name);
                                if (auto_txn) {
                                    storage_engine_->abortTransaction(txn);
                                    current_txn_ = nullptr;
                                }
                                return ExecutionResult::error("Unknown column: " + columns[i]);
                            }
                        }

                        // 根据列类型创建字段
                        const auto& col_def = table_meta->schema.getColumn(col_idx);
                        storage::Field field;

                        if (val == "NULL") {
                            // NULL 值处理 - 使用默认构造函数创建 NULL 字段
                            field = storage::Field();
                        } else {
                            switch (col_def.type) {
                                case storage::DataType::INT32:
                                    field = storage::Field(static_cast<int32_t>(std::stoll(val)));
                                    break;
                                case storage::DataType::INT64:
                                    field = storage::Field(static_cast<int64_t>(std::stoll(val)));
                                    break;
                                case storage::DataType::FLOAT:
                                    field = storage::Field(static_cast<float>(std::stod(val)));
                                    break;
                                case storage::DataType::DOUBLE:
                                    field = storage::Field(static_cast<double>(std::stod(val)));
                                    break;
                                case storage::DataType::DECIMAL:
                                    field = storage::Field(std::stod(val));
                                    break;
                                case storage::DataType::BOOL:
                                    field = storage::Field(val == "true" || val == "1");
                                    break;
                                case storage::DataType::CHAR:
                                case storage::DataType::VARCHAR:
                                    // 去除引号
                                    if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                                        val = val.substr(1, val.size() - 2);
                                    }
                                    field = storage::Field(val, col_def.type);
                                    break;
                                default:
                                    field = storage::Field(val, storage::DataType::VARCHAR);
                                    break;
                            }
                        }

                        tuple.setField(col_idx, field);
                    }
                }

                // 插入数据
                auto tid = storage_engine_->insert(table_name, tuple);

                if (!tid.isValid()) {
                    releaseTableLock(table_name);
                    if (auto_txn) {
                        storage_engine_->abortTransaction(txn);
                        current_txn_ = nullptr;
                    }
                    return ExecutionResult::error("Failed to insert row");
                }

                // 记录插入（用于回滚）
                if (txn) {
                    txn->addInsertRecord(table_name, tid);
                }

                inserted_count++;
            }
        } else {
            // 单行插入（原有逻辑）
            const auto& values = stmt->values();
            storage::Tuple tuple(&table_meta->schema);

            for (size_t i = 0; i < values.size(); ++i) {
                const auto* literal = dynamic_cast<const sql::LiteralExpr*>(values[i].get());
                if (literal) {
                    std::string val = literal->value();

                    // 确定列索引
                    size_t col_idx = i;
                    if (i < columns.size()) {
                        col_idx = table_meta->schema.findColumnIndex(columns[i]);
                        if (col_idx == static_cast<size_t>(-1)) {
                            releaseTableLock(table_name);
                            if (auto_txn) {
                                storage_engine_->abortTransaction(txn);
                                current_txn_ = nullptr;
                            }
                            return ExecutionResult::error("Unknown column: " + columns[i]);
                        }
                    }

                    // 根据列类型创建字段
                    const auto& col_def = table_meta->schema.getColumn(col_idx);
                    storage::Field field;

                    if (val == "NULL") {
                        field = storage::Field();
                    } else {
                        switch (col_def.type) {
                            case storage::DataType::INT32:
                                field = storage::Field(static_cast<int32_t>(std::stoll(val)));
                                break;
                            case storage::DataType::INT64:
                                field = storage::Field(static_cast<int64_t>(std::stoll(val)));
                                break;
                            case storage::DataType::FLOAT:
                                field = storage::Field(static_cast<float>(std::stod(val)));
                                break;
                            case storage::DataType::DOUBLE:
                                field = storage::Field(static_cast<double>(std::stod(val)));
                                break;
                            case storage::DataType::DECIMAL:
                                field = storage::Field(std::stod(val));
                                break;
                            case storage::DataType::BOOL:
                                field = storage::Field(val == "true" || val == "1");
                                break;
                            case storage::DataType::CHAR:
                            case storage::DataType::VARCHAR:
                                // 去除引号
                                if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                                    val = val.substr(1, val.size() - 2);
                                }
                                field = storage::Field(val, col_def.type);
                                break;
                            default:
                                field = storage::Field(val, storage::DataType::VARCHAR);
                                break;
                        }
                    }

                    tuple.setField(col_idx, field);
                }
            }

            // 插入数据
            auto tid = storage_engine_->insert(table_name, tuple);

            // 释放锁
            releaseTableLock(table_name);

            if (!tid.isValid()) {
                if (auto_txn) {
                    storage_engine_->abortTransaction(txn);
                    current_txn_ = nullptr;
                }
                return ExecutionResult::error("Failed to insert row");
            }

            // 记录插入（用于回滚）
            if (txn) {
                txn->addInsertRecord(table_name, tid);
            }

            inserted_count = 1;
        }

        // 释放锁
        releaseTableLock(table_name);

        // 如果没有显式事务，自动提交
        if (auto_txn) {
            if (!storage_engine_->commitTransaction(txn)) {
                current_txn_ = nullptr;
                return ExecutionResult::error("Failed to commit transaction");
            }
            current_txn_ = nullptr;
        }

        return ExecutionResult::ok("INSERT " + std::to_string(inserted_count));
    }

    return ExecutionResult::ok("INSERT executed");
}

ExecutionResult Executor::executeCreateTable(const sql::CreateTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("CREATE TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否已存在
        if (storage_engine_->tableExists(table_name)) {
            if (stmt->ifNotExists()) {
                return ExecutionResult::ok("Table already exists, skipped");
            }
            return ExecutionResult::error("Table already exists: " + table_name);
        }

        // 构建Schema
        storage::Schema schema;
        for (const auto& col : stmt->columns()) {
            storage::DataType type = parseDataType(col.type);
            uint32_t length = 0;

            // 解析长度（如 VARCHAR(32)）
            size_t paren_pos = col.type.find('(');
            if (paren_pos != std::string::npos) {
                size_t close_pos = col.type.find(')', paren_pos);
                if (close_pos != std::string::npos) {
                    std::string len_str = col.type.substr(paren_pos + 1, close_pos - paren_pos - 1);
                    length = static_cast<uint32_t>(std::stoul(len_str));
                }
            }

            // 构建完整的 ColumnDef，包含约束信息
            storage::ColumnDef col_def;
            col_def.name = col.name;
            col_def.type = type;
            col_def.length = length;
            col_def.is_nullable = !col.is_not_null;  // NOT NULL 约束
            col_def.is_primary_key = col.is_primary_key;  // PRIMARY KEY 约束

            schema.addColumn(col_def);

            // 如果有 PRIMARY KEY 约束，自动创建索引
            if (col.is_primary_key) {
                LOG_INFO("Column '" << col.name << "' is PRIMARY KEY");
                // 存储引擎会在 createTable 后自动为主键创建索引
            }
        }

        // 创建表
        if (storage_engine_->createTable(table_name, schema)) {
            return ExecutionResult::ok("CREATE TABLE " + table_name);
        } else {
            return ExecutionResult::error("Failed to create table: " + table_name);
        }
    }

    return ExecutionResult::ok("CREATE TABLE executed");
}

ExecutionResult Executor::executeDropTable(const sql::DropTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("DROP TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        // 删除表
        if (storage_engine_->dropTable(table_name)) {
            return ExecutionResult::ok("DROP TABLE " + table_name);
        } else {
            return ExecutionResult::error("Failed to drop table: " + table_name);
        }
    }

    return ExecutionResult::ok("DROP TABLE executed");
}

ExecutionResult Executor::executeUpdate(const sql::UpdateStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("UPDATE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取或开始事务
        storage::Transaction* txn = current_txn_;
        bool auto_txn = false;
        if (!txn) {
            txn = storage_engine_->beginTransaction();
            current_txn_ = txn;
            auto_txn = true;
        }

        // 获取排他锁（写锁）
        if (!acquireTableLock(table_name, storage::LockType::EXCLUSIVE)) {
            if (auto_txn) {
                storage_engine_->abortTransaction(txn);
                current_txn_ = nullptr;
            }
            return ExecutionResult::error("Failed to acquire lock on table: " + table_name);
        }

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt->whereCondition();

        // 获取赋值列表
        const auto& assignments = stmt->assignments();

        // 遍历表中的所有行，收集需要更新的TID和更新后的元组
        auto iter = storage_engine_->scan(table_name);
        int update_count = 0;

        // 先收集需要更新的记录，避免在迭代过程中修改表导致迭代器失效
        struct UpdateRecord {
            storage::TID tid;
            storage::Tuple updated_tuple;
        };
        std::vector<UpdateRecord> records_to_update;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            storage::TID tid = iter.getCurrentTID();

            // 应用 WHERE 条件过滤
            if (where_condition && !evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 创建更新后的元组
            storage::Tuple updated_tuple(&table_meta->schema);

            // 先复制原元组的所有字段
            for (size_t i = 0; i < tuple.getFieldCount(); ++i) {
                updated_tuple.addField(tuple.getField(i));
            }

            // 应用赋值
            for (const auto& assignment : assignments) {
                const std::string& col_name = assignment.first;
                const sql::Expression* value_expr = assignment.second.get();

                int col_idx = table_meta->schema.findColumnIndex(col_name);
                if (col_idx < 0) {
                    releaseTableLock(table_name);
                    if (auto_txn) {
                        storage_engine_->abortTransaction(txn);
                        current_txn_ = nullptr;
                    }
                    return ExecutionResult::error("Unknown column: " + col_name);
                }

                // 使用表达式求值来支持算术运算和其他复杂表达式
                storage::Field field = evaluateExpression(value_expr, tuple, &table_meta->schema);

                // Debug output
                LOG_INFO("Updating column " << col_name << " at index " << col_idx
                         << " with value " << field.toString() << " (type=" << static_cast<int>(field.getType()) << ")");

                updated_tuple.setField(col_idx, field);
            }

            // 收集记录
            records_to_update.push_back({tid, std::move(updated_tuple)});
        }

        // 现在执行更新操作（在迭代完成后）
        for (auto& record : records_to_update) {
            // 使用存储引擎的 update 方法
            if (!storage_engine_->update(table_name, record.tid, record.updated_tuple)) {
                releaseTableLock(table_name);
                if (auto_txn) {
                    storage_engine_->abortTransaction(txn);
                    current_txn_ = nullptr;
                }
                return ExecutionResult::error("Failed to update row");
            }

            update_count++;
        }

        // 释放锁
        releaseTableLock(table_name);

        // 如果没有显式事务，自动提交
        if (auto_txn) {
            if (!storage_engine_->commitTransaction(txn)) {
                current_txn_ = nullptr;
                return ExecutionResult::error("Failed to commit update");
            }
            current_txn_ = nullptr;
        }

        return ExecutionResult::ok("UPDATE " + std::to_string(update_count) + " row(s) in table " + table_name);
    }

    return ExecutionResult::ok("UPDATE executed");
}

ExecutionResult Executor::executeDelete(const sql::DeleteStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("DELETE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取或开始事务
        storage::Transaction* txn = current_txn_;
        bool auto_txn = false;
        if (!txn) {
            txn = storage_engine_->beginTransaction();
            current_txn_ = txn;
            auto_txn = true;
        }

        // 获取排他锁（写锁）
        if (!acquireTableLock(table_name, storage::LockType::EXCLUSIVE)) {
            if (auto_txn) {
                storage_engine_->abortTransaction(txn);
                current_txn_ = nullptr;
            }
            return ExecutionResult::error("Failed to acquire lock on table: " + table_name);
        }

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt->whereCondition();

        // 遍历表中的所有行
        auto iter = storage_engine_->scan(table_name);
        int delete_count = 0;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            storage::TID tid = iter.getCurrentTID();

            // 应用 WHERE 条件过滤
            if (where_condition && !evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 记录删除（用于回滚）
            if (txn) {
                txn->addDeleteRecord(table_name, tuple, tid);
            }

            // 删除元组
            if (storage_engine_->remove(table_name, tid)) {
                delete_count++;
            }
        }

        // 释放锁
        releaseTableLock(table_name);

        // 如果没有显式事务，自动提交
        if (auto_txn) {
            if (!storage_engine_->commitTransaction(txn)) {
                current_txn_ = nullptr;
                return ExecutionResult::error("Failed to commit delete");
            }
            current_txn_ = nullptr;
        }

        return ExecutionResult::ok("DELETE " + std::to_string(delete_count) + " row(s) from table " + table_name);
    }

    return ExecutionResult::ok("DELETE executed");
}

ExecutionResult Executor::executeAlterTable(const sql::AlterTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("ALTER TABLE executed (simulated - no storage engine)");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid ALTER TABLE statement");
    }

    std::string table_name = stmt->table();

    // 检查表是否存在
    if (!storage_engine_->tableExists(table_name)) {
        return ExecutionResult::error("Table does not exist: " + table_name);
    }

    // 根据动作类型执行不同的操作
    switch (stmt->action()) {
        case sql::AlterTableStmt::ActionType::ADD_COLUMN: {
            // 添加列
            storage::ColumnDef col_def(
                stmt->columnName(),
                parseDataType(stmt->columnType()),
                parseTypeLength(stmt->columnType())
            );

            if (storage_engine_->addColumn(table_name, col_def)) {
                return ExecutionResult::ok("ALTER TABLE ADD COLUMN " + stmt->columnName() +
                                           " " + stmt->columnType() + " ON " + table_name);
            } else {
                return ExecutionResult::error("Failed to add column " + stmt->columnName() +
                                              " to table " + table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::DROP_COLUMN: {
            // 删除列
            if (storage_engine_->dropColumn(table_name, stmt->columnName())) {
                return ExecutionResult::ok("ALTER TABLE DROP COLUMN " + stmt->columnName() +
                                           " FROM " + table_name);
            } else {
                return ExecutionResult::error("Failed to drop column " + stmt->columnName() +
                                              " from table " + table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::MODIFY_COLUMN: {
            // 修改列
            storage::ColumnDef col_def(
                stmt->columnName(),
                parseDataType(stmt->columnType()),
                parseTypeLength(stmt->columnType())
            );

            if (storage_engine_->modifyColumn(table_name, stmt->columnName(), col_def)) {
                return ExecutionResult::ok("ALTER TABLE MODIFY COLUMN " + stmt->columnName() +
                                           " " + stmt->columnType() + " ON " + table_name);
            } else {
                return ExecutionResult::error("Failed to modify column " + stmt->columnName() +
                                              " in table " + table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::RENAME_TABLE: {
            // 重命名表
            std::string new_table_name = stmt->newTableName();
            if (storage_engine_->renameTable(table_name, new_table_name)) {
                return ExecutionResult::ok("ALTER TABLE RENAME " + table_name + " TO " + new_table_name);
            } else {
                return ExecutionResult::error("Failed to rename table " + table_name + " to " + new_table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::RENAME_COLUMN: {
            // 重命名列
            std::string old_col_name = stmt->columnName();
            std::string new_col_name = stmt->newColumnName();
            if (storage_engine_->renameColumn(table_name, old_col_name, new_col_name)) {
                return ExecutionResult::ok("ALTER TABLE RENAME COLUMN " + old_col_name + " TO " + new_col_name);
            } else {
                return ExecutionResult::error("Failed to rename column " + old_col_name + " to " + new_col_name);
            }
        }

        default:
            return ExecutionResult::error("Unknown ALTER TABLE action");
    }
}

storage::DataType Executor::parseDataType(const std::string& type_str) {
    std::string upper_type = type_str;
    for (auto& c : upper_type) {
        c = std::toupper(c);
    }

    // 提取基本类型（去掉括号部分）
    size_t paren_pos = upper_type.find('(');
    std::string base_type = (paren_pos != std::string::npos) ?
                            upper_type.substr(0, paren_pos) : upper_type;

    if (base_type == "INT" || base_type == "INTEGER") {
        return storage::DataType::INT32;
    } else if (base_type == "BIGINT") {
        return storage::DataType::INT64;
    } else if (base_type == "FLOAT" || base_type == "REAL") {
        return storage::DataType::FLOAT;
    } else if (base_type == "DOUBLE") {
        return storage::DataType::DOUBLE;
    } else if (base_type == "DECIMAL" || base_type == "NUMERIC") {
        return storage::DataType::DECIMAL;
    } else if (base_type == "BOOL" || base_type == "BOOLEAN") {
        return storage::DataType::BOOL;
    } else if (base_type == "CHAR") {
        return storage::DataType::CHAR;
    } else if (base_type == "VARCHAR") {
        return storage::DataType::VARCHAR;
    }

    // 默认类型
    return storage::DataType::VARCHAR;
}

// 解析类型长度（如 VARCHAR(32) 返回 32）
uint32_t Executor::parseTypeLength(const std::string& type_str) {
    size_t paren_pos = type_str.find('(');
    if (paren_pos != std::string::npos) {
        size_t close_pos = type_str.find(')', paren_pos);
        if (close_pos != std::string::npos) {
            std::string len_str = type_str.substr(paren_pos + 1, close_pos - paren_pos - 1);
            try {
                return static_cast<uint32_t>(std::stoul(len_str));
            } catch (...) {
                return 0;
            }
        }
    }
    // 对于没有长度指定的类型，返回默认值
    return 0;
}

// 表达式求值（阶段三新增：支持算术运算）
storage::Field Executor::evaluateExpression(const sql::Expression* expr, const storage::Tuple& tuple, const storage::Schema* schema) {
    if (!expr) {
        return storage::Field();
    }

    // 处理字面量
    if (auto literal = dynamic_cast<const sql::LiteralExpr*>(expr)) {
        std::string val = literal->value();
        // 去除引号
        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
            val = val.substr(1, val.size() - 2);
        }
        // 尝试解析为数字
        try {
            // 先尝试整数
            return storage::Field(static_cast<int64_t>(std::stoll(val)));
        } catch (...) {
            // 再尝试浮点数
            try {
                return storage::Field(std::stod(val));
            } catch (...) {
                return storage::Field(val, storage::DataType::VARCHAR);
            }
        }
    }

    // 处理列引用
    if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr)) {
        int col_idx = schema->findColumnIndex(col_ref->columnName());
        if (col_idx >= 0) {
            return tuple.getField(col_idx);
        }
        return storage::Field();
    }

    // 处理二元操作表达式
    if (auto binary_expr = dynamic_cast<const sql::BinaryOpExpr*>(expr)) {
        auto left_val = evaluateExpression(binary_expr->left(), tuple, schema);
        auto right_val = evaluateExpression(binary_expr->right(), tuple, schema);

        // 检查是否有浮点类型
        bool has_float = (left_val.getType() == storage::DataType::FLOAT ||
                          left_val.getType() == storage::DataType::DOUBLE ||
                          left_val.getType() == storage::DataType::DECIMAL ||
                          right_val.getType() == storage::DataType::FLOAT ||
                          right_val.getType() == storage::DataType::DOUBLE ||
                          right_val.getType() == storage::DataType::DECIMAL);

        if (has_float) {
            // 使用浮点数运算
            double left_num = 0.0, right_num = 0.0;
            if (!left_val.isNull()) {
                if (left_val.getType() == storage::DataType::FLOAT ||
                    left_val.getType() == storage::DataType::DOUBLE ||
                    left_val.getType() == storage::DataType::DECIMAL) {
                    left_num = left_val.getDouble();
                } else {
                    try {
                        left_num = std::stod(left_val.toString());
                    } catch (...) {
                        left_num = static_cast<double>(left_val.getInt64());
                    }
                }
            }
            if (!right_val.isNull()) {
                if (right_val.getType() == storage::DataType::FLOAT ||
                    right_val.getType() == storage::DataType::DOUBLE ||
                    right_val.getType() == storage::DataType::DECIMAL) {
                    right_num = right_val.getDouble();
                } else {
                    try {
                        right_num = std::stod(right_val.toString());
                    } catch (...) {
                        right_num = static_cast<double>(right_val.getInt64());
                    }
                }
            }

            switch (binary_expr->op()) {
                case sql::OpType::ADD:
                    return storage::Field(left_num + right_num);
                case sql::OpType::SUB:
                    return storage::Field(left_num - right_num);
                case sql::OpType::MUL:
                    return storage::Field(left_num * right_num);
                case sql::OpType::DIV:
                    if (right_num != 0.0) {
                        return storage::Field(left_num / right_num);
                    }
                    return storage::Field(0.0);
                default:
                    return storage::Field();
            }
        } else {
            // 使用整数运算
            int64_t left_num = 0, right_num = 0;
            try {
                left_num = std::stoll(left_val.toString());
            } catch (...) {
                left_num = left_val.getInt64();
            }
            try {
                right_num = std::stoll(right_val.toString());
            } catch (...) {
                right_num = right_val.getInt64();
            }

            switch (binary_expr->op()) {
                case sql::OpType::ADD:
                    return storage::Field(left_num + right_num);
                case sql::OpType::SUB:
                    return storage::Field(left_num - right_num);
                case sql::OpType::MUL:
                    return storage::Field(left_num * right_num);
                case sql::OpType::DIV:
                    if (right_num != 0) {
                        return storage::Field(left_num / right_num);
                    }
                    return storage::Field(static_cast<int64_t>(0));
                default:
                    return storage::Field();
            }
        }
    }

    return storage::Field();
}

// 事务执行方法（阶段三新增）
ExecutionResult Executor::executeBegin(const sql::BeginStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (current_txn_) {
        return ExecutionResult::error("Transaction already in progress");
    }

    current_txn_ = storage_engine_->beginTransaction();
    if (!current_txn_) {
        return ExecutionResult::error("Failed to begin transaction");
    }

    LOG_INFO("Transaction started: " << current_txn_->getId());
    return ExecutionResult::ok("BEGIN");
}

ExecutionResult Executor::executeCommit(const sql::CommitStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!current_txn_) {
        return ExecutionResult::error("No active transaction");
    }

    if (storage_engine_->commitTransaction(current_txn_)) {
        LOG_INFO("Transaction committed: " << current_txn_->getId());
        current_txn_ = nullptr;
        return ExecutionResult::ok("COMMIT");
    }

    return ExecutionResult::error("Failed to commit transaction");
}

ExecutionResult Executor::executeRollback(const sql::RollbackStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!current_txn_) {
        return ExecutionResult::error("No active transaction");
    }

    if (storage_engine_->abortTransaction(current_txn_)) {
        LOG_INFO("Transaction rolled back: " << current_txn_->getId());
        current_txn_ = nullptr;
        return ExecutionResult::ok("ROLLBACK");
    }

    return ExecutionResult::error("Failed to rollback transaction");
}

// 锁管理辅助方法（阶段三新增）
bool Executor::acquireTableLock(const std::string& table_name, storage::LockType lock_type) {
    if (!storage_engine_) {
        return false;
    }

    // 如果没有活跃事务，无法获取锁
    if (!current_txn_) {
        LOG_ERROR("Cannot acquire lock without active transaction");
        return false;
    }

    return storage_engine_->lockTable(current_txn_, table_name, lock_type);
}

bool Executor::releaseTableLock(const std::string& table_name) {
    if (!storage_engine_ || !current_txn_) {
        return false;
    }

    return storage_engine_->unlockTable(current_txn_, table_name);
}

// Phase 4: CREATE INDEX
ExecutionResult Executor::executeCreateIndex(const sql::CreateIndexStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid CREATE INDEX statement");
    }

    std::string index_name = stmt->indexName();
    std::string table_name = stmt->tableName();
    std::string column_name = stmt->columnName();
    bool is_unique = stmt->isUnique();

    // 检查表是否存在
    if (!storage_engine_->tableExists(table_name)) {
        return ExecutionResult::error("Table does not exist: " + table_name);
    }

    // 获取表元数据，检查列是否存在
    auto table_meta = storage_engine_->getTable(table_name);
    if (!table_meta) {
        return ExecutionResult::error("Failed to get table metadata: " + table_name);
    }

    int col_idx = table_meta->schema.findColumnIndex(column_name);
    if (col_idx < 0) {
        return ExecutionResult::error("Column does not exist: " + column_name);
    }

    // 创建索引
    if (storage_engine_->createIndex(index_name, table_name, column_name, is_unique)) {
        return ExecutionResult::ok("CREATE INDEX " + index_name + " ON " + table_name +
                                   "(" + column_name + ")");
    }

    return ExecutionResult::error("Failed to create index: " + index_name);
}

// Phase 4: DROP INDEX
ExecutionResult Executor::executeDropIndex(const sql::DropIndexStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid DROP INDEX statement");
    }

    std::string index_name = stmt->indexName();

    if (storage_engine_->getIndex(index_name)) {
        if (storage_engine_->dropIndex(index_name)) {
            return ExecutionResult::ok("DROP INDEX " + index_name);
        }
        return ExecutionResult::error("Failed to drop index: " + index_name);
    }

    return ExecutionResult::error("Index does not exist: " + index_name);
}

// Phase 4: ANALYZE
ExecutionResult Executor::executeAnalyze(const sql::AnalyzeStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid ANALYZE statement");
    }

    std::string table_name = stmt->tableName();

    if (!storage_engine_->tableExists(table_name)) {
        return ExecutionResult::error("Table does not exist: " + table_name);
    }

    if (storage_engine_->analyzeTable(table_name)) {
        return ExecutionResult::ok("ANALYZE " + table_name);
    }

    return ExecutionResult::error("Failed to analyze table: " + table_name);
}

// Phase 4: EXPLAIN
ExecutionResult Executor::executeExplain(const sql::ExplainStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt || !stmt->innerStatement()) {
        return ExecutionResult::error("Invalid EXPLAIN statement");
    }

    // 检查是否是SELECT语句
    if (auto select_stmt = dynamic_cast<const sql::SelectStmt*>(stmt->innerStatement())) {
        if (!optimizer_) {
            optimizer_ = std::make_unique<QueryOptimizer>(
                storage_engine_->getStatisticsManager(),
                storage_engine_->getIndexManager());
        }

        ExecutionPlan plan = optimizer_->optimize(select_stmt);
        return ExecutionResult::ok("\n" + plan.toString());
    }

    return ExecutionResult::ok("EXPLAIN: Query plan not available for this statement type");
}

// CREATE VIEW
ExecutionResult Executor::executeCreateView(const sql::CreateViewStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid CREATE VIEW statement");
    }

    std::string view_name = stmt->viewName();

    // Check if view or table already exists
    if (views_.count(view_name) > 0 || storage_engine_->tableExists(view_name)) {
        return ExecutionResult::error("View or table already exists: " + view_name);
    }

    // Get the underlying SELECT statement
    const sql::SelectStmt* select_stmt = stmt->selectStmt();
    if (!select_stmt) {
        return ExecutionResult::error("VIEW must have a SELECT statement");
    }

    // Validate the base table exists
    std::string base_table = select_stmt->fromTable();
    if (!base_table.empty() && !storage_engine_->tableExists(base_table)) {
        return ExecutionResult::error("Base table does not exist: " + base_table);
    }

    // Store view metadata
    ViewMeta meta;
    meta.view_name = view_name;
    meta.sql_definition = select_stmt->toString();

    views_[view_name] = std::move(meta);

    LOG_INFO("Created view: " << view_name);
    return ExecutionResult::ok("CREATE VIEW " + view_name);
}

// DROP VIEW
ExecutionResult Executor::executeDropView(const sql::DropViewStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid DROP VIEW statement");
    }

    std::string view_name = stmt->viewName();

    // Check if view exists
    auto it = views_.find(view_name);
    if (it == views_.end()) {
        if (stmt->ifExists()) {
            return ExecutionResult::ok("View does not exist: " + view_name);
        }
        return ExecutionResult::error("View does not exist: " + view_name);
    }

    // Drop the view
    views_.erase(it);
    LOG_INFO("Dropped view: " << view_name);

    return ExecutionResult::ok("DROP VIEW " + view_name);
}

// Phase 4: JOIN execution
ExecutionResult Executor::executeJoin(const sql::SelectStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    // 获取所有表
    auto tables = stmt->getAllTables();
    if (tables.size() < 2) {
        return ExecutionResult::error("JOIN requires at least 2 tables");
    }

    // 检查所有表是否存在
    for (const auto& table_name : tables) {
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }
    }

    // 使用优化器生成执行计划
    if (!optimizer_) {
        optimizer_ = std::make_unique<QueryOptimizer>(
            storage_engine_->getStatisticsManager(),
            storage_engine_->getIndexManager());
    }

    ExecutionPlan plan = optimizer_->optimize(stmt);

    // 执行Nested Loop Join（简化实现）
    std::string result = "JOIN Result:\n";
    result += "--------------------\n";

    // 获取表元数据
    auto left_table = storage_engine_->getTable(tables[0]);
    auto right_table = storage_engine_->getTable(tables[1]);

    // 执行Nested Loop Join
    auto left_iter = storage_engine_->scan(tables[0]);
    int row_count = 0;

    while (left_iter.hasNext()) {
        auto left_tuple = left_iter.getNext();

        auto right_iter = storage_engine_->scan(tables[1]);
        while (right_iter.hasNext()) {
            auto right_tuple = right_iter.getNext();

            // 检查JOIN条件（简化：使用WHERE条件）
            std::vector<storage::Tuple> tuples = {left_tuple, right_tuple};
            std::vector<storage::Schema*> schemas = {&left_table->schema, &right_table->schema};

            if (stmt->whereCondition()) {
                if (!evaluateJoinWhereCondition(tuples, schemas, stmt->whereCondition())) {
                    continue;
                }
            }

            // 格式化输出
            result += formatJoinTuple(tuples, schemas, stmt->selectList()) + "\n";
            row_count++;
        }
    }

    result += "--------------------\n";
    result += "Total rows: " + std::to_string(row_count);

    return ExecutionResult::ok(result);
}

// Phase 4: JOIN WHERE条件评估
bool Executor::evaluateJoinWhereCondition(const std::vector<storage::Tuple>& tuples,
                                          const std::vector<storage::Schema*>& schemas,
                                          const sql::Expression* condition) {
    if (!condition) return true;

    // 处理比较表达式
    if (auto comp_expr = dynamic_cast<const sql::ComparisonExpr*>(condition)) {
        auto left = comp_expr->left();
        auto right = comp_expr->right();

        // 获取左值
        storage::Field left_field = evaluateJoinExpression(left, tuples, schemas);
        storage::Field right_field = evaluateJoinExpression(right, tuples, schemas);

        std::string left_val = left_field.toString();
        std::string right_val = right_field.toString();

        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }
        if (right_val.size() >= 2 && right_val.front() == '\'' && right_val.back() == '\'') {
            right_val = right_val.substr(1, right_val.size() - 2);
        }

        // 比较操作
        switch (comp_expr->op()) {
            case sql::OpType::EQ:
                return left_val == right_val;
            case sql::OpType::NE:
                return left_val != right_val;
            case sql::OpType::LT:
                try {
                    return std::stod(left_val) < std::stod(right_val);
                } catch (...) {
                    return left_val < right_val;
                }
            case sql::OpType::LE:
                try {
                    return std::stod(left_val) <= std::stod(right_val);
                } catch (...) {
                    return left_val <= right_val;
                }
            case sql::OpType::GT:
                try {
                    return std::stod(left_val) > std::stod(right_val);
                } catch (...) {
                    return left_val > right_val;
                }
            case sql::OpType::GE:
                try {
                    return std::stod(left_val) >= std::stod(right_val);
                } catch (...) {
                    return left_val >= right_val;
                }
            default:
                return false;
        }
    }

    // 处理逻辑表达式
    if (auto logic_expr = dynamic_cast<const sql::LogicalExpr*>(condition)) {
        auto left_result = evaluateJoinWhereCondition(tuples, schemas, logic_expr->left());

        if (logic_expr->op() == sql::OpType::AND) {
            if (!left_result) return false;
            return evaluateJoinWhereCondition(tuples, schemas, logic_expr->right());
        } else if (logic_expr->op() == sql::OpType::OR) {
            if (left_result) return true;
            return evaluateJoinWhereCondition(tuples, schemas, logic_expr->right());
        } else if (logic_expr->op() == sql::OpType::NOT) {
            return !left_result;
        }
    }

    // 处理 IN 表达式
    if (auto in_expr = dynamic_cast<const sql::InExpr*>(condition)) {
        // 获取左值
        storage::Field left_field = evaluateJoinExpression(in_expr->left(), tuples, schemas);
        std::string left_val = left_field.toString();
        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }

        // 检查是否在值列表中
        bool found = false;
        for (const auto& val_expr : in_expr->values()) {
            storage::Field val_field = evaluateJoinExpression(val_expr.get(), tuples, schemas);
            std::string val = val_field.toString();
            // 去除引号
            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                val = val.substr(1, val.size() - 2);
            }
            if (left_val == val) {
                found = true;
                break;
            }
        }

        return in_expr->isNot() ? !found : found;
    }

    // 处理 BETWEEN 表达式
    if (auto between_expr = dynamic_cast<const sql::BetweenExpr*>(condition)) {
        // 获取左值
        storage::Field left_field = evaluateJoinExpression(between_expr->left(), tuples, schemas);
        std::string left_val = left_field.toString();
        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }

        // 获取下界和上界值
        storage::Field lower_field = evaluateJoinExpression(between_expr->lower(), tuples, schemas);
        storage::Field upper_field = evaluateJoinExpression(between_expr->upper(), tuples, schemas);
        std::string lower_val = lower_field.toString();
        std::string upper_val = upper_field.toString();
        // 去除引号
        if (lower_val.size() >= 2 && lower_val.front() == '\'' && lower_val.back() == '\'') {
            lower_val = lower_val.substr(1, lower_val.size() - 2);
        }
        if (upper_val.size() >= 2 && upper_val.front() == '\'' && upper_val.back() == '\'') {
            upper_val = upper_val.substr(1, upper_val.size() - 2);
        }

        // 比较
        bool in_range = false;
        try {
            double left_num = std::stod(left_val);
            double lower_num = std::stod(lower_val);
            double upper_num = std::stod(upper_val);
            in_range = (left_num >= lower_num && left_num <= upper_num);
        } catch (...) {
            in_range = (left_val >= lower_val && left_val <= upper_val);
        }

        return between_expr->isNot() ? !in_range : in_range;
    }

    // 处理 LIKE 表达式
    if (auto like_expr = dynamic_cast<const sql::LikeExpr*>(condition)) {
        // 获取左值
        storage::Field left_field = evaluateJoinExpression(like_expr->left(), tuples, schemas);
        std::string left_val = left_field.toString();
        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }

        // 获取模式值
        storage::Field pattern_field = evaluateJoinExpression(like_expr->pattern(), tuples, schemas);
        std::string pattern = pattern_field.toString();
        // 去除引号
        if (pattern.size() >= 2 && pattern.front() == '\'' && pattern.back() == '\'') {
            pattern = pattern.substr(1, pattern.size() - 2);
        }

        // LIKE 匹配
        bool matches = matchLikePattern(left_val, pattern);
        return like_expr->isNot() ? !matches : matches;
    }

    // 处理 IS NULL 表达式
    if (auto is_null_expr = dynamic_cast<const sql::IsNullExpr*>(condition)) {
        storage::Field left_field = evaluateJoinExpression(is_null_expr->left(), tuples, schemas);
        bool is_null = left_field.isNull();
        return is_null_expr->isNot() ? !is_null : is_null;
    }

    return true;
}

// Phase 4: JOIN表达式求值
storage::Field Executor::evaluateJoinExpression(const sql::Expression* expr,
                                                const std::vector<storage::Tuple>& tuples,
                                                const std::vector<storage::Schema*>& schemas) {
    if (!expr) {
        return storage::Field();
    }

    // 处理字面量
    if (auto literal = dynamic_cast<const sql::LiteralExpr*>(expr)) {
        std::string val = literal->value();
        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
            val = val.substr(1, val.size() - 2);
        }
        try {
            return storage::Field(static_cast<int64_t>(std::stoll(val)));
        } catch (...) {
            return storage::Field(val, storage::DataType::VARCHAR);
        }
    }

    // 处理列引用（需要在多个表中查找）
    if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr)) {
        std::string col_name = col_ref->columnName();

        // 检查是否是 qualified name (table.column)
        size_t dot_pos = col_name.find('.');
        if (dot_pos != std::string::npos) {
            std::string table_hint = col_name.substr(0, dot_pos);
            std::string actual_col = col_name.substr(dot_pos + 1);

            // 在指定表中查找
            for (size_t i = 0; i < schemas.size(); ++i) {
                // 简化处理：假设表名在schema中（需要扩展）
                int col_idx = schemas[i]->findColumnIndex(actual_col);
                if (col_idx >= 0 && i < tuples.size()) {
                    return tuples[i].getField(col_idx);
                }
            }
        } else {
            // 在所有表中查找（返回第一个匹配的）
            for (size_t i = 0; i < schemas.size(); ++i) {
                int col_idx = schemas[i]->findColumnIndex(col_name);
                if (col_idx >= 0 && i < tuples.size()) {
                    return tuples[i].getField(col_idx);
                }
            }
        }
        return storage::Field();
    }

    return storage::Field();
}

// Phase 4: JOIN结果格式化
std::string Executor::formatJoinTuple(const std::vector<storage::Tuple>& tuples,
                                      const std::vector<storage::Schema*>& schemas,
                                      const std::vector<std::unique_ptr<sql::Expression>>& select_list) {
    if (select_list.empty()) {
        // 返回所有列
        std::string result = "(";
        for (size_t t = 0; t < tuples.size(); ++t) {
            if (t > 0) result += ", ";
            for (size_t i = 0; i < tuples[t].getFieldCount(); ++i) {
                if (i > 0) result += ", ";
                result += tuples[t].getField(i).toString();
            }
        }
        result += ")";
        return result;
    }

    std::string result = "(";
    for (size_t i = 0; i < select_list.size(); ++i) {
        if (i > 0) result += ", ";

        storage::Field field = evaluateJoinExpression(select_list[i].get(), tuples, schemas);
        result += field.toString();
    }
    result += ")";
    return result;
}

// LIKE 模式匹配辅助函数
bool Executor::matchLikePattern(const std::string& value, const std::string& pattern) {
    // 将 SQL LIKE 模式转换为类似 glob 的匹配
    // % 匹配任意字符序列，_ 匹配单个字符

    size_t val_idx = 0;
    size_t pat_idx = 0;
    size_t val_len = value.size();
    size_t pat_len = pattern.size();

    // 用于处理 % 的回溯位置
    size_t val_backup = 0;
    size_t pat_backup = 0;
    bool has_backup = false;

    while (val_idx < val_len) {
        if (pat_idx < pat_len) {
            char pat_char = pattern[pat_idx];

            if (pat_char == '%') {
                // % 匹配零个或多个字符
                pat_idx++;
                // 记录回溯位置
                val_backup = val_idx;
                pat_backup = pat_idx;
                has_backup = true;
                continue;
            } else if (pat_char == '_') {
                // _ 匹配单个字符
                pat_idx++;
                val_idx++;
                continue;
            } else if (pat_char == value[val_idx]) {
                // 精确匹配
                pat_idx++;
                val_idx++;
                continue;
            }
        }

        // 当前不匹配，尝试回溯
        if (has_backup && val_backup < val_len) {
            val_backup++;
            val_idx = val_backup;
            pat_idx = pat_backup;
            continue;
        }

        // 无法匹配
        return false;
    }

    // 跳过模式末尾的 %
    while (pat_idx < pat_len && pattern[pat_idx] == '%') {
        pat_idx++;
    }

    // 如果模式也处理完毕，则匹配成功
    return pat_idx == pat_len;
}

} // namespace engine
} // namespace tinydb
