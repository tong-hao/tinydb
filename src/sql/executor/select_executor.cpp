#include "common/logger.h"
#include "select_executor.h"
#include "sql/stmt/system_view_manager.h"
#include "sql/parser/parser.h"

namespace tinydb {
namespace engine {

SelectExecutor::SelectExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {
    LOG_INFO("SelectExecutor initialized");
}

SelectExecutor::~SelectExecutor() {
}

ExecutionResult SelectExecutor::execute(const sql::SelectStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("SELECT executed (simulated - no storage engine)");
    }

    std::string table_name = stmt ? stmt->fromTable() : "";

    // Phase 4: 检查是否是多表JOIN查询
    if (stmt && !stmt->joinTables().empty()) {
        return executeJoin(stmt);
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
                            result += Executor::formatTuple(tuple, stmt->selectList(), &table_meta->schema) + "\n";
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
                    // TODO: 简化处理：检查字符串匹配
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
                if (view_where && !Executor::evaluateWhereCondition(tuple, view_where, &table_meta->schema)) {
                    continue;
                }

                // Apply current query's WHERE condition
                if (current_where && !Executor::evaluateWhereCondition(tuple, current_where, &table_meta->schema)) {
                    continue;
                }

                // For view, use the view's select list
                result += Executor::formatTuple(tuple, view_select->selectList(), &table_meta->schema) + "\n";
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
            if (where_condition && !Executor::evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 格式化输出，支持选择特定列
            if (stmt) {
                result += Executor::formatTuple(tuple, stmt->selectList(), &table_meta->schema) + "\n";
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

// Phase 4: JOIN execution
ExecutionResult SelectExecutor::executeJoin(const sql::SelectStmt* stmt) {
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


    // 执行Nested Loop Join（支持 INNER 和 LEFT JOIN）
    std::string result = "JOIN Result:\n";
    result += "--------------------\n";

    // 获取主表
    auto left_table = storage_engine_->getTable(tables[0]);

    // 收集所有JOIN表的信息
    const auto& join_tables = stmt->joinTables();
    if (join_tables.empty()) {
        // 如果没有明确的JOIN表定义，使用旧逻辑（INNER JOIN）
        return executeSimpleJoin(stmt, tables, result);
    }

    // 执行多表JOIN（支持不同的JOIN类型）
    std::vector<std::string> joined_tables = {tables[0]};
    std::vector<storage::Schema*> all_schemas = {&left_table->schema};

    // 使用嵌套循环处理多个JOIN
    auto left_iter = storage_engine_->scan(tables[0]);
    int row_count = 0;

    // 收集所有结果行
    std::vector<std::vector<storage::Tuple>> all_results;

    while (left_iter.hasNext()) {
        auto left_tuple = left_iter.getNext();

        // 处理第一个JOIN
        if (!join_tables.empty()) {
            processJoinClause(stmt, left_tuple, &left_table->schema,
                            join_tables, 0, {left_tuple}, all_schemas,
                            all_results, row_count);
        }
    }

    // 格式化输出所有结果
    for (const auto& tuples : all_results) {
        result += Executor::formatJoinTuple(tuples, all_schemas, stmt->selectList()) + "\n";
    }

    result += "--------------------\n";
    result += "Total rows: " + std::to_string(row_count);

    return ExecutionResult::ok(result);
}

// 处理单个JOIN子句
void SelectExecutor::processJoinClause(const sql::SelectStmt* stmt,
                                 const storage::Tuple& current_tuple,
                                 storage::Schema* current_schema,
                                 const std::vector<std::unique_ptr<sql::JoinTable>>& join_tables,
                                 size_t join_index,
                                 std::vector<storage::Tuple> accumulated_tuples,
                                 std::vector<storage::Schema*>& schemas,
                                 std::vector<std::vector<storage::Tuple>>& results,
                                 int& row_count) {
    if (join_index >= join_tables.size()) {
        // 所有JOIN处理完成，检查WHERE条件
        if (stmt->whereCondition()) {
            if (!Executor::evaluateJoinWhereCondition(accumulated_tuples, schemas, stmt->whereCondition())) {
                return;
            }
        }
        // 添加到结果
        results.push_back(accumulated_tuples);
        row_count++;
        return;
    }

    const auto& join_table_info = join_tables[join_index];
    const std::string& right_table_name = join_table_info->tableName();
    sql::JoinType join_type = join_table_info->joinType();
    sql::Expression* join_condition = join_table_info->joinCondition();

    auto right_table = storage_engine_->getTable(right_table_name);
    auto right_iter = storage_engine_->scan(right_table_name);

    bool has_match = false;
    std::vector<storage::Schema*> current_schemas = schemas;
    current_schemas.push_back(&right_table->schema);

    // 收集匹配的行
    std::vector<storage::Tuple> matched_tuples;

    while (right_iter.hasNext()) {
        auto right_tuple = right_iter.getNext();

        // 检查JOIN条件
        std::vector<storage::Tuple> test_tuples = accumulated_tuples;
        test_tuples.push_back(right_tuple);

        bool condition_passed = true;
        if (join_condition) {
            condition_passed = Executor::evaluateJoinWhereCondition(test_tuples, current_schemas, join_condition);
        }

        if (condition_passed) {
            has_match = true;

            // 递归处理下一个JOIN
            if (join_index + 1 < join_tables.size()) {
                std::vector<storage::Schema*> next_schemas = current_schemas;
                processJoinClause(stmt, right_tuple, &right_table->schema,
                                join_tables, join_index + 1,
                                test_tuples, next_schemas, results, row_count);
            } else {
                // 检查WHERE条件
                if (stmt->whereCondition()) {
                    if (!Executor::evaluateJoinWhereCondition(test_tuples, current_schemas, stmt->whereCondition())) {
                        continue;
                    }
                }
                results.push_back(test_tuples);
                row_count++;
            }
        }
    }

    // 对于LEFT JOIN，如果没有匹配，保留左表数据，右表填NULL
    if (!has_match && join_type == sql::JoinType::LEFT) {
        // 创建NULL元组
        storage::Tuple null_tuple;
        for (size_t i = 0; i < right_table->schema.getColumnCount(); ++i) {
            null_tuple.addField(storage::Field());  // NULL字段
        }

        std::vector<storage::Tuple> test_tuples = accumulated_tuples;
        test_tuples.push_back(null_tuple);

        // 递归处理下一个JOIN（对于LEFT JOIN，没有匹配时也要继续）
        if (join_index + 1 < join_tables.size()) {
            std::vector<storage::Schema*> next_schemas = current_schemas;
            processJoinClause(stmt, null_tuple, &right_table->schema,
                            join_tables, join_index + 1,
                            test_tuples, next_schemas, results, row_count);
        } else {
            // 检查WHERE条件（注意：LEFT JOIN 的 WHERE 可能排除 NULL 行）
            if (stmt->whereCondition()) {
                if (!Executor::evaluateJoinWhereCondition(test_tuples, current_schemas, stmt->whereCondition())) {
                    return;
                }
            }
            results.push_back(test_tuples);
            row_count++;
        }
    }
}

// 简单JOIN实现（向后兼容）
ExecutionResult SelectExecutor::executeSimpleJoin(const sql::SelectStmt* stmt,
                                            const std::vector<std::string>& tables,
                                            std::string& result) {
    auto left_table = storage_engine_->getTable(tables[0]);
    auto right_table = storage_engine_->getTable(tables[1]);

    auto left_iter = storage_engine_->scan(tables[0]);
    int row_count = 0;

    while (left_iter.hasNext()) {
        auto left_tuple = left_iter.getNext();

        auto right_iter = storage_engine_->scan(tables[1]);
        while (right_iter.hasNext()) {
            auto right_tuple = right_iter.getNext();

            std::vector<storage::Tuple> tuples = {left_tuple, right_tuple};
            std::vector<storage::Schema*> schemas = {&left_table->schema, &right_table->schema};

            if (stmt->whereCondition()) {
                if (!Executor::evaluateJoinWhereCondition(tuples, schemas, stmt->whereCondition())) {
                    continue;
                }
            }

            result += Executor::formatJoinTuple(tuples, schemas, stmt->selectList()) + "\n";
            row_count++;
        }
    }

    result += "--------------------\n";
    result += "Total rows: " + std::to_string(row_count);

    return ExecutionResult::ok(result);
}

} // namespace engine
} // namespace tinydb
