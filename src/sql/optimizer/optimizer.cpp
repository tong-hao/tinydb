#include "optimizer.h"
#include <algorithm>
#include <sstream>
#include <functional>

namespace tinydb {
namespace engine {

// Helper function to clone expressions (simplified - only handles ComparisonExpr for now)
static std::unique_ptr<sql::Expression> cloneExpression(const sql::Expression* expr) {
    if (!expr) return nullptr;

    // For now, just return nullptr to indicate we can't clone
    // The optimizer should work with original pointers
    return nullptr;
}

// Helper function to clone ScanNode
static ScanNode cloneScanNode(const ScanNode& scan) {
    ScanNode clone;
    clone.table_name = scan.table_name;
    clone.alias = scan.alias;
    clone.scan_type = scan.scan_type;
    clone.index_name = scan.index_name;
    clone.index_column = scan.index_column;
    clone.estimated_cost = scan.estimated_cost;
    clone.estimated_rows = scan.estimated_rows;
    // Note: index_condition is not cloned (unique_ptr), set to nullptr
    return clone;
}

// Helper function to clone JoinNode
static JoinNode cloneJoinNode(const JoinNode& join) {
    JoinNode clone;
    clone.join_type = join.join_type;
    clone.estimated_cost = join.estimated_cost;
    clone.estimated_rows = join.estimated_rows;
    clone.left = join.left;
    clone.right = join.right;
    // Note: join_condition is not cloned (unique_ptr), set to nullptr
    return clone;
}

QueryOptimizer::QueryOptimizer(storage::StatisticsManager* stats_mgr,
                               storage::IndexManager* index_mgr)
    : stats_mgr_(stats_mgr), index_mgr_(index_mgr) {}

QueryOptimizer::~QueryOptimizer() = default;

ExecutionPlan QueryOptimizer::optimize(const sql::SelectStmt* stmt) {
    ExecutionPlan plan;

    if (stmt->fromTable().empty()) {
        // 无表查询（如 SELECT 1+2）
        // 使用原始指针，不转移所有权
        std::vector<std::unique_ptr<sql::Expression>> projections;
        plan.root = createProjectNode(std::move(projections));
        plan.total_cost = 0;
        plan.estimated_rows = 1;
        return plan;
    }

    // 单表查询
    auto scan_options = analyzeScanOptions(stmt->fromTable(), stmt->whereCondition());
    auto best_scan = chooseBestScan(scan_options);

    if (!best_scan) {
        plan.root = nullptr;
        return plan;
    }

    // Create a copy of the ScanNode to move
    ScanNode scan_copy;
    scan_copy.table_name = best_scan->table_name;
    scan_copy.alias = best_scan->alias;
    scan_copy.scan_type = best_scan->scan_type;
    scan_copy.index_name = best_scan->index_name;
    scan_copy.index_column = best_scan->index_column;
    scan_copy.estimated_cost = best_scan->estimated_cost;
    scan_copy.estimated_rows = best_scan->estimated_rows;
    // Note: index_condition is unique_ptr, we leave it nullptr for the copy
    // The actual condition will be handled separately

    std::shared_ptr<PlanNode> current = createScanNode(std::move(scan_copy));

    // 添加过滤器（如果有WHERE条件且未被索引处理）
    // Note: We don't take ownership of the condition, just pass nullptr
    // The condition is owned by the SQLParseTree
    if (stmt->whereCondition()) {
        double sel = estimateSelectivity(stmt->fromTable(), stmt->whereCondition());
        // Create filter without taking ownership of condition
        auto filter = createFilterNode(nullptr, sel);
        filter->children.push_back(current);
        current = filter;
    }

    // 添加投影
    std::vector<std::unique_ptr<sql::Expression>> projections;
    // Don't clone expressions - they are owned by the SQLParseTree
    plan.root = createProjectNode(std::move(projections));
    plan.root->children.push_back(current);

    plan.total_cost = best_scan->estimated_cost;
    plan.estimated_rows = best_scan->estimated_rows;

    return plan;
}

std::vector<ScanNode> QueryOptimizer::analyzeScanOptions(const std::string& table_name,
                                                         const sql::Expression* where_condition) {
    std::vector<ScanNode> options;
    std::optional<storage::TableStats> stats_opt;

    // 选项1：全表扫描
    ScanNode seq_scan;
    seq_scan.table_name = table_name;
    seq_scan.scan_type = ScanType::SEQ_SCAN;

    if (stats_mgr_) {
        stats_opt = stats_mgr_->getTableStats(table_name);
        if (stats_opt.has_value()) {
            seq_scan.estimated_rows = stats_opt->row_count;
            seq_scan.estimated_cost = stats_opt->page_count * SEQ_SCAN_COST_PER_PAGE;
        } else {
            // 无统计信息，使用默认值
            seq_scan.estimated_rows = 1000;
            seq_scan.estimated_cost = 1000 * SEQ_SCAN_COST_PER_PAGE;
        }
    } else {
        // 无统计管理器，使用默认值
        seq_scan.estimated_rows = 1000;
        seq_scan.estimated_cost = 1000 * SEQ_SCAN_COST_PER_PAGE;
    }

    options.push_back(std::move(seq_scan));

    // 选项2：索引扫描（如果有索引可用）
    if (index_mgr_) {
        auto table_indexes = index_mgr_->getTableIndexes(table_name);
        for (const auto& index : table_indexes) {
            if (!index) continue;
            const auto& meta = index->getMeta();

            // 检查是否有条件可以使用该索引
            if (where_condition && canUseIndex(table_name, meta.column_name, where_condition)) {
                ScanNode index_scan;
                index_scan.table_name = table_name;
                index_scan.scan_type = ScanType::INDEX_SCAN;
                index_scan.index_name = meta.index_name;
                index_scan.index_column = meta.column_name;
                index_scan.index_condition = extractIndexCondition(table_name, meta.column_name,
                                                                   where_condition);

                // 估计代价
                double selectivity = estimateSelectivity(table_name, where_condition);
                if (stats_opt.has_value()) {
                    index_scan.estimated_rows = static_cast<uint64_t>(stats_opt->row_count * selectivity);
                    // 索引扫描代价 = 索引页读取 + 数据页读取
                    index_scan.estimated_cost = stats_opt->page_count * 0.1 * INDEX_SCAN_COST_PER_PAGE +
                                               index_scan.estimated_rows * INDEX_LOOKUP_COST;
                } else {
                    index_scan.estimated_rows = 100;
                    index_scan.estimated_cost = 50;
                }

                options.push_back(std::move(index_scan));
            }
        }
    }

    return options;
}

const ScanNode* QueryOptimizer::chooseBestScan(const std::vector<ScanNode>& options) {
    if (options.empty()) {
        return nullptr;
    }

    // 选择代价最低的扫描方式
    const ScanNode* best = &options[0];
    for (const auto& opt : options) {
        if (opt.estimated_cost < best->estimated_cost) {
            best = &opt;
        }
    }

    return best;
}

std::vector<JoinNode> QueryOptimizer::analyzeJoinOptions(const std::vector<std::string>& tables,
                                                         const sql::Expression* join_condition) {
    std::vector<JoinNode> options;

    // 目前只支持Nested Loop Join
    // 生成所有可能的JOIN顺序
    for (size_t i = 0; i < tables.size(); ++i) {
        for (size_t j = i + 1; j < tables.size(); ++j) {
            JoinNode join;
            join.join_type = JoinType::NESTED_LOOP;
            join.left = std::make_shared<ScanNode>();
            join.left->table_name = tables[i];
            join.right = std::make_shared<ScanNode>();
            join.right->table_name = tables[j];

            options.push_back(std::move(join));
        }
    }

    return options;
}

std::shared_ptr<PlanNode> QueryOptimizer::chooseBestJoinOrder(
    const std::vector<std::shared_ptr<ScanNode>>& scans,
    const sql::Expression* join_condition) {

    if (scans.empty()) {
        return nullptr;
    }

    if (scans.size() == 1) {
        return createScanNode(cloneScanNode(*scans[0]));
    }

    // 简单的贪心算法：选择最小的表作为驱动表
    // 实际应该使用动态规划（DPsize或DPsube）

    // 按估计行数排序
    auto sorted_scans = scans;
    std::sort(sorted_scans.begin(), sorted_scans.end(),
              [](const auto& a, const auto& b) {
                  return a->estimated_rows < b->estimated_rows;
              });

    // 构建左深树
    std::shared_ptr<PlanNode> left = createScanNode(cloneScanNode(*sorted_scans[0]));

    for (size_t i = 1; i < sorted_scans.size(); ++i) {
        JoinNode join;
        join.join_type = JoinType::NESTED_LOOP;
        join.left = sorted_scans[i-1];
        join.right = sorted_scans[i];

        std::shared_ptr<PlanNode> right = createScanNode(cloneScanNode(*sorted_scans[i]));
        left = createJoinNode(cloneJoinNode(join), left, right);
    }

    return left;
}

double QueryOptimizer::estimateScanCost(const ScanNode& scan) {
    return scan.estimated_cost;
}

double QueryOptimizer::estimateJoinCost(const JoinNode& join, uint64_t left_rows, uint64_t right_rows) {
    // Nested Loop Join代价 = 外表行数 * (内表扫描代价 + 连接代价)
    if (join.join_type == JoinType::NESTED_LOOP) {
        double inner_scan_cost = 0;
        if (stats_mgr_) {
            auto stats_opt = stats_mgr_->getTableStats(join.right->table_name);
            if (stats_opt.has_value()) {
                inner_scan_cost = stats_opt->page_count * SEQ_SCAN_COST_PER_PAGE;
            }
        }

        return left_rows * (inner_scan_cost + NESTED_LOOP_COST_PER_ROW * right_rows);
    }

    return 0;
}

double QueryOptimizer::estimateSelectivity(const std::string& table_name,
                                           const sql::Expression* condition) {
    if (!condition) return 1.0;

    // 尝试从条件中提取列和运算符
    auto conditions = decomposeAndConditions(condition);
    double combined_selectivity = 1.0;

    for (const auto& cond : conditions) {
        // 简化处理：假设每个条件的独立选择率
        combined_selectivity *= 0.05;  // 默认选择率5%
    }

    return std::max(combined_selectivity, 0.001);  // 最小选择率0.1%
}

bool QueryOptimizer::canUseIndex(const std::string& table_name,
                                 const std::string& column_name,
                                 const sql::Expression* condition) {
    if (!condition) return false;

    // 检查条件中是否包含该列的等值或范围条件
    auto conditions = decomposeAndConditions(condition);

    for (const auto& cond : conditions) {
        // 检查是否是等值或范围比较
        if (auto comp = dynamic_cast<const sql::ComparisonExpr*>(cond)) {
            if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(comp->left())) {
                if (col_ref->columnName() == column_name) {
                    // 等值 (=) 或范围 (<, >, <=, >=) 都可以使用索引
                    return true;
                }
            }
        }
    }

    return false;
}

std::unique_ptr<sql::Expression> QueryOptimizer::extractIndexCondition(
    const std::string& table_name,
    const std::string& column_name,
    const sql::Expression* condition) {

    auto conditions = decomposeAndConditions(condition);

    for (const auto& cond : conditions) {
        if (auto comp = dynamic_cast<const sql::ComparisonExpr*>(cond)) {
            if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(comp->left())) {
                if (col_ref->columnName() == column_name) {
                    // Clone the expression to return it
                    // For now, return nullptr as we can't easily clone
                    return nullptr;
                }
            }
        }
    }

    return nullptr;
}

std::shared_ptr<PlanNode> QueryOptimizer::createScanNode(ScanNode&& scan) {
    return std::make_shared<ScanPlanNode>(std::move(scan));
}

std::shared_ptr<PlanNode> QueryOptimizer::createJoinNode(JoinNode&& join,
                                                         std::shared_ptr<PlanNode> left,
                                                         std::shared_ptr<PlanNode> right) {
    auto node = std::make_shared<JoinPlanNode>(std::move(join));
    node->children.push_back(left);
    node->children.push_back(right);
    return node;
}

std::shared_ptr<PlanNode> QueryOptimizer::createFilterNode(const sql::Expression* condition,
                                                           double selectivity) {
    auto node = std::make_shared<FilterPlanNode>();
    node->condition = condition;  // Just store the pointer, don't take ownership
    node->selectivity = selectivity;
    return node;
}

std::shared_ptr<PlanNode> QueryOptimizer::createProjectNode(
    std::vector<std::unique_ptr<sql::Expression>> projections) {
    auto node = std::make_shared<ProjectPlanNode>();
    node->projections = std::move(projections);
    return node;
}

bool QueryOptimizer::isIndexOnlyScan(const std::string& table_name,
                                     const std::string& index_name,
                                     const std::vector<std::unique_ptr<sql::Expression>>& select_list) {
    // 检查查询列是否都在索引中
    // 简化处理：只检查单列索引
    if (!index_mgr_) return false;

    auto index = index_mgr_->getIndex(index_name);
    if (!index) return false;

    const auto& meta = index->getMeta();

    // 遍历所有SELECT列
    for (const auto& expr : select_list) {
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr.get())) {
            if (col_ref->columnName() != meta.column_name) {
                // 需要访问表数据
                return false;
            }
        }
    }

    return true;
}

// Forward declaration for helper
template<typename T>
static std::vector<T> decomposeAndConditionsImpl(const sql::Expression* condition);

std::vector<const sql::Expression*> QueryOptimizer::decomposeAndConditions(
    const sql::Expression* condition) {
    std::vector<const sql::Expression*> result;
    if (!condition) return result;

    if (auto logical = dynamic_cast<const sql::LogicalExpr*>(condition)) {
        if (logical->op() == sql::OpType::AND) {
            auto left_decomposed = decomposeAndConditions(logical->left());
            result.insert(result.end(), left_decomposed.begin(), left_decomposed.end());

            if (logical->right()) {
                auto right_decomposed = decomposeAndConditions(logical->right());
                result.insert(result.end(), right_decomposed.begin(), right_decomposed.end());
            }
            return result;
        }
    }

    // 不是AND，直接返回原始指针
    result.push_back(condition);
    return result;
}

std::string ExecutionPlan::toString() const {
    std::ostringstream oss;
    oss << "Execution Plan (estimated cost=" << total_cost << ", rows=" << estimated_rows << ")\n";

    // 递归打印计划树
    std::function<void(const std::shared_ptr<PlanNode>&, int)> printNode;
    printNode = [&](const std::shared_ptr<PlanNode>& node, int indent) {
        std::string prefix(indent * 2, ' ');

        switch (node->type) {
            case PlanNodeType::SCAN: {
                auto scan = std::static_pointer_cast<ScanPlanNode>(node);
                oss << prefix;
                switch (scan->scan_info.scan_type) {
                    case ScanType::SEQ_SCAN:
                        oss << "Seq Scan on " << scan->scan_info.table_name;
                        break;
                    case ScanType::INDEX_SCAN:
                        oss << "Index Scan using " << scan->scan_info.index_name
                            << " on " << scan->scan_info.table_name;
                        break;
                    case ScanType::INDEX_ONLY_SCAN:
                        oss << "Index Only Scan using " << scan->scan_info.index_name
                            << " on " << scan->scan_info.table_name;
                        break;
                }
                oss << "  (cost=" << scan->scan_info.estimated_cost
                    << " rows=" << scan->scan_info.estimated_rows << ")\n";
                break;
            }
            case PlanNodeType::JOIN: {
                auto join = std::static_pointer_cast<JoinPlanNode>(node);
                oss << prefix << "Nested Loop Join"
                        << "  (cost=" << join->join_info.estimated_cost << ")\n";
                break;
            }
            case PlanNodeType::FILTER: {
                auto filter = std::static_pointer_cast<FilterPlanNode>(node);
                oss << prefix << "Filter"
                        << "  (selectivity=" << filter->selectivity << ")\n";
                break;
            }
            case PlanNodeType::PROJECT: {
                oss << prefix << "Project\n";
                break;
            }
            default:
                oss << prefix << "Unknown\n";
        }

        for (const auto& child : node->children) {
            printNode(child, indent + 1);
        }
    };

    if (root) {
        printNode(root, 0);
    }

    return oss.str();
}

} // namespace engine
} // namespace tinydb
