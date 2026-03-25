#pragma once

#include "sql/parser/ast.h"
#include "storage/statistics.h"
#include "storage/index_manager.h"
#include <memory>
#include <vector>
#include <string>

namespace tinydb {
namespace engine {

// 扫描类型
enum class ScanType {
    SEQ_SCAN,           // 全表扫描
    INDEX_SCAN,         // 索引扫描
    INDEX_ONLY_SCAN     // 仅索引扫描
};

// 扫描节点
struct ScanNode {
    std::string table_name;
    std::string alias;
    ScanType scan_type = ScanType::SEQ_SCAN;
    std::string index_name;              // 索引扫描时使用的索引
    std::string index_column;            // 索引列
    std::unique_ptr<sql::Expression> index_condition;  // 索引条件
    double estimated_cost = 0.0;         // 估计代价
    uint64_t estimated_rows = 0;         // 估计行数
};

// JOIN类型
enum class JoinType {
    NESTED_LOOP,
    HASH_JOIN,
    MERGE_JOIN
};

// JOIN节点
struct JoinNode {
    JoinType join_type = JoinType::NESTED_LOOP;
    std::unique_ptr<sql::Expression> join_condition;
    std::shared_ptr<ScanNode> left;
    std::shared_ptr<ScanNode> right;
    double estimated_cost = 0.0;
    uint64_t estimated_rows = 0;
};

// 执行计划节点类型
enum class PlanNodeType {
    SCAN,
    JOIN,
    FILTER,
    PROJECT,
    AGGREGATE
};

// 执行计划基类
struct PlanNode {
    PlanNodeType type;
    std::vector<std::shared_ptr<PlanNode>> children;

    explicit PlanNode(PlanNodeType t) : type(t) {}
};

// 扫描计划节点
struct ScanPlanNode : public PlanNode {
    ScanNode scan_info;

    explicit ScanPlanNode(ScanNode&& scan)
        : PlanNode(PlanNodeType::SCAN), scan_info(std::move(scan)) {}
};

// JOIN计划节点
struct JoinPlanNode : public PlanNode {
    JoinNode join_info;

    explicit JoinPlanNode(JoinNode&& join)
        : PlanNode(PlanNodeType::JOIN), join_info(std::move(join)) {}
};

// 过滤计划节点
struct FilterPlanNode : public PlanNode {
    const sql::Expression* condition = nullptr;  // Raw pointer - doesn't own
    double selectivity = 1.0;

    FilterPlanNode() : PlanNode(PlanNodeType::FILTER) {}
};

// 投影计划节点
struct ProjectPlanNode : public PlanNode {
    std::vector<std::unique_ptr<sql::Expression>> projections;

    ProjectPlanNode() : PlanNode(PlanNodeType::PROJECT) {}
};

// 执行计划
struct ExecutionPlan {
    std::shared_ptr<PlanNode> root;
    double total_cost = 0.0;
    uint64_t estimated_rows = 0;

    // 转换为字符串表示（EXPLAIN输出）
    std::string toString() const;
};

// 查询优化器
class QueryOptimizer {
public:
    QueryOptimizer(storage::StatisticsManager* stats_mgr,
                   storage::IndexManager* index_mgr);
    ~QueryOptimizer();

    // 生成执行计划
    ExecutionPlan optimize(const sql::SelectStmt* stmt);

    // 分析单表扫描代价
    std::vector<ScanNode> analyzeScanOptions(const std::string& table_name,
                                             const sql::Expression* where_condition);

    // 选择最优扫描路径
    const ScanNode* chooseBestScan(const std::vector<ScanNode>& options);

    // 分析JOIN代价
    std::vector<JoinNode> analyzeJoinOptions(const std::vector<std::string>& tables,
                                             const sql::Expression* join_condition);

    // 选择最优JOIN顺序
    std::shared_ptr<PlanNode> chooseBestJoinOrder(const std::vector<std::shared_ptr<ScanNode>>& scans,
                                                  const sql::Expression* join_condition);

    // 估计扫描代价
    double estimateScanCost(const ScanNode& scan);

    // 估计JOIN代价
    double estimateJoinCost(const JoinNode& join, uint64_t left_rows, uint64_t right_rows);

    // 估计选择率
    double estimateSelectivity(const std::string& table_name,
                               const sql::Expression* condition);

    // 检查条件是否可以使用索引
    bool canUseIndex(const std::string& table_name,
                     const std::string& column_name,
                     const sql::Expression* condition);

    // 获取可以使用索引的条件
    std::unique_ptr<sql::Expression> extractIndexCondition(const std::string& table_name,
                                                           const std::string& column_name,
                                                           const sql::Expression* condition);

    // 创建执行计划节点
    std::shared_ptr<PlanNode> createScanNode(ScanNode&& scan);
    std::shared_ptr<PlanNode> createJoinNode(JoinNode&& join,
                                              std::shared_ptr<PlanNode> left,
                                              std::shared_ptr<PlanNode> right);
    std::shared_ptr<PlanNode> createFilterNode(const sql::Expression* condition,
                                               double selectivity);
    std::shared_ptr<PlanNode> createProjectNode(std::vector<std::unique_ptr<sql::Expression>> projections);

    // 检查条件是否能被索引覆盖
    bool isIndexOnlyScan(const std::string& table_name,
                         const std::string& index_name,
                         const std::vector<std::unique_ptr<sql::Expression>>& select_list);

private:
    storage::StatisticsManager* stats_mgr_;
    storage::IndexManager* index_mgr_;

    // 代价模型参数
    static constexpr double SEQ_SCAN_COST_PER_PAGE = 1.0;
    static constexpr double INDEX_SCAN_COST_PER_PAGE = 1.5;
    static constexpr double INDEX_LOOKUP_COST = 0.5;
    static constexpr double NESTED_LOOP_COST_PER_ROW = 1.0;

    // 递归分析条件中的列引用
    void extractColumnReferences(const sql::Expression* expr,
                                 std::vector<std::string>& columns);

    // 分解AND条件 - returns raw pointers, no ownership transfer
    std::vector<const sql::Expression*> decomposeAndConditions(
        const sql::Expression* condition);

    // 获取列的条件
    std::unique_ptr<sql::Expression> getConditionForColumn(
        const std::vector<std::unique_ptr<sql::Expression>>& conditions,
        const std::string& column_name);
};

} // namespace engine
} // namespace tinydb
