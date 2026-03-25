#include <gtest/gtest.h>
#include "sql/parser/ast.h"
#include "storage/tuple.h"
#include "storage/table.h"

using namespace tinydb;
using namespace tinydb::sql;
using namespace tinydb::storage;

class ExpressionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建一个测试用的Schema
        schema_.addColumn("id", DataType::INT32);
        schema_.addColumn("name", DataType::VARCHAR, 50);
        schema_.addColumn("age", DataType::INT32);
        schema_.addColumn("salary", DataType::DOUBLE);

        // 创建一个测试用的Tuple
        tuple_.setSchema(&schema_);
        tuple_.addField(Field(static_cast<int32_t>(1)));
        tuple_.addField(Field("Alice", DataType::VARCHAR));
        tuple_.addField(Field(static_cast<int32_t>(25)));
        tuple_.addField(Field(static_cast<double>(50000.0)));
    }

    Schema schema_;
    Tuple tuple_;
};

// 测试字面量表达式
TEST_F(ExpressionTest, LiteralExpr) {
    LiteralExpr expr("42");
    EXPECT_EQ(expr.value(), "42");
    EXPECT_EQ(expr.toString(), "Literal(42)");
}

// 测试列引用表达式
TEST_F(ExpressionTest, ColumnRefExpr) {
    ColumnRefExpr expr("age");
    EXPECT_EQ(expr.columnName(), "age");
    EXPECT_EQ(expr.toString(), "ColumnRef(age)");
}

// 测试二元算术表达式 - 加法
TEST_F(ExpressionTest, BinaryOpExprAddition) {
    auto left = std::make_unique<LiteralExpr>("10");
    auto right = std::make_unique<LiteralExpr>("20");
    BinaryOpExpr expr(OpType::ADD, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::ADD);
    EXPECT_EQ(expr.toString(), "(Literal(10) + Literal(20))");
}

// 测试二元算术表达式 - 减法
TEST_F(ExpressionTest, BinaryOpExprSubtraction) {
    auto left = std::make_unique<LiteralExpr>("50");
    auto right = std::make_unique<LiteralExpr>("20");
    BinaryOpExpr expr(OpType::SUB, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::SUB);
    EXPECT_EQ(expr.toString(), "(Literal(50) - Literal(20))");
}

// 测试二元算术表达式 - 乘法
TEST_F(ExpressionTest, BinaryOpExprMultiplication) {
    auto left = std::make_unique<LiteralExpr>("10");
    auto right = std::make_unique<LiteralExpr>("5");
    BinaryOpExpr expr(OpType::MUL, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::MUL);
    EXPECT_EQ(expr.toString(), "(Literal(10) * Literal(5))");
}

// 测试二元算术表达式 - 除法
TEST_F(ExpressionTest, BinaryOpExprDivision) {
    auto left = std::make_unique<LiteralExpr>("100");
    auto right = std::make_unique<LiteralExpr>("4");
    BinaryOpExpr expr(OpType::DIV, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::DIV);
    EXPECT_EQ(expr.toString(), "(Literal(100) / Literal(4))");
}

// 测试二元算术表达式 - 取模
TEST_F(ExpressionTest, BinaryOpExprModulo) {
    auto left = std::make_unique<LiteralExpr>("17");
    auto right = std::make_unique<LiteralExpr>("5");
    BinaryOpExpr expr(OpType::MOD, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::MOD);
    EXPECT_EQ(expr.toString(), "(Literal(17) % Literal(5))");
}

// 测试嵌套算术表达式
TEST_F(ExpressionTest, NestedArithmeticExpr) {
    // (10 + 20) * 3
    auto left = std::make_unique<BinaryOpExpr>(
        OpType::ADD,
        std::make_unique<LiteralExpr>("10"),
        std::make_unique<LiteralExpr>("20")
    );
    auto right = std::make_unique<LiteralExpr>("3");
    BinaryOpExpr expr(OpType::MUL, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::MUL);
    EXPECT_NE(expr.toString().find("+"), std::string::npos);
    EXPECT_NE(expr.toString().find("*"), std::string::npos);
}

// 测试比较表达式 - 等于
TEST_F(ExpressionTest, ComparisonExprEqual) {
    auto left = std::make_unique<ColumnRefExpr>("id");
    auto right = std::make_unique<LiteralExpr>("1");
    ComparisonExpr expr(OpType::EQ, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::EQ);
    EXPECT_EQ(expr.toString(), "(ColumnRef(id) = Literal(1))");
}

// 测试比较表达式 - 不等于
TEST_F(ExpressionTest, ComparisonExprNotEqual) {
    auto left = std::make_unique<ColumnRefExpr>("id");
    auto right = std::make_unique<LiteralExpr>("2");
    ComparisonExpr expr(OpType::NE, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::NE);
    EXPECT_EQ(expr.toString(), "(ColumnRef(id) <> Literal(2))");
}

// 测试比较表达式 - 小于
TEST_F(ExpressionTest, ComparisonExprLessThan) {
    auto left = std::make_unique<ColumnRefExpr>("age");
    auto right = std::make_unique<LiteralExpr>("30");
    ComparisonExpr expr(OpType::LT, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::LT);
    EXPECT_EQ(expr.toString(), "(ColumnRef(age) < Literal(30))");
}

// 测试比较表达式 - 小于等于
TEST_F(ExpressionTest, ComparisonExprLessEqual) {
    auto left = std::make_unique<ColumnRefExpr>("age");
    auto right = std::make_unique<LiteralExpr>("25");
    ComparisonExpr expr(OpType::LE, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::LE);
    EXPECT_EQ(expr.toString(), "(ColumnRef(age) <= Literal(25))");
}

// 测试比较表达式 - 大于
TEST_F(ExpressionTest, ComparisonExprGreaterThan) {
    auto left = std::make_unique<ColumnRefExpr>("salary");
    auto right = std::make_unique<LiteralExpr>("40000");
    ComparisonExpr expr(OpType::GT, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::GT);
    EXPECT_EQ(expr.toString(), "(ColumnRef(salary) > Literal(40000))");
}

// 测试比较表达式 - 大于等于
TEST_F(ExpressionTest, ComparisonExprGreaterEqual) {
    auto left = std::make_unique<ColumnRefExpr>("salary");
    auto right = std::make_unique<LiteralExpr>("50000");
    ComparisonExpr expr(OpType::GE, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::GE);
    EXPECT_EQ(expr.toString(), "(ColumnRef(salary) >= Literal(50000))");
}

// 测试逻辑表达式 - AND
TEST_F(ExpressionTest, LogicalExprAnd) {
    auto left = std::make_unique<ComparisonExpr>(
        OpType::GT,
        std::make_unique<ColumnRefExpr>("age"),
        std::make_unique<LiteralExpr>("20")
    );
    auto right = std::make_unique<ComparisonExpr>(
        OpType::LT,
        std::make_unique<ColumnRefExpr>("age"),
        std::make_unique<LiteralExpr>("30")
    );
    LogicalExpr expr(OpType::AND, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::AND);
    EXPECT_NE(expr.toString().find("AND"), std::string::npos);
    EXPECT_NE(expr.toString().find(">"), std::string::npos);
    EXPECT_NE(expr.toString().find("<"), std::string::npos);
}

// 测试逻辑表达式 - OR
TEST_F(ExpressionTest, LogicalExprOr) {
    auto left = std::make_unique<ComparisonExpr>(
        OpType::EQ,
        std::make_unique<ColumnRefExpr>("name"),
        std::make_unique<LiteralExpr>("'Alice'")
    );
    auto right = std::make_unique<ComparisonExpr>(
        OpType::EQ,
        std::make_unique<ColumnRefExpr>("name"),
        std::make_unique<LiteralExpr>("'Bob'")
    );
    LogicalExpr expr(OpType::OR, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::OR);
    EXPECT_NE(expr.toString().find("OR"), std::string::npos);
}

// 测试逻辑表达式 - NOT
TEST_F(ExpressionTest, LogicalExprNot) {
    auto operand = std::make_unique<ComparisonExpr>(
        OpType::EQ,
        std::make_unique<ColumnRefExpr>("id"),
        std::make_unique<LiteralExpr>("0")
    );
    LogicalExpr expr(OpType::NOT, std::move(operand));

    EXPECT_EQ(expr.op(), OpType::NOT);
    EXPECT_NE(expr.left(), nullptr);  // NOT是单目运算符，操作数存储在 left 中
    EXPECT_EQ(expr.right(), nullptr);  // right 为 nullptr
    EXPECT_NE(expr.toString().find("NOT"), std::string::npos);
}

// 测试复杂WHERE条件表达式
TEST_F(ExpressionTest, ComplexWhereCondition) {
    // (age > 20 AND age < 30) OR salary >= 50000
    auto age_gt = std::make_unique<ComparisonExpr>(
        OpType::GT,
        std::make_unique<ColumnRefExpr>("age"),
        std::make_unique<LiteralExpr>("20")
    );
    auto age_lt = std::make_unique<ComparisonExpr>(
        OpType::LT,
        std::make_unique<ColumnRefExpr>("age"),
        std::make_unique<LiteralExpr>("30")
    );
    auto age_range = std::make_unique<LogicalExpr>(
        OpType::AND,
        std::move(age_gt),
        std::move(age_lt)
    );
    auto salary_cond = std::make_unique<ComparisonExpr>(
        OpType::GE,
        std::make_unique<ColumnRefExpr>("salary"),
        std::make_unique<LiteralExpr>("50000")
    );
    LogicalExpr expr(OpType::OR, std::move(age_range), std::move(salary_cond));

    EXPECT_EQ(expr.op(), OpType::OR);
    EXPECT_NE(expr.toString().find("AND"), std::string::npos);
    EXPECT_NE(expr.toString().find("OR"), std::string::npos);
}

// 测试算术表达式与列引用组合
TEST_F(ExpressionTest, ArithmeticWithColumnRef) {
    // salary * 1.1 (涨薪10%)
    auto left = std::make_unique<ColumnRefExpr>("salary");
    auto right = std::make_unique<LiteralExpr>("1.1");
    BinaryOpExpr expr(OpType::MUL, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::MUL);
    EXPECT_NE(expr.toString().find("salary"), std::string::npos);
    EXPECT_NE(expr.toString().find("*"), std::string::npos);
}

// 测试字符串字面量
TEST_F(ExpressionTest, StringLiteralExpr) {
    LiteralExpr expr("'Hello World'");
    EXPECT_EQ(expr.value(), "'Hello World'");
}

// 测试浮点数字面量
TEST_F(ExpressionTest, FloatLiteralExpr) {
    LiteralExpr expr("3.14159");
    EXPECT_EQ(expr.value(), "3.14159");
}

// 测试负数字面量
TEST_F(ExpressionTest, NegativeLiteralExpr) {
    LiteralExpr expr("-42");
    EXPECT_EQ(expr.value(), "-42");
}

// 测试OpType枚举值
TEST_F(ExpressionTest, OpTypeValues) {
    // 比较操作符
    EXPECT_EQ(static_cast<int>(OpType::EQ), 0);
    EXPECT_EQ(static_cast<int>(OpType::NE), 1);
    EXPECT_EQ(static_cast<int>(OpType::LT), 2);
    EXPECT_EQ(static_cast<int>(OpType::LE), 3);
    EXPECT_EQ(static_cast<int>(OpType::GT), 4);
    EXPECT_EQ(static_cast<int>(OpType::GE), 5);

    // 算术操作符
    EXPECT_EQ(static_cast<int>(OpType::ADD), 6);
    EXPECT_EQ(static_cast<int>(OpType::SUB), 7);
    EXPECT_EQ(static_cast<int>(OpType::MUL), 8);
    EXPECT_EQ(static_cast<int>(OpType::DIV), 9);
    EXPECT_EQ(static_cast<int>(OpType::MOD), 10);

    // 逻辑操作符
    EXPECT_EQ(static_cast<int>(OpType::AND), 11);
    EXPECT_EQ(static_cast<int>(OpType::OR), 12);
    EXPECT_EQ(static_cast<int>(OpType::NOT), 13);
}

// 测试比较表达式的左右子表达式访问
TEST_F(ExpressionTest, ComparisonExprAccessors) {
    auto left = std::make_unique<ColumnRefExpr>("id");
    auto right = std::make_unique<LiteralExpr>("1");
    ComparisonExpr expr(OpType::EQ, std::move(left), std::move(right));

    EXPECT_NE(expr.left(), nullptr);
    EXPECT_NE(expr.right(), nullptr);

    // 验证左操作数是列引用
    auto* left_col = dynamic_cast<const ColumnRefExpr*>(expr.left());
    ASSERT_NE(left_col, nullptr);
    EXPECT_EQ(left_col->columnName(), "id");

    // 验证右操作数是字面量
    auto* right_lit = dynamic_cast<const LiteralExpr*>(expr.right());
    ASSERT_NE(right_lit, nullptr);
    EXPECT_EQ(right_lit->value(), "1");
}

// 测试二元算术表达式的左右子表达式访问
TEST_F(ExpressionTest, BinaryOpExprAccessors) {
    auto left = std::make_unique<LiteralExpr>("10");
    auto right = std::make_unique<LiteralExpr>("20");
    BinaryOpExpr expr(OpType::ADD, std::move(left), std::move(right));

    EXPECT_NE(expr.left(), nullptr);
    EXPECT_NE(expr.right(), nullptr);

    // 验证左操作数
    auto* left_lit = dynamic_cast<const LiteralExpr*>(expr.left());
    ASSERT_NE(left_lit, nullptr);
    EXPECT_EQ(left_lit->value(), "10");

    // 验证右操作数
    auto* right_lit = dynamic_cast<const LiteralExpr*>(expr.right());
    ASSERT_NE(right_lit, nullptr);
    EXPECT_EQ(right_lit->value(), "20");
}

// 测试复杂算术表达式链
TEST_F(ExpressionTest, ArithmeticExpressionChain) {
    // 10 + 20 - 5
    auto add_expr = std::make_unique<BinaryOpExpr>(
        OpType::ADD,
        std::make_unique<LiteralExpr>("10"),
        std::make_unique<LiteralExpr>("20")
    );
    BinaryOpExpr expr(OpType::SUB, std::move(add_expr), std::make_unique<LiteralExpr>("5"));

    EXPECT_EQ(expr.op(), OpType::SUB);
    EXPECT_NE(expr.toString().find("+"), std::string::npos);
    EXPECT_NE(expr.toString().find("-"), std::string::npos);
}

// 测试表达式作为UPDATE的赋值值
TEST_F(ExpressionTest, ExpressionAsUpdateValue) {
    // SET age = age + 1
    auto left = std::make_unique<ColumnRefExpr>("age");
    auto right = std::make_unique<LiteralExpr>("1");
    BinaryOpExpr expr(OpType::ADD, std::move(left), std::move(right));

    EXPECT_EQ(expr.op(), OpType::ADD);
    EXPECT_NE(expr.toString().find("age"), std::string::npos);
    EXPECT_NE(expr.toString().find("+"), std::string::npos);
}
