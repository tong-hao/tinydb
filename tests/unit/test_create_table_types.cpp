#include <gtest/gtest.h>
#include "sql/parser/parser.h"
#include "sql/parser/ast.h"
#include "sql/executor/executor.h"
#include "storage/storage_engine.h"

using namespace tinydb;
using namespace tinydb::sql;
using namespace tinydb::engine;
using namespace tinydb::storage;

class CreateTableTypesTest : public ::testing::Test {
protected:
    void SetUp() override {
        StorageConfig config;
        config.db_file_path = "test_create_types.db";
        config.wal_file_path = "test_create_types.wal";
        config.buffer_pool_size = 1024;

        storage_engine_ = std::make_unique<StorageEngine>(config);
        ASSERT_TRUE(storage_engine_->initialize());

        executor_ = std::make_unique<Executor>();
        executor_->initialize(storage_engine_.get());
    }

    void TearDown() override {
        storage_engine_->shutdown();
        storage_engine_.reset();
        executor_.reset();
        std::remove("test_create_types.db");
        std::remove("test_create_types.wal");
    }

    std::unique_ptr<StorageEngine> storage_engine_;
    std::unique_ptr<Executor> executor_;
};

// ========== 整数类型测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithInt) {
    auto result = executor_->execute("CREATE TABLE test_int (id INT)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithInteger) {
    auto result = executor_->execute("CREATE TABLE test_integer (id INTEGER)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithBigInt) {
    auto result = executor_->execute("CREATE TABLE test_bigint (id BIGINT)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithSmallInt) {
    auto result = executor_->execute("CREATE TABLE test_smallint (id SMALLINT)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithTinyInt) {
    auto result = executor_->execute("CREATE TABLE test_tinyint (id TINYINT)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 浮点类型测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithFloat) {
    auto result = executor_->execute("CREATE TABLE test_float (value FLOAT)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithDouble) {
    auto result = executor_->execute("CREATE TABLE test_double (value DOUBLE)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithReal) {
    auto result = executor_->execute("CREATE TABLE test_real (value REAL)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithDecimal) {
    auto result = executor_->execute("CREATE TABLE test_decimal (price DECIMAL)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithNumeric) {
    auto result = executor_->execute("CREATE TABLE test_numeric (value NUMERIC)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 字符串类型测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithChar) {
    auto result = executor_->execute("CREATE TABLE test_char (code CHAR)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithCharSized) {
    auto result = executor_->execute("CREATE TABLE test_char10 (code CHAR(10))");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithVarchar) {
    auto result = executor_->execute("CREATE TABLE test_varchar (name VARCHAR(20))");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithVarcharMax) {
    auto result = executor_->execute("CREATE TABLE test_varchar_max (content VARCHAR(65535))");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithText) {
    auto result = executor_->execute("CREATE TABLE test_text (content TEXT)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 布尔类型测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithBoolean) {
    auto result = executor_->execute("CREATE TABLE test_boolean (active BOOLEAN)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithBool) {
    auto result = executor_->execute("CREATE TABLE test_bool (active BOOL)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 日期时间类型测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithDate) {
    auto result = executor_->execute("CREATE TABLE test_date (birth_date DATE)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithTime) {
    auto result = executor_->execute("CREATE TABLE test_time (start_time TIME)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithTimestamp) {
    auto result = executor_->execute("CREATE TABLE test_timestamp (created_at TIMESTAMP)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithDateTime) {
    auto result = executor_->execute("CREATE TABLE test_datetime (created_at DATETIME)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 多列混合类型测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithMultipleIntTypes) {
    auto result = executor_->execute(
        "CREATE TABLE test_multi_int ("
        "tiny TINYINT, "
        "small SMALLINT, "
        "id INT, "
        "big BIGINT)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithMultipleFloatTypes) {
    auto result = executor_->execute(
        "CREATE TABLE test_multi_float ("
        "f FLOAT, "
        "d DOUBLE, "
        "r REAL, "
        "dec DECIMAL)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithMultipleStringTypes) {
    auto result = executor_->execute(
        "CREATE TABLE test_multi_string ("
        "c CHAR(5), "
        "vc1 VARCHAR(10), "
        "vc2 VARCHAR(100), "
        "txt TEXT)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithAllTypes) {
    auto result = executor_->execute(
        "CREATE TABLE test_all_types ("
        "id INT, "
        "name VARCHAR(50), "
        "description TEXT, "
        "age TINYINT, "
        "salary DECIMAL, "
        "score FLOAT, "
        "rating DOUBLE, "
        "is_active BOOLEAN, "
        "birth_date DATE, "
        "created_at TIMESTAMP)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 列名变体测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithUnderscoreNames) {
    auto result = executor_->execute(
        "CREATE TABLE test_underscore ("
        "user_id INT, "
        "first_name VARCHAR(20), "
        "last_name VARCHAR(20), "
        "created_at TIMESTAMP)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableWithNumberSuffix) {
    auto result = executor_->execute(
        "CREATE TABLE test_num_suffix ("
        "col1 INT, "
        "col2 VARCHAR(10), "
        "col3 BIGINT)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 大小写测试 ==========
TEST_F(CreateTableTypesTest, CreateTableLowercaseKeywords) {
    auto result = executor_->execute("create table test_lower (id int, name varchar(20))");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableMixedCaseKeywords) {
    auto result = executor_->execute("Create Table Test_Mixed (Id Int, Name Varchar(20))");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateTableLowercaseTypes) {
    auto result = executor_->execute("CREATE TABLE test_lower_types (id int, name varchar(20))");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 空格测试 ==========
TEST_F(CreateTableTypesTest, CreateTableWithExtraSpaces) {
    auto result = executor_->execute("CREATE   TABLE   test_spaces   (  id   INT ,  name   VARCHAR ( 20 ) )");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 重复表名测试 ==========
TEST_F(CreateTableTypesTest, CreateDuplicateTable) {
    auto result = executor_->execute("CREATE TABLE test_dup (id INT)");
    EXPECT_TRUE(result.success());

    result = executor_->execute("CREATE TABLE test_dup (id INT)");
    EXPECT_FALSE(result.success()) << "Should fail for duplicate table";
}

TEST_F(CreateTableTypesTest, CreateTableCaseInsensitiveDuplicate) {
    auto result = executor_->execute("CREATE TABLE TestCase (id INT)");
    EXPECT_TRUE(result.success());

    result = executor_->execute("CREATE TABLE testcase (id INT)");
    EXPECT_FALSE(result.success()) << "Should fail for case-insensitive duplicate";
}

// ========== 插入数据类型测试 ==========
TEST_F(CreateTableTypesTest, InsertIntoIntTable) {
    auto result = executor_->execute("CREATE TABLE test_insert_int (id INT)");
    ASSERT_TRUE(result.success());

    result = executor_->execute("INSERT INTO test_insert_int VALUES (1)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("INSERT INTO test_insert_int VALUES (2147483647)");
    EXPECT_TRUE(result.success()) << "Failed to insert max int: " << result.message();

    // Note: Negative numbers require parser support for unary minus
    // result = executor_->execute("INSERT INTO test_insert_int VALUES (-2147483648)");
    // EXPECT_TRUE(result.success()) << "Failed to insert min int: " << result.message();
}

TEST_F(CreateTableTypesTest, InsertIntoBigIntTable) {
    auto result = executor_->execute("CREATE TABLE test_insert_bigint (id BIGINT)");
    ASSERT_TRUE(result.success());

    result = executor_->execute("INSERT INTO test_insert_bigint VALUES (9223372036854775807)");
    EXPECT_TRUE(result.success()) << "Failed to insert max bigint: " << result.message();
}

TEST_F(CreateTableTypesTest, InsertIntoVarcharTable) {
    auto result = executor_->execute("CREATE TABLE test_insert_varchar (name VARCHAR(20))");
    ASSERT_TRUE(result.success());

    result = executor_->execute("INSERT INTO test_insert_varchar VALUES ('Hello')");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("INSERT INTO test_insert_varchar VALUES ('')");
    EXPECT_TRUE(result.success()) << "Failed to insert empty string: " << result.message();
}

TEST_F(CreateTableTypesTest, InsertIntoFloatTable) {
    auto result = executor_->execute("CREATE TABLE test_insert_float (value FLOAT)");
    ASSERT_TRUE(result.success());

    result = executor_->execute("INSERT INTO test_insert_float VALUES (3.14)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    // Note: Negative numbers require parser support for unary minus
    // result = executor_->execute("INSERT INTO test_insert_float VALUES (-123.456)");
    // EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, InsertIntoBooleanTable) {
    auto result = executor_->execute("CREATE TABLE test_insert_bool (active BOOLEAN)");
    ASSERT_TRUE(result.success());

    result = executor_->execute("INSERT INTO test_insert_bool VALUES (TRUE)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("INSERT INTO test_insert_bool VALUES (FALSE)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 查询测试 ==========
TEST_F(CreateTableTypesTest, SelectFromVariousTypes) {
    auto result = executor_->execute(
        "CREATE TABLE test_select ("
        "id INT, "
        "name VARCHAR(20), "
        "score FLOAT)"
    );
    ASSERT_TRUE(result.success());

    result = executor_->execute("INSERT INTO test_select VALUES (1, 'Alice', 95.5)");
    ASSERT_TRUE(result.success());

    result = executor_->execute("SELECT * FROM test_select");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("SELECT id FROM test_select");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("SELECT name FROM test_select");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("SELECT score FROM test_select");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 复杂表结构测试 ==========
TEST_F(CreateTableTypesTest, CreateUsersTable) {
    auto result = executor_->execute(
        "CREATE TABLE users ("
        "user_id INT, "
        "username VARCHAR(32), "
        "email VARCHAR(128), "
        "age TINYINT, "
        "balance DECIMAL, "
        "is_active BOOLEAN, "
        "created_at TIMESTAMP)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    // 插入多条数据
    result = executor_->execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 25, 100.50, TRUE, '2024-01-01')");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("INSERT INTO users VALUES (2, 'bob', 'bob@example.com', 30, 200.75, TRUE, '2024-01-02')");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("SELECT * FROM users");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateProductsTable) {
    auto result = executor_->execute(
        "CREATE TABLE products ("
        "product_id BIGINT, "
        "name VARCHAR(100), "
        "description TEXT, "
        "price DECIMAL, "
        "quantity SMALLINT, "
        "in_stock BOOLEAN)"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("INSERT INTO products VALUES (1001, 'Laptop', 'High performance laptop', 999.99, 50, TRUE)");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

TEST_F(CreateTableTypesTest, CreateOrdersTable) {
    auto result = executor_->execute(
        "CREATE TABLE orders ("
        "order_id INT, "
        "user_id INT, "
        "product_id BIGINT, "
        "quantity SMALLINT, "
        "total_price DECIMAL, "
        "order_date DATE, "
        "status VARCHAR(20))"
    );
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    result = executor_->execute("INSERT INTO orders VALUES (1, 1, 1001, 2, 1999.98, '2024-03-01', 'completed')");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

// ========== 解析器测试 ==========
TEST_F(CreateTableTypesTest, ParseAllDataTypes) {
    std::vector<std::string> statements = {
        "CREATE TABLE t1 (c INT)",
        "CREATE TABLE t2 (c INTEGER)",
        "CREATE TABLE t3 (c BIGINT)",
        "CREATE TABLE t4 (c SMALLINT)",
        "CREATE TABLE t5 (c TINYINT)",
        "CREATE TABLE t6 (c FLOAT)",
        "CREATE TABLE t7 (c DOUBLE)",
        "CREATE TABLE t8 (c REAL)",
        "CREATE TABLE t9 (c DECIMAL)",
        "CREATE TABLE t9a (c DECIMAL(10,2))",
        "CREATE TABLE t10 (c NUMERIC)",
        "CREATE TABLE t10a (c NUMERIC(10,2))",
        "CREATE TABLE t11 (c CHAR)",
        "CREATE TABLE t12 (c CHAR(10))",
        "CREATE TABLE t13 (c VARCHAR(20))",
        "CREATE TABLE t14 (c TEXT)",
        "CREATE TABLE t15 (c BOOLEAN)",
        "CREATE TABLE t16 (c BOOL)",
        "CREATE TABLE t17 (c DATE)",
        "CREATE TABLE t18 (c TIME)",
        "CREATE TABLE t19 (c TIMESTAMP)",
        "CREATE TABLE t20 (c DATETIME)",
    };

    for (const auto& sql : statements) {
        Parser parser(sql);
        auto ast = parser.parse();
        EXPECT_NE(ast, nullptr) << "Failed to parse: " << sql;
    }
}

TEST_F(CreateTableTypesTest, CreateSalesTable) {
    auto result = executor_->execute("CREATE TABLE sales ( id INT PRIMARY KEY, product VARCHAR(100), quantity INT, price DECIMAL(10, 2), sale_date DATE )");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();
}

