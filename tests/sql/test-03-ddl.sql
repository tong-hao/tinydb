-- ============================================
-- DDL (Data Definition Language) 测试
-- ============================================

-- 1. 基本表操作
-- 创建带各种数据类型的表
CREATE TABLE test_types (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INT DEFAULT 0,
    salary FLOAT,
    score DOUBLE,
    is_active BOOL DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP
);

-- 查看表结构
SELECT * FROM tn_tables WHERE table_name = 'test_types';

-- 2. 约束测试
CREATE TABLE test_constraints (
    id INT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE,
    age INT CHECK (age >= 0),
    status VARCHAR(20) DEFAULT 'active'
);

-- 3. ALTER TABLE 测试
-- 添加列
ALTER TABLE test_types ADD COLUMN address VARCHAR(200);

-- 修改列
ALTER TABLE test_types ALTER COLUMN name TYPE VARCHAR(150);

-- 删除列
ALTER TABLE test_types DROP COLUMN description;

-- 重命名列
ALTER TABLE test_types RENAME COLUMN age TO user_age;

-- 重命名表
ALTER TABLE test_types RENAME TO test_types_renamed;

-- 4. 索引操作
CREATE INDEX idx_username ON test_constraints(username);
CREATE UNIQUE INDEX idx_email ON test_constraints(email);

-- 查看索引
SELECT * FROM tn_indexes WHERE table_name = 'test_constraints';

-- 删除索引
DROP INDEX idx_email;

-- 5. 删除表
DROP TABLE test_types_renamed;
DROP TABLE test_constraints;

-- 6. IF NOT EXISTS / IF EXISTS 测试
CREATE TABLE IF NOT EXISTS t_test(id INT);
DROP TABLE IF EXISTS t_test;

