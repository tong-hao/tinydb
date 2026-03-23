-- ============================================
-- 边界情况和异常测试
-- ============================================

-- 1. 空表测试
CREATE TABLE empty_table (id INT, name VARCHAR(50));
SELECT * FROM empty_table;
SELECT COUNT(*) FROM empty_table;
DROP TABLE empty_table;

-- 2. 单条记录测试
CREATE TABLE single_row (id INT PRIMARY KEY, value VARCHAR(50));
INSERT INTO single_row VALUES (1, 'only one');
SELECT * FROM single_row;
UPDATE single_row SET value = 'updated';
SELECT * FROM single_row;
DELETE FROM single_row;
SELECT * FROM single_row;
DROP TABLE single_row;

-- 3. 大值测试
CREATE TABLE large_values (
    id INT PRIMARY KEY,
    long_string VARCHAR(10000),
    large_int INT
);

-- 插入边界值
INSERT INTO large_values VALUES (1, 'a', 0);
INSERT INTO large_values VALUES (2, 'test string with some length', 2147483647);
INSERT INTO large_values VALUES (3, '', -2147483648);

SELECT * FROM large_values;
DROP TABLE large_values;

-- 4. 特殊字符测试
CREATE TABLE special_chars (id INT, name VARCHAR(100), description TEXT);
INSERT INTO special_chars VALUES (1, 'Test''s Name', 'Line 1\nLine 2');
INSERT INTO special_chars VALUES (2, 'Name with ""quotes""', 'Tab\there');
INSERT INTO special_chars VALUES (3, '中文测试', 'Unicode content: 🎉');

SELECT * FROM special_chars;
DROP TABLE special_chars;

-- 5. 重复值测试
CREATE TABLE duplicates (value INT);
INSERT INTO duplicates VALUES (1), (1), (2), (2), (3);
SELECT * FROM duplicates;
SELECT DISTINCT value FROM duplicates;
DROP TABLE duplicates;

-- 6. NULL 值处理
CREATE TABLE null_test (id INT, col1 INT, col2 VARCHAR(50));
INSERT INTO null_test VALUES (1, NULL, NULL);
INSERT INTO null_test VALUES (2, 10, 'test');
INSERT INTO null_test VALUES (3, NULL, 'not null');
INSERT INTO null_test VALUES (4, 20, NULL);

-- NULL 比较
SELECT * FROM null_test WHERE col1 IS NULL;
SELECT * FROM null_test WHERE col1 IS NOT NULL;
SELECT * FROM null_test WHERE col2 IS NULL;

-- NULL 在聚合中
SELECT COUNT(*) FROM null_test;
SELECT COUNT(col1) FROM null_test;
SELECT AVG(col1) FROM null_test;
SELECT SUM(col1) FROM null_test;

-- 7. 自引用/递归测试（如果支持 CTE）
-- CREATE TABLE employees_hierarchy (
--     id INT PRIMARY KEY,
--     name VARCHAR(100),
--     manager_id INT
-- );
-- INSERT INTO employees_hierarchy VALUES (1, 'CEO', NULL);
-- INSERT INTO employees_hierarchy VALUES (2, 'Manager A', 1);
-- INSERT INTO employees_hierarchy VALUES (3, 'Manager B', 1);
-- INSERT INTO employees_hierarchy VALUES (4, 'Employee A1', 2);
-- INSERT INTO employees_hierarchy VALUES (5, 'Employee A2', 2);
-- INSERT INTO employees_hierarchy VALUES (6, 'Employee B1', 3);

-- 8. 外键测试（如果支持）
-- CREATE TABLE parent (
--     id INT PRIMARY KEY,
--     name VARCHAR(50)
-- );
-- CREATE TABLE child (
--     id INT PRIMARY KEY,
--     parent_id INT REFERENCES parent(id),
--     name VARCHAR(50)
-- );
-- INSERT INTO parent VALUES (1, 'Parent 1');
-- INSERT INTO child VALUES (1, 1, 'Child 1');
-- -- 以下应该失败（外键约束）
-- -- INSERT INTO child VALUES (2, 999, 'Child with invalid parent');

-- 9. 大数据量性能测试（可选）
-- CREATE TABLE performance_test (id INT PRIMARY KEY, data VARCHAR(100));
-- -- 插入大量数据
-- -- INSERT INTO performance_test SELECT generate_series(1, 100000), 'data';
-- -- 测试查询性能
-- -- SELECT COUNT(*) FROM performance_test;
-- -- SELECT * FROM performance_test WHERE id = 50000;
-- -- DROP TABLE performance_test;

-- 10. 并发冲突测试（需要多会话）
-- Session 1: BEGIN; UPDATE test_table SET value = value + 1 WHERE id = 1;
-- Session 2: BEGIN; UPDATE test_table SET value = value + 1 WHERE id = 1; -- 可能阻塞或报错
-- Session 1: COMMIT;
-- Session 2: -- 继续执行

