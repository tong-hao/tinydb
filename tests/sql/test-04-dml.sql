-- ============================================
-- DML (Data Manipulation Language) 测试
-- ============================================

-- 准备测试表
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50),
    salary INT,
    age INT,
    join_date TIMESTAMP
);

CREATE TABLE departments (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100)
);

-- 1. INSERT 测试
-- 单行插入
INSERT INTO departments VALUES (1, 'Engineering', 'Building A');
INSERT INTO departments VALUES (2, 'Sales', 'Building B');
INSERT INTO departments VALUES (3, 'HR', 'Building C');

-- 指定列插入
INSERT INTO employees (id, name, department, salary, age) VALUES (1, 'Alice', 'Engineering', 8000, 28);
INSERT INTO employees (id, name, department, salary, age) VALUES (2, 'Bob', 'Sales', 6000, 32);
INSERT INTO employees (id, name, department, salary, age) VALUES (3, 'Charlie', 'Engineering', 9000, 35);
INSERT INTO employees (id, name, department, salary, age) VALUES (4, 'David', 'HR', 5000, 29);
INSERT INTO employees (id, name, department, salary, age) VALUES (5, 'Eve', 'Sales', 6500, 31);

-- 批量插入（如果支持）
INSERT INTO employees VALUES (6, 'Frank', 'Engineering', 7500, 27, NULL);
INSERT INTO employees VALUES (7, 'Grace', 'HR', 5200, 26, NULL);

-- 2. SELECT 基础查询
SELECT * FROM employees;
SELECT * FROM departments;

-- 选择特定列
SELECT name, salary FROM employees;

-- 3. WHERE 条件测试
-- 比较运算
SELECT * FROM employees WHERE salary > 6000;
SELECT * FROM employees WHERE age >= 30;
SELECT * FROM employees WHERE department = 'Engineering';

-- AND/OR 条件
SELECT * FROM employees WHERE department = 'Engineering' AND salary > 8000;
SELECT * FROM employees WHERE department = 'Sales' OR department = 'HR';

-- IN / NOT IN
SELECT * FROM employees WHERE department IN ('Engineering', 'Sales');
SELECT * FROM employees WHERE id NOT IN (1, 2, 3);

-- BETWEEN
SELECT * FROM employees WHERE salary BETWEEN 5000 AND 7000;
SELECT * FROM employees WHERE age BETWEEN 25 AND 30;

-- LIKE 模糊查询
SELECT * FROM employees WHERE name LIKE 'A%';
SELECT * FROM employees WHERE name LIKE '%e';
SELECT * FROM employees WHERE name LIKE '%li%';

-- IS NULL
SELECT * FROM employees WHERE join_date IS NULL;

-- 4. ORDER BY 排序
SELECT * FROM employees ORDER BY salary;
SELECT * FROM employees ORDER BY salary DESC;
SELECT * FROM employees ORDER BY department ASC, salary DESC;

-- 5. LIMIT / OFFSET（如果支持）
SELECT * FROM employees LIMIT 3;
SELECT * FROM employees ORDER BY id LIMIT 3 OFFSET 2;

-- 6. DISTINCT 去重
SELECT DISTINCT department FROM employees;

-- 7. UPDATE 测试
UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering';
SELECT * FROM employees WHERE department = 'Engineering';

UPDATE employees SET department = 'Marketing', salary = 5500 WHERE id = 4;
SELECT * FROM employees WHERE id = 4;

-- 8. DELETE 测试
DELETE FROM employees WHERE id = 7;
SELECT * FROM employees;

-- DELETE 带条件
DELETE FROM employees WHERE salary < 5500;
SELECT * FROM employees;

-- 9. 聚合函数
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM employees WHERE department = 'Engineering';
SELECT SUM(salary) FROM employees;
SELECT AVG(salary) FROM employees;
SELECT MAX(salary) FROM employees;
SELECT MIN(salary) FROM employees;

-- 10. GROUP BY 分组
SELECT department, COUNT(*) as count FROM employees GROUP BY department;
SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department;
SELECT department, MAX(salary) as max_salary, MIN(salary) as min_salary FROM employees GROUP BY department;

-- 11. HAVING 过滤
SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department HAVING AVG(salary) > 6000;

-- 12. JOIN 连接查询
SELECT e.name, e.department, d.location FROM employees e JOIN departments d ON e.department = d.name;
SELECT * FROM employees e LEFT JOIN departments d ON e.department = d.name;

-- 13. 子查询
SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees);
SELECT * FROM employees WHERE department IN (SELECT name FROM departments WHERE location = 'Building A');

-- 14. EXISTS 子查询
SELECT * FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department = d.name);

-- 15. TRUNCATE 清空表（保留表结构）
-- CREATE TABLE t_truncate_test(id INT);
-- INSERT INTO t_truncate_test VALUES (1), (2), (3);
-- SELECT * FROM t_truncate_test;
-- TRUNCATE TABLE t_truncate_test;
-- SELECT * FROM t_truncate_test;
-- DROP TABLE t_truncate_test;

-- 清理
DROP TABLE employees;
DROP TABLE departments;

