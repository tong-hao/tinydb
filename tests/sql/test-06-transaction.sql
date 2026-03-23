-- ============================================
-- 事务控制测试
-- ============================================

CREATE TABLE accounts (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    balance INT
);

INSERT INTO accounts VALUES (1, 'Alice', 1000);
INSERT INTO accounts VALUES (2, 'Bob', 2000);
INSERT INTO accounts VALUES (3, 'Charlie', 1500);

-- 查看初始数据
SELECT * FROM accounts;

-- 1. 基本事务提交
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;

-- 验证提交结果
SELECT * FROM accounts;

-- 2. 事务回滚
BEGIN;
UPDATE accounts SET balance = balance - 500 WHERE id = 1;
UPDATE accounts SET balance = balance + 500 WHERE id = 3;
-- 发现错误，回滚
ROLLBACK;

-- 验证回滚结果（数据应恢复）
SELECT * FROM accounts;

-- 3. 保存点测试（如果支持）
-- BEGIN;
-- UPDATE accounts SET balance = balance - 200 WHERE id = 1;
-- SAVEPOINT sp1;
-- UPDATE accounts SET balance = balance + 200 WHERE id = 2;
-- -- 回滚到保存点
-- ROLLBACK TO sp1;
-- COMMIT;

-- 4. 事务隔离测试（多会话，手动测试）
-- Session 1:
-- BEGIN;
-- SELECT * FROM accounts WHERE id = 1;
-- -- 在 Session 2 中更新同一行
-- -- 回到 Session 1:
-- SELECT * FROM accounts WHERE id = 1; -- 检查是否看到未提交更改
-- COMMIT;

-- 清理
DROP TABLE accounts;

