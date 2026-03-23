-- ============================================
-- 高级功能测试
-- ============================================

-- 1. 视图测试
CREATE TABLE sales (
    id INT PRIMARY KEY,
    product VARCHAR(100),
    quantity INT,
    price DECIMAL(10, 2),
    sale_date DATE
);

INSERT INTO sales VALUES (1, 'Laptop', 2, 999.99, '2024-01-15');
INSERT INTO sales VALUES (2, 'Mouse', 10, 29.99, '2024-01-16');
INSERT INTO sales VALUES (3, 'Keyboard', 5, 79.99, '2024-01-17');

-- 创建视图
CREATE VIEW v_sales_summary AS
SELECT product, quantity, quantity * price as total_amount
FROM sales;

-- 查询视图
SELECT * FROM v_sales_summary;

-- 带条件的视图查询
SELECT * FROM v_sales_summary WHERE total_amount > 100;

-- 删除视图
DROP VIEW IF EXISTS v_sales_summary;

-- 2. 复杂查询场景
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT,
    product_name VARCHAR(100),
    quantity INT,
    unit_price DECIMAL(10, 2)
);

INSERT INTO orders VALUES (1, 101, '2024-01-15 10:00:00', 'completed');
INSERT INTO orders VALUES (2, 102, '2024-01-16 14:30:00', 'pending');
INSERT INTO orders VALUES (3, 101, '2024-01-17 09:00:00', 'completed');

INSERT INTO order_items VALUES (1, 1, 'Product A', 2, 50.00);
INSERT INTO order_items VALUES (2, 1, 'Product B', 1, 100.00);
INSERT INTO order_items VALUES (3, 2, 'Product C', 5, 20.00);
INSERT INTO order_items VALUES (4, 3, 'Product A', 3, 50.00);

-- 复杂 JOIN 和聚合 【join还不支持】
-- SELECT
--     o.order_id,
--     o.customer_id,
--     o.status,
--     COUNT(oi.item_id) as item_count,
--     SUM(oi.quantity * oi.unit_price) as total_value
-- FROM orders o
-- JOIN order_items oi ON o.order_id = oi.order_id
-- GROUP BY o.order_id, o.customer_id, o.status
-- ORDER BY total_value DESC;

-- 子查询作为派生表【不支持】
-- SELECT * FROM (
--     SELECT customer_id, COUNT(*) as order_count
--     FROM orders
--     GROUP BY customer_id
-- ) as customer_orders
-- WHERE order_count > 1;


-- UNION 测试【不支持】
-- SELECT product_name FROM order_items WHERE quantity > 2
-- UNION
-- SELECT product_name FROM order_items WHERE unit_price > 40;

-- INTERSECT / EXCEPT【不支持】
-- SELECT customer_id FROM orders WHERE status = 'completed'
-- INTERSECT
-- SELECT customer_id FROM orders WHERE order_date > '2024-01-15';

-- 清理
DROP TABLE sales;
DROP TABLE order_items;
DROP TABLE orders;

-- 3. 表达式和函数测试
CREATE TABLE test_expr (id INT, value INT);
INSERT INTO test_expr VALUES (1, 10), (2, 20), (3, NULL);

-- 算术表达式
SELECT id, value, value * 2, value + 5, value - 3 FROM test_expr;

-- CASE 表达式【不支持】
-- SELECT id, value,
--     CASE
--         WHEN value > 15 THEN 'high'
--         WHEN value > 10 THEN 'medium'
--         ELSE 'low'
--     END as category
-- FROM test_expr;

-- COALESCE / NULLIF【不支持】
-- SELECT id, COALESCE(value, 0) as safe_value FROM test_expr;

DROP TABLE test_expr;

-- 4. 并发和锁定测试（需要多会话）
-- SELECT * FROM tn_locks;

-- 5. 权限测试【不支持】
-- CREATE USER test_user WITH PASSWORD 'password';
-- GRANT SELECT ON orders TO test_user;
-- REVOKE SELECT ON orders FROM test_user;
-- DROP USER test_user;

