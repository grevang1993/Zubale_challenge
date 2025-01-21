-- Query: Date with the highest number of orders
SELECT created_date, COUNT(*) AS order_count
FROM orders
GROUP BY created_date
ORDER BY order_count DESC
LIMIT 1;

--Query: Most demanded product
SELECT p.name, SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.id, p.name
ORDER BY total_quantity DESC
LIMIT 1;

--Query: Top 3 most demanded categories
SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY p.category
ORDER BY total_quantity DESC
LIMIT 3;