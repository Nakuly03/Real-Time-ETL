-- Total Revenue

SELECT
    SUM(total_amount) AS total_revenue
FROM sales_stream;

-- Total Orders

SELECT
    COUNT(*) AS total_orders
FROM sales_stream;

-- Revenue By Product

SELECT
    product,
    SUM(total_amount) AS revenue
FROM sales_stream
GROUP BY product
ORDER BY revenue DESC;

-- Top Selling Product

SELECT
    product,
    SUM(quantity) AS units_sold
FROM sales_stream
GROUP BY product
ORDER BY units_sold DESC;

-- Average Order Value

SELECT
    ROUND(AVG(total_amount),2) AS avg_order_value
FROM sales_stream;

-- Most Expensive Order

SELECT *
FROM sales_stream
ORDER BY total_amount DESC
LIMIT 1;

-- Orders Per Product

SELECT
    product,
    COUNT(*) AS order_count
FROM sales_stream
GROUP BY product
ORDER BY order_count DESC;

-- Today's Revenue

SELECT
    SUM(total_amount) AS today_revenue
FROM sales_stream
WHERE DATE(event_time) = CURDATE();

-- Hourly Trend

SELECT
    HOUR(event_time) AS hour_of_day,
    COUNT(*) AS orders
FROM sales_stream
GROUP BY HOUR(event_time)
ORDER BY hour_of_day;

-- Product Performance

SELECT
    product,
    COUNT(*) AS orders,
    SUM(quantity) AS quantity_sold,
    SUM(total_amount) AS revenue
FROM sales_stream
GROUP BY product
ORDER BY revenue DESC;