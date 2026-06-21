-- DAY 3 | Script 4 — Gold Layer SQL Analytics
-- Run: psql -U postgres -d postgres -f 04_gold_sql_analytics.sql

CREATE SCHEMA IF NOT EXISTS gold;

-- ─────────────────────────────────────────────────────────────────────────────
-- Query 1: Top 5 customers by total revenue (CTE + RANK)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.top_customers AS
WITH customer_revenue AS (
    SELECT
        o.customer_id,
        c.full_name,
        c.city,
        c.state,
        SUM(o.total_amount)          AS total_revenue,
        COUNT(DISTINCT o.order_id)   AS order_count,
        AVG(o.total_amount)          AS avg_order_value
    FROM silver.orders o
    JOIN silver.customers c ON o.customer_id = c.customer_id
    WHERE o.is_cancelled = FALSE
    GROUP BY o.customer_id, c.full_name, c.city, c.state
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM customer_revenue
)
SELECT * FROM ranked
ORDER BY revenue_rank;

-- View the result:
SELECT * FROM gold.top_customers LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 2: Month-over-Month Revenue Growth (LAG window function)
-- ─────────────────────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date) AS order_month,
        SUM(total_amount)               AS revenue
    FROM silver.orders
    WHERE is_cancelled = FALSE
    GROUP BY DATE_TRUNC('month', order_date)
),
with_lag AS (
    SELECT
        order_month,
        ROUND(revenue::NUMERIC, 2)  AS revenue,
        LAG(revenue) OVER (ORDER BY order_month) AS prev_month_revenue
    FROM monthly
)
SELECT
    TO_CHAR(order_month, 'YYYY-MM')   AS month,
    revenue,
    ROUND(prev_month_revenue::NUMERIC, 2) AS prev_revenue,
    CASE
        WHEN prev_month_revenue IS NULL THEN NULL
        ELSE ROUND(
            100.0 * (revenue - prev_month_revenue) / prev_month_revenue, 1
        )
    END AS growth_pct
FROM with_lag
ORDER BY order_month;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 3: Products NEVER Ordered (anti-join)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    p.is_available
FROM silver.products p
LEFT JOIN silver.order_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL
ORDER BY p.category, p.product_name;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 4: Customer Cohort Retention
-- (signup month vs month of first order)
-- ─────────────────────────────────────────────────────────────────────────────
WITH first_orders AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM silver.orders
    WHERE is_cancelled = FALSE
    GROUP BY customer_id
),
cohort AS (
    SELECT
        c.customer_id,
        c.full_name,
        TO_CHAR(DATE_TRUNC('month', c.signup_date), 'YYYY-MM')     AS signup_month,
        TO_CHAR(DATE_TRUNC('month', fo.first_order_date), 'YYYY-MM') AS first_order_month,
        DATE_PART('month', AGE(fo.first_order_date, c.signup_date)) AS months_to_first_order
    FROM silver.customers c
    LEFT JOIN first_orders fo ON c.customer_id = fo.customer_id
)
SELECT *
FROM cohort
ORDER BY signup_month, months_to_first_order NULLS LAST;


-- ─────────────────────────────────────────────────────────────────────────────
-- Query 5: Revenue by Product Category (gold summary)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.category_revenue AS
SELECT
    p.category,
    COUNT(DISTINCT oi.order_id)   AS orders,
    SUM(oi.quantity)              AS units_sold,
    ROUND(SUM(oi.line_total)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(oi.line_total)::NUMERIC, 2) AS avg_line_total,
    RANK() OVER (ORDER BY SUM(oi.line_total) DESC) AS revenue_rank
FROM silver.order_items oi
JOIN silver.products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;

SELECT * FROM gold.category_revenue;
