-- DAY 4 | Final Business Report SQL
-- Run: psql -U postgres -d postgres -f 04_final_report_query.sql
-- Covers: Revenue KPIs, Category breakdown, Customer acquisition,
--         Inventory health, Web traffic summary

\echo ''
\echo '=================================================================='
\echo '   RETAIL ANALYTICS — BUSINESS REPORT'
\echo '=================================================================='


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. OVERALL REVENUE KPIs
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '1. OVERALL REVENUE KPIs'
\echo '──────────────────────'

SELECT
    COUNT(DISTINCT order_id)                          AS total_orders,
    COUNT(DISTINCT order_id) FILTER (WHERE is_cancelled)  AS cancelled_orders,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_cancelled) / COUNT(*), 1
    )                                                 AS cancellation_rate_pct,
    ROUND(SUM(total_amount) FILTER (WHERE NOT is_cancelled)::NUMERIC, 2) AS gross_revenue,
    ROUND(AVG(total_amount) FILTER (WHERE NOT is_cancelled)::NUMERIC, 2) AS avg_order_value,
    COUNT(DISTINCT customer_id)                       AS active_customers,
    MIN(order_date)::DATE                             AS first_order_date,
    MAX(order_date)::DATE                             AS last_order_date
FROM silver.orders;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. TOP 3 CATEGORIES BY REVENUE
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '2. TOP 3 CATEGORIES BY REVENUE'
\echo '──────────────────────────────'

SELECT
    p.category,
    COUNT(DISTINCT oi.order_id)               AS orders,
    SUM(oi.quantity)                          AS units_sold,
    ROUND(SUM(oi.line_total)::NUMERIC, 2)     AS revenue,
    ROUND(
        100.0 * SUM(oi.line_total) /
        SUM(SUM(oi.line_total)) OVER (), 1
    )                                         AS revenue_share_pct
FROM silver.order_items oi
JOIN silver.products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 3;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. CUSTOMER ACQUISITION BY MONTH
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '3. CUSTOMER ACQUISITION BY MONTH'
\echo '────────────────────────────────'

SELECT
    TO_CHAR(DATE_TRUNC('month', signup_date), 'YYYY-MM') AS signup_month,
    COUNT(*)                                              AS new_customers,
    SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('month', signup_date)) AS cumulative_customers
FROM silver.customers
GROUP BY DATE_TRUNC('month', signup_date)
ORDER BY signup_month;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. INVENTORY HEALTH
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '4. INVENTORY HEALTH'
\echo '────────────────────'

SELECT
    COUNT(*)                                     AS total_products,
    COUNT(*) FILTER (WHERE is_low_stock)         AS low_stock_count,
    COUNT(*) FILTER (WHERE stock_qty = 0)        AS out_of_stock_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_low_stock) / COUNT(*), 1
    )                                            AS low_stock_pct,
    ROUND(AVG(stock_qty), 1)                     AS avg_stock_qty,
    ROUND(AVG(days_since_update), 1)             AS avg_days_since_update
FROM silver.inventory;

\echo ''
\echo 'Products at low stock:'
SELECT
    i.product_id,
    p.product_name,
    i.stock_qty,
    i.reorder_level,
    i.days_since_update
FROM silver.inventory i
JOIN silver.products p ON i.product_id = p.product_id
WHERE i.is_low_stock = TRUE
ORDER BY i.stock_qty ASC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. WEB TRAFFIC SUMMARY
-- ─────────────────────────────────────────────────────────────────────────────
\echo ''
\echo '5. WEB TRAFFIC SUMMARY'
\echo '──────────────────────'

SELECT
    COUNT(*)                                                  AS total_requests,
    COUNT(DISTINCT ip)                                        AS unique_ips,
    COUNT(*) FILTER (WHERE status_code = 200)                 AS ok_200,
    COUNT(*) FILTER (WHERE status_code = 404)                 AS not_found_404,
    COUNT(*) FILTER (WHERE status_code = 500)                 AS error_500,
    COUNT(*) FILTER (WHERE status_code = 403)                 AS forbidden_403,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status_code >= 400) / COUNT(*), 1
    )                                                         AS error_rate_pct
FROM bronze.web_logs;

\echo ''
\echo 'Top 5 endpoints by hits:'
SELECT endpoint, COUNT(*) AS hits,
    COUNT(*) FILTER (WHERE status_code >= 400) AS errors
FROM bronze.web_logs
GROUP BY endpoint
ORDER BY hits DESC
LIMIT 5;

\echo ''
\echo '=================================================================='
\echo '   END OF REPORT'
\echo '=================================================================='
