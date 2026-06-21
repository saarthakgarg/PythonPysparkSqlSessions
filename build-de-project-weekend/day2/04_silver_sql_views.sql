-- DAY 2 | Script 4 — Silver SQL Views
-- Run this file against postgres to create analytical views on silver data.
-- Usage: psql -U postgres -d postgres -f 04_silver_sql_views.sql

CREATE SCHEMA IF NOT EXISTS silver;

-- ─────────────────────────────────────────────────────────────────────────────
-- View 1: Order Summary — joins orders with customer name
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.v_order_summary AS
SELECT
    o.order_id,
    o.customer_id,
    o.customer_name,
    c.city          AS customer_city,
    c.state         AS customer_state,
    o.order_date,
    o.status,
    o.payment_method,
    o.total_amount,
    o.is_cancelled,
    o.shipping_city
FROM silver.orders o
LEFT JOIN silver.customers c ON o.customer_id = c.customer_id;


-- ─────────────────────────────────────────────────────────────────────────────
-- View 2: Product Stock — joins products with inventory
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.v_product_stock AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.unit_price,
    p.is_available,
    i.warehouse_id,
    i.stock_qty,
    i.reorder_level,
    i.is_low_stock,
    i.days_since_update,
    i.last_updated
FROM silver.products p
LEFT JOIN silver.inventory i ON p.product_id = i.product_id;


-- ─────────────────────────────────────────────────────────────────────────────
-- View 3: Top Web Endpoints — from bronze.web_logs
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.v_top_endpoints AS
SELECT
    endpoint,
    COUNT(*)                                        AS total_hits,
    COUNT(*) FILTER (WHERE status_code = 200)       AS hits_200,
    COUNT(*) FILTER (WHERE status_code = 404)       AS hits_404,
    COUNT(*) FILTER (WHERE status_code = 500)       AS hits_500,
    COUNT(*) FILTER (WHERE status_code NOT IN (200, 301, 302)) AS error_hits,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status_code >= 400) / COUNT(*), 1
    )                                               AS error_rate_pct,
    ROUND(AVG(response_size), 0)                    AS avg_response_bytes
FROM bronze.web_logs
GROUP BY endpoint
ORDER BY total_hits DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- View 4: Daily order totals (useful for trend queries later)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver.v_daily_orders AS
SELECT
    DATE(order_date)    AS order_day,
    COUNT(*)            AS order_count,
    SUM(total_amount)   AS daily_revenue,
    AVG(total_amount)   AS avg_order_value,
    COUNT(*) FILTER (WHERE is_cancelled) AS cancelled_count
FROM silver.orders
GROUP BY DATE(order_date)
ORDER BY order_day;


-- ─────────────────────────────────────────────────────────────────────────────
-- Quick validation queries (uncomment to run)
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT * FROM silver.v_order_summary LIMIT 5;
-- SELECT * FROM silver.v_product_stock WHERE is_low_stock = TRUE;
-- SELECT * FROM silver.v_top_endpoints;
-- SELECT * FROM silver.v_daily_orders ORDER BY daily_revenue DESC LIMIT 5;
