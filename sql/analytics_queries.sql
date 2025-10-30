-- Sample analytical queries for the data warehouse

-- 1. Daily transaction summary
SELECT 
    d.date,
    d.day_name,
    COUNT(f.transaction_id) as transaction_count,
    ROUND(SUM(f.amount), 2) as total_revenue,
    ROUND(AVG(f.amount), 2) as avg_transaction_amount
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.date, d.day_name
ORDER BY d.date DESC
LIMIT 30;

-- 2. Top performing merchants
SELECT 
    m.merchant_name,
    m.category,
    m.city,
    COUNT(f.transaction_id) as transaction_count,
    ROUND(SUM(f.amount), 2) as total_revenue
FROM fact_transactions f
JOIN dim_merchants m ON f.merchant_key = m.merchant_key
WHERE f.status = 'Completed'
GROUP BY m.merchant_name, m.category, m.city
ORDER BY total_revenue DESC
LIMIT 20;

-- 3. Customer segmentation analysis
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) as customer_count,
    COUNT(f.transaction_id) as transaction_count,
    ROUND(AVG(f.amount), 2) as avg_transaction_amount,
    ROUND(SUM(f.amount), 2) as total_revenue
FROM dim_customers c
LEFT JOIN fact_transactions f ON c.customer_key = f.customer_key
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- 4. Monthly trends
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.transaction_id) as transaction_count,
    ROUND(SUM(f.amount), 2) as total_revenue,
    ROUND(AVG(f.amount), 2) as avg_transaction_amount
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- 5. Transaction type distribution
SELECT 
    tt.transaction_type,
    tt.description,
    COUNT(f.transaction_id) as transaction_count,
    ROUND(SUM(f.amount), 2) as total_amount,
    ROUND(AVG(f.amount), 2) as avg_amount
FROM fact_transactions f
JOIN dim_transaction_type tt ON f.transaction_type_key = tt.transaction_type_key
GROUP BY tt.transaction_type, tt.description
ORDER BY transaction_count DESC;

-- 6. Weekend vs Weekday analysis
SELECT 
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(f.transaction_id) as transaction_count,
    ROUND(SUM(f.amount), 2) as total_revenue,
    ROUND(AVG(f.amount), 2) as avg_transaction_amount
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend;

-- 7. Payment method preferences
SELECT 
    payment_method,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY payment_method
ORDER BY transaction_count DESC;

-- 8. Failed transaction analysis
SELECT 
    d.date,
    COUNT(*) as failed_count,
    ROUND(SUM(amount), 2) as failed_amount
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.status = 'Failed'
GROUP BY d.date
ORDER BY failed_count DESC
LIMIT 10;
