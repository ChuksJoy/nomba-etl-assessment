-- models/active_plans_summary.sql
{{ config(materialized='view') }}

SELECT
    product_type,
    COUNT(*) AS active_plans,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM {{ source('analytics', 'savings_plan') }}
WHERE status = 'active'
GROUP BY product_type
ORDER BY total_amount DESC
