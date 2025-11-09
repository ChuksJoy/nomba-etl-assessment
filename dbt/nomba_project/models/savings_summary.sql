SELECT
    product_type,
    COUNT(*) AS total_plans,
    SUM(amount) AS total_amount
FROM {{ source('analytics', 'savings_plan') }}
GROUP BY product_type