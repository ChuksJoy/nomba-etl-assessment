-- models/total_plans_by_product.sql
{{ config(materialized='view') }}

SELECT
    product_type,
    COUNT(*) AS total_plans,
    SUM(amount) AS total_amount
FROM {{ source('analytics', 'savings_plan') }}
GROUP BY product_type
ORDER BY total_amount DESC