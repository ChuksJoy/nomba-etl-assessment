{{ config(materialized='table') }}

SELECT
    plan_id,
    product_type,
    customer_uid,
    amount,
    frequency,
    start_date,
    end_date,
    status,
    created_at,
    updated_at
FROM {{ ref('stg_savings_plan') }}
-- Optional deduplication if necessary
QUALIFY ROW_NUMBER() OVER (PARTITION BY plan_id ORDER BY updated_at DESC) = 1