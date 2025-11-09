{{ config(materialized='view') }}

SELECT
    plan_id,
    product_type,
    customer_uid,
    amount::numeric AS amount,
    frequency,
    start_date::date AS start_date,
    end_date::date AS end_date,
    status,
    created_at::timestamp AS created_at,
    updated_at::timestamp AS updated_at,
    deleted_at::timestamp AS deleted_at
FROM {{ source('analytics', 'savings_plan') }}
WHERE deleted_at IS NULL