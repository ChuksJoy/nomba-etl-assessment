{{ config(materialized='table') }}

SELECT
    customer_uid,
    first_name,
    last_name,
    occupation,
    state
FROM {{ ref('stg_user') }}
-- Optional deduplication
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_uid ORDER BY customer_uid) = 1