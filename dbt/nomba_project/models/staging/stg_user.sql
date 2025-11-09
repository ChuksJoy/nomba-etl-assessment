{{ config(materialized='view') }}

SELECT
    _id AS user_id,
    Uid AS customer_uid,
    firstName AS first_name,
    lastName AS last_name,
    occupation,
    state
FROM {{ source('analytics', 'user') }}
WHERE Uid IS NOT NULL