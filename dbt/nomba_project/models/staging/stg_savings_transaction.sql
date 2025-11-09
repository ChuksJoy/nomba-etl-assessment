{{ config(materialized='view') }}

SELECT
    txn_id,
    plan_id,
    amount::numeric AS amount,
    currency,
    side,
    rate::numeric AS rate,
    txn_timestamp::timestamp AS txn_timestamp,
    updated_at::timestamp AS updated_at,
    deleted_at::timestamp AS deleted_at
FROM {{ source('analytics', 'savings_transaction') }}
WHERE deleted_at IS NULL
  AND plan_id IS NOT NULL