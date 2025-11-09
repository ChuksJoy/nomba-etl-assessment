-- models/total_transactions_by_plan.sql
{{ config(materialized='view') }}

SELECT
    st.plan_id,
    sp.product_type,
    COUNT(*) AS total_transactions,
    SUM(st.amount) AS total_amount,
    SUM(CASE WHEN st.side = 'credit' THEN st.amount ELSE 0 END) AS total_credit,
    SUM(CASE WHEN st.side = 'debit' THEN st.amount ELSE 0 END) AS total_debit
FROM {{ source('analytics', 'savings_transaction') }} st
JOIN {{ source('analytics', 'savings_plan') }} sp
    ON st.plan_id = sp.plan_id
GROUP BY st.plan_id, sp.product_type
ORDER BY total_amount DESC