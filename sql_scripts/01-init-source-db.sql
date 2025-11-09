-- Create the savings_plan table
CREATE TABLE IF NOT EXISTS analytics.savings_plan (
    plan_id UUID PRIMARY KEY,
    product_type TEXT,
    customer_uid TEXT,
    amount NUMERIC,
    frequency TEXT,
    start_date DATE,
    end_date DATE,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

-- Create the savingsTransaction table
CREATE TABLE IF NOT EXISTS analytics.savings_transaction (
    txn_id UUID PRIMARY KEY,
    plan_id UUID REFERENCES savings_plan(plan_id),
    amount NUMERIC,
    currency TEXT,
    side TEXT,
    rate NUMERIC,
    txn_timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

