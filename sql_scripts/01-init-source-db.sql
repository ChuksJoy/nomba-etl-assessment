-- Create the savings_plan table
CREATE TABLE IF NOT EXISTS savings_plan (
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
CREATE TABLE IF NOT EXISTS savings_transaction (
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

-- Insert some sample data into savings_plan
INSERT INTO savings_plan VALUES
('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'TypeA', 'uid_123', 100.00, 'daily', '2025-01-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'TypeB', 'uid_456', 250.00, 'weekly', '2025-02-01', '2025-11-30', 'completed', NOW(), NOW(), NULL);
