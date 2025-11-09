-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- TRUNCATE existing data to avoid duplicates
TRUNCATE TABLE analytics.savings_transaction CASCADE;
TRUNCATE TABLE analytics.savings_plan CASCADE;

-- Insert 20 rows into savings_plan
INSERT INTO analytics.savings_plan 
(plan_id, product_type, customer_uid, amount, frequency, start_date, end_date, status, created_at, updated_at, deleted_at)
VALUES
(gen_random_uuid(), 'TypeA', 'cust_001', 100.00, 'daily', '2025-01-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_002', 200.00, 'weekly', '2025-02-01', '2025-11-30', 'completed', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeC', 'cust_003', 150.00, 'monthly', '2025-03-01', '2025-09-30', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeA', 'cust_004', 300.00, 'daily', '2025-04-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_005', 120.00, 'weekly', '2025-05-01', '2025-10-31', 'completed', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeC', 'cust_006', 220.00, 'monthly', '2025-06-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeA', 'cust_007', 180.00, 'daily', '2025-07-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_008', 250.00, 'weekly', '2025-08-01', '2025-11-30', 'completed', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeC', 'cust_009', 200.00, 'monthly', '2025-09-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeA', 'cust_010', 300.00, 'daily', '2025-10-01', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_011', 100.00, 'weekly', '2025-01-15', '2025-12-31', 'completed', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeC', 'cust_012', 150.00, 'monthly', '2025-02-15', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeA', 'cust_013', 180.00, 'daily', '2025-03-15', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_014', 220.00, 'weekly', '2025-04-15', '2025-12-31', 'completed', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeC', 'cust_015', 250.00, 'monthly', '2025-05-15', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeA', 'cust_016', 200.00, 'daily', '2025-06-15', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_017', 120.00, 'weekly', '2025-07-15', '2025-12-31', 'completed', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeC', 'cust_018', 150.00, 'monthly', '2025-08-15', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeA', 'cust_019', 180.00, 'daily', '2025-09-15', '2025-12-31', 'active', NOW(), NOW(), NULL),
(gen_random_uuid(), 'TypeB', 'cust_020', 220.00, 'weekly', '2025-10-15', '2025-12-31', 'completed', NOW(), NOW(), NULL);

-- Insert 20 rows into savings_transaction linked to savings_plan
INSERT INTO analytics.savings_transaction 
(txn_id, plan_id, amount, currency, side, rate, txn_timestamp, updated_at, deleted_at)
SELECT
  gen_random_uuid(),
  plan_id,
  ROUND((RANDOM() * 500 + 50)::numeric, 2),
  'USD',
  CASE WHEN RANDOM() > 0.5 THEN 'credit' ELSE 'debit' END,
  1.0,
  NOW() - ((RANDOM() * 30)::INT || ' days')::INTERVAL,
  NOW(),
  NULL
FROM analytics.savings_plan
LIMIT 20;
