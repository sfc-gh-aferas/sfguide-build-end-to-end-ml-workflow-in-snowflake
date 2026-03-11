-- Using ACCOUNTADMIN, create a new role for this exercise 
USE ROLE ACCOUNTADMIN;
SET USERNAME = (SELECT CURRENT_USER());
SET ALLOW_EXTERNAL_ACCESS_FOR_TRIAL_ACCOUNTS = TRUE;
CREATE ROLE IF NOT EXISTS PRGX_DEMO_ROLE;

-- Grant necessary permissions to create databases, compute pools, and service endpoints to new role
GRANT CREATE DATABASE on ACCOUNT to ROLE PRGX_DEMO_ROLE; 
GRANT CREATE COMPUTE POOL on ACCOUNT to ROLE PRGX_DEMO_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT to ROLE PRGX_DEMO_ROLE;
GRANT BIND SERVICE ENDPOINT on ACCOUNT to ROLE PRGX_DEMO_ROLE;

-- grant new role to user and switch to that role
GRANT ROLE PRGX_DEMO_ROLE to USER identifier($USERNAME);
USE ROLE PRGX_DEMO_ROLE;

-- Create warehouse
CREATE OR REPLACE WAREHOUSE PRGX_DEMO_WH WITH WAREHOUSE_SIZE='MEDIUM';
CREATE OR REPLACE WAREHOUSE PRGX_DEMO_XL_WH WITH WAREHOUSE_SIZE='XLARGE';

-- Create Database 
CREATE OR REPLACE DATABASE PRGX_DEMO_DB;

-- Create Schema
CREATE OR REPLACE SCHEMA MLOPS_SCHEMA;

-- Create compute pool
CREATE COMPUTE POOL IF NOT EXISTS MLOPS_COMPUTE_POOL 
 MIN_NODES = 1
 MAX_NODES = 3
 INSTANCE_FAMILY = CPU_X64_M;

-- Using accountadmin, grant privilege to create network rules and integrations on newly created db
USE ROLE ACCOUNTADMIN;
-- GRANT CREATE NETWORK RULE on SCHEMA MLOPS_SCHEMA to ROLE PRGX_DEMO_ROLE;
GRANT CREATE INTEGRATION on ACCOUNT to ROLE PRGX_DEMO_ROLE;
USE ROLE PRGX_DEMO_ROLE;

-- Create an API integration with Github
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION_E2E_SNOW_MLOPS
   api_provider = git_https_api
   api_allowed_prefixes = ('https://github.com/Snowflake-Labs')
   API_USER_AUTHENTICATION = (TYPE = SNOWFLAKE_GITHUB_APP)
   enabled = true
   comment='Git integration with Snowflake Demo Github Repository.';

-- =====================================================
-- RECOVERY AUDIT DEMO DATA
-- Schema and dummy data for overpayment detection
-- =====================================================

USE DATABASE PRGX_DEMO_DB;
USE SCHEMA MLOPS_SCHEMA;

-- Create Vendors table
CREATE OR REPLACE TABLE VENDORS (
    VENDOR_ID VARCHAR(20) PRIMARY KEY,
    VENDOR_NAME VARCHAR(100),
    VENDOR_CATEGORY VARCHAR(50),
    CONTRACT_START_DATE DATE,
    CONTRACT_END_DATE DATE,
    PAYMENT_TERMS_DAYS INT,
    REBATE_ELIGIBLE BOOLEAN,
    REBATE_PERCENTAGE DECIMAL(5,2)
);

-- Create Purchase Orders table
CREATE OR REPLACE TABLE PURCHASE_ORDERS (
    PO_ID VARCHAR(20) PRIMARY KEY,
    VENDOR_ID VARCHAR(20),
    PO_DATE DATE,
    TOTAL_AMOUNT DECIMAL(12,2),
    CURRENCY VARCHAR(3),
    DEPARTMENT VARCHAR(50),
    APPROVER VARCHAR(100),
    STATUS VARCHAR(20)
);

-- Create Invoices table
CREATE OR REPLACE TABLE INVOICES (
    INVOICE_ID VARCHAR(20) PRIMARY KEY,
    PO_ID VARCHAR(20),
    VENDOR_ID VARCHAR(20),
    INVOICE_DATE DATE,
    DUE_DATE DATE,
    INVOICE_AMOUNT DECIMAL(12,2),
    TAX_AMOUNT DECIMAL(10,2),
    TOTAL_AMOUNT DECIMAL(12,2),
    INVOICE_NUMBER VARCHAR(50),
    STATUS VARCHAR(20)
);

-- Create Payments table
CREATE OR REPLACE TABLE PAYMENTS (
    PAYMENT_ID VARCHAR(20) PRIMARY KEY,
    INVOICE_ID VARCHAR(20),
    VENDOR_ID VARCHAR(20),
    PAYMENT_DATE TIMESTAMP,
    PAYMENT_AMOUNT DECIMAL(12,2),
    PAYMENT_METHOD VARCHAR(30),
    CHECK_NUMBER VARCHAR(20),
    BANK_ACCOUNT VARCHAR(20),
    BATCH_ID VARCHAR(20),
    PROCESSED_BY VARCHAR(100)
);

-- Create Contract Pricing table
CREATE OR REPLACE TABLE CONTRACT_PRICING (
    CONTRACT_ID VARCHAR(20) PRIMARY KEY,
    VENDOR_ID VARCHAR(20),
    ITEM_CODE VARCHAR(30),
    ITEM_DESCRIPTION VARCHAR(200),
    CONTRACTED_UNIT_PRICE DECIMAL(10,2),
    EFFECTIVE_DATE DATE,
    EXPIRATION_DATE DATE,
    VOLUME_DISCOUNT_THRESHOLD INT,
    VOLUME_DISCOUNT_PERCENTAGE DECIMAL(5,2)
);

-- Create Recovery Audit Data table (main ML dataset)
CREATE OR REPLACE TABLE RECOVERY_AUDIT_DATA (
    TRANSACTION_ID VARCHAR(20) PRIMARY KEY,
    PAYMENT_ID VARCHAR(20),
    INVOICE_ID VARCHAR(20),
    PO_ID VARCHAR(20),
    VENDOR_ID VARCHAR(20),
    TS TIMESTAMP,
    PAYMENT_AMOUNT DECIMAL(12,2),
    INVOICE_AMOUNT DECIMAL(12,2),
    PO_AMOUNT DECIMAL(12,2),
    QUANTITY INT,
    DAYS_TO_PAYMENT INT,
    PAYMENT_TERMS_DAYS INT,
    VENDOR_CATEGORY VARCHAR(50),
    DEPARTMENT VARCHAR(50),
    PAYMENT_METHOD VARCHAR(30),
    APPROVER_ID VARCHAR(20),
    IS_DUPLICATE_INVOICE BOOLEAN,
    IS_DUPLICATE_PAYMENT BOOLEAN,
    REBATE_ELIGIBLE BOOLEAN,
    REBATE_CLAIMED BOOLEAN,
    REBATE_AMOUNT DECIMAL(10,2),
    INVOICE_PO_MATCH BOOLEAN,
    OVERPAYMENT_FLAG INT,
    PAYMENT_MEMO VARCHAR(500),
    -- New behavioral/pattern columns for feature engineering
    FIRST_DIGIT INT,
    IS_ROUND_AMOUNT BOOLEAN,
    VENDOR_INVOICE_COUNT_7D INT,
    VENDOR_AVG_INVOICE_COUNT_7D DECIMAL(8,2),
    DAYS_SINCE_LAST_VENDOR_INVOICE INT,
    INVOICES_PER_PO INT,
    APPROVER_VENDOR_TRANSACTION_COUNT INT,
    IS_WEEKEND_SUBMISSION BOOLEAN,
    IS_MONTH_END BOOLEAN,
    SEQUENTIAL_INVOICE_GAP INT,
    AMOUNT_VS_APPROVAL_THRESHOLD DECIMAL(8,4)
);

-- Insert dummy Vendors data
INSERT INTO VENDORS VALUES
('V001', 'Acme Supplies Inc', 'Office Supplies', '2024-01-01', '2026-12-31', 30, TRUE, 2.50),
('V002', 'TechPro Solutions', 'IT Equipment', '2024-03-15', '2026-03-14', 45, TRUE, 3.00),
('V003', 'Global Logistics Co', 'Shipping', '2023-06-01', '2025-05-31', 15, FALSE, 0.00),
('V004', 'Premier Consulting', 'Professional Services', '2024-01-01', '2025-12-31', 60, FALSE, 0.00),
('V005', 'Industrial Parts Ltd', 'Manufacturing', '2023-09-01', '2026-08-31', 30, TRUE, 4.00),
('V006', 'CleanCo Services', 'Facilities', '2024-02-01', '2026-01-31', 30, TRUE, 1.50),
('V007', 'DataSafe Systems', 'IT Services', '2024-04-01', '2026-03-31', 45, TRUE, 2.00),
('V008', 'Quick Print Shop', 'Marketing', '2023-11-01', '2025-10-31', 15, FALSE, 0.00),
('V009', 'SafeGuard Insurance', 'Insurance', '2024-01-01', '2026-12-31', 30, FALSE, 0.00),
('V010', 'MegaWare Distributors', 'IT Equipment', '2024-05-01', '2026-04-30', 45, TRUE, 3.50);

-- Generate synthetic Recovery Audit Data
CREATE OR REPLACE PROCEDURE GENERATE_RECOVERY_AUDIT_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM RECOVERY_AUDIT_DATA;
    
    INSERT INTO RECOVERY_AUDIT_DATA
    WITH date_range AS (
        SELECT DATEADD(day, SEQ4(), '2025-03-10'::DATE - INTERVAL '365 days') AS payment_date
        FROM TABLE(GENERATOR(ROWCOUNT => 365))
    ),
    transactions AS (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn,
            d.payment_date,
            'V' || LPAD(UNIFORM(1, 10, RANDOM())::STRING, 3, '0') AS vendor_id,
            UNIFORM(1, 5, RANDOM()) AS dept_num,
            UNIFORM(1, 4, RANDOM()) AS payment_method_num,
            'APR' || LPAD(UNIFORM(1, 20, RANDOM())::STRING, 3, '0') AS approver_id,
            UNIFORM(100, 50000, RANDOM())::DECIMAL(12,2) AS base_amount,
            UNIFORM(1, 100, RANDOM()) AS quantity,
            RANDOM() AS rand_val,
            DAYOFWEEK(d.payment_date) AS dow,
            DAY(d.payment_date) AS dom
        FROM date_range d,
        TABLE(GENERATOR(ROWCOUNT => 28)) g
    ),
    enriched AS (
        SELECT 
            t.*,
            -- Determine if this is a suspicious transaction
            CASE 
                WHEN t.rand_val < 0.03 THEN TRUE  -- Duplicate payment
                WHEN t.rand_val < 0.08 THEN TRUE  -- Pricing/fraud
                WHEN t.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND t.rand_val <= 0.15 AND t.rand_val >= 0.08 THEN TRUE -- Unclaimed rebate
                ELSE FALSE
            END AS is_suspicious,
            -- Calculate payment amount (with fraud patterns)
            CASE 
                WHEN t.rand_val < 0.03 THEN t.base_amount * 2
                WHEN t.rand_val < 0.08 THEN t.base_amount * (1 + UNIFORM(5, 25, RANDOM())/100.0)
                ELSE t.base_amount
            END AS calc_payment_amount
        FROM transactions t
    )
    SELECT
        'TXN' || LPAD(e.rn::STRING, 8, '0') AS TRANSACTION_ID,
        'PAY' || LPAD(e.rn::STRING, 8, '0') AS PAYMENT_ID,
        'INV' || LPAD(e.rn::STRING, 8, '0') AS INVOICE_ID,
        'PO' || LPAD(e.rn::STRING, 8, '0') AS PO_ID,
        e.vendor_id AS VENDOR_ID,
        e.payment_date::TIMESTAMP AS TS,
        -- Make suspicious amounts more likely to be round numbers
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 40 
            THEN ROUND(e.calc_payment_amount, -2)  -- Round to nearest 100
            ELSE e.calc_payment_amount
        END AS PAYMENT_AMOUNT,
        e.base_amount AS INVOICE_AMOUNT,
        e.base_amount * 0.98 AS PO_AMOUNT,
        e.quantity AS QUANTITY,
        UNIFORM(-10, 60, RANDOM()) AS DAYS_TO_PAYMENT,
        CASE e.vendor_id
            WHEN 'V001' THEN 30 WHEN 'V002' THEN 45 WHEN 'V003' THEN 15
            WHEN 'V004' THEN 60 WHEN 'V005' THEN 30 WHEN 'V006' THEN 30
            WHEN 'V007' THEN 45 WHEN 'V008' THEN 15 WHEN 'V009' THEN 30
            ELSE 45
        END AS PAYMENT_TERMS_DAYS,
        CASE e.vendor_id
            WHEN 'V001' THEN 'Office Supplies' WHEN 'V002' THEN 'IT Equipment'
            WHEN 'V003' THEN 'Shipping' WHEN 'V004' THEN 'Professional Services'
            WHEN 'V005' THEN 'Manufacturing' WHEN 'V006' THEN 'Facilities'
            WHEN 'V007' THEN 'IT Services' WHEN 'V008' THEN 'Marketing'
            WHEN 'V009' THEN 'Insurance' ELSE 'IT Equipment'
        END AS VENDOR_CATEGORY,
        CASE e.dept_num
            WHEN 1 THEN 'Finance' WHEN 2 THEN 'Operations'
            WHEN 3 THEN 'IT' WHEN 4 THEN 'HR' ELSE 'Marketing'
        END AS DEPARTMENT,
        CASE e.payment_method_num
            WHEN 1 THEN 'ACH' WHEN 2 THEN 'Wire' WHEN 3 THEN 'Check' ELSE 'Credit Card'
        END AS PAYMENT_METHOD,
        e.approver_id AS APPROVER_ID,
        e.rand_val < 0.02 AS IS_DUPLICATE_INVOICE,
        e.rand_val < 0.03 AS IS_DUPLICATE_PAYMENT,
        e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AS REBATE_ELIGIBLE,
        CASE 
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') 
            THEN e.rand_val > 0.15
            ELSE FALSE
        END AS REBATE_CLAIMED,
        CASE 
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND e.rand_val <= 0.15
            THEN e.base_amount * UNIFORM(15, 40, RANDOM())/1000.0
            ELSE 0
        END AS REBATE_AMOUNT,
        e.rand_val > 0.05 AS INVOICE_PO_MATCH,
        CASE 
            WHEN e.rand_val < 0.03 THEN 1
            WHEN e.rand_val < 0.08 THEN 1
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND e.rand_val <= 0.15 AND e.rand_val >= 0.08 THEN 1
            ELSE 0
        END AS OVERPAYMENT_FLAG,
        CASE 
            -- Duplicate payment memos (suspicious patterns)
            WHEN e.rand_val < 0.01 THEN 
                CASE UNIFORM(1, 5, RANDOM())
                    WHEN 1 THEN 'Resubmission of invoice ' || 'INV' || LPAD(e.rn::STRING, 8, '0') || ' - original payment not received'
                    WHEN 2 THEN 'Duplicate payment request - urgent processing required'
                    WHEN 3 THEN 'Second payment for same services - vendor claims non-receipt'
                    WHEN 4 THEN 'Re-issued invoice after system error - please expedite'
                    ELSE 'Payment resubmitted per vendor request - ref original batch'
                END
            WHEN e.rand_val < 0.03 THEN 
                CASE UNIFORM(1, 5, RANDOM())
                    WHEN 1 THEN 'Corrected invoice amount - disregard previous submission'
                    WHEN 2 THEN 'Updated payment - previous transaction voided'
                    WHEN 3 THEN 'Replacement payment for returned check'
                    WHEN 4 THEN 'Reprocessed after bank rejection - same invoice'
                    ELSE 'Manual override - duplicate approval obtained'
                END
            -- Pricing error memos (inflated amounts)
            WHEN e.rand_val < 0.05 THEN 
                CASE UNIFORM(1, 5, RANDOM())
                    WHEN 1 THEN 'Rush order surcharge applied - expedited delivery'
                    WHEN 2 THEN 'Premium pricing tier - volume threshold not met'
                    WHEN 3 THEN 'Price adjustment per vendor notification - market rates'
                    WHEN 4 THEN 'Updated unit cost - raw material increase passed through'
                    ELSE 'Non-contract pricing - spot purchase authorized'
                END
            WHEN e.rand_val < 0.08 THEN 
                CASE UNIFORM(1, 5, RANDOM())
                    WHEN 1 THEN 'Emergency procurement - standard pricing waived'
                    WHEN 2 THEN 'Vendor price increase effective this period'
                    WHEN 3 THEN 'Additional fees for custom specifications'
                    WHEN 4 THEN 'Fuel surcharge and handling fees included'
                    ELSE 'Out-of-scope work - change order pricing applied'
                END
            -- Unclaimed rebate memos
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND e.rand_val <= 0.12 AND e.rand_val >= 0.08 THEN 
                CASE UNIFORM(1, 5, RANDOM())
                    WHEN 1 THEN 'Standard payment - rebate to be claimed separately Q' || UNIFORM(1, 4, RANDOM())::STRING
                    WHEN 2 THEN 'Volume rebate pending reconciliation - will process EOQ'
                    WHEN 3 THEN 'Rebate credit memo expected from vendor next cycle'
                    WHEN 4 THEN 'Payment processed - rebate tracking ref pending'
                    ELSE 'Full invoice amount - rebate application deferred'
                END
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND e.rand_val <= 0.15 AND e.rand_val >= 0.12 THEN 
                CASE UNIFORM(1, 5, RANDOM())
                    WHEN 1 THEN 'Rebate not applied - below quarterly threshold'
                    WHEN 2 THEN 'Vendor rebate program under review - paid gross'
                    WHEN 3 THEN 'Credit memo for rebate to follow per agreement'
                    WHEN 4 THEN 'Rebate calculation pending final volume tally'
                    ELSE 'Gross payment - rebate reconciliation scheduled'
                END
            -- Normal transaction memos
            ELSE 
                CASE 
                    WHEN e.vendor_id = 'V001' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Office supplies order - ' || UNIFORM(1, 12, RANDOM())::STRING || ' month replenishment'
                            WHEN 2 THEN 'Stationery and paper products per standing order'
                            WHEN 3 THEN 'Desk supplies for new hire onboarding batch'
                            WHEN 4 THEN 'Printer cartridges and toner - scheduled delivery'
                            WHEN 5 THEN 'Break room supplies - monthly restocking'
                            ELSE 'General office supplies per approved requisition'
                        END
                    WHEN e.vendor_id = 'V002' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Laptop computers for ' || CASE e.dept_num WHEN 1 THEN 'Finance' WHEN 2 THEN 'Operations' WHEN 3 THEN 'IT' WHEN 4 THEN 'HR' ELSE 'Marketing' END || ' team'
                            WHEN 2 THEN 'Network switches and cabling - infrastructure upgrade'
                            WHEN 3 THEN 'Monitors and docking stations - hybrid work setup'
                            WHEN 4 THEN 'Server hardware refresh - datacenter project'
                            WHEN 5 THEN 'Peripheral equipment - keyboards and mice bulk order'
                            ELSE 'IT equipment per approved capital expenditure'
                        END
                    WHEN e.vendor_id = 'V003' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Freight charges - shipment tracking #SHP' || UNIFORM(100000, 999999, RANDOM())::STRING
                            WHEN 2 THEN 'Domestic shipping - ' || UNIFORM(5, 50, RANDOM())::STRING || ' packages'
                            WHEN 3 THEN 'Express delivery services - time-sensitive materials'
                            WHEN 4 THEN 'International freight - customs cleared'
                            WHEN 5 THEN 'Monthly logistics services per contract terms'
                            ELSE 'Shipping and handling charges - standard rates'
                        END
                    WHEN e.vendor_id = 'V004' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Management consulting - Phase ' || UNIFORM(1, 4, RANDOM())::STRING || ' deliverables'
                            WHEN 2 THEN 'Strategic advisory services - ' || UNIFORM(40, 200, RANDOM())::STRING || ' hours billed'
                            WHEN 3 THEN 'Process improvement engagement - milestone payment'
                            WHEN 4 THEN 'Due diligence support - project completion'
                            WHEN 5 THEN 'Training and workshop facilitation services'
                            ELSE 'Professional services per statement of work'
                        END
                    WHEN e.vendor_id = 'V005' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Machine parts order - production line ' || UNIFORM(1, 8, RANDOM())::STRING
                            WHEN 2 THEN 'Replacement components - preventive maintenance'
                            WHEN 3 THEN 'Raw materials - Q' || UNIFORM(1, 4, RANDOM())::STRING || ' production schedule'
                            WHEN 4 THEN 'Specialty tooling and fixtures - custom fabrication'
                            WHEN 5 THEN 'Safety equipment and PPE - compliance order'
                            ELSE 'Manufacturing supplies per production requirements'
                        END
                    WHEN e.vendor_id = 'V006' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Janitorial services - ' || CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'Building A' WHEN 2 THEN 'Building B' ELSE 'HQ Campus' END
                            WHEN 2 THEN 'Deep cleaning services - quarterly schedule'
                            WHEN 3 THEN 'Facility maintenance - HVAC filter replacement'
                            WHEN 4 THEN 'Landscaping and grounds maintenance - monthly'
                            WHEN 5 THEN 'Waste management and recycling services'
                            ELSE 'Facilities services per maintenance agreement'
                        END
                    WHEN e.vendor_id = 'V007' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Cloud hosting services - ' || CASE UNIFORM(1, 3, RANDOM()) WHEN 1 THEN 'production' WHEN 2 THEN 'development' ELSE 'DR' END || ' environment'
                            WHEN 2 THEN 'Managed security services - SOC monitoring'
                            WHEN 3 THEN 'Data backup and recovery - monthly subscription'
                            WHEN 4 THEN 'Software licensing - ' || UNIFORM(50, 500, RANDOM())::STRING || ' seats'
                            WHEN 5 THEN 'IT support services - ' || UNIFORM(100, 300, RANDOM())::STRING || ' tickets resolved'
                            ELSE 'IT services per master service agreement'
                        END
                    WHEN e.vendor_id = 'V008' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Marketing collateral - ' || UNIFORM(500, 5000, RANDOM())::STRING || ' units printed'
                            WHEN 2 THEN 'Trade show materials - booth displays and banners'
                            WHEN 3 THEN 'Business cards and letterhead - brand refresh'
                            WHEN 4 THEN 'Product brochures - new launch campaign'
                            WHEN 5 THEN 'Promotional items - customer appreciation gifts'
                            ELSE 'Print services per marketing department request'
                        END
                    WHEN e.vendor_id = 'V009' THEN 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'General liability insurance - annual premium Q' || UNIFORM(1, 4, RANDOM())::STRING
                            WHEN 2 THEN 'Property insurance - coverage period ' || UNIFORM(2024, 2026, RANDOM())::STRING
                            WHEN 3 THEN 'Workers compensation - quarterly installment'
                            WHEN 4 THEN 'Professional liability - E&O coverage renewal'
                            WHEN 5 THEN 'Cyber insurance premium - policy year ' || UNIFORM(2024, 2026, RANDOM())::STRING
                            ELSE 'Insurance premium per policy schedule'
                        END
                    ELSE 
                        CASE UNIFORM(1, 6, RANDOM())
                            WHEN 1 THEN 'Hardware refresh - workstation upgrades dept-wide'
                            WHEN 2 THEN 'Storage expansion - SAN capacity increase'
                            WHEN 3 THEN 'Networking equipment - branch office deployment'
                            WHEN 4 THEN 'Security hardware - firewall and IDS appliances'
                            WHEN 5 THEN 'Telecom equipment - VoIP phone system'
                            ELSE 'IT equipment procurement per approved budget'
                        END
                END
        END AS PAYMENT_MEMO,
        -- Benford's Law: First digit of payment amount (fraudulent transactions skewed away from natural distribution)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 30 
            THEN UNIFORM(5, 9, RANDOM())  -- Suspicious: bias toward higher first digits
            ELSE SUBSTRING(CAST(FLOOR(e.calc_payment_amount) AS VARCHAR), 1, 1)::INT
        END AS FIRST_DIGIT,
        -- Round amount indicator (suspicious amounts more likely to be round)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 40 THEN TRUE
            WHEN MOD(FLOOR(e.calc_payment_amount), 100) = 0 THEN TRUE
            ELSE FALSE
        END AS IS_ROUND_AMOUNT,
        -- Invoice velocity: suspicious vendors have unusual spikes
        CASE 
            WHEN e.is_suspicious THEN UNIFORM(8, 25, RANDOM())  -- Anomalous spike
            ELSE UNIFORM(1, 6, RANDOM())  -- Normal volume
        END AS VENDOR_INVOICE_COUNT_7D,
        -- Historical average for this vendor (baseline)
        UNIFORM(2, 5, RANDOM())::DECIMAL(8,2) AS VENDOR_AVG_INVOICE_COUNT_7D,
        -- Days since last invoice from vendor (low = potential rapid-fire invoicing)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 50 THEN UNIFORM(0, 2, RANDOM())  -- Very recent
            ELSE UNIFORM(3, 30, RANDOM())
        END AS DAYS_SINCE_LAST_VENDOR_INVOICE,
        -- Invoice splitting: multiple invoices per PO
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 35 THEN UNIFORM(3, 8, RANDOM())  -- Suspicious splitting
            ELSE UNIFORM(1, 2, RANDOM())
        END AS INVOICES_PER_PO,
        -- Approver-vendor affinity (same approver always approving same vendor = collusion risk)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 40 THEN UNIFORM(15, 50, RANDOM())  -- High affinity
            ELSE UNIFORM(1, 10, RANDOM())
        END AS APPROVER_VENDOR_TRANSACTION_COUNT,
        -- Weekend/holiday submission (suspicious)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 25 THEN TRUE
            WHEN e.dow IN (0, 6) THEN TRUE  -- Saturday=6, Sunday=0
            ELSE FALSE
        END AS IS_WEEKEND_SUBMISSION,
        -- Month-end rush (higher risk period)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 35 THEN TRUE
            WHEN e.dom >= 28 THEN TRUE
            ELSE FALSE
        END AS IS_MONTH_END,
        -- Sequential invoice number gaps (missing numbers = cherry-picked submissions)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 30 THEN UNIFORM(5, 20, RANDOM())  -- Large gap
            ELSE UNIFORM(0, 2, RANDOM())
        END AS SEQUENTIAL_INVOICE_GAP,
        -- Just-under-threshold amounts (e.g., $9,999 when limit is $10K)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 30 
            THEN 0.95 + (UNIFORM(1, 4, RANDOM()) / 100.0)  -- 95-99% of threshold
            ELSE UNIFORM(20, 80, RANDOM()) / 100.0  -- Normal distribution
        END AS AMOUNT_VS_APPROVAL_THRESHOLD
    FROM enriched e;
    
    RETURN 'Successfully generated ' || (SELECT COUNT(*) FROM RECOVERY_AUDIT_DATA) || ' recovery audit records';
END;
$$;

CALL GENERATE_RECOVERY_AUDIT_DATA();

-- =====================================================
-- CORTEX FINE-TUNING TRAINING DATA
-- Training dataset for risk classification from payment memos
-- =====================================================

-- Create training data table for Cortex Fine-tuning
-- Format: prompt (input) and completion (expected output)
CREATE OR REPLACE TABLE FINETUNE_RISK_CLASSIFICATION_TRAINING (
    PROMPT VARCHAR(2000),
    COMPLETION VARCHAR(500)
);

-- Generate training examples from PAYMENT_MEMO with labeled risk classifications
INSERT INTO FINETUNE_RISK_CLASSIFICATION_TRAINING (PROMPT, COMPLETION)

SELECT * FROM (
    -- HIGH_RISK: Duplicate Payment patterns
    SELECT 
        'Classify the risk level of this payment memo: "' || PAYMENT_MEMO || '"' AS PROMPT,
        '{"risk_level": "HIGH_RISK", "risk_type": "DUPLICATE_PAYMENT", "confidence": 0.95, "action": "HOLD_FOR_REVIEW", "explanation": "Payment memo indicates potential duplicate - references resubmission, voided transaction, or duplicate request"}' AS COMPLETION
    FROM RECOVERY_AUDIT_DATA 
    WHERE IS_DUPLICATE_PAYMENT = TRUE
    LIMIT 500
)

UNION ALL

SELECT * FROM (
    -- HIGH_RISK: Pricing Error patterns  
    SELECT 
        'Classify the risk level of this payment memo: "' || PAYMENT_MEMO || '"' AS PROMPT,
        '{"risk_level": "HIGH_RISK", "risk_type": "PRICING_ERROR", "confidence": 0.90, "action": "VERIFY_PRICING", "explanation": "Payment memo indicates non-standard pricing - rush charges, price adjustments, or non-contract rates"}' AS COMPLETION
    FROM RECOVERY_AUDIT_DATA 
    WHERE IS_ROUND_AMOUNT = TRUE AND IS_DUPLICATE_PAYMENT = FALSE AND OVERPAYMENT_FLAG = 1
    LIMIT 500
)

UNION ALL

SELECT * FROM (
    -- MEDIUM_RISK: Unclaimed Rebate patterns
    SELECT 
        'Classify the risk level of this payment memo: "' || PAYMENT_MEMO || '"' AS PROMPT,
        '{"risk_level": "MEDIUM_RISK", "risk_type": "UNCLAIMED_REBATE", "confidence": 0.85, "action": "FLAG_FOR_REBATE_RECOVERY", "explanation": "Payment memo indicates rebate eligible but not applied - mentions deferred rebate, pending reconciliation, or gross payment"}' AS COMPLETION
    FROM RECOVERY_AUDIT_DATA 
    WHERE REBATE_ELIGIBLE = TRUE AND REBATE_CLAIMED = FALSE AND OVERPAYMENT_FLAG = 1
    LIMIT 500
)

UNION ALL

SELECT * FROM (
    -- LOW_RISK: Normal transaction patterns
    SELECT 
        'Classify the risk level of this payment memo: "' || PAYMENT_MEMO || '"' AS PROMPT,
        '{"risk_level": "LOW_RISK", "risk_type": "NORMAL", "confidence": 0.92, "action": "APPROVE", "explanation": "Standard business transaction with no risk indicators"}' AS COMPLETION
    FROM RECOVERY_AUDIT_DATA 
    WHERE OVERPAYMENT_FLAG = 0 
      AND IS_DUPLICATE_PAYMENT = FALSE 
      AND IS_ROUND_AMOUNT = FALSE
    LIMIT 2000
);

-- Create validation dataset (separate sample)
CREATE OR REPLACE TABLE FINETUNE_RISK_CLASSIFICATION_VALIDATION (
    PROMPT VARCHAR(2000),
    COMPLETION VARCHAR(500)
);

INSERT INTO FINETUNE_RISK_CLASSIFICATION_VALIDATION (PROMPT, COMPLETION)

SELECT 
    'Classify the risk level of this payment memo: "' || PAYMENT_MEMO || '"' AS PROMPT,
    CASE 
        WHEN IS_DUPLICATE_PAYMENT = TRUE THEN 
            '{"risk_level": "HIGH_RISK", "risk_type": "DUPLICATE_PAYMENT", "confidence": 0.95, "action": "HOLD_FOR_REVIEW", "explanation": "Payment memo indicates potential duplicate - references resubmission, voided transaction, or duplicate request"}'
        WHEN IS_ROUND_AMOUNT = TRUE AND OVERPAYMENT_FLAG = 1 AND IS_DUPLICATE_PAYMENT = FALSE THEN 
            '{"risk_level": "HIGH_RISK", "risk_type": "PRICING_ERROR", "confidence": 0.90, "action": "VERIFY_PRICING", "explanation": "Payment memo indicates non-standard pricing - rush charges, price adjustments, or non-contract rates"}'
        WHEN REBATE_ELIGIBLE = TRUE AND REBATE_CLAIMED = FALSE AND OVERPAYMENT_FLAG = 1 THEN 
            '{"risk_level": "MEDIUM_RISK", "risk_type": "UNCLAIMED_REBATE", "confidence": 0.85, "action": "FLAG_FOR_REBATE_RECOVERY", "explanation": "Payment memo indicates rebate eligible but not applied - mentions deferred rebate, pending reconciliation, or gross payment"}'
        ELSE 
            '{"risk_level": "LOW_RISK", "risk_type": "NORMAL", "confidence": 0.92, "action": "APPROVE", "explanation": "Standard business transaction with no risk indicators"}'
    END AS COMPLETION
FROM RECOVERY_AUDIT_DATA 
WHERE TRANSACTION_ID NOT IN (SELECT DISTINCT SUBSTRING(PROMPT, 47, 20) FROM FINETUNE_RISK_CLASSIFICATION_TRAINING)
ORDER BY RANDOM()
LIMIT 500;

-- View training data distribution
SELECT 
    PARSE_JSON(COMPLETION):risk_level::STRING AS RISK_LEVEL,
    PARSE_JSON(COMPLETION):risk_type::STRING AS RISK_TYPE,
    COUNT(*) AS COUNT
FROM FINETUNE_RISK_CLASSIFICATION_TRAINING
GROUP BY 1, 2
ORDER BY 3 DESC;

-- =====================================================
-- CORTEX RISK CLASSIFICATION (Prompt Engineering Approach)
-- =====================================================

CREATE OR REPLACE FUNCTION CLASSIFY_PAYMENT_RISK(payment_memo VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    PARSE_JSON(
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'You are a payment error classifier for accounts payable audit. Analyze the payment memo and classify it.

Risk types:
- HIGH_RISK: Duplicate payments, pricing errors, resubmissions, voided transactions
- MEDIUM_RISK: Unclaimed rebates, deferred credits, pending reconciliations  
- LOW_RISK: Normal business transactions

Payment memo: "' || payment_memo || '"

Respond with ONLY valid JSON (no markdown):
{"risk_level": "HIGH_RISK|MEDIUM_RISK|LOW_RISK", "risk_type": "DUPLICATE_PAYMENT|PRICING_ERROR|UNCLAIMED_REBATE|NORMAL", "action": "HOLD_FOR_REVIEW|VERIFY_PRICING|FLAG_FOR_REBATE_RECOVERY|APPROVE", "explanation": "brief reason"}'
        )
    )
$$;

SELECT 
    TRANSACTION_ID,
    PAYMENT_MEMO,
    CLASSIFY_PAYMENT_RISK(PAYMENT_MEMO) AS RISK_CLASSIFICATION
FROM RECOVERY_AUDIT_DATA
LIMIT 5;

