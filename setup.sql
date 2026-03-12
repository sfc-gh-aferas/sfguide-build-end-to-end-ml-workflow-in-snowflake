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
            DAY(d.payment_date) AS dom,
            HOUR(TIMEADD(hour, UNIFORM(0, 23, RANDOM()), d.payment_date::TIMESTAMP)) AS submission_hour,
            QUARTER(d.payment_date) AS qtr,
            MONTH(d.payment_date) AS mth
        FROM date_range d,
        TABLE(GENERATOR(ROWCOUNT => 137)) g
    ),
    enriched AS (
        SELECT 
            t.*,
            -- Risk factors that correlate with overpayments (realistic patterns)
            -- High-risk approvers (APR001-APR003 have 3x higher fraud rate)
            t.approver_id IN ('APR001', 'APR002', 'APR003') AS is_high_risk_approver,
            -- High-risk vendors (V003, V004 have history of billing issues)
            t.vendor_id IN ('V003', 'V004') AS is_problem_vendor,
            -- Rush periods: month-end (dom >= 26), quarter-end, fiscal year-end
            (t.dom >= 26 OR (t.mth IN (3, 6, 9, 12) AND t.dom >= 20)) AS is_rush_period,
            -- Off-hours submission (before 7am or after 7pm = suspicious)
            (t.submission_hour < 7 OR t.submission_hour > 19) AS is_off_hours,
            -- Large amounts more likely to have errors
            t.base_amount > 20000 AS is_large_amount,
            -- Determine if this is a suspicious transaction with REALISTIC CORRELATIONS
            CASE 
                -- Duplicate payments: higher rate for problem vendors, rush periods, off-hours
                WHEN t.rand_val < 0.005 THEN TRUE  -- Base 0.5% random duplicates
                WHEN t.vendor_id IN ('V003', 'V004') AND t.rand_val < 0.04 THEN TRUE  -- Problem vendors: 4%
                WHEN t.approver_id IN ('APR001', 'APR002', 'APR003') AND t.rand_val < 0.05 THEN TRUE  -- Risky approvers: 5%
                WHEN (t.dom >= 26) AND t.rand_val < 0.06 THEN TRUE  -- Month-end rush: 6%
                WHEN (t.submission_hour < 7 OR t.submission_hour > 19) AND t.rand_val < 0.07 THEN TRUE  -- Off-hours: 7%
                WHEN t.base_amount > 30000 AND t.rand_val < 0.08 THEN TRUE  -- Large amounts: 8%
                -- Pricing errors: correlate with specific vendor categories and amounts
                WHEN t.vendor_id IN ('V004') AND t.base_amount > 15000 AND t.rand_val < 0.12 THEN TRUE  -- Consulting overcharges
                WHEN t.vendor_id IN ('V002', 'V010') AND t.rand_val < 0.06 THEN TRUE  -- IT equipment pricing issues
                -- Unclaimed rebates: only for rebate-eligible vendors with specific patterns
                WHEN t.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND t.rand_val <= 0.15 AND t.rand_val >= 0.08 THEN TRUE
                ELSE FALSE
            END AS is_suspicious,
            -- Fraud TYPE determination for more granular patterns
            CASE
                WHEN t.rand_val < 0.03 OR (t.vendor_id IN ('V003', 'V004') AND t.rand_val < 0.04) OR 
                     (t.approver_id IN ('APR001', 'APR002', 'APR003') AND t.rand_val < 0.05) THEN 'DUPLICATE'
                WHEN t.rand_val < 0.08 OR (t.vendor_id IN ('V004') AND t.base_amount > 15000 AND t.rand_val < 0.12) THEN 'PRICING'
                WHEN t.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND t.rand_val <= 0.15 AND t.rand_val >= 0.08 THEN 'REBATE'
                ELSE 'NORMAL'
            END AS fraud_type,
            -- Calculate payment amount with realistic error patterns
            CASE 
                -- Duplicates: exact 2x more common, but also 1.5x, 3x variations
                WHEN t.rand_val < 0.02 THEN t.base_amount * 2.0
                WHEN t.rand_val < 0.03 THEN t.base_amount * CASE UNIFORM(1,3,RANDOM()) WHEN 1 THEN 1.5 WHEN 2 THEN 2.0 ELSE 3.0 END
                -- Pricing errors: percentage overcharges cluster around common markup amounts
                WHEN t.rand_val < 0.05 THEN t.base_amount * (1 + CASE UNIFORM(1,4,RANDOM()) WHEN 1 THEN 0.10 WHEN 2 THEN 0.15 WHEN 3 THEN 0.20 ELSE 0.25 END)
                WHEN t.rand_val < 0.08 THEN t.base_amount * (1 + UNIFORM(5, 30, RANDOM())/100.0)
                -- Just-under-threshold amounts (fraud pattern: $9,900 when limit is $10K)
                WHEN t.base_amount > 9000 AND t.base_amount < 11000 AND t.rand_val < 0.10 THEN 9900 + UNIFORM(0, 99, RANDOM())
                WHEN t.base_amount > 24000 AND t.base_amount < 26000 AND t.rand_val < 0.10 THEN 24900 + UNIFORM(0, 99, RANDOM())
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
        -- Days to payment: suspicious transactions often paid faster (urgent/rush)
        CASE
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 60 THEN UNIFORM(-5, 5, RANDOM())  -- Rush payment
            WHEN e.is_rush_period THEN UNIFORM(-3, 15, RANDOM())  -- Month-end faster processing
            ELSE UNIFORM(10, 45, RANDOM())  -- Normal payment timing
        END AS DAYS_TO_PAYMENT,
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
        -- Payment method: suspicious transactions more likely Wire (faster, harder to reverse)
        CASE 
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 45 THEN 'Wire'
            WHEN e.is_suspicious AND UNIFORM(1, 100, RANDOM()) < 70 THEN 'ACH'
            WHEN e.payment_method_num = 1 THEN 'ACH' 
            WHEN e.payment_method_num = 2 THEN 'Wire' 
            WHEN e.payment_method_num = 3 THEN 'Check' 
            ELSE 'Credit Card'
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
            -- Duplicate payment memos (suspicious patterns) - STRONG OVERPAYMENT INDICATORS
            WHEN e.rand_val < 0.01 THEN 
                CASE UNIFORM(1, 10, RANDOM())
                    WHEN 1 THEN 'DUPLICATE PAYMENT ALERT: Resubmission of invoice ' || 'INV' || LPAD(e.rn::STRING, 8, '0') || ' - original payment not received - POSSIBLE OVERPAYMENT - verify against prior disbursements'
                    WHEN 2 THEN 'WARNING: Duplicate payment request detected - urgent processing required - OVERPAYMENT RISK HIGH - same invoice paid twice - manual review mandatory'
                    WHEN 3 THEN 'OVERPAYMENT DETECTED: Second payment for same services - vendor claims non-receipt of first payment - potential double-billing fraud - escalate immediately'
                    WHEN 4 THEN 'DUPLICATE SUBMISSION: Re-issued invoice after system error - please expedite - OVERPAYMENT WARNING - cross-reference batch ' || UNIFORM(1000, 9999, RANDOM())::STRING || ' for prior payment'
                    WHEN 5 THEN 'Payment resubmitted per vendor request - ref original batch - DUPLICATE OVERPAYMENT RISK - original transaction ID already processed - verify before release'
                    WHEN 6 THEN 'CRITICAL OVERPAYMENT FLAG: Invoice previously paid on ' || DATEADD(day, -UNIFORM(5, 30, RANDOM()), e.payment_date)::VARCHAR || ' - duplicate detected - hold for audit confirmation'
                    WHEN 7 THEN 'DOUBLE PAYMENT WARNING: Same vendor, same amount, same period - highly suspicious duplicate - overpayment investigation required - do not release'
                    WHEN 8 THEN 'OVERPAYMENT ALERT: Vendor resubmitted after claiming check lost - original payment cleared bank - this is excess payment - recovery needed'
                    WHEN 9 THEN 'DUPLICATE DETECTED: Payment matches prior disbursement within 30 days - OVERPAYMENT CONFIRMED - initiate recovery process ref ' || UNIFORM(10000, 99999, RANDOM())::STRING
                    ELSE 'RED FLAG OVERPAYMENT: Multiple submissions for identical services - duplicate payment pattern detected - excess funds at risk - audit trail attached'
                END
            WHEN e.rand_val < 0.03 THEN 
                CASE UNIFORM(1, 10, RANDOM())
                    WHEN 1 THEN 'OVERPAYMENT CORRECTION NEEDED: Invoice amount adjusted upward - disregard previous submission - OVERBILLING DETECTED - variance of $' || UNIFORM(500, 5000, RANDOM())::STRING || ' requires justification'
                    WHEN 2 THEN 'VOIDED TRANSACTION OVERPAYMENT: Previous payment voided but funds already released - EXCESS PAYMENT - recovery action pending - do not issue replacement until confirmed'
                    WHEN 3 THEN 'DUPLICATE CHECK REPLACEMENT: Original check cashed despite stop payment - OVERPAYMENT OCCURRED - vendor received funds twice - $' || ROUND(e.base_amount, 2)::VARCHAR || ' to recover'
                    WHEN 4 THEN 'OVERPAYMENT WARNING: Reprocessed after bank rejection - same invoice - verify original was not honored before releasing - duplicate risk confirmed'
                    WHEN 5 THEN 'MANUAL OVERRIDE OVERPAYMENT: Duplicate approval obtained bypassing controls - EXCESS PAYMENT AUTHORIZED - audit exception logged - recovery may be required'
                    WHEN 6 THEN 'OVERPAYMENT INVESTIGATION: Same invoice submitted through multiple channels - consolidated overpayment of $' || UNIFORM(1000, 10000, RANDOM())::STRING || ' detected'
                    WHEN 7 THEN 'DOUBLE-BILLED OVERPAYMENT: Vendor submitted duplicate under different invoice numbers - cross-reference reveals overpayment - flag for recovery'
                    WHEN 8 THEN 'SYSTEM DUPLICATE OVERPAYMENT: Batch processing error caused duplicate disbursement - OVERPAYMENT CONFIRMED - initiate vendor credit request'
                    WHEN 9 THEN 'OVERPAYMENT ALERT: ACH and check issued for same invoice - dual payment method overpayment - one payment must be reversed - excess funds $' || ROUND(e.base_amount, 2)::VARCHAR
                    ELSE 'EXCESS PAYMENT DETECTED: Original transaction found in cleared items - this duplicate creates overpayment - recovery authorization required'
                END
            -- Pricing error memos (inflated amounts) - OVERPAYMENT INDICATORS
            WHEN e.rand_val < 0.05 THEN 
                CASE UNIFORM(1, 10, RANDOM())
                    WHEN 1 THEN 'OVERCHARGE ALERT: Rush order surcharge applied without authorization - expedited delivery not requested - OVERPAYMENT of $' || UNIFORM(200, 2000, RANDOM())::STRING || ' - dispute required'
                    WHEN 2 THEN 'PRICE VARIANCE OVERPAYMENT: Premium pricing tier charged - volume threshold WAS met - OVERBILLED by ' || UNIFORM(5, 25, RANDOM())::STRING || '% - contract rate should apply'
                    WHEN 3 THEN 'OVERPAYMENT WARNING: Price adjustment per vendor notification exceeds contract cap - market rates not applicable - excess charge of $' || UNIFORM(500, 3000, RANDOM())::STRING
                    WHEN 4 THEN 'OVERBILLING DETECTED: Updated unit cost passed through without approval - raw material increase not validated - OVERPAYMENT vs contracted rate confirmed'
                    WHEN 5 THEN 'NON-CONTRACT OVERPAYMENT: Spot purchase pricing applied to contract items - OVERCHARGED $' || UNIFORM(1000, 5000, RANDOM())::STRING || ' - rebate pricing ignored'
                    WHEN 6 THEN 'PRICE INFLATION OVERPAYMENT: Invoice exceeds PO amount by ' || UNIFORM(8, 30, RANDOM())::STRING || '% - no change order on file - EXCESS PAYMENT - dispute immediately'
                    WHEN 7 THEN 'OVERCHARGE FLAG: List price billed instead of negotiated discount - OVERPAYMENT of ' || UNIFORM(10, 35, RANDOM())::STRING || '% above contract - recovery needed'
                    WHEN 8 THEN 'PRICING ERROR OVERPAYMENT: Wrong SKU pricing applied - higher cost item billed - EXCESS CHARGE $' || UNIFORM(300, 3000, RANDOM())::STRING || ' detected'
                    WHEN 9 THEN 'OVERPAYMENT AUDIT FINDING: Historical pricing shows ' || UNIFORM(15, 40, RANDOM())::STRING || '% increase without contract amendment - OVERBILLING pattern confirmed'
                    ELSE 'EXCESSIVE CHARGE OVERPAYMENT: Fees exceed standard rate card by significant margin - OVERPAYMENT FLAG - requires pricing committee review'
                END
            WHEN e.rand_val < 0.08 THEN 
                CASE UNIFORM(1, 10, RANDOM())
                    WHEN 1 THEN 'EMERGENCY PROCUREMENT OVERPAYMENT: Standard pricing waived without emergency declaration - OVERCHARGED $' || UNIFORM(500, 4000, RANDOM())::STRING || ' above normal rates'
                    WHEN 2 THEN 'UNAPPROVED PRICE INCREASE OVERPAYMENT: Vendor price increase not in contract - EXCESS PAYMENT - rates locked until ' || DATEADD(month, UNIFORM(3, 12, RANDOM()), e.payment_date)::VARCHAR
                    WHEN 3 THEN 'OVERPAYMENT FOR SPECIFICATIONS: Additional fees for custom specs already included in base - DOUBLE-CHARGED - excess of $' || UNIFORM(400, 2500, RANDOM())::STRING
                    WHEN 4 THEN 'SURCHARGE OVERPAYMENT: Fuel surcharge and handling fees not per contract - OVERBILLED ' || UNIFORM(3, 15, RANDOM())::STRING || '% - dispute and recover excess'
                    WHEN 5 THEN 'SCOPE CREEP OVERPAYMENT: Out-of-scope work billed at change order rates - work was in original scope - OVERPAYMENT of $' || UNIFORM(1000, 8000, RANDOM())::STRING
                    WHEN 6 THEN 'RATE OVERPAYMENT: Billed at T&M rates despite fixed-price agreement - EXCESS CHARGE - contract violation - overpayment confirmed'
                    WHEN 7 THEN 'QUANTITY OVERPAYMENT: Invoiced for ' || UNIFORM(110, 150, RANDOM())::STRING || ' units, only ' || UNIFORM(80, 100, RANDOM())::STRING || ' delivered - OVERBILLED - physical count required'
                    WHEN 8 THEN 'TAX OVERPAYMENT: Sales tax charged on exempt items - OVERCHARGE of $' || UNIFORM(100, 1500, RANDOM())::STRING || ' - tax exemption certificate on file'
                    WHEN 9 THEN 'MARKUP OVERPAYMENT: Distributor markup exceeds contractual ' || UNIFORM(5, 15, RANDOM())::STRING || '% cap - EXCESS MARGIN CHARGED - recovery action needed'
                    ELSE 'UNAUTHORIZED FEE OVERPAYMENT: Administrative fees not in agreement - OVERPAYMENT FLAG - these charges are not legitimate per MSA terms'
                END
            -- Unclaimed rebate memos - OVERPAYMENT DUE TO MISSED REBATES
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND e.rand_val <= 0.12 AND e.rand_val >= 0.08 THEN 
                CASE UNIFORM(1, 10, RANDOM())
                    WHEN 1 THEN 'REBATE OVERPAYMENT: Standard payment made - rebate should have been deducted - OVERPAID by $' || ROUND(e.base_amount * UNIFORM(2, 5, RANDOM())/100, 2)::VARCHAR || ' - Q' || UNIFORM(1, 4, RANDOM())::STRING || ' rebate not applied'
                    WHEN 2 THEN 'VOLUME REBATE MISSED - OVERPAYMENT: Threshold met but discount not taken - EXCESS PAYMENT of ' || UNIFORM(2, 5, RANDOM())::STRING || '% - reconciliation shows overpayment'
                    WHEN 3 THEN 'REBATE CREDIT OVERPAYMENT: Expected credit memo never received - OVERPAID THIS CYCLE - vendor owes $' || ROUND(e.base_amount * UNIFORM(15, 40, RANDOM())/1000, 2)::VARCHAR
                    WHEN 4 THEN 'OVERPAYMENT - REBATE TRACKING FAILURE: Payment processed at gross - rebate ref pending - NET OVERPAYMENT confirmed - add to recovery queue'
                    WHEN 5 THEN 'DEFERRED REBATE OVERPAYMENT: Full invoice amount paid - rebate application deferred indefinitely - CHRONIC OVERPAYMENT PATTERN - $' || ROUND(e.base_amount * UNIFORM(2, 4, RANDOM())/100, 2)::VARCHAR || ' not recovered'
                    WHEN 6 THEN 'OVERPAYMENT ALERT: Eligible rebate of ' || UNIFORM(2, 5, RANDOM())::STRING || '% not deducted - PAID GROSS INSTEAD OF NET - overpayment accumulating'
                    WHEN 7 THEN 'REBATE LEAKAGE OVERPAYMENT: Contract rebate ignored in payment calculation - SYSTEMATIC OVERPAYMENT - YTD excess $' || UNIFORM(5000, 25000, RANDOM())::STRING
                    WHEN 8 THEN 'MISSED DISCOUNT OVERPAYMENT: Early payment discount not taken despite eligible date - OVERPAID ' || UNIFORM(1, 3, RANDOM())::STRING || '% - $' || ROUND(e.base_amount * UNIFORM(1, 3, RANDOM())/100, 2)::VARCHAR
                    WHEN 9 THEN 'OVERPAYMENT DUE TO REBATE TIMING: Rebate period active but not applied - EXCESS PAYMENT - vendor compliance issue - initiate credit request'
                    ELSE 'REBATE RECOVERY OVERPAYMENT: Paid without contractual deduction - OVERPAYMENT CONFIRMED - rebate recapture needed - vendor contact required'
                END
            WHEN e.vendor_id IN ('V001', 'V002', 'V005', 'V006', 'V007', 'V010') AND e.rand_val <= 0.15 AND e.rand_val >= 0.12 THEN 
                CASE UNIFORM(1, 10, RANDOM())
                    WHEN 1 THEN 'THRESHOLD OVERPAYMENT: Rebate not applied despite meeting quarterly threshold - OVERPAID - retroactive credit of $' || ROUND(e.base_amount * UNIFORM(2, 4, RANDOM())/100, 2)::VARCHAR || ' due'
                    WHEN 2 THEN 'PROGRAM REVIEW OVERPAYMENT: Vendor rebate program under review - paid gross - OVERPAYMENT CONTINUES - ' || UNIFORM(3, 8, RANDOM())::STRING || ' months of excess payments'
                    WHEN 3 THEN 'CREDIT MEMO OVERPAYMENT: Credit for rebate never followed - OUTSTANDING OVERPAYMENT - vendor balance shows $' || UNIFORM(2000, 15000, RANDOM())::STRING || ' owed to us'
                    WHEN 4 THEN 'REBATE CALCULATION OVERPAYMENT: Final volume exceeded tier - higher rebate rate applies - UNDERCLAIMED by $' || ROUND(e.base_amount * UNIFORM(1, 3, RANDOM())/100, 2)::VARCHAR || ' - OVERPAYMENT'
                    WHEN 5 THEN 'GROSS PAYMENT OVERPAYMENT: Rebate reconciliation scheduled but never performed - CHRONIC OVERPAYMENT - recovery backlog growing'
                    WHEN 6 THEN 'OVERPAYMENT FLAG: Net price should be $' || ROUND(e.base_amount * 0.97, 2)::VARCHAR || ' but paid $' || ROUND(e.base_amount, 2)::VARCHAR || ' - rebate missing - EXCESS $' || ROUND(e.base_amount * 0.03, 2)::VARCHAR
                    WHEN 7 THEN 'ANNUAL REBATE OVERPAYMENT: Year-end true-up not processed - CUMULATIVE OVERPAYMENT - vendor owes significant credit - audit in progress'
                    WHEN 8 THEN 'TIERED REBATE OVERPAYMENT: Wrong tier applied - volume qualifies for ' || UNIFORM(3, 6, RANDOM())::STRING || '% but only ' || UNIFORM(1, 2, RANDOM())::STRING || '% taken - OVERPAYMENT CONFIRMED'
                    WHEN 9 THEN 'RETROSPECTIVE REBATE OVERPAYMENT: Rebate effective date passed - prior payments not adjusted - HISTORICAL OVERPAYMENT of $' || UNIFORM(3000, 20000, RANDOM())::STRING
                    ELSE 'COMPLIANCE REBATE OVERPAYMENT: Vendor required to provide rebate per contract - NOT HONORED - OVERPAYMENT EACH INVOICE - escalate to procurement'
                END
            -- Normal transaction memos - simple, routine language with no risk indicators
            ELSE 
                CASE UNIFORM(1, 20, RANDOM())
                    WHEN 1 THEN 'Monthly supplies'
                    WHEN 2 THEN 'Standard order'
                    WHEN 3 THEN 'Regular delivery'
                    WHEN 4 THEN 'Scheduled service'
                    WHEN 5 THEN 'Routine maintenance'
                    WHEN 6 THEN 'Weekly shipment'
                    WHEN 7 THEN 'Contract service'
                    WHEN 8 THEN 'Standing order'
                    WHEN 9 THEN 'Quarterly service'
                    WHEN 10 THEN 'Annual subscription'
                    WHEN 11 THEN 'Regular purchase'
                    WHEN 12 THEN 'Standard delivery'
                    WHEN 13 THEN 'Recurring service'
                    WHEN 14 THEN 'Planned order'
                    WHEN 15 THEN 'Routine order'
                    WHEN 16 THEN 'Monthly service'
                    WHEN 17 THEN 'Standard purchase'
                    WHEN 18 THEN 'Regular service'
                    WHEN 19 THEN 'Scheduled delivery'
                    ELSE 'Normal transaction'
                END
        END AS PAYMENT_MEMO,
        -- Benford's Law: First digit (fraudulent amounts deviate from natural log distribution)
        SUBSTRING(CAST(FLOOR(e.calc_payment_amount) AS VARCHAR), 1, 1)::INT AS FIRST_DIGIT,
        -- Round amount indicator: fraudulent amounts cluster at round numbers
        CASE 
            WHEN e.fraud_type = 'DUPLICATE' AND UNIFORM(1, 100, RANDOM()) < 50 THEN TRUE  -- Duplicates often round
            WHEN e.fraud_type = 'PRICING' AND UNIFORM(1, 100, RANDOM()) < 35 THEN TRUE  -- Pricing errors sometimes round
            WHEN MOD(FLOOR(e.calc_payment_amount), 1000) = 0 THEN TRUE
            WHEN MOD(FLOOR(e.calc_payment_amount), 500) = 0 AND UNIFORM(1,100,RANDOM()) < 30 THEN TRUE
            ELSE FALSE
        END AS IS_ROUND_AMOUNT,
        -- Invoice velocity: problem vendors and rush periods show spikes
        CASE 
            WHEN e.is_problem_vendor AND e.is_suspicious THEN UNIFORM(12, 30, RANDOM())
            WHEN e.is_rush_period AND e.is_suspicious THEN UNIFORM(10, 25, RANDOM())
            WHEN e.is_suspicious THEN UNIFORM(8, 20, RANDOM())
            WHEN e.is_rush_period THEN UNIFORM(4, 10, RANDOM())  -- Normal month-end increase
            ELSE UNIFORM(1, 6, RANDOM())
        END AS VENDOR_INVOICE_COUNT_7D,
        -- Historical average by vendor (creates learnable baseline)
        CASE e.vendor_id
            WHEN 'V001' THEN 3.2 WHEN 'V002' THEN 2.8 WHEN 'V003' THEN 4.5
            WHEN 'V004' THEN 1.9 WHEN 'V005' THEN 3.7 WHEN 'V006' THEN 2.1
            WHEN 'V007' THEN 2.5 WHEN 'V008' THEN 1.5 WHEN 'V009' THEN 0.8
            ELSE 2.9
        END::DECIMAL(8,2) AS VENDOR_AVG_INVOICE_COUNT_7D,
        -- Days since last invoice: rapid-fire invoicing correlates with duplicates
        CASE 
            WHEN e.fraud_type = 'DUPLICATE' THEN UNIFORM(0, 3, RANDOM())  -- Very recent = duplicate risk
            WHEN e.is_suspicious THEN UNIFORM(1, 7, RANDOM())
            ELSE UNIFORM(5, 45, RANDOM())
        END AS DAYS_SINCE_LAST_VENDOR_INVOICE,
        -- Invoice splitting pattern: correlates with threshold avoidance
        CASE 
            WHEN e.calc_payment_amount BETWEEN 9000 AND 10000 THEN UNIFORM(3, 6, RANDOM())  -- Splitting to stay under $10K
            WHEN e.calc_payment_amount BETWEEN 24000 AND 25000 THEN UNIFORM(2, 5, RANDOM())  -- Splitting to stay under $25K
            WHEN e.is_suspicious AND e.fraud_type = 'PRICING' THEN UNIFORM(2, 4, RANDOM())
            ELSE UNIFORM(1, 2, RANDOM())
        END AS INVOICES_PER_PO,
        -- Approver-vendor affinity: high-risk approvers show patterns
        CASE 
            WHEN e.is_high_risk_approver AND e.is_suspicious THEN UNIFORM(25, 75, RANDOM())  -- Collusion signal
            WHEN e.is_high_risk_approver THEN UNIFORM(15, 40, RANDOM())
            WHEN e.is_suspicious THEN UNIFORM(10, 25, RANDOM())
            ELSE UNIFORM(1, 12, RANDOM())
        END AS APPROVER_VENDOR_TRANSACTION_COUNT,
        -- Weekend/off-hours: strong correlation with suspicious activity
        CASE 
            WHEN e.is_off_hours AND e.is_suspicious THEN TRUE
            WHEN e.is_off_hours AND UNIFORM(1, 100, RANDOM()) < 40 THEN TRUE  -- Off-hours more often flagged
            WHEN e.dow IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS IS_WEEKEND_SUBMISSION,
        -- Month-end: correlates with rush processing errors
        e.is_rush_period AS IS_MONTH_END,
        -- Invoice number gaps: correlates with cherry-picking
        CASE 
            WHEN e.fraud_type = 'DUPLICATE' AND UNIFORM(1, 100, RANDOM()) < 60 THEN UNIFORM(8, 25, RANDOM())  -- Large gaps
            WHEN e.is_suspicious THEN UNIFORM(3, 12, RANDOM())
            ELSE UNIFORM(0, 3, RANDOM())
        END AS SEQUENTIAL_INVOICE_GAP,
        -- Amount vs threshold: just-under pattern is strong fraud signal
        CASE 
            WHEN e.calc_payment_amount BETWEEN 9500 AND 9999 THEN 0.95 + (e.calc_payment_amount - 9500) / 10000.0
            WHEN e.calc_payment_amount BETWEEN 24500 AND 24999 THEN 0.98 + (e.calc_payment_amount - 24500) / 50000.0
            WHEN e.calc_payment_amount BETWEEN 49500 AND 49999 THEN 0.99 + (e.calc_payment_amount - 49500) / 100000.0
            ELSE e.calc_payment_amount / 50000.0  -- Normalized ratio
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

