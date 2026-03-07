-- ============================================================
--  GIT-LIKE VCS DEMO — RETAIL BANKING BRANCH (9 Tables)
--  Run after: retail_banking_setup_final.sql + 01..05 VCS files
--
--  SCENARIO OVERVIEW:
--   Step 1  — Init & Snapshot      : Track all 9 tables, baseline v1.0
--   Step 2  — Basic Commit         : New customer + account onboarding
--   Step 3  — Branch               : Create feature/loan-restructure branch
--   Step 4  — Feature Work         : Approve + restructure loan on branch
--   Step 5  — Parallel Work (main) : HR update + interest rate change on main
--   Step 6  — History & Diff       : vcs_log, vcs_diff, vcs_blame, vcs_row_history
--   Step 7  — Tags                 : Tag stable states
--   Step 8  — Conflict Detection   : Show merge conflict, then resolve
--   Step 9  — Merge                : Clean merge of feature into main
--   Step 10 — Rollback             : Undo accidental wrong salary entry
--   Step 11 — Time Travel          : Reconstruct customer state at v1.0
-- ============================================================

-- Auto-resets on every re-run
-- ============================================================
-- RESET: Clear all VCS tracking data (safe to re-run)
-- ============================================================
DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN (SELECT table_name FROM vcs_repository WHERE is_active = TRUE)
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS vcs_track_%I ON %I', tbl.table_name, tbl.table_name);
    END LOOP;
END $$;

TRUNCATE TABLE vcs_tag CASCADE;
TRUNCATE TABLE vcs_staged_change CASCADE;
TRUNCATE TABLE vcs_change CASCADE;
TRUNCATE TABLE vcs_commit_parent CASCADE;
TRUNCATE TABLE vcs_commit CASCADE;
TRUNCATE TABLE vcs_branch CASCADE;
TRUNCATE TABLE vcs_repository CASCADE;

DELETE FROM vcs_config;
INSERT INTO vcs_config (key, value) VALUES
    ('active_branch', 'main'),
    ('auto_track',    'true');

INSERT INTO vcs_branch (branch_name, description)
VALUES ('main', 'Default branch - production state');

INSERT INTO vcs_commit (branch_name, commit_hash, message, author)
VALUES ('main', md5('genesis-' || NOW()::TEXT), 'Initial commit - system initialized', CURRENT_USER);

-- Clean up any rows added by a previous demo run
-- Order matters: delete children before parents (FK-safe)
DELETE FROM fund_transfer      WHERE remarks          LIKE 'LOAN RESTRUCTURE MEMO%';
DELETE FROM transaction        WHERE reference_number  = 'REF20240120001';
DELETE FROM loan               WHERE customer_id       = 7;
DELETE FROM account            WHERE account_number    = 'SB10000000007';
DELETE FROM customer           WHERE customer_id       = 7;
DELETE FROM employee_hr_update WHERE reason LIKE 'DATA ENTRY ERROR%'
                                  OR reason LIKE 'Outstanding appraisal%'
                                  OR reason LIKE 'WRONG:%';

-- Restore modified rows to seed values
UPDATE employee SET employment_type = 'contract', salary = 38000 WHERE emp_id = 6;
UPDATE employee SET salary = 65000                                 WHERE emp_id = 2;
UPDATE account  SET interest_rate = 3.50 WHERE account_type = 'savings';
UPDATE account  SET status = 'dormant'   WHERE customer_id  = 5;
UPDATE customer SET kyc_status = 'expired', status = 'dormant' WHERE customer_id = 5;
UPDATE loan SET tenure_months=240, emi_amount=39204, interest_rate=8.50,
               application_status='disbursed', sanctioned_amount=4500000,
               disbursed_amount=4500000, outstanding_principal=4388000, status='active'
WHERE loan_id = 1;
UPDATE loan SET application_status='approved', sanctioned_amount=NULL,
               disbursed_amount=NULL, interest_rate=NULL, tenure_months=NULL,
               emi_amount=NULL, disbursement_date=NULL, maturity_date=NULL,
               outstanding_principal=NULL, status='pending'
WHERE loan_id = 3;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '============================================================'; END $$;
DO $$ BEGIN RAISE NOTICE '  RETAIL BANKING VCS DEMO — START'; END $$;
DO $$ BEGIN RAISE NOTICE '============================================================'; END $$;

-- ============================================================
-- STEP 1 — INIT: Register all 9 tables for VCS tracking
--          Equivalent to: git init (per table)
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 1 — INIT & SNAPSHOT (git init + git commit -m baseline)'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

SELECT vcs_init('branch');
SELECT vcs_init('department');
SELECT vcs_init('employee');
SELECT vcs_init('employee_hr_update');
SELECT vcs_init('customer');
SELECT vcs_init('account');
SELECT vcs_init('transaction');
SELECT vcs_init('fund_transfer');
SELECT vcs_init('loan');

-- Confirm all tables are tracked
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Tracked tables in vcs_repository:'; END $$;
SELECT table_name, primary_key_column, tracked_since FROM vcs_repository ORDER BY repo_id;

-- Snapshot all existing seed data as the baseline commit
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Snapshotting all seed data as v1.0 baseline...'; END $$;
SELECT vcs_snapshot_all('v1.0 baseline — Kottayam Branch initial state');

-- Tag this as the official baseline
SELECT vcs_tag_create('v1.0', NULL, 'Production baseline — all seed data captured');

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Current commit log after baseline:'; END $$;
SELECT commit_id, hash, message, change_count, committed_at FROM vcs_log();

-- ============================================================
-- STEP 2 — COMMIT: New customer onboarding
--          Equivalent to: git add + git commit
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 2 — BASIC COMMIT: New customer + account onboarding'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

-- Add a new customer (Vivek Chandran, walk-in)
-- Use RETURNING to capture the real IDs (avoids hardcoded FK issues)
DO $$
DECLARE
    v_cust_id  INT;
    v_acct_id  INT;
BEGIN
    -- Insert customer, capture real customer_id
    INSERT INTO customer (
        branch_id, assigned_rm_id, full_name, dob, gender,
        phone, email, occupation, income_bracket,
        aadhaar_number, pan_number,
        kyc_status, kyc_verified_by, kyc_verified_on,
        customer_since, status
    ) VALUES (
        1, 2, 'Vivek Chandran', '1992-09-27', 'M',
        '9876543220', 'vivek.c@email.com', 'Chartered Accountant', '10L_25L',
        '123456781007', 'PQRVC9012G',
        'verified', 4, CURRENT_DATE,
        CURRENT_DATE, 'active'
    ) RETURNING customer_id INTO v_cust_id;

    -- Insert account using the real customer_id
    INSERT INTO account (
        customer_id, branch_id, opened_by,
        account_number, account_type, min_balance, interest_rate,
        opened_date, current_balance, status
    ) VALUES (
        v_cust_id, 1, 2,
        'SB10000000007', 'savings', 1000, 3.50,
        CURRENT_DATE, 10000.00, 'active'
    ) RETURNING account_id INTO v_acct_id;

    -- Insert initial deposit using the real account_id
    INSERT INTO transaction (
        account_id, txn_type, channel, amount, balance_after,
        reference_number, txn_date, description, initiated_by
    ) VALUES (
        v_acct_id, 'credit', 'branch', 10000, 10000,
        'REF20240120001', NOW(), 'Initial deposit — account opening', 4
    );

    RAISE NOTICE 'Vivek Chandran onboarded: customer_id=%, account_id=%', v_cust_id, v_acct_id;
END $$;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Staged changes after onboarding (git status):'; END $$;
SELECT table_name, operation, row_pk, changed_columns FROM vcs_status();

-- Commit the onboarding
SELECT vcs_commit('Onboarded new customer Vivek Chandran (CUST-7) with savings account and initial deposit', 'Priya Menon');

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Log after onboarding commit:'; END $$;
SELECT commit_id, hash, message, change_count FROM vcs_log();

-- ============================================================
-- STEP 3 — BRANCH: Create feature branch for loan restructure
--          Equivalent to: git checkout -b feature/loan-restructure
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 3 — BRANCH: feature/loan-restructure'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

SELECT vcs_branch_create(
    'feature/loan-restructure',
    'main',
    'Restructure Arjun Pillai home loan + approve Meena personal loan',
    TRUE   -- auto checkout
);

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> All branches:'; END $$;
SELECT branch, is_current, created_from, commit_count FROM vcs_branch_list();

-- ============================================================
-- STEP 4 — FEATURE WORK on feature/loan-restructure
--          Approve Meena's personal loan + restructure Arjun's home loan
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 4 — FEATURE WORK on feature/loan-restructure'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

-- 4a. Approve and disburse Meena Suresh's personal loan (loan_id=3)
DO $$ BEGIN RAISE NOTICE '>> 4a. Approving personal loan for Meena Suresh...'; END $$;
UPDATE loan SET
    application_status   = 'disbursed',
    sanctioned_amount    = 300000,
    disbursed_amount     = 300000,
    interest_rate        = 11.50,   -- negotiated rate
    tenure_months        = 36,
    emi_amount           = 9885,
    disbursement_date    = CURRENT_DATE,
    maturity_date        = CURRENT_DATE + INTERVAL '36 months',
    outstanding_principal= 300000,
    collateral_type      = 'none',
    status               = 'active'
WHERE loan_id = 3;

-- Disbursal credit to Meena's account
INSERT INTO transaction (
    account_id, txn_type, channel, amount, balance_after,
    reference_number, txn_date, description, initiated_by
) VALUES (
    2, 'credit', 'neft', 300000, 322500,
    'REF20240121001', NOW(), 'Personal loan disbursement — LOAN-3', 3
);

SELECT vcs_commit('Approved & disbursed personal loan for Meena Suresh — ₹3L @ 11.5% for 36 months', 'Arun Kumar');

-- 4b. Restructure Arjun's home loan — extend tenure, reduce EMI
DO $$ BEGIN RAISE NOTICE '>> 4b. Restructuring Arjun Pillai home loan (tenure extension)...'; END $$;
UPDATE loan SET
    tenure_months        = 300,        -- extended from 240 to 300 months
    emi_amount           = 33150,      -- reduced EMI
    interest_rate        = 8.25,       -- revised rate
    outstanding_principal= 4388000,
    status               = 'active'
WHERE loan_id = 1;

-- Log the restructure as an HR-style event in employee_hr_update
-- (In reality you'd have a loan_restructure table, but for demo
--  we use a new loan record to show VCS tracking the change)
DO $$ BEGIN RAISE NOTICE '>> 4c. Adding restructure note via fund_transfer (internal memo)...'; END $$;
INSERT INTO fund_transfer (
    from_account_id, to_account_id, to_ifsc, to_account_number,
    transfer_mode, amount, status, initiated_at, remarks
) VALUES (
    1, 1, NULL, NULL,
    'internal', 1, 'completed', NOW(),
    'LOAN RESTRUCTURE MEMO: Loan-1 tenure extended 240→300m, rate 8.5→8.25%, EMI 39204→33150'
);

SELECT vcs_commit('Restructured home loan LOAN-1 for Arjun Pillai — tenure 240→300m, rate 8.5→8.25%', 'Arun Kumar');

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Feature branch log:'; END $$;
SELECT commit_id, hash, message, change_count FROM vcs_log('feature/loan-restructure');

-- ============================================================
-- STEP 5 — PARALLEL WORK on main branch
--          HR update + savings interest rate hike
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 5 — PARALLEL WORK on main (HR update + interest rate)'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

SELECT vcs_checkout('main');

-- 5a. Manoj Krishnan: contract → permanent + salary hike
DO $$ BEGIN RAISE NOTICE '>> 5a. Converting Manoj Krishnan to permanent employee...'; END $$;

-- Record the HR event
INSERT INTO employee_hr_update (
    emp_id, update_type, effective_date,
    old_designation, old_dept_id, old_salary, old_emp_type,
    new_designation, new_dept_id, new_salary, new_emp_type,
    reason, authorized_by
) VALUES (
    6, 'employment_type_change', CURRENT_DATE,
    'Credit Analyst', 2, 38000, 'contract',
    'Credit Analyst', 2, 43000, 'permanent',
    'Outstanding appraisal: 4.8/5 — converted to permanent with 13% hike', 1
);

-- Update actual employee record
UPDATE employee SET
    employment_type = 'permanent',
    salary          = 43000
WHERE emp_id = 6;

SELECT vcs_commit('HR: Manoj Krishnan (EMP-6) converted contract→permanent, salary ₹38K→₹43K', 'Rajesh Nair');

-- 5b. RBI policy: savings interest rate revised 3.50% → 4.00%
DO $$ BEGIN RAISE NOTICE '>> 5b. RBI policy update — savings interest rate 3.50 → 4.00...'; END $$;
UPDATE account
SET interest_rate = 4.00
WHERE account_type = 'savings' AND status = 'active';

SELECT vcs_commit('RBI policy update: savings account interest rate revised 3.50% → 4.00%', 'Rajesh Nair');

-- 5c. Rajan Varma KYC expired — block dormant account
DO $$ BEGIN RAISE NOTICE '>> 5c. Blocking Rajan Varma account (KYC expired)...'; END $$;
UPDATE customer SET
    kyc_status = 'expired',
    status     = 'blocked'
WHERE customer_id = 5;

UPDATE account SET status = 'frozen' WHERE customer_id = 5;

SELECT vcs_commit('Compliance: Rajan Varma (CUST-5) KYC expired — account frozen pending renewal', 'Suresh Pillai');

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Main branch log after parallel work:'; END $$;
SELECT commit_id, hash, message, change_count FROM vcs_log('main');

-- ============================================================
-- STEP 6 — HISTORY: vcs_log, vcs_diff, vcs_blame, vcs_row_history
--          Equivalent to: git log, git diff, git blame, git log -p
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 6 — HISTORY, DIFF & BLAME'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

-- 6a. View full cross-branch log
DO $$ BEGIN RAISE NOTICE '>> 6a. Full commit log across all branches (git log --all):'; END $$;
SELECT commit_id, hash, branch, message, change_count, committed_at FROM vcs_log_all(20);

-- 6b. Diff between baseline (v1.0 = commit 2) and current main HEAD
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 6b. Diff: baseline → current main (what changed since v1.0):'; END $$;
SELECT table_name, row_pk, operation, changed_columns
FROM vcs_diff(2, vcs_get_head_commit('main'))
ORDER BY table_name, row_pk;

-- 6c. Who last touched each row in loan table (blame)
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 6c. vcs_blame on loan table (who last changed each loan row):'; END $$;
SELECT row_pk, last_operation, last_author, last_message, modified_at
FROM vcs_blame('loan', 'feature/loan-restructure');

-- 6d. Full history of Arjun's home loan row
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 6d. Row history of LOAN-1 (Arjun Pillai home loan):'; END $$;
SELECT commit_id, branch, operation, changed_columns, committed_at
FROM vcs_row_history('loan', '1');

-- 6e. Full history of employee_hr_update for Manoj (emp_id=6)
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 6e. Row history of employee_hr_update for Manoj Krishnan:'; END $$;
SELECT commit_id, branch, operation, changed_columns, committed_at
FROM vcs_row_history('employee_hr_update', '6');

-- 6f. Diff between the two branches
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 6f. Branch diff: feature/loan-restructure vs main:'; END $$;
SELECT table_name, row_pk, operation, branch, changed_columns
FROM vcs_diff_branch('feature/loan-restructure', 'main')
ORDER BY table_name;

-- ============================================================
-- STEP 7 — TAGS: Snapshot meaningful states
--          Equivalent to: git tag
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 7 — TAGS'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

-- Tag current main state before merge
SELECT vcs_tag_create('v1.1-pre-merge', NULL, 'Main branch state before loan restructure merge');

-- Tag the feature branch HEAD
SELECT vcs_checkout('feature/loan-restructure');
SELECT vcs_tag_create('feature-loan-restructure-ready', NULL, 'Loan restructure feature complete and ready to merge');
SELECT vcs_checkout('main');

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> All tags:'; END $$;
SELECT tag, commit_id, commit_branch, message, created_at FROM vcs_tag_list();

-- ============================================================
-- STEP 8 — CONFLICT DETECTION
--          Both branches touched the same loan row (loan_id=1)
--          Show the conflict, then resolve it
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 8 — CONFLICT DETECTION & RESOLUTION'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

DO $$ BEGIN RAISE NOTICE '>> Checking for merge conflicts between feature and main...'; END $$;
SELECT * FROM vcs_merge_conflicts('feature/loan-restructure', 'main');

-- The conflict exists on loan_id=1 (Arjun's home loan was touched on both
-- branches). We resolve it by aligning main to match the restructured terms
-- before merging.
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> RESOLVING: Accepting restructured terms on main before merge...'; END $$;
UPDATE loan SET
    tenure_months        = 300,
    emi_amount           = 33150,
    interest_rate        = 8.25
WHERE loan_id = 1;

SELECT vcs_commit('Conflict resolution: align main loan-1 terms with restructure branch before merge', 'Rajesh Nair');

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Re-checking conflicts after resolution:'; END $$;
SELECT * FROM vcs_merge_conflicts('feature/loan-restructure', 'main');
-- Should return 0 rows now

-- ============================================================
-- STEP 9 — MERGE: Bring feature branch into main
--          Equivalent to: git merge feature/loan-restructure
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 9 — MERGE: feature/loan-restructure → main'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

SELECT vcs_merge(
    'feature/loan-restructure',
    'main',
    'Merge feature/loan-restructure: Meena loan approved + Arjun loan restructured'
);

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Main branch log after merge:'; END $$;
SELECT commit_id, hash, message, is_merge, change_count FROM vcs_log('main');

-- Tag the post-merge state
SELECT vcs_tag_create('v1.2', NULL, 'Post-merge: loan restructure and new customer fully integrated');

-- ============================================================
-- STEP 10 — ROLLBACK: Undo a wrong salary entry
--           Equivalent to: git revert
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 10 — ROLLBACK: Undo accidental wrong salary entry'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
-- Simulate a data entry mistake — wrong salary entered for Priya
DO $$ BEGIN RAISE NOTICE '>> Simulating wrong salary update for Priya Menon...'; END $$;
UPDATE employee SET salary = 999999 WHERE emp_id = 2;
INSERT INTO employee_hr_update (emp_id, update_type, effective_date,old_designation, old_dept_id, old_salary, old_emp_type,new_designation,
 new_dept_id, new_salary, new_emp_type,reason, authorized_by) VALUES (2, 'salary_revision', CURRENT_DATE,'Senior Relationship Manager', 1, 65000, 
    'permanent','Senior Relationship Manager', 1, 999999, 'permanent', 'DATA ENTRY ERROR — wrong amount entered', 1);
SELECT vcs_commit('WRONG: Salary entry error for Priya Menon — ₹999999 (should be ₹68000)', 'Suresh Pillai');
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Priya salary BEFORE rollback:'; END $$;
SELECT emp_id, full_name, designation, salary FROM employee WHERE emp_id = 2;
-- Show what the rollback will undo (dry run first)
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> DRY RUN: What will rollback undo?'; END $$;
SELECT vcs_rollback(vcs_get_head_commit('main') - 1, NULL, TRUE);
-- Get the commit ID just before the wrong entry   (the merge commit = HEAD-1)
DO $$
DECLARE
    v_rollback_to INT;
BEGIN
    SELECT commit_id INTO v_rollback_to
    FROM vcs_commit
    WHERE branch_name = 'main'
    ORDER BY commit_id DESC
    OFFSET 1 LIMIT 1;
    RAISE NOTICE 'Rolling back to commit #%', v_rollback_to;
    PERFORM vcs_rollback(v_rollback_to);
END $$;
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Priya salary AFTER rollback (should be back to 65000):'; END $$;
SELECT emp_id, full_name, designation, salary FROM employee WHERE emp_id = 2;
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Log shows rollback commit appended (audit trail preserved):'; END $$;
SELECT commit_id, hash, message, change_count FROM vcs_log('main') LIMIT 5;
-- ============================================================
-- STEP 11 — TIME TRAVEL: Reconstruct data at v1.0
--           Equivalent to: git checkout v1.0 -- table
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'STEP 11 — TIME TRAVEL: Reconstruct state at v1.0 baseline'; END $$;
DO $$ BEGIN RAISE NOTICE '------------------------------------------------------------'; END $$;

-- Get the commit_id of v1.0 tag
DO $$ BEGIN RAISE NOTICE '>> 11a. Customer table as it was at v1.0 baseline:'; END $$;
SELECT row_pk, row_state->>'full_name' AS name,
       row_state->>'kyc_status'        AS kyc_status,
       row_state->>'status'            AS status
FROM vcs_time_travel('customer',
    (SELECT commit_id FROM vcs_tag WHERE tag_name = 'v1.0'))
ORDER BY row_pk::INT;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 11b. Current customer table for comparison:'; END $$;
SELECT customer_id, full_name, kyc_status, status FROM customer ORDER BY customer_id;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 11c. Reconstruct Arjun Pillai home loan at v1.0 (before restructure):'; END $$;
SELECT vcs_reconstruct_at('loan', '1',
    (SELECT commit_id FROM vcs_tag WHERE tag_name = 'v1.0'));

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 11d. Arjun Pillai home loan NOW (after restructure):'; END $$;
SELECT loan_id, loan_type, interest_rate, tenure_months, emi_amount, status
FROM loan WHERE loan_id = 1;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 11e. Account interest_rate history — see RBI rate change effect:'; END $$;
SELECT row_pk, row_state->>'account_number' AS account_no,
       row_state->>'account_type'           AS type,
       row_state->>'interest_rate'          AS interest_rate
FROM vcs_time_travel('account',
    (SELECT commit_id FROM vcs_tag WHERE tag_name = 'v1.0'))
ORDER BY row_pk::INT;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> 11f. Account interest rates NOW (after RBI hike to 4.00%):'; END $$;
SELECT account_id, account_number, account_type, interest_rate
FROM account ORDER BY account_id;

-- ============================================================
-- FINAL SUMMARY
-- ============================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '============================================================'; END $$;
DO $$ BEGIN RAISE NOTICE '  DEMO COMPLETE — FINAL STATE'; END $$;
DO $$ BEGIN RAISE NOTICE '============================================================'; END $$;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> All commits across all branches:'; END $$;
SELECT commit_id, hash, branch, message, is_merge, change_count, committed_at
FROM vcs_log_all(30);

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> All tags:'; END $$;
SELECT tag, commit_id, commit_branch, message FROM vcs_tag_list();

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> All branches:'; END $$;
SELECT branch, is_current, created_from, commit_count FROM vcs_branch_list();

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '>> Branch summary (live data):'; END $$;
SELECT * FROM branch_summary;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '============================================================'; END $$;
DO $$ BEGIN RAISE NOTICE '  VCS FEATURES EXERCISED IN THIS DEMO:'; END $$;
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_init()              — Step 1  : Tracked 9 tables'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_snapshot_all()      — Step 1  : Baseline all seed data'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_commit()            — Steps 2,4,5,8,10: 10+ commits made'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_status()            — Step 2  : Staged change inspection'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_branch_create()     — Step 3  : feature/loan-restructure'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_checkout()          — Steps 3,5,7: Branch switching'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_log() / log_all()   — Step 6  : Commit history'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_diff()              — Step 6  : Baseline vs current'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_diff_branch()       — Step 6  : Cross-branch diff'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_blame()             — Step 6  : Row-level attribution'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_row_history()       — Step 6  : Full row change timeline'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_tag_create()        — Steps 1,7,9: v1.0, v1.1, v1.2'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_tag_list()          — Step 7  : Tag listing'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_merge_conflicts()   — Step 8  : Conflict detection'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_merge()             — Step 9  : Feature → main merge'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_rollback()          — Step 10 : Undo wrong salary entry'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_time_travel()       — Step 11 : Table state at v1.0'; END $$;
DO $$ BEGIN RAISE NOTICE '  vcs_reconstruct_at()    — Step 11 : Single row at v1.0'; END $$;
DO $$ BEGIN RAISE NOTICE '============================================================'; END $$;