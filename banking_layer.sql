-- ============================================================
-- COMPLETE BANKING LAYER - TRANSACTIONS, LOANS & ROLE VIEWS
-- ============================================================
-- Extends the Git-Like VCS Banking Database with:
--   A. Deposit & Withdrawal stored procedures
--   B. Account-to-Account Transfer
--   C. Loan Application & Employee Approval Workflow
--   D. Role-Based Views (Customer / Employee / Manager)
--
-- Prerequisites: setup.sql + 01..05_vcs_*.sql must be loaded
-- ============================================================

-- ============================================================
-- SECTION 0: HELPER TABLE
-- ============================================================

-- Loan applications submitted by customers (pre-approval)
DROP TABLE IF EXISTS loan_application CASCADE;

CREATE TABLE loan_application (
    application_id   SERIAL PRIMARY KEY,
    customer_id      INT NOT NULL REFERENCES customer(customer_id),
    assigned_emp_id  INT REFERENCES employee(emp_id),
    requested_amount NUMERIC(15,2) NOT NULL,
    purpose          TEXT,
    application_date TIMESTAMP DEFAULT NOW(),
    status           VARCHAR(20) DEFAULT 'submitted'
        CHECK (status IN ('submitted','under_review','approved','rejected')),
    reviewed_at      TIMESTAMP,
    decision_notes   TEXT
);

-- ============================================================
-- SECTION 1: TRANSACTION SEQUENCE (auto-increment TXN IDs)
-- ============================================================
-- We use a sequence so concurrent calls never collide
DROP SEQUENCE IF EXISTS txn_seq;
CREATE SEQUENCE txn_seq START 17 INCREMENT 1;  -- seeds after TXN016

CREATE OR REPLACE FUNCTION next_txn_id()
RETURNS VARCHAR(10) AS $$
BEGIN
    RETURN 'TXN' || LPAD(nextval('txn_seq')::TEXT, 3, '0');
END;
$$ LANGUAGE plpgsql;

DROP SEQUENCE IF EXISTS app_seq;
CREATE SEQUENCE app_seq START 1 INCREMENT 1;

CREATE OR REPLACE FUNCTION next_app_id()
RETURNS VARCHAR(15) AS $$
BEGIN
    RETURN 'APP' || LPAD(nextval('app_seq')::TEXT, 4, '0');
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SECTION 2: DEPOSIT
-- ============================================================
-- bank_deposit(account_id, amount, [description])
--   • Validates account is Active
--   • Credits balance
--   • Writes transaction record
-- ============================================================
CREATE OR REPLACE FUNCTION bank_deposit(
    p_account_id INT,
    p_amount NUMERIC
)
RETURNS TEXT AS $$
DECLARE
    v_balance NUMERIC;
BEGIN
    SELECT current_balance INTO v_balance
    FROM account
    WHERE account_id = p_account_id AND status = 'active';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Invalid or inactive account';
    END IF;

    UPDATE account
    SET current_balance = current_balance + p_amount
    WHERE account_id = p_account_id;

    INSERT INTO transaction (
        account_id, txn_type, channel, amount,
        balance_after, reference_number
    )
    VALUES (
        p_account_id, 'credit', 'system', p_amount,
        v_balance + p_amount,
        'REF' || floor(random()*1000000)
    );

    RETURN 'Deposit successful';
END;
$$ LANGUAGE plpgsql;
-- ============================================================
-- SECTION 3: WITHDRAWAL
-- ============================================================
-- bank_withdraw(account_id, amount, [description])
--   • Validates Active status & sufficient funds
--   • Debits balance
--   • Records transaction
-- ============================================================
CREATE OR REPLACE FUNCTION bank_withdraw(
    p_account_id INT,
    p_amount NUMERIC
)
RETURNS TEXT AS $$
DECLARE
    v_balance NUMERIC;
BEGIN
    SELECT current_balance INTO v_balance
    FROM account
    WHERE account_id = p_account_id AND status = 'active';

    IF v_balance < p_amount THEN
        RAISE EXCEPTION 'Insufficient balance';
    END IF;

    UPDATE account
    SET current_balance = current_balance - p_amount
    WHERE account_id = p_account_id;

    INSERT INTO transaction (
        account_id, txn_type, channel, amount,
        balance_after, reference_number
    )
    VALUES (
        p_account_id, 'debit', 'system', p_amount,
        v_balance - p_amount,
        'REF' || floor(random()*1000000)
    );

    RETURN 'Withdrawal successful';
END;
$$ LANGUAGE plpgsql;
-- ============================================================
-- SECTION 4: TRANSFER (ACCOUNT-TO-ACCOUNT)
-- ============================================================
-- bank_transfer(from_account, to_account, amount, [note])
--   • Validates both accounts are Active
--   • Sufficient funds check
--   • Atomic debit + credit
--   • Writes two-sided transaction record
-- ============================================================
CREATE OR REPLACE FUNCTION bank_transfer(
    p_from INT,
    p_to INT,
    p_amount NUMERIC
)
RETURNS TEXT AS $$
DECLARE
    v_balance NUMERIC;
BEGIN
    SELECT current_balance INTO v_balance
    FROM account WHERE account_id = p_from;

    IF v_balance < p_amount THEN
        RAISE EXCEPTION 'Insufficient funds';
    END IF;

    UPDATE account SET current_balance = current_balance - p_amount WHERE account_id = p_from;
    UPDATE account SET current_balance = current_balance + p_amount WHERE account_id = p_to;

    RETURN 'Transfer successful';
END;
$$ LANGUAGE plpgsql;
-- ============================================================
-- SECTION 5: LOAN APPLICATION (Customer submits)
-- ============================================================
-- bank_apply_loan(customer_id, requested_amount, purpose)
--   • Creates a loan_application record
--   • Auto-assigns a loan officer from the customer's branch
--   • Returns application ID
-- ============================================================
CREATE OR REPLACE FUNCTION bank_apply_loan(
    p_customer_id       INTEGER,
    p_requested_amount  DECIMAL,
    p_purpose           TEXT DEFAULT 'General purpose'
)
RETURNS TEXT AS $$
DECLARE
    v_app_id      VARCHAR(15);
    v_officer_id  INTEGER    ;
    v_branch_id   Int;
BEGIN
    -- Validate customer
    IF NOT EXISTS (SELECT 1 FROM customer WHERE customer_id = p_customer_id) THEN
        RAISE EXCEPTION 'Customer "%" not found', p_customer_id;
    END IF;
    IF p_requested_amount <= 0 THEN
        RAISE EXCEPTION 'Loan amount must be positive';
    END IF;

    -- Find the customer's primary branch (first active Savings account)
    SELECT a.branch_id INTO v_branch_id
      FROM account a
     WHERE a.customer_id = p_customer_id
       AND a.status = 'Active'
     ORDER BY a.opened_date
     LIMIT 1;

    -- Assign a loan officer from that branch (prefer role = 'Loan Officer')
    SELECT emp_id INTO v_officer_id
      FROM employee
     WHERE branch_id = COALESCE(v_branch_id, 001)
       AND designation = 'Loan Officer'
     ORDER BY emp_id
     LIMIT 1;

    -- Fallback: any employee in the branch
    IF v_officer_id IS NULL THEN
        SELECT emp_id INTO v_officer_id
          FROM employee
         WHERE branch_id = COALESCE(v_branch_id, 'BR001')
         ORDER BY emp_id LIMIT 1;
    END IF;
INSERT INTO loan_application (
    customer_id, assigned_emp_id,
    requested_amount, purpose, status
)
VALUES (
    p_customer_id, v_officer_id,
    p_requested_amount, p_purpose, 'submitted'
)
RETURNING application_id INTO v_app_id;

    RETURN format(
        'Loan application %s submitted.' || chr(10) ||
        '   Customer  : %s' || chr(10) ||
        '   Amount    : ₹%s' || chr(10) ||
        '   Purpose   : %s' || chr(10) ||
        '   Assigned to: %s' || chr(10) ||
        '   Status    : submitted',
        v_app_id, p_customer_id,
        p_requested_amount::NUMERIC(15,2),
        p_purpose, COALESCE(v_officer_id::TEXT, '(unassigned)')
    );
END;
$$ LANGUAGE plpgsql;
-- ============================================================
-- SECTION 6: LOAN REVIEW (Employee updates status)
-- ============================================================
-- bank_review_loan(application_id, emp_id, new_status, notes)
--   • Employee can move: Pending → Under Review
--   • Employee can move: Under Review → Approved / Rejected
--   • Manager can override any status
-- ============================================================
CREATE OR REPLACE FUNCTION bank_review_loan(
    p_app_id     VARCHAR,
    p_emp_id     INTEGER,
    p_new_status VARCHAR,
    p_notes      TEXT DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_app        loan_application%ROWTYPE;
    v_emp_role   VARCHAR(30);
BEGIN
    SELECT * INTO v_app FROM loan_application WHERE application_id = p_app_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Application "%" not found', p_app_id;
    END IF;

    SELECT role INTO v_emp_role FROM employee WHERE emp_id = p_emp_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Employee "%" not found', p_emp_id;
    END IF;

    -- ROLE CHECK
    IF v_emp_role = 'Branch Manager' THEN
        NULL;

    ELSIF v_emp_role = 'Loan Officer' THEN
        IF v_app.assigned_emp_id != p_emp_id THEN
            RAISE EXCEPTION 'Application % is assigned to %, not you (%)',
                            p_app_id, v_app.assigned_emp_id, p_emp_id;
        END IF;

    ELSE
        RAISE EXCEPTION 'No permission';
    END IF;

    -- UPDATE
    UPDATE loan_application
       SET status = p_new_status,
           reviewed_at = NOW(),
           decision_notes = COALESCE(p_notes, decision_notes),
           assigned_emp_id = p_emp_id
     WHERE application_id = p_app_id;

    -- APPROVAL BLOCK
    IF p_new_status = 'Approved' THEN
        DECLARE
            v_new_loan_id VARCHAR(10);
            v_rate        DECIMAL(5,2);
            v_income      DECIMAL(15,2);
            v_credit      INT;
        BEGIN
            SELECT cf.annual_income, cf.credit_score
              INTO v_income, v_credit
              FROM customer_financials cf
             WHERE cf.customer_id = v_app.customer_id
             ORDER BY cf.fin_id DESC LIMIT 1;

            v_rate := CASE
                WHEN COALESCE(v_credit, 650) >= 750 THEN 7.00
                WHEN COALESCE(v_credit, 650) >= 700 THEN 8.00
                ELSE 10.00
            END;

            v_new_loan_id := 'LOAN' || LPAD(
                (SELECT COUNT(*) + 1 FROM loan_current)::TEXT, 3, '0');

            INSERT INTO loan_current (
                loan_id, borrower_id, loan_amount, interest_rate,
                application_income, employment_length,
                approved_by_emp_id, loan_status, updated_at
            )
            VALUES (
                v_new_loan_id, v_app.customer_id,
                v_app.requested_amount, v_rate,
                COALESCE(v_income, 0),
                3,
                p_emp_id, 'Current', NOW()
            );
        END;
    END IF;

    RETURN 'Updated successfully';
END;
$$ LANGUAGE plpgsql;
-- ============================================================
-- SECTION 7: ACCOUNT MINI-STATEMENT (Customer view helper)
-- ============================================================
CREATE OR REPLACE FUNCTION bank_mini_statement(
    p_account_id VARCHAR,
    p_limit      INT DEFAULT 10
)
RETURNS TABLE (
    txn_id      TEXT,
    txn_type    TEXT,
    amount      DECIMAL,
    direction   TEXT,    -- CR or DR
    txn_time    TIMESTAMP,
    counterpart TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.txn_id::TEXT,
        t.txn_type::TEXT,
        t.amount,
        CASE
            WHEN t.txn_type IN ('Deposit') THEN 'CR'
            WHEN t.txn_type IN ('Withdrawal') THEN 'DR'
            WHEN t.txn_type = 'Transfer' AND t.account_id = p_account_id THEN 'DR'
            ELSE 'CR'
        END,
        t.txn_time,
        COALESCE(t.related_account_id, '—')::TEXT
    FROM transaction t
    WHERE t.account_id = p_account_id
       OR t.related_account_id = p_account_id
    ORDER BY t.txn_time DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SECTION 8: UTILITY FUNCTIONS
-- ============================================================

-- Get a customer's complete account summary
CREATE OR REPLACE FUNCTION bank_customer_summary(p_customer_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    v_name    TEXT;
    v_result  TEXT := '';
    rec       RECORD;
BEGIN
    SELECT full_name INTO v_name FROM customer WHERE customer_id = p_customer_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'Customer "%" not found', p_customer_id; END IF;

    v_result := format('=== Customer Summary: %s (%s) ===' || chr(10), v_name, p_customer_id);

    v_result := v_result || chr(10) || '--- Accounts ---' || chr(10);
    FOR rec IN (
        SELECT account_id, account_type, balance, status FROM account
        WHERE customer_id = p_customer_id ORDER BY opened_date
    ) LOOP
        v_result := v_result || format('  %s [%s] %s — ₹%s' || chr(10),
            rec.account_id, rec.account_type, rec.status, rec.balance::NUMERIC(15,2));
    END LOOP;

    v_result := v_result || chr(10) || '--- Active Loans ---' || chr(10);
    FOR rec IN (
        SELECT loan_id, loan_amount, interest_rate, loan_status FROM loan_current
        WHERE borrower_id = p_customer_id ORDER BY updated_at
    ) LOOP
        v_result := v_result || format('  %s ₹%s @%s%% — %s' || chr(10),
            rec.loan_id, rec.loan_amount::NUMERIC(15,2), rec.interest_rate, rec.loan_status);
    END LOOP;

    v_result := v_result || chr(10) || '--- Loan Applications ---' || chr(10);
    FOR rec IN (
        SELECT application_id, requested_amount, status, application_date FROM loan_application
        WHERE customer_id = p_customer_id ORDER BY application_date
    ) LOOP
        v_result := v_result || format('  %s ₹%s — %s (applied %s)' || chr(10),
            rec.application_id, rec.requested_amount::NUMERIC(15,2), rec.status,
            rec.application_date::DATE);
    END LOOP;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Get an employee's pending work queue
CREATE OR REPLACE FUNCTION bank_emp_queue(p_emp_id VARCHAR)
RETURNS TABLE (
    app_id          TEXT,
    customer        INT,
    amount          DECIMAL,
    status          TEXT,
    days_waiting    INT,
    credit_score    INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        la.application_id::TEXT,
        c.full_name::TEXT,
        la.requested_amount,
        la.status::TEXT,
        EXTRACT(DAY FROM NOW() - la.application_date)::INT,
        cf.credit_score
    FROM loan_application la
    JOIN customer c ON c.customer_id = la.customer_id
    LEFT JOIN customer_financials cf ON cf.customer_id = la.customer_id
    WHERE la.assigned_emp_id = p_emp_id
      AND la.status NOT IN ('Approved','Rejected','Disbursed')
    ORDER BY la.application_date;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SECTION 10: SAMPLE DATA — Loan Applications
-- ============================================================
SELECT bank_apply_loan(001, 30000.00, 'Home renovation');
SELECT bank_apply_loan(004, 12000.00, 'Medical expenses');
SELECT bank_apply_loan(003, 50000.00, 'Business expansion');
SELECT bank_apply_loan(002, 8000.00,  'Education fees');

-- ============================================================
-- SECTION 11: QUICK DEMO QUERIES
-- ============================================================
DO $$ BEGIN
  RAISE NOTICE 'Banking Layer installed. Try these:';
  RAISE NOTICE '';
  RAISE NOTICE '-- Deposit / Withdraw / Transfer';
  RAISE NOTICE 'SELECT bank_deposit(''ACC001'', 5000.00, ''Salary credit'');';
  RAISE NOTICE 'SELECT bank_withdraw(''ACC001'', 1000.00, ''ATM withdrawal'');';
  RAISE NOTICE 'SELECT bank_transfer(''ACC001'', ''ACC002'', 2000.00, ''Rent payment'');';
  RAISE NOTICE '';
  RAISE NOTICE '-- Loan workflow';
  RAISE NOTICE 'SELECT bank_apply_loan( 3, 25000.00, ''Car loan'');';
  RAISE NOTICE 'SELECT bank_review_loan(''APP0001'', ''EMP002'', ''Under Review'', ''Checking docs'');';
  RAISE NOTICE 'SELECT bank_review_loan(''APP0001'', ''EMP002'', ''Approved'', ''All good'');';
  RAISE NOTICE '';
  RAISE NOTICE '-- Role views';
  RAISE NOTICE 'SELECT * FROM view_customer_portal   WHERE customer_id = ''CUST001'';';
  RAISE NOTICE 'SELECT * FROM view_employee_workbench WHERE emp_id     = ''EMP002'';';
  RAISE NOTICE 'SELECT * FROM view_manager_dashboard  WHERE branch_id  = ''BR001'';';
  RAISE NOTICE 'SELECT * FROM view_loan_pipeline;';
  RAISE NOTICE 'SELECT * FROM view_account_ledger WHERE account_id = ''ACC001'';';
  RAISE NOTICE '';
  RAISE NOTICE '-- Helpers';
  RAISE NOTICE 'SELECT bank_customer_summary(''CUST001'');';
  RAISE NOTICE 'SELECT * FROM bank_emp_queue(''EMP002'');';
  RAISE NOTICE 'SELECT * FROM bank_mini_statement(''ACC001'', 5);';
END $$;

SELECT 'Banking Layer created: deposits, withdrawals, transfers, loan workflow, 5 role-based views.' AS status;

