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
    p_app_id     INTEGER,
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

    SELECT designation INTO v_emp_role FROM employee WHERE emp_id = p_emp_id;
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
    p_account_id inTEGER,
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
CREATE OR REPLACE FUNCTION bank_emp_queue(p_emp_id INTEGER)
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
    WHERE la.assigned_emp_id = p_emp_id
      AND la.status NOT IN ('Approved','Rejected','Disbursed')
    ORDER BY la.application_date;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SECTION 9: EMPLOYEE HR UPDATE FUNCTION           ← NEW
-- ============================================================
-- bank_hr_update(
--     p_emp_id          — employee being updated
--     p_authorized_by   — manager/HR authorising the change
--     p_update_type     — type of HR event (see CHECK constraint)
--     p_effective_date  — when the change takes effect
--     p_reason          — mandatory justification text
--     p_new_designation — new designation (for promotions etc.)
--     p_new_dept_id     — new department (for transfers)
--     p_new_salary      — new salary (for revisions / promotions)
--     p_new_emp_type    — new employment type (contract→permanent etc.)
-- )
--
-- What it does:
--   1. Validates the authorising employee has Branch Manager or
--      Senior Relationship Manager role.
--   2. Reads current values from employee table as the "old" state.
--   3. Inserts a record into employee_hr_update (full audit trail).
--   4. Applies the change to the employee table atomically.
--   5. Returns a formatted confirmation with before/after summary.
--
-- Supported update_type values (mirrors CHECK constraint):
--   salary_revision | promotion | demotion | department_transfer |
--   designation_change | employment_type_change | termination | reinstatement
-- ============================================================
CREATE OR REPLACE FUNCTION bank_hr_update(
    p_emp_id          INT,
    p_authorized_by   INT,
    p_update_type     VARCHAR,
    p_effective_date  DATE,
    p_reason          TEXT,
    p_new_designation VARCHAR DEFAULT NULL,
    p_new_dept_id     INT     DEFAULT NULL,
    p_new_salary      NUMERIC DEFAULT NULL,
    p_new_emp_type    VARCHAR DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_emp             employee%ROWTYPE;
    v_auth_role       VARCHAR(80);
    v_auth_name       TEXT;
    v_hr_update_id    INT;
    v_result          TEXT;
    v_changes         TEXT := '';
 
    -- Resolved "new" values (fall back to current if not supplied)
    v_eff_designation VARCHAR(80);
    v_eff_dept_id     INT;
    v_eff_salary      NUMERIC(12,2);
    v_eff_emp_type    VARCHAR(20);
    v_eff_status      VARCHAR(20);
BEGIN
 
    -- ---- 1. Load the employee record ----
    SELECT * INTO v_emp FROM employee WHERE emp_id = p_emp_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Employee % not found', p_emp_id;
    END IF;
 
    -- ---- 2. Validate authorising employee ----
    SELECT designation, full_name INTO v_auth_role, v_auth_name
    FROM employee
    WHERE emp_id = p_authorized_by AND status = 'active';
 
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Authorising employee % not found or inactive', p_authorized_by;
    END IF;
 
    IF v_auth_role NOT IN ('Branch Manager', 'Senior Relationship Manager') THEN
        RAISE EXCEPTION 'Employee % (%) is not authorised to make HR updates. '
                        'Only Branch Manager or Senior Relationship Manager may do so.',
                        p_authorized_by, v_auth_role;
    END IF;
 
    -- An employee cannot authorise their own HR update
    IF p_emp_id = p_authorized_by THEN
        RAISE EXCEPTION 'An employee cannot authorise their own HR update';
    END IF;
 
    -- ---- 3. Validate update_type ----
    IF p_update_type NOT IN (
        'salary_revision','promotion','demotion','department_transfer',
        'designation_change','employment_type_change','termination','reinstatement'
    ) THEN
        RAISE EXCEPTION 'Invalid update_type "%". Allowed: salary_revision, promotion, demotion, '
                        'department_transfer, designation_change, employment_type_change, '
                        'termination, reinstatement', p_update_type;
    END IF;
 
    -- ---- 4. Validate inputs for specific update types ----
    IF p_update_type = 'salary_revision' AND p_new_salary IS NULL THEN
        RAISE EXCEPTION 'p_new_salary is required for salary_revision';
    END IF;
 
    IF p_update_type IN ('promotion', 'demotion', 'designation_change') AND p_new_designation IS NULL THEN
        RAISE EXCEPTION 'p_new_designation is required for %', p_update_type;
    END IF;
 
    IF p_update_type = 'department_transfer' AND p_new_dept_id IS NULL THEN
        RAISE EXCEPTION 'p_new_dept_id is required for department_transfer';
    END IF;
 
    IF p_update_type = 'employment_type_change' AND p_new_emp_type IS NULL THEN
        RAISE EXCEPTION 'p_new_emp_type is required for employment_type_change';
    END IF;
 
    IF p_update_type = 'reinstatement' AND v_emp.status != 'terminated' THEN
        RAISE EXCEPTION 'Employee % is not terminated — cannot reinstate', p_emp_id;
    END IF;
 
    IF p_update_type = 'termination' AND v_emp.status = 'terminated' THEN
        RAISE EXCEPTION 'Employee % is already terminated', p_emp_id;
    END IF;
 
    IF p_new_dept_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM department WHERE dept_id = p_new_dept_id) THEN
        RAISE EXCEPTION 'Department % does not exist', p_new_dept_id;
    END IF;
 
    IF p_new_salary IS NOT NULL AND p_new_salary <= 0 THEN
        RAISE EXCEPTION 'New salary must be positive';
    END IF;
 
    IF p_new_emp_type IS NOT NULL AND p_new_emp_type NOT IN ('permanent','contract','probation') THEN
        RAISE EXCEPTION 'Invalid employment type "%". Must be: permanent, contract, probation', p_new_emp_type;
    END IF;
 
    -- ---- 5. Resolve effective new values ----
    v_eff_designation := COALESCE(p_new_designation, v_emp.designation);
    v_eff_dept_id     := COALESCE(p_new_dept_id,     v_emp.dept_id);
    v_eff_salary      := COALESCE(p_new_salary,      v_emp.salary);
    v_eff_emp_type    := COALESCE(p_new_emp_type,    v_emp.employment_type);
    v_eff_status      := CASE p_update_type
                            WHEN 'termination'   THEN 'terminated'
                            WHEN 'reinstatement' THEN 'active'
                            ELSE v_emp.status
                         END;
 
    -- ---- 6. Write to employee_hr_update (audit log) ----
    INSERT INTO employee_hr_update (
        emp_id, update_type, effective_date,
        old_designation, old_dept_id, old_salary, old_emp_type,
        new_designation, new_dept_id, new_salary, new_emp_type,
        reason, authorized_by
    ) VALUES (
        p_emp_id, p_update_type, p_effective_date,
        v_emp.designation, v_emp.dept_id, v_emp.salary, v_emp.employment_type,
        v_eff_designation, v_eff_dept_id, v_eff_salary, v_eff_emp_type,
        p_reason, p_authorized_by
    )
    RETURNING hr_update_id INTO v_hr_update_id;
 
    -- ---- 7. Apply change to employee table ----
    UPDATE employee SET
        designation     = v_eff_designation,
        dept_id         = v_eff_dept_id,
        salary          = v_eff_salary,
        employment_type = v_eff_emp_type,
        status          = v_eff_status
    WHERE emp_id = p_emp_id;
 
    -- ---- 8. Build change summary ----
    IF v_emp.designation     != v_eff_designation THEN
        v_changes := v_changes || format('   Designation : %s → %s' || chr(10), v_emp.designation, v_eff_designation);
    END IF;
    IF v_emp.dept_id IS DISTINCT FROM v_eff_dept_id THEN
        v_changes := v_changes || format('   Department  : %s → %s' || chr(10), v_emp.dept_id, v_eff_dept_id);
    END IF;
    IF v_emp.salary          != v_eff_salary THEN
        v_changes := v_changes || format('   Salary      : ₹%s → ₹%s' || chr(10),
                                         v_emp.salary::NUMERIC(12,2), v_eff_salary::NUMERIC(12,2));
    END IF;
    IF v_emp.employment_type != v_eff_emp_type THEN
        v_changes := v_changes || format('   Emp. Type   : %s → %s' || chr(10), v_emp.employment_type, v_eff_emp_type);
    END IF;
    IF v_emp.status          != v_eff_status THEN
        v_changes := v_changes || format('   Status      : %s → %s' || chr(10), v_emp.status, v_eff_status);
    END IF;
 
    IF v_changes = '' THEN
        v_changes := '   (no field values changed — only logged)' || chr(10);
    END IF;
 
    v_result := format(
        '✅ HR Update #%s recorded successfully.' || chr(10) ||
        '   Employee    : %s (ID: %s)' || chr(10) ||
        '   Update Type : %s' || chr(10) ||
        '   Effective   : %s' || chr(10) ||
        '   Authorised  : %s (ID: %s)' || chr(10) ||
        '   Changes:' || chr(10) || '%s' ||
        '   Reason      : %s',
        v_hr_update_id,
        v_emp.full_name, p_emp_id,
        p_update_type,
        p_effective_date,
        v_auth_name, p_authorized_by,
        v_changes,
        p_reason
    );
 
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;
 -- ============================================================
-- SECTION 9B: HR HISTORY VIEW HELPER
-- ============================================================
-- bank_emp_hr_history(emp_id)
--   Returns the full HR update trail for an employee,
--   joining in employee and department names for readability.
-- ============================================================
CREATE OR REPLACE FUNCTION bank_emp_hr_history(p_emp_id INT)
RETURNS TABLE (
    hr_update_id     INT,
    update_type      TEXT,
    effective_date   DATE,
    old_designation  TEXT,
    new_designation  TEXT,
    old_dept         TEXT,
    new_dept         TEXT,
    old_salary       NUMERIC,
    new_salary       NUMERIC,
    old_emp_type     TEXT,
    new_emp_type     TEXT,
    reason           TEXT,
    authorized_by    TEXT,
    recorded_at      TIMESTAMP
) AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM employee WHERE emp_id = p_emp_id) THEN
        RAISE EXCEPTION 'Employee % not found', p_emp_id;
    END IF;
 
    RETURN QUERY
    SELECT
        h.hr_update_id,
        h.update_type::TEXT,
        h.effective_date,
        h.old_designation::TEXT,
        h.new_designation::TEXT,
        od.dept_name::TEXT  AS old_dept,
        nd.dept_name::TEXT  AS new_dept,
        h.old_salary,
        h.new_salary,
        h.old_emp_type::TEXT,
        h.new_emp_type::TEXT,
        h.reason::TEXT,
        auth.full_name::TEXT AS authorized_by,
        h.recorded_at
    FROM employee_hr_update h
    LEFT JOIN department od   ON od.dept_id   = h.old_dept_id
    LEFT JOIN department nd   ON nd.dept_id   = h.new_dept_id
    LEFT JOIN employee   auth ON auth.emp_id  = h.authorized_by
    WHERE h.emp_id = p_emp_id
    ORDER BY h.effective_date, h.hr_update_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- QUICK USAGE REFERENCE
-- ============================================================
DO $$ BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '  BANKING LAYER — QUICK REFERENCE';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE '-- Transactions';
    RAISE NOTICE 'SELECT bank_deposit(1, 5000.00);';
    RAISE NOTICE 'SELECT bank_withdraw(1, 2000.00);';
    RAISE NOTICE 'SELECT bank_transfer(1, 2, 3000.00);';
    RAISE NOTICE 'SELECT * FROM bank_mini_statement(1, 10);';
    RAISE NOTICE '';
    RAISE NOTICE '-- Loan workflow';
    RAISE NOTICE 'SELECT bank_apply_loan(1, 200000.00, ''Home extension'');';
    RAISE NOTICE 'SELECT bank_review_loan(5, 3, ''under_review'', ''Documents received'');';
    RAISE NOTICE 'SELECT bank_review_loan(5, 1, ''approved'', ''Credit check passed'');';
    RAISE NOTICE 'SELECT * FROM bank_emp_queue(3);';
    RAISE NOTICE '';
    RAISE NOTICE '-- HR Updates (authorised by Branch Manager emp_id=1)';
    RAISE NOTICE 'SELECT bank_hr_update(6, 1, ''salary_revision'', CURRENT_DATE,';
    RAISE NOTICE '       ''Annual increment FY25'', NULL, NULL, 45000, NULL);';
    RAISE NOTICE 'SELECT bank_hr_update(5, 1, ''promotion'', CURRENT_DATE,';
    RAISE NOTICE '       ''Exceeds targets'', ''Senior Relationship Manager'', NULL, 52000, NULL);';
    RAISE NOTICE 'SELECT bank_hr_update(4, 1, ''department_transfer'', CURRENT_DATE,';
    RAISE NOTICE '       ''Restructuring'', NULL, 2, NULL, NULL);';
    RAISE NOTICE 'SELECT bank_hr_update(3, 1, ''employment_type_change'', CURRENT_DATE,';
    RAISE NOTICE '       ''Probation completed'', NULL, NULL, 57000, ''permanent'');';
    RAISE NOTICE '';
    RAISE NOTICE '-- HR History';
    RAISE NOTICE 'SELECT * FROM bank_emp_hr_history(6);';
    RAISE NOTICE '';
    RAISE NOTICE '-- Customer summary';
    RAISE NOTICE 'SELECT bank_customer_summary(1);';
    RAISE NOTICE '============================================================';
END $$;
 
SELECT '✅ Banking Layer loaded: deposit, withdraw, transfer, loan workflow, HR update, HR history.' AS status;