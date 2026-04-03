-- ============================================================
-- ROLE-BASED ACCESS CONTROL & VIEWS
-- ============================================================
-- Three-tier access model:
--   ROLE: bank_customer  — sees only their own data
--   ROLE: bank_employee  — sees customers they handle + own HR record
--   ROLE: bank_manager   — full branch-wide superuser access
--
-- Compatible with: retail_banking_setup_final.sql + banking_layer.sql
--
-- Load order: run AFTER banking_layer.sql
-- ============================================================

-- ============================================================
-- PART 1: POSTGRESQL DATABASE ROLES
-- ============================================================
-- Creates three login roles. In production, individual DB users
-- are GRANTED one of these roles. The views + RLS policies then
-- enforce what each role can read/write.
--
-- NOTE: CREATE ROLE will error if the role already exists.
--       Use DO block to create idempotently.
-- ============================================================

DO $$
BEGIN
    -- Customer role: read-only access to their own data only
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bank_customer') THEN
        CREATE ROLE bank_customer NOLOGIN;
    END IF;

    -- Employee role: read own HR data + customers they are assigned to
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bank_employee') THEN
        CREATE ROLE bank_employee NOLOGIN;
    END IF;

    -- Manager role: full read access to all branch data, can call HR functions
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bank_manager') THEN
        CREATE ROLE bank_manager NOLOGIN;
    END IF;
END $$;

-- ============================================================
-- PART 2: ROW-LEVEL SECURITY (RLS) ON CORE TABLES
-- ============================================================
-- RLS enforces data isolation at the PostgreSQL engine level —
-- even if a role has SELECT on a table, they only see rows
-- that pass the policy predicate.
--
-- We use a session variable (app.current_user_id) to carry the
-- logged-in user's ID into every query. The application layer
-- sets this at connection time:
--   SET app.current_user_id = '3';           -- customer_id
--   SET app.current_employee_id = '5';       -- emp_id
-- ============================================================

-- ---- customer table ----
ALTER TABLE customer ENABLE ROW LEVEL SECURITY;
ALTER TABLE customer FORCE ROW LEVEL SECURITY;

-- Customers see only themselves
DROP POLICY IF EXISTS policy_customer_self ON customer;
CREATE POLICY policy_customer_self ON customer
    FOR SELECT
    TO bank_customer
    USING (customer_id = current_setting('app.current_user_id', TRUE)::INT);

-- Employees see customers assigned to them (as RM) or in their loan applications
DROP POLICY IF EXISTS policy_employee_customers ON customer;
CREATE POLICY policy_employee_customers ON customer
    FOR SELECT
    TO bank_employee
    USING (
        assigned_rm_id = current_setting('app.current_employee_id', TRUE)::INT
        OR customer_id IN (
            SELECT customer_id FROM loan_application
            WHERE assigned_emp_id = current_setting('app.current_employee_id', TRUE)::INT
        )
    );

-- Managers see all customers (unrestricted)
DROP POLICY IF EXISTS policy_manager_all_customers ON customer;
CREATE POLICY policy_manager_all_customers ON customer
    FOR ALL
    TO bank_manager
    USING (TRUE);

-- ---- account table ----
ALTER TABLE account ENABLE ROW LEVEL SECURITY;
ALTER TABLE account FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_customer_own_accounts ON account;
CREATE POLICY policy_customer_own_accounts ON account
    FOR SELECT
    TO bank_customer
    USING (customer_id = current_setting('app.current_user_id', TRUE)::INT);

DROP POLICY IF EXISTS policy_employee_managed_accounts ON account;
CREATE POLICY policy_employee_managed_accounts ON account
    FOR SELECT
    TO bank_employee
    USING (
        customer_id IN (
            SELECT customer_id FROM customer
            WHERE assigned_rm_id = current_setting('app.current_employee_id', TRUE)::INT
        )
        OR customer_id IN (
            SELECT customer_id FROM loan_application
            WHERE assigned_emp_id = current_setting('app.current_employee_id', TRUE)::INT
        )
    );

DROP POLICY IF EXISTS policy_manager_all_accounts ON account;
CREATE POLICY policy_manager_all_accounts ON account
    FOR ALL TO bank_manager USING (TRUE);

-- ---- transaction table ----
ALTER TABLE transaction ENABLE ROW LEVEL SECURITY;
ALTER TABLE transaction FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_customer_own_txns ON transaction;
CREATE POLICY policy_customer_own_txns ON transaction
    FOR SELECT
    TO bank_customer
    USING (
        account_id IN (
            SELECT account_id FROM account
            WHERE customer_id = current_setting('app.current_user_id', TRUE)::INT
        )
    );

DROP POLICY IF EXISTS policy_employee_managed_txns ON transaction;
CREATE POLICY policy_employee_managed_txns ON transaction
    FOR SELECT
    TO bank_employee
    USING (
        account_id IN (
            SELECT a.account_id FROM account a
            JOIN customer c ON c.customer_id = a.customer_id
            WHERE c.assigned_rm_id = current_setting('app.current_employee_id', TRUE)::INT
        )
    );

DROP POLICY IF EXISTS policy_manager_all_txns ON transaction;
CREATE POLICY policy_manager_all_txns ON transaction
    FOR ALL TO bank_manager USING (TRUE);

-- ---- loan table ----
ALTER TABLE loan ENABLE ROW LEVEL SECURITY;
ALTER TABLE loan FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_customer_own_loans ON loan;
CREATE POLICY policy_customer_own_loans ON loan
    FOR SELECT
    TO bank_customer
    USING (customer_id = current_setting('app.current_user_id', TRUE)::INT);

DROP POLICY IF EXISTS policy_employee_managed_loans ON loan;
CREATE POLICY policy_employee_managed_loans ON loan
    FOR SELECT
    TO bank_employee
    USING (
        assigned_officer = current_setting('app.current_employee_id', TRUE)::INT
        OR customer_id IN (
            SELECT customer_id FROM customer
            WHERE assigned_rm_id = current_setting('app.current_employee_id', TRUE)::INT
        )
    );

DROP POLICY IF EXISTS policy_manager_all_loans ON loan;
CREATE POLICY policy_manager_all_loans ON loan
    FOR ALL TO bank_manager USING (TRUE);

-- ---- loan_application table ----
ALTER TABLE loan_application ENABLE ROW LEVEL SECURITY;
ALTER TABLE loan_application FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_customer_own_apps ON loan_application;
CREATE POLICY policy_customer_own_apps ON loan_application
    FOR SELECT
    TO bank_customer
    USING (customer_id = current_setting('app.current_user_id', TRUE)::INT);

DROP POLICY IF EXISTS policy_employee_assigned_apps ON loan_application;
CREATE POLICY policy_employee_assigned_apps ON loan_application
    FOR ALL
    TO bank_employee
    USING (assigned_emp_id = current_setting('app.current_employee_id', TRUE)::INT);

DROP POLICY IF EXISTS policy_manager_all_apps ON loan_application;
CREATE POLICY policy_manager_all_apps ON loan_application
    FOR ALL TO bank_manager USING (TRUE);

-- ---- employee table ----
-- Employees may see their own record only; managers see all.
-- Customers have no direct access to employee records.
ALTER TABLE employee ENABLE ROW LEVEL SECURITY;
ALTER TABLE employee FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_employee_self ON employee;
CREATE POLICY policy_employee_self ON employee
    FOR SELECT
    TO bank_employee
    USING (emp_id = current_setting('app.current_employee_id', TRUE)::INT);

DROP POLICY IF EXISTS policy_manager_all_employees ON employee;
CREATE POLICY policy_manager_all_employees ON employee
    FOR ALL TO bank_manager USING (TRUE);

-- ---- employee_hr_update table ----
-- Employees see only their own HR events; managers see all.
ALTER TABLE employee_hr_update ENABLE ROW LEVEL SECURITY;
ALTER TABLE employee_hr_update FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_employee_own_hr ON employee_hr_update;
CREATE POLICY policy_employee_own_hr ON employee_hr_update
    FOR SELECT
    TO bank_employee
    USING (emp_id = current_setting('app.current_employee_id', TRUE)::INT);

DROP POLICY IF EXISTS policy_manager_all_hr ON employee_hr_update;
CREATE POLICY policy_manager_all_hr ON employee_hr_update
    FOR ALL TO bank_manager USING (TRUE);

-- ---- fund_transfer table ----
ALTER TABLE fund_transfer ENABLE ROW LEVEL SECURITY;
ALTER TABLE fund_transfer FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS policy_customer_own_transfers ON fund_transfer;
CREATE POLICY policy_customer_own_transfers ON fund_transfer
    FOR SELECT
    TO bank_customer
    USING (
        from_account_id IN (
            SELECT account_id FROM account
            WHERE customer_id = current_setting('app.current_user_id', TRUE)::INT
        )
        OR to_account_id IN (
            SELECT account_id FROM account
            WHERE customer_id = current_setting('app.current_user_id', TRUE)::INT
        )
    );

DROP POLICY IF EXISTS policy_employee_managed_transfers ON fund_transfer;
CREATE POLICY policy_employee_managed_transfers ON fund_transfer
    FOR SELECT
    TO bank_employee
    USING (
        from_account_id IN (
            SELECT a.account_id FROM account a
            JOIN customer c ON c.customer_id = a.customer_id
            WHERE c.assigned_rm_id = current_setting('app.current_employee_id', TRUE)::INT
        )
    );

DROP POLICY IF EXISTS policy_manager_all_transfers ON fund_transfer;
CREATE POLICY policy_manager_all_transfers ON fund_transfer
    FOR ALL TO bank_manager USING (TRUE);

-- ============================================================
-- PART 3: GRANT TABLE PERMISSIONS TO ROLES
-- ============================================================
-- Tables that customers may read (their own rows only — RLS filters)
GRANT SELECT ON customer, account, transaction, loan, loan_application, fund_transfer
    TO bank_customer;

-- Tables employees may read/update (RLS filters to their scope)
GRANT SELECT ON
    customer, account, transaction, loan, loan_application,
    fund_transfer, employee, employee_hr_update, department, branch
    TO bank_employee;

GRANT INSERT, UPDATE ON loan_application TO bank_employee;

-- Managers get full DML across all business tables
GRANT SELECT, INSERT, UPDATE, DELETE ON
    customer, account, transaction, loan, loan_application,
    fund_transfer, employee, employee_hr_update, department, branch
    TO bank_manager;

-- Grant execute on banking functions
GRANT EXECUTE ON FUNCTION bank_deposit(INT, NUMERIC)             TO bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_withdraw(INT, NUMERIC)            TO bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_transfer(INT, INT, NUMERIC)       TO bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_apply_loan(INT, DECIMAL, TEXT)    TO bank_customer, bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_review_loan(INT, INT, VARCHAR, TEXT) TO bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_mini_statement(INT, INT)          TO bank_customer, bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_customer_summary(INT)             TO bank_customer, bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_emp_queue(INT)                    TO bank_employee, bank_manager;
GRANT EXECUTE ON FUNCTION bank_hr_update(INT,INT,VARCHAR,DATE,TEXT,VARCHAR,INT,NUMERIC,VARCHAR)
    TO bank_manager;
GRANT EXECUTE ON FUNCTION bank_emp_hr_history(INT)               TO bank_employee, bank_manager;

-- ============================================================
-- PART 4: ROLE-SCOPED VIEWS
-- ============================================================
-- These views give each role a clean, purpose-built interface.
-- RLS policies on underlying tables ensure each role only sees
-- rows they are entitled to even when querying views directly.
-- ============================================================

-- ----------------------------------------------------------
-- VIEW: Customer Portal
-- Purpose  : Everything a customer needs about their own profile,
--            accounts, loans, and applications — nothing else.
-- Access   : bank_customer (via RLS on underlying tables)
--            bank_employee & bank_manager can also query this
--            and will see only the rows their RLS allows.
-- Usage    :
--   SET app.current_user_id = '2';
--   SELECT * FROM view_customer_portal;
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_customer_portal CASCADE;

CREATE VIEW view_customer_portal AS
SELECT
    -- Identity
    c.customer_id,
    c.full_name,
    c.phone,
    c.email,
    c.occupation,
    c.income_bracket,
    c.kyc_status,
    c.customer_since,
    c.status                              AS customer_status,

    -- Branch
    b.branch_name,
    b.city,
    b.phone                               AS branch_phone,

    -- Accounts rollup
    acct.active_account_count,
    acct.total_active_balance,
    acct.account_details,                  -- JSONB array of all accounts

    -- Loans rollup
    loans.active_loan_count,
    loans.outstanding_loan_balance,
    loans.loan_details,                    -- JSONB array of active loans

    -- Applications rollup
    apps.pending_application_count,
    apps.latest_application_date,
    apps.application_details               -- JSONB array of all applications

FROM customer c
JOIN branch b ON b.branch_id = c.branch_id

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)        FILTER (WHERE a.status = 'active')           AS active_account_count,
        COALESCE(SUM(a.current_balance) FILTER (WHERE a.status = 'active'), 0) AS total_active_balance,
        jsonb_agg(jsonb_build_object(
            'account_id',      a.account_id,
            'account_number',  a.account_number,
            'account_type',    a.account_type,
            'balance',         a.current_balance,
            'status',          a.status,
            'opened_date',     a.opened_date
        ) ORDER BY a.opened_date)                                     AS account_details
    FROM account a
    WHERE a.customer_id = c.customer_id
) acct ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)        FILTER (WHERE l.status = 'active')           AS active_loan_count,
        COALESCE(SUM(l.outstanding_principal) FILTER (WHERE l.status = 'active'), 0) AS outstanding_loan_balance,
        jsonb_agg(jsonb_build_object(
            'loan_id',            l.loan_id,
            'loan_type',          l.loan_type,
            'status',             l.status,
            'interest_rate',      l.interest_rate,
            'tenure_months',      l.tenure_months,
            'emi_amount',         l.emi_amount,
            'outstanding_principal', l.outstanding_principal,
            'maturity_date',      l.maturity_date
        ) ORDER BY l.disbursement_date)                               AS loan_details
    FROM loan l
    WHERE l.customer_id = c.customer_id
) loans ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)        FILTER (WHERE la.status IN ('submitted','under_review')) AS pending_application_count,
        MAX(la.application_date)                                      AS latest_application_date,
        jsonb_agg(jsonb_build_object(
            'application_id',    la.application_id,
            'requested_amount',  la.requested_amount,
            'purpose',           la.purpose,
            'status',            la.status,
            'application_date',  la.application_date,
            'reviewed_at',       la.reviewed_at,
            'decision_notes',    la.decision_notes
        ) ORDER BY la.application_date DESC)                          AS application_details
    FROM loan_application la
    WHERE la.customer_id = c.customer_id
) apps ON TRUE;

GRANT SELECT ON view_customer_portal TO bank_customer, bank_employee, bank_manager;

-- ----------------------------------------------------------
-- VIEW: Employee Workbench
-- Purpose  : Shows an employee their own profile, the customers
--            they are responsible for, and the loan applications
--            in their queue. Sensitive data (Aadhaar, PAN)
--            is masked for non-manager employees.
-- Access   : bank_employee (RLS scopes to their assigned customers)
--            bank_manager (sees all)
-- Usage    :
--   SET app.current_employee_id = '3';
--   SELECT * FROM view_employee_workbench;
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_employee_workbench CASCADE;

CREATE VIEW view_employee_workbench AS
SELECT
    -- Employee identity
    e.emp_id,
    e.full_name                           AS employee_name,
    e.designation,
    e.employment_type,
    e.salary,
    e.status                              AS employee_status,
    d.dept_name,
    b.branch_name,

    -- Customer the employee is responsible for
    c.customer_id,
    c.full_name                           AS customer_name,
    c.phone                               AS customer_phone,
    c.email                               AS customer_email,
    c.occupation,
    c.kyc_status,
    c.status                              AS customer_status,
    -- Sensitive identity fields masked for employees; shown for managers
    CASE
        WHEN current_setting('app.current_role', TRUE) = 'manager'
        THEN c.aadhaar_number
        ELSE 'XXXX-XXXX-' || RIGHT(c.aadhaar_number, 4)
    END                                   AS aadhaar_masked,
    CASE
        WHEN current_setting('app.current_role', TRUE) = 'manager'
        THEN c.pan_number
        ELSE LEFT(c.pan_number, 3) || 'XXXXXXX'
    END                                   AS pan_masked,

    -- Loan application in this employee's queue
    la.application_id,
    la.requested_amount,
    la.purpose                            AS loan_purpose,
    la.status                             AS application_status,
    la.application_date,
    la.reviewed_at,
    la.decision_notes,
    EXTRACT(DAY FROM NOW() - la.application_date)::INT AS days_pending,

    -- Active loan the employee is assigned as loan officer
    lo.loan_id,
    lo.loan_type,
    lo.outstanding_principal,
    lo.interest_rate                      AS loan_rate,
    lo.emi_amount,
    lo.status                             AS loan_status

FROM employee e
JOIN branch     b ON b.branch_id = e.branch_id
LEFT JOIN department d ON d.dept_id  = e.dept_id

-- Customers this employee is the assigned RM for
LEFT JOIN customer c
    ON c.assigned_rm_id = e.emp_id

-- Loan applications assigned to this employee
LEFT JOIN loan_application la
    ON la.assigned_emp_id = e.emp_id
    AND (c.customer_id IS NULL OR la.customer_id = c.customer_id)

-- Active loans where this employee is the assigned officer
LEFT JOIN loan lo
    ON lo.assigned_officer = e.emp_id
    AND (c.customer_id IS NULL OR lo.customer_id = c.customer_id)
    AND lo.status = 'active'

WHERE e.status = 'active';

GRANT SELECT ON view_employee_workbench TO bank_employee, bank_manager;

-- ----------------------------------------------------------
-- VIEW: Employee HR Record (self-service)
-- Purpose  : An employee's own HR history — salary changes,
--            promotions, transfers. Only their own record.
-- Access   : bank_employee (RLS on employee_hr_update restricts to self)
--            bank_manager (sees all)
-- Usage    :
--   SET app.current_employee_id = '6';
--   SELECT * FROM view_employee_hr_record;
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_employee_hr_record CASCADE;

CREATE VIEW view_employee_hr_record AS
SELECT
    e.emp_id,
    e.full_name,
    e.designation                         AS current_designation,
    e.employment_type                     AS current_emp_type,
    e.salary                              AS current_salary,
    e.status                              AS current_status,
    e.join_date,

    h.hr_update_id,
    h.update_type,
    h.effective_date,
    h.old_designation,
    h.new_designation,
    od.dept_name                          AS old_dept_name,
    nd.dept_name                          AS new_dept_name,
    h.old_salary,
    h.new_salary,
    h.old_emp_type,
    h.new_emp_type,
    h.reason,
    auth.full_name                        AS authorised_by,
    h.recorded_at

FROM employee e
LEFT JOIN employee_hr_update h  ON h.emp_id        = e.emp_id
LEFT JOIN department         od ON od.dept_id       = h.old_dept_id
LEFT JOIN department         nd ON nd.dept_id       = h.new_dept_id
LEFT JOIN employee           auth ON auth.emp_id    = h.authorized_by

ORDER BY e.emp_id, h.effective_date, h.hr_update_id;

GRANT SELECT ON view_employee_hr_record TO bank_employee, bank_manager;

-- ----------------------------------------------------------
-- VIEW: Manager Dashboard
-- Purpose  : Branch-level operational overview — staffing,
--            customer counts, deposit book, loan book,
--            and pending application pipeline.
--            Full superuser access; no masking.
-- Access   : bank_manager only
-- Usage    :
--   SELECT * FROM view_manager_dashboard;
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_manager_dashboard CASCADE;

CREATE VIEW view_manager_dashboard AS
SELECT
    b.branch_id,
    b.branch_name,
    b.city,
    b.state,
    b.ifsc_code,
    b.status                              AS branch_status,

    -- Staffing
    staff.total_staff,
    staff.permanent_staff,
    staff.contract_staff,
    staff.on_probation,
    staff.staff_details,                   -- JSONB

    -- Customer & Account book
    cust.total_customers,
    cust.active_customers,
    cust.dormant_blocked_customers,
    cust.kyc_pending_count,

    acct.total_accounts,
    acct.active_accounts,
    acct.total_deposit_book,

    -- Loan book
    lbook.active_loans,
    lbook.total_outstanding,
    lbook.npa_loans,

    -- Application pipeline
    pipeline.submitted_count,
    pipeline.under_review_count,
    pipeline.approved_count,
    pipeline.rejected_count

FROM branch b

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)                                                       AS total_staff,
        COUNT(*) FILTER (WHERE employment_type = 'permanent')         AS permanent_staff,
        COUNT(*) FILTER (WHERE employment_type = 'contract')          AS contract_staff,
        COUNT(*) FILTER (WHERE employment_type = 'probation')         AS on_probation,
        jsonb_agg(jsonb_build_object(
            'emp_id',         e.emp_id,
            'name',           e.full_name,
            'designation',    e.designation,
            'employment_type',e.employment_type,
            'salary',         e.salary,
            'dept_id',        e.dept_id,
            'status',         e.status
        ) ORDER BY e.emp_id)                                          AS staff_details
    FROM employee e
    WHERE e.branch_id = b.branch_id AND e.status = 'active'
) staff ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)                                                       AS total_customers,
        COUNT(*) FILTER (WHERE status = 'active')                     AS active_customers,
        COUNT(*) FILTER (WHERE status IN ('dormant','blocked'))       AS dormant_blocked_customers,
        COUNT(*) FILTER (WHERE kyc_status IN ('pending','expired'))   AS kyc_pending_count
    FROM customer c
    WHERE c.branch_id = b.branch_id
) cust ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*)                                                       AS total_accounts,
        COUNT(*) FILTER (WHERE a.status = 'active')                   AS active_accounts,
        COALESCE(SUM(a.current_balance), 0)                           AS total_deposit_book
    FROM account a
    WHERE a.branch_id = b.branch_id
) acct ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE l.status = 'active')                   AS active_loans,
        COALESCE(SUM(l.outstanding_principal) FILTER (WHERE l.status = 'active'), 0) AS total_outstanding,
        COUNT(*) FILTER (WHERE l.status = 'npa')                      AS npa_loans
    FROM loan l
    JOIN customer c ON c.customer_id = l.customer_id
    WHERE c.branch_id = b.branch_id
) lbook ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE la.status = 'submitted')               AS submitted_count,
        COUNT(*) FILTER (WHERE la.status = 'under_review')            AS under_review_count,
        COUNT(*) FILTER (WHERE la.status = 'approved')                AS approved_count,
        COUNT(*) FILTER (WHERE la.status = 'rejected')                AS rejected_count
    FROM loan_application la
    JOIN customer c ON c.customer_id = la.customer_id
    WHERE c.branch_id = b.branch_id
) pipeline ON TRUE;

GRANT SELECT ON view_manager_dashboard TO bank_manager;

-- ----------------------------------------------------------
-- VIEW: Loan Pipeline (employee + manager)
-- Purpose  : All loan applications with customer, officer, and
--            age-of-application detail.
--            Employees see only applications assigned to them
--            (RLS on loan_application enforces this).
-- Access   : bank_employee, bank_manager
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_loan_pipeline CASCADE;

CREATE VIEW view_loan_pipeline AS
SELECT
    la.application_id,
    la.application_date,
    la.status,
    la.requested_amount,
    la.purpose,
    la.decision_notes,
    la.reviewed_at,

    c.customer_id,
    c.full_name                           AS customer_name,
    c.phone                               AS customer_phone,
    c.kyc_status,

    e.emp_id                              AS officer_id,
    e.full_name                           AS officer_name,
    e.designation                         AS officer_designation,

    EXTRACT(DAY FROM NOW() - la.application_date)::INT AS days_open,

    CASE
        WHEN la.status = 'submitted'     THEN '🟡 Awaiting review'
        WHEN la.status = 'under_review'  THEN '🔵 In review'
        WHEN la.status = 'approved'      THEN '🟢 Approved'
        WHEN la.status = 'rejected'      THEN '🔴 Rejected'
    END                                   AS status_label

FROM loan_application la
JOIN customer  c ON c.customer_id  = la.customer_id
LEFT JOIN employee e ON e.emp_id   = la.assigned_emp_id
ORDER BY la.application_date DESC;

GRANT SELECT ON view_loan_pipeline TO bank_employee, bank_manager;

-- ----------------------------------------------------------
-- VIEW: Account Ledger
-- Purpose  : Full transaction history with debit/credit labels.
--            Customers see only their own account transactions
--            (RLS on transaction table enforces this).
-- Access   : bank_customer, bank_employee, bank_manager
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_account_ledger CASCADE;

CREATE VIEW view_account_ledger AS
SELECT
    t.txn_id,
    t.txn_date,
    t.account_id,
    a.account_number,
    a.account_type,
    c.customer_id,
    c.full_name                           AS customer_name,
    t.txn_type,
    CASE WHEN t.txn_type = 'credit' THEN 'CR' ELSE 'DR' END AS dr_cr,
    t.channel,
    t.amount,
    t.balance_after,
    t.reference_number,
    COALESCE(t.description, '—')          AS description
FROM transaction t
JOIN account  a ON a.account_id  = t.account_id
JOIN customer c ON c.customer_id = a.customer_id
ORDER BY t.txn_date DESC;

GRANT SELECT ON view_account_ledger TO bank_customer, bank_employee, bank_manager;

-- ----------------------------------------------------------
-- VIEW: Full Employee Directory (manager-only)
-- Purpose  : Complete staff list with department, salary,
--            and HR update counts. No restrictions.
-- Access   : bank_manager only
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_employee_directory CASCADE;

CREATE VIEW view_employee_directory AS
SELECT
    e.emp_id,
    e.full_name,
    e.designation,
    e.employment_type,
    e.salary,
    e.join_date,
    e.status,
    d.dept_name,
    b.branch_name,
    mgr.full_name                         AS manager_name,
    hr_count.total_hr_events,
    hr_count.last_hr_event_type,
    hr_count.last_hr_event_date
FROM employee e
JOIN branch      b   ON b.branch_id   = e.branch_id
LEFT JOIN department d   ON d.dept_id     = e.dept_id
LEFT JOIN employee   mgr ON mgr.emp_id    = e.manager_id
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)                      AS total_hr_events,
        (SELECT update_type FROM employee_hr_update h2
         WHERE h2.emp_id = e.emp_id
         ORDER BY h2.effective_date DESC LIMIT 1) AS last_hr_event_type,
        MAX(h.effective_date)         AS last_hr_event_date
    FROM employee_hr_update h
    WHERE h.emp_id = e.emp_id
) hr_count ON TRUE
ORDER BY e.emp_id;

GRANT SELECT ON view_employee_directory TO bank_manager;

-- ----------------------------------------------------------
-- VIEW: HR Events Log (manager-only)
-- Purpose  : All HR update events across all employees with
--            full before/after detail — complete audit trail.
-- Access   : bank_manager only
-- ----------------------------------------------------------
DROP VIEW IF EXISTS view_hr_audit_log CASCADE;

CREATE VIEW view_hr_audit_log AS
SELECT
    h.hr_update_id,
    h.effective_date,
    h.recorded_at,
    h.update_type,

    emp.emp_id,
    emp.full_name                         AS employee_name,
    emp.designation                       AS current_designation,

    h.old_designation,
    h.new_designation,
    od.dept_name                          AS old_dept,
    nd.dept_name                          AS new_dept,
    h.old_salary,
    h.new_salary,
    h.old_emp_type,
    h.new_emp_type,

    h.reason,
    auth.full_name                        AS authorised_by,
    auth.designation                      AS authoriser_role,

    -- Salary delta
    CASE
        WHEN h.new_salary IS NOT NULL AND h.old_salary IS NOT NULL
        THEN h.new_salary - h.old_salary
    END                                   AS salary_delta,

    CASE
        WHEN h.new_salary IS NOT NULL AND h.old_salary IS NOT NULL AND h.old_salary > 0
        THEN ROUND(((h.new_salary - h.old_salary) / h.old_salary * 100)::NUMERIC, 2)
    END                                   AS salary_change_pct

FROM employee_hr_update h
JOIN employee emp  ON emp.emp_id  = h.emp_id
JOIN employee auth ON auth.emp_id = h.authorized_by
LEFT JOIN department od ON od.dept_id = h.old_dept_id
LEFT JOIN department nd ON nd.dept_id = h.new_dept_id
ORDER BY h.effective_date DESC, h.hr_update_id DESC;

GRANT SELECT ON view_hr_audit_log TO bank_manager;

-- ============================================================
-- PART 5: USAGE EXAMPLES
-- ============================================================
DO $$ BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '  ROLE-BASED ACCESS — USAGE EXAMPLES';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE '-- CUSTOMER (sees only their own data)';
    RAISE NOTICE 'SET app.current_user_id = ''2'';';
    RAISE NOTICE 'SELECT customer_id, full_name, total_active_balance,';
    RAISE NOTICE '       active_loan_count, pending_application_count';
    RAISE NOTICE 'FROM view_customer_portal;';
    RAISE NOTICE '';
    RAISE NOTICE 'SELECT * FROM view_account_ledger;    -- own txns only';
    RAISE NOTICE 'SELECT bank_apply_loan(2, 50000, ''Education'');';
    RAISE NOTICE '';
    RAISE NOTICE '-- EMPLOYEE (sees assigned customers + own HR)';
    RAISE NOTICE 'SET app.current_employee_id = ''3'';';
    RAISE NOTICE 'SELECT customer_name, application_id, application_status,';
    RAISE NOTICE '       days_pending FROM view_employee_workbench;';
    RAISE NOTICE '';
    RAISE NOTICE 'SELECT * FROM view_loan_pipeline;     -- assigned apps only';
    RAISE NOTICE 'SELECT * FROM view_employee_hr_record;-- own HR trail only';
    RAISE NOTICE 'SELECT * FROM bank_emp_hr_history(3); -- own HR events';
    RAISE NOTICE 'SELECT bank_review_loan(5, 3, ''under_review'', ''Docs received'');';
    RAISE NOTICE '';
    RAISE NOTICE '-- MANAGER (full branch access)';
    RAISE NOTICE 'SELECT * FROM view_manager_dashboard;';
    RAISE NOTICE 'SELECT * FROM view_employee_directory;';
    RAISE NOTICE 'SELECT * FROM view_hr_audit_log;';
    RAISE NOTICE '';
    RAISE NOTICE '-- HR Updates (manager only)';
    RAISE NOTICE 'SELECT bank_hr_update(6, 1, ''salary_revision'', CURRENT_DATE,';
    RAISE NOTICE '    ''FY2025 increment'', NULL, NULL, 45000, NULL);';
    RAISE NOTICE 'SELECT bank_hr_update(5, 1, ''promotion'', CURRENT_DATE,';
    RAISE NOTICE '    ''Exceeds targets FY25'', ''Senior Relationship Manager'', NULL, 52000, NULL);';
    RAISE NOTICE 'SELECT bank_hr_update(3, 1, ''employment_type_change'', CURRENT_DATE,';
    RAISE NOTICE '    ''Probation complete'', NULL, NULL, 57000, ''permanent'');';
    RAISE NOTICE 'SELECT bank_hr_update(4, 1, ''department_transfer'', CURRENT_DATE,';
    RAISE NOTICE '    ''Branch restructuring'', NULL, 2, NULL, NULL);';
    RAISE NOTICE '';
    RAISE NOTICE '-- Grant an actual DB login user one of these roles:';
    RAISE NOTICE '-- CREATE USER arjun_pillai WITH PASSWORD ''secret'';';
    RAISE NOTICE '-- GRANT bank_customer TO arjun_pillai;';
    RAISE NOTICE '-- CREATE USER arun_kumar WITH PASSWORD ''secret'';';
    RAISE NOTICE '-- GRANT bank_employee TO arun_kumar;';
    RAISE NOTICE '-- CREATE USER rajesh_nair WITH PASSWORD ''secret'';';
    RAISE NOTICE '-- GRANT bank_manager TO rajesh_nair;';
    RAISE NOTICE '============================================================';
END $$;

SELECT '✅ Role-based views + RLS policies loaded: bank_customer / bank_employee / bank_manager' AS status;