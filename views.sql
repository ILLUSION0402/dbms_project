-- ============================================================
-- ROLE-BASED AND REPORTING VIEWS
-- ============================================================
-- Compatible with retail_banking_setup_final.sql + banking_layer.sql
-- ============================================================
DROP VIEW IF EXISTS view_customer_portal CASCADE;

CREATE VIEW view_customer_portal AS
SELECT
    c.customer_id,
    c.full_name,
    c.phone,
    c.email,
    c.occupation,
    c.kyc_status,
    c.status AS customer_status,

    b.branch_id,
    b.branch_name,
    b.city,

    acct.active_account_count,
    acct.total_active_balance,

    loans.open_loan_count,
    loans.outstanding_loan_amount,

    apps.pending_application_count,
    apps.latest_application_date

FROM customer c
JOIN branch b ON b.branch_id = c.branch_id

LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE a.status = 'active') AS active_account_count,
        COALESCE(SUM(a.current_balance),0) AS total_active_balance
    FROM account a
    WHERE a.customer_id = c.customer_id
) acct ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE l.status = 'active') AS open_loan_count,
        COALESCE(SUM(l.outstanding_principal),0) AS outstanding_loan_amount
    FROM loan l
    WHERE l.customer_id = c.customer_id
) loans ON TRUE

LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE la.status IN ('submitted','under_review')) AS pending_application_count,
        MAX(la.application_date) AS latest_application_date
    FROM loan_application la
    WHERE la.customer_id = c.customer_id
) apps ON TRUE;
DROP VIEW IF EXISTS view_employee_workbench CASCADE;

CREATE VIEW view_employee_workbench AS
SELECT
    e.emp_id,
    e.full_name AS employee_name,
    e.designation,
    e.branch_id,
    b.branch_name,

    la.application_id,
    la.status,
    la.application_date,
    la.reviewed_at,
    la.requested_amount,
    la.decision_notes,

    c.customer_id,
    c.full_name AS customer_name,
    c.phone,
    c.occupation,
    c.kyc_status

FROM employee e
JOIN branch b ON b.branch_id = e.branch_id

LEFT JOIN loan_application la
    ON la.assigned_emp_id = e.emp_id

LEFT JOIN customer c
    ON c.customer_id = la.customer_id

WHERE e.status = 'active';

DROP VIEW IF EXISTS view_manager_dashboard CASCADE;

CREATE VIEW view_manager_dashboard AS
SELECT
    b.branch_id,
    b.branch_name,
    b.city,
    b.status,

    (SELECT COUNT(*) FROM employee e WHERE e.branch_id=b.branch_id AND e.status='active') AS active_staff_count,

    (SELECT COUNT(*) FROM customer c WHERE c.branch_id=b.branch_id) AS customer_count,

    (SELECT COUNT(*) FROM account a WHERE a.branch_id=b.branch_id AND a.status='active') AS active_accounts,

    (SELECT COALESCE(SUM(a.current_balance),0) FROM account a WHERE a.branch_id=b.branch_id) AS total_deposits,

    (SELECT COUNT(*) FROM loan l WHERE l.customer_id IN
        (SELECT customer_id FROM customer WHERE branch_id=b.branch_id)
    ) AS total_loans

FROM branch b;
DROP VIEW IF EXISTS view_loan_pipeline CASCADE;

CREATE VIEW view_loan_pipeline AS
SELECT
    la.application_id,
    la.application_date,
    la.status,
    la.requested_amount,
    la.purpose,

    c.customer_id,
    c.full_name,

    e.emp_id,
    e.full_name AS officer_name,

    EXTRACT(DAY FROM NOW() - la.application_date)::INT AS days_open

FROM loan_application la
JOIN customer c ON c.customer_id = la.customer_id
LEFT JOIN employee e ON e.emp_id = la.assigned_emp_id;
DROP VIEW IF EXISTS view_account_ledger CASCADE;

CREATE VIEW view_account_ledger AS
SELECT
    t.txn_id,
    t.txn_date,
    t.account_id,
    a.account_type,
    c.customer_id,
    c.full_name,

    t.txn_type,

    CASE
        WHEN t.txn_type = 'credit' THEN 'CR'
        ELSE 'DR'
    END AS dr_cr,

    t.amount,
    t.balance_after

FROM transaction t
JOIN account a ON a.account_id = t.account_id
JOIN customer c ON c.customer_id = a.customer_id
ORDER BY t.txn_date DESC;