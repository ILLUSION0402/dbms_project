-- ============================================================
--  RETAIL BANKING BRANCH - Final Schema (9 Tables)
--  Optimised for Git-Like VCS System | PostgreSQL 13+
-- ============================================================

DROP TABLE IF EXISTS fund_transfer       CASCADE;
DROP TABLE IF EXISTS transaction         CASCADE;
DROP TABLE IF EXISTS loan                CASCADE;
DROP TABLE IF EXISTS account             CASCADE;
DROP TABLE IF EXISTS customer            CASCADE;
DROP TABLE IF EXISTS employee_hr_update  CASCADE;
DROP TABLE IF EXISTS employee            CASCADE;
DROP TABLE IF EXISTS department          CASCADE;
DROP TABLE IF EXISTS branch              CASCADE;

-- ============================================================
-- 1. BRANCH
-- ============================================================
CREATE TABLE branch (
    branch_id        SERIAL PRIMARY KEY,
    branch_name      VARCHAR(100)  NOT NULL,
    ifsc_code        VARCHAR(11)   NOT NULL UNIQUE,
    address          TEXT          NOT NULL,
    city             VARCHAR(60)   NOT NULL,
    state            VARCHAR(60)   NOT NULL,
    pincode          CHAR(6)       NOT NULL,
    phone            VARCHAR(15),
    email            VARCHAR(100),
    established_date DATE          NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','inactive','closed'))
);

-- ============================================================
-- 2. DEPARTMENT
-- ============================================================
CREATE TABLE department (
    dept_id      SERIAL PRIMARY KEY,
    branch_id    INT          NOT NULL REFERENCES branch(branch_id),
    dept_name    VARCHAR(80)  NOT NULL,
    dept_head_id INT          -- FK to employee, set after insert
);


-- ============================================================
-- 3. EMPLOYEE
-- ============================================================
CREATE TABLE employee (
    emp_id           SERIAL PRIMARY KEY,
    branch_id        INT           NOT NULL REFERENCES branch(branch_id),
    dept_id          INT           REFERENCES department(dept_id),
    manager_id       INT           REFERENCES employee(emp_id),
    full_name        VARCHAR(100)  NOT NULL,
    designation      VARCHAR(80)   NOT NULL,
    employment_type  VARCHAR(20)   NOT NULL DEFAULT 'permanent'
                     CHECK (employment_type IN ('permanent','contract','probation')),
    join_date        DATE          NOT NULL,
    salary           NUMERIC(12,2) NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','resigned','terminated'))
);

ALTER TABLE department
    ADD CONSTRAINT fk_dept_head FOREIGN KEY (dept_head_id) REFERENCES employee(emp_id);

-- ============================================================
-- 4. EMPLOYEE HR UPDATE
-- ============================================================
CREATE TABLE employee_hr_update (
    hr_update_id     SERIAL PRIMARY KEY,
    emp_id           INT           NOT NULL REFERENCES employee(emp_id),
    update_type      VARCHAR(30)   NOT NULL
                     CHECK (update_type IN ('salary_revision','promotion','demotion',
                         'department_transfer','designation_change',
                         'employment_type_change','termination','reinstatement')),
    effective_date   DATE          NOT NULL,
    old_designation  VARCHAR(80),
    old_dept_id      INT           REFERENCES department(dept_id),
    old_salary       NUMERIC(12,2),
    old_emp_type     VARCHAR(20),
    new_designation  VARCHAR(80),
    new_dept_id      INT           REFERENCES department(dept_id),
    new_salary       NUMERIC(12,2),
    new_emp_type     VARCHAR(20),
    reason           TEXT          NOT NULL,
    authorized_by    INT           NOT NULL REFERENCES employee(emp_id),
    recorded_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_hr_emp ON employee_hr_update(emp_id);

-- ============================================================
-- 5. CUSTOMER  (identity merged in)
-- ============================================================
CREATE TABLE customer (
    customer_id      SERIAL PRIMARY KEY,
    branch_id        INT           NOT NULL REFERENCES branch(branch_id),
    assigned_rm_id   INT           REFERENCES employee(emp_id),
    full_name        VARCHAR(100)  NOT NULL,
    dob              DATE          NOT NULL,
    gender           CHAR(1)       CHECK (gender IN ('M','F','O')),
    phone            VARCHAR(15)   NOT NULL,
    email            VARCHAR(100),
    occupation       VARCHAR(80),
    income_bracket   VARCHAR(20)   CHECK (income_bracket IN
                         ('below_2L','2L_5L','5L_10L','10L_25L','above_25L')),
    -- Identity (merged from customer_identity)
    aadhaar_number   CHAR(12)      UNIQUE,
    pan_number       CHAR(10)      UNIQUE,
    kyc_status       VARCHAR(20)   NOT NULL DEFAULT 'pending'
                     CHECK (kyc_status IN ('pending','verified','expired','rejected')),
    kyc_verified_by  INT           REFERENCES employee(emp_id),
    kyc_verified_on  DATE,
    -- Meta
    customer_since   DATE          NOT NULL DEFAULT CURRENT_DATE,
    status           VARCHAR(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','dormant','closed','blocked'))
);


-- ============================================================
-- 5. ACCOUNT  (account_type inlined)
-- ============================================================
CREATE TABLE account (
    account_id       SERIAL PRIMARY KEY,
    customer_id      INT           NOT NULL REFERENCES customer(customer_id),
    branch_id        INT           NOT NULL REFERENCES branch(branch_id),
    opened_by        INT           REFERENCES employee(emp_id),
    account_number   VARCHAR(20)   NOT NULL UNIQUE,
    -- Type inlined (replaces account_type table)
    account_type     VARCHAR(20)   NOT NULL
                     CHECK (account_type IN ('savings','current','salary','nre','nro')),
    min_balance      NUMERIC(12,2) NOT NULL DEFAULT 0,
    interest_rate    NUMERIC(5,2)  NOT NULL DEFAULT 0,
    -- State
    opened_date      DATE          NOT NULL DEFAULT CURRENT_DATE,
    current_balance  NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    status           VARCHAR(20)   NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active','frozen','dormant','closed'))
);

-- ============================================================
-- 6. TRANSACTION
-- ============================================================
CREATE TABLE transaction (
    txn_id           SERIAL PRIMARY KEY,
    account_id       INT           NOT NULL REFERENCES account(account_id),
    txn_type         VARCHAR(10)   NOT NULL CHECK (txn_type IN ('credit','debit')),
    channel          VARCHAR(20)   NOT NULL
                     CHECK (channel IN ('branch','atm','upi','neft','rtgs','imps','system')),
    amount           NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    balance_after    NUMERIC(15,2) NOT NULL,
    reference_number VARCHAR(40)   NOT NULL UNIQUE,
    txn_date         TIMESTAMP     NOT NULL DEFAULT NOW(),
    description      TEXT,
    initiated_by     INT           REFERENCES employee(emp_id)
);


-- ============================================================
-- 7. FUND TRANSFER
-- ============================================================
CREATE TABLE fund_transfer (
    transfer_id       SERIAL PRIMARY KEY,
    from_account_id   INT           NOT NULL REFERENCES account(account_id),
    to_account_id     INT           REFERENCES account(account_id),
    to_ifsc           VARCHAR(11),
    to_account_number VARCHAR(20),
    transfer_mode     VARCHAR(10)   NOT NULL
                      CHECK (transfer_mode IN ('neft','rtgs','imps','upi','internal')),
    amount            NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    status            VARCHAR(20)   NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','processing','completed','failed','reversed')),
    initiated_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    settled_at        TIMESTAMP,
    remarks           TEXT
);


-- ============================================================
-- 8. LOAN  (loan_product + loan_collateral )
-- ============================================================
CREATE TABLE loan (
    loan_id              SERIAL PRIMARY KEY,
    customer_id          INT           NOT NULL REFERENCES customer(customer_id),
    account_id           INT           NOT NULL REFERENCES account(account_id),
    assigned_officer     INT           REFERENCES employee(emp_id),
    loan_type            VARCHAR(30)   NOT NULL CHECK (loan_type IN ('home','personal','vehicle','education','gold')),
    base_interest_rate   NUMERIC(5,2)  NOT NULL,
    processing_fee_pct   NUMERIC(5,2)  NOT NULL DEFAULT 0.50,
    applied_amount       NUMERIC(15,2) NOT NULL,
    purpose              TEXT,
    application_date     DATE          NOT NULL DEFAULT CURRENT_DATE,
    application_status   VARCHAR(20)   NOT NULL DEFAULT 'submitted' CHECK (application_status IN
	('submitted','under_review','approved','rejected','disbursed','withdrawn')),
    rejection_reason     TEXT,
    -- Disbursement details
    sanctioned_amount    NUMERIC(15,2),
    disbursed_amount     NUMERIC(15,2),
    interest_rate        NUMERIC(5,2),
    tenure_months        INT,
    emi_amount           NUMERIC(12,2),
    disbursement_date    DATE,
    maturity_date        DATE,
    outstanding_principal NUMERIC(15,2),
    -- Collateral (merged from loan_collateral)
    collateral_type      VARCHAR(20) CHECK (collateral_type IN ('property','gold','vehicle','fd','none')),
    collateral_desc      TEXT,
    collateral_value     NUMERIC(15,2),
    collateral_valued_on DATE,
    status               VARCHAR(20)   NOT NULL DEFAULT 'pending'CHECK (status IN
	('pending','active','closed','npa','written_off','foreclosed')) );
	select * from loan;

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX idx_customer_branch    ON customer(branch_id);
CREATE INDEX idx_account_customer   ON account(customer_id);
CREATE INDEX idx_txn_account        ON transaction(account_id);
CREATE INDEX idx_txn_date           ON transaction(txn_date);
CREATE INDEX idx_loan_customer      ON loan(customer_id);
CREATE INDEX idx_loan_status        ON loan(status);
CREATE INDEX idx_transfer_from      ON fund_transfer(from_account_id);

-- ============================================================
-- SEED DATA
-- ============================================================

-- Branch
INSERT INTO branch VALUES (
    1, 'Kottayam Main Branch', 'SBIN0001234',
    '15 Baker Junction, MG Road', 'Kottayam', 'Kerala', '686001',
    '0481-2301234', 'kottayam.main@sbi.co.in', '2005-04-01', 'active'
);

-- Departments
INSERT INTO department (branch_id, dept_name) VALUES
    (1, 'Retail Banking'),
    (1, 'Loans & Credit'),
    (1, 'Operations');

-- Employees
INSERT INTO employee (branch_id,dept_id,manager_id,full_name,designation,employment_type,join_date,salary,status) VALUES
(1,1,NULL, 'Rajesh Nair',     'Branch Manager',              'permanent','2010-06-01',120000,'active'),
(1,1,1,    'Priya Menon',     'Senior Relationship Manager', 'permanent','2015-03-15', 65000,'active'),
(1,2,1,    'Arun Kumar',      'Loan Officer',                'permanent','2018-07-01', 55000,'active'),
(1,3,1,    'Suresh Pillai',   'Operations Executive',        'permanent','2020-01-10', 40000,'active'),
(1,1,2,    'Anitha Varghese', 'Relationship Manager',        'permanent','2021-05-05', 45000,'active'),
(1,2,3,    'Manoj Krishnan',  'Credit Analyst',              'contract', '2022-08-15', 38000,'active');

-- Update dept heads
UPDATE department SET dept_head_id = 1 WHERE dept_id = 1;
UPDATE department SET dept_head_id = 3 WHERE dept_id = 2;
UPDATE department SET dept_head_id = 4 WHERE dept_id = 3;

-- Customers (identity merged in)
INSERT INTO customer (branch_id,assigned_rm_id,full_name,dob,gender,phone,email,
    occupation,income_bracket,aadhaar_number,pan_number,
    kyc_status,kyc_verified_by,kyc_verified_on,customer_since,status) VALUES
(1,2,'Arjun Pillai',  '1985-03-22','M','9876543210','arjun.pillai@email.com',
    'Software Engineer','10L_25L','123456781001','ABCPK1234D',
    'verified',4,'2015-06-10','2015-06-10','active'),
(1,2,'Meena Suresh',  '1990-07-14','F','9876543211','meena.s@email.com',
    'Teacher','2L_5L','123456781002',NULL,
    'verified',4,'2017-09-01','2017-09-01','active'),
(1,5,'Thomas George', '1978-11-30','M','9876543212','thomas.g@business.com',
    'Business Owner','above_25L','123456781003','XYZGT5678F',
    'verified',4,'2012-03-15','2012-03-15','active'),
(1,5,'Lakshmi Nair',  '1995-01-08','F','9876543213','lakshmi.nair@hospital.com',
    'Doctor','10L_25L','123456781004',NULL,
    'verified',4,'2020-11-20','2020-11-20','active'),
(1,2,'Rajan Varma',   '1965-05-19','M','9876543214','rajan.v@email.com',
    'Retired','2L_5L','123456781005',NULL,
    'expired',4,'2008-04-05','2008-04-05','dormant'),
(1,5,'Sreeja Mohan',  '1988-12-03','F','9876543215','sreeja.m@email.com',
    'Homemaker','below_2L','123456781006',NULL,
    'verified',4,'2019-07-22','2019-07-22','active');

-- Accounts (type inlined)
INSERT INTO account (customer_id,branch_id,opened_by,account_number,
    account_type,min_balance,interest_rate,opened_date,current_balance,status) VALUES
(1,1,2,'SB10000000001','savings',  1000,3.50,'2015-06-10', 85000.00,'active'),
(2,1,2,'SB10000000002','savings',  1000,3.50,'2017-09-01', 22500.00,'active'),
(3,1,2,'CA10000000001','current', 10000,0.00,'2012-03-15',450000.00,'active'),
(4,1,5,'SB10000000003','savings',  1000,3.50,'2020-11-20',135000.00,'active'),
(5,1,2,'SB10000000004','savings',  1000,3.50,'2008-04-05',  5200.00,'dormant'),
(6,1,5,'SB10000000005','salary',      0,3.50,'2019-07-22', 18000.00,'active');

-- Transactions
INSERT INTO transaction (account_id,txn_type,channel,amount,balance_after,
    reference_number,txn_date,description,initiated_by) VALUES
(1,'credit','neft',  50000, 85000,'REF20240115001','2024-01-15 10:30:00','Salary credit',NULL),
(1,'debit', 'upi',    2500, 82500,'REF20240115002','2024-01-15 14:20:00','UPI - Grocery',NULL),
(2,'credit','branch',10000, 22500,'REF20240115003','2024-01-15 11:00:00','Cash deposit',4),
(3,'debit', 'rtgs', 200000,450000,'REF20240115004','2024-01-15 09:45:00','Vendor payment',4),
(4,'credit','neft', 135000,135000,'REF20240115005','2024-01-15 12:00:00','Salary credit',NULL),
(1,'debit', 'neft',  15000, 70000,'REF20240116001','2024-01-16 09:00:00','Loan EMI',NULL),
(2,'debit', 'atm',    5000, 17500,'REF20240116002','2024-01-16 10:30:00','ATM withdrawal',NULL),
(6,'credit','neft',  18000, 18000,'REF20240116003','2024-01-16 08:00:00','Salary credit',NULL);

-- Fund transfers
INSERT INTO fund_transfer (from_account_id,to_account_id,to_ifsc,to_account_number,
    transfer_mode,amount,status,initiated_at,settled_at,remarks) VALUES
(1,4,   NULL, NULL,           'internal',  5000,'completed','2024-01-10 10:00:00','2024-01-10 10:00:01','Internal transfer'),
(3,NULL,'HDFC0001234','HDFC1234567890','rtgs',200000,'completed','2024-01-15 09:45:00','2024-01-15 11:00:00','Vendor payment'),
(1,NULL,'ICIC0005678','ICIC9876543210','neft', 20000,'completed','2024-01-14 15:00:00','2024-01-14 17:30:00','Investment transfer');

-- Loans (product + collateral merged in)
INSERT INTO loan (customer_id,account_id,assigned_officer,
    loan_type,base_interest_rate,processing_fee_pct,
    applied_amount,purpose,application_date,application_status,
    sanctioned_amount,disbursed_amount,interest_rate,tenure_months,
    emi_amount,disbursement_date,maturity_date,outstanding_principal,
    collateral_type,collateral_desc,collateral_value,collateral_valued_on,status) VALUES
(1,1,3,
 'home',8.50,0.50,
 4500000,'Purchase of residential flat','2023-05-10','disbursed',
 4500000,4500000,8.50,240,
 39204,'2023-06-01','2043-06-01',4388000,
 'property','3BHK Apartment, Nagampadam, Kottayam - 1200 sqft',5500000,'2023-05-20','active'),

(4,4,3,
 'vehicle',9.25,0.75,
 900000,'Purchase of car','2023-08-15','disbursed',
 900000,900000,9.25,60,
 18735,'2023-09-01','2028-09-01',820000,
 'vehicle','2023 Maruti Swift Dzire VXI - KL-05-AB-1234',750000,'2023-08-20','active'),

(2,2,3,
 'personal',12.00,1.00,
 300000,'Medical expenses','2024-01-05','approved',
 NULL,NULL,NULL,NULL,
 NULL,NULL,NULL,NULL,
 'none',NULL,NULL,NULL,'pending'),

(6,6,6,
 'personal',12.00,1.00,
 500000,'Home renovation','2024-01-12','under_review',
 NULL,NULL,NULL,NULL,
 NULL,NULL,NULL,NULL,
 'none',NULL,NULL,NULL,'pending');

-- ============================================================
-- SUMMARY VIEW
-- ============================================================
CREATE OR REPLACE VIEW branch_summary AS
SELECT
    b.branch_name,
    COUNT(DISTINCT c.customer_id)       AS total_customers,
    COUNT(DISTINCT a.account_id)        AS total_accounts,
    COALESCE(SUM(a.current_balance),0)  AS total_deposits,
    COUNT(DISTINCT l.loan_id)
        FILTER (WHERE l.status = 'active') AS active_loans,
    COALESCE(SUM(l.outstanding_principal)
        FILTER (WHERE l.status = 'active'),0) AS total_loan_book,
    COUNT(DISTINCT e.emp_id)            AS total_staff
FROM branch b
LEFT JOIN customer c ON c.branch_id = b.branch_id
LEFT JOIN account  a ON a.customer_id = c.customer_id AND a.status = 'active'
LEFT JOIN loan     l ON l.customer_id = c.customer_id
LEFT JOIN employee e ON e.branch_id = b.branch_id AND e.status = 'active'
GROUP BY b.branch_name;

select'================================================================';
 select ' Retail Banking Branch — 9-table schema loaded successfully.';
select  ' Tables: branch, department, employee, employee_hr_update, customer,';
select '    account, transaction, fund_transfer, loan';
select 	' Run: SELECT * FROM branch_summary;';
select '================================================================';

-- HR Updates seed data
INSERT INTO employee_hr_update
    (emp_id, update_type, effective_date,
     old_designation, old_dept_id, old_salary, old_emp_type,
     new_designation, new_dept_id, new_salary, new_emp_type,
     reason, authorized_by) VALUES
-- Priya promoted from RM to Senior RM
(2, 'promotion',       '2020-04-01',
 'Relationship Manager',       1, 52000, 'permanent',
 'Senior Relationship Manager',1, 65000, 'permanent',
 'Exceptional performance in FY2019-20; exceeded targets by 32%', 1),

-- Arun got annual salary revision
(3, 'salary_revision', '2023-04-01',
 'Loan Officer', 2, 50000, 'permanent',
 'Loan Officer', 2, 55000, 'permanent',
 'Annual increment — FY2023-24 appraisal cycle', 1),

-- Manoj converted from contract to permanent
(6, 'employment_type_change', '2024-01-01',
 'Credit Analyst', 2, 38000, 'contract',
 'Credit Analyst', 2, 42000, 'permanent',
 'Completed probation period; performance rating: excellent', 1),

-- Suresh transferred from Operations to Retail Banking
(4, 'department_transfer', '2023-10-01',
 'Operations Executive', 3, 40000, 'permanent',
 'Operations Executive', 1, 40000, 'permanent',
 'Branch restructuring — Retail Banking team expansion', 1),

-- Anitha designation change
(5, 'designation_change', '2024-01-15',
 'Relationship Manager',        1, 45000, 'permanent',
 'Senior Relationship Manager', 1, 45000, 'permanent',
 'Salary revision pending approval; designation effective immediately', 1);
