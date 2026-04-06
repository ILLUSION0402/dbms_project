import express from 'express';
import cors from 'cors';
import pg from 'pg';

const { Pool } = pg;

const pool = new Pool({
  user: process.env.PGUSER || 'postgres',
  host: process.env.PGHOST || 'localhost',
  database: process.env.PGDATABASE || 'bank_versioning',
  password: process.env.PGPASSWORD || 'postgres',
  port: parseInt(process.env.PGPORT || '5432'),
});

const app = express();
app.use(cors());
app.use(express.json());

// ── Helper ──
async function query(text, params) {
  const res = await pool.query(text, params);
  return res.rows;
}

// ════════════════════════════════════════════════════════════
//  BANKING ENDPOINTS
// ════════════════════════════════════════════════════════════

app.get('/api/branches', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM branch ORDER BY branch_id');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/departments', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT d.*, e.full_name AS head_name
      FROM department d
      LEFT JOIN employee e ON d.dept_head_id = e.emp_id
      ORDER BY d.dept_id`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employees', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_employee_directory ORDER BY emp_id');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customers', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT c.*, b.branch_name, e.full_name AS rm_name
      FROM customer c
      LEFT JOIN branch b ON c.branch_id = b.branch_id
      LEFT JOIN employee e ON c.assigned_rm_id = e.emp_id
      ORDER BY c.customer_id`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/accounts', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT a.*, c.full_name AS customer_name, b.branch_name
      FROM account a
      LEFT JOIN customer c ON a.customer_id = c.customer_id
      LEFT JOIN branch b ON a.branch_id = b.branch_id
      ORDER BY a.account_id`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/transactions', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_account_ledger ORDER BY txn_date DESC');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/loans', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT l.*, c.full_name AS customer_name, e.full_name AS officer_name
      FROM loan l
      LEFT JOIN customer c ON l.customer_id = c.customer_id
      LEFT JOIN employee e ON l.assigned_officer = e.emp_id
      ORDER BY l.loan_id`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/transfers', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT f.*,
             fa.account_number AS from_account_number,
             ta.account_number AS to_account_number_internal
      FROM fund_transfer f
      LEFT JOIN account fa ON f.from_account_id = fa.account_id
      LEFT JOIN account ta ON f.to_account_id = ta.account_id
      ORDER BY f.initiated_at DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/hr-updates', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_hr_audit_log ORDER BY effective_date DESC');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/branch-summary', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM branch_summary');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  VIEW-BASED ENDPOINTS (from views.sql)
// ════════════════════════════════════════════════════════════

app.get('/api/manager-dashboard', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_manager_dashboard');
    res.json(rows[0] || {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee-directory', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_employee_directory ORDER BY emp_id');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/hr-audit-log', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_hr_audit_log ORDER BY effective_date DESC');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/loan-pipeline', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM view_loan_pipeline ORDER BY application_date');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/account-ledger', async (req, res) => {
  try {
    const { account_id } = req.query;
    let sql = 'SELECT * FROM view_account_ledger';
    const params = [];
    if (account_id) { sql += ' WHERE account_id = $1'; params.push(account_id); }
    sql += ' ORDER BY txn_date DESC';
    const rows = await query(sql, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  BANKING FUNCTIONS (from banking_layer.sql)
// ════════════════════════════════════════════════════════════

app.post('/api/banking/deposit', async (req, res) => {
  try {
    const { account_id, amount } = req.body;
    const rows = await query('SELECT bank_deposit($1, $2) AS result', [account_id, amount]);
    res.json({ message: rows[0].result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/banking/withdraw', async (req, res) => {
  try {
    const { account_id, amount } = req.body;
    const rows = await query('SELECT bank_withdraw($1, $2) AS result', [account_id, amount]);
    res.json({ message: rows[0].result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/banking/transfer', async (req, res) => {
  try {
    const { from_account_id, to_account_id, amount } = req.body;
    const rows = await query('SELECT bank_transfer($1, $2, $3) AS result',
      [from_account_id, to_account_id, amount]);
    res.json({ message: rows[0].result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/banking/apply-loan', async (req, res) => {
  try {
    const { customer_id, amount, purpose } = req.body;
    const rows = await query('SELECT bank_apply_loan($1, $2, $3) AS result',
      [customer_id, amount, purpose || 'General purpose']);
    res.json({ message: rows[0].result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/banking/review-loan', async (req, res) => {
  try {
    const { application_id, emp_id, status, notes } = req.body;
    const rows = await query('SELECT bank_review_loan($1, $2, $3, $4) AS result',
      [application_id, emp_id, status, notes || null]);
    res.json({ message: rows[0].result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/banking/mini-statement/:accountId', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const rows = await query('SELECT * FROM bank_mini_statement($1, $2)',
      [req.params.accountId, limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/banking/customer-summary/:customerId', async (req, res) => {
  try {
    const rows = await query('SELECT bank_customer_summary($1) AS summary',
      [req.params.customerId]);
    res.json({ summary: rows[0].summary });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Loan Applications CRUD ──
app.get('/api/loan-applications', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT la.*, c.full_name AS customer_name, e.full_name AS officer_name
      FROM loan_application la
      LEFT JOIN customer c ON la.customer_id = c.customer_id
      LEFT JOIN employee e ON la.assigned_emp_id = e.emp_id
      ORDER BY la.application_date DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  VCS ENDPOINTS
// ════════════════════════════════════════════════════════════

// ── Config ──
app.get('/api/vcs/active-branch', async (_req, res) => {
  try {
    const rows = await query("SELECT vcs_get_active_branch() AS branch");
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Branches ──
app.get('/api/vcs/branches', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM vcs_branch_list()');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vcs/branches', async (req, res) => {
  try {
    const { name, from_branch, description, checkout } = req.body;
    const rows = await query(
      'SELECT vcs_branch_create($1, $2, $3, $4) AS result',
      [name, from_branch || null, description || null, checkout || false]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vcs/checkout', async (req, res) => {
  try {
    const { branch } = req.body;
    const rows = await query('SELECT vcs_checkout($1) AS result', [branch]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Commits ──
app.get('/api/vcs/log', async (req, res) => {
  try {
    const branch = req.query.branch || null;
    const limit = parseInt(req.query.limit) || 50;
    const rows = branch
      ? await query('SELECT * FROM vcs_log($1, $2)', [branch, limit])
      : await query('SELECT * FROM vcs_log_all($1)', [limit]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vcs/show/:commitId', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM vcs_show($1)', [req.params.commitId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vcs/commit', async (req, res) => {
  try {
    const { message, author } = req.body;
    const rows = await query('SELECT vcs_commit($1, $2) AS result', [message, author || 'web_user']);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Status (staged changes) ──
app.get('/api/vcs/status', async (req, res) => {
  try {
    const branch = req.query.branch || null;
    const rows = await query('SELECT * FROM vcs_status($1)', [branch]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Diffs ──
app.get('/api/vcs/diff', async (req, res) => {
  try {
    const { from, to } = req.query;
    const rows = await query('SELECT * FROM vcs_diff($1, $2)', [from, to]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vcs/diff-branch', async (req, res) => {
  try {
    const { a, b } = req.query;
    const rows = await query('SELECT * FROM vcs_diff_branch($1, $2)', [a, b]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Tags ──
app.get('/api/vcs/tags', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM vcs_tag_list()');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vcs/tags', async (req, res) => {
  try {
    const { name, commit_id, message } = req.body;
    const rows = await query(
      'SELECT vcs_tag_create($1, $2, $3) AS result',
      [name, commit_id || null, message || null]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Blame ──
app.get('/api/vcs/blame/:table', async (req, res) => {
  try {
    const branch = req.query.branch || null;
    const rows = await query('SELECT * FROM vcs_blame($1, $2)', [req.params.table, branch]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Row History ──
app.get('/api/vcs/row-history/:table/:pk', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM vcs_row_history($1, $2)', [req.params.table, req.params.pk]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Merge ──
app.post('/api/vcs/merge', async (req, res) => {
  try {
    const { source, target, message } = req.body;
    const rows = await query(
      'SELECT vcs_merge($1, $2, $3) AS result',
      [source, target || null, message || null]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vcs/merge-conflicts', async (req, res) => {
  try {
    const { source, target } = req.query;
    const rows = await query('SELECT * FROM vcs_merge_conflicts($1, $2)', [source, target]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Rollback ──
app.post('/api/vcs/rollback', async (req, res) => {
  try {
    const { commit_id, table_name, dry_run } = req.body;
    const rows = await query(
      'SELECT vcs_rollback($1, $2, $3) AS result',
      [commit_id, table_name || null, dry_run || false]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Time Travel ──
app.get('/api/vcs/time-travel/:table/:commitId', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM vcs_time_travel($1, $2)', [req.params.table, req.params.commitId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Tracked repos ──
app.get('/api/vcs/repositories', async (_req, res) => {
  try {
    const rows = await query('SELECT * FROM vcs_repository ORDER BY repo_id');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Init tracking ──
app.post('/api/vcs/init', async (req, res) => {
  try {
    const { table_name } = req.body;
    const rows = await query('SELECT vcs_init($1) AS result', [table_name]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Snapshot ──
app.post('/api/vcs/snapshot', async (req, res) => {
  try {
    const { message } = req.body;
    const rows = await query('SELECT vcs_snapshot_all($1) AS result', [message]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Stats ──
app.get('/api/vcs/stats', async (_req, res) => {
  try {
    const rows = await query(`
      SELECT
        (SELECT COUNT(*) FROM vcs_commit) AS total_commits,
        (SELECT COUNT(*) FROM vcs_change) AS total_changes,
        (SELECT COUNT(*) FROM vcs_branch WHERE is_active) AS active_branches,
        (SELECT COUNT(*) FROM vcs_tag) AS total_tags,
        (SELECT COUNT(*) FROM vcs_repository WHERE is_active) AS tracked_tables,
        (SELECT COUNT(*) FROM vcs_staged_change) AS staged_changes
    `);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  AUTH
// ════════════════════════════════════════════════════════════

app.post('/api/auth/login', async (req, res) => {
  try {
    const { name, password } = req.body;
    if (!name || !password) return res.status(400).json({ error: 'Name and password are required' });

    // Try employee
    const empRows = await query(
      `SELECT e.*, d.dept_name, b.branch_name, m.full_name AS manager_name
       FROM employee e
       LEFT JOIN department d ON e.dept_id = d.dept_id
       LEFT JOIN branch b ON e.branch_id = b.branch_id
       LEFT JOIN employee m ON e.manager_id = m.emp_id
       WHERE LOWER(e.full_name) = LOWER($1) AND e.password = $2`, [name, password]);

    if (empRows.length) {
      const emp = empRows[0];
      const d = (emp.designation || '').toLowerCase();
      const isManager = d === 'branch manager' || d === 'general manager' || d === 'regional manager';
      return res.json({ role: isManager ? 'manager' : 'employee', user: emp });
    }

    // Try customer
    const custRows = await query(
      `SELECT c.*, b.branch_name, e.full_name AS rm_name
       FROM customer c
       LEFT JOIN branch b ON c.branch_id = b.branch_id
       LEFT JOIN employee e ON c.assigned_rm_id = e.emp_id
       WHERE LOWER(c.full_name) = LOWER($1) AND c.password = $2`, [name, password]);

    if (custRows.length) {
      return res.json({ role: 'customer', user: custRows[0] });
    }

    return res.status(401).json({ error: 'Invalid name or password' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Change password ──
app.post('/api/auth/change-password', async (req, res) => {
  try {
    const { role, id, currentPassword, newPassword } = req.body;
    if (!role || !id || !currentPassword || !newPassword)
      return res.status(400).json({ error: 'All fields are required' });
    if (newPassword.length < 3)
      return res.status(400).json({ error: 'New password must be at least 3 characters' });

    const table = role === 'customer' ? 'customer' : 'employee';
    const idCol = role === 'customer' ? 'customer_id' : 'emp_id';

    // Verify current password
    const rows = await query(`SELECT password FROM ${table} WHERE ${idCol} = $1`, [id]);
    if (!rows.length) return res.status(404).json({ error: 'User not found' });
    if (rows[0].password !== currentPassword)
      return res.status(401).json({ error: 'Current password is incorrect' });

    await query(`UPDATE ${table} SET password = $1 WHERE ${idCol} = $2`, [newPassword, id]);
    return res.json({ message: 'Password changed successfully' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  BANKING CRUD (Manager)
// ════════════════════════════════════════════════════════════

// ── Customers CRUD ──
app.post('/api/customers', async (req, res) => {
  try {
    const { branch_id, assigned_rm_id, full_name, dob, gender, phone, email,
            occupation, income_bracket, aadhaar_number, pan_number, kyc_status } = req.body;
    const rows = await query(
      `INSERT INTO customer (branch_id, assigned_rm_id, full_name, dob, gender, phone, email,
        occupation, income_bracket, aadhaar_number, pan_number, kyc_status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
      [branch_id, assigned_rm_id, full_name, dob, gender, phone, email,
       occupation, income_bracket, aadhaar_number || null, pan_number || null, kyc_status || 'pending']);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/customers/:id', async (req, res) => {
  try {
    const { full_name, phone, email, occupation, income_bracket, kyc_status, status, assigned_rm_id } = req.body;
    const rows = await query(
      `UPDATE customer SET full_name=COALESCE($1,full_name), phone=COALESCE($2,phone),
        email=COALESCE($3,email), occupation=COALESCE($4,occupation),
        income_bracket=COALESCE($5,income_bracket), kyc_status=COALESCE($6,kyc_status),
        status=COALESCE($7,status), assigned_rm_id=COALESCE($8,assigned_rm_id)
       WHERE customer_id=$9 RETURNING *`,
      [full_name, phone, email, occupation, income_bracket, kyc_status, status, assigned_rm_id, req.params.id]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Accounts CRUD ──
app.post('/api/accounts', async (req, res) => {
  try {
    const { customer_id, branch_id, opened_by, account_number, account_type,
            min_balance, interest_rate } = req.body;
    const rows = await query(
      `INSERT INTO account (customer_id, branch_id, opened_by, account_number,
        account_type, min_balance, interest_rate)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [customer_id, branch_id, opened_by, account_number, account_type,
       min_balance || 0, interest_rate || 0]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/accounts/:id', async (req, res) => {
  try {
    const { interest_rate, status, min_balance } = req.body;
    const rows = await query(
      `UPDATE account SET interest_rate=COALESCE($1,interest_rate),
        status=COALESCE($2,status), min_balance=COALESCE($3,min_balance)
       WHERE account_id=$4 RETURNING *`,
      [interest_rate, status, min_balance, req.params.id]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Transactions CRUD ──
app.post('/api/transactions', async (req, res) => {
  try {
    const { account_id, txn_type, channel, amount, description, initiated_by } = req.body;
    // Get current balance
    const [acct] = await query('SELECT current_balance FROM account WHERE account_id=$1', [account_id]);
    if (!acct) return res.status(404).json({ error: 'Account not found' });
    const bal = parseFloat(acct.current_balance);
    const amt = parseFloat(amount);
    const newBal = txn_type === 'credit' ? bal + amt : bal - amt;
    const ref = `REF${Date.now()}`;
    const rows = await query(
      `INSERT INTO transaction (account_id,txn_type,channel,amount,balance_after,reference_number,description,initiated_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [account_id, txn_type, channel, amt, newBal, ref, description, initiated_by || null]);
    await query('UPDATE account SET current_balance=$1 WHERE account_id=$2', [newBal, account_id]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Loans CRUD ──
app.post('/api/loans', async (req, res) => {
  try {
    const { customer_id, account_id, assigned_officer, loan_type, base_interest_rate,
            processing_fee_pct, applied_amount, purpose, collateral_type, collateral_desc, collateral_value } = req.body;
    const rows = await query(
      `INSERT INTO loan (customer_id,account_id,assigned_officer,loan_type,base_interest_rate,
        processing_fee_pct,applied_amount,purpose,collateral_type,collateral_desc,collateral_value)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [customer_id, account_id, assigned_officer, loan_type, base_interest_rate,
       processing_fee_pct || 0.5, applied_amount, purpose, collateral_type || 'none',
       collateral_desc || null, collateral_value || null]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/loans/:id', async (req, res) => {
  try {
    const { application_status, sanctioned_amount, disbursed_amount, interest_rate,
            tenure_months, emi_amount, disbursement_date, maturity_date,
            outstanding_principal, status, rejection_reason } = req.body;
    const rows = await query(
      `UPDATE loan SET
        application_status=COALESCE($1,application_status),
        sanctioned_amount=COALESCE($2,sanctioned_amount),
        disbursed_amount=COALESCE($3,disbursed_amount),
        interest_rate=COALESCE($4,interest_rate),
        tenure_months=COALESCE($5,tenure_months),
        emi_amount=COALESCE($6,emi_amount),
        disbursement_date=COALESCE($7,disbursement_date),
        maturity_date=COALESCE($8,maturity_date),
        outstanding_principal=COALESCE($9,outstanding_principal),
        status=COALESCE($10,status),
        rejection_reason=COALESCE($11,rejection_reason)
       WHERE loan_id=$12 RETURNING *`,
      [application_status, sanctioned_amount, disbursed_amount, interest_rate,
       tenure_months, emi_amount, disbursement_date, maturity_date,
       outstanding_principal, status, rejection_reason, req.params.id]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Fund Transfers CRUD ──
app.post('/api/transfers', async (req, res) => {
  try {
    const { from_account_id, to_account_id, to_ifsc, to_account_number,
            transfer_mode, amount, remarks } = req.body;
    const rows = await query(
      `INSERT INTO fund_transfer (from_account_id,to_account_id,to_ifsc,to_account_number,
        transfer_mode,amount,remarks)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [from_account_id, to_account_id || null, to_ifsc || null,
       to_account_number || null, transfer_mode, amount, remarks || null]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/transfers/:id', async (req, res) => {
  try {
    const { status } = req.body;
    const rows = await query(
      `UPDATE fund_transfer SET status=$1, settled_at=CASE WHEN $1='completed' THEN NOW() ELSE settled_at END
       WHERE transfer_id=$2 RETURNING *`, [status, req.params.id]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Employees CRUD ──
app.post('/api/employees', async (req, res) => {
  try {
    const { branch_id, dept_id, manager_id, full_name, designation,
            employment_type, join_date, salary } = req.body;
    const rows = await query(
      `INSERT INTO employee (branch_id,dept_id,manager_id,full_name,designation,employment_type,join_date,salary)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [branch_id, dept_id, manager_id || null, full_name, designation,
       employment_type || 'permanent', join_date, salary]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/employees/:id', async (req, res) => {
  try {
    const { designation, salary, employment_type, dept_id, status } = req.body;
    const rows = await query(
      `UPDATE employee SET designation=COALESCE($1,designation), salary=COALESCE($2,salary),
        employment_type=COALESCE($3,employment_type), dept_id=COALESCE($4,dept_id),
        status=COALESCE($5,status)
       WHERE emp_id=$6 RETURNING *`,
      [designation, salary, employment_type, dept_id, status, req.params.id]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── HR Updates — uses bank_hr_update() function with validation ──
app.post('/api/hr-updates', async (req, res) => {
  try {
    const { emp_id, authorized_by, update_type, effective_date, reason,
            new_designation, new_dept_id, new_salary, new_emp_type } = req.body;
    const rows = await query(
      `SELECT bank_hr_update($1,$2,$3,$4,$5,$6,$7,$8,$9) AS result`,
      [emp_id, authorized_by, update_type, effective_date || null, reason || null,
       new_designation || null, new_dept_id || null, new_salary || null, new_emp_type || null]);
    res.json({ message: rows[0].result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  EMPLOYEE-SCOPED ENDPOINTS
// ════════════════════════════════════════════════════════════

app.get('/api/employee/:empId/profile', async (req, res) => {
  try {
    const rows = await query(
      `SELECT e.*, d.dept_name, b.branch_name, m.full_name AS manager_name
       FROM employee e
       LEFT JOIN department d ON e.dept_id = d.dept_id
       LEFT JOIN branch b ON e.branch_id = b.branch_id
       LEFT JOIN employee m ON e.manager_id = m.emp_id
       WHERE e.emp_id = $1`, [req.params.empId]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/customers', async (req, res) => {
  try {
    const rows = await query(
      `SELECT c.*, b.branch_name FROM customer c
       LEFT JOIN branch b ON c.branch_id = b.branch_id
       WHERE c.assigned_rm_id = $1 ORDER BY c.customer_id`, [req.params.empId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/accounts', async (req, res) => {
  try {
    const rows = await query(
      `SELECT a.*, c.full_name AS customer_name, b.branch_name
       FROM account a
       JOIN customer c ON a.customer_id = c.customer_id
       LEFT JOIN branch b ON a.branch_id = b.branch_id
       WHERE c.assigned_rm_id = $1 ORDER BY a.account_id`, [req.params.empId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/transactions', async (req, res) => {
  try {
    const rows = await query(
      `SELECT t.*, a.account_number, c.full_name AS customer_name
       FROM transaction t
       JOIN account a ON t.account_id = a.account_id
       JOIN customer c ON a.customer_id = c.customer_id
       WHERE c.assigned_rm_id = $1
       ORDER BY t.txn_date DESC LIMIT 100`, [req.params.empId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/loans', async (req, res) => {
  try {
    const rows = await query(
      `SELECT l.*, c.full_name AS customer_name
       FROM loan l
       JOIN customer c ON l.customer_id = c.customer_id
       WHERE l.assigned_officer = $1 ORDER BY l.loan_id`, [req.params.empId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/hr-history', async (req, res) => {
  try {
    const rows = await query(
      `SELECT h.*, a.full_name AS authorized_by_name
       FROM employee_hr_update h
       LEFT JOIN employee a ON h.authorized_by = a.emp_id
       WHERE h.emp_id = $1 ORDER BY h.effective_date DESC`, [req.params.empId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/stats', async (req, res) => {
  try {
    const rows = await query(`
      SELECT
        (SELECT COUNT(*) FROM customer WHERE assigned_rm_id = $1) AS assigned_customers,
        (SELECT COUNT(*) FROM account a JOIN customer c ON a.customer_id = c.customer_id WHERE c.assigned_rm_id = $1) AS customer_accounts,
        (SELECT COUNT(*) FROM loan WHERE assigned_officer = $1) AS managed_loans,
        (SELECT COALESCE(SUM(a.current_balance),0) FROM account a JOIN customer c ON a.customer_id = c.customer_id WHERE c.assigned_rm_id = $1 AND a.status='active') AS total_deposits,
        (SELECT COUNT(*) FROM loan_application WHERE assigned_emp_id = $1 AND status NOT IN ('approved','rejected')) AS pending_applications
    `, [req.params.empId]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/queue', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM bank_emp_queue($1)', [req.params.empId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/employee/:empId/workbench', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM view_employee_workbench WHERE emp_id = $1',
      [req.params.empId]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
//  CUSTOMER-SCOPED ENDPOINTS
// ════════════════════════════════════════════════════════════

app.get('/api/customer/:custId/profile', async (req, res) => {
  try {
    const rows = await query(
      `SELECT c.*, b.branch_name, e.full_name AS rm_name
       FROM customer c
       LEFT JOIN branch b ON c.branch_id = b.branch_id
       LEFT JOIN employee e ON c.assigned_rm_id = e.emp_id
       WHERE c.customer_id = $1`, [req.params.custId]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/accounts', async (req, res) => {
  try {
    const rows = await query(
      `SELECT a.*, b.branch_name FROM account a
       LEFT JOIN branch b ON a.branch_id = b.branch_id
       WHERE a.customer_id = $1 ORDER BY a.account_id`, [req.params.custId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/transactions', async (req, res) => {
  try {
    const rows = await query(
      `SELECT t.*, a.account_number FROM transaction t
       JOIN account a ON t.account_id = a.account_id
       WHERE a.customer_id = $1 ORDER BY t.txn_date DESC`, [req.params.custId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/loans', async (req, res) => {
  try {
    const rows = await query(
      `SELECT l.*, e.full_name AS officer_name FROM loan l
       LEFT JOIN employee e ON l.assigned_officer = e.emp_id
       WHERE l.customer_id = $1 ORDER BY l.loan_id`, [req.params.custId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/transfers', async (req, res) => {
  try {
    const rows = await query(
      `SELECT f.*, fa.account_number AS from_account_number,
              ta.account_number AS to_account_number_internal
       FROM fund_transfer f
       JOIN account fa ON f.from_account_id = fa.account_id
       LEFT JOIN account ta ON f.to_account_id = ta.account_id
       WHERE fa.customer_id = $1 ORDER BY f.initiated_at DESC`, [req.params.custId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/stats', async (req, res) => {
  try {
    const rows = await query(`
      SELECT
        (SELECT COALESCE(SUM(current_balance),0) FROM account WHERE customer_id=$1 AND status='active') AS total_balance,
        (SELECT COUNT(*) FROM account WHERE customer_id=$1) AS total_accounts,
        (SELECT COUNT(*) FROM loan WHERE customer_id=$1 AND status IN ('active','pending')) AS active_loans,
        (SELECT COUNT(*) FROM transaction t JOIN account a ON t.account_id=a.account_id WHERE a.customer_id=$1) AS total_transactions,
        (SELECT COUNT(*) FROM loan_application WHERE customer_id=$1 AND status NOT IN ('approved','rejected')) AS pending_applications
    `, [req.params.custId]);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/portal', async (req, res) => {
  try {
    const rows = await query('SELECT * FROM view_customer_portal WHERE customer_id = $1',
      [req.params.custId]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/customer/:custId/loan-applications', async (req, res) => {
  try {
    const rows = await query(
      `SELECT la.*, e.full_name AS officer_name
       FROM loan_application la
       LEFT JOIN employee e ON la.assigned_emp_id = e.emp_id
       WHERE la.customer_id = $1 ORDER BY la.application_date DESC`,
      [req.params.custId]);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Banking VCS API running on http://localhost:${PORT}`);
});
