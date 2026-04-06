const BASE = '/api';

async function request(url, options = {}) {
  const res = await fetch(`${BASE}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || res.statusText);
  }
  return res.json();
}

// ── Banking ──
export const getBranches      = ()  => request('/branches');
export const getDepartments   = ()  => request('/departments');
export const getEmployees     = ()  => request('/employees');
export const getCustomers     = ()  => request('/customers');
export const getAccounts      = ()  => request('/accounts');
export const getTransactions  = ()  => request('/transactions');
export const getLoans         = ()  => request('/loans');
export const getTransfers     = ()  => request('/transfers');
export const getHrUpdates     = ()  => request('/hr-updates');
export const getBranchSummary = ()  => request('/branch-summary');

// ── VCS ──
export const getActiveBranch  = ()  => request('/vcs/active-branch');
export const getVcsBranches   = ()  => request('/vcs/branches');
export const createBranch     = (b) => request('/vcs/branches', { method: 'POST', body: JSON.stringify(b) });
export const checkoutBranch   = (branch) => request('/vcs/checkout', { method: 'POST', body: JSON.stringify({ branch }) });

export const getVcsLog        = (branch, limit) => {
  const params = new URLSearchParams();
  if (branch) params.set('branch', branch);
  if (limit)  params.set('limit', String(limit));
  return request(`/vcs/log?${params}`);
};
export const getCommitDetail  = (id) => request(`/vcs/show/${id}`);
export const createCommit     = (c)  => request('/vcs/commit', { method: 'POST', body: JSON.stringify(c) });

export const getVcsStatus     = (branch) => {
  const params = branch ? `?branch=${branch}` : '';
  return request(`/vcs/status${params}`);
};

export const getVcsDiff       = (from, to) => request(`/vcs/diff?from=${from}&to=${to}`);
export const getVcsDiffBranch = (a, b) => request(`/vcs/diff-branch?a=${a}&b=${b}`);

export const getVcsTags       = ()  => request('/vcs/tags');
export const createTag        = (t) => request('/vcs/tags', { method: 'POST', body: JSON.stringify(t) });

export const getBlame         = (table, branch) => {
  const params = branch ? `?branch=${branch}` : '';
  return request(`/vcs/blame/${table}${params}`);
};
export const getRowHistory    = (table, pk) => request(`/vcs/row-history/${table}/${pk}`);

export const mergeBranch      = (m) => request('/vcs/merge', { method: 'POST', body: JSON.stringify(m) });
export const getMergeConflicts = (source, target) =>
  request(`/vcs/merge-conflicts?source=${source}&target=${target}`);

export const rollback         = (r) => request('/vcs/rollback', { method: 'POST', body: JSON.stringify(r) });
export const getTimeTravel    = (table, commitId) => request(`/vcs/time-travel/${table}/${commitId}`);

export const getRepositories  = ()  => request('/vcs/repositories');
export const initTracking     = (table_name) => request('/vcs/init', { method: 'POST', body: JSON.stringify({ table_name }) });
export const snapshotAll      = (message) => request('/vcs/snapshot', { method: 'POST', body: JSON.stringify({ message }) });
export const getVcsStats      = ()  => request('/vcs/stats');

// ── Auth ──
export const loginUser = (name, password) => request('/auth/login', { method: 'POST', body: JSON.stringify({ name, password }) });
export const changePassword = (role, id, currentPassword, newPassword) => request('/auth/change-password', { method: 'POST', body: JSON.stringify({ role, id, currentPassword, newPassword }) });

// ── Banking CRUD ──
export const createCustomer    = (d) => request('/customers', { method: 'POST', body: JSON.stringify(d) });
export const updateCustomer    = (id, d) => request(`/customers/${id}`, { method: 'PUT', body: JSON.stringify(d) });
export const createAccount     = (d) => request('/accounts', { method: 'POST', body: JSON.stringify(d) });
export const updateAccount     = (id, d) => request(`/accounts/${id}`, { method: 'PUT', body: JSON.stringify(d) });
export const createTransaction = (d) => request('/transactions', { method: 'POST', body: JSON.stringify(d) });
export const createLoan        = (d) => request('/loans', { method: 'POST', body: JSON.stringify(d) });
export const updateLoan        = (id, d) => request(`/loans/${id}`, { method: 'PUT', body: JSON.stringify(d) });
export const createTransfer    = (d) => request('/transfers', { method: 'POST', body: JSON.stringify(d) });
export const updateTransfer    = (id, d) => request(`/transfers/${id}`, { method: 'PUT', body: JSON.stringify(d) });
export const createEmployee    = (d) => request('/employees', { method: 'POST', body: JSON.stringify(d) });
export const updateEmployee    = (id, d) => request(`/employees/${id}`, { method: 'PUT', body: JSON.stringify(d) });
export const createHrUpdate    = (d) => request('/hr-updates', { method: 'POST', body: JSON.stringify(d) });

// ── View-based endpoints ──
export const getManagerDashboard  = ()  => request('/manager-dashboard');
export const getEmployeeDirectory = ()  => request('/employee-directory');
export const getHrAuditLog        = ()  => request('/hr-audit-log');
export const getLoanPipeline       = ()  => request('/loan-pipeline');
export const getAccountLedger     = (accountId) => request(`/account-ledger${accountId ? `?account_id=${accountId}` : ''}`);
export const getLoanApplications   = ()  => request('/loan-applications');

// ── Banking functions ──
export const bankDeposit    = (d) => request('/banking/deposit', { method: 'POST', body: JSON.stringify(d) });
export const bankWithdraw   = (d) => request('/banking/withdraw', { method: 'POST', body: JSON.stringify(d) });
export const bankTransfer   = (d) => request('/banking/transfer', { method: 'POST', body: JSON.stringify(d) });
export const bankApplyLoan  = (d) => request('/banking/apply-loan', { method: 'POST', body: JSON.stringify(d) });
export const bankReviewLoan = (d) => request('/banking/review-loan', { method: 'POST', body: JSON.stringify(d) });
export const getMiniStatement = (accountId, limit) => request(`/banking/mini-statement/${accountId}${limit ? `?limit=${limit}` : ''}`);
export const getCustomerSummary = (id) => request(`/banking/customer-summary/${id}`);

// ── Employee-scoped ──
export const getEmployeeProfile      = (id) => request(`/employee/${id}/profile`);
export const getEmployeeCustomers    = (id) => request(`/employee/${id}/customers`);
export const getEmployeeAccounts     = (id) => request(`/employee/${id}/accounts`);
export const getEmployeeTransactions = (id) => request(`/employee/${id}/transactions`);
export const getEmployeeLoans        = (id) => request(`/employee/${id}/loans`);
export const getEmployeeHrHistory    = (id) => request(`/employee/${id}/hr-history`);
export const getEmployeeStats        = (id) => request(`/employee/${id}/stats`);
export const getEmployeeQueue        = (id) => request(`/employee/${id}/queue`);
export const getEmployeeWorkbench    = (id) => request(`/employee/${id}/workbench`);

// ── Customer-scoped ──
export const getCustomerProfile      = (id) => request(`/customer/${id}/profile`);
export const getCustomerAccounts     = (id) => request(`/customer/${id}/accounts`);
export const getCustomerTransactions = (id) => request(`/customer/${id}/transactions`);
export const getCustomerLoans        = (id) => request(`/customer/${id}/loans`);
export const getCustomerTransfers    = (id) => request(`/customer/${id}/transfers`);
export const getCustomerStats        = (id) => request(`/customer/${id}/stats`);
export const getCustomerPortal       = (id) => request(`/customer/${id}/portal`);
export const getCustomerLoanApps     = (id) => request(`/customer/${id}/loan-applications`);
