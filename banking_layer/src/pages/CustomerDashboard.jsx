import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getCustomerAccounts, getCustomerTransactions, getCustomerLoans, getCustomerStats,
         getCustomerLoanApps, bankApplyLoan, bankDeposit, bankWithdraw } from '../api';
import { PageHeader, Card, StatCard, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';
import { CreditCard, TrendingUp, Landmark, IndianRupee, FileText } from 'lucide-react';

export default function CustomerDashboard() {
  const { auth } = useAuth();
  const cid = auth.user.customer_id;

  const stats    = useApi(() => getCustomerStats(cid), [cid]);
  const accounts = useApi(() => getCustomerAccounts(cid), [cid]);
  const txns     = useApi(() => getCustomerTransactions(cid), [cid]);
  const loans    = useApi(() => getCustomerLoans(cid), [cid]);
  const loanApps = useApi(() => getCustomerLoanApps(cid), [cid]);

  const [loanModal, setLoanModal] = useState(false);
  const [loanForm, setLoanForm] = useState({ amount: '', purpose: '' });
  const [txnModal, setTxnModal] = useState(false);
  const [txnForm, setTxnForm] = useState({ account_id: '', type: 'deposit', amount: '' });
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState(null);

  const applyLoan = async () => {
    setSaving(true);
    try {
      const r = await bankApplyLoan({ customer_id: cid, amount: loanForm.amount, purpose: loanForm.purpose });
      setMsg(r.message); setLoanModal(false); loanApps.refetch(); stats.refetch();
    } catch (e) { setMsg(e.message); }
    finally { setSaving(false); }
  };

  const doTxn = async () => {
    setSaving(true);
    try {
      const fn = txnForm.type === 'deposit' ? bankDeposit : bankWithdraw;
      const r = await fn({ account_id: txnForm.account_id, amount: txnForm.amount });
      setMsg(r.message); setTxnModal(false); accounts.refetch(); txns.refetch(); stats.refetch();
    } catch (e) { setMsg(e.message); }
    finally { setSaving(false); }
  };

  if (stats.loading) return <Spinner />;

  const s = stats.data || {};

  return (
    <>
      <PageHeader title={`Welcome, ${auth.user.full_name}`} subtitle="Your banking overview">
        <div className="flex gap-2">
          <Btn onClick={() => { setTxnForm({ account_id: '', type: 'deposit', amount: '' }); setTxnModal(true); }}>Deposit / Withdraw</Btn>
          <Btn variant="secondary" onClick={() => { setLoanForm({ amount: '', purpose: '' }); setLoanModal(true); }}>Apply for Loan</Btn>
        </div>
      </PageHeader>

      {msg && <div className="mb-4 p-3 bg-blue-50 text-blue-800 rounded-lg text-sm flex justify-between"><span>{msg}</span><button onClick={() => setMsg(null)} className="text-blue-600 hover:underline cursor-pointer">×</button></div>}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 mb-8">
        <StatCard label="Total Balance" value={`₹${Number(s.total_balance || 0).toLocaleString('en-IN')}`} icon={IndianRupee} color="indigo" />
        <StatCard label="Accounts" value={s.total_accounts} icon={CreditCard} color="green" />
        <StatCard label="Active Loans" value={s.active_loans} icon={Landmark} color="amber" />
        <StatCard label="Pending Apps" value={s.pending_applications} icon={FileText} color="red" />
        <StatCard label="Transactions" value={s.total_transactions} icon={TrendingUp} color="sky" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Your Accounts</h3>
          {accounts.loading ? <Spinner /> :
           accounts.error ? <ErrorBox message={accounts.error} /> :
            <DataTable columns={[
              { key: 'account_number', label: 'Account #' },
              { key: 'account_type', label: 'Type', render: r => <Badge variant="blue">{r.account_type}</Badge> },
              { key: 'current_balance', label: 'Balance', render: r => `₹${Number(r.current_balance).toLocaleString('en-IN')}` },
              { key: 'status', label: 'Status', render: r => <Badge variant={r.status === 'active' ? 'green' : 'amber'}>{r.status}</Badge> },
            ]} rows={accounts.data} />}
        </Card>

        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Recent Transactions</h3>
          {txns.loading ? <Spinner /> :
           txns.error ? <ErrorBox message={txns.error} /> :
            <DataTable columns={[
              { key: 'txn_date', label: 'Date', render: r => r.txn_date?.slice(0, 10) },
              { key: 'txn_type', label: 'Type', render: r => <Badge variant={r.txn_type === 'credit' ? 'green' : 'red'}>{r.txn_type}</Badge> },
              { key: 'amount', label: 'Amount', render: r => `₹${Number(r.amount).toLocaleString('en-IN')}` },
              { key: 'description', label: 'Description' },
            ]} rows={(txns.data || []).slice(0, 10)} />}
        </Card>
      </div>

      {loans.data?.length > 0 && (
        <Card className="p-6 mt-6">
          <h3 className="font-semibold text-gray-900 mb-4">Your Loans</h3>
          <DataTable columns={[
            { key: 'loan_type', label: 'Type', render: r => <Badge variant="purple">{r.loan_type}</Badge> },
            { key: 'applied_amount', label: 'Applied', render: r => `₹${Number(r.applied_amount).toLocaleString('en-IN')}` },
            { key: 'interest_rate', label: 'Rate %', render: r => r.interest_rate || r.base_interest_rate },
            { key: 'application_status', label: 'Status', render: r => {
              const c = { submitted: 'amber', under_review: 'blue', approved: 'green', rejected: 'red', disbursed: 'green', withdrawn: 'red' };
              return <Badge variant={c[r.application_status] || 'default'}>{r.application_status}</Badge>;
            }},
            { key: 'emi_amount', label: 'EMI', render: r => r.emi_amount ? `₹${Number(r.emi_amount).toLocaleString('en-IN')}` : '—' },
          ]} rows={loans.data} />
        </Card>
      )}

      {loanApps.data?.length > 0 && (
        <Card className="p-6 mt-6">
          <h3 className="font-semibold text-gray-900 mb-4">Loan Applications</h3>
          <DataTable columns={[
            { key: 'application_id', label: 'ID' },
            { key: 'requested_amount', label: 'Amount', render: r => `₹${Number(r.requested_amount).toLocaleString('en-IN')}` },
            { key: 'purpose', label: 'Purpose' },
            { key: 'status', label: 'Status', render: r => {
              const c = { submitted: 'amber', under_review: 'blue', approved: 'green', rejected: 'red' };
              return <Badge variant={c[r.status] || 'default'}>{r.status}</Badge>;
            }},
            { key: 'officer_name', label: 'Officer' },
            { key: 'application_date', label: 'Applied', render: r => r.application_date?.slice(0, 10) },
          ]} rows={loanApps.data} />
        </Card>
      )}

      {/* Apply Loan Modal */}
      <Modal open={loanModal} onClose={() => setLoanModal(false)} title="Apply for Loan">
        <div className="space-y-3">
          <FormField label="Amount"><Input type="number" step="0.01" value={loanForm.amount} onChange={e => setLoanForm(f => ({ ...f, amount: e.target.value }))} /></FormField>
          <FormField label="Purpose"><Input value={loanForm.purpose} onChange={e => setLoanForm(f => ({ ...f, purpose: e.target.value }))} placeholder="e.g., Home renovation" /></FormField>
        </div>
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={() => setLoanModal(false)}>Cancel</Btn>
          <Btn onClick={applyLoan} disabled={saving}>{saving ? 'Submitting...' : 'Submit Application'}</Btn>
        </div>
      </Modal>

      {/* Deposit/Withdraw Modal */}
      <Modal open={txnModal} onClose={() => setTxnModal(false)} title="Deposit / Withdraw">
        <div className="space-y-3">
          <FormField label="Account">
            <Select value={txnForm.account_id} onChange={e => setTxnForm(f => ({ ...f, account_id: e.target.value }))}>
              <option value="">Select</option>
              {(accounts.data || []).filter(a => a.status === 'active').map(a => <option key={a.account_id} value={a.account_id}>{a.account_number} — ₹{Number(a.current_balance).toLocaleString('en-IN')}</option>)}
            </Select>
          </FormField>
          <FormField label="Type">
            <Select value={txnForm.type} onChange={e => setTxnForm(f => ({ ...f, type: e.target.value }))}>
              <option value="deposit">Deposit</option><option value="withdraw">Withdraw</option>
            </Select>
          </FormField>
          <FormField label="Amount"><Input type="number" step="0.01" value={txnForm.amount} onChange={e => setTxnForm(f => ({ ...f, amount: e.target.value }))} /></FormField>
        </div>
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={() => setTxnModal(false)}>Cancel</Btn>
          <Btn onClick={doTxn} disabled={saving}>{saving ? 'Processing...' : txnForm.type === 'deposit' ? 'Deposit' : 'Withdraw'}</Btn>
        </div>
      </Modal>
    </>
  );
}
