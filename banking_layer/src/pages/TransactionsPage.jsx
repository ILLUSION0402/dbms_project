import { useState } from 'react';
import { useApi } from '../hooks';
import { getTransactions, createTransaction, getAccounts, getEmployees } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const columns = [
  { key: 'txn_id',           label: 'ID' },
  { key: 'account_number',   label: 'Account' },
  { key: 'txn_type',         label: 'Type', render: (r) =>
    <Badge variant={r.txn_type === 'credit' ? 'green' : 'red'}>{r.txn_type}</Badge> },
  { key: 'channel',          label: 'Channel', render: (r) => <Badge variant="blue">{r.channel}</Badge> },
  { key: 'amount',           label: 'Amount', render: (r) => `₹${Number(r.amount).toLocaleString('en-IN')}` },
  { key: 'balance_after',    label: 'Balance After', render: (r) => `₹${Number(r.balance_after).toLocaleString('en-IN')}` },
  { key: 'reference_number', label: 'Reference' },
  { key: 'txn_date',         label: 'Date', render: (r) => new Date(r.txn_date).toLocaleString() },
  { key: 'description',      label: 'Description' },
];

const empty = { account_id: '', txn_type: 'credit', channel: 'branch', amount: '', description: '', initiated_by: '' };

export default function TransactionsPage() {
  const { data, loading, error, refetch } = useApi(getTransactions);
  const accounts = useApi(getAccounts);
  const employees = useApi(getEmployees);
  const [modal, setModal] = useState(false);
  const [form, setForm] = useState(empty);
  const [saving, setSaving] = useState(false);

  const openAdd = () => { setForm(empty); setModal(true); };
  const close = () => setModal(false);
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const save = async () => {
    setSaving(true);
    try {
      await createTransaction(form);
      close(); refetch();
    } catch { }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;

  return (
    <>
      <PageHeader title="Transactions" subtitle={`${data.length} records`}>
        <Btn onClick={openAdd}>+ New Transaction</Btn>
      </PageHeader>
      <Card><DataTable columns={columns} rows={data} /></Card>

      <Modal open={modal} onClose={close} title="New Transaction">
        <div className="grid grid-cols-2 gap-3">
          <FormField label="Account">
            <Select value={form.account_id} onChange={e => set('account_id', e.target.value)}>
              <option value="">Select</option>
              {(accounts.data || []).map(a => <option key={a.account_id} value={a.account_id}>{a.account_number} — {a.customer_name}</option>)}
            </Select>
          </FormField>
          <FormField label="Type">
            <Select value={form.txn_type} onChange={e => set('txn_type', e.target.value)}>
              <option value="credit">Credit</option><option value="debit">Debit</option>
            </Select>
          </FormField>
          <FormField label="Channel">
            <Select value={form.channel} onChange={e => set('channel', e.target.value)}>
              {['branch','atm','upi','neft','rtgs','imps','online','pos'].map(v => <option key={v} value={v}>{v.toUpperCase()}</option>)}
            </Select>
          </FormField>
          <FormField label="Amount"><Input type="number" step="0.01" value={form.amount} onChange={e => set('amount', e.target.value)} /></FormField>
          <FormField label="Description" className="col-span-2"><Input value={form.description} onChange={e => set('description', e.target.value)} /></FormField>
          <FormField label="Initiated By">
            <Select value={form.initiated_by} onChange={e => set('initiated_by', e.target.value)}>
              <option value="">System</option>
              {(employees.data || []).map(e => <option key={e.emp_id} value={e.emp_id}>{e.full_name}</option>)}
            </Select>
          </FormField>
        </div>
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={close}>Cancel</Btn>
          <Btn onClick={save} disabled={saving}>{saving ? 'Saving...' : 'Save'}</Btn>
        </div>
      </Modal>
    </>
  );
}
