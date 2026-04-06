import { useState } from 'react';
import { useApi } from '../hooks';
import { getTransfers, createTransfer, updateTransfer, getAccounts } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const statusColor = { completed: 'green', pending: 'amber', processing: 'blue', failed: 'red', reversed: 'red' };

const columns = (onEdit) => [
  { key: 'transfer_id',           label: 'ID' },
  { key: 'from_account_number',   label: 'From Account' },
  { key: 'to_account_number_internal', label: 'To (Internal)', render: (r) => r.to_account_number_internal || r.to_account_number || '—' },
  { key: 'to_ifsc',               label: 'To IFSC' },
  { key: 'transfer_mode',         label: 'Mode', render: (r) => <Badge variant="blue">{r.transfer_mode}</Badge> },
  { key: 'amount',                label: 'Amount', render: (r) => `₹${Number(r.amount).toLocaleString('en-IN')}` },
  { key: 'status',                label: 'Status', render: (r) => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
  { key: 'initiated_at',          label: 'Initiated', render: (r) => new Date(r.initiated_at).toLocaleString() },
  { key: '_edit', label: '', render: (r) => r.status === 'pending' ? <Btn variant="secondary" onClick={() => onEdit(r)}>Update</Btn> : null },
];

const empty = { from_account_id: '', to_account_id: '', to_ifsc: '', to_account_number: '', transfer_mode: 'neft', amount: '', remarks: '' };

export default function TransfersPage() {
  const { data, loading, error, refetch } = useApi(getTransfers);
  const accounts = useApi(getAccounts);
  const [modal, setModal] = useState(null);
  const [form, setForm] = useState(empty);
  const [saving, setSaving] = useState(false);

  const openAdd = () => { setForm(empty); setModal('add'); };
  const openEdit = (r) => { setForm({ ...r }); setModal(r); };
  const close = () => setModal(null);
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const save = async () => {
    setSaving(true);
    try {
      if (modal === 'add') await createTransfer(form);
      else await updateTransfer(form.transfer_id, { status: form.status });
      close(); refetch();
    } catch { }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;

  return (
    <>
      <PageHeader title="Fund Transfers" subtitle={`${data.length} records`}>
        <Btn onClick={openAdd}>+ New Transfer</Btn>
      </PageHeader>
      <Card><DataTable columns={columns(openEdit)} rows={data} /></Card>

      <Modal open={!!modal} onClose={close} title={modal === 'add' ? 'New Fund Transfer' : 'Update Transfer Status'}>
        {modal === 'add' ? (
          <div className="grid grid-cols-2 gap-3">
            <FormField label="From Account">
              <Select value={form.from_account_id} onChange={e => set('from_account_id', e.target.value)}>
                <option value="">Select</option>
                {(accounts.data || []).map(a => <option key={a.account_id} value={a.account_id}>{a.account_number} — {a.customer_name}</option>)}
              </Select>
            </FormField>
            <FormField label="To Account (Internal)">
              <Select value={form.to_account_id} onChange={e => set('to_account_id', e.target.value)}>
                <option value="">External / None</option>
                {(accounts.data || []).map(a => <option key={a.account_id} value={a.account_id}>{a.account_number} — {a.customer_name}</option>)}
              </Select>
            </FormField>
            <FormField label="To IFSC (External)"><Input value={form.to_ifsc} onChange={e => set('to_ifsc', e.target.value)} placeholder="e.g. SBIN0001234" /></FormField>
            <FormField label="To Account # (External)"><Input value={form.to_account_number} onChange={e => set('to_account_number', e.target.value)} /></FormField>
            <FormField label="Transfer Mode">
              <Select value={form.transfer_mode} onChange={e => set('transfer_mode', e.target.value)}>
                {['neft','rtgs','imps','upi','internal'].map(v => <option key={v} value={v}>{v.toUpperCase()}</option>)}
              </Select>
            </FormField>
            <FormField label="Amount"><Input type="number" step="0.01" value={form.amount} onChange={e => set('amount', e.target.value)} /></FormField>
            <FormField label="Remarks" className="col-span-2"><Input value={form.remarks} onChange={e => set('remarks', e.target.value)} /></FormField>
          </div>
        ) : (
          <FormField label="Status">
            <Select value={form.status} onChange={e => set('status', e.target.value)}>
              {['pending','processing','completed','failed','reversed'].map(v => <option key={v} value={v}>{v}</option>)}
            </Select>
          </FormField>
        )}
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={close}>Cancel</Btn>
          <Btn onClick={save} disabled={saving}>{saving ? 'Saving...' : 'Save'}</Btn>
        </div>
      </Modal>
    </>
  );
}
