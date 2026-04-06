import { useState } from 'react';
import { useApi } from '../hooks';
import { getCustomers, createCustomer, updateCustomer, getBranches, getEmployees } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const statusColor = { active: 'green', dormant: 'amber', closed: 'red', blocked: 'red' };

const columns = (onEdit) => [
  { key: 'customer_id', label: 'ID' },
  { key: 'full_name',   label: 'Name' },
  { key: 'phone',       label: 'Phone' },
  { key: 'email',       label: 'Email' },
  { key: 'occupation',  label: 'Occupation' },
  { key: 'income_bracket', label: 'Income', render: (r) => r.income_bracket?.replace(/_/g, ' ') || '—' },
  { key: 'kyc_status', label: 'KYC', render: (r) => <Badge variant={r.kyc_status === 'verified' ? 'green' : 'amber'}>{r.kyc_status}</Badge> },
  { key: 'status', label: 'Status', render: (r) => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
  { key: 'rm_name', label: 'RM' },
  { key: '_edit', label: '', render: (r) => <Btn variant="secondary" onClick={() => onEdit(r)}>Edit</Btn> },
];

const empty = { full_name: '', dob: '', gender: 'M', phone: '', email: '', occupation: '', income_bracket: '', aadhaar_number: '', pan_number: '', kyc_status: 'pending', branch_id: '', assigned_rm_id: '' };

export default function CustomersPage() {
  const { data, loading, error, refetch } = useApi(getCustomers);
  const branches = useApi(getBranches);
  const employees = useApi(getEmployees);
  const [modal, setModal] = useState(null); // null | 'add' | row
  const [form, setForm] = useState(empty);
  const [saving, setSaving] = useState(false);

  const openAdd = () => { setForm(empty); setModal('add'); };
  const openEdit = (r) => { setForm({ ...r }); setModal(r); };
  const close = () => setModal(null);
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const save = async () => {
    setSaving(true);
    try {
      if (modal === 'add') await createCustomer(form);
      else await updateCustomer(form.customer_id, form);
      close(); refetch();
    } catch { /* keep modal open */ }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;

  return (
    <>
      <PageHeader title="Customers" subtitle={`${data.length} records`}>
        <Btn onClick={openAdd}>+ Add Customer</Btn>
      </PageHeader>
      <Card><DataTable columns={columns(openEdit)} rows={data} /></Card>

      <Modal open={!!modal} onClose={close} title={modal === 'add' ? 'Add Customer' : 'Edit Customer'}>
        <div className="grid grid-cols-2 gap-3">
          <FormField label="Full Name"><Input value={form.full_name} onChange={e => set('full_name', e.target.value)} /></FormField>
          <FormField label="Date of Birth"><Input type="date" value={form.dob?.slice(0, 10) || ''} onChange={e => set('dob', e.target.value)} /></FormField>
          <FormField label="Gender">
            <Select value={form.gender} onChange={e => set('gender', e.target.value)}>
              <option value="M">Male</option><option value="F">Female</option><option value="O">Other</option>
            </Select>
          </FormField>
          <FormField label="Phone"><Input value={form.phone} onChange={e => set('phone', e.target.value)} /></FormField>
          <FormField label="Email"><Input type="email" value={form.email} onChange={e => set('email', e.target.value)} /></FormField>
          <FormField label="Occupation"><Input value={form.occupation} onChange={e => set('occupation', e.target.value)} /></FormField>
          <FormField label="Income Bracket">
            <Select value={form.income_bracket} onChange={e => set('income_bracket', e.target.value)}>
              <option value="">Select</option>
              {['below_2L','2L_5L','5L_10L','10L_25L','above_25L'].map(v => <option key={v} value={v}>{v.replace(/_/g, ' ')}</option>)}
            </Select>
          </FormField>
          <FormField label="Aadhaar"><Input value={form.aadhaar_number || ''} onChange={e => set('aadhaar_number', e.target.value)} /></FormField>
          <FormField label="PAN"><Input value={form.pan_number || ''} onChange={e => set('pan_number', e.target.value)} /></FormField>
          <FormField label="KYC Status">
            <Select value={form.kyc_status} onChange={e => set('kyc_status', e.target.value)}>
              {['pending','verified','expired','rejected'].map(v => <option key={v} value={v}>{v}</option>)}
            </Select>
          </FormField>
          <FormField label="Status">
            <Select value={form.status || 'active'} onChange={e => set('status', e.target.value)}>
              {['active','dormant','closed','blocked'].map(v => <option key={v} value={v}>{v}</option>)}
            </Select>
          </FormField>
          <FormField label="Branch">
            <Select value={form.branch_id} onChange={e => set('branch_id', e.target.value)}>
              <option value="">Select</option>
              {(branches.data || []).map(b => <option key={b.branch_id} value={b.branch_id}>{b.branch_name}</option>)}
            </Select>
          </FormField>
          <FormField label="Relationship Manager">
            <Select value={form.assigned_rm_id || ''} onChange={e => set('assigned_rm_id', e.target.value)}>
              <option value="">None</option>
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
