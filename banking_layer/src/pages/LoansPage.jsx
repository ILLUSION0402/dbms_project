import { useState } from 'react';
import { useApi } from '../hooks';
import { getLoans, createLoan, updateLoan, getCustomers, getAccounts, getEmployees } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const statusColor = { active: 'green', pending: 'amber', closed: 'default', npa: 'red', written_off: 'red', foreclosed: 'red' };
const appColor = { submitted: 'blue', under_review: 'amber', approved: 'green', rejected: 'red', disbursed: 'indigo', withdrawn: 'default' };

const columns = (onEdit) => [
  { key: 'loan_id',        label: 'ID' },
  { key: 'customer_name',  label: 'Customer' },
  { key: 'loan_type',      label: 'Type', render: (r) => <Badge variant="purple">{r.loan_type}</Badge> },
  { key: 'applied_amount', label: 'Applied', render: (r) => `₹${Number(r.applied_amount).toLocaleString('en-IN')}` },
  { key: 'sanctioned_amount', label: 'Sanctioned', render: (r) => r.sanctioned_amount ? `₹${Number(r.sanctioned_amount).toLocaleString('en-IN')}` : '—' },
  { key: 'interest_rate',  label: 'Rate %', render: (r) => r.interest_rate ?? r.base_interest_rate },
  { key: 'application_status', label: 'App Status', render: (r) => <Badge variant={appColor[r.application_status]}>{r.application_status}</Badge> },
  { key: 'status',         label: 'Status', render: (r) => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
  { key: 'officer_name',   label: 'Officer' },
  { key: '_edit', label: '', render: (r) => <Btn variant="secondary" onClick={() => onEdit(r)}>Edit</Btn> },
];

const emptyLoan = { customer_id: '', account_id: '', assigned_officer: '', loan_type: 'personal_loan', base_interest_rate: '', processing_fee_pct: '0.5', applied_amount: '', purpose: '', collateral_type: 'none', collateral_desc: '', collateral_value: '' };

export default function LoansPage() {
  const { data, loading, error, refetch } = useApi(getLoans);
  const customers = useApi(getCustomers);
  const accounts  = useApi(getAccounts);
  const employees = useApi(getEmployees);
  const [modal, setModal] = useState(null);
  const [form, setForm] = useState(emptyLoan);
  const [saving, setSaving] = useState(false);

  const openAdd = () => { setForm(emptyLoan); setModal('add'); };
  const openEdit = (r) => { setForm({ ...r }); setModal(r); };
  const close = () => setModal(null);
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const save = async () => {
    setSaving(true);
    try {
      if (modal === 'add') await createLoan(form);
      else await updateLoan(form.loan_id, form);
      close(); refetch();
    } catch { }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error)   return <ErrorBox message={error} onRetry={refetch} />;

  return (
    <>
      <PageHeader title="Loans" subtitle={`${data.length} records`}>
        <Btn onClick={openAdd}>+ New Loan</Btn>
      </PageHeader>
      <Card><DataTable columns={columns(openEdit)} rows={data} /></Card>

      <Modal open={!!modal} onClose={close} title={modal === 'add' ? 'New Loan Application' : 'Update Loan'}>
        <div className="grid grid-cols-2 gap-3">
          {modal === 'add' ? (
            <>
              <FormField label="Customer">
                <Select value={form.customer_id} onChange={e => set('customer_id', e.target.value)}>
                  <option value="">Select</option>
                  {(customers.data || []).map(c => <option key={c.customer_id} value={c.customer_id}>{c.full_name}</option>)}
                </Select>
              </FormField>
              <FormField label="Account">
                <Select value={form.account_id} onChange={e => set('account_id', e.target.value)}>
                  <option value="">Select</option>
                  {(accounts.data || []).map(a => <option key={a.account_id} value={a.account_id}>{a.account_number}</option>)}
                </Select>
              </FormField>
              <FormField label="Officer">
                <Select value={form.assigned_officer} onChange={e => set('assigned_officer', e.target.value)}>
                  <option value="">Select</option>
                  {(employees.data || []).map(e => <option key={e.emp_id} value={e.emp_id}>{e.full_name}</option>)}
                </Select>
              </FormField>
              <FormField label="Loan Type">
                <Select value={form.loan_type} onChange={e => set('loan_type', e.target.value)}>
                  {['home_loan','personal_loan','vehicle_loan','education_loan','gold_loan','business_loan'].map(v => <option key={v} value={v}>{v.replace(/_/g, ' ')}</option>)}
                </Select>
              </FormField>
              <FormField label="Base Interest Rate %"><Input type="number" step="0.01" value={form.base_interest_rate} onChange={e => set('base_interest_rate', e.target.value)} /></FormField>
              <FormField label="Applied Amount"><Input type="number" value={form.applied_amount} onChange={e => set('applied_amount', e.target.value)} /></FormField>
              <FormField label="Purpose" className="col-span-2"><Input value={form.purpose} onChange={e => set('purpose', e.target.value)} /></FormField>
              <FormField label="Collateral Type">
                <Select value={form.collateral_type} onChange={e => set('collateral_type', e.target.value)}>
                  {['none','property','gold','vehicle','fd','other'].map(v => <option key={v} value={v}>{v}</option>)}
                </Select>
              </FormField>
              <FormField label="Collateral Value"><Input type="number" value={form.collateral_value} onChange={e => set('collateral_value', e.target.value)} /></FormField>
            </>
          ) : (
            <>
              <FormField label="Application Status">
                <Select value={form.application_status} onChange={e => set('application_status', e.target.value)}>
                  {['submitted','under_review','approved','rejected','disbursed','withdrawn'].map(v => <option key={v} value={v}>{v.replace(/_/g, ' ')}</option>)}
                </Select>
              </FormField>
              <FormField label="Sanctioned Amount"><Input type="number" value={form.sanctioned_amount || ''} onChange={e => set('sanctioned_amount', e.target.value)} /></FormField>
              <FormField label="Disbursed Amount"><Input type="number" value={form.disbursed_amount || ''} onChange={e => set('disbursed_amount', e.target.value)} /></FormField>
              <FormField label="Interest Rate %"><Input type="number" step="0.01" value={form.interest_rate || ''} onChange={e => set('interest_rate', e.target.value)} /></FormField>
              <FormField label="Tenure (months)"><Input type="number" value={form.tenure_months || ''} onChange={e => set('tenure_months', e.target.value)} /></FormField>
              <FormField label="EMI"><Input type="number" value={form.emi_amount || ''} onChange={e => set('emi_amount', e.target.value)} /></FormField>
              <FormField label="Disbursement Date"><Input type="date" value={form.disbursement_date?.slice(0, 10) || ''} onChange={e => set('disbursement_date', e.target.value)} /></FormField>
              <FormField label="Maturity Date"><Input type="date" value={form.maturity_date?.slice(0, 10) || ''} onChange={e => set('maturity_date', e.target.value)} /></FormField>
              <FormField label="Outstanding Principal"><Input type="number" value={form.outstanding_principal || ''} onChange={e => set('outstanding_principal', e.target.value)} /></FormField>
              <FormField label="Loan Status">
                <Select value={form.status} onChange={e => set('status', e.target.value)}>
                  {['pending','active','closed','npa','written_off','foreclosed'].map(v => <option key={v} value={v}>{v.replace(/_/g, ' ')}</option>)}
                </Select>
              </FormField>
            </>
          )}
        </div>
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={close}>Cancel</Btn>
          <Btn onClick={save} disabled={saving}>{saving ? 'Saving...' : 'Save'}</Btn>
        </div>
      </Modal>
    </>
  );
}
