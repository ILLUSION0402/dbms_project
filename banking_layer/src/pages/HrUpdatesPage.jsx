import { useState } from 'react';
import { useApi } from '../hooks';
import { getHrUpdates, createHrUpdate, getEmployees, getDepartments } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const typeColor = {
  promotion: 'green', demotion: 'red', salary_revision: 'blue',
  department_transfer: 'amber', designation_change: 'purple',
  employment_type_change: 'indigo', termination: 'red', reinstatement: 'green',
};

const columns = [
  { key: 'hr_update_id',     label: 'ID' },
  { key: 'employee_name',    label: 'Employee' },
  { key: 'update_type',      label: 'Type', render: (r) =>
    <Badge variant={typeColor[r.update_type]}>{r.update_type.replace(/_/g, ' ')}</Badge> },
  { key: 'effective_date',   label: 'Effective', render: (r) => r.effective_date?.slice(0, 10) },
  { key: 'old_designation',  label: 'Old Designation' },
  { key: 'new_designation',  label: 'New Designation' },
  { key: 'old_salary',       label: 'Old Salary', render: (r) => r.old_salary ? `₹${Number(r.old_salary).toLocaleString('en-IN')}` : '—' },
  { key: 'new_salary',       label: 'New Salary', render: (r) => r.new_salary ? `₹${Number(r.new_salary).toLocaleString('en-IN')}` : '—' },
  { key: 'salary_delta',     label: 'Δ Salary', render: (r) => r.salary_delta ? <span className={Number(r.salary_delta) >= 0 ? 'text-green-600' : 'text-red-600'}>₹{Number(r.salary_delta).toLocaleString('en-IN')} ({r.salary_change_pct}%)</span> : '—' },
  { key: 'reason',           label: 'Reason', render: (r) => <span className="max-w-xs truncate block">{r.reason}</span> },
  { key: 'authorised_by', label: 'Authorized By' },
];

const emptyHr = { emp_id: '', update_type: 'salary_revision', effective_date: '', new_designation: '', new_dept_id: '', new_salary: '', new_emp_type: '', reason: '', authorized_by: '' };

export default function HrUpdatesPage() {
  const { data, loading, error, refetch } = useApi(getHrUpdates);
  const employees = useApi(getEmployees);
  const depts = useApi(getDepartments);
  const [modal, setModal] = useState(false);
  const [form, setForm] = useState(emptyHr);
  const [saving, setSaving] = useState(false);
  const [errMsg, setErrMsg] = useState(null);

  const openAdd = () => { setForm(emptyHr); setErrMsg(null); setModal(true); };
  const close = () => setModal(false);
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const save = async () => {
    setSaving(true);
    setErrMsg(null);
    try {
      await createHrUpdate(form);
      close(); refetch();
    } catch (e) { setErrMsg(e.message); }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;

  return (
    <>
      <PageHeader title="HR Updates" subtitle={`${data.length} records — uses bank_hr_update() with validation`}>
        <Btn onClick={openAdd}>+ New HR Update</Btn>
      </PageHeader>
      <Card><DataTable columns={columns} rows={data} /></Card>

      <Modal open={modal} onClose={close} title="New HR Update (validated)">
        {errMsg && <div className="mb-3 p-2 bg-red-50 text-red-700 text-sm rounded">{errMsg}</div>}
        <p className="text-xs text-gray-400 mb-3">Old values are auto-captured by bank_hr_update(). Self-authorization is blocked.</p>
        <div className="grid grid-cols-2 gap-3">
          <FormField label="Employee">
            <Select value={form.emp_id} onChange={e => set('emp_id', e.target.value)}>
              <option value="">Select</option>
              {(employees.data || []).map(e => <option key={e.emp_id} value={e.emp_id}>{e.full_name} ({e.designation})</option>)}
            </Select>
          </FormField>
          <FormField label="Update Type">
            <Select value={form.update_type} onChange={e => set('update_type', e.target.value)}>
              {['promotion','demotion','salary_revision','department_transfer','designation_change','employment_type_change','termination','reinstatement'].map(v => <option key={v} value={v}>{v.replace(/_/g, ' ')}</option>)}
            </Select>
          </FormField>
          <FormField label="Effective Date"><Input type="date" value={form.effective_date} onChange={e => set('effective_date', e.target.value)} /></FormField>
          <FormField label="Authorized By">
            <Select value={form.authorized_by} onChange={e => set('authorized_by', e.target.value)}>
              <option value="">Select</option>
              {(employees.data || []).filter(e => e.designation?.toLowerCase().includes('manager')).map(e => <option key={e.emp_id} value={e.emp_id}>{e.full_name}</option>)}
            </Select>
          </FormField>
          <FormField label="New Designation"><Input value={form.new_designation} onChange={e => set('new_designation', e.target.value)} placeholder="Leave blank if unchanged" /></FormField>
          <FormField label="New Salary"><Input type="number" value={form.new_salary} onChange={e => set('new_salary', e.target.value)} placeholder="Leave blank if unchanged" /></FormField>
          <FormField label="New Dept">
            <Select value={form.new_dept_id || ''} onChange={e => set('new_dept_id', e.target.value)}>
              <option value="">— unchanged —</option>
              {(depts.data || []).map(d => <option key={d.dept_id} value={d.dept_id}>{d.dept_name}</option>)}
            </Select>
          </FormField>
          <FormField label="New Emp Type">
            <Select value={form.new_emp_type || ''} onChange={e => set('new_emp_type', e.target.value)}>
              <option value="">— unchanged —</option>
              {['permanent','contract','probation','intern'].map(v => <option key={v} value={v}>{v}</option>)}
            </Select>
          </FormField>
          <FormField label="Reason" className="col-span-2"><Input value={form.reason} onChange={e => set('reason', e.target.value)} /></FormField>
        </div>
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={close}>Cancel</Btn>
          <Btn onClick={save} disabled={saving}>{saving ? 'Saving...' : 'Save'}</Btn>
        </div>
      </Modal>
    </>
  );
}
