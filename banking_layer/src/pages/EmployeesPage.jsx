import { useState } from 'react';
import { useApi } from '../hooks';
import { getEmployees, createEmployee, updateEmployee, getBranches, getDepartments } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const statusColor = { active: 'green', resigned: 'amber', terminated: 'red' };

const columns = (onEdit) => [
  { key: 'emp_id',          label: 'ID' },
  { key: 'full_name',      label: 'Name' },
  { key: 'designation',    label: 'Designation' },
  { key: 'dept_name',      label: 'Department' },
  { key: 'branch_name',    label: 'Branch' },
  { key: 'employment_type', label: 'Type', render: (r) => <Badge variant="blue">{r.employment_type}</Badge> },
  { key: 'salary',         label: 'Salary', render: (r) => `₹${Number(r.salary).toLocaleString('en-IN')}` },
  { key: 'join_date',      label: 'Joined', render: (r) => r.join_date?.slice(0, 10) },
  { key: 'status', label: 'Status', render: (r) => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
  { key: 'manager_name',   label: 'Manager' },
  { key: '_edit', label: '', render: (r) => <Btn variant="secondary" onClick={() => onEdit(r)}>Edit</Btn> },
];

const emptyEmp = { branch_id: '', dept_id: '', manager_id: '', full_name: '', designation: '', employment_type: 'permanent', join_date: '', salary: '' };

export default function EmployeesPage() {
  const { data, loading, error, refetch } = useApi(getEmployees);
  const branches = useApi(getBranches);
  const depts = useApi(getDepartments);
  const [modal, setModal] = useState(null);
  const [form, setForm] = useState(emptyEmp);
  const [saving, setSaving] = useState(false);

  const openAdd = () => { setForm(emptyEmp); setModal('add'); };
  const openEdit = (r) => { setForm({ ...r }); setModal(r); };
  const close = () => setModal(null);
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }));

  const save = async () => {
    setSaving(true);
    try {
      if (modal === 'add') await createEmployee(form);
      else await updateEmployee(form.emp_id, form);
      close(); refetch();
    } catch { }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;

  return (
    <>
      <PageHeader title="Employees" subtitle={`${data.length} records`}>
        <Btn onClick={openAdd}>+ Add Employee</Btn>
      </PageHeader>
      <Card><DataTable columns={columns(openEdit)} rows={data} /></Card>

      <Modal open={!!modal} onClose={close} title={modal === 'add' ? 'Add Employee' : 'Edit Employee'}>
        <div className="grid grid-cols-2 gap-3">
          <FormField label="Full Name"><Input value={form.full_name} onChange={e => set('full_name', e.target.value)} /></FormField>
          <FormField label="Designation"><Input value={form.designation} onChange={e => set('designation', e.target.value)} /></FormField>
          <FormField label="Branch">
            <Select value={form.branch_id} onChange={e => set('branch_id', e.target.value)}>
              <option value="">Select</option>
              {(branches.data || []).map(b => <option key={b.branch_id} value={b.branch_id}>{b.branch_name}</option>)}
            </Select>
          </FormField>
          <FormField label="Department">
            <Select value={form.dept_id} onChange={e => set('dept_id', e.target.value)}>
              <option value="">Select</option>
              {(depts.data || []).map(d => <option key={d.dept_id} value={d.dept_id}>{d.dept_name}</option>)}
            </Select>
          </FormField>
          <FormField label="Manager">
            <Select value={form.manager_id || ''} onChange={e => set('manager_id', e.target.value)}>
              <option value="">None</option>
              {(data || []).map(e => <option key={e.emp_id} value={e.emp_id}>{e.full_name}</option>)}
            </Select>
          </FormField>
          <FormField label="Employment Type">
            <Select value={form.employment_type} onChange={e => set('employment_type', e.target.value)}>
              {['permanent','contract','probation','intern'].map(v => <option key={v} value={v}>{v}</option>)}
            </Select>
          </FormField>
          {modal === 'add' && <FormField label="Join Date"><Input type="date" value={form.join_date} onChange={e => set('join_date', e.target.value)} /></FormField>}
          <FormField label="Salary"><Input type="number" value={form.salary} onChange={e => set('salary', e.target.value)} /></FormField>
          {modal !== 'add' && (
            <FormField label="Status">
              <Select value={form.status} onChange={e => set('status', e.target.value)}>
                {['active','resigned','terminated'].map(v => <option key={v} value={v}>{v}</option>)}
              </Select>
            </FormField>
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
