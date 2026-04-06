import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getEmployeeProfile, getEmployeeHrHistory } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

function Field({ label, value }) {
  return (
    <div>
      <p className="text-xs text-gray-400 uppercase tracking-wider">{label}</p>
      <p className="text-sm font-medium text-gray-900 mt-0.5">{value || '—'}</p>
    </div>
  );
}

export default function EmployeeProfilePage() {
  const { auth } = useAuth();
  const eid = auth.user.emp_id;
  const profile = useApi(() => getEmployeeProfile(eid), [eid]);
  const hr      = useApi(() => getEmployeeHrHistory(eid), [eid]);

  if (profile.loading) return <Spinner />;
  if (profile.error) return <ErrorBox message={profile.error} onRetry={profile.refetch} />;

  const u = profile.data;
  const statusColor = { active: 'green', resigned: 'red', terminated: 'red' };

  return (
    <>
      <PageHeader title="My Profile" subtitle="Employment details & HR history" />

      <Card className="p-6 mb-6">
        <h3 className="font-semibold text-gray-900 mb-4">Employee Details</h3>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
          <Field label="Name" value={u.full_name} />
          <Field label="Designation" value={u.designation} />
          <Field label="Department" value={u.dept_name} />
          <Field label="Branch" value={u.branch_name} />
          <Field label="Employment Type" value={u.employment_type} />
          <Field label="Join Date" value={u.join_date?.slice(0, 10)} />
          <Field label="Salary" value={`₹${Number(u.salary).toLocaleString('en-IN')}`} />
          <div>
            <p className="text-xs text-gray-400 uppercase tracking-wider">Status</p>
            <Badge variant={statusColor[u.status]}>{u.status}</Badge>
          </div>
          <Field label="Manager" value={u.manager_name} />
        </div>
      </Card>

      <Card className="p-6">
        <h3 className="font-semibold text-gray-900 mb-4">HR Update History</h3>
        {hr.loading ? <Spinner /> :
         hr.error ? <ErrorBox message={hr.error} /> :
          <DataTable columns={[
            { key: 'update_type', label: 'Type', render: r => <Badge variant="indigo">{r.update_type?.replace(/_/g, ' ')}</Badge> },
            { key: 'effective_date', label: 'Effective', render: r => r.effective_date?.slice(0, 10) },
            { key: 'old_designation', label: 'Old Designation' },
            { key: 'new_designation', label: 'New Designation' },
            { key: 'old_salary', label: 'Old Salary', render: r => r.old_salary ? `₹${Number(r.old_salary).toLocaleString('en-IN')}` : '—' },
            { key: 'new_salary', label: 'New Salary', render: r => r.new_salary ? `₹${Number(r.new_salary).toLocaleString('en-IN')}` : '—' },
            { key: 'reason', label: 'Reason' },
          ]} rows={hr.data} emptyMsg="No HR updates found" />}
      </Card>
    </>
  );
}
