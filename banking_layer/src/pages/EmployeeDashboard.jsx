import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getEmployeeStats, getEmployeeCustomers, getEmployeeLoans, getEmployeeQueue } from '../api';
import { PageHeader, Card, StatCard, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';
import { Users, Landmark, CreditCard, TrendingUp, FileText } from 'lucide-react';

export default function EmployeeDashboard() {
  const { auth } = useAuth();
  const eid = auth.user.emp_id;

  const stats     = useApi(() => getEmployeeStats(eid), [eid]);
  const customers = useApi(() => getEmployeeCustomers(eid), [eid]);
  const loans     = useApi(() => getEmployeeLoans(eid), [eid]);
  const queue     = useApi(() => getEmployeeQueue(eid), [eid]);

  if (stats.loading) return <Spinner />;

  const s = stats.data || {};

  return (
    <>
      <PageHeader title={`Welcome, ${auth.user.full_name}`} subtitle={`${auth.user.designation} — ${auth.user.dept_name}`} />

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 mb-8">
        <StatCard label="Assigned Customers" value={s.assigned_customers} icon={Users} color="indigo" />
        <StatCard label="Customer Accounts" value={s.customer_accounts} icon={CreditCard} color="green" />
        <StatCard label="Managed Loans" value={s.managed_loans} icon={Landmark} color="amber" />
        <StatCard label="Pending Apps" value={s.pending_applications} icon={FileText} color="red" />
        <StatCard label="Total Deposits" value={`₹${Number(s.total_deposits || 0).toLocaleString('en-IN')}`} icon={TrendingUp} color="sky" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Assigned Customers</h3>
          {customers.loading ? <Spinner /> :
           customers.error ? <ErrorBox message={customers.error} /> :
            <DataTable columns={[
              { key: 'customer_id', label: 'ID' },
              { key: 'full_name', label: 'Name' },
              { key: 'phone', label: 'Phone' },
              { key: 'kyc_status', label: 'KYC', render: r => <Badge variant={r.kyc_status === 'verified' ? 'green' : 'amber'}>{r.kyc_status}</Badge> },
              { key: 'status', label: 'Status', render: r => <Badge variant={r.status === 'active' ? 'green' : 'amber'}>{r.status}</Badge> },
            ]} rows={customers.data} />}
        </Card>

        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Managed Loans</h3>
          {loans.loading ? <Spinner /> :
           loans.error ? <ErrorBox message={loans.error} /> :
            <DataTable columns={[
              { key: 'loan_id', label: 'ID' },
              { key: 'customer_name', label: 'Customer' },
              { key: 'loan_type', label: 'Type', render: r => <Badge variant="purple">{r.loan_type}</Badge> },
              { key: 'applied_amount', label: 'Amount', render: r => `₹${Number(r.applied_amount).toLocaleString('en-IN')}` },
              { key: 'application_status', label: 'Status', render: r => <Badge variant={r.application_status === 'disbursed' ? 'green' : 'amber'}>{r.application_status}</Badge> },
            ]} rows={loans.data} />}
        </Card>
      </div>

      {queue.data?.length > 0 && (
        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Loan Application Queue</h3>
          <DataTable columns={[
            { key: 'app_id', label: 'App ID' },
            { key: 'customer_name', label: 'Customer' },
            { key: 'amount', label: 'Amount', render: r => `₹${Number(r.amount).toLocaleString('en-IN')}` },
            { key: 'app_status', label: 'Status', render: r => <Badge variant={r.app_status === 'submitted' ? 'amber' : 'blue'}>{r.app_status}</Badge> },
            { key: 'days_waiting', label: 'Days Waiting', render: r => <span className={r.days_waiting > 7 ? 'text-red-600 font-semibold' : ''}>{r.days_waiting}d</span> },
          ]} rows={queue.data} />
        </Card>
      )}
    </>
  );
}
