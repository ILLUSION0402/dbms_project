import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getEmployeeAccounts } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const statusColor = { active: 'green', frozen: 'amber', dormant: 'amber', closed: 'red' };

const columns = [
  { key: 'account_id', label: 'ID' },
  { key: 'account_number', label: 'Account #' },
  { key: 'customer_name', label: 'Customer' },
  { key: 'account_type', label: 'Type', render: r => <Badge variant="blue">{r.account_type}</Badge> },
  { key: 'current_balance', label: 'Balance', render: r => `₹${Number(r.current_balance).toLocaleString('en-IN')}` },
  { key: 'interest_rate', label: 'Rate %' },
  { key: 'opened_date', label: 'Opened', render: r => r.opened_date?.slice(0, 10) },
  { key: 'status', label: 'Status', render: r => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
];

export default function MyAccountsPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getEmployeeAccounts(auth.user.emp_id), [auth.user.emp_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="Customer Accounts" subtitle={`${data.length} account(s) under your customers`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
