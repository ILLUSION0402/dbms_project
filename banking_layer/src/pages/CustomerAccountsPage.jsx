import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getCustomerAccounts } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const statusColor = { active: 'green', frozen: 'amber', dormant: 'amber', closed: 'red' };

const columns = [
  { key: 'account_number', label: 'Account #' },
  { key: 'account_type', label: 'Type', render: r => <Badge variant="blue">{r.account_type}</Badge> },
  { key: 'current_balance', label: 'Balance', render: r => `₹${Number(r.current_balance).toLocaleString('en-IN')}` },
  { key: 'interest_rate', label: 'Interest %' },
  { key: 'min_balance', label: 'Min Balance', render: r => `₹${Number(r.min_balance).toLocaleString('en-IN')}` },
  { key: 'opened_date', label: 'Opened', render: r => r.opened_date?.slice(0, 10) },
  { key: 'status', label: 'Status', render: r => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
];

export default function CustomerAccountsPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getCustomerAccounts(auth.user.customer_id), [auth.user.customer_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="My Accounts" subtitle={`${data.length} account(s)`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
