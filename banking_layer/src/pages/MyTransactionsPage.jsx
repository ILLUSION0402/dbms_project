import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getEmployeeTransactions } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const columns = [
  { key: 'txn_id', label: 'Txn ID' },
  { key: 'account_number', label: 'Account #' },
  { key: 'customer_name', label: 'Customer' },
  { key: 'txn_type', label: 'Type', render: r => <Badge variant={r.txn_type === 'credit' ? 'green' : 'red'}>{r.txn_type}</Badge> },
  { key: 'channel', label: 'Channel', render: r => <Badge variant="blue">{r.channel}</Badge> },
  { key: 'amount', label: 'Amount', render: r => `₹${Number(r.amount).toLocaleString('en-IN')}` },
  { key: 'balance_after', label: 'Balance After', render: r => `₹${Number(r.balance_after).toLocaleString('en-IN')}` },
  { key: 'txn_date', label: 'Date', render: r => r.txn_date?.slice(0, 10) },
];

export default function MyTransactionsPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getEmployeeTransactions(auth.user.emp_id), [auth.user.emp_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="Customer Transactions" subtitle={`${data.length} transaction(s) from your customers`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
