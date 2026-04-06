import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getCustomerTransfers } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const statusColor = { pending: 'amber', processing: 'blue', completed: 'green', failed: 'red', reversed: 'red' };

const columns = [
  { key: 'transfer_id', label: 'ID' },
  { key: 'from_account_number', label: 'From Acct' },
  { key: 'to_account_number', label: 'To Acct', render: r => r.to_account_number_internal || r.to_account_number || '—' },
  { key: 'to_ifsc', label: 'To IFSC' },
  { key: 'transfer_mode', label: 'Mode', render: r => <Badge variant="blue">{r.transfer_mode?.toUpperCase()}</Badge> },
  { key: 'amount', label: 'Amount', render: r => `₹${Number(r.amount).toLocaleString('en-IN')}` },
  { key: 'status', label: 'Status', render: r => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
  { key: 'initiated_at', label: 'Date', render: r => r.initiated_at?.slice(0, 10) },
  { key: 'remarks', label: 'Remarks' },
];

export default function CustomerTransfersPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getCustomerTransfers(auth.user.customer_id), [auth.user.customer_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="My Transfers" subtitle={`${data.length} transfer(s)`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
