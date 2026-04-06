import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getEmployeeCustomers } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const statusColor = { active: 'green', dormant: 'amber', closed: 'red', blocked: 'red' };

const columns = [
  { key: 'customer_id', label: 'ID' },
  { key: 'full_name', label: 'Name' },
  { key: 'phone', label: 'Phone' },
  { key: 'email', label: 'Email' },
  { key: 'occupation', label: 'Occupation' },
  { key: 'income_bracket', label: 'Income', render: r => r.income_bracket?.replace(/_/g, ' ') || '—' },
  { key: 'kyc_status', label: 'KYC', render: r => <Badge variant={r.kyc_status === 'verified' ? 'green' : 'amber'}>{r.kyc_status}</Badge> },
  { key: 'status', label: 'Status', render: r => <Badge variant={statusColor[r.status]}>{r.status}</Badge> },
  { key: 'customer_since', label: 'Since', render: r => r.customer_since?.slice(0, 10) },
];

export default function MyCustomersPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getEmployeeCustomers(auth.user.emp_id), [auth.user.emp_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="My Assigned Customers" subtitle={`${data.length} customer(s) assigned to you`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
