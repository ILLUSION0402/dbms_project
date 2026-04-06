import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getEmployeeLoans } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const statusColor = { submitted: 'amber', under_review: 'blue', approved: 'green', rejected: 'red', disbursed: 'green', withdrawn: 'red' };

const columns = [
  { key: 'loan_id', label: 'Loan ID' },
  { key: 'customer_name', label: 'Customer' },
  { key: 'loan_type', label: 'Type', render: r => <Badge variant="purple">{r.loan_type}</Badge> },
  { key: 'applied_amount', label: 'Applied', render: r => `₹${Number(r.applied_amount).toLocaleString('en-IN')}` },
  { key: 'sanctioned_amount', label: 'Sanctioned', render: r => r.sanctioned_amount ? `₹${Number(r.sanctioned_amount).toLocaleString('en-IN')}` : '—' },
  { key: 'base_interest_rate', label: 'Base Rate %' },
  { key: 'application_status', label: 'App Status', render: r => <Badge variant={statusColor[r.application_status]}>{r.application_status}</Badge> },
  { key: 'purpose', label: 'Purpose' },
];

export default function MyLoansPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getEmployeeLoans(auth.user.emp_id), [auth.user.emp_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="Managed Loans" subtitle={`${data.length} loan(s) assigned to you`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
