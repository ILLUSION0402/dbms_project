import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getCustomerLoans } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const statusColor = { submitted: 'amber', under_review: 'blue', approved: 'green', rejected: 'red', disbursed: 'green', withdrawn: 'red' };
const loanStatusColor = { pending: 'amber', active: 'green', closed: 'default', npa: 'red', written_off: 'red', foreclosed: 'red' };

const columns = [
  { key: 'loan_id', label: 'Loan ID' },
  { key: 'loan_type', label: 'Type', render: r => <Badge variant="purple">{r.loan_type}</Badge> },
  { key: 'applied_amount', label: 'Applied', render: r => `₹${Number(r.applied_amount).toLocaleString('en-IN')}` },
  { key: 'sanctioned_amount', label: 'Sanctioned', render: r => r.sanctioned_amount ? `₹${Number(r.sanctioned_amount).toLocaleString('en-IN')}` : '—' },
  { key: 'interest_rate', label: 'Rate %', render: r => r.interest_rate || r.base_interest_rate },
  { key: 'tenure_months', label: 'Tenure (mo)' },
  { key: 'emi_amount', label: 'EMI', render: r => r.emi_amount ? `₹${Number(r.emi_amount).toLocaleString('en-IN')}` : '—' },
  { key: 'application_status', label: 'App Status', render: r => <Badge variant={statusColor[r.application_status]}>{r.application_status}</Badge> },
  { key: 'status', label: 'Status', render: r => <Badge variant={loanStatusColor[r.status]}>{r.status}</Badge> },
  { key: 'outstanding_principal', label: 'Outstanding', render: r => r.outstanding_principal ? `₹${Number(r.outstanding_principal).toLocaleString('en-IN')}` : '—' },
  { key: 'collateral_type', label: 'Collateral' },
];

export default function CustomerLoansPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(() => getCustomerLoans(auth.user.customer_id), [auth.user.customer_id]);
  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="My Loans" subtitle={`${data.length} loan(s)`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
