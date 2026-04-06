import { useApi } from '../hooks';
import { getBranches } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

const columns = [
  { key: 'branch_id',        label: 'ID' },
  { key: 'branch_name',      label: 'Name' },
  { key: 'ifsc_code',        label: 'IFSC' },
  { key: 'city',             label: 'City' },
  { key: 'state',            label: 'State' },
  { key: 'phone',            label: 'Phone' },
  { key: 'established_date', label: 'Established', render: (r) => r.established_date?.slice(0, 10) },
  { key: 'status',           label: 'Status', render: (r) => <Badge variant={r.status === 'active' ? 'green' : 'red'}>{r.status}</Badge> },
];

export default function BranchesPage() {
  const { data, loading, error, refetch } = useApi(getBranches);
  if (loading) return <Spinner />;
  if (error)   return <ErrorBox message={error} onRetry={refetch} />;
  return (
    <>
      <PageHeader title="Branches" subtitle={`${data.length} records`} />
      <Card><DataTable columns={columns} rows={data} /></Card>
    </>
  );
}
