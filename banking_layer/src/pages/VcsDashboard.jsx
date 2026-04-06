import { useApi } from '../hooks';
import { getVcsStats, getVcsLog, getActiveBranch, getRepositories } from '../api';
import { PageHeader, Card, StatCard, DataTable, Spinner, ErrorBox, Badge } from '../components/UI';
import { GitCommitHorizontal, GitBranch, Tag, Database, FileText, Clock } from 'lucide-react';

export default function VcsDashboard() {
  const stats  = useApi(getVcsStats);
  const branch = useApi(getActiveBranch);
  const log    = useApi(() => getVcsLog(null, 8));
  const repos  = useApi(getRepositories);

  if (stats.loading) return <Spinner />;
  if (stats.error) return <ErrorBox message={stats.error} onRetry={stats.refetch} />;

  const v = stats.data || {};

  return (
    <>
      <PageHeader title="Version Control Dashboard" subtitle="Git-like database versioning overview">
        {branch.data && (
          <Badge variant="indigo">
            <GitBranch size={14} className="mr-1 inline" />
            {branch.data.branch}
          </Badge>
        )}
      </PageHeader>

      <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4 mb-8">
        <StatCard label="Commits"         value={v.total_commits}   icon={GitCommitHorizontal} color="violet" />
        <StatCard label="Changes"         value={v.total_changes}   icon={FileText}            color="indigo" />
        <StatCard label="Branches"        value={v.active_branches} icon={GitBranch}           color="green" />
        <StatCard label="Tags"            value={v.total_tags}      icon={Tag}                 color="amber" />
        <StatCard label="Tracked Tables"  value={v.tracked_tables}  icon={Database}            color="sky" />
        <StatCard label="Staged"          value={v.staged_changes}  icon={Clock}               color="rose" />
      </div>

      <div className="grid lg:grid-cols-2 gap-6">
        <Card className="p-5">
          <h3 className="font-semibold text-gray-900 mb-3">Recent Commits</h3>
          {log.loading ? <Spinner /> : log.error ? <ErrorBox message={log.error} /> : (
            <DataTable
              columns={[
                { key: 'commit_id', label: 'ID' },
                { key: 'branch', label: 'Branch', render: (r) => <Badge variant="indigo">{r.branch}</Badge> },
                { key: 'message', label: 'Message' },
                { key: 'author', label: 'Author' },
                { key: 'committed_at', label: 'Date', render: (r) => new Date(r.committed_at).toLocaleString() },
              ]}
              rows={log.data}
            />
          )}
        </Card>

        <Card className="p-5">
          <h3 className="font-semibold text-gray-900 mb-3">Tracked Tables</h3>
          {repos.loading ? <Spinner /> : repos.error ? <ErrorBox message={repos.error} /> : (
            <DataTable
              columns={[
                { key: 'table_name', label: 'Table', render: (r) => <Badge variant="blue">{r.table_name}</Badge> },
                { key: 'primary_key_column', label: 'PK Column' },
                { key: 'tracked_since', label: 'Since', render: (r) => new Date(r.tracked_since).toLocaleDateString() },
                { key: 'is_active', label: 'Active', render: (r) => r.is_active ? '✓' : '✗' },
              ]}
              rows={repos.data}
            />
          )}
        </Card>
      </div>
    </>
  );
}
