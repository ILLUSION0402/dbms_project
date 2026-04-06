import { useState } from 'react';
import { useApi } from '../hooks';
import { getVcsLog, getCommitDetail } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';
import { GitCommitHorizontal } from 'lucide-react';

export default function CommitsPage() {
  const [selected, setSelected] = useState(null);
  const log = useApi(() => getVcsLog(null, 100));
  const detail = useApi(() => selected ? getCommitDetail(selected) : Promise.resolve(null), [selected]);

  if (log.loading) return <Spinner />;
  if (log.error) return <ErrorBox message={log.error} onRetry={log.refetch} />;

  return (
    <>
      <PageHeader title="Commit History" subtitle="Full commit log across all branches" />

      <div className="grid lg:grid-cols-5 gap-6">
        <Card className="lg:col-span-3 p-0">
          <DataTable
            columns={[
              { key: 'commit_id', label: 'ID', render: (r) => (
                <button
                  onClick={() => setSelected(r.commit_id)}
                  className={`font-mono font-bold px-2 py-0.5 rounded ${
                    selected === r.commit_id ? 'bg-indigo-100 text-indigo-700' : 'text-indigo-600 hover:underline'
                  }`}
                >
                  #{r.commit_id}
                </button>
              )},
              { key: 'branch', label: 'Branch', render: (r) => <Badge variant="indigo">{r.branch}</Badge> },
              { key: 'hash', label: 'Hash', render: (r) => <span className="font-mono text-xs text-gray-400">{r.hash?.slice(0, 8)}</span> },
              { key: 'message', label: 'Message' },
              { key: 'author', label: 'Author' },
              { key: 'committed_at', label: 'Date', render: (r) => new Date(r.committed_at).toLocaleString() },
            ]}
            rows={log.data}
          />
        </Card>

        <Card className="lg:col-span-2 p-5">
          <h3 className="font-semibold text-gray-900 mb-3 flex items-center gap-2">
            <GitCommitHorizontal size={18} />
            Commit Details
          </h3>
          {!selected && <p className="text-gray-400 text-sm">Click a commit to see details</p>}
          {selected && detail.loading && <Spinner />}
          {selected && detail.error && <ErrorBox message={detail.error} />}
          {selected && detail.data && (
            <div className="space-y-2 text-sm max-h-[60vh] overflow-y-auto">
              {detail.data.map((row, i) => (
                <div key={i} className="bg-gray-50 rounded-lg p-3 border border-gray-100">
                  <div className="flex justify-between">
                    <Badge variant={row.operation === 'INSERT' ? 'green' : row.operation === 'DELETE' ? 'red' : 'amber'}>
                      {row.operation}
                    </Badge>
                    <span className="text-xs text-gray-400">{row.table_name} / {row.row_pk}</span>
                  </div>
                  {row.field && (
                    <div className="mt-2 grid grid-cols-3 gap-1 text-xs">
                      <span className="font-medium text-gray-600">{row.field}</span>
                      <span className="text-red-500 line-through">{row.old_value ?? '—'}</span>
                      <span className="text-green-600">{row.new_value ?? '—'}</span>
                    </div>
                  )}
                </div>
              ))}
              {detail.data.length === 0 && <p className="text-gray-400">No changes in this commit</p>}
            </div>
          )}
        </Card>
      </div>
    </>
  );
}
