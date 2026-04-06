import { useState } from 'react';
import { getVcsDiff, getVcsDiffBranch } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

export default function DiffPage() {
  const [mode, setMode] = useState('commit');
  const [from, setFrom] = useState('');
  const [to, setTo]     = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleDiff = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const data = mode === 'commit'
        ? await getVcsDiff(from, to)
        : await getVcsDiffBranch(from, to);
      setResult(data);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  };

  return (
    <>
      <PageHeader title="Diff" subtitle="Compare commits or branches" />

      <Card className="p-5 mb-6">
        <div className="flex gap-4 mb-4">
          <button
            onClick={() => { setMode('commit'); setResult(null); }}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              mode === 'commit' ? 'bg-indigo-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
          >
            Commit Diff
          </button>
          <button
            onClick={() => { setMode('branch'); setResult(null); }}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              mode === 'branch' ? 'bg-indigo-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
          >
            Branch Diff
          </button>
        </div>

        <form onSubmit={handleDiff} className="flex gap-3 items-end">
          <div className="flex-1">
            <label className="block text-xs font-medium text-gray-500 mb-1">
              {mode === 'commit' ? 'From Commit ID' : 'Branch A'}
            </label>
            <input
              required value={from} onChange={(e) => setFrom(e.target.value)}
              placeholder={mode === 'commit' ? 'e.g. 2' : 'e.g. main'}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
          </div>
          <div className="flex-1">
            <label className="block text-xs font-medium text-gray-500 mb-1">
              {mode === 'commit' ? 'To Commit ID' : 'Branch B'}
            </label>
            <input
              required value={to} onChange={(e) => setTo(e.target.value)}
              placeholder={mode === 'commit' ? 'e.g. 5' : 'e.g. feature/x'}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
          </div>
          <button type="submit" className="bg-indigo-600 text-white rounded-lg px-5 py-2 text-sm font-medium hover:bg-indigo-700 transition-colors shrink-0">
            Compare
          </button>
        </form>
      </Card>

      {loading && <Spinner />}
      {error && <ErrorBox message={error} />}
      {result && (
        <Card className="p-0">
          <DataTable
            columns={[
              { key: 'table_name', label: 'Table', render: (r) => <Badge variant="blue">{r.table_name}</Badge> },
              { key: 'row_pk', label: 'Row PK' },
              { key: 'operation', label: 'Op', render: (r) =>
                <Badge variant={r.operation === 'INSERT' ? 'green' : r.operation === 'DELETE' ? 'red' : 'amber'}>
                  {r.operation}
                </Badge> },
              { key: 'changed_columns', label: 'Changed', render: (r) =>
                r.changed_columns ? r.changed_columns.join(', ') : '—' },
              { key: 'branch', label: 'Branch', render: (r) => r.branch ? <Badge variant="indigo">{r.branch}</Badge> : '—' },
            ]}
            rows={result}
            emptyMsg="No differences found"
          />
        </Card>
      )}
    </>
  );
}
