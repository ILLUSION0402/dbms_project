import { useState } from 'react';
import { getBlame, getRowHistory, getRepositories } from '../api';
import { useApi } from '../hooks';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

export default function BlamePage() {
  const repos = useApi(getRepositories);
  const [table, setTable]   = useState('');
  const [pk, setPk]         = useState('');
  const [mode, setMode]     = useState('blame');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]   = useState(null);

  const handleQuery = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const data = mode === 'blame'
        ? await getBlame(table)
        : await getRowHistory(table, pk);
      setResult(data);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  };

  return (
    <>
      <PageHeader title="Blame & Row History" subtitle="Who changed what, and full row audit trail" />

      <Card className="p-5 mb-6">
        <div className="flex gap-4 mb-4">
          <button
            onClick={() => { setMode('blame'); setResult(null); }}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              mode === 'blame' ? 'bg-indigo-600 text-white' : 'bg-gray-100 text-gray-600'}`}
          >
            Blame (Who changed each row)
          </button>
          <button
            onClick={() => { setMode('history'); setResult(null); }}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              mode === 'history' ? 'bg-indigo-600 text-white' : 'bg-gray-100 text-gray-600'}`}
          >
            Row History (Single row audit)
          </button>
        </div>

        <form onSubmit={handleQuery} className="flex gap-3 items-end">
          <div className="flex-1">
            <label className="block text-xs font-medium text-gray-500 mb-1">Table</label>
            <select
              required value={table} onChange={(e) => setTable(e.target.value)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
            >
              <option value="">Select table...</option>
              {repos.data?.map((r) => (
                <option key={r.table_name} value={r.table_name}>{r.table_name}</option>
              ))}
            </select>
          </div>
          {mode === 'history' && (
            <div className="flex-1">
              <label className="block text-xs font-medium text-gray-500 mb-1">Primary Key</label>
              <input
                required value={pk} onChange={(e) => setPk(e.target.value)}
                placeholder="e.g. 1"
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
              />
            </div>
          )}
          <button type="submit" className="bg-indigo-600 text-white rounded-lg px-5 py-2 text-sm font-medium hover:bg-indigo-700 transition-colors shrink-0">
            Query
          </button>
        </form>
      </Card>

      {loading && <Spinner />}
      {error && <ErrorBox message={error} />}
      {result && mode === 'blame' && (
        <Card className="p-0">
          <DataTable
            columns={[
              { key: 'row_pk', label: 'Row PK' },
              { key: 'last_operation', label: 'Last Op', render: (r) =>
                <Badge variant={r.last_operation === 'INSERT' ? 'green' : r.last_operation === 'DELETE' ? 'red' : 'amber'}>
                  {r.last_operation}
                </Badge> },
              { key: 'last_author', label: 'Author' },
              { key: 'last_commit_id', label: 'Commit #' },
              { key: 'last_message', label: 'Message' },
              { key: 'last_changed_at', label: 'When', render: (r) => new Date(r.last_changed_at).toLocaleString() },
            ]}
            rows={result}
            emptyMsg="No blame data"
          />
        </Card>
      )}
      {result && mode === 'history' && (
        <Card className="p-0">
          <DataTable
            columns={[
              { key: 'commit_id', label: 'Commit #' },
              { key: 'operation', label: 'Op', render: (r) =>
                <Badge variant={r.operation === 'INSERT' ? 'green' : r.operation === 'DELETE' ? 'red' : 'amber'}>
                  {r.operation}
                </Badge> },
              { key: 'author', label: 'Author' },
              { key: 'message', label: 'Message' },
              { key: 'changed_columns', label: 'Changed', render: (r) => r.changed_columns ? r.changed_columns.join(', ') : '—' },
              { key: 'changed_at', label: 'When', render: (r) => new Date(r.changed_at).toLocaleString() },
            ]}
            rows={result}
            emptyMsg="No history for this row"
          />
        </Card>
      )}
    </>
  );
}
