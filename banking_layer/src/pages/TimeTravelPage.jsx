import { useState } from 'react';
import { getTimeTravel, getRepositories, getVcsTags, getVcsLog } from '../api';
import { useApi } from '../hooks';
import { PageHeader, Card, Spinner, ErrorBox, Badge } from '../components/UI';

export default function TimeTravelPage() {
  const repos = useApi(getRepositories);
  const tags  = useApi(getVcsTags);
  const log   = useApi(() => getVcsLog(null, 50));
  const [table, setTable]     = useState('');
  const [commitId, setCommitId] = useState('');
  const [result, setResult]   = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState(null);

  const handleQuery = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const data = await getTimeTravel(table, commitId);
      setResult(data);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  };

  return (
    <>
      <PageHeader title="Time Travel" subtitle="Reconstruct table state at any commit" />

      <Card className="p-5 mb-6">
        <form onSubmit={handleQuery} className="flex gap-3 items-end flex-wrap">
          <div className="flex-1 min-w-[180px]">
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
          <div className="flex-1 min-w-[180px]">
            <label className="block text-xs font-medium text-gray-500 mb-1">At Commit</label>
            <input
              required value={commitId}
              onChange={(e) => setCommitId(e.target.value)}
              placeholder="Commit ID"
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
            />
          </div>
          <button type="submit" className="bg-indigo-600 text-white rounded-lg px-5 py-2 text-sm font-medium hover:bg-indigo-700 transition-colors shrink-0">
            Time Travel
          </button>
        </form>

        {/* Quick pick from tags */}
        {tags.data && tags.data.length > 0 && (
          <div className="mt-3 flex gap-2 flex-wrap items-center">
            <span className="text-xs text-gray-400">Quick pick:</span>
            {tags.data.map((t) => (
              <button key={t.tag_name} onClick={() => setCommitId(String(t.commit_id))}
                className="text-xs px-2 py-1 rounded bg-amber-50 text-amber-700 hover:bg-amber-100 transition-colors">
                {t.tag_name} (#{t.commit_id})
              </button>
            ))}
          </div>
        )}
      </Card>

      {loading && <Spinner />}
      {error && <ErrorBox message={error} />}
      {result && (
        <Card className="p-0 overflow-x-auto">
          {result.length === 0 ? (
            <p className="text-center py-10 text-gray-400 text-sm">No rows reconstructed at this commit</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  {Object.keys(result[0]).map((k) => (
                    <th key={k} className="text-left px-4 py-3 font-semibold text-gray-500 text-xs uppercase tracking-wider whitespace-nowrap">
                      {k}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.map((row, i) => (
                  <tr key={i} className="border-b border-gray-100 hover:bg-gray-50">
                    {Object.values(row).map((v, j) => (
                      <td key={j} className="px-4 py-3 text-gray-700 whitespace-nowrap">
                        {v !== null ? String(v) : '—'}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      )}
    </>
  );
}
