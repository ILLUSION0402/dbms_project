import { useState } from 'react';
import { rollback, getVcsLog } from '../api';
import { useApi } from '../hooks';
import { PageHeader, Card, Badge, Spinner, ErrorBox } from '../components/UI';
import { AlertTriangle } from 'lucide-react';

export default function RollbackPage() {
  const log = useApi(() => getVcsLog(null, 30));
  const [commitId, setCommitId] = useState('');
  const [dryRun, setDryRun]     = useState(true);
  const [result, setResult]     = useState(null);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState(null);

  const handleRollback = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const data = await rollback({ commit_id: parseInt(commitId), dry_run: dryRun });
      setResult(data.result);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  };

  return (
    <>
      <PageHeader title="Rollback" subtitle="Revert data to a previous commit state" />

      <div className="grid lg:grid-cols-3 gap-6">
        <Card className="p-5 lg:col-span-1">
          <h3 className="font-semibold text-gray-900 mb-3 flex items-center gap-2">
            <AlertTriangle size={18} className="text-amber-500" />
            Rollback
          </h3>
          <form onSubmit={handleRollback} className="space-y-4">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Target Commit ID</label>
              <input
                required value={commitId}
                onChange={(e) => setCommitId(e.target.value)}
                placeholder="e.g. 3"
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 outline-none"
              />
            </div>
            <div className="flex items-center gap-2">
              <input
                type="checkbox" id="dryRun" checked={dryRun}
                onChange={(e) => setDryRun(e.target.checked)}
                className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
              />
              <label htmlFor="dryRun" className="text-sm text-gray-600">Dry run (preview only)</label>
            </div>
            <button
              type="submit"
              className={`w-full rounded-lg py-2 text-sm font-medium transition-colors ${
                dryRun
                  ? 'bg-amber-500 text-white hover:bg-amber-600'
                  : 'bg-red-600 text-white hover:bg-red-700'
              }`}
            >
              {dryRun ? 'Preview Rollback' : '⚠️ Execute Rollback'}
            </button>
          </form>

          {loading && <Spinner />}
          {error && <div className="mt-3"><ErrorBox message={error} /></div>}
          {result && (
            <div className="mt-4 p-3 rounded-lg bg-green-50 border border-green-200 text-green-700 text-sm">
              {result}
            </div>
          )}
        </Card>

        <Card className="p-5 lg:col-span-2">
          <h3 className="font-semibold text-gray-900 mb-3">Recent Commits (pick a target)</h3>
          {log.loading ? <Spinner /> : log.error ? <ErrorBox message={log.error} /> : (
            <div className="space-y-2 max-h-[60vh] overflow-y-auto">
              {log.data?.map((c) => (
                <button
                  key={c.commit_id}
                  onClick={() => setCommitId(String(c.commit_id))}
                  className={`w-full text-left p-3 rounded-lg border transition-colors ${
                    commitId === String(c.commit_id)
                      ? 'bg-indigo-50 border-indigo-300'
                      : 'bg-gray-50 border-gray-100 hover:bg-gray-100'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-mono font-bold text-indigo-600">#{c.commit_id}</span>
                    <Badge variant="indigo">{c.branch}</Badge>
                  </div>
                  <p className="text-sm text-gray-700 mt-1">{c.message}</p>
                  <p className="text-xs text-gray-400 mt-1">{c.author} · {new Date(c.committed_at).toLocaleString()}</p>
                </button>
              ))}
            </div>
          )}
        </Card>
      </div>
    </>
  );
}
