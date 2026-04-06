import { useState } from 'react';
import { useApi } from '../hooks';
import { getVcsBranches, createBranch, checkoutBranch, getActiveBranch } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

export default function VcsBranchesPage() {
  const branches = useApi(getVcsBranches);
  const active   = useApi(getActiveBranch);
  const [form, setForm] = useState({ name: '', description: '' });
  const [msg, setMsg]   = useState('');

  const handleCreate = async (e) => {
    e.preventDefault();
    try {
      const r = await createBranch({ name: form.name, description: form.description });
      setMsg(r.result);
      setForm({ name: '', description: '' });
      branches.refetch();
      active.refetch();
    } catch (err) { setMsg(err.message); }
  };

  const handleCheckout = async (name) => {
    try {
      const r = await checkoutBranch(name);
      setMsg(r.result);
      branches.refetch();
      active.refetch();
    } catch (err) { setMsg(err.message); }
  };

  if (branches.loading) return <Spinner />;
  if (branches.error) return <ErrorBox message={branches.error} onRetry={branches.refetch} />;

  return (
    <>
      <PageHeader title="Branches" subtitle="Manage VCS branches">
        {active.data && <Badge variant="indigo">On: {active.data.branch}</Badge>}
      </PageHeader>

      {msg && <div className="mb-4 p-3 rounded-lg bg-blue-50 text-blue-700 text-sm">{msg}</div>}

      <div className="grid lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 p-0">
          <DataTable
            columns={[
              { key: 'branch_name', label: 'Name', render: (r) => (
                <span className="flex items-center gap-2">
                  {r.is_current === '*' && <span className="w-2 h-2 rounded-full bg-green-500" />}
                  <span className="font-medium">{r.branch_name}</span>
                </span>
              )},
              { key: 'commit_count', label: 'Commits' },
              { key: 'latest_commit', label: 'Latest', render: (r) =>
                <span className="text-xs max-w-xs truncate block">{r.latest_commit}</span> },
              { key: 'actions', label: '', render: (r) =>
                r.is_current !== '*' ? (
                  <button
                    onClick={() => handleCheckout(r.branch_name)}
                    className="text-indigo-600 hover:underline text-xs font-medium"
                  >
                    Checkout
                  </button>
                ) : <Badge variant="green">current</Badge>
              },
            ]}
            rows={branches.data}
          />
        </Card>

        <Card className="p-5">
          <h3 className="font-semibold text-gray-900 mb-3">Create Branch</h3>
          <form onSubmit={handleCreate} className="space-y-3">
            <input
              required
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="Branch name"
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
            <input
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
              placeholder="Description (optional)"
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
            <button type="submit" className="w-full bg-indigo-600 text-white rounded-lg py-2 text-sm font-medium hover:bg-indigo-700 transition-colors">
              Create & Checkout
            </button>
          </form>
        </Card>
      </div>
    </>
  );
}
