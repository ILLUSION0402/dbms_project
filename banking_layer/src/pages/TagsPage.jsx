import { useState } from 'react';
import { useApi } from '../hooks';
import { getVcsTags, createTag } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox } from '../components/UI';

export default function TagsPage() {
  const tags = useApi(getVcsTags);
  const [form, setForm] = useState({ name: '', message: '' });
  const [msg, setMsg]   = useState('');

  const handleCreate = async (e) => {
    e.preventDefault();
    try {
      const r = await createTag({ name: form.name, message: form.message });
      setMsg(r.result);
      setForm({ name: '', message: '' });
      tags.refetch();
    } catch (err) { setMsg(err.message); }
  };

  if (tags.loading) return <Spinner />;
  if (tags.error)   return <ErrorBox message={tags.error} onRetry={tags.refetch} />;

  return (
    <>
      <PageHeader title="Tags" subtitle="Immutable named references to commits" />
      {msg && <div className="mb-4 p-3 rounded-lg bg-blue-50 text-blue-700 text-sm">{msg}</div>}

      <div className="grid lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2 p-0">
          <DataTable
            columns={[
              { key: 'tag_name', label: 'Tag', render: (r) => <Badge variant="amber">{r.tag_name}</Badge> },
              { key: 'commit_id', label: 'Commit #' },
              { key: 'branch', label: 'Branch', render: (r) => <Badge variant="indigo">{r.branch}</Badge> },
              { key: 'message', label: 'Message' },
              { key: 'created_by', label: 'By' },
              { key: 'created_at', label: 'Date', render: (r) => new Date(r.created_at).toLocaleString() },
            ]}
            rows={tags.data}
          />
        </Card>

        <Card className="p-5">
          <h3 className="font-semibold text-gray-900 mb-3">Create Tag</h3>
          <form onSubmit={handleCreate} className="space-y-3">
            <input
              required
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="Tag name (e.g. v1.0)"
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
            <input
              value={form.message}
              onChange={(e) => setForm({ ...form, message: e.target.value })}
              placeholder="Message (optional)"
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none"
            />
            <button type="submit" className="w-full bg-indigo-600 text-white rounded-lg py-2 text-sm font-medium hover:bg-indigo-700 transition-colors">
              Create Tag on HEAD
            </button>
          </form>
        </Card>
      </div>
    </>
  );
}
