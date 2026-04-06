import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { useApi } from '../hooks';
import { getLoanPipeline, bankReviewLoan } from '../api';
import { PageHeader, Card, DataTable, Badge, Spinner, ErrorBox, Modal, FormField, Input, Select, Btn } from '../components/UI';

const statusColor = { submitted: 'amber', under_review: 'blue', approved: 'green', rejected: 'red' };

const columns = [
  { key: 'application_id',   label: 'ID' },
  { key: 'customer_name',    label: 'Customer' },
  { key: 'requested_amount', label: 'Amount', render: r => `₹${Number(r.requested_amount).toLocaleString('en-IN')}` },
  { key: 'purpose',          label: 'Purpose' },
  { key: 'status_label',     label: 'Status', render: r => <Badge variant={statusColor[r.status] || 'default'}>{r.status_label || r.status}</Badge> },
  { key: 'officer_name',     label: 'Officer' },
  { key: 'days_open',        label: 'Days Open', render: r => <span className={r.days_open > 7 ? 'text-red-600 font-semibold' : ''}>{r.days_open}d</span> },
  { key: 'application_date', label: 'Applied', render: r => r.application_date?.slice(0, 10) },
  { key: 'reviewed_at',      label: 'Reviewed', render: r => r.reviewed_at ? new Date(r.reviewed_at).toLocaleString() : '—' },
];

export default function LoanApplicationsPage() {
  const { auth } = useAuth();
  const { data, loading, error, refetch } = useApi(getLoanPipeline);
  const [modal, setModal] = useState(false);
  const [selected, setSelected] = useState(null);
  const [reviewForm, setReviewForm] = useState({ status: 'approved', notes: '' });
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState(null);

  const openReview = (row) => {
    setSelected(row);
    setReviewForm({ status: 'approved', notes: '' });
    setMsg(null);
    setModal(true);
  };

  const review = async () => {
    setSaving(true);
    setMsg(null);
    try {
      const empId = auth.user.emp_id;
      const r = await bankReviewLoan({
        application_id: selected.application_id,
        emp_id: empId,
        status: reviewForm.status,
        notes: reviewForm.notes,
      });
      setMsg(r.message); setModal(false); refetch();
    } catch (e) { setMsg(e.message); }
    finally { setSaving(false); }
  };

  if (loading) return <Spinner />;
  if (error) return <ErrorBox message={error} onRetry={refetch} />;

  const pending = (data || []).filter(r => r.status === 'submitted' || r.status === 'under_review');
  const resolved = (data || []).filter(r => r.status === 'approved' || r.status === 'rejected');

  const reviewColumns = [
    ...columns,
    { key: '_actions', label: '', render: r =>
      (r.status === 'submitted' || r.status === 'under_review') ?
        <Btn size="sm" onClick={() => openReview(r)}>Review</Btn> : null
    },
  ];

  return (
    <>
      <PageHeader title="Loan Applications" subtitle={`${data.length} total — ${pending.length} pending`} />

      {msg && <div className="mb-4 p-3 bg-blue-50 text-blue-800 rounded-lg text-sm flex justify-between"><span>{msg}</span><button onClick={() => setMsg(null)} className="text-blue-600 hover:underline cursor-pointer">×</button></div>}

      {pending.length > 0 && (
        <Card className="mb-6">
          <div className="p-4 border-b border-gray-100">
            <h3 className="font-semibold text-gray-900">Pending Applications</h3>
          </div>
          <DataTable columns={reviewColumns} rows={pending} />
        </Card>
      )}

      <Card>
        <div className="p-4 border-b border-gray-100">
          <h3 className="font-semibold text-gray-900">All Applications</h3>
        </div>
        <DataTable columns={reviewColumns} rows={data} />
      </Card>

      <Modal open={modal} onClose={() => setModal(false)} title={`Review Application #${selected?.application_id}`}>
        {selected && (
          <div className="space-y-3">
            <div className="bg-gray-50 p-3 rounded text-sm space-y-1">
              <p><strong>Customer:</strong> {selected.customer_name}</p>
              <p><strong>Amount:</strong> ₹{Number(selected.requested_amount).toLocaleString('en-IN')}</p>
              <p><strong>Purpose:</strong> {selected.purpose}</p>
              <p><strong>Applied:</strong> {selected.application_date?.slice(0, 10)} ({selected.days_open}d ago)</p>
            </div>
            <FormField label="Decision">
              <Select value={reviewForm.status} onChange={e => setReviewForm(f => ({ ...f, status: e.target.value }))}>
                <option value="approved">Approve</option>
                <option value="rejected">Reject</option>
                <option value="under_review">Under Review</option>
              </Select>
            </FormField>
            <FormField label="Notes">
              <Input value={reviewForm.notes} onChange={e => setReviewForm(f => ({ ...f, notes: e.target.value }))} placeholder="Decision notes..." />
            </FormField>
          </div>
        )}
        <div className="flex justify-end gap-2 mt-4">
          <Btn variant="secondary" onClick={() => setModal(false)}>Cancel</Btn>
          <Btn onClick={review} disabled={saving}>{saving ? 'Submitting...' : 'Submit Decision'}</Btn>
        </div>
      </Modal>
    </>
  );
}
