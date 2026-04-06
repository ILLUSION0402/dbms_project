import { useApi } from '../hooks';
import { getBranchSummary, getVcsStats } from '../api';
import { PageHeader, Card, StatCard, Spinner, ErrorBox } from '../components/UI';
import { Users, CreditCard, Landmark, Briefcase, GitCommitHorizontal, GitBranch, Tag, Database } from 'lucide-react';

export default function Dashboard() {
  const summary = useApi(getBranchSummary);
  const vcs     = useApi(getVcsStats);

  if (summary.loading || vcs.loading) return <Spinner />;
  if (summary.error) return <ErrorBox message={summary.error} onRetry={summary.refetch} />;

  const s = summary.data?.[0] || {};
  const v = vcs.data || {};

  return (
    <>
      <PageHeader title="Dashboard" subtitle="Retail banking overview + version control stats" />

      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Banking</p>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard label="Customers"      value={s.total_customers}  icon={Users}      color="indigo" />
        <StatCard label="Accounts"       value={s.total_accounts}   icon={CreditCard} color="green" />
        <StatCard label="Active Loans"   value={s.active_loans}     icon={Landmark}    color="amber" />
        <StatCard label="Staff"          value={s.total_staff}      icon={Briefcase}   color="sky" />
      </div>

      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Version Control</p>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard label="Commits"         value={v.total_commits}   icon={GitCommitHorizontal} color="violet" />
        <StatCard label="Branches"        value={v.active_branches} icon={GitBranch}           color="indigo" />
        <StatCard label="Tags"            value={v.total_tags}      icon={Tag}                 color="green" />
        <StatCard label="Tracked Tables"  value={v.tracked_tables}  icon={Database}            color="amber" />
      </div>

      <Card className="p-6">
        <h3 className="font-semibold text-gray-900 mb-2">Branch Summary</h3>
        <p className="text-sm text-gray-500 mb-4">{s.branch_name}</p>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 text-sm">
          <div>
            <span className="text-gray-400">Total Deposits</span>
            <p className="font-bold text-gray-900">₹{Number(s.total_deposits || 0).toLocaleString('en-IN')}</p>
          </div>
          <div>
            <span className="text-gray-400">Loan Book</span>
            <p className="font-bold text-gray-900">₹{Number(s.total_loan_book || 0).toLocaleString('en-IN')}</p>
          </div>
          <div>
            <span className="text-gray-400">VCS Changes Tracked</span>
            <p className="font-bold text-gray-900">{v.total_changes ?? 0}</p>
          </div>
        </div>
      </Card>
    </>
  );
}
