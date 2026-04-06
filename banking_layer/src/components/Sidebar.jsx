import { NavLink } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import {
  LayoutDashboard, Users, Building2, Briefcase, CreditCard,
  ArrowLeftRight, Receipt, Landmark, UserCog, GitBranch,
  GitCommitHorizontal, Tag, Clock, Search, RotateCcw, FileText, Database,
  LogOut, User, Shield, ClipboardList, Key
} from 'lucide-react';

const managerBankingLinks = [
  { to: '/',               icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/customers',      icon: Users,           label: 'Customers' },
  { to: '/accounts',       icon: CreditCard,      label: 'Accounts' },
  { to: '/transactions',   icon: Receipt,         label: 'Transactions' },
  { to: '/loans',          icon: Landmark,         label: 'Loans' },
  { to: '/loan-applications', icon: ClipboardList, label: 'Loan Applications' },
  { to: '/transfers',      icon: ArrowLeftRight,   label: 'Transfers' },
  { to: '/employees',      icon: Briefcase,        label: 'Employees' },
  { to: '/hr-updates',     icon: UserCog,          label: 'HR Updates' },
  { to: '/branches',       icon: Building2,        label: 'Branches' },
  { to: '/change-password', icon: Key,              label: 'Change Password' },
];

const vcsLinks = [
  { to: '/vcs',            icon: Database,              label: 'VCS Dashboard' },
  { to: '/vcs/commits',    icon: GitCommitHorizontal,   label: 'Commits' },
  { to: '/vcs/branches',   icon: GitBranch,             label: 'Branches' },
  { to: '/vcs/tags',       icon: Tag,                   label: 'Tags' },
  { to: '/vcs/diff',       icon: FileText,              label: 'Diff' },
  { to: '/vcs/blame',      icon: Search,                label: 'Blame' },
  { to: '/vcs/time-travel',icon: Clock,                 label: 'Time Travel' },
  { to: '/vcs/rollback',   icon: RotateCcw,             label: 'Rollback' },
];

const employeeLinks = [
  { to: '/',                icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/my-customers',   icon: Users,           label: 'My Customers' },
  { to: '/my-accounts',    icon: CreditCard,      label: 'Customer Accounts' },
  { to: '/my-transactions',icon: Receipt,         label: 'Transactions' },
  { to: '/my-loans',       icon: Landmark,         label: 'Managed Loans' },
  { to: '/loan-applications', icon: ClipboardList, label: 'Loan Applications' },
  { to: '/profile',        icon: User,             label: 'My Profile' },
  { to: '/change-password', icon: Key,              label: 'Change Password' },
];

const customerLinks = [
  { to: '/',                icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/my-accounts',    icon: CreditCard,      label: 'My Accounts' },
  { to: '/my-transactions',icon: Receipt,         label: 'My Transactions' },
  { to: '/my-loans',       icon: Landmark,         label: 'My Loans' },
  { to: '/my-transfers',   icon: ArrowLeftRight,   label: 'My Transfers' },
  { to: '/profile',        icon: User,             label: 'My Profile' },
  { to: '/change-password', icon: Key,              label: 'Change Password' },
];

function SideLink({ to, icon: Icon, label }) {
  return (
    <NavLink
      to={to}
      end={to === '/' || to === '/vcs'}
      className={({ isActive }) =>
        `flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors
         ${isActive
           ? 'bg-indigo-50 text-indigo-700 font-semibold'
           : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'}`
      }
    >
      <Icon size={18} />
      <span>{label}</span>
    </NavLink>
  );
}

const roleColors = {
  manager:  'bg-indigo-100 text-indigo-700',
  employee: 'bg-emerald-100 text-emerald-700',
  customer: 'bg-amber-100 text-amber-700',
};

const roleIcons = { manager: Shield, employee: Briefcase, customer: User };

export default function Sidebar() {
  const { auth, logout } = useAuth();
  const role = auth?.role;
  const user = auth?.user;
  const RoleIcon = roleIcons[role] || User;

  return (
    <aside className="w-60 shrink-0 border-r border-gray-200 bg-white h-screen overflow-y-auto sticky top-0 flex flex-col">
      <div className="px-5 py-5">
        <h1 className="text-lg font-bold text-gray-900 tracking-tight">🏦 BankVCS</h1>
        <p className="text-xs text-gray-400 mt-0.5">Git-like DB Versioning</p>
      </div>

      {/* Role badge */}
      <div className="px-4 mb-3">
        <div className={`flex items-center gap-2 px-3 py-2 rounded-lg text-xs font-semibold ${roleColors[role]}`}>
          <RoleIcon size={14} />
          <span className="capitalize">{role}</span>
          <span className="text-[10px] font-normal opacity-70 truncate ml-auto">{user?.full_name}</span>
        </div>
      </div>

      <nav className="px-3 space-y-1 flex-1">
        {role === 'manager' && (
          <>
            <p className="px-3 pt-3 pb-1 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Banking</p>
            {managerBankingLinks.map((l) => <SideLink key={l.to} {...l} />)}
            <p className="px-3 pt-5 pb-1 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Version Control</p>
            {vcsLinks.map((l) => <SideLink key={l.to} {...l} />)}
          </>
        )}

        {role === 'employee' && (
          <>
            <p className="px-3 pt-3 pb-1 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Employee Portal</p>
            {employeeLinks.map((l) => <SideLink key={l.to} {...l} />)}
          </>
        )}

        {role === 'customer' && (
          <>
            <p className="px-3 pt-3 pb-1 text-[11px] font-semibold text-gray-400 uppercase tracking-wider">Customer Portal</p>
            {customerLinks.map((l) => <SideLink key={l.to} {...l} />)}
          </>
        )}
      </nav>

      <div className="px-3 py-4 border-t border-gray-100">
        <button
          onClick={logout}
          className="flex items-center gap-3 w-full px-3 py-2 rounded-lg text-sm text-red-600 hover:bg-red-50 transition-colors cursor-pointer"
        >
          <LogOut size={18} />
          <span>Sign Out</span>
        </button>
      </div>
    </aside>
  );
}
