import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { useAuth } from './context/AuthContext';
import Layout from './components/Layout';
import LoginPage from './pages/LoginPage';
// Manager / shared banking pages
import Dashboard from './pages/Dashboard';
import CustomersPage from './pages/CustomersPage';
import AccountsPage from './pages/AccountsPage';
import TransactionsPage from './pages/TransactionsPage';
import LoansPage from './pages/LoansPage';
import TransfersPage from './pages/TransfersPage';
import EmployeesPage from './pages/EmployeesPage';
import HrUpdatesPage from './pages/HrUpdatesPage';
import BranchesPage from './pages/BranchesPage';
import LoanApplicationsPage from './pages/LoanApplicationsPage';
// VCS pages (manager only)
import VcsDashboard from './pages/VcsDashboard';
import CommitsPage from './pages/CommitsPage';
import VcsBranchesPage from './pages/VcsBranchesPage';
import TagsPage from './pages/TagsPage';
import DiffPage from './pages/DiffPage';
import BlamePage from './pages/BlamePage';
import TimeTravelPage from './pages/TimeTravelPage';
import RollbackPage from './pages/RollbackPage';
// Employee pages
import EmployeeDashboard from './pages/EmployeeDashboard';
import MyCustomersPage from './pages/MyCustomersPage';
import MyLoansPage from './pages/MyLoansPage';
import MyAccountsPage from './pages/MyAccountsPage';
import MyTransactionsPage from './pages/MyTransactionsPage';
import EmployeeProfilePage from './pages/EmployeeProfilePage';
// Customer pages
import CustomerDashboard from './pages/CustomerDashboard';
import CustomerAccountsPage from './pages/CustomerAccountsPage';
import CustomerTransactionsPage from './pages/CustomerTransactionsPage';
import CustomerLoansPage from './pages/CustomerLoansPage';
import CustomerTransfersPage from './pages/CustomerTransfersPage';
import CustomerProfilePage from './pages/CustomerProfilePage';
import ChangePasswordPage from './pages/ChangePasswordPage';

function ManagerRoutes() {
  return (
    <Route element={<Layout />}>
      <Route index element={<Dashboard />} />
      <Route path="customers" element={<CustomersPage />} />
      <Route path="accounts" element={<AccountsPage />} />
      <Route path="transactions" element={<TransactionsPage />} />
      <Route path="loans" element={<LoansPage />} />
      <Route path="loan-applications" element={<LoanApplicationsPage />} />
      <Route path="transfers" element={<TransfersPage />} />
      <Route path="employees" element={<EmployeesPage />} />
      <Route path="hr-updates" element={<HrUpdatesPage />} />
      <Route path="branches" element={<BranchesPage />} />
      <Route path="vcs" element={<VcsDashboard />} />
      <Route path="vcs/commits" element={<CommitsPage />} />
      <Route path="vcs/branches" element={<VcsBranchesPage />} />
      <Route path="vcs/tags" element={<TagsPage />} />
      <Route path="vcs/diff" element={<DiffPage />} />
      <Route path="vcs/blame" element={<BlamePage />} />
      <Route path="vcs/time-travel" element={<TimeTravelPage />} />
      <Route path="vcs/rollback" element={<RollbackPage />} />
      <Route path="change-password" element={<ChangePasswordPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Route>
  );
}

function EmployeeRoutes() {
  return (
    <Route element={<Layout />}>
      <Route index element={<EmployeeDashboard />} />
      <Route path="my-customers" element={<MyCustomersPage />} />
      <Route path="my-accounts" element={<MyAccountsPage />} />
      <Route path="my-transactions" element={<MyTransactionsPage />} />
      <Route path="my-loans" element={<MyLoansPage />} />
      <Route path="loan-applications" element={<LoanApplicationsPage />} />
      <Route path="profile" element={<EmployeeProfilePage />} />
      <Route path="change-password" element={<ChangePasswordPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Route>
  );
}

function CustomerRoutes() {
  return (
    <Route element={<Layout />}>
      <Route index element={<CustomerDashboard />} />
      <Route path="my-accounts" element={<CustomerAccountsPage />} />
      <Route path="my-transactions" element={<CustomerTransactionsPage />} />
      <Route path="my-loans" element={<CustomerLoansPage />} />
      <Route path="my-transfers" element={<CustomerTransfersPage />} />
      <Route path="profile" element={<CustomerProfilePage />} />
      <Route path="change-password" element={<ChangePasswordPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Route>
  );
}

function AuthenticatedApp() {
  const { auth } = useAuth();

  return (
    <Routes>
      {auth.role === 'manager' && ManagerRoutes()}
      {auth.role === 'employee' && EmployeeRoutes()}
      {auth.role === 'customer' && CustomerRoutes()}
    </Routes>
  );
}

export default function App() {
  const { auth } = useAuth();

  return (
    <BrowserRouter>
      {auth ? <AuthenticatedApp /> : (
        <Routes>
          <Route path="*" element={<LoginPage />} />
        </Routes>
      )}
    </BrowserRouter>
  );
}

