import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { changePassword } from '../api';
import { Key, Eye, EyeOff, CheckCircle } from 'lucide-react';

export default function ChangePasswordPage() {
  const { auth } = useAuth();
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showCurrent, setShowCurrent] = useState(false);
  const [showNew, setShowNew] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);

  const id = auth.role === 'customer' ? auth.user.customer_id : auth.user.emp_id;

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(''); setSuccess('');

    if (!currentPassword || !newPassword || !confirmPassword) {
      setError('All fields are required'); return;
    }
    if (newPassword !== confirmPassword) {
      setError('New passwords do not match'); return;
    }
    if (newPassword.length < 3) {
      setError('New password must be at least 3 characters'); return;
    }

    setLoading(true);
    try {
      const data = await changePassword(auth.role, id, currentPassword, newPassword);
      setSuccess(data.message);
      setCurrentPassword(''); setNewPassword(''); setConfirmPassword('');
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-lg mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <Key className="text-indigo-600" size={24} />
        <h1 className="text-2xl font-bold text-gray-900">Change Password</h1>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <p className="text-sm text-gray-500 mb-6">
          Logged in as <span className="font-semibold text-gray-700">{auth.user.full_name}</span>
          <span className="text-xs ml-2 capitalize px-2 py-0.5 bg-gray-100 rounded">{auth.role}</span>
        </p>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Current Password</label>
            <div className="relative">
              <input
                type={showCurrent ? 'text' : 'password'}
                value={currentPassword}
                onChange={(e) => { setCurrentPassword(e.target.value); setError(''); setSuccess(''); }}
                className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 pr-12"
              />
              <button type="button" onClick={() => setShowCurrent(!showCurrent)} className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                {showCurrent ? <EyeOff size={16} /> : <Eye size={16} />}
              </button>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">New Password</label>
            <div className="relative">
              <input
                type={showNew ? 'text' : 'password'}
                value={newPassword}
                onChange={(e) => { setNewPassword(e.target.value); setError(''); setSuccess(''); }}
                className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 pr-12"
              />
              <button type="button" onClick={() => setShowNew(!showNew)} className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
                {showNew ? <EyeOff size={16} /> : <Eye size={16} />}
              </button>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Confirm New Password</label>
            <input
              type="password"
              value={confirmPassword}
              onChange={(e) => { setConfirmPassword(e.target.value); setError(''); setSuccess(''); }}
              className="w-full px-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
          </div>

          {error && <div className="bg-red-50 border border-red-200 text-red-700 text-sm rounded-lg p-3">{error}</div>}
          {success && (
            <div className="bg-green-50 border border-green-200 text-green-700 text-sm rounded-lg p-3 flex items-center gap-2">
              <CheckCircle size={16} /> {success}
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full py-2.5 bg-indigo-600 text-white font-semibold rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors"
          >
            {loading ? 'Changing...' : 'Change Password'}
          </button>
        </form>
      </div>
    </div>
  );
}
