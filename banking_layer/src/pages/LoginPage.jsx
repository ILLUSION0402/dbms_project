import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { loginUser } from '../api';
import { LogIn, Eye, EyeOff } from 'lucide-react';

export default function LoginPage() {
  const { login } = useAuth();
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [showPw, setShowPw] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleLogin = async (e) => {
    e.preventDefault();
    if (!name || !password) { setError('Enter name and password'); return; }
    setError('');
    setLoading(true);
    try {
      const data = await loginUser(name.trim(), password);
      login(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-indigo-950 to-slate-900 flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">🏦 BankVCS</h1>
          <p className="text-indigo-300">Git-like Version Control for Banking Data</p>
          <p className="text-indigo-400/60 text-xs mt-1">IIT Palakkad — DS3020 DBMS Project</p>
        </div>

        <div className="bg-white rounded-2xl shadow-2xl p-8">
          <h2 className="text-xl font-bold text-gray-900 mb-2 text-center">Sign In</h2>
          <p className="text-sm text-gray-400 text-center mb-6">Role is detected automatically</p>

          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Full Name</label>
              <input
                type="text"
                value={name}
                onChange={(e) => { setName(e.target.value); setError(''); }}
                placeholder="e.g. Rajesh Nair"
                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-gray-900"
                autoFocus
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
              <div className="relative">
                <input
                  type={showPw ? 'text' : 'password'}
                  value={password}
                  onChange={(e) => { setPassword(e.target.value); setError(''); }}
                  placeholder="Enter password"
                  className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-gray-900 pr-12"
                />
                <button
                  type="button"
                  onClick={() => setShowPw(!showPw)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                >
                  {showPw ? <EyeOff size={18} /> : <Eye size={18} />}
                </button>
              </div>
            </div>

            {error && (
              <div className="bg-red-50 border border-red-200 text-red-700 text-sm rounded-lg p-3">{error}</div>
            )}

            <button
              type="submit"
              disabled={loading || !name || !password}
              className="w-full py-3 bg-indigo-600 text-white font-semibold rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center justify-center gap-2"
            >
              <LogIn size={18} />
              {loading ? 'Signing in...' : 'Sign In'}
            </button>
          </form>

          <div className="mt-6 pt-4 border-t border-gray-100">
            <p className="text-xs text-gray-400 text-center mb-2">Demo Credentials</p>
            <div className="grid grid-cols-1 gap-1 text-xs text-gray-500">
              <p className="text-center"><span className="font-semibold">Manager:</span> Rajesh Nair</p>
              <p className="text-center"><span className="font-semibold">Employee:</span> Priya Menon, Arun Kumar, ...</p>
              <p className="text-center"><span className="font-semibold">Customer:</span> Arjun Pillai, Meena Suresh, ...</p>
            </div>
            <p className="text-[10px] text-gray-400 text-center mt-2">Default password = your full name</p>
          </div>
        </div>
      </div>
    </div>
  );
}
