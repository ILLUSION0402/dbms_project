import { Loader2, X } from 'lucide-react';

export function PageHeader({ title, subtitle, children }) {
  return (
    <div className="flex items-center justify-between mb-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">{title}</h2>
        {subtitle && <p className="text-sm text-gray-500 mt-0.5">{subtitle}</p>}
      </div>
      {children}
    </div>
  );
}

export function Card({ children, className = '' }) {
  return (
    <div className={`bg-white rounded-xl border border-gray-200 shadow-sm ${className}`}>
      {children}
    </div>
  );
}

export function StatCard({ label, value, icon: Icon, color = 'indigo' }) {
  const colors = {
    indigo:  'bg-indigo-50 text-indigo-600',
    green:   'bg-emerald-50 text-emerald-600',
    amber:   'bg-amber-50  text-amber-600',
    rose:    'bg-rose-50   text-rose-600',
    sky:     'bg-sky-50    text-sky-600',
    violet:  'bg-violet-50 text-violet-600',
  };
  return (
    <Card className="p-5">
      <div className="flex items-center gap-4">
        <div className={`p-3 rounded-lg ${colors[color]}`}>
          <Icon size={22} />
        </div>
        <div>
          <p className="text-sm text-gray-500">{label}</p>
          <p className="text-2xl font-bold text-gray-900">{value ?? '—'}</p>
        </div>
      </div>
    </Card>
  );
}

export function Spinner() {
  return (
    <div className="flex items-center justify-center py-20">
      <Loader2 className="animate-spin text-indigo-500" size={32} />
    </div>
  );
}

export function ErrorBox({ message, onRetry }) {
  return (
    <div className="rounded-lg bg-red-50 border border-red-200 p-4 text-sm text-red-700">
      <p className="font-medium">Error</p>
      <p>{message}</p>
      {onRetry && (
        <button onClick={onRetry} className="mt-2 text-red-600 underline text-xs">
          Retry
        </button>
      )}
    </div>
  );
}

export function Badge({ children, variant = 'default' }) {
  const styles = {
    default: 'bg-gray-100 text-gray-700',
    green:   'bg-emerald-100 text-emerald-700',
    red:     'bg-red-100 text-red-700',
    amber:   'bg-amber-100 text-amber-700',
    blue:    'bg-blue-100 text-blue-700',
    indigo:  'bg-indigo-100 text-indigo-700',
    purple:  'bg-purple-100 text-purple-700',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${styles[variant]}`}>
      {children}
    </span>
  );
}

export function DataTable({ columns, rows, emptyMsg = 'No data' }) {
  if (!rows || rows.length === 0) {
    return <p className="text-center py-10 text-gray-400 text-sm">{emptyMsg}</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200">
            {columns.map((col) => (
              <th key={col.key} className="text-left px-4 py-3 font-semibold text-gray-500 text-xs uppercase tracking-wider whitespace-nowrap">
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-gray-100 hover:bg-gray-50 transition-colors">
              {columns.map((col) => (
                <td key={col.key} className="px-4 py-3 text-gray-700 whitespace-nowrap">
                  {col.render ? col.render(row) : String(row[col.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function Modal({ open, onClose, title, children }) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg max-h-[90vh] overflow-y-auto mx-4" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-bold text-gray-900">{title}</h3>
          <button onClick={onClose} className="p-1 rounded-lg hover:bg-gray-100 text-gray-400 hover:text-gray-600 cursor-pointer"><X size={20} /></button>
        </div>
        <div className="px-6 py-4">{children}</div>
      </div>
    </div>
  );
}

export function FormField({ label, children, className = '' }) {
  return (
    <div className={className}>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      {children}
    </div>
  );
}

export function Input({ ...props }) {
  return (
    <input
      {...props}
      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-gray-900"
    />
  );
}

export function Select({ children, ...props }) {
  return (
    <select
      {...props}
      className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-gray-900"
    >
      {children}
    </select>
  );
}

export function Btn({ children, variant = 'primary', ...props }) {
  const styles = {
    primary: 'bg-indigo-600 text-white hover:bg-indigo-700',
    secondary: 'bg-gray-100 text-gray-700 hover:bg-gray-200',
    danger: 'bg-red-600 text-white hover:bg-red-700',
    success: 'bg-emerald-600 text-white hover:bg-emerald-700',
  };
  return (
    <button
      {...props}
      className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-50 cursor-pointer ${styles[variant]} ${props.className || ''}`}
    >
      {children}
    </button>
  );
}
