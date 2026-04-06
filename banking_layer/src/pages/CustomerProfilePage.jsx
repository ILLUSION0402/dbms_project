import { useAuth } from '../context/AuthContext';
import { PageHeader, Card, Badge } from '../components/UI';

function Field({ label, value }) {
  return (
    <div>
      <p className="text-xs text-gray-400 uppercase tracking-wider">{label}</p>
      <p className="text-sm font-medium text-gray-900 mt-0.5">{value || '—'}</p>
    </div>
  );
}

export default function CustomerProfilePage() {
  const { auth } = useAuth();
  const u = auth.user;

  const kycColor = { verified: 'green', pending: 'amber', expired: 'red', rejected: 'red' };
  const statusColor = { active: 'green', dormant: 'amber', closed: 'red', blocked: 'red' };

  return (
    <>
      <PageHeader title="My Profile" subtitle="Personal & KYC information" />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">Personal Details</h3>
          <div className="grid grid-cols-2 gap-4">
            <Field label="Full Name" value={u.full_name} />
            <Field label="Date of Birth" value={u.dob?.slice(0, 10)} />
            <Field label="Gender" value={u.gender === 'M' ? 'Male' : u.gender === 'F' ? 'Female' : 'Other'} />
            <Field label="Phone" value={u.phone} />
            <Field label="Email" value={u.email} />
            <Field label="Occupation" value={u.occupation} />
            <Field label="Income Bracket" value={u.income_bracket?.replace(/_/g, ' ')} />
            <Field label="Customer Since" value={u.customer_since?.slice(0, 10)} />
          </div>
        </Card>

        <Card className="p-6">
          <h3 className="font-semibold text-gray-900 mb-4">KYC & Account Status</h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs text-gray-400 uppercase tracking-wider">KYC Status</p>
              <Badge variant={kycColor[u.kyc_status]}>{u.kyc_status}</Badge>
            </div>
            <div>
              <p className="text-xs text-gray-400 uppercase tracking-wider">Account Status</p>
              <Badge variant={statusColor[u.status]}>{u.status}</Badge>
            </div>
            <Field label="Aadhaar" value={u.aadhaar_number ? `XXXX-XXXX-${u.aadhaar_number.slice(-4)}` : null} />
            <Field label="PAN" value={u.pan_number} />
            <Field label="KYC Verified On" value={u.kyc_verified_on?.slice(0, 10)} />
            <Field label="Branch" value={u.branch_name} />
            <Field label="Relationship Manager" value={u.rm_name} />
          </div>
        </Card>
      </div>
    </>
  );
}
