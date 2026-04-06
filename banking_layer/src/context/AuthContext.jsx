import { createContext, useContext, useState, useCallback } from 'react';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [auth, setAuth] = useState(() => {
    try {
      const saved = sessionStorage.getItem('bankAuth');
      return saved ? JSON.parse(saved) : null;
    } catch { return null; }
  });

  const login = useCallback((data) => {
    setAuth(data);
    sessionStorage.setItem('bankAuth', JSON.stringify(data));
  }, []);

  const logout = useCallback(() => {
    setAuth(null);
    sessionStorage.removeItem('bankAuth');
  }, []);

  return (
    <AuthContext.Provider value={{ auth, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be inside AuthProvider');
  return ctx;
}
