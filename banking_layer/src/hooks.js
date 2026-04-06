import { useState, useEffect } from 'react';

export function useApi(fetcher, deps = []) {
  const [data, setData]     = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState(null);

  const refetch = () => {
    setLoading(true);
    setError(null);
    fetcher()
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(refetch, deps);

  return { data, loading, error, refetch };
}
