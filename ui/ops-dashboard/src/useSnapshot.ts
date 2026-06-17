import { useEffect, useState } from "react";
import { fetchSnapshot } from "./api";
import type { ApiEnvelope } from "./types";

export function useSnapshot<T>(path: string) {
  const [snapshot, setSnapshot] = useState<ApiEnvelope<T> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetchSnapshot<T>(path)
      .then((payload) => {
        if (!cancelled) {
          setSnapshot(payload);
          setError(null);
        }
      })
      .catch((err: Error) => {
        if (!cancelled) {
          setError(err.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [path]);

  return { snapshot, data: snapshot?.data ?? null, loading, error };
}
