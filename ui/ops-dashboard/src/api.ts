import type { ApiEnvelope } from "./types";

export interface SessionPayload {
  authenticated: boolean;
  username?: string;
}

export async function getSession(): Promise<SessionPayload> {
  const response = await fetch("/api/session", {
    credentials: "include"
  });
  if (!response.ok) {
    return { authenticated: false };
  }
  return response.json() as Promise<SessionPayload>;
}

export async function login(username: string, password: string): Promise<SessionPayload> {
  const response = await fetch("/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify({ username, password })
  });
  if (!response.ok) {
    return { authenticated: false };
  }
  return response.json() as Promise<SessionPayload>;
}

export async function logout(): Promise<void> {
  await fetch("/logout", {
    method: "POST",
    credentials: "include"
  });
}

export async function fetchSnapshot<T>(path: string): Promise<ApiEnvelope<T>> {
  const response = await fetch(path, {
    credentials: "include"
  });
  if (!response.ok) {
    throw new Error(`snapshot request failed: ${response.status}`);
  }
  return response.json() as Promise<ApiEnvelope<T>>;
}
