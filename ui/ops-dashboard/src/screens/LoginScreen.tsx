import { FormEvent, useState } from "react";
import { XCircle } from "lucide-react";
import { login } from "../api";
import { Brand } from "../components/Brand";

interface LoginScreenProps {
  onLogin: () => void;
}

export function LoginScreen({ onLogin }: LoginScreenProps) {
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const username = String(data.get("username") ?? "");
    const password = String(data.get("password") ?? "");
    if (!username || !password) {
      setError(true);
      return;
    }
    setLoading(true);
    setError(false);
    const session = await login(username, password);
    setLoading(false);
    if (session.authenticated) {
      onLogin();
      return;
    }
    setError(true);
  }

  return (
    <main className="login-page">
      <section className="login-card">
        <Brand />
        <div className="login-heading">
          <h1>Sign in</h1>
        </div>
        <form onSubmit={submit} className="login-form">
          <label>
            Username
            <input autoComplete="username" name="username" type="text" />
          </label>
          <label>
            Password
            <input autoComplete="current-password" name="password" type="password" />
          </label>
          {error ? (
            <div className="form-error">
              <XCircle size={15} />
              Invalid credentials.
            </div>
          ) : null}
          <button disabled={loading} type="submit">
            {loading ? "Signing in..." : "Sign in"}
          </button>
        </form>
      </section>
    </main>
  );
}
