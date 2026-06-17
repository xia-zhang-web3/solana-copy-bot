import { useEffect, useState, type ReactElement } from "react";
import { getSession, logout } from "./api";
import { AppShell } from "./components/AppShell";
import { Alerts } from "./screens/Alerts";
import { Discovery } from "./screens/Discovery";
import { Execution } from "./screens/Execution";
import { LoginScreen } from "./screens/LoginScreen";
import { Overview } from "./screens/Overview";
import { Storage } from "./screens/Storage";
import { Strategy } from "./screens/Strategy";
import type { NavKey } from "./types";

const screens: Record<NavKey, ReactElement> = {
  overview: <Overview />,
  execution: <Execution />,
  strategy: <Strategy />,
  discovery: <Discovery />,
  storage: <Storage />,
  alerts: <Alerts />
};

export function App() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [active, setActive] = useState<NavKey>("overview");

  useEffect(() => {
    let cancelled = false;
    getSession().then((session) => {
      if (!cancelled) {
        setAuthenticated(session.authenticated);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (authenticated === null) {
    return (
      <main className="login-page">
        <section className="login-card login-loading">
          <h1>GrindScout</h1>
          <p>Checking session...</p>
        </section>
      </main>
    );
  }

  if (!authenticated) {
    return <LoginScreen onLogin={() => setAuthenticated(true)} />;
  }

  async function handleLogout() {
    await logout();
    setAuthenticated(false);
  }

  return (
    <AppShell active={active} onLogout={handleLogout} onNavigate={setActive}>
      {screens[active]}
    </AppShell>
  );
}
