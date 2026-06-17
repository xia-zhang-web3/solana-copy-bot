import {
  Activity,
  Bell,
  Database,
  Gauge,
  LogOut,
  MoreHorizontal,
  Search,
  TrendingUp
} from "lucide-react";
import { useState } from "react";
import type { NavKey } from "../types";
import { Brand } from "./Brand";

const navItems: Array<{ key: NavKey; label: string; icon: typeof Gauge }> = [
  { key: "overview", label: "Overview", icon: Gauge },
  { key: "execution", label: "Execution", icon: Activity },
  { key: "strategy", label: "Strategy", icon: TrendingUp },
  { key: "discovery", label: "Discovery", icon: Search },
  { key: "storage", label: "Storage", icon: Database },
  { key: "alerts", label: "Alerts", icon: Bell }
];

const mobilePrimaryItems = navItems.slice(0, 3);
const mobileOverflowItems = navItems.slice(3);

interface AppShellProps {
  active: NavKey;
  onNavigate: (key: NavKey) => void;
  onLogout: () => void;
  children: React.ReactNode;
}

export function AppShell({ active, onNavigate, onLogout, children }: AppShellProps) {
  const [moreOpen, setMoreOpen] = useState(false);
  const moreActive = mobileOverflowItems.some((item) => item.key === active);

  function navigate(key: NavKey) {
    setMoreOpen(false);
    onNavigate(key);
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <Brand />
        <nav className="side-nav" aria-label="Dashboard">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                className={item.key === active ? "nav-button nav-button-active" : "nav-button"}
                key={item.key}
                onClick={() => navigate(item.key)}
                type="button"
                aria-current={item.key === active ? "page" : undefined}
              >
                <Icon size={19} />
                {item.label}
              </button>
            );
          })}
        </nav>
        <button className="user-card" onClick={onLogout} type="button">
          <span className="avatar">op</span>
          <span>
            <strong>account</strong>
            <small>session 9h left</small>
          </span>
          <LogOut size={16} />
        </button>
      </aside>
      <main className="app-main">{children}</main>
      <nav className="bottom-nav" aria-label="Mobile dashboard">
        {mobilePrimaryItems.map((item) => {
          const Icon = item.icon;
          return (
            <button
              className={item.key === active ? "bottom-nav-item active" : "bottom-nav-item"}
              key={item.key}
              onClick={() => navigate(item.key)}
              type="button"
              aria-current={item.key === active ? "page" : undefined}
            >
              <Icon size={19} />
              <span>{item.label}</span>
            </button>
          );
        })}
        <button
          className={moreActive || moreOpen ? "bottom-nav-item active" : "bottom-nav-item"}
          onClick={() => setMoreOpen((open) => !open)}
          type="button"
          aria-expanded={moreOpen}
        >
          <MoreHorizontal size={20} />
          <span>More</span>
        </button>
        {moreOpen ? (
          <div className="bottom-overflow" role="menu">
            {mobileOverflowItems.map((item) => {
              const Icon = item.icon;
              return (
                <button
                  className={item.key === active ? "overflow-item active" : "overflow-item"}
                  key={item.key}
                  onClick={() => navigate(item.key)}
                  type="button"
                  role="menuitem"
                >
                  <Icon size={18} />
                  {item.label}
                </button>
              );
            })}
          </div>
        ) : null}
      </nav>
    </div>
  );
}
