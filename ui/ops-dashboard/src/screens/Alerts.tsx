import { Bell, Mail, Send } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Ui";
import { headerSubtitle } from "../snapshotFormat";
import type { AlertsSnapshot } from "../types";
import { useSnapshot } from "../useSnapshot";

export function Alerts() {
  const { snapshot, data } = useSnapshot<AlertsSnapshot>("/api/alerts");
  const events = data?.events?.length ? data.events : unavailableEvents();
  const activeCount = events.filter((event) => event.state === "active").length;

  return (
    <>
      <PageHeader
        title="Alerts"
        subtitle={headerSubtitle(snapshot, "critical transitions · push")}
        status={snapshot?.stale ? "Stale" : `${activeCount} active`}
        statusLevel={snapshot?.stale || activeCount > 0 ? "warning" : "safe"}
      />
      <div className="screen">
        <div className="alerts-layout">
          <Panel className="timeline-panel" eyebrow="Critical transitions">
            <div className="timeline">
              {events.map((event) => (
                <div className={`timeline-event timeline-${event.level}`} key={event.title}>
                  <span className="timeline-dot" />
                  <div>
                    <strong>{event.title}</strong>
                    <p>{event.detail}</p>
                  </div>
                  <em>{event.state}</em>
                </div>
              ))}
            </div>
          </Panel>

          <Panel className="channel-panel" eyebrow="Push channels" meta="V1.5">
            <div className="channel-list">
              <div>
                <Send size={18} />
                Telegram
                <span>planned</span>
              </div>
              <div>
                <Bell size={18} />
                ntfy
                <span>planned</span>
              </div>
              <div className="muted-row">
                <Mail size={18} />
                Email
                <span>off</span>
              </div>
            </div>
            <p className="muted">Alerting fires only on critical transitions. No destructive ops are triggered from push.</p>
          </Panel>
        </div>
      </div>
    </>
  );
}

function unavailableEvents(): AlertsSnapshot["events"] {
  return [
    {
      title: "Alert source unavailable",
      detail: "Current transition status is not reported.",
      state: "active",
      level: "warning"
    }
  ];
}
