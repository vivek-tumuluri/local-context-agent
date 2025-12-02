import React, { useEffect, useMemo, useState } from "react";
import { useAuth } from "./AuthContext";
import { apiGet, apiPost, fetchRelevantNow, searchKnowledgeBase, startCalendarIngest } from "./api";
import { useIngestStatus } from "./hooks/useIngestStatus";

const POLL_INTERVAL_MS = 3000;
const TERMINAL_STATUSES = new Set(["completed", "completed_with_errors", "failed", "succeeded"]);

const navItems = [
  { id: "home", label: "Home", icon: "⌂" },
  { id: "search", label: "Search", icon: "🔎" },
  { id: "sources", label: "Sources", icon: "📁" },
  { id: "activity", label: "Activity", icon: "⏱" },
  { id: "settings", label: "Settings", icon: "⚙️" },
];

const statusPillClass = (code) => {
  const value = (code || "").toString().toLowerCase();
  if (value.includes("succeed") || value.includes("completed")) return "activity-status-pill activity-status-succeeded";
  if (value.includes("fail") || value.includes("error")) return "activity-status-pill activity-status-failed";
  if (value.includes("run") || value.includes("queue") || value.includes("progress"))
    return "activity-status-pill activity-status-running";
  return "activity-status-pill activity-status-running";
};

const formatTimestamp = (ts) => {
  if (!ts) return "Unknown time";
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return "Unknown time";
  return date.toLocaleString();
};

const formatJobMeta = (job = {}) => {
  const processed =
    job.processed_files ??
    job.processed_count ??
    job.metrics?.ingested ??
    job.ingested ??
    job.found ??
    job.metrics?.found ??
    0;
  const total = job.total_files ?? job.metrics?.found ?? job.total ?? undefined;
  const errors = job.errors ?? job.error_count ?? job.metrics?.errors ?? 0;
  const parts = [`Processed ${processed}${total ? ` / ${total}` : ""}`, `errors ${errors}`];
  return parts.join(" · ");
};

function SidebarItem({ item, isActive, isCollapsed, onClick, onKeyDown }) {
  return (
    <button
      type="button"
      className={`sidebar-item ${isActive ? "sidebar-item-active" : ""} ${isCollapsed ? "sidebar-item--collapsed" : ""}`}
      onClick={onClick}
      onKeyDown={onKeyDown}
      title={isCollapsed ? item.label : undefined}
    >
      <span className="sidebar-item__icon">{item.icon}</span>
      <span className="sidebar-item__label">{item.label}</span>
    </button>
  );
}

function Sidebar({ activeSection, onSelect, isCollapsed, onToggle }) {
  const handleKeyDown = (e, id) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      onSelect(id);
    }
  };

  return (
    <aside className={`sidebar ${isCollapsed ? "sidebar--collapsed" : ""}`}>
      <div className="sidebar-header">
        <div className="sidebar-logo">
          <div className="badge">Azeryn</div>
          <div className="sidebar-logo-text topbar-title">Local Context Agent</div>
        </div>
      </div>
      <div className="sidebar-nav">
        {navItems.map((item) => {
          const isActive = activeSection === item.id;
          return (
            <SidebarItem
              key={item.id}
              item={item}
              isActive={isActive}
              isCollapsed={isCollapsed}
              onClick={() => onSelect(item.id)}
              onKeyDown={(e) => handleKeyDown(e, item.id)}
            />
          );
        })}
      </div>
      <button
        type="button"
        className="sidebar-toggle"
        onClick={onToggle}
        aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        title={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        {isCollapsed ? "›" : "‹"}
      </button>
    </aside>
  );
}

export default function Dashboard() {
  const { user, csrfToken, refreshAuth, isDriveConnected, isCalendarConnected } = useAuth();
  const [driveJobId, setDriveJobId] = useState(null);
  const [driveJob, setDriveJob] = useState(null);
  const [jobPolling, setJobPolling] = useState(false);

  const [question, setQuestion] = useState("");
  const [answerLoading, setAnswerLoading] = useState(false);
  const [messages, setMessages] = useState([]);
  const [inputFocused, setInputFocused] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [relevant, setRelevant] = useState([]);
  const [relevantLoading, setRelevantLoading] = useState(false);
  const [relevantError, setRelevantError] = useState(null);
  const [ingestError, setIngestError] = useState(null);
  const [activeSection, setActiveSection] = useState("home");
  const [activityFilter, setActivityFilter] = useState("all");
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchSource, setSearchSource] = useState("all");
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState(null);

  const {
    jobs,
    loading: jobsLoading,
    error: jobsError,
    lastDriveJob,
    lastCalendarJob,
    driveStatus,
    calendarStatus,
    reload: reloadIngestStatus,
  } = useIngestStatus(csrfToken);

  const loadRelevantNow = async () => {
    if (!user || !csrfToken) return;
    setRelevantLoading(true);
    setRelevantError(null);
    try {
      const data = await fetchRelevantNow(csrfToken);
      setRelevant(data?.results || []);
    } catch (err) {
      console.error("Failed to load relevant now", err);
      setRelevantError("Failed to load");
    } finally {
      setRelevantLoading(false);
    }
  };

  const suggestionPrompts = useMemo(
    () => [
      "What should I review for my next meeting?",
      "Summarize the docs for my upcoming presentation.",
      "What are the next steps from my latest notes?",
    ],
    []
  );

  useEffect(() => {
    if (!driveJobId || !jobPolling) {
      return undefined;
    }

    let cancelled = false;

    const intervalId = setInterval(async () => {
      try {
        const data = await apiGet(`/ingest/jobs/${driveJobId}`);
        if (!cancelled && data) {
          setDriveJob(data);
          const status = (data.status || "").toLowerCase();
          if (status && TERMINAL_STATUSES.has(status)) {
            setJobPolling(false);
            refreshAuth()?.catch(() => {});
          }
        }
      } catch (err) {
        console.error("Failed to fetch job status", err);
      }
    }, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      clearInterval(intervalId);
    };
  }, [driveJobId, jobPolling]);

  useEffect(() => {
    loadRelevantNow();
  }, [user, csrfToken]);

  useEffect(() => {
    const status = (driveJob?.status || "").toLowerCase();
    if (status && TERMINAL_STATUSES.has(status)) {
      reloadIngestStatus();
    }
  }, [driveJob?.status, reloadIngestStatus]);

  const handleDriveIngest = async () => {
    try {
      const payload = {
        query: "",
        max_files: 650,
        reembed_all: false,
      };
      setIngestError(null);
      const job = await apiPost("/ingest/drive/start", payload, csrfToken);
      if (job && job.job_id) {
        setDriveJobId(job.job_id);
        setDriveJob(job);
        setJobPolling(true);
        reloadIngestStatus();
        setActiveSection("activity");
      } else {
        alert("Drive ingest did not return a job_id.");
      }
    } catch (err) {
      console.error("Drive ingest failed", err);
      if (err?.message?.includes("503")) {
        setIngestError("Ingestion is temporarily unavailable – worker offline. Please try again later.");
      } else {
        setIngestError("Drive ingest failed. Check backend logs.");
      }
    }
  };

  const handleCalendarIngest = async () => {
    try {
      setIngestError(null);
      const job = await startCalendarIngest({ force_reembed: false }, csrfToken);
      if (job && job.job_id) {
        reloadIngestStatus();
        setActiveSection("activity");
      } else {
        alert("Calendar ingest did not return a job_id.");
      }
    } catch (err) {
      console.error("Calendar ingest failed", err);
      if (err?.message?.includes("503")) {
        setIngestError("Ingestion is temporarily unavailable – worker offline. Please try again later.");
      } else {
        setIngestError("Calendar ingest failed. Check backend logs.");
      }
    }
  };

  const handleAsk = async () => {
    if (!question.trim()) {
      return;
    }
    const prompt = question.trim();
    const userMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      text: prompt,
    };
    setMessages((prev) => [...prev, userMessage]);
    setQuestion("");
    setAnswerLoading(true);
    try {
      const payload = {
        query: prompt,
        k: 6,
        max_ctx_chars: 4000,
        allow_partial: true,
      };
      const response = await apiPost("/rag/answer", payload, csrfToken);
      const assistantMessage = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        text: response?.answer || JSON.stringify(response, null, 2),
        meta: {
          retrieved: response?.retrieved,
          confidence: response?.confidence,
          sources: Array.isArray(response?.sources) ? response.sources : [],
        },
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      console.error("Question failed", err);
      alert("Question failed. Check backend.");
    } finally {
      setAnswerLoading(false);
    }
  };

  const handleSearch = async (e) => {
    e?.preventDefault();
    if (!searchQuery.trim() || !csrfToken) return;
    setSearchLoading(true);
    setSearchError(null);
    try {
      const payload = {
        query: searchQuery.trim(),
        k: 8,
        source: searchSource === "all" ? undefined : searchSource,
      };
      const resp = await searchKnowledgeBase(payload, csrfToken);
      setSearchResults(resp?.results || []);
    } catch (err) {
      console.error("Search failed", err);
      setSearchError("Search failed. Please try again.");
    } finally {
      setSearchLoading(false);
    }
  };

  const handleDisconnect = async () => {
    const confirmed = window.confirm("This will delete your data and disconnect your Google account. Continue?");
    if (!confirmed) return;
    setDisconnecting(true);
    try {
      await apiPost("/auth/disconnect", {}, csrfToken);
      await refreshAuth();
    } catch (err) {
      console.error("Disconnect failed", err);
      alert("Failed to disconnect. Check backend.");
    } finally {
      setDisconnecting(false);
    }
  };

  if (!user) {
    return <p className="loading">No user loaded.</p>;
  }

  const effectiveDriveStatus = useMemo(() => {
    const ready = !!(user?.drive_ready);
    const base = driveStatus || { label: "Not synced yet", code: "none" };
    if (base.code === "running") return base;
    if (base.code === "failed") return base;
    if (ready) return { label: "Synced", code: "succeeded" };
    return base;
  }, [driveStatus, user]);

  const effectiveCalendarStatus = useMemo(() => {
    const ready = !!(user?.calendar_ready);
    const base = calendarStatus || { label: "Not synced yet", code: "none" };
    if (base.code === "running") return base;
    if (base.code === "failed") return base;
    if (ready) return { label: "Synced", code: "succeeded" };
    return base;
  }, [calendarStatus, user]);

  const initials = (user.full_name || user.email || "")
    .split(" ")
    .map((p) => p[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();

  const renderSourcesCard = ({
    title,
    connected,
    status,
    job,
    onRunIngest,
    onViewHistory,
    includeDisconnect,
  }) => (
    <div className="card" style={{ width: "100%" }}>
      <div className="card-header">
        <div>
          <div className="card-title">{title}</div>
          <div className="card-subtitle">{connected ? "Connected" : "Not connected"}</div>
        </div>
        <span className={statusPillClass(status.code)}>{status.label}</span>
      </div>
      <div className="text-muted" style={{ marginBottom: "8px" }}>
        {job ? (
          <>
            Last run: {formatTimestamp(job.created_at || job.started_at)} <br />
            {formatJobMeta(job)}
          </>
        ) : (
          "No ingest job yet."
        )}
      </div>
      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
        <button className="button-primary" onClick={onRunIngest} disabled={status.code === "running"}>
          {status.code === "running" ? "Syncing…" : "Run ingest"}
        </button>
        <button className="button-secondary" onClick={onViewHistory}>
          View ingest history
        </button>
        {includeDisconnect && (
          <button className="button-secondary" onClick={handleDisconnect} disabled={disconnecting}>
            {disconnecting ? "Disconnecting..." : "Disconnect"}
          </button>
        )}
      </div>
    </div>
  );

  const renderSearchResult = (hit, idx) => {
    const meta = hit.meta || {};
    const title = meta.title || meta.name || "(untitled)";
    const docId = meta.doc_id || meta.id || "";
    const source = (meta.source || "unknown").toLowerCase();
    const link =
      meta.link ||
      meta.webViewLink ||
      (source === "drive" && docId ? `https://drive.google.com/file/d/${docId}/view` : null);
    const snippet = (hit.text || "").slice(0, 240);
    const confidence =
      typeof hit.confidence === "number"
        ? hit.confidence
        : typeof hit.similarity === "number"
          ? hit.similarity
          : null;
    return (
      <div key={hit.id || docId || idx} className="card" style={{ padding: "12px 14px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: "8px", alignItems: "center" }}>
          <div>
            <div className="card-title">
              {link ? (
                <a href={link} target="_blank" rel="noreferrer">
                  {title}
                </a>
              ) : (
                title
              )}
            </div>
            <div className="text-muted-sm">
              {source.charAt(0).toUpperCase() + source.slice(1)}
              {docId ? ` · ${docId}` : ""}
            </div>
          </div>
          {confidence !== null && (
            <span className="badge-neutral">{`score: ${(confidence * 100).toFixed(0)}%`}</span>
          )}
        </div>
        {snippet && (
          <p className="text-muted" style={{ marginTop: "6px" }}>
            {snippet}
            {hit.text && hit.text.length > 240 ? "…" : ""}
          </p>
        )}
      </div>
    );
  };

  const filteredJobs = useMemo(() => {
    if (activityFilter === "drive") return jobs.filter((j) => j.source === "drive" || j.kind === "drive");
    if (activityFilter === "calendar") return jobs.filter((j) => j.source === "calendar" || j.kind === "calendar");
    return jobs;
  }, [jobs, activityFilter]);

  const searchView = (
    <div style={{ padding: "20px 24px 24px" }}>
      <div className="card" style={{ marginBottom: "16px" }}>
        <div className="card-header" style={{ gap: "12px" }}>
          <div>
            <div className="card-title">Search your knowledge base</div>
            <div className="card-subtitle">Find documents with snippets and direct links.</div>
          </div>
        </div>
        <form onSubmit={handleSearch} style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
          <input
            type="text"
            className="chat-textarea"
            style={{ minHeight: "48px", flex: "1 1 320px" }}
            placeholder="Search across your synced docs…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
          <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
            {["all", "drive", "calendar"].map((src) => (
              <button
                key={src}
                type="button"
                className={`chat-suggestion-chip${searchSource === src ? " active" : ""}`}
                onClick={() => setSearchSource(src)}
              >
                {src === "all" ? "All" : src.charAt(0).toUpperCase() + src.slice(1)}
              </button>
            ))}
          </div>
          <button className="button-primary" type="submit" disabled={searchLoading || !searchQuery.trim()}>
            {searchLoading ? "Searching..." : "Search"}
          </button>
        </form>
        <div style={{ marginTop: "12px" }}>
          {searchError && <div style={{ color: "var(--danger)", fontSize: "0.9rem" }}>{searchError}</div>}
          {!searchLoading && !searchError && searchResults.length === 0 && (
            <div className="text-muted">No results yet. Try a search.</div>
          )}
          {searchLoading && <div className="text-muted">Searching…</div>}
          {!searchLoading && searchResults.length > 0 && (
            <div style={{ display: "grid", gap: "10px", marginTop: "8px" }}>
              {searchResults.map((hit, idx) => renderSearchResult(hit, idx))}
            </div>
          )}
        </div>
      </div>
    </div>
  );

  const activityView = (
    <div style={{ padding: "20px 24px 24px" }}>
      <div className="card" style={{ marginBottom: "16px" }}>
        <div className="card-header">
          <div>
            <div className="card-title">Activity</div>
            <div className="card-subtitle">Ingestion timeline across sources.</div>
          </div>
        </div>
        <div className="chat-suggestions">
          {["all", "drive", "calendar"].map((f) => (
            <button
              key={f}
              className={`chat-suggestion-chip${activityFilter === f ? " active" : ""}`}
              type="button"
              onClick={() => setActivityFilter(f)}
            >
              {f === "all" ? "All" : f.charAt(0).toUpperCase() + f.slice(1)}
            </button>
          ))}
        </div>
        {jobsLoading && <div className="text-muted">Loading activity…</div>}
        {jobsError && <div style={{ color: "var(--danger)", fontSize: "0.9rem" }}>Could not load activity</div>}
        {!jobsLoading && !jobsError && filteredJobs.length === 0 && (
          <div className="text-muted">No activity yet. Run an ingest to get started.</div>
        )}
        {!jobsLoading && !jobsError && filteredJobs.length > 0 && (
          <div className="activity-list" style={{ marginTop: "8px" }}>
            {filteredJobs.slice(0, 50).map((job) => (
              <div key={job.job_id || job.id} className="activity-row">
                <div>
                  <div>{job.source === "calendar" || job.kind === "calendar" ? "Calendar ingest" : "Drive ingest"}</div>
                  <div className="text-muted-sm">
                    {formatJobMeta(job)} · {formatTimestamp(job.created_at || job.started_at)}
                    {job.errorSummary ? ` · Error: ${job.errorSummary}` : ""}
                  </div>
                  {Array.isArray(job.logs) && job.logs.length > 0 && (
                    <div className="text-muted-sm">
                      {job.logs.slice(-3).map((log, idx) => (
                        <div key={idx}>{log.message || JSON.stringify(log)}</div>
                      ))}
                    </div>
                  )}
                </div>
                <span className={statusPillClass(job.status)}>{job.status || "unknown"}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );

  const sourcesView = (
    <div style={{ padding: "20px 24px 24px" }}>
      <div className="card" style={{ marginBottom: "16px" }}>
        <div className="card-header">
          <div>
            <div className="card-title">Connected sources</div>
            <div className="card-subtitle">Manage where Azeryn ingests from and see sync status.</div>
          </div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: "16px" }}>
          {renderSourcesCard({
            title: "Google Drive",
            connected: isDriveConnected,
            status: effectiveDriveStatus,
            job: lastDriveJob,
            onRunIngest: handleDriveIngest,
            onViewHistory: () => setActiveSection("activity"),
            includeDisconnect: true,
          })}
          {renderSourcesCard({
            title: "Google Calendar",
            connected: isCalendarConnected,
            status: effectiveCalendarStatus,
            job: lastCalendarJob,
            onRunIngest: handleCalendarIngest,
            onViewHistory: () => setActiveSection("activity"),
            includeDisconnect: false,
          })}
        </div>
      </div>
    </div>
  );

  const settingsView = (
    <div style={{ padding: "20px 24px 24px" }}>
      <div className="card">
        <div className="card-header">
          <div>
            <div className="card-title">Settings</div>
            <div className="card-subtitle">Manage your account and sessions.</div>
          </div>
        </div>
        <div style={{ display: "grid", gap: "10px" }}>
          <div className="text-label">Profile</div>
          <div className="text-muted">Name: {user.full_name || "Unknown"}</div>
          <div className="text-muted">Email: {user.email}</div>
          <div className="text-label" style={{ marginTop: "10px" }}>
            Session
          </div>
          <div className="text-muted">Environment: Early technical preview</div>
          <button className="button-primary" onClick={handleDisconnect} disabled={disconnecting} style={{ width: "fit-content" }}>
            {disconnecting ? "Disconnecting..." : "Disconnect account"}
          </button>
        </div>
      </div>
    </div>
  );

  const renderSourcesSummaryCard = () => (
    <div className="card">
      <div className="card-header">
        <div>
          <div className="card-title">Data sources</div>
          <div className="card-subtitle">Manage ingest and connectivity.</div>
        </div>
        <span className={statusPillClass(effectiveDriveStatus.code)}>{effectiveDriveStatus.label}</span>
      </div>

      {ingestError && <div style={{ color: "var(--danger)", fontSize: "0.85rem", marginBottom: "6px" }}>{ingestError}</div>}

      <div className="data-source-block">
        <div className="data-source-header">
          <span>Google Drive</span>
          <span className={statusPillClass(effectiveDriveStatus.code)}>{effectiveDriveStatus.label}</span>
        </div>
        <div className="text-muted">
          {lastDriveJob ? `Last run ${formatTimestamp(lastDriveJob.created_at || lastDriveJob.started_at)}. ${formatJobMeta(lastDriveJob)}` : "Not synced yet. Start a run to ingest your Drive."}
        </div>
        <div style={{ marginTop: "8px", display: "flex", gap: "8px", flexWrap: "wrap" }}>
          <button className="button-primary" onClick={handleDriveIngest} disabled={effectiveDriveStatus.code === "running"}>
            {effectiveDriveStatus.code === "running" ? "Syncing..." : "Run Drive ingest"}
          </button>
          <button className="button-secondary" onClick={handleDisconnect} disabled={disconnecting}>
            {disconnecting ? "Disconnecting..." : "Disconnect / Delete my data"}
          </button>
        </div>
      </div>

      <div className="data-source-block">
        <div className="data-source-header">
          <span>Calendar</span>
          <span className={statusPillClass(effectiveCalendarStatus.code)}>{effectiveCalendarStatus.label}</span>
        </div>
        <div className="text-muted">Used for Relevant now context.</div>
      </div>
    </div>
  );

  const renderActivitySummaryCard = () => (
    <div className="card">
      <div className="card-header">
        <div>
          <div className="card-title">Activity</div>
          <div className="card-subtitle">Latest ingest summary.</div>
        </div>
        <span className={statusPillClass(effectiveDriveStatus.code)}>{effectiveDriveStatus.label}</span>
      </div>
      {jobsLoading && <div className="text-muted">Loading activity…</div>}
          {jobsError && <div style={{ color: "var(--danger)", fontSize: "0.85rem" }}>Error loading activity</div>}
          {!jobsLoading && !jobsError && jobs.length === 0 && (
            <div className="text-muted">No recent activity. Kick off an ingest to populate your workspace.</div>
          )}
          {!jobsLoading && !jobsError && jobs.length > 0 && (
            <div className="activity-list">
              {jobs.slice(0, 3).map((job) => (
                <div key={job.job_id || job.id} className="activity-row">
                  <div>
                    <div>{job.source === "calendar" || job.kind === "calendar" ? "Calendar ingest" : "Drive ingest"}</div>
                    <div className="text-muted-sm">
                      {formatJobMeta(job)} · {formatTimestamp(job.created_at || job.started_at)}
                    </div>
                    {job.errorSummary && <div style={{ color: "var(--danger)", fontSize: "0.85rem" }}>{job.errorSummary}</div>}
                    {Array.isArray(job.logs) && job.logs.length > 0 && (
                      <div className="text-muted-sm">
                        {job.logs.slice(-3).map((log, idx) => (
                          <div key={idx}>{log.message || JSON.stringify(log)}</div>
                        ))}
                      </div>
                    )}
                  </div>
                  <span className={statusPillClass(job.status)}>{job.status || "Not started"}</span>
                </div>
              ))}
              {driveJob?.error_summary && (
            <div style={{ color: "var(--danger)", fontSize: "0.85rem" }}>Error: {driveJob.error_summary}</div>
          )}
        </div>
      )}
    </div>
  );

  const homeView = (
    <div className="app-main-inner">
      <section className="chat-pane">
        <div className="card" style={{ display: "flex", flexDirection: "column", height: "100%", gap: "10px" }}>
          <div className="chat-header">
            <div className="card-title">Ask your workspace</div>
            <div className="card-subtitle">Questions grounded in your Drive + Calendar data.</div>
            <div style={{ display: "flex", gap: "6px", marginTop: "6px" }}>
              <span className="badge-neutral">Drive</span>
              <span className="badge-neutral">Calendar</span>
            </div>
          </div>

          {messages.length === 0 && (
            <div className="chat-suggestions">
              {suggestionPrompts.map((text) => (
                <button key={text} className="chat-suggestion-chip" type="button" onClick={() => setQuestion(text)}>
                  {text}
                </button>
              ))}
            </div>
          )}

          <div className="chat-history">
            {messages.length === 0 ? (
              <div className="text-muted">Ask a question to get started.</div>
            ) : (
              messages.map((m) => (
                <div
                  key={m.id}
                  className={`chat-message ${m.role === "user" ? "chat-message-user" : "chat-message-assistant"}`}
                >
                  <div>{m.text}</div>
                  {m.role === "assistant" &&
                    m.meta &&
                    (() => {
                      const hasSources = m.meta.sources && m.meta.sources.length > 0;
                      const parts = [
                        m.meta.retrieved !== undefined ? `Based on ${m.meta.retrieved} chunks` : null,
                        m.meta.confidence !== undefined ? `confidence: ${m.meta.confidence}` : null,
                      ].filter(Boolean);
                      if (parts.length === 0 && !hasSources) return null;
                      const renderLabel = (source, idx) => {
                        const label =
                          source.title ||
                          source.doc_id ||
                          source.link ||
                          source.source ||
                          `source ${idx + 1}`;
                        if (source.link) {
                          return (
                            <a href={source.link} target="_blank" rel="noreferrer">
                              {`[${idx + 1}] ${label}`}
                            </a>
                          );
                        }
                        return `[${idx + 1}] ${label}`;
                      };
                      return (
                        <div className="answer-meta">
                          {parts.join(" · ")}
                          {hasSources && (
                            <div style={{ marginTop: "4px" }}>
                              Sources:{" "}
                              {m.meta.sources.map((s, idx) => (
                                <span key={s.doc_id || s.id || idx} style={{ marginRight: "8px" }}>
                                  {renderLabel(s, idx)}
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })()}
                </div>
              ))
            )}
          </div>

          <div className="chat-footer">
            <div className={`chat-input-card${inputFocused ? " focused" : ""}`}>
              <div className="chat-input-row">
                <textarea
                  className="chat-textarea"
                  placeholder="Ask anything about your synced docs and calendar..."
                  value={question}
                  onChange={(e) => setQuestion(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      handleAsk();
                    }
                  }}
                  onFocus={() => setInputFocused(true)}
                  onBlur={() => setInputFocused(false)}
                />
                <button className="button-primary" onClick={handleAsk} disabled={answerLoading || !question.trim()}>
                  {answerLoading ? "Asking..." : "Ask"}
                </button>
              </div>
              <div className="chat-meta-row">
                <span className="text-muted">Scope: Drive · Calendar</span>
                {answerLoading && <span className="text-muted">Thinking…</span>}
              </div>
            </div>
          </div>
        </div>
      </section>

      <aside className="context-rail">
        <div className="card">
          <div className="card-header">
            <div>
              <div className="card-title">Upcoming events</div>
              <div className="card-subtitle">Upcoming events and suggested docs.</div>
            </div>
            <button className="button-secondary" onClick={loadRelevantNow} disabled={relevantLoading || !user}>
              {relevantLoading ? "Loading..." : "Refresh"}
            </button>
          </div>

        {relevantLoading && <div className="text-muted">Loading...</div>}
        {relevantError && <div style={{ color: "var(--danger)", fontSize: "0.85rem" }}>{relevantError}</div>}
        {!relevantLoading && !relevantError && relevant.length === 0 && (
          <div className="text-muted">No upcoming events with suggestions.</div>
        )}

        {!relevantLoading && !relevantError && relevant.length > 0 && (
          <div className="relevant-events-list">
            {relevant.slice(0, 5).map((item, idx) => (
              <div key={idx} className="relevant-event">
                <div className="relevant-event-title">{item.event?.title || "(No title)"}</div>
                <div className="text-muted">
                  {item.event?.start_time || item.event?.start || ""}
                  {item.event?.end_time || item.event?.end ? ` – ${item.event.end_time || item.event.end}` : ""}
                  {item.event?.location ? ` · ${item.event.location}` : ""}
                </div>
                {item.event?.description && (
                  <p className="text-muted" style={{ marginTop: "6px" }}>
                    {item.event.description.slice(0, 220)}
                    {item.event.description.length > 220 ? "…" : ""}
                  </p>
                )}
                {(item.docs || []).slice(0, 3).map((doc, dIdx) => (
                  <div key={dIdx} style={{ marginTop: "10px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <div className="card-title">
                        {doc.link ? (
                          <a href={doc.link} target="_blank" rel="noreferrer">
                            {doc.title || "Untitled"}
                          </a>
                        ) : (
                          doc.title || "Untitled"
                        )}
                      </div>
                      {doc.confidence !== undefined && (
                        <div className="badge-neutral">{`score: ${(doc.confidence * 100).toFixed(0)}%`}</div>
                      )}
                    </div>
                    {doc.snippet && (
                      <p className="text-muted" style={{ marginTop: "4px" }}>
                        {doc.snippet.slice(0, 240)}
                        {doc.snippet.length > 240 ? "…" : ""}
                      </p>
                    )}
                  </div>
                ))}
              </div>
            ))}
          </div>
        )}
        </div>

        {renderSourcesSummaryCard()}
        {renderActivitySummaryCard()}
      </aside>
    </div>
  );

  return (
    <div className="app-shell">
      <Sidebar
        activeSection={activeSection}
        onSelect={setActiveSection}
        isCollapsed={isSidebarCollapsed}
        onToggle={() => setIsSidebarCollapsed((prev) => !prev)}
      />

      <div className="app-main">
        <header className="topbar">
          <div className="topbar-left">
            <div className="badge">Azeryn</div>
            <div>
              <div className="topbar-title">Local Context Agent</div>
              <div className="topbar-subtitle">Ask across your Drive + Calendar</div>
            </div>
          </div>
          <div className="topbar-right" style={{ gap: "12px" }}>
            <span className="badge">Early technical preview</span>
            <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
              <div className="avatar">{user.picture ? <img src={user.picture} alt="avatar" /> : <span>{initials}</span>}</div>
              <div>
                <div className="card-title">{user.full_name || user.email}</div>
                <div className="text-muted">{user.email}</div>
              </div>
            </div>
          </div>
        </header>

        {activeSection === "home" && homeView}
        {activeSection === "search" && searchView}
        {activeSection === "sources" && sourcesView}
        {activeSection === "activity" && activityView}
        {activeSection === "settings" && settingsView}
      </div>
    </div>
  );
}
