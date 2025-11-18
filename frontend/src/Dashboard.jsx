import React, { useEffect, useState } from "react";
import { useAuth } from "./AuthContext";
import { apiGet, apiPost } from "./api";

const POLL_INTERVAL_MS = 3000;
const TERMINAL_STATUSES = new Set(["completed", "completed_with_errors", "failed"]);

export default function Dashboard() {
  const { user, csrfToken, refreshAuth } = useAuth();
  const [driveJobId, setDriveJobId] = useState(null);
  const [driveJob, setDriveJob] = useState(null);
  const [jobPolling, setJobPolling] = useState(false);

  const [question, setQuestion] = useState("");
  const [answer, setAnswer] = useState(null);
  const [answerLoading, setAnswerLoading] = useState(false);
  const [chatHistory, setChatHistory] = useState([]);
  const [disconnecting, setDisconnecting] = useState(false);

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
          if (data.status && TERMINAL_STATUSES.has(data.status)) {
            setJobPolling(false);
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

  const handleDriveIngest = async () => {
    try {
      const payload = {
        query: "",
        max_files: 5,
        reembed_all: false,
      };
      const job = await apiPost("/ingest/drive/start", payload, csrfToken);
      if (job && job.job_id) {
        setDriveJobId(job.job_id);
        setDriveJob(job);
        setJobPolling(true);
      } else {
        alert("Drive ingest did not return a job_id.");
      }
    } catch (err) {
      console.error("Drive ingest failed", err);
      alert("Drive ingest failed. Check backend logs.");
    }
  };

  const handleAsk = async () => {
    if (!question.trim()) {
      return;
    }
    setAnswerLoading(true);
    setAnswer(null);
    try {
      const payload = {
        query: question.trim(),
        k: 6,
        max_ctx_chars: 4000,
        allow_partial: true,
      };
      const response = await apiPost("/rag/answer", payload, csrfToken);
      setAnswer(response || null);
      setChatHistory((prev) => [
        { q: question.trim(), a: response?.answer || JSON.stringify(response, null, 2) },
        ...prev,
      ]);
    } catch (err) {
      console.error("Question failed", err);
      alert("Question failed. Check backend.");
    } finally {
      setAnswerLoading(false);
    }
  };

  const handleDisconnect = async () => {
    const confirmed = window.confirm(
      "This will delete your data and disconnect your Google account. Continue?"
    );
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

  const metrics = (driveJob && driveJob.metrics) || {};
  const found = metrics.found ?? 0;
  const ingested = metrics.ingested ?? 0;
  const errors = metrics.errors ?? 0;

  const initials = (user.full_name || user.email || "")
    .split(" ")
    .map((p) => p[0])
    .join("")
    .slice(0, 2)
    .toUpperCase();

  return (
    <main className="app-main">
      <header className="app-header">
        <div>
          <h1>Local Context Agent</h1>
          <div className="tagline">Your personal knowledge layer.</div>
        </div>
        <div className="user-info">
          <div className="avatar">
            {user.picture ? <img src={user.picture} alt="avatar" /> : <span>{initials}</span>}
          </div>
          <div className="user-text">
            <span className="user-name">{user.full_name || user.email}</span>
            <span className="user-email">{user.email}</span>
          </div>
        </div>
      </header>

      <div className="main-grid">
        <section className="card">
          <div className="card-header">
            <div>
              <h3 className="card-title">Ask about your docs</h3>
              <p className="card-subtext">Ask questions about your synced Google Drive and get concise answers grounded in your content.</p>
            </div>
          </div>

          <div className="composer">
            <textarea
              rows={4}
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Ask something about your synced documents..."
            />
            <div className="composer-footer">
              <button className="btn btn-primary" onClick={handleAsk} disabled={answerLoading || !question.trim()}>
                {answerLoading ? "Asking..." : "Ask"}
              </button>
            </div>
            <div className="placeholder">Tip: ask about itineraries, summaries, statuses, or next steps.</div>
          </div>

          <div className="answer-box">
            <strong>Answer</strong>
            {answer ? (
              <div className="answer-text">
                {answer.answer ? (
                  <p className="answer-text">{answer.answer}</p>
                ) : (
                  <pre className="answer-text">{JSON.stringify(answer, null, 2)}</pre>
                )}
                {(answer.retrieved !== undefined || answer.confidence !== undefined) && (
                  <div className="answer-meta">
                    {answer.retrieved !== undefined ? `Based on ${answer.retrieved} chunks` : ""}
                    {answer.retrieved !== undefined && answer.confidence !== undefined ? " · " : ""}
                    {answer.confidence !== undefined ? `confidence: ${answer.confidence}` : ""}
                  </div>
                )}
              </div>
            ) : (
              <p className="placeholder">Your answer will appear here once you ask a question about your docs.</p>
            )}
          </div>
        </section>

        <aside className="card">
          <div className="card-header">
            <div>
              <h3 className="card-title">Ingestion status</h3>
              <p className="card-subtext">Monitor your Drive ingest and rerun as needed.</p>
            </div>
            <div
              className={[
                "status-pill",
                driveJob?.status ? `status-${(driveJob.status || "").toLowerCase()}` : "status-queued",
              ].join(" ")}
            >
              {driveJob?.status ? driveJob.status : "No job yet"}
            </div>
          </div>

          {driveJob ? (
            <>
              <p className="muted-line">Job ID: {driveJob.job_id}</p>
              <div className="metrics-grid">
                <div className="metric">
                  <span className="metric-label">Found</span>
                  <span className="metric-value">{found}</span>
                </div>
                <div className="metric">
                  <span className="metric-label">Ingested</span>
                  <span className="metric-value">{ingested}</span>
                </div>
                <div className="metric">
                  <span className="metric-label">Errors</span>
                  <span className="metric-value">{errors}</span>
                </div>
              </div>
              {driveJob.error_summary && <div className="error-text">Error: {driveJob.error_summary}</div>}
            </>
          ) : (
            <p className="placeholder muted-line">No ingestion job yet. Start a run to index your Drive files.</p>
          )}

          <div className="ingest-actions">
            <button className="btn btn-primary" onClick={handleDriveIngest} disabled={driveJob && driveJob.status === "running"}>
              Run Drive Ingest
            </button>
            <button
              className="btn btn-secondary btn-danger"
              onClick={handleDisconnect}
              disabled={disconnecting}
            >
              {disconnecting ? "Disconnecting..." : "Disconnect / Delete my data"}
            </button>
          </div>
        </aside>
      </div>
    </main>
  );
}
