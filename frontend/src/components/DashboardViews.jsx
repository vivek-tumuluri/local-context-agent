import React from "react";
import { formatJobMeta, formatTimestamp, statusPillClass } from "../dashboardUtils";
import {
  ActivityIcon,
  CalendarIcon,
  DriveIcon,
  RefreshIcon,
  RelevantIcon,
  SearchIcon,
  SourcesIcon,
  SparkIcon,
} from "./Icons";

export function Panel({ title, subtitle, actions, children, className = "", accent = "violet", icon: Icon }) {
  return (
    <section className={`panel panel-accent-${accent} ${className}`}>
      {(title || actions) && (
        <div className="panel-header">
          <div className="panel-heading">
            {Icon && (
              <span className="panel-icon">
                <Icon />
              </span>
            )}
            <div>
              {title && <div className="panel-title">{title}</div>}
              {subtitle && <div className="panel-subtitle">{subtitle}</div>}
            </div>
          </div>
          {actions && <div className="panel-actions">{actions}</div>}
        </div>
      )}
      {children}
    </section>
  );
}

function EmptyState({ icon: Icon = SparkIcon, title, copy }) {
  return (
    <div className="empty-rich">
      <div className="empty-icon">
        {React.createElement(Icon)}
      </div>
      <div>
        <div className="empty-title">{title}</div>
        {copy && <div className="empty-copy">{copy}</div>}
      </div>
    </div>
  );
}

export function TabBar({ items, active, onChange }) {
  return (
    <div className="tab-bar">
      {items.map((item) => (
        <button
          key={item.id}
          type="button"
          className={`tab-button${active === item.id ? " is-active" : ""}`}
          onClick={() => onChange(item.id)}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}

function SourceLink({ source, index }) {
  const label = source.title || source.doc_id || source.link || source.source || `Source ${index + 1}`;
  if (source.link) {
    return (
      <a className="source-chip" href={source.link} target="_blank" rel="noreferrer">
        <span>{index + 1}</span>
        {label}
      </a>
    );
  }
  return (
    <span className="source-chip">
      <span>{index + 1}</span>
      {label}
    </span>
  );
}

export function AskView({
  question,
  setQuestion,
  messages,
  answerLoading,
  inputFocused,
  setInputFocused,
  handleAsk,
  suggestionPrompts,
  relevant,
  relevantLoading,
  relevantError,
  loadRelevantNow,
  driveStatus,
  calendarStatus,
  ingestError,
  lastDriveJob,
  jobs,
  jobsLoading,
  jobsError,
  onDriveSync,
  onOpenSources,
  onOpenActivity,
}) {
  return (
    <main className="dashboard-grid page-surface">
      <section className="ask-canvas">
        <div className="ask-canvas-header">
          <div className="ask-title-row">
            <span className="panel-icon ask-header-icon">
              <SparkIcon />
            </span>
            <div>
              <h2>Ask Azeryn</h2>
              <p>Answers grounded in your synced workspace.</p>
            </div>
          </div>
          <div className="ask-scope-pills">
            <span>Drive</span>
            <span>Calendar</span>
            <span>Citations</span>
          </div>
        </div>

        <div className="chat-stream open-chat-stream">
          {messages.length === 0 ? (
            <div className="ask-empty-state">
              <div className="empty-icon">
                <SparkIcon />
              </div>
              <h3>What do you want to understand?</h3>
              <p>Ask about indexed documents, upcoming meetings, notes, plans, or source material.</p>
              <div className="ask-suggestion-grid">
                {suggestionPrompts.map((text) => (
                  <button key={text} className="chip ask-suggestion-chip" type="button" onClick={() => setQuestion(text)}>
                    {text}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            messages.map((message) => (
              <article key={message.id} className={`message ${message.role === "user" ? "message-user" : "message-assistant"}`}>
                <div className="message-label">{message.role === "user" ? "You" : "Azeryn"}</div>
                <div className="message-text">{message.text}</div>
                {message.role === "assistant" && message.meta && (
                  <div className="answer-meta">
                    {message.meta.retrieved !== undefined && <span className="meta-pill">{message.meta.retrieved} chunks</span>}
                    {message.meta.confidence !== undefined && (
                      <span className="meta-pill">confidence {Number(message.meta.confidence).toFixed(3)}</span>
                    )}
                    {Array.isArray(message.meta.sources) && message.meta.sources.length > 0 && (
                      <div className="source-line">
                        {message.meta.sources.map((source, index) => (
                          <SourceLink key={source.doc_id || source.link || index} source={source} index={index} />
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </article>
            ))
          )}
        </div>

        <div className="floating-composer-wrap">
          <div className={`floating-composer${inputFocused ? " is-focused" : ""}`}>
            <textarea
              className="floating-composer-input"
              placeholder="Ask about your synced workspace..."
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onFocus={() => setInputFocused(true)}
              onBlur={() => setInputFocused(false)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  handleAsk();
                }
              }}
            />
            <button
              className={`primary-button ask-button floating-ask-button${answerLoading ? " is-loading" : ""}`}
              type="button"
              onClick={handleAsk}
              disabled={answerLoading || !question.trim()}
            >
              {answerLoading ? "Asking" : "Ask"}
            </button>
          </div>
          <div className="floating-composer-meta">
            <span>Top 6 chunks</span>
            <span>Strict citation grounding</span>
          </div>
        </div>
      </section>

      <aside className="right-rail">
        <ContextPanel
          relevant={relevant}
          relevantLoading={relevantLoading}
          relevantError={relevantError}
          loadRelevantNow={loadRelevantNow}
          driveStatus={driveStatus}
          calendarStatus={calendarStatus}
          ingestError={ingestError}
          lastDriveJob={lastDriveJob}
          jobs={jobs}
          jobsLoading={jobsLoading}
          jobsError={jobsError}
          onDriveSync={onDriveSync}
          onOpenSources={onOpenSources}
          onOpenActivity={onOpenActivity}
        />
      </aside>
    </main>
  );
}

function ContextPanel({
  relevant,
  relevantLoading,
  relevantError,
  loadRelevantNow,
  driveStatus,
  calendarStatus,
  ingestError,
  lastDriveJob,
  jobs,
  jobsLoading,
  jobsError,
  onDriveSync,
  onOpenSources,
  onOpenActivity,
}) {
  return (
    <section className="context-panel">
      <div className="context-panel-header">
        <div>
          <div className="context-title">Context</div>
          <div className="context-subtitle">What Azeryn can use right now</div>
        </div>
        <button className="ghost-button compact-action" type="button" onClick={loadRelevantNow} disabled={relevantLoading}>
          <RefreshIcon />
          <span>Refresh</span>
        </button>
      </div>

      {ingestError && <div className="error-text">{ingestError}</div>}

      <div className="context-section">
        <div className="context-section-title">
          <SourcesIcon />
          Sources
        </div>
        <div className="status-card-list context-status-list">
          <MiniSourceStatus icon={DriveIcon} title="Google Drive" status={driveStatus} tone="green" />
          <MiniSourceStatus icon={CalendarIcon} title="Calendar" status={calendarStatus} tone="violet" />
        </div>
        <div className="context-inline-actions">
          <button className="secondary-button icon-label-button" type="button" onClick={onDriveSync} disabled={driveStatus.code === "running"}>
            <DriveIcon />
            Sync Drive
          </button>
          <button className="ghost-button icon-label-button" type="button" onClick={onOpenSources}>
            <SourcesIcon />
            Manage
          </button>
        </div>
        <div className="muted tiny context-note">{lastDriveJob ? formatJobMeta(lastDriveJob) : "No Drive job yet."}</div>
      </div>

      <div className="context-section">
        <div className="context-section-title">
          <CalendarIcon />
          Relevant Now
        </div>
        {relevantLoading && (
          <div className="skeleton-stack compact-skeleton">
            <div className="skeleton-line wide" />
            <div className="skeleton-line" />
          </div>
        )}
        {relevantError && <div className="error-text">{relevantError}</div>}
        {!relevantLoading && !relevantError && relevant.length === 0 && (
          <EmptyState icon={CalendarIcon} title="No upcoming context" copy="Calendar matches will appear here." />
        )}
        {!relevantLoading && !relevantError && relevant.length > 0 && (
          <div className="event-list context-event-list">
            {relevant.slice(0, 3).map((item, index) => (
              <div key={index} className="event-item">
                <div className="event-title">{item.event?.title || "(No title)"}</div>
                <div className="muted tiny">{item.event?.start_time || item.event?.start || ""}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="context-section">
        <div className="context-section-title">
          <ActivityIcon />
          Recent Activity
        </div>
        {jobsLoading && (
          <div className="skeleton-stack compact-skeleton">
            <div className="skeleton-line wide" />
            <div className="skeleton-line" />
          </div>
        )}
        {jobsError && <div className="error-text">Could not load activity.</div>}
        {!jobsLoading && !jobsError && jobs.length === 0 && (
          <EmptyState icon={ActivityIcon} title="No jobs yet" copy="Sync jobs will appear here." />
        )}
        {!jobsLoading && !jobsError && jobs.length > 0 && (
          <div className="mini-table context-activity-list">
            {jobs.slice(0, 3).map((job) => (
              <div key={job.job_id || job.id} className={`mini-row job-state-${(job.status || "unknown").toLowerCase()}`}>
                <span className="mini-row-icon">
                  {job.source === "calendar" || job.kind === "calendar" ? <CalendarIcon /> : <DriveIcon />}
                </span>
                <div className="mini-row-main">
                  <div className="mini-row-title">{job.source === "calendar" || job.kind === "calendar" ? "Calendar ingest" : "Drive ingest"}</div>
                  <div className="muted tiny">{formatTimestamp(job.created_at || job.started_at)}</div>
                </div>
                <span className={statusPillClass(job.status)}>{job.status || "unknown"}</span>
              </div>
            ))}
          </div>
        )}
        <button className="ghost-button compact-action context-view-all" type="button" onClick={onOpenActivity}>
          <ActivityIcon />
          <span>View activity</span>
        </button>
      </div>
    </section>
  );
}

export function RelevantPanel({ relevant, relevantLoading, relevantError, loadRelevantNow }) {
  return (
    <Panel
      title="Relevant Now"
      subtitle="Upcoming events and matched docs."
      accent="cyan"
      icon={CalendarIcon}
      actions={
        <button className={`ghost-button compact-action${relevantLoading ? " is-loading" : ""}`} type="button" onClick={loadRelevantNow} disabled={relevantLoading}>
          <RefreshIcon />
          <span>Refresh</span>
        </button>
      }
    >
      {relevantLoading && (
        <div className="skeleton-stack">
          <div className="skeleton-line wide" />
          <div className="skeleton-line" />
          <div className="skeleton-line short" />
        </div>
      )}
      {relevantError && <div className="error-text">{relevantError}</div>}
      {!relevantLoading && !relevantError && relevant.length === 0 && (
        <EmptyState
          icon={CalendarIcon}
          title="No upcoming context"
          copy="Calendar matches will appear when events have related docs."
        />
      )}
      {!relevantLoading && !relevantError && relevant.length > 0 && (
        <div className="event-list">
          {relevant.slice(0, 5).map((item, index) => (
            <div key={index} className="event-item">
              <div className="event-title">{item.event?.title || "(No title)"}</div>
              <div className="muted tiny">
                {item.event?.start_time || item.event?.start || ""}
                {item.event?.location ? ` · ${item.event.location}` : ""}
              </div>
              {(item.docs || []).slice(0, 2).map((doc, docIndex) => (
                <div key={docIndex} className="event-doc">
                  {doc.link ? (
                    <a href={doc.link} target="_blank" rel="noreferrer">
                      {doc.title || "Untitled"}
                    </a>
                  ) : (
                    doc.title || "Untitled"
                  )}
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

function RelevantDocChip({ doc, index }) {
  const label = doc.title || doc.name || "Untitled";
  const score = typeof doc.score === "number" ? doc.score : typeof doc.confidence === "number" ? doc.confidence : null;
  const content = (
    <>
      <span className="relevant-doc-index">{index + 1}</span>
      <span className="relevant-doc-title">{label}</span>
      {score !== null && <span className="relevant-doc-score">{Math.round(score * 100)}%</span>}
    </>
  );

  if (doc.link) {
    return (
      <a className="relevant-doc-chip" href={doc.link} target="_blank" rel="noreferrer">
        {content}
      </a>
    );
  }

  return <span className="relevant-doc-chip">{content}</span>;
}

function RelevantEventItem({ item, index }) {
  const event = item.event || {};
  const docs = item.docs || [];
  const eventTime = event.start_time || event.start || "";
  const location = event.location || "";

  return (
    <article className="relevant-event">
      <div className="relevant-event-marker">
        <span>{index + 1}</span>
      </div>
      <div className="relevant-event-content">
        <div className="relevant-event-kicker">
          <CalendarIcon />
          <span>{eventTime || "Upcoming"}</span>
          {location && <span>{location}</span>}
        </div>
        <h3>{event.title || "(No title)"}</h3>
        {docs.length > 0 ? (
          <div className="relevant-doc-list">
            {docs.slice(0, 5).map((doc, docIndex) => (
              <RelevantDocChip key={doc.link || doc.title || docIndex} doc={doc} index={docIndex} />
            ))}
          </div>
        ) : (
          <div className="relevant-event-note">No matched docs for this event yet.</div>
        )}
      </div>
    </article>
  );
}

function RelevantPageSkeleton() {
  return (
    <div className="relevant-agenda relevant-skeleton" aria-label="Loading relevant context">
      {[0, 1, 2].map((item) => (
        <div key={item} className="relevant-event">
          <div className="relevant-event-marker skeleton-marker" />
          <div className="relevant-event-content">
            <div className="skeleton-line short" />
            <div className="skeleton-line wide" />
            <div className="relevant-doc-list">
              <div className="skeleton-pill" />
              <div className="skeleton-pill" />
              <div className="skeleton-pill short" />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function RelevantPageEmpty({ loadRelevantNow, relevantLoading }) {
  return (
    <div className="relevant-empty">
      <div className="empty-icon relevant-empty-icon">
        <CalendarIcon />
      </div>
      <h3>No upcoming context</h3>
      <p>Calendar matches will appear here when upcoming events have related Drive or Calendar context.</p>
      <button className={`secondary-button icon-label-button${relevantLoading ? " is-loading" : ""}`} type="button" onClick={loadRelevantNow} disabled={relevantLoading}>
        <RefreshIcon />
        Refresh
      </button>
    </div>
  );
}

export function SourcesSummaryPanel({ driveStatus, calendarStatus, ingestError, lastDriveJob, onDriveSync, onOpenSources }) {
  return (
    <Panel title="Sources" subtitle="Connection and ingest status." accent="green" icon={SourcesIcon}>
      {ingestError && <div className="error-text">{ingestError}</div>}
      <div className="status-card-list">
        <MiniSourceStatus icon={DriveIcon} title="Google Drive" status={driveStatus} tone="green" />
        <MiniSourceStatus icon={CalendarIcon} title="Calendar" status={calendarStatus} tone="violet" />
      </div>
      <div className="muted tiny">{lastDriveJob ? formatJobMeta(lastDriveJob) : "No Drive job yet."}</div>
      <div className="button-row">
        <button className="secondary-button icon-label-button" type="button" onClick={onDriveSync} disabled={driveStatus.code === "running"}>
          <DriveIcon />
          Sync Drive
        </button>
        <button className="ghost-button icon-label-button" type="button" onClick={onOpenSources}>
          <SourcesIcon />
          Manage
        </button>
      </div>
    </Panel>
  );
}

function MiniSourceStatus({ icon: Icon, title, status, tone }) {
  return (
    <div className={`mini-source-status mini-source-${tone}`}>
      <span className="mini-source-icon">
        {React.createElement(Icon)}
      </span>
      <div>
        <div className="mini-source-title">{title}</div>
        <div className="muted tiny">{status.code === "succeeded" ? "Connected and indexed" : "Waiting for sync"}</div>
      </div>
      <span className={statusPillClass(status.code)}>{status.label}</span>
    </div>
  );
}

export function ActivitySummaryPanel({ jobs, jobsLoading, jobsError, onOpenActivity }) {
  return (
    <Panel
      title="Activity"
      subtitle="Latest background jobs."
      accent="violet"
      icon={ActivityIcon}
      actions={
        <button className="ghost-button compact-action" type="button" onClick={onOpenActivity}>
          <ActivityIcon />
          <span>View all</span>
        </button>
      }
    >
      {jobsLoading && (
        <div className="skeleton-stack">
          <div className="skeleton-line wide" />
          <div className="skeleton-line" />
        </div>
      )}
      {jobsError && <div className="error-text">Could not load activity.</div>}
      {!jobsLoading && !jobsError && jobs.length === 0 && (
        <EmptyState icon={ActivityIcon} title="No jobs yet" copy="Drive and Calendar sync jobs will appear here." />
      )}
      {!jobsLoading && !jobsError && jobs.length > 0 && (
        <div className="mini-table">
          {jobs.slice(0, 4).map((job) => (
            <div key={job.job_id || job.id} className={`mini-row job-state-${(job.status || "unknown").toLowerCase()}`}>
              <span className="mini-row-icon">
                {job.source === "calendar" || job.kind === "calendar" ? <CalendarIcon /> : <DriveIcon />}
              </span>
              <div className="mini-row-main">
                <div className="mini-row-title">{job.source === "calendar" || job.kind === "calendar" ? "Calendar ingest" : "Drive ingest"}</div>
                <div className="muted tiny">{formatTimestamp(job.created_at || job.started_at)}</div>
              </div>
              <span className={statusPillClass(job.status)}>{job.status || "unknown"}</span>
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

export function SearchView({
  searchQuery,
  setSearchQuery,
  searchSource,
  setSearchSource,
  searchResults,
  searchLoading,
  searchError,
  lastSearchedQuery,
  handleSearch,
}) {
  const queryLength = searchQuery.trim().length;
  const hasSearched = !!lastSearchedQuery || searchResults.length > 0 || searchLoading || !!searchError;
  const activeSourceLabel = searchSource === "all" ? "All context" : searchSource.charAt(0).toUpperCase() + searchSource.slice(1);

  return (
    <main className="single-view search-page">
      <section className="search-hero">
        <div className="search-hero-copy">
          <div className="panel-icon">
            <SearchIcon />
          </div>
          <div>
            <h2>Search your synced workspace</h2>
            <p>Find Drive files, calendar context, notes, and source material with semantic search.</p>
          </div>
        </div>

        <form className="search-input-shell" onSubmit={handleSearch}>
          <SearchIcon />
          <input
            type="search"
            placeholder="Search across Drive and Calendar..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
          <button className={`primary-button${searchLoading ? " is-loading" : ""}`} type="submit" disabled={searchLoading || !searchQuery.trim()}>
            {searchLoading ? "Searching" : "Search"}
          </button>
        </form>

        <SearchFilterPills active={searchSource} onChange={setSearchSource} />
      </section>

      {(hasSearched || queryLength > 0) && <section className="search-results-surface">
        {searchError && <div className="error-text">{searchError}</div>}

        {searchLoading && <SearchSkeleton />}

        {!searchLoading && !searchError && queryLength > 0 && queryLength < 3 && (
          <div className="search-subtle-hint">Keep typing to search your synced workspace.</div>
        )}

        {!searchLoading && !searchError && hasSearched && queryLength >= 3 && searchResults.length === 0 && (
          <SearchEmptyState
            title="No matching context found"
            copy="Try a broader phrase or switch the source filter."
          />
        )}

        {!searchLoading && searchResults.length > 0 && (
          <>
            <div className="search-summary">
              <span>{searchResults.length} results</span>
              {lastSearchedQuery && <span>for "{lastSearchedQuery}"</span>}
              <span>{activeSourceLabel}</span>
            </div>
            <div className="result-list search-result-list">
              {searchResults.map((hit, index) => (
                <SearchResult key={hit.id || index} hit={hit} />
              ))}
            </div>
          </>
        )}
      </section>}
    </main>
  );
}

function SearchFilterPills({ active, onChange }) {
  const items = [
    { id: "all", label: "All context", icon: SparkIcon },
    { id: "drive", label: "Drive", icon: DriveIcon },
    { id: "calendar", label: "Calendar", icon: CalendarIcon },
  ];
  return (
    <div className="search-filter-pills">
      {items.map((item) => {
        const Icon = item.icon;
        return (
          <button
            key={item.id}
            type="button"
            className={`search-filter-pill${active === item.id ? " is-active" : ""}`}
            onClick={() => onChange(item.id)}
          >
            <Icon />
            {item.label}
          </button>
        );
      })}
    </div>
  );
}

function SearchEmptyState({ title, copy }) {
  return (
    <div className="search-empty-state">
      <div className="empty-icon">
        <SearchIcon />
      </div>
      <div>
        <div className="empty-title">{title}</div>
        <div className="empty-copy">{copy}</div>
      </div>
    </div>
  );
}

function SearchSkeleton() {
  return (
    <div className="search-skeleton-list">
      {[0, 1, 2].map((item) => (
        <div key={item} className="search-skeleton-card">
          <div className="skeleton-line wide" />
          <div className="skeleton-line" />
          <div className="skeleton-line short" />
        </div>
      ))}
    </div>
  );
}

function SearchResult({ hit }) {
  const meta = hit.meta || {};
  const title = meta.title || meta.name || "(untitled)";
  const docId = meta.doc_id || meta.id || "";
  const source = (meta.source || "unknown").toLowerCase();
  const link = meta.link || meta.webViewLink || (source === "drive" && docId ? `https://drive.google.com/file/d/${docId}/view` : null);
  const confidence = typeof hit.confidence === "number" ? hit.confidence : typeof hit.similarity === "number" ? hit.similarity : null;
  const SourceIcon = source === "calendar" ? CalendarIcon : source === "drive" ? DriveIcon : SparkIcon;
  return (
    <article className="result-row search-result-card">
      <div className="search-result-body">
        <span className="search-result-icon">
          <SourceIcon />
        </span>
        <div className="search-result-main">
          <div className="result-title">
            {link ? (
              <a href={link} target="_blank" rel="noreferrer">
                {title}
              </a>
            ) : (
              title
            )}
          </div>
          <div className="muted tiny">
            {source}
            {docId ? ` · ${docId}` : ""}
          </div>
          <p>{(hit.text || "").slice(0, 280)}{hit.text && hit.text.length > 280 ? "..." : ""}</p>
          {link && (
            <div className="search-result-actions">
              <a className="ghost-button compact-action" href={link} target="_blank" rel="noreferrer">
                Open source
              </a>
            </div>
          )}
        </div>
      </div>
      {confidence !== null && <span className="score-badge">{(confidence * 100).toFixed(0)}%</span>}
    </article>
  );
}

export function RelevantView({ relevant = [], relevantLoading, relevantError, loadRelevantNow }) {
  return (
    <main className="single-view relevant-page">
      <section className="relevant-canvas">
        <div className="relevant-hero">
          <div className="relevant-hero-copy">
            <span className="panel-icon relevant-hero-icon">
              <RelevantIcon />
            </span>
            <div>
              <h2>Relevant Now</h2>
              <p>Upcoming events paired with the workspace context you are most likely to need.</p>
            </div>
          </div>
          <button className={`secondary-button icon-label-button relevant-refresh-button${relevantLoading ? " is-loading" : ""}`} type="button" onClick={loadRelevantNow} disabled={relevantLoading}>
            <RefreshIcon />
            Refresh
          </button>
        </div>

        {relevantError && <div className="error-text relevant-error">{relevantError}</div>}
        {relevantLoading && <RelevantPageSkeleton />}
        {!relevantLoading && !relevantError && relevant.length === 0 && (
          <RelevantPageEmpty loadRelevantNow={loadRelevantNow} relevantLoading={relevantLoading} />
        )}
        {!relevantLoading && !relevantError && relevant.length > 0 && (
          <div className="relevant-agenda">
            {relevant.slice(0, 8).map((item, index) => (
              <RelevantEventItem key={item.event?.id || item.event?.title || index} item={item} index={index} />
            ))}
          </div>
        )}
      </section>
    </main>
  );
}

export function SourcesView({
  isDriveConnected,
  isCalendarConnected,
  driveStatus,
  calendarStatus,
  lastDriveJob,
  lastCalendarJob,
  handleDriveIngest,
  handleCalendarIngest,
  handleDisconnect,
  disconnecting,
  ingestError,
}) {
  return (
    <main className="single-view">
      <Panel title="Sources" subtitle="Manage Google integrations and ingestion." className="fill-panel" accent="green" icon={SourcesIcon}>
        {ingestError && <div className="error-text">{ingestError}</div>}
        <div className="source-grid">
          <SourceCard
            title="Google Drive"
            connected={isDriveConnected}
            status={driveStatus}
            job={lastDriveJob}
            onSync={handleDriveIngest}
            syncLabel="Run Drive ingest"
            extraAction={
              <button className="ghost-button icon-label-button" type="button" onClick={handleDisconnect} disabled={disconnecting}>
                <SourcesIcon />
                {disconnecting ? "Disconnecting" : "Disconnect"}
              </button>
            }
          />
          <SourceCard
            title="Google Calendar"
            connected={isCalendarConnected}
            status={calendarStatus}
            job={lastCalendarJob}
            onSync={handleCalendarIngest}
            syncLabel="Run Calendar ingest"
          />
        </div>
      </Panel>
    </main>
  );
}

function SourceCard({ title, connected, status, job, onSync, syncLabel, extraAction }) {
  return (
    <div className="source-card">
      <div className="source-card-head">
        <div>
          <div className="source-card-title">
            {title === "Google Drive" ? <DriveIcon /> : <CalendarIcon />}
            {title}
          </div>
          <div className="muted tiny">{connected ? "Connected" : "Not connected"}</div>
        </div>
        <span className={statusPillClass(status.code)}>{status.label}</span>
      </div>
      <div className="muted">{job ? `${formatTimestamp(job.created_at || job.started_at)} · ${formatJobMeta(job)}` : "No ingest job yet."}</div>
      <div className="button-row">
        <button className={`primary-button icon-label-button${status.code === "running" ? " is-loading" : ""}`} type="button" onClick={onSync} disabled={status.code === "running"}>
          {title === "Google Drive" ? <DriveIcon /> : <CalendarIcon />}
          {status.code === "running" ? "Syncing" : syncLabel}
        </button>
        {extraAction}
      </div>
    </div>
  );
}

export function ActivityView({ jobs, jobsLoading, jobsError, activityFilter, setActivityFilter }) {
  const filteredJobs =
    activityFilter === "drive"
      ? jobs.filter((j) => j.source === "drive" || j.kind === "drive")
      : activityFilter === "calendar"
        ? jobs.filter((j) => j.source === "calendar" || j.kind === "calendar")
        : jobs;

  return (
    <main className="single-view">
      <Panel title="Activity" subtitle="Ingestion history and job state." className="fill-panel" accent="violet" icon={ActivityIcon}>
        <TabBar
          items={[
            { id: "all", label: "All" },
            { id: "drive", label: "Drive" },
            { id: "calendar", label: "Calendar" },
          ]}
          active={activityFilter}
          onChange={setActivityFilter}
        />
        {jobsLoading && (
          <div className="skeleton-stack">
            <div className="skeleton-line wide" />
            <div className="skeleton-line" />
            <div className="skeleton-line short" />
          </div>
        )}
        {jobsError && <div className="error-text">Could not load activity.</div>}
        {!jobsLoading && !jobsError && filteredJobs.length === 0 && <div className="empty-table">No jobs match this view.</div>}
        {!jobsLoading && !jobsError && filteredJobs.length > 0 && (
          <div className="job-table">
            <div className="job-row job-head">
              <span>Source</span>
              <span>Status</span>
              <span>Progress</span>
              <span>Started</span>
            </div>
            {filteredJobs.slice(0, 60).map((job) => (
              <div key={job.job_id || job.id} className="job-row">
                <span>{job.source === "calendar" || job.kind === "calendar" ? "Calendar ingest" : "Drive ingest"}</span>
                <span className={statusPillClass(job.status)}>{job.status || "unknown"}</span>
                <span>{formatJobMeta(job)}</span>
                <span>{formatTimestamp(job.created_at || job.started_at)}</span>
              </div>
            ))}
          </div>
        )}
      </Panel>
    </main>
  );
}

export function SettingsView({ user, handleDisconnect, disconnecting }) {
  return (
    <main className="single-view">
      <Panel title="Settings" subtitle="Account and data controls." className="settings-panel" accent="violet" icon={SparkIcon}>
        <div className="settings-grid">
          <div>
            <div className="label">Profile</div>
            <div>{user.full_name || "Unknown"}</div>
            <div className="muted">{user.email}</div>
          </div>
          <div>
            <div className="label">Session</div>
            <div>Authenticated Google workspace</div>
            <div className="muted">Early technical preview</div>
          </div>
        </div>
        <div className="danger-zone">
          <div>
            <div className="label">Disconnect</div>
            <div className="muted">Deletes synced data and disconnects Google credentials.</div>
          </div>
          <button className="danger-button" type="button" onClick={handleDisconnect} disabled={disconnecting}>
            {disconnecting ? "Disconnecting" : "Disconnect account"}
          </button>
        </div>
      </Panel>
    </main>
  );
}
