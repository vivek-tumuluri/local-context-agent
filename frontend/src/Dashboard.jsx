import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "./AuthContext";
import { apiGet, apiPost, fetchRelevantNow, searchKnowledgeBase, startCalendarIngest } from "./api";
import AppShell from "./components/AppShell";
import {
  ActivityView,
  AskView,
  RelevantView,
  SearchView,
  SettingsView,
  SourcesView,
} from "./components/DashboardViews";
import { TERMINAL_STATUSES } from "./dashboardUtils";
import { useIngestStatus } from "./hooks/useIngestStatus";

const POLL_INTERVAL_MS = 3000;

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
  const [activeSection, setActiveSection] = useState("ask");
  const [activityFilter, setActivityFilter] = useState("all");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchSource, setSearchSource] = useState("all");
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState(null);
  const [lastSearchedQuery, setLastSearchedQuery] = useState("");
  const [refreshingStatus, setRefreshingStatus] = useState(false);
  const searchRequestIdRef = useRef(0);

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

  const suggestionPrompts = useMemo(
    () => [
      "What should I review for my next meeting?",
      "Summarize the docs for my upcoming presentation.",
      "What are the next steps from my latest notes?",
    ],
    []
  );

  const loadRelevantNow = useCallback(async () => {
    if (!user || !csrfToken) return;
    setRelevantLoading(true);
    setRelevantError(null);
    try {
      const data = await fetchRelevantNow(csrfToken);
      setRelevant(data?.results || []);
    } catch (err) {
      console.error("Failed to load relevant now", err);
      setRelevantError("Failed to load relevant context.");
    } finally {
      setRelevantLoading(false);
    }
  }, [csrfToken, user]);

  useEffect(() => {
    if (!driveJobId || !jobPolling) return undefined;
    let cancelled = false;

    const intervalId = setInterval(async () => {
      try {
        const data = await apiGet(`/ingest/jobs/${driveJobId}`);
        if (!cancelled && data) {
          setDriveJob(data);
          const status = (data.status || "").toLowerCase();
          reloadIngestStatus();
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
  }, [driveJobId, jobPolling, refreshAuth, reloadIngestStatus]);

  useEffect(() => {
    loadRelevantNow();
  }, [loadRelevantNow]);

  useEffect(() => {
    const status = (driveJob?.status || "").toLowerCase();
    if (status && TERMINAL_STATUSES.has(status)) {
      reloadIngestStatus();
    }
  }, [driveJob?.status, reloadIngestStatus]);

  const effectiveDriveStatus = useMemo(() => {
    const ready = !!user?.drive_ready;
    const base = driveStatus || { label: "Not synced yet", code: "none" };
    if (base.code === "running" || base.code === "failed") return base;
    if (ready) return { label: "Synced", code: "succeeded" };
    return base;
  }, [driveStatus, user]);

  const effectiveCalendarStatus = useMemo(() => {
    const ready = !!user?.calendar_ready;
    const base = calendarStatus || { label: "Not synced yet", code: "none" };
    if (base.code === "running" || base.code === "failed") return base;
    if (ready) return { label: "Synced", code: "succeeded" };
    return base;
  }, [calendarStatus, user]);

  const initials = useMemo(() => {
    return (user?.full_name || user?.email || "")
      .split(" ")
      .map((part) => part[0])
      .join("")
      .slice(0, 2)
      .toUpperCase();
  }, [user]);

  const handleDriveIngest = async () => {
    try {
      setIngestError(null);
      const job = await apiPost(
        "/ingest/drive/start",
        {
          query: "",
          max_files: 650,
          reembed_all: false,
        },
        csrfToken
      );
      if (job?.job_id) {
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
      setIngestError(
        err?.message?.includes("503")
          ? "Ingestion is temporarily unavailable because the worker is offline."
          : "Drive ingest failed. Check backend logs."
      );
    }
  };

  const handleCalendarIngest = async () => {
    try {
      setIngestError(null);
      const job = await startCalendarIngest({ force_reembed: false }, csrfToken);
      if (job?.job_id) {
        reloadIngestStatus();
        setActiveSection("activity");
      } else {
        alert("Calendar ingest did not return a job_id.");
      }
    } catch (err) {
      console.error("Calendar ingest failed", err);
      setIngestError(
        err?.message?.includes("503")
          ? "Ingestion is temporarily unavailable because the worker is offline."
          : "Calendar ingest failed. Check backend logs."
      );
    }
  };

  const handleAsk = async () => {
    if (!question.trim()) return;

    const prompt = question.trim();
    setMessages((prev) => [...prev, { id: `user-${Date.now()}`, role: "user", text: prompt }]);
    setQuestion("");
    setAnswerLoading(true);

    try {
      const response = await apiPost(
        "/rag/answer",
        {
          query: prompt,
          k: 6,
          max_ctx_chars: 4000,
          allow_partial: true,
        },
        csrfToken
      );
      setMessages((prev) => [
        ...prev,
        {
          id: `assistant-${Date.now()}`,
          role: "assistant",
          text: response?.answer || JSON.stringify(response, null, 2),
          meta: {
            retrieved: response?.retrieved,
            confidence: response?.confidence,
            sources: Array.isArray(response?.sources) ? response.sources : [],
          },
        },
      ]);
    } catch (err) {
      console.error("Question failed", err);
      alert("Question failed. Check backend.");
    } finally {
      setAnswerLoading(false);
    }
  };

  const runSearch = useCallback(async (queryOverride) => {
    const query = (queryOverride ?? searchQuery).trim();
    if (!query || !csrfToken) return;
    const requestId = ++searchRequestIdRef.current;
    setSearchLoading(true);
    setSearchError(null);
    try {
      const response = await searchKnowledgeBase(
        {
          query,
          k: 8,
          source: searchSource === "all" ? undefined : searchSource,
        },
        csrfToken
      );
      if (requestId !== searchRequestIdRef.current) return;
      setSearchResults(response?.results || []);
      setLastSearchedQuery(query);
    } catch (err) {
      if (requestId !== searchRequestIdRef.current) return;
      console.error("Search failed", err);
      setSearchError("Search failed. Please try again.");
    } finally {
      if (requestId === searchRequestIdRef.current) {
        setSearchLoading(false);
      }
    }
  }, [csrfToken, searchQuery, searchSource]);

  useEffect(() => {
    if (activeSection !== "search") return undefined;
    const query = searchQuery.trim();
    if (query.length < 3) {
      searchRequestIdRef.current += 1;
      setSearchResults([]);
      setSearchError(null);
      setSearchLoading(false);
      setLastSearchedQuery("");
      return undefined;
    }

    const timer = setTimeout(() => {
      runSearch(query);
    }, 350);

    return () => clearTimeout(timer);
  }, [activeSection, searchQuery, searchSource, runSearch]);

  const handleSearch = async (event) => {
    event?.preventDefault();
    await runSearch();
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

  const pageTitles = {
    ask: "Ask",
    search: "Search",
    relevant: "Relevant Now",
    sources: "Sources",
    activity: "Activity",
    settings: "Settings",
  };

  const shellActions = {
    onDriveSync: handleDriveIngest,
    onCalendarSync: handleCalendarIngest,
    onRefresh: async () => {
      setRefreshingStatus(true);
      try {
        await Promise.all([reloadIngestStatus(), loadRelevantNow()]);
      } finally {
        setRefreshingStatus(false);
      }
    },
    driveDisabled: effectiveDriveStatus.code === "running",
    calendarDisabled: effectiveCalendarStatus.code === "running",
    refreshing: refreshingStatus,
  };

  return (
    <AppShell
      activeSection={activeSection}
      onSelectSection={setActiveSection}
      user={user}
      initials={initials}
      actions={shellActions}
      pageTitle={pageTitles[activeSection] || "Azeryn"}
    >
      {activeSection === "ask" && (
        <AskView
          question={question}
          setQuestion={setQuestion}
          messages={messages}
          answerLoading={answerLoading}
          inputFocused={inputFocused}
          setInputFocused={setInputFocused}
          handleAsk={handleAsk}
          suggestionPrompts={suggestionPrompts}
          relevant={relevant}
          relevantLoading={relevantLoading}
          relevantError={relevantError}
          loadRelevantNow={loadRelevantNow}
          driveStatus={effectiveDriveStatus}
          calendarStatus={effectiveCalendarStatus}
          ingestError={ingestError}
          lastDriveJob={lastDriveJob}
          jobs={jobs}
          jobsLoading={jobsLoading}
          jobsError={jobsError}
          onDriveSync={handleDriveIngest}
          onOpenSources={() => setActiveSection("sources")}
          onOpenActivity={() => setActiveSection("activity")}
        />
      )}
      {activeSection === "search" && (
        <SearchView
          searchQuery={searchQuery}
          setSearchQuery={setSearchQuery}
          searchSource={searchSource}
          setSearchSource={setSearchSource}
          searchResults={searchResults}
          searchLoading={searchLoading}
          searchError={searchError}
          lastSearchedQuery={lastSearchedQuery}
          handleSearch={handleSearch}
        />
      )}
      {activeSection === "relevant" && (
        <RelevantView
          relevant={relevant}
          relevantLoading={relevantLoading}
          relevantError={relevantError}
          loadRelevantNow={loadRelevantNow}
        />
      )}
      {activeSection === "sources" && (
        <SourcesView
          isDriveConnected={isDriveConnected}
          isCalendarConnected={isCalendarConnected}
          driveStatus={effectiveDriveStatus}
          calendarStatus={effectiveCalendarStatus}
          lastDriveJob={lastDriveJob}
          lastCalendarJob={lastCalendarJob}
          handleDriveIngest={handleDriveIngest}
          handleCalendarIngest={handleCalendarIngest}
          handleDisconnect={handleDisconnect}
          disconnecting={disconnecting}
          ingestError={ingestError}
        />
      )}
      {activeSection === "activity" && (
        <ActivityView
          jobs={jobs}
          jobsLoading={jobsLoading}
          jobsError={jobsError}
          activityFilter={activityFilter}
          setActivityFilter={setActivityFilter}
        />
      )}
      {activeSection === "settings" && (
        <SettingsView user={user} handleDisconnect={handleDisconnect} disconnecting={disconnecting} />
      )}
    </AppShell>
  );
}
