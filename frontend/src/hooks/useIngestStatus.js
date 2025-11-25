import { useEffect, useState, useCallback } from "react";
import { fetchIngestJobs } from "../api";

function normalizeJob(job) {
  if (!job) return null;
  const metrics = job.metrics || {};
  const logs = Array.isArray(job.logs) ? job.logs : Array.isArray(metrics.logs) ? metrics.logs : [];
  return {
    ...job,
    id: job.job_id || job.id,
    source: job.source || job.kind,
    status: job.status,
    createdAt: job.created_at || job.started_at,
    updatedAt: job.updated_at || job.finished_at,
    processed: job.processed_files ?? job.processed_count ?? metrics.ingested ?? metrics.found ?? 0,
    total: job.total_files ?? job.total ?? metrics.found ?? undefined,
    errors: job.errors ?? metrics.errors ?? metrics.error_count ?? 0,
    errorSummary: job.error_summary,
    logs,
  };
}

function normalizeJobs(data) {
  const list = Array.isArray(data?.jobs) ? data.jobs : Array.isArray(data) ? data : [];
  const normalized = list.map((j) => normalizeJob(j) || j);
  const sorted = [...normalized];
  sorted.sort((a, b) => {
    const aTime = new Date(a?.createdAt || a?.created_at || a?.started_at || 0).getTime();
    const bTime = new Date(b?.createdAt || b?.created_at || b?.started_at || 0).getTime();
    return bTime - aTime;
  });
  return sorted;
}

function mapStatus(job) {
  if (!job) return { label: "Not synced yet", code: "none" };
  switch (job.status) {
    case "succeeded":
    case "completed":
      return { label: "Up to date", code: "succeeded" };
    case "running":
    case "queued":
    case "in_progress":
      return { label: "Syncing…", code: "running" };
    case "failed":
    case "completed_with_errors":
      return { label: "Last run failed", code: "failed" };
    default:
      return { label: job.status || "Unknown", code: "unknown" };
  }
}

export function useIngestStatus(csrfToken) {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await fetchIngestJobs(csrfToken);
      setJobs(normalizeJobs(data));
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [csrfToken]);

  useEffect(() => {
    load();
  }, [load]);

  const driveJobs = jobs.filter((j) => j.source === "drive" || j.kind === "drive");
  const calendarJobs = jobs.filter((j) => j.source === "calendar" || j.kind === "calendar");

  const lastDriveJob = driveJobs[0] || null;
  const lastCalendarJob = calendarJobs[0] || null;

  return {
    jobs,
    loading,
    error,
    lastDriveJob,
    lastCalendarJob,
    driveStatus: mapStatus(lastDriveJob),
    calendarStatus: mapStatus(lastCalendarJob),
    reload: load,
  };
}
