import { useEffect, useState, useCallback } from "react";
import { fetchIngestJobs } from "../api";

function normalizeJobs(data) {
  const list = Array.isArray(data?.jobs) ? data.jobs : Array.isArray(data) ? data : [];
  const sorted = [...list];
  sorted.sort((a, b) => {
    const aTime = new Date(a?.created_at || a?.started_at || 0).getTime();
    const bTime = new Date(b?.created_at || b?.started_at || 0).getTime();
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
