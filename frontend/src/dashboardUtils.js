export const TERMINAL_STATUSES = new Set(["completed", "completed_with_errors", "failed", "succeeded"]);

export function statusPillClass(code) {
  const value = (code || "").toString().toLowerCase();
  if (value.includes("succeed") || value.includes("completed")) return "status-pill status-pill-ok";
  if (value.includes("fail") || value.includes("error")) return "status-pill status-pill-error";
  if (value.includes("run") || value.includes("queue") || value.includes("progress")) return "status-pill status-pill-warn";
  return "status-pill";
}

export function formatTimestamp(ts) {
  if (!ts) return "Unknown time";
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) return "Unknown time";
  return date.toLocaleString();
}

export function formatJobMeta(job = {}) {
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
  return [`Processed ${processed}${total ? ` / ${total}` : ""}`, `errors ${errors}`].join(" · ");
}
