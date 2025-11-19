const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function handleResponse(response) {
  if (response.status === 204) {
    return null;
  }

  const text = await response.text();
  if (response.ok) {
    return text ? JSON.parse(text) : null;
  }

  const message = text || `Request failed with status ${response.status}`;
  throw new Error(`${response.status}: ${message}`);
}

export async function apiGet(path, csrfToken) {
  const headers = {};
  if (csrfToken) {
    headers["X-CSRF-Token"] = csrfToken;
  }
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "GET",
    credentials: "include",
    headers,
  });
  return handleResponse(response);
}

export async function apiPost(path, body, csrfToken) {
  const headers = {
    "Content-Type": "application/json",
  };
  if (csrfToken) {
    headers["X-CSRF-Token"] = csrfToken;
  }

  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    credentials: "include",
    headers,
    body: JSON.stringify(body ?? {}),
  });
  return handleResponse(response);
}

export async function fetchRelevantNow(csrfToken) {
  return apiGet("/relevant/now", csrfToken);
}

export async function fetchIngestJobs(csrfToken) {
  return apiGet("/ingest/jobs", csrfToken);
}
