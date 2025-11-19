import React, { useState } from "react";
import { apiGet } from "./api";

export default function LoginPage() {
  const [loading, setLoading] = useState(false);

  const handleLogin = async () => {
    if (loading) return;
    setLoading(true);
    try {
      const data = await apiGet("/auth/google");
      if (data && data.authorization_url) {
        window.location.href = data.authorization_url;
      } else {
        throw new Error("Missing authorization_url");
      }
    } catch (err) {
      console.error("Google login start failed", err);
      alert("Failed to start Google login. Check backend.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="badge" style={{ marginBottom: "8px" }}>Azeryn</div>
        <h2 className="login-title">Context-aware assistant for your own data</h2>
        <p className="login-subtitle">
          Connect Google to ingest Drive and Calendar, then ask grounded questions with inline citations.
        </p>
        <button className="button-primary full-width" onClick={handleLogin} disabled={loading}>
          {loading ? "Redirecting..." : "Sign in with Google"}
        </button>
        <div className="text-muted" style={{ marginTop: "12px" }}>Early technical preview</div>
      </div>
    </div>
  );
}
