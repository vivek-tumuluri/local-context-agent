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
        <h1>Local Context Agent</h1>
        <p>Your personal knowledge layer. Log in with Google to sync Drive and ask grounded questions.</p>
        <button className="btn btn-primary full-width" onClick={handleLogin} disabled={loading}>
          {loading ? "Redirecting..." : "Continue with Google"}
        </button>
      </div>
    </div>
  );
}
