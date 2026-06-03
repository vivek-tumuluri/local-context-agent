import React, { useState } from "react";
import { apiGet } from "./api";
import { CalendarIcon, DriveIcon, GoogleIcon, SparkIcon, SourcesIcon } from "./components/Icons";

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
    <div className="login-shell login-shell-product">
      <div className="login-orb login-orb-violet" />
      <div className="login-orb login-orb-cyan" />

      <div className="login-card login-product-card">
        <div className="login-brand">
          <div className="login-logo-mark">A</div>
          <div>
            <div className="login-kicker">Azeryn</div>
            <div className="login-brand-subtitle">Personal context workspace</div>
          </div>
        </div>

        <div className="login-copy">
          <h1 className="login-title">Ask across your Drive and Calendar context.</h1>
          <p className="login-subtitle">
            Connect Google to sync your workspace, retrieve relevant context, and answer with grounded citations.
          </p>
        </div>

        <button className={`google-login-button${loading ? " is-loading" : ""}`} onClick={handleLogin} disabled={loading}>
          <GoogleIcon />
          <span>{loading ? "Redirecting to Google" : "Continue with Google"}</span>
        </button>

        <div className="login-feature-grid">
          <div className="login-feature-chip">
            <DriveIcon />
            <span>Drive sync</span>
          </div>
          <div className="login-feature-chip">
            <CalendarIcon />
            <span>Calendar context</span>
          </div>
          <div className="login-feature-chip">
            <SparkIcon />
            <span>Grounded answers</span>
          </div>
        </div>

        <div className="login-preview" aria-hidden="true">
          <div className="login-preview-header">
            <div>
              <div className="login-preview-title">Workspace preview</div>
              <div className="muted tiny">Synced context, ready to ask</div>
            </div>
            <span className="status-pill status-pill-ok">ready</span>
          </div>
          <div className="login-preview-row">
            <span className="login-preview-icon">
              <SparkIcon />
            </span>
            <div>
              <div>Ask Azeryn</div>
              <div className="muted tiny">Answers with citations</div>
            </div>
          </div>
          <div className="login-preview-row">
            <span className="login-preview-icon">
              <SourcesIcon />
            </span>
            <div>
              <div>Sources</div>
              <div className="muted tiny">Drive and Calendar ingestion</div>
            </div>
          </div>
        </div>

        <div className="login-footnote">Early technical preview</div>
      </div>
    </div>
  );
}
