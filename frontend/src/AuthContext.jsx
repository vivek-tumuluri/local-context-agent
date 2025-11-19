import React, { createContext, useContext, useEffect, useState } from "react";
import { apiGet } from "./api";

const AuthContext = createContext({
  user: null,
  csrfToken: null,
  loading: true,
});

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [csrfToken, setCsrfToken] = useState(null);
  const [loading, setLoading] = useState(true);

  const refreshAuth = async () => {
    try {
      const data = await apiGet("/auth/me");
      setUser(data.user || null);
      setCsrfToken(data.csrf_token || null);
    } catch (err) {
      setUser(null);
      setCsrfToken(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let cancelled = false;

    async function loadAuth() {
      try {
        const data = await apiGet("/auth/me");
        if (!cancelled && data) {
          setUser(data.user || null);
          setCsrfToken(data.csrf_token || null);
        }
      } catch (err) {
        if (!cancelled) {
          setUser(null);
          setCsrfToken(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadAuth();

    return () => {
      cancelled = true;
    };
  }, []);

  const isDriveConnected =
    !!(user?.has_drive_session ?? user?.has_drive_credentials ?? user?.drive_connected ?? user?.drive_ready);
  const isCalendarConnected =
    !!(user?.has_calendar_session ?? user?.has_calendar_credentials ?? user?.calendar_connected ?? user?.calendar_ready);

  const value = { user, csrfToken, loading, refreshAuth, isDriveConnected, isCalendarConnected };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
