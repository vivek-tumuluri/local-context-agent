import React, { useEffect, useState } from "react";
import { AuthProvider, useAuth } from "./AuthContext";
import LoginPage from "./LoginPage";
import Dashboard from "./Dashboard";

const THEME_STORAGE_KEY = "azeryn-theme";

function getInitialTheme() {
  if (typeof window === "undefined") return "dark";
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    return stored === "light" ? "light" : "dark";
  } catch {
    return "dark";
  }
}

function AppInner({ theme, onToggleTheme }) {
  const { user, loading } = useAuth();

  if (loading) {
    return <p className="loading">Loading...</p>;
  }

  if (!user) {
    return <LoginPage />;
  }

  return <Dashboard theme={theme} onToggleTheme={onToggleTheme} />;
}

export default function App() {
  const [theme, setTheme] = useState(getInitialTheme);

  useEffect(() => {
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // Theme persistence is a convenience; keep the app usable if storage is unavailable.
    }
  }, [theme]);

  const handleToggleTheme = () => {
    setTheme((current) => (current === "light" ? "dark" : "light"));
  };

  return (
    <AuthProvider>
      <div className="app-root" data-theme={theme}>
        <AppInner theme={theme} onToggleTheme={handleToggleTheme} />
      </div>
    </AuthProvider>
  );
}
