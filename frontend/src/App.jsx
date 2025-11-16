import React from "react";
import { AuthProvider, useAuth } from "./AuthContext";
import LoginPage from "./LoginPage";
import Dashboard from "./Dashboard";

function AppInner() {
  const { user, loading } = useAuth();

  if (loading) {
    return <p className="loading">Loading...</p>;
  }

  if (!user) {
    return <LoginPage />;
  }

  return <Dashboard />;
}

export default function App() {
  return (
    <AuthProvider>
      <div className="app-root">
        <AppInner />
      </div>
    </AuthProvider>
  );
}
