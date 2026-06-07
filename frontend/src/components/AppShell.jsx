import React from "react";
import {
  ActivityIcon,
  AskIcon,
  CalendarIcon,
  DriveIcon,
  MoonIcon,
  RefreshIcon,
  RelevantIcon,
  SearchIcon,
  SettingsIcon,
  SourcesIcon,
  SunIcon,
} from "./Icons";

const navItems = [
  { id: "ask", label: "Ask", icon: AskIcon },
  { id: "search", label: "Search", icon: SearchIcon },
  { id: "relevant", label: "Relevant Now", icon: RelevantIcon },
  { id: "sources", label: "Sources", icon: SourcesIcon },
  { id: "activity", label: "Activity", icon: ActivityIcon },
  { id: "settings", label: "Settings", icon: SettingsIcon },
];

function Sidebar({ activeSection, onSelect, theme, onToggleTheme }) {
  const isLight = theme === "light";

  return (
    <aside className="az-sidebar">
      <div className="az-logo" aria-label="Azeryn">
        <span className="az-logo-mark">A</span>
        <span className="az-logo-wordmark">Azeryn</span>
      </div>
      <nav className="az-nav" aria-label="Primary">
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              type="button"
              className={`az-nav-item${activeSection === item.id ? " is-active" : ""}`}
              onClick={() => onSelect(item.id)}
              title={item.label}
              aria-label={item.label}
            >
              <span className="az-nav-glyph">
                <Icon />
              </span>
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>
      <div className="az-sidebar-footer">
        <button
          type="button"
          className="theme-toggle"
          onClick={onToggleTheme}
          aria-pressed={isLight}
          title={isLight ? "Switch to dark mode" : "Switch to light mode"}
        >
          <span className="theme-toggle-track" aria-hidden="true">
            <span className="theme-toggle-thumb">{isLight ? <SunIcon /> : <MoonIcon />}</span>
          </span>
          <span className="theme-toggle-label">{isLight ? "Light mode" : "Dark mode"}</span>
        </button>
      </div>
    </aside>
  );
}

function UserBadge({ user, initials }) {
  return (
    <div className="user-badge" title={user.email}>
      <div className="avatar">{user.picture ? <img src={user.picture} alt="" /> : <span>{initials}</span>}</div>
    </div>
  );
}

export default function AppShell({
  activeSection,
  onSelectSection,
  user,
  initials,
  actions,
  pageTitle,
  theme,
  onToggleTheme,
  children,
}) {
  return (
    <div className="az-app">
      <Sidebar activeSection={activeSection} onSelect={onSelectSection} theme={theme} onToggleTheme={onToggleTheme} />
      <div className="az-workspace">
        <header className="az-topbar">
          <div className="top-page-title">
            <h1>{pageTitle}</h1>
          </div>
          <div className="top-actions">
            <button type="button" className="control-button top-sync-button" onClick={actions.onDriveSync} disabled={actions.driveDisabled}>
              <DriveIcon />
              Sync Drive
            </button>
            <button type="button" className="control-button top-sync-button" onClick={actions.onCalendarSync} disabled={actions.calendarDisabled}>
              <CalendarIcon />
              Sync Calendar
            </button>
            <button
              type="button"
              className={`icon-button refresh-button${actions.refreshing ? " is-loading" : ""}`}
              onClick={actions.onRefresh}
              disabled={actions.refreshing}
              title="Refresh status"
              aria-label="Refresh status"
            >
              <RefreshIcon />
            </button>
            <UserBadge user={user} initials={initials} />
          </div>
        </header>
        {children}
      </div>
    </div>
  );
}
