import React from "react";

function IconBase({ children, className = "", title, ...props }) {
  return (
    <svg
      className={`ui-icon ${className}`.trim()}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.9"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden={title ? undefined : true}
      role={title ? "img" : undefined}
      {...props}
    >
      {title && <title>{title}</title>}
      {children}
    </svg>
  );
}

export function AskIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M5 6.5A3.5 3.5 0 0 1 8.5 3h7A3.5 3.5 0 0 1 19 6.5v5A3.5 3.5 0 0 1 15.5 15H11l-4.5 3v-3A3.5 3.5 0 0 1 3 11.5v-5" />
      <path d="M9 8h6" />
      <path d="M9 11h3.5" />
      <path d="m17.5 17.5.5 1.5 1.5.5-1.5.5-.5 1.5-.5-1.5-1.5-.5 1.5-.5.5-1.5Z" />
    </IconBase>
  );
}

export function SearchIcon(props) {
  return (
    <IconBase {...props}>
      <circle cx="10.5" cy="10.5" r="5.5" />
      <path d="m15 15 4 4" />
    </IconBase>
  );
}

export function RelevantIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M7 3v3" />
      <path d="M17 3v3" />
      <path d="M4.5 8h15" />
      <rect x="4.5" y="5" width="15" height="15" rx="2.5" />
      <path d="m13.5 12.5.5 1.5 1.5.5-1.5.5-.5 1.5-.5-1.5-1.5-.5 1.5-.5.5-1.5Z" />
    </IconBase>
  );
}

export function SourcesIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M4 7.5c0-1.4 3.6-2.5 8-2.5s8 1.1 8 2.5-3.6 2.5-8 2.5-8-1.1-8-2.5Z" />
      <path d="M4 7.5v5c0 1.4 3.6 2.5 8 2.5s8-1.1 8-2.5v-5" />
      <path d="M4 12.5v5c0 1.4 3.6 2.5 8 2.5s8-1.1 8-2.5v-5" />
    </IconBase>
  );
}

export function ActivityIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M4 13h3l2-6 4 12 2-6h5" />
      <path d="M4 5h16" opacity=".45" />
    </IconBase>
  );
}

export function SettingsIcon(props) {
  return (
    <IconBase {...props}>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.7 1.7 0 0 0 .34 1.88l.05.05a2 2 0 1 1-2.83 2.83l-.05-.05a1.7 1.7 0 0 0-1.88-.34 1.7 1.7 0 0 0-1 1.55V21a2 2 0 1 1-4 0v-.07a1.7 1.7 0 0 0-1-1.55 1.7 1.7 0 0 0-1.88.34l-.05.05a2 2 0 1 1-2.83-2.83l.05-.05A1.7 1.7 0 0 0 4.6 15a1.7 1.7 0 0 0-1.55-1H3a2 2 0 1 1 0-4h.07a1.7 1.7 0 0 0 1.55-1 1.7 1.7 0 0 0-.34-1.88l-.05-.05a2 2 0 1 1 2.83-2.83l.05.05A1.7 1.7 0 0 0 9 4.6a1.7 1.7 0 0 0 1-1.55V3a2 2 0 1 1 4 0v.07a1.7 1.7 0 0 0 1 1.55 1.7 1.7 0 0 0 1.88-.34l.05-.05a2 2 0 1 1 2.83 2.83l-.05.05A1.7 1.7 0 0 0 19.4 9c.12.4.43.73.82.88.2.08.42.12.65.12H21a2 2 0 1 1 0 4h-.07a1.7 1.7 0 0 0-1.55 1Z" />
    </IconBase>
  );
}

export function RefreshIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M20 6v5h-5" />
      <path d="M4 18v-5h5" />
      <path d="M18.1 10A6.8 6.8 0 0 0 6.2 7.8L4 10" />
      <path d="M5.9 14a6.8 6.8 0 0 0 11.9 2.2L20 14" />
    </IconBase>
  );
}

export function DriveIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m8.5 4-6 10.5L6 20h12l3.5-5.5L15.5 4h-7Z" />
      <path d="M8.5 4 12 10.5 6 20" />
      <path d="M15.5 4 12 10.5 18 20" />
      <path d="M12 10.5h9.5" />
    </IconBase>
  );
}

export function CalendarIcon(props) {
  return (
    <IconBase {...props}>
      <path d="M7 3v3" />
      <path d="M17 3v3" />
      <rect x="4" y="5" width="16" height="17" rx="2.5" />
      <path d="M4 9h16" />
      <path d="M8 13h3" />
      <path d="M8 17h6" />
    </IconBase>
  );
}

export function SparkIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m12 2 1.4 5.1L18 9l-4.6 1.9L12 16l-1.4-5.1L6 9l4.6-1.9L12 2Z" />
      <path d="m18.5 14 .7 2.3L21.5 17l-2.3.7-.7 2.3-.7-2.3-2.3-.7 2.3-.7.7-2.3Z" />
      <path d="m5 14 .5 1.5L7 16l-1.5.5L5 18l-.5-1.5L3 16l1.5-.5L5 14Z" />
    </IconBase>
  );
}

export function CheckIcon(props) {
  return (
    <IconBase {...props}>
      <path d="m5 12 4 4 10-10" />
    </IconBase>
  );
}

export function GoogleIcon({ className = "", title, ...props }) {
  return (
    <svg
      className={`ui-icon google-icon ${className}`.trim()}
      viewBox="0 0 24 24"
      aria-hidden={title ? undefined : true}
      role={title ? "img" : undefined}
      {...props}
    >
      {title && <title>{title}</title>}
      <path
        fill="#4285F4"
        d="M21.6 12.23c0-.76-.07-1.49-.2-2.18H12v4.12h5.38a4.6 4.6 0 0 1-1.99 3.02v2.51h3.23c1.89-1.74 2.98-4.3 2.98-7.47Z"
      />
      <path
        fill="#34A853"
        d="M12 22c2.7 0 4.96-.89 6.62-2.4l-3.23-2.51c-.9.6-2.04.95-3.39.95-2.6 0-4.8-1.76-5.59-4.12H3.07v2.6A10 10 0 0 0 12 22Z"
      />
      <path
        fill="#FBBC05"
        d="M6.41 13.92A6 6 0 0 1 6.1 12c0-.66.11-1.3.31-1.92v-2.6H3.07A10 10 0 0 0 2 12c0 1.61.39 3.13 1.07 4.52l3.34-2.6Z"
      />
      <path
        fill="#EA4335"
        d="M12 5.96c1.47 0 2.79.5 3.82 1.5l2.87-2.87C16.95 2.98 14.69 2 12 2a10 10 0 0 0-8.93 5.48l3.34 2.6C7.2 7.72 9.4 5.96 12 5.96Z"
      />
    </svg>
  );
}
