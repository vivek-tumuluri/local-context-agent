import os
from fastapi import APIRouter
from fastapi.responses import RedirectResponse
from itsdangerous import URLSafeSerializer
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from .google_clients import build_flow
from fastapi.responses import RedirectResponse, HTMLResponse

router = APIRouter(prefix="/auth", tags=["auth"])
signer = URLSafeSerializer(os.getenv("SESSION_SECRET", "dev-secret"))

@router.get("/google")
def start_google_auth():
    flow = build_flow()
    flow.redirect_uri = os.getenv("OAUTH_REDIRECT_URI")
    auth_url, state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
    )
    return {"authorization_url": auth_url, "state": state}

@router.get("/google/callback")
def google_callback(code: str, state: str):
    flow = build_flow()
    flow.redirect_uri = os.getenv("OAUTH_REDIRECT_URI")
    flow.fetch_token(code=code)
    creds = flow.credentials
    token_dump = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or []),
    }
    session = signer.dumps(token_dump)

    return RedirectResponse(url=f"/auth/debug/authed?session={session}")

@router.get("/me")
def me(session: str):
    creds = Credentials(**signer.loads(session))
    svc = build("oauth2", "v2", credentials=creds)
    userinfo = svc.userinfo().get().execute()
    return {"user": userinfo}

@router.get("/debug/authed", response_class=HTMLResponse)
def authed(session: str):
    return f"""
    <html>
      <body style="font-family: ui-monospace, Menlo, monospace; padding: 24px;">
        <h2>Authenticated ✔</h2>
        <p>Copy your session token below and use it with the API.</p>
        <pre style="white-space: pre-wrap; word-break: break-all; background:#f5f5f5; padding:12px; border-radius:8px;">
{session}
        </pre>
        <p>Quick test: <a href="/auth/me?session={session}">/auth/me</a></p>
      </body>
    </html>
    """


def creds_from_session(session: str) -> Credentials:
    return Credentials(**signer.loads(session))
