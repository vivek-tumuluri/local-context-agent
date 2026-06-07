import pytest

from app.routes import rag_routes


@pytest.mark.asyncio
async def test_rag_search_filters_drop_tiny_and_trashed_hits(api_client, monkeypatch):
    token = "csrf-token"

    def fake_query(query: str, k: int, user_id: str, source=None):
        return [
            {
                "text": "tiny",
                "meta": {"source": "drive", "title": "Tiny", "doc_id": "tiny-doc", "is_trashed": False},
                "similarity": 0.8,
            },
            {
                "text": "This is a valid chunk that should remain after filtering.",
                "meta": {"source": "drive", "title": "Valid", "doc_id": "valid-doc", "is_trashed": False},
                "similarity": 0.9,
            },
            {
                "text": "Trashed content should be removed.",
                "meta": {"source": "drive", "title": "Trash", "doc_id": "trash-doc", "is_trashed": True},
                "similarity": 0.95,
            },
        ]

    monkeypatch.setattr(rag_routes, "hybrid_query", fake_query)
    from app.core import auth

    headers = {auth.CSRF_HEADER_NAME: token}
    cookies = {
        auth.SESSION_COOKIE_NAME: "session-token",
        auth.CSRF_COOKIE_NAME: token,
    }

    resp = await api_client.post(
        "/rag/search",
        json={"query": "hi", "k": 5},
        headers=headers,
        cookies=cookies,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["hits"] == 1
    assert len(data["results"]) == 1
    assert data["results"][0]["meta"]["doc_id"] == "valid-doc"
