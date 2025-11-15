from . import auth_routes, ingest_routes, rag_routes, health_routes, jobs

auth_router = auth_routes.router
ingest_router = ingest_routes.router
rag_router = rag_routes.router
health_router = health_routes.router
jobs_router = jobs.router

__all__ = [
    "auth_routes",
    "ingest_routes",
    "rag_routes",
    "health_routes",
    "jobs",
    "auth_router",
    "ingest_router",
    "rag_router",
    "health_router",
    "jobs_router",
]
