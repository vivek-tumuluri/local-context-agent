import os
import time

from dotenv import load_dotenv
from redis import from_url
from redis.exceptions import RedisError
from rq import Queue, Worker

from app.logging_utils import log_event

load_dotenv()


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    listen = ["ingest"]
    try:
        conn = from_url(redis_url)
        conn.ping()
    except RedisError as exc:
        log_event("worker_start_failed", error=str(exc), level="error")
        raise
    queues = [Queue(name, connection=conn) for name in listen]

    class LoggingWorker(Worker):
        def execute_job(self, job, queue):
            start = time.perf_counter()
            log_event(
                "worker_job_start",
                rq_job_id=job.id,
                queue=queue.name,
                func_name=job.func_name,
                description=job.description,
                user_id=job.meta.get("user_id") if hasattr(job, "meta") else None,
                enqueued_at=str(job.enqueued_at) if job.enqueued_at else None,
            )
            try:
                super().execute_job(job, queue)
                duration_ms = round((time.perf_counter() - start) * 1000, 3)
                log_event(
                    "worker_job_completed",
                    rq_job_id=job.id,
                    queue=queue.name,
                    duration_ms=duration_ms,
                    status="succeeded",
                )
            except Exception as exc:
                duration_ms = round((time.perf_counter() - start) * 1000, 3)
                retries_left = getattr(job, "retries_left", None)
                log_event(
                    "worker_job_failed",
                    rq_job_id=job.id,
                    queue=queue.name,
                    duration_ms=duration_ms,
                    error=str(exc),
                    retries_left=retries_left,
                    level="error",
                )
                raise

    worker = LoggingWorker(queues, connection=conn)
    log_event("worker_started", queues=listen, redis_url=redis_url)
    worker.work(with_scheduler=True)

if __name__ == "__main__":
    main()
