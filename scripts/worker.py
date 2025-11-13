import os

from dotenv import load_dotenv
from redis import from_url
from rq import Queue, Worker

load_dotenv()


def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    listen = ["ingest"]
    conn = from_url(redis_url)
    queues = [Queue(name, connection=conn) for name in listen]
    worker = Worker(queues, connection=conn)
    worker.work()


if __name__ == "__main__":
    main()
