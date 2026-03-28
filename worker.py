"""
worker.py - dedicated worker process for scheduled/background price checks.
"""

from __future__ import annotations

import logging
import os
import socket
from datetime import datetime

from observability import log_event
from runtime_bootstrap import configure_logging, ensure_database_ready, start_runtime_warmups
from scheduler import create_worker_scheduler, shutdown_worker, worker_heartbeat


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def main() -> None:
    worker_id = _worker_id()
    configure_logging()
    ensure_database_ready()
    start_runtime_warmups(process_kind="worker")
    worker_heartbeat(worker_id)
    scheduler = create_worker_scheduler(worker_id)
    scheduler.start()
    log_event("worker.start", worker_id=worker_id)
    logging.info(f"[{datetime.now()}] Worker started: {worker_id}")
    try:
        while True:
            # Keep process alive while APScheduler owns the background jobs.
            import time

            time.sleep(30)
    except KeyboardInterrupt:
        logging.info(f"[{datetime.now()}] Worker stopping: {worker_id}")
    finally:
        scheduler.shutdown(wait=False)
        shutdown_worker(worker_id)
        log_event("worker.stop", worker_id=worker_id)


if __name__ == "__main__":
    main()
