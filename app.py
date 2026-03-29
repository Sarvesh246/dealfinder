"""
Application factory and web entrypoint for PricePulse.
"""

from __future__ import annotations

import atexit
import logging
import os
import subprocess
import sys
from flask import Flask

from config import (
    APP_DIR,
    AUTO_START_LOCAL_WORKER,
    FLASK_DEBUG,
    JOB_RUNNER_MODE,
    PORT,
    SECRET_KEY,
    STATIC_DIR,
)
from database import get_runtime_diagnostics
from routes import main_bp
from runtime_bootstrap import configure_logging, ensure_database_ready, start_runtime_warmups
from template_utils import register_template_utils


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(APP_DIR / "templates"),
        static_folder=str(STATIC_DIR),
        static_url_path="/static",
    )
    app.secret_key = SECRET_KEY

    @app.after_request
    def apply_response_headers(response):
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault(
            "Permissions-Policy",
            "geolocation=(), microphone=(), camera=(), payment=()",
        )
        return response

    register_template_utils(app)
    app.register_blueprint(main_bp, name="")

    ensure_database_ready()
    start_runtime_warmups(process_kind="web")
    return app


configure_logging()
app = create_app()


def _should_autostart_local_worker(*, env: dict[str, str] | None = None, flask_debug: bool = FLASK_DEBUG) -> bool:
    env = env or os.environ
    if JOB_RUNNER_MODE != "worker":
        return False
    if not AUTO_START_LOCAL_WORKER:
        return False
    if flask_debug and env.get("WERKZEUG_RUN_MAIN") != "true":
        return False
    return True


def _stop_local_worker(process: subprocess.Popen | None) -> None:
    if not process or process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def _start_local_worker_if_needed() -> subprocess.Popen | None:
    if not _should_autostart_local_worker():
        return None
    runtime = get_runtime_diagnostics()
    if runtime.get("worker_online"):
        logging.info("Local worker already online; skipping auto-start.")
        return None
    worker_path = APP_DIR / "worker.py"
    process = subprocess.Popen(
        [sys.executable, str(worker_path)],
        cwd=str(APP_DIR),
    )
    atexit.register(_stop_local_worker, process)
    logging.info("Auto-started local worker pid=%s", process.pid)
    return process


if __name__ == "__main__":
    _start_local_worker_if_needed()
    app.run(host="0.0.0.0", port=PORT, debug=FLASK_DEBUG, use_reloader=FLASK_DEBUG)
