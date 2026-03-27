"""
Application factory and web entrypoint for PricePulse.
"""

from __future__ import annotations

from flask import Flask

from config import APP_DIR, FLASK_DEBUG, PORT, SECRET_KEY, STATIC_DIR
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
    start_runtime_warmups()
    return app


configure_logging()
app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=FLASK_DEBUG, use_reloader=FLASK_DEBUG)
