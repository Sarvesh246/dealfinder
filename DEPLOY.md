# Deploy PricePulse with separate web + worker processes

PricePulse now runs best as two long-lived processes that share the same SQLite file:

- `web`: Flask UI and API (`python app.py`)
- `worker`: scheduled checks, startup backfill, and queued manual `/check` jobs (`python worker.py`)

This keeps the web process responsive while the worker owns background jobs.

## Process layout

Use the included [Procfile](Procfile):

```procfile
web: python app.py
worker: python worker.py
```

On Railway, Render, a VM, or a self-hosted PC, run both processes with the same env vars and the same persistent `DB_PATH`.

## Railway setup

1. Create a **web** service from this repo.
2. Confirm the start command is `python app.py` (see [railway.toml](railway.toml)).
3. Create a second **worker** service from the same repo with start command `python worker.py`.
4. Chromium for Selenium is declared in [railpack.toml](railpack.toml).

## Required environment

Set these in both the web and worker services:

| Variable | Notes |
|----------|--------|
| `SECRET_KEY` | Long random string for Flask sessions / UI check token |
| `CHECK_CRON_SECRET` | Random string; used by `/check?token=` and optional GitHub Actions calls |
| `DB_PATH` | e.g. `/data/price_tracker.db` |
| `PORT` | Usually set automatically for the web service |
| `DISCORD_WEBHOOK_URL` | Optional |
| `GMAIL_USER` / `GMAIL_APP_PASSWORD` / `ALERT_EMAIL` | Optional email alerts |
| `HF_TOKEN` | Optional Hugging Face |
| `CHECK_INTERVAL_HOURS` | Periodic worker check interval, default `6` |
| `WORKER_LEASE_SECONDS` | Worker lease/heartbeat window, default `90` |
| `WORKER_HEARTBEAT_SECONDS` | Worker heartbeat cadence, default `20` |
| `MANUAL_CHECK_POLL_SECONDS` | Manual queue poll cadence, default `10` |

## Persistent SQLite volume

1. Add a shared volume mounted at `/data`.
2. Set `DB_PATH=/data/price_tracker.db` in both services.

Without a persistent volume, tracked products and history are lost on restart/redeploy.

## Browser/runtime expectations

The scraper relies on:

- Python
- Chromium / Chrome
- compatible chromedriver path resolution

[railpack.toml](railpack.toml) installs `chromium` for supported hosts. On a VM or local machine, install Chromium/Chrome and keep the worker process running continuously.

## Health and diagnostics

The web process now exposes:

- `/healthz` — liveness and enabled-source counts
- `/readyz` — runtime readiness plus worker/queue status
- `/diagnostics` — current worker heartbeat, queue depth, last jobs, and recent runtime failures

Use `/diagnostics` from the Settings page or directly for deployment troubleshooting.

## Manual checks

`/check` no longer runs the scrape inline. It now queues a manual job for the worker:

- if a worker is online, the job is picked up shortly
- if no worker is online, the request stays queued until one starts

This avoids long request timeouts on the web service.

## Optional GitHub Actions trigger

If you still want GitHub Actions to trigger checks:

- set `RAILWAY_APP_URL` to the public web URL
- set `CHECK_CRON_SECRET` in GitHub Actions secrets
- call `GET {RAILWAY_APP_URL}/check?token={CHECK_CRON_SECRET}`

The worker will process the queued manual check; the web request returns immediately.

## Local development

Run these in separate terminals:

```powershell
python app.py
python worker.py
```

Leave `CHECK_CRON_SECRET` unset locally to keep `/check` open without a token.
