# Deploy PricePulse (Deal Finder) on Railway (free tier)

This app is a Flask server started with `python3 app.py`. It uses SQLite (`price_tracker.db`) and benefits from **persistent storage** on Railway.

## 1. Create the service

1. Sign up at [Railway](https://railway.app) and **New Project** → **Deploy from GitHub** (or upload the repo).
2. Select this repository. Railway will detect Python via Nixpacks.
3. Confirm **Start command** is `python3 app.py` (see [`railway.toml`](railway.toml)).

## 2. Environment variables

In the service **Variables** tab, set (copy values from your local `.env`; never commit secrets):

| Variable | Notes |
|----------|--------|
| `SECRET_KEY` | Long random string for Flask sessions / UI check token |
| `CHECK_CRON_SECRET` | Random string; GitHub Actions and optional `?token=` must match |
| `DB_PATH` | e.g. `/data/price_tracker.db` (must be under the volume mount) |
| `CHECK_INTERVAL_HOURS` | e.g. `999` if you use **only** GitHub cron for periodic checks (avoids double runs with APScheduler) |
| `PORT` | Usually set automatically by Railway |
| `DISCORD_WEBHOOK_URL` | Optional |
| `GMAIL_USER` / `GMAIL_APP_PASSWORD` / `ALERT_EMAIL` | Optional email alerts |
| `HF_TOKEN` | Optional Hugging Face |
| Other tuning | See comments in `.env.example` |

## 3. Persistent volume (SQLite)

1. In Railway: open your service → **Volumes** → **Add volume**.
2. Mount path: **`/data`** (recommended).
3. Set `DB_PATH=/data/price_tracker.db`.

Without a volume, SQLite is stored on ephemeral disk and **tracked products are lost** on redeploy/restart.

## 4. GitHub Actions cron

The workflow [`.github/workflows/check.yml`](.github/workflows/check.yml) calls your app on a schedule so checks run even when the Railway instance is idle.

**Repository secrets** (GitHub → Settings → Secrets and variables → Actions):

- **`RAILWAY_APP_URL`** — Public base URL only, e.g. `https://your-service.up.railway.app` (no trailing slash).
- **`CHECK_CRON_SECRET`** — Same value as the Railway variable `CHECK_CRON_SECRET`.

The workflow requests:

`GET {RAILWAY_APP_URL}/check?token={CHECK_CRON_SECRET}`

## 5. “Check Now” in the UI

When `CHECK_CRON_SECRET` is set, the navbar **Check Now** link includes a daily **`ui_token`** derived from `SECRET_KEY`. No need to paste the cron secret in the browser.

## Local development

Leave `CHECK_CRON_SECRET` unset to keep `/check` open without a token.
