"""
alerts.py — Send price-drop notifications via Discord webhook and/or Gmail.

Both channels are optional. If the relevant env variables are not set the
function returns silently.
"""

import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests


# ---------------------------------------------------------------------------
# Discord
# ---------------------------------------------------------------------------

def send_discord_alert(
    product_name: str,
    current_price: float,
    target_price: float,
    url: str,
) -> None:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    try:
        embed = {
            "title": "\U0001f4b0 Price Drop Alert \u2014 PricePulse",
            "description": f"**{product_name}** just hit your target price!",
            "color": 0x00E5A0,
            "fields": [
                {"name": "Current Price", "value": f"**${current_price:,.2f}**", "inline": True},
                {"name": "Your Target", "value": f"${target_price:,.2f}", "inline": True},
                {"name": "You save", "value": f"${target_price - current_price:,.2f}", "inline": True},
            ],
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "footer": {"text": "PricePulse Tracker"},
        }

        if url:
            embed["fields"].append(
                {"name": "Product Link", "value": f"[View Product]({url})", "inline": False}
            )

        resp = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        if resp.status_code == 204:
            logging.info(f"[{datetime.now()}] Discord alert sent for '{product_name}'")
        else:
            logging.warning(
                f"[{datetime.now()}] Discord webhook returned {resp.status_code} "
                f"for '{product_name}'"
            )
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Discord alert error for '{product_name}': {exc}")


# ---------------------------------------------------------------------------
# Gmail
# ---------------------------------------------------------------------------

def send_gmail_alert(
    product_name: str,
    current_price: float,
    target_price: float,
    url: str,
) -> None:
    gmail_user = os.getenv("GMAIL_USER", "").strip()
    gmail_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()
    alert_email = os.getenv("ALERT_EMAIL", "").strip()

    if not (gmail_user and gmail_password and alert_email):
        return

    try:
        subject = f"\U0001f4b0 Price Drop: {product_name} is now ${current_price:,.2f}"
        body = (
            f"PricePulse Price Drop Alert\n"
            f"{'=' * 40}\n\n"
            f"Product      : {product_name}\n"
            f"Current Price: ${current_price:,.2f}\n"
            f"Your Target  : ${target_price:,.2f}\n"
            f"You save     : ${target_price - current_price:,.2f}\n\n"
        )
        if url:
            body += f"View product : {url}\n\n"
        body += (
            f"Checked at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"---\nThis alert was sent by PricePulse. "
            f"It will not repeat until the price rises and falls again."
        )

        msg = MIMEMultipart()
        msg["From"] = gmail_user
        msg["To"] = alert_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, alert_email, msg.as_string())

        logging.info(f"[{datetime.now()}] Gmail alert sent to {alert_email} for '{product_name}'")
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Gmail alert error for '{product_name}': {exc}")


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def send_alerts(
    product_name: str,
    current_price: float,
    target_price: float,
    url: str = "",
) -> None:
    """Fire all configured alert channels. Safe to call unconditionally."""
    send_discord_alert(product_name, current_price, target_price, url)
    send_gmail_alert(product_name, current_price, target_price, url)
