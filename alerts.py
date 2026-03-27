"""
alerts.py — Send price-drop notifications via Discord webhook and/or Gmail.

Both channels are optional. If the relevant env variables are not set the
function returns silently.
"""

import logging
import os
import smtplib
import ssl
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from observability import log_event


# ---------------------------------------------------------------------------
# Discord
# ---------------------------------------------------------------------------

def get_notification_status() -> dict[str, object]:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    gmail_user = os.getenv("GMAIL_USER", "").strip()
    gmail_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()
    alert_email = os.getenv("ALERT_EMAIL", "").strip()
    return {
        "discord_configured": bool(webhook_url),
        "gmail_configured": bool(gmail_user and gmail_password and alert_email),
        "gmail_target": alert_email,
    }


def _alert_copy(
    product_name: str,
    current_price: float,
    *,
    alert_mode: str,
    target_price: float | None = None,
    previous_price: float | None = None,
    url: str = "",
    is_test: bool = False,
) -> dict[str, object]:
    if alert_mode == "any_drop":
        previous = previous_price if previous_price is not None else current_price
        drop_amount = max(0.0, float(previous) - float(current_price))
        headline = "test any-drop alert" if is_test else "new lower price"
        subject = f"📉 Price Drop: {product_name} is now ${current_price:,.2f}"
        description = f"**{product_name}** just hit a new lower verified price."
        body = (
            f"PricePulse {headline.title()}\n"
            f"{'=' * 40}\n\n"
            f"Product       : {product_name}\n"
            f"Previous Price: ${previous:,.2f}\n"
            f"New Price     : ${current_price:,.2f}\n"
            f"Drop Amount   : ${drop_amount:,.2f}\n\n"
        )
        fields = [
            {"name": "Previous Price", "value": f"${previous:,.2f}", "inline": True},
            {"name": "New Price", "value": f"**${current_price:,.2f}**", "inline": True},
            {"name": "Drop Amount", "value": f"${drop_amount:,.2f}", "inline": True},
        ]
        repeat_line = "It will alert again only when a new lower verified price appears."
    else:
        safe_target = target_price if target_price is not None else current_price
        savings = max(0.0, float(safe_target) - float(current_price))
        headline = "test target alert" if is_test else "target price hit"
        subject = f"💰 Price Drop: {product_name} is now ${current_price:,.2f}"
        description = f"**{product_name}** just hit your target price!"
        body = (
            f"PricePulse {headline.title()}\n"
            f"{'=' * 40}\n\n"
            f"Product      : {product_name}\n"
            f"Current Price: ${current_price:,.2f}\n"
            f"Your Target  : ${safe_target:,.2f}\n"
            f"You save     : ${savings:,.2f}\n\n"
        )
        fields = [
            {"name": "Current Price", "value": f"**${current_price:,.2f}**", "inline": True},
            {"name": "Your Target", "value": f"${safe_target:,.2f}", "inline": True},
            {"name": "You save", "value": f"${savings:,.2f}", "inline": True},
        ]
        repeat_line = "It will not repeat until the price rises and falls again."

    if url:
        body += f"View product  : {url}\n\n"
    body += (
        f"Checked at    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"---\nThis alert was sent by PricePulse. {repeat_line}"
    )
    return {
        "subject": subject,
        "description": description,
        "body": body,
        "fields": fields,
    }


def send_discord_alert(
    product_name: str,
    current_price: float,
    target_price: float | None = None,
    url: str = "",
    *,
    alert_mode: str = "target_threshold",
    previous_price: float | None = None,
    is_test: bool = False,
) -> bool:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return False

    try:
        copy = _alert_copy(
            product_name,
            current_price,
            alert_mode=alert_mode,
            target_price=target_price,
            previous_price=previous_price,
            url=url,
            is_test=is_test,
        )
        embed = {
            "title": "\U0001f4b0 Price Drop Alert \u2014 PricePulse",
            "description": copy["description"],
            "color": 0x00E5A0,
            "fields": copy["fields"],
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "footer": {"text": "PricePulse Tracker"},
        }

        if url:
            embed["fields"].append(
                {"name": "Product Link", "value": f"[View Product]({url})", "inline": False}
            )

        resp = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        if resp.status_code == 204:
            logging.info(f"[{datetime.now()}] Discord alert sent for '{product_name}'")
            log_event(
                "alert.delivery",
                channel="discord",
                product_name=product_name,
                alert_mode=alert_mode,
                current_price=current_price,
                delivered=True,
                is_test=is_test,
            )
            return True
        else:
            logging.warning(
                f"[{datetime.now()}] Discord webhook returned {resp.status_code} "
                f"for '{product_name}'"
            )
            log_event(
                "alert.delivery",
                level="warning",
                channel="discord",
                product_name=product_name,
                alert_mode=alert_mode,
                current_price=current_price,
                delivered=False,
                status_code=resp.status_code,
                is_test=is_test,
            )
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Discord alert error for '{product_name}': {exc}")
        log_event(
            "alert.delivery",
            level="error",
            channel="discord",
            product_name=product_name,
            alert_mode=alert_mode,
            current_price=current_price,
            delivered=False,
            error=str(exc),
            is_test=is_test,
        )
    return False


# ---------------------------------------------------------------------------
# Gmail
# ---------------------------------------------------------------------------

def send_gmail_alert(
    product_name: str,
    current_price: float,
    target_price: float | None = None,
    url: str = "",
    *,
    alert_mode: str = "target_threshold",
    previous_price: float | None = None,
    is_test: bool = False,
) -> bool:
    gmail_user = os.getenv("GMAIL_USER", "").strip()
    gmail_password = os.getenv("GMAIL_APP_PASSWORD", "").strip()
    alert_email = os.getenv("ALERT_EMAIL", "").strip()

    if not (gmail_user and gmail_password and alert_email):
        return False

    try:
        copy = _alert_copy(
            product_name,
            current_price,
            alert_mode=alert_mode,
            target_price=target_price,
            previous_price=previous_price,
            url=url,
            is_test=is_test,
        )
        subject = str(copy["subject"])
        body = str(copy["body"])

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
        log_event(
            "alert.delivery",
            channel="gmail",
            product_name=product_name,
            alert_mode=alert_mode,
            current_price=current_price,
            delivered=True,
            target=alert_email,
            is_test=is_test,
        )
        return True
    except Exception as exc:
        logging.error(f"[{datetime.now()}] Gmail alert error for '{product_name}': {exc}")
        log_event(
            "alert.delivery",
            level="error",
            channel="gmail",
            product_name=product_name,
            alert_mode=alert_mode,
            current_price=current_price,
            delivered=False,
            target=alert_email,
            error=str(exc),
            is_test=is_test,
        )
    return False


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def send_alerts(
    product_name: str,
    current_price: float,
    target_price: float | None = None,
    url: str = "",
    *,
    alert_mode: str = "target_threshold",
    previous_price: float | None = None,
    is_test: bool = False,
) -> bool:
    """Fire all configured alert channels and report whether any alert succeeded."""
    discord_sent = send_discord_alert(
        product_name,
        current_price,
        alert_mode=alert_mode,
        target_price=target_price,
        previous_price=previous_price,
        url=url,
        is_test=is_test,
    )
    gmail_sent = send_gmail_alert(
        product_name,
        current_price,
        alert_mode=alert_mode,
        target_price=target_price,
        previous_price=previous_price,
        url=url,
        is_test=is_test,
    )
    return bool(discord_sent or gmail_sent)
