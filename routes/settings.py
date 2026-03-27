"""
Settings routes.
"""

from __future__ import annotations

from flask import flash, redirect, render_template, request, url_for

from alerts import get_notification_status, send_alerts, send_discord_alert, send_gmail_alert
from database import get_all_sources, get_last_checked_time, get_runtime_diagnostics, update_source_enabled

from . import main_bp


@main_bp.route("/settings", endpoint="settings_page")
def settings_page():
    return render_template(
        "settings.html",
        sources=get_all_sources(),
        notification_status=get_notification_status(),
        runtime_diagnostics=get_runtime_diagnostics(),
        last_checked_time=get_last_checked_time(),
    )


@main_bp.route("/settings/sources", methods=["POST"], endpoint="settings_sources_save")
def settings_sources_save():
    enabled_ids = set()
    for sid in request.form.getlist("source_ids"):
        try:
            enabled_ids.add(int(sid))
        except ValueError:
            continue

    sources = get_all_sources()
    blocked_names = []
    for source in sources:
        if source["id"] in enabled_ids and not int(source["certified"]):
            blocked_names.append(source["name"])
        update_source_enabled(source["id"], 1 if source["id"] in enabled_ids else 0)

    if blocked_names:
        flash(
            "Some sources are still in certification and cannot be enabled yet: "
            + ", ".join(blocked_names),
            "warning",
        )
    flash("Default sources updated.", "success")
    return redirect(url_for("settings_page"))


@main_bp.route("/settings/notifications/test/<channel>", methods=["POST"], endpoint="settings_test_notification")
def settings_test_notification(channel):
    status = get_notification_status()
    channel = (channel or "").strip().lower()
    if channel not in {"discord", "gmail", "all"}:
        flash("Unknown notification channel.", "error")
        return redirect(url_for("settings_page"))

    if channel == "discord" and not status["discord_configured"]:
        flash("Discord webhook is not configured yet.", "error")
        return redirect(url_for("settings_page"))
    if channel == "gmail" and not status["gmail_configured"]:
        flash("Gmail alert settings are not configured yet.", "error")
        return redirect(url_for("settings_page"))

    sent = False
    if channel == "discord":
        sent = send_discord_alert(
            "PricePulse test alert",
            179.0,
            alert_mode="any_drop",
            previous_price=199.0,
            url="https://example.com/test-product",
            is_test=True,
        )
    elif channel == "gmail":
        sent = send_gmail_alert(
            "PricePulse test alert",
            179.0,
            alert_mode="any_drop",
            previous_price=199.0,
            url="https://example.com/test-product",
            is_test=True,
        )
    else:
        sent = send_alerts(
            "PricePulse test alert",
            179.0,
            alert_mode="any_drop",
            previous_price=199.0,
            url="https://example.com/test-product",
            is_test=True,
        )

    flash(
        "Test notification sent." if sent else "Test notification could not be sent. Check your current channel settings.",
        "success" if sent else "error",
    )
    return redirect(url_for("settings_page"))
