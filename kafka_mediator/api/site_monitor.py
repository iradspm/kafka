import logging

import frappe
import requests

from kafka_mediator import format_response, validate_payload

logger = logging.getLogger(__name__)

PING_PATH = "/api/method/ping"
TIMEOUT = 8


def _ping(url):
    """Return (status, response_ms) for a site URL."""
    base = url.rstrip("/")
    try:
        resp = requests.get(base + PING_PATH, timeout=TIMEOUT)
        ok = resp.status_code == 200
        try:
            ok = ok and resp.json().get("message") == "pong"
        except Exception:
            pass
        return ("Up" if ok else "Down"), resp.elapsed.total_seconds() * 1000
    except Exception:
        return "Down", None


@frappe.whitelist()
def list_sites(**kwargs):
    sites = frappe.get_all(
        "Kafka Monitored Site",
        fields=["name", "site_name", "url", "enabled", "status", "last_checked", "last_down"],
        order_by="site_name asc"
    )
    for s in sites:
        s["last_checked"] = str(s["last_checked"]) if s.get("last_checked") else None
        s["last_down"] = str(s["last_down"]) if s.get("last_down") else None
    return format_response(status_code=200, message=f"{len(sites)} site(s) found.", data=sites)


@frappe.whitelist()
@validate_payload("site_name", "url")
def add_site(**kwargs):
    payload = kwargs
    payload.pop("cmd", None)

    site_name = payload.get("site_name").strip()
    url = payload.get("url").strip().rstrip("/")

    if frappe.db.exists("Kafka Monitored Site", {"site_name": site_name}):
        return format_response(status_code=400, message=f"Site '{site_name}' already exists.")

    doc = frappe.get_doc({
        "doctype": "Kafka Monitored Site",
        "site_name": site_name,
        "url": url,
        "enabled": 1,
        "status": "Unknown"
    })
    doc.insert()
    frappe.db.commit()
    return format_response(status_code=200, message=f"Site '{site_name}' added.", data={"name": doc.name})


@frappe.whitelist()
@validate_payload("name")
def toggle_site(**kwargs):
    payload = kwargs
    payload.pop("cmd", None)

    name = payload.get("name")
    if not frappe.db.exists("Kafka Monitored Site", name):
        return format_response(status_code=404, message="Site not found.")

    current = frappe.db.get_value("Kafka Monitored Site", name, "enabled")
    new_val = 0 if current else 1
    frappe.db.set_value("Kafka Monitored Site", name, "enabled", new_val)
    frappe.db.commit()
    state = "enabled" if new_val else "disabled"
    return format_response(status_code=200, message=f"Site {state}.", data={"enabled": new_val})


@frappe.whitelist()
@validate_payload("name")
def delete_site(**kwargs):
    payload = kwargs
    payload.pop("cmd", None)

    name = payload.get("name")
    if not frappe.db.exists("Kafka Monitored Site", name):
        return format_response(status_code=404, message="Site not found.")

    frappe.delete_doc("Kafka Monitored Site", name)
    frappe.db.commit()
    return format_response(status_code=200, message="Site removed.")


@frappe.whitelist()
def check_sites(**kwargs):
    """Ping all enabled sites and return live status — called by the monitor page."""
    sites = frappe.get_all(
        "Kafka Monitored Site",
        filters={"enabled": 1},
        fields=["name", "site_name", "url", "status"]
    )
    results = []
    now = frappe.utils.now_datetime()

    for s in sites:
        status, ms = _ping(s.url)
        updates = {"status": status, "last_checked": now}
        if status == "Down":
            updates["last_down"] = now
        frappe.db.set_value("Kafka Monitored Site", s.name, updates, update_modified=False)
        results.append({
            "name":        s.name,
            "site_name":   s.site_name,
            "url":         s.url,
            "status":      status,
            "response_ms": round(ms) if ms is not None else None,
            "last_checked": str(now),
        })

    frappe.db.commit()
    return format_response(status_code=200, message=f"{len(results)} site(s) checked.", data=results)
