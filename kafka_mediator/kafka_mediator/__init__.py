import frappe
import threading
import logging

logger = logging.getLogger(__name__)

STALE_HEARTBEAT_SECONDS = 120  # consumer is considered dead if no heartbeat for 2 min


def restart_dead_consumers():
    """
    Scheduled every minute. Finds enabled consumers whose thread has died
    (stale heartbeat or thread_status not 'running') and restarts them.
    """
    stale_threshold = frappe.utils.get_datetime(
        frappe.utils.add_to_date(frappe.utils.now(), seconds=-STALE_HEARTBEAT_SECONDS)
    )

    dead = frappe.get_all(
        "Kafka Consumer",
        filters={
            "enabled": 1,
        },
        fields=["name", "thread_status", "last_heartbeat"],
    )

    for row in dead:
        is_stale_heartbeat = (
            not row.last_heartbeat or row.last_heartbeat < stale_threshold
        )
        is_not_running = row.thread_status != "running"

        if is_stale_heartbeat or is_not_running:
            logger.warning(
                f"Watchdog: consumer {row.name} is dead "
                f"(status={row.thread_status}, last_heartbeat={row.last_heartbeat}) — restarting."
            )
            try:
                doc = frappe.get_doc("Kafka Consumer", row.name)
                site = frappe.local.site
                threading.Thread(
                    target=doc.consume_messages, args=(site,), daemon=True
                ).start()
            except Exception as e:
                frappe.log_error(f"Watchdog failed to restart consumer {row.name}: {str(e)}")
