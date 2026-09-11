import threading

import frappe
from frappe.utils.password import get_decrypted_password

from kafka_mediator import (
    get_default_settings,
    validate_payload,
    format_response,
    scale_consumer,
    scale_down_consumer,
)


@frappe.whitelist()
@validate_payload("topic_name", "consumer_group", "api_endpoint")
def create_consumer(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name      = payload.get("topic_name").strip().lower()
    consumer_group  = payload.get("consumer_group").strip()
    api_endpoint    = payload.get("api_endpoint").strip()
    api_key         = payload.get("api_key")
    api_username    = payload.get("api_username")
    api_password    = payload.get("api_password")
    api_consumer_key = payload.get("consumer_key")
    api_consumer_secret = payload.get("consumer_secret")
    output_topic    = payload.get("output_topic", "").strip().lower() or None
    failure_topic   = payload.get("failure_topic", "").strip().lower() or None

    settings = get_default_settings()

    if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        return format_response(
            status_code=404,
            message=f"Topic '{topic_name}' does not exist. Create it first."
        )

    # Auto-detect cluster from the topic
    topic_cluster = frappe.db.get_value("Kafka Topic", {"topic_name": topic_name}, "cluster")
    target_cluster = topic_cluster or settings.default_cluster

    if output_topic and not frappe.db.exists("Kafka Topic", {"topic_name": output_topic}):
        return format_response(
            status_code=404,
            message=f"Output topic '{output_topic}' does not exist."
        )

    if failure_topic and not frappe.db.exists("Kafka Topic", {"topic_name": failure_topic}):
        return format_response(
            status_code=404,
            message=f"Failure topic '{failure_topic}' does not exist."
        )

    if frappe.db.exists("Kafka Consumer", {"topic": topic_name, "consumer_group": consumer_group, "enabled": 1}):
        return format_response(
            status_code=400,
            message=f"An active consumer for topic '{topic_name}' in group '{consumer_group}' already exists."
        )

    try:
        # Build consumer dict with only non-empty password fields
        consumer_dict = {
            "doctype": "Kafka Consumer",
            "cluster": target_cluster,
            "topic": topic_name,
            "consumer_group": consumer_group,
            "api_endpoint": api_endpoint,
            "api_key": api_key,
            "api_username": api_username,
            "output_topic": output_topic,
            "failure_topic": failure_topic,
            "max_lag_threshold": settings.max_lag_threshold,
            "min_lag_threshold": settings.min_lag_threshold,
            "max_consumers": settings.max_consumers,
            "scale_up_cooldown": settings.scale_up_cooldown,
            "scale_down_cooldown": settings.scale_down_cooldown,
            "enabled": 1
        }
        # Only add password fields if they have actual values
        if api_password:
            consumer_dict["api_password"] = api_password
        if api_consumer_key:
            consumer_dict["api_consumer_key"] = api_consumer_key
        if api_consumer_secret:
            consumer_dict["api_consumer_secret"] = api_consumer_secret

        consumer = frappe.get_doc(consumer_dict)
        consumer.insert()

        return format_response(
            status_code=200,
            message="Consumer created and started successfully",
            data=dict(
                consumer_id=consumer.name,
                topic_name=topic_name,
                consumer_group=consumer_group,
                output_topic=output_topic,
                failure_topic=failure_topic
            )
        )

    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to create consumer for topic '{topic_name}': {str(e)}"
        )


@frappe.whitelist()
@validate_payload("consumer_id")
def disable_consumer(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    consumer_id = payload.get("consumer_id").strip()

    if not frappe.db.exists("Kafka Consumer", consumer_id):
        return format_response(
            status_code=404,
            message=f"Consumer '{consumer_id}' not found."
        )

    try:
        frappe.db.set_value("Kafka Consumer", consumer_id, "enabled", 0, update_modified=False)
        frappe.db.commit()
        return format_response(
            status_code=200,
            message=f"Consumer '{consumer_id}' disabled."
        )
    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to disable consumer '{consumer_id}': {str(e)}"
        )


@frappe.whitelist()
@validate_payload("consumer_id")
def enable_consumer(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    consumer_id = payload.get("consumer_id").strip()

    if not frappe.db.exists("Kafka Consumer", consumer_id):
        return format_response(
            status_code=404,
            message=f"Consumer '{consumer_id}' not found."
        )

    try:
        thread_status = frappe.db.get_value("Kafka Consumer", consumer_id, "thread_status")
        if thread_status == "running":
            return format_response(
                status_code=400,
                message=f"Consumer '{consumer_id}' is already running. Disable it first if you want to restart."
            )

        frappe.db.set_value("Kafka Consumer", consumer_id, "enabled", 1, update_modified=False)
        frappe.get_doc("Kafka Consumer", consumer_id).restart_consumer_thread()
        return format_response(
            status_code=200,
            message=f"Consumer '{consumer_id}' enabled and thread started."
        )
    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to enable consumer '{consumer_id}': {str(e)}"
        )


@frappe.whitelist()
def list_consumers(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name     = payload.get("topic_name", "").strip().lower() or None
    consumer_group = payload.get("consumer_group", "").strip() or None
    enabled        = payload.get("enabled")
    search         = (payload.get("search") or "").strip() or None
    start          = int(payload.get("start") or 0)
    page_length    = int(payload.get("page_length") or 5)

    filters = []
    if topic_name:
        filters.append(["topic", "=", topic_name])
    if consumer_group:
        filters.append(["consumer_group", "=", consumer_group])
    if enabled is not None:
        filters.append(["enabled", "=", int(enabled)])
    if search:
        filters.append(["topic", "like", f"%{search}%"])

    try:
        all_consumers = frappe.get_all(
            "Kafka Consumer",
            filters=filters,
            fields=[
                "name", "topic", "consumer_group", "api_endpoint",
                "output_topic", "failure_topic", "enabled", "current_lag",
                "thread_status", "last_heartbeat", "assigned_partitions", "creation"
            ],
            order_by="creation desc"
        )
        total = len(all_consumers)
        consumers = all_consumers[start:start + page_length]
        for c in consumers:
            c["creation"] = str(c["creation"]) if c.get("creation") else None
        return format_response(
            status_code=200,
            message=f"{total} consumer(s) found.",
            data=consumers,
            pagination={"total": total, "start": start, "page_length": page_length}
        )
    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to list consumers: {str(e)}"
        )

@frappe.whitelist()
@validate_payload("topic_name", "consumer_group")
def scale_up_consumer(**kwargs):
    """Add another consumer instance to an existing group.

    Reuses an existing disabled consumer doc if one is available (re-enabling it
    and starting its thread so it rejoins the Kafka group), and only inserts a
    brand-new doc when the group has no spare disabled instance. This avoids the
    doc sprawl and repeated credential-copying of always inserting.

    The effective cap is min(max_consumers, topic partitions): a consumer group
    can never use more consumers than the topic has partitions — extras sit idle.
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name     = payload.get("topic_name").strip().lower()
    consumer_group = payload.get("consumer_group").strip()

    # Config source — prefer an enabled consumer, but fall back to any consumer
    # in the group (all may be disabled after a reset, yet still valid to scale).
    existing = frappe.db.get_value(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group, "enabled": 1},
        ["name", "cluster", "api_endpoint", "api_username", "output_topic", "failure_topic"],
        as_dict=True
    ) or frappe.db.get_value(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group},
        ["name", "cluster", "api_endpoint", "api_username", "output_topic", "failure_topic"],
        as_dict=True
    )

    if not existing:
        return format_response(
            status_code=404,
            message=(
                f"No consumer found for topic '{topic_name}' in group '{consumer_group}'. "
                f"Use create_consumer to register the first instance for this group."
            )
        )

    enabled_count = frappe.db.count("Kafka Consumer", {
        "topic": topic_name,
        "consumer_group": consumer_group,
        "enabled": 1
    })

    settings = get_default_settings()
    # Never provision more consumers than the topic has partitions.
    num_partitions = frappe.db.get_value(
        "Kafka Topic", {"topic_name": topic_name}, "num_partitions"
    ) or (settings.max_consumers or 6)
    cap = min(settings.max_consumers or 6, num_partitions)

    if enabled_count >= cap:
        return format_response(
            status_code=400,
            message=(
                f"Already at capacity ({enabled_count}/{cap}) for group "
                f"'{consumer_group}'. Topic has {num_partitions} partition(s); "
                f"additional consumers would sit idle."
            )
        )

    # Reuse a disabled doc if one exists — re-enable it and start its thread so
    # it rejoins the Kafka group, rather than creating another near-identical doc.
    reusable = frappe.db.get_value(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group, "enabled": 0},
        "name",
    )
    if reusable:
        try:
            frappe.db.set_value(
                "Kafka Consumer", reusable,
                {"enabled": 1, "thread_status": "restarting",
                 "last_heartbeat": None, "assigned_partitions": "[]"},
                update_modified=False,
            )
            frappe.db.commit()
            frappe.get_doc("Kafka Consumer", reusable).restart_consumer_thread()
            return format_response(
                status_code=200,
                message=f"Scaled up to {enabled_count + 1} consumer(s) for group '{consumer_group}' (re-enabled {reusable}).",
                data={"consumer_id": reusable, "total_instances": enabled_count + 1, "reused": True},
            )
        except Exception as e:
            return format_response(status_code=500, message=f"Failed to re-enable {reusable}: {str(e)}")

    # No spare disabled doc — create a new one, copying config from the source.
    source_name = existing.name
    source_full = frappe.db.get_value(
        "Kafka Consumer", source_name,
        ["api_key", "api_username", "max_retries"],
        as_dict=True
    )
    api_password        = get_decrypted_password("Kafka Consumer", source_name, "api_password", raise_exception=False) or None
    api_consumer_key    = get_decrypted_password("Kafka Consumer", source_name, "api_consumer_key", raise_exception=False) or None
    api_consumer_secret = get_decrypted_password("Kafka Consumer", source_name, "api_consumer_secret", raise_exception=False) or None

    try:
        # Build consumer dict with only non-None password fields
        consumer_dict = {
            "doctype": "Kafka Consumer",
            "cluster": existing.cluster,
            "topic": topic_name,
            "consumer_group": consumer_group,
            "api_endpoint": existing.api_endpoint,
            "api_key": source_full.api_key,
            "api_username": source_full.api_username,
            "output_topic": existing.output_topic,
            "failure_topic": existing.failure_topic,
            "max_retries": source_full.max_retries,
            "max_lag_threshold": settings.max_lag_threshold,
            "min_lag_threshold": settings.min_lag_threshold,
            "max_consumers": settings.max_consumers,
            "scale_up_cooldown": settings.scale_up_cooldown,
            "scale_down_cooldown": settings.scale_down_cooldown,
            "enabled": 1
        }
        # Only add password fields if they have actual values
        if api_password:
            consumer_dict["api_password"] = api_password
        if api_consumer_key:
            consumer_dict["api_consumer_key"] = api_consumer_key
        if api_consumer_secret:
            consumer_dict["api_consumer_secret"] = api_consumer_secret

        consumer = frappe.get_doc(consumer_dict)
        consumer.insert()

        return format_response(
            status_code=200,
            message=f"Scaled up to {enabled_count + 1} consumer(s) for group '{consumer_group}' (created {consumer.name}).",
            data={"consumer_id": consumer.name, "total_instances": enabled_count + 1, "reused": False}
        )
    except Exception as e:
        return format_response(status_code=500, message=str(e))


@frappe.whitelist()
@validate_payload("topic_name", "consumer_group")
def restart_consumer_group(**kwargs):
    """
    Restart all enabled consumer threads for a given topic+group in the web process.
    Called from the monitor when lag > 0 and live member_count == 0.
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name     = payload.get("topic_name").strip().lower()
    consumer_group = payload.get("consumer_group").strip()

    consumers = frappe.get_all(
        "Kafka Consumer",
        filters={"topic": topic_name, "consumer_group": consumer_group, "enabled": 1},
        fields=["name"]
    )

    if not consumers:
        return format_response(
            status_code=404,
            message=f"No enabled consumers for topic '{topic_name}' in group '{consumer_group}'."
        )

    site = frappe.local.site
    started = []
    errors = []
    for c in consumers:
        try:
            doc = frappe.get_doc("Kafka Consumer", c["name"])
            frappe.db.set_value("Kafka Consumer", c["name"], "thread_status", "restarting", update_modified=False)
            frappe.db.commit()
            threading.Thread(target=doc.consume_messages, args=(site,), daemon=True).start()
            started.append(c["name"])
        except Exception as e:
            errors.append(f"{c['name']}: {e}")

    if errors and not started:
        return format_response(status_code=500, message="; ".join(errors))

    return format_response(
        status_code=200,
        message=f"Restarted {len(started)} consumer(s) for group '{consumer_group}'.",
        data={"started": started, "errors": errors}
    )


@frappe.whitelist()
@validate_payload("topic_name", "consumer_group", "target_count")
def scale_consumers(**kwargs):
    """
    Scale consumers to target_count, running directly in the web process.

    Consumer threads must start in the gunicorn worker, not an RQ fork —
    daemon threads spawned inside an RQ job die the moment the job completes.
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name     = payload.get("topic_name").strip().lower()
    consumer_group = payload.get("consumer_group").strip()

    try:
        target_count = int(payload.get("target_count"))
    except (TypeError, ValueError):
        return format_response(status_code=400, message="target_count must be an integer.")

    settings = get_default_settings()
    max_consumers = settings.max_consumers or 6
    if target_count < 1 or target_count > max_consumers:
        return format_response(
            status_code=400,
            message=f"target_count must be between 1 and {max_consumers}."
        )

    source = frappe.db.get_value(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group, "enabled": 1},
        "name"
    )
    if not source:
        return format_response(
            status_code=404,
            message=f"No active consumers for topic '{topic_name}' in group '{consumer_group}'."
        )

    current_count = frappe.db.count(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group, "enabled": 1}
    )

    if target_count == current_count:
        return format_response(
            status_code=200,
            message=f"Already at {current_count} consumer(s). No change needed."
        )

    try:
        if target_count > current_count:
            scale_consumer(source, topic_name, consumer_group, target_count)
        else:
            scale_down_consumer(source, topic_name, consumer_group, target_count)
    except Exception as e:
        return format_response(status_code=500, message=f"Scaling failed: {str(e)}")

    new_count = frappe.db.count(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group, "enabled": 1}
    )
    direction = "up" if target_count > current_count else "down"
    return format_response(
        status_code=200,
        message=f"Scaled {direction} from {current_count} → {new_count} consumer(s).",
        data={
            "topic_name":     topic_name,
            "consumer_group": consumer_group,
            "from_count":     current_count,
            "target_count":   new_count,
        }
    )