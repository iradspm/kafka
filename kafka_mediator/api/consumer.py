import frappe

from kafka_mediator import (
    get_default_settings,
    validate_payload,
    format_response
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
        consumer = frappe.get_doc({
            "doctype": "Kafka Consumer",
            "cluster": settings.default_cluster,
            "topic": topic_name,
            "consumer_group": consumer_group,
            "api_endpoint": api_endpoint,
            "api_key": api_key,
            "api_username": api_username,
            "api_password": api_password,
            "api_consumer_key":api_consumer_key,
            "api_consumer_secret": api_consumer_secret,
            "output_topic": output_topic,
            "failure_topic": failure_topic,
            "max_lag_threshold": settings.max_lag_threshold,
            "min_lag_threshold": settings.min_lag_threshold,
            "max_consumers": settings.max_consumers,
            "scale_up_cooldown": settings.scale_up_cooldown,
            "scale_down_cooldown": settings.scale_down_cooldown,
            "enabled": 1
        })
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
        frappe.db.set_value("Kafka Consumer", consumer_id, {
            "enabled": 0,
            "thread_status": "stopped"
        })
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

        frappe.db.set_value("Kafka Consumer", consumer_id, "enabled", 1)
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

    filters = {}
    if topic_name:
        filters["topic"] = topic_name
    if consumer_group:
        filters["consumer_group"] = consumer_group
    if enabled is not None:
        filters["enabled"] = int(enabled)

    try:
        consumers = frappe.get_all(
            "Kafka Consumer",
            filters=filters,
            fields=[
                "name", "topic", "consumer_group", "api_endpoint",
                "output_topic", "failure_topic", "enabled", "current_lag",
                "thread_status", "last_heartbeat", "assigned_partitions"
            ]
        )
        return format_response(
            status_code=200,
            message=f"{len(consumers)} consumer(s) found.",
            data=consumers
        )
    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to list consumers: {str(e)}"
        )

@frappe.whitelist()
@validate_payload("topic_name", "consumer_group")
def scale_up_consumer(**kwargs):
    """Add another consumer instance to an existing group."""
    payload = kwargs
    payload.pop("cmd", None)

    topic_name     = payload.get("topic_name").strip().lower()
    consumer_group = payload.get("consumer_group").strip()

    # Get config from an existing consumer in the group
    existing = frappe.db.get_value(
        "Kafka Consumer",
        {"topic": topic_name, "consumer_group": consumer_group, "enabled": 1},
        ["name", "cluster", "api_endpoint", "api_username", "output_topic", "failure_topic"],
        as_dict=True
    )

    if not existing:
        return format_response(
            status_code=404,
            message=(
                f"No active consumer found for topic '{topic_name}' in group '{consumer_group}'. "
                f"Use create_consumer to register the first instance for this group."
            )
        )

    existing_count = frappe.db.count("Kafka Consumer", {
        "topic": topic_name,
        "consumer_group": consumer_group,
        "enabled": 1
    })

    settings = get_default_settings()
    if existing_count >= (settings.max_consumers or 6):
        return format_response(
            status_code=400,
            message=f"Already at max consumers ({existing_count}) for this group."
        )

    source_name = existing.name
    api_key             = frappe.db.get_value("Kafka Consumer", source_name, "api_key")
    api_password        = frappe.utils.password.get_decrypted_password("Kafka Consumer", source_name, "api_password", raise_exception=False)
    api_consumer_key    = frappe.utils.password.get_decrypted_password("Kafka Consumer", source_name, "api_consumer_key", raise_exception=False)
    api_consumer_secret = frappe.utils.password.get_decrypted_password("Kafka Consumer", source_name, "api_consumer_secret", raise_exception=False)

    try:
        consumer = frappe.get_doc({
            "doctype": "Kafka Consumer",
            "cluster": existing.cluster,
            "topic": topic_name,
            "consumer_group": consumer_group,
            "api_endpoint": existing.api_endpoint,
            "api_key": api_key,
            "api_username": existing.api_username,
            "api_password": api_password,
            "api_consumer_key": api_consumer_key,
            "api_consumer_secret": api_consumer_secret,
            "output_topic": existing.output_topic,
            "failure_topic": existing.failure_topic,
            "max_lag_threshold": settings.max_lag_threshold,
            "min_lag_threshold": settings.min_lag_threshold,
            "max_consumers": settings.max_consumers,
            "scale_up_cooldown": settings.scale_up_cooldown,
            "scale_down_cooldown": settings.scale_down_cooldown,
            "enabled": 1
        })
        consumer.insert()

        return format_response(
            status_code=200,
            message=f"Scaled up to {existing_count + 1} consumers for group '{consumer_group}'.",
            data={"consumer_id": consumer.name, "total_instances": existing_count + 1}
        )
    except Exception as e:
        return format_response(status_code=500, message=str(e))