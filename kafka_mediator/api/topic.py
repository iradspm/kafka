import frappe
import json

from kafka_mediator import (
    get_default_settings,
    validate_payload,
    format_response
    )

@frappe.whitelist()
@validate_payload("topic_name", "num_partitions", "replication_factor")
def create_topic(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    num_partitions = int(payload.get("num_partitions", 6))
    replication_factor = int(payload.get("replication_factor", 3))

    settings = get_default_settings()
    if frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        return format_response(
            status_code = 400,
            message = f"Topic {topic_name} already exists"
        )

    try:
        topic = frappe.get_doc({
            "doctype": "Kafka Topic",
            "cluster": settings.default_cluster,
            "topic_name": topic_name,
            "num_partitions": num_partitions or settings.default_partitions,
            "replication_factor": replication_factor or settings.default_replication_factor
        })
        topic.insert()

        producer = frappe.get_doc({
            "doctype": "Kafka Producer",
            "cluster": settings.default_cluster,
            "topic": topic_name
        })
        producer.insert()

        return format_response(
            status_code = 200,
            message = "Topic successfully created",
            data = dict(
                topic_name = topic_name,
                producer_id = producer.name,

            )
        )

    except Exception as e:
        return format_response(
            status_code = 500,
            message = f"Failed to create topic: {topic_name} - {str(e)}"
        )


@frappe.whitelist()
@validate_payload("topic_name", "message")
def produce_message(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    message = payload.get("message")

    # Accept dict/list passed as JSON object, or a JSON string
    if isinstance(message, (dict, list)):
        message = json.dumps(message)
    elif isinstance(message, str):
        try:
            json.loads(message)  # validate it's proper JSON if string
        except (ValueError, TypeError):
            pass  # plain string messages are also allowed

    producer_name = frappe.db.get_value("Kafka Producer", {"topic": topic_name}, "name")
    if not producer_name:
        return format_response(
            status_code = 404,
            message = f"No producer found for topic: {topic_name}"
        )

    try:
        producer = frappe.get_doc("Kafka Producer", producer_name)
        producer.publish_message(message)

        return format_response(
            status_code = 200,
            message = "Message published successfully",
            data = dict(
                topic_name = topic_name,
                producer_id = producer_name
            )
        )

    except Exception as e:
        return format_response(
            status_code = 500,
            message = f"Failed to produce message to {topic_name}: {str(e)}"
        )


@frappe.whitelist()
@validate_payload("topic_name")
def get_topic_stats(**kwargs):
    """
    Stats for a single topic — message count, lag, active consumers,
    partition count, replication factor, and freshness timestamp.

    GET /api/method/kafka_mediator.api.topic.get_topic_stats
        ?topic_name=<topic>
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()

    if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        return format_response(
            status_code=404,
            message=f"Topic '{topic_name}' not found."
        )

    try:
        topic = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})

        consumers_raw = frappe.get_all(
            "Kafka Consumer",
            filters={"topic": topic_name},
            fields=["name", "consumer_group", "enabled", "current_lag",
                    "thread_status", "last_heartbeat", "assigned_partitions"]
        )

        # Parse assigned_partitions from stored JSON string to a real list
        consumers = []
        for c in consumers_raw:
            try:
                c["assigned_partitions"] = json.loads(c["assigned_partitions"] or "[]")
            except (ValueError, TypeError):
                c["assigned_partitions"] = []
            consumers.append(c)

        return format_response(
            status_code=200,
            message="Topic stats retrieved successfully.",
            data={
                "topic_name":         topic.topic_name,
                "cluster":            topic.cluster,
                "partitions":         topic.num_partitions,
                "replication_factor": topic.replication_factor,
                "message_count":      topic.message_count or 0,
                "total_lag":          topic.lag or 0,
                "active_consumers":   topic.active_consumers or 0,
                "last_updated":       str(topic.last_updated) if topic.last_updated else None,
                "consumers":          consumers
            }
        )

    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to retrieve stats for topic '{topic_name}': {str(e)}"
        )


@frappe.whitelist()
@validate_payload("cluster_name")
def get_cluster_stats(**kwargs):
    """
    Summary stats for a cluster — broker status, totals, and a per-topic
    breakdown including per-topic consumer counts.

    GET /api/method/kafka_mediator.api.topic.get_cluster_stats
        ?cluster_name=<cluster>
    """
    payload = kwargs
    payload.pop("cmd", None)

    cluster_name = payload.get("cluster_name").strip()

    if not frappe.db.exists("Kafka Cluster", cluster_name):
        return format_response(
            status_code=404,
            message=f"Cluster '{cluster_name}' not found."
        )

    try:
        # Cluster-level summary
        if not frappe.db.exists("Kafka Cluster Stats", {"cluster": cluster_name}):
            return format_response(
                status_code=404,
                message=f"No stats recorded yet for cluster '{cluster_name}'. "
                        f"Stats are refreshed every 5 minutes."
            )

        stats = frappe.get_doc("Kafka Cluster Stats", {"cluster": cluster_name})

        # Per-topic breakdown — single query
        topics = frappe.get_all(
            "Kafka Topic",
            filters={"cluster": cluster_name},
            fields=[
                "topic_name", "num_partitions", "replication_factor",
                "message_count", "lag", "active_consumers", "last_updated"
            ],
            order_by="topic_name asc"
        )

        # Fetch all consumers for this cluster in one query, group in Python
        # (avoids N+1: one query here instead of one per topic)
        topic_names = [t.topic_name for t in topics]
        all_consumers = frappe.get_all(
            "Kafka Consumer",
            filters=[["topic", "in", topic_names]] if topic_names else [["topic", "=", ""]],
            fields=["name", "topic", "consumer_group", "enabled", "current_lag", "thread_status"]
        )

        # Build two views per topic:
        #   consumers      — every individual consumer doc (shows per-instance thread_status)
        #   consumer_groups — one entry per group (aggregated lag, worst-case status)
        consumers_by_topic = {}
        groups_by_topic = {}
        for c in all_consumers:
            consumers_by_topic.setdefault(c.topic, []).append({
                "name": c.name,
                "consumer_group": c.consumer_group,
                "enabled": c.enabled,
                "current_lag": c.current_lag,
                "thread_status": c.thread_status
            })
            # Aggregate per group: sum lag, surface worst status
            group_key = (c.topic, c.consumer_group)
            if group_key not in groups_by_topic:
                groups_by_topic[group_key] = {
                    "consumer_group": c.consumer_group,
                    "total_lag": 0,
                    "active_count": 0,
                    "status": c.thread_status or "stopped"
                }
            entry = groups_by_topic[group_key]
            entry["total_lag"] += c.current_lag or 0
            if c.enabled:
                entry["active_count"] += 1
            # Escalate: error > restarting > running > stopped
            priority = {"error": 3, "restarting": 2, "running": 1, "stopped": 0}
            if priority.get(c.thread_status or "stopped", 0) > priority.get(entry["status"], 0):
                entry["status"] = c.thread_status

        for topic in topics:
            topic["consumers"] = consumers_by_topic.get(topic.topic_name, [])
            topic["consumer_groups"] = [
                v for k, v in groups_by_topic.items() if k[0] == topic.topic_name
            ]
            topic["last_updated"] = str(topic["last_updated"]) if topic.get("last_updated") else None

        return format_response(
            status_code=200,
            message="Cluster stats retrieved successfully.",
            data={
                "cluster_name":    cluster_name,
                "broker_status":   stats.broker_status,
                "total_topics":    stats.total_topics or 0,
                "total_messages":  stats.total_messages or 0,
                "total_lag":       stats.total_lag or 0,
                "total_consumers": stats.total_consumers or 0,
                "last_updated":    str(stats.last_updated) if stats.last_updated else None,
                "topics":          topics
            }
        )

    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to retrieve stats for cluster '{cluster_name}': {str(e)}"
        )


@frappe.whitelist()
def list_topics(**kwargs):
    """
    List all topics, optionally filtered by cluster, with their cached stats.

    GET /api/method/kafka_mediator.api.topic.list_topics
        ?cluster_name=<cluster>   (optional)
    """
    payload = kwargs
    payload.pop("cmd", None)

    cluster_name = (payload.get("cluster_name") or "").strip() or None

    filters = {}
    if cluster_name:
        if not frappe.db.exists("Kafka Cluster", cluster_name):
            return format_response(
                status_code=404,
                message=f"Cluster '{cluster_name}' not found."
            )
        filters["cluster"] = cluster_name

    try:
        topics = frappe.get_all(
            "Kafka Topic",
            filters=filters,
            fields=[
                "topic_name", "cluster", "num_partitions", "replication_factor",
                "message_count", "lag", "active_consumers", "last_updated"
            ],
            order_by="cluster asc, topic_name asc"
        )

        for topic in topics:
            topic["last_updated"] = str(topic["last_updated"]) if topic.get("last_updated") else None

        return format_response(
            status_code=200,
            message=f"{len(topics)} topic(s) found.",
            data=topics
        )

    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to list topics: {str(e)}"
        )
