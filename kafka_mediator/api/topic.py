import traceback

import frappe
import json
import uuid
from datetime import datetime, timezone

from kafka_mediator import (
    get_default_settings,
    validate_payload,
    format_response,
    fetch_live_topic_stats,
    fetch_live_cluster_stats,
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

def wrap_message(agent=None, bundle_id=None, source=None, resource=None):
    return {
        "meta": {
            "event": "bundle_received",
            "source": source or "kafka-mediator",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "traceId": uuid.uuid4().hex[:10],
            "agentId": agent,
            "bundleId": bundle_id,
            "version": "v1"
        },
        "resource": resource
    }

@frappe.whitelist()
@validate_payload("topic_name", "message")
def produce_message(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    message = payload.get("message")

    # Wrap any FHIR resource with metadata for downstream processing
    agent = frappe.request.headers.get("X-HIE-AGENT", "")
    source = frappe.request.headers.get("X-HIE-SOURCE", "kafka-mediator")

    # Normalise message to a dict if it arrived as a JSON string
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except (ValueError, TypeError):
            pass

    if isinstance(message, dict) and message.get("resourceType"):
        bundle_id = message.get("id", None)
        if message.get("resourceType") == "Bundle" and not bundle_id:
            return format_response(
                status_code=400,
                message="Bundle.id is required in the payload when resourceType is Bundle"
            )
        message = json.dumps(wrap_message(agent=agent, bundle_id=bundle_id, source=source, resource=message))
    else:
        if isinstance(message, (dict, list)):
            message = json.dumps(message)

    producer_name = frappe.db.get_value("Kafka Producer", {"topic": topic_name}, "name")
    if not producer_name:
        frappe.log_error("Producer Configuration Error", f"No producer configured for topic: {topic_name}")
        return format_response(
            status_code = 404,
            message = "Missing key configuration. Kindly contact support for assistance."
        )

    try:
        producer = frappe.get_doc("Kafka Producer", producer_name)
        producer.publish_message(message)

        if bundle_id:
            return format_response(
                status_code=202,
                message="Bundle received and queued for processing. Please use the bundle resource ids to track processing status.",
                data={"mediator_id": bundle_id}
            )

        return format_response(
            status_code=200,
            message="Message received and queued for processing."
        )

    except Exception:
        frappe.log_error("Failed to publish message", f"Failed to publish message to topic: {topic_name}. {traceback.format_exc()}")
        return format_response(
            status_code = 500,
            message = "Unexpected error occurred. Please try again later.",
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
        live = fetch_live_topic_stats(topic_name)

        consumers_raw = frappe.get_all(
            "Kafka Consumer",
            filters={"topic": topic_name},
            fields=["name", "consumer_group", "enabled", "current_lag",
                    "thread_status", "last_heartbeat", "assigned_partitions"]
        )
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
                "partitions":         live["partition_count"],
                "replication_factor": live["replication_factor"],
                "message_count":      live["message_count"],
                "total_lag":          live["total_lag"],
                "active_consumers":   live["active_consumers"],
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
        live_by_topic = fetch_live_cluster_stats(cluster_name)

        topic_docs = frappe.get_all(
            "Kafka Topic",
            filters={"cluster": cluster_name},
            fields=["topic_name", "num_partitions", "replication_factor"],
            order_by="topic_name asc"
        )
        topic_names = [t.topic_name for t in topic_docs]

        all_consumers = frappe.get_all(
            "Kafka Consumer",
            filters=[["topic", "in", topic_names]] if topic_names else [["topic", "=", ""]],
            fields=["name", "topic", "consumer_group", "enabled", "current_lag", "thread_status"]
        )

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
            priority = {"error": 3, "restarting": 2, "running": 1, "stopped": 0}
            if priority.get(c.thread_status or "stopped", 0) > priority.get(entry["status"], 0):
                entry["status"] = c.thread_status

        total_messages = 0
        total_lag = 0
        total_consumers = 0
        topics = []
        for t in topic_docs:
            live = live_by_topic.get(t.topic_name, {})
            msg_count = live.get("message_count", 0)
            lag = live.get("total_lag", 0)
            active = live.get("active_consumers", 0)
            total_messages += msg_count
            total_lag += lag
            total_consumers += active
            topics.append({
                "topic_name":         t.topic_name,
                "partitions":         live.get("partition_count", t.num_partitions),
                "replication_factor": live.get("replication_factor", t.replication_factor),
                "message_count":      msg_count,
                "lag":                lag,
                "active_consumers":   active,
                "consumers":          consumers_by_topic.get(t.topic_name, []),
                "consumer_groups":    [v for k, v in groups_by_topic.items() if k[0] == t.topic_name],
            })

        return format_response(
            status_code=200,
            message="Cluster stats retrieved successfully.",
            data={
                "cluster_name":    cluster_name,
                "broker_status":   "Connected",
                "total_topics":    len(topics),
                "total_messages":  total_messages,
                "total_lag":       total_lag,
                "total_consumers": total_consumers,
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
