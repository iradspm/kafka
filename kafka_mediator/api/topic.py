import frappe
import traceback
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