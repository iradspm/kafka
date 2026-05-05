import frappe
import json

from kafka.admin import KafkaAdminClient
from kafka.protocol.admin import (
    AlterPartitionReassignmentsRequest_v0,
    ListPartitionReassignmentsRequest_v0,
)

from kafka_mediator import (
    validate_payload,
    format_response,
)


def _build_kafka_python_conf(cluster):
    conf = {
        "bootstrap_servers": cluster.bootstrap_servers,
        "security_protocol": cluster.security_protocol or "PLAINTEXT",
    }
    if cluster.sasl_mechanism:
        conf["sasl_mechanism"] = cluster.sasl_mechanism
    if cluster.sasl_username:
        conf["sasl_plain_username"] = cluster.sasl_username
    sasl_password = frappe.utils.password.get_decrypted_password(
        "Kafka Cluster", cluster.name, "sasl_password", raise_exception=False
    )
    if sasl_password:
        conf["sasl_plain_password"] = sasl_password
    return conf


def _get_admin_client(topic_name):
    topic_doc = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
    cluster = frappe.get_doc("Kafka Cluster", topic_doc.cluster)
    return KafkaAdminClient(**_build_kafka_python_conf(cluster)), cluster, topic_doc


def _get_admin_client_for_cluster(cluster_name):
    cluster = frappe.get_doc("Kafka Cluster", cluster_name)
    return KafkaAdminClient(**_build_kafka_python_conf(cluster))


def _send(admin, request):
    """Send a request to the controller and return the response."""
    future = admin._send_request_to_node(admin._controller_id, request)
    admin._wait_for_futures([future])
    return future.value


def _collect_alter_errors(response):
    # responses items are plain tuples: (name, partitions, tags)
    # partition items are plain tuples: (partition_index, error_code, error_message, tags)
    errors = {}
    for topic_resp in (response.responses or []):
        for part in (topic_resp[1] or []):
            if part[1] != 0:
                errors[f"{topic_resp[0]}[{part[0]}]"] = part[2] or f"error_code={part[1]}"
    return errors


@frappe.whitelist()
@validate_payload("topic_name", "assignments")
def reassign_partitions(**kwargs):
    """
    Initiate partition reassignment for a topic.

    POST /api/method/kafka_mediator.api.partition.reassign_partitions
    {
        "topic_name": "my-topic",
        "assignments": [
            {"partition": 0, "replicas": [1, 2, 3]},
            {"partition": 1, "replicas": [2, 3, 1]}
        ],
        "timeout_ms": 30000
    }
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    assignments = payload.get("assignments")
    timeout_ms = int(payload.get("timeout_ms") or 30000)

    if isinstance(assignments, str):
        try:
            assignments = json.loads(assignments)
        except (ValueError, TypeError):
            return format_response(status_code=400, message="'assignments' must be a JSON array.")

    if not isinstance(assignments, list) or not assignments:
        return format_response(status_code=400, message="'assignments' must be a non-empty list.")

    if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        return format_response(status_code=404, message=f"Topic '{topic_name}' not found.")

    admin = None
    try:
        admin, _, _ = _get_admin_client(topic_name)

        partitions_tuples = []
        for item in assignments:
            partition = item.get("partition")
            replicas = item.get("replicas")
            if partition is None or not isinstance(replicas, list) or not replicas:
                return format_response(
                    status_code=400,
                    message=f"Each assignment needs 'partition' (int) and 'replicas' (non-empty list). Got: {item}"
                )
            # tuple order matches schema: (partition_index, replicas, tags)
            partitions_tuples.append((int(partition), [int(r) for r in replicas], {}))

        request = AlterPartitionReassignmentsRequest_v0(
            timeout_ms=timeout_ms,
            topics=[(topic_name, partitions_tuples, {})],
            tags={},
        )

        response = _send(admin, request)

        if response.error_code != 0:
            return format_response(
                status_code=500,
                message=f"Broker error {response.error_code}: {response.error_message}"
            )

        errors = _collect_alter_errors(response)
        if errors:
            return format_response(status_code=207, message="Reassignment partially failed.", data={"errors": errors})

        return format_response(
            status_code=200,
            message=f"Partition reassignment initiated for '{topic_name}'.",
            data={"topic_name": topic_name, "partitions_reassigned": len(assignments)}
        )

    except Exception as e:
        return format_response(status_code=500, message=f"Failed to initiate reassignment: {str(e)}")
    finally:
        try:
            if admin:
                admin.close()
        except Exception:
            pass


@frappe.whitelist()
def get_reassignment_status(**kwargs):
    """
    List ongoing partition reassignments, optionally scoped to a topic.

    GET /api/method/kafka_mediator.api.partition.get_reassignment_status
        ?topic_name=<topic>         (optional — omit to list all on the cluster)
        &cluster_name=<cluster>     (required if topic_name is omitted)
        &timeout_ms=30000           (optional)
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name = (payload.get("topic_name") or "").strip().lower() or None
    cluster_name = (payload.get("cluster_name") or "").strip() or None
    timeout_ms = int(payload.get("timeout_ms") or 30000)

    admin = None
    try:
        if topic_name:
            if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
                return format_response(status_code=404, message=f"Topic '{topic_name}' not found.")
            admin, _, _ = _get_admin_client(topic_name)
            # tuple order: (name, partition_index_list, tags) — None list = all partitions
            topics_filter = [(topic_name, None, {})]

        elif cluster_name:
            if not frappe.db.exists("Kafka Cluster", cluster_name):
                return format_response(status_code=404, message=f"Cluster '{cluster_name}' not found.")
            admin = _get_admin_client_for_cluster(cluster_name)
            topics_filter = None  # None = all topics on cluster

        else:
            return format_response(status_code=400, message="Provide either 'topic_name' or 'cluster_name'.")

        request = ListPartitionReassignmentsRequest_v0(
            timeout_ms=timeout_ms,
            topics=topics_filter,
            tags={},
        )

        response = _send(admin, request)

        if response.error_code != 0:
            return format_response(
                status_code=500,
                message=f"Broker error {response.error_code}: {response.error_message}"
            )

        # topics items are plain tuples: (name, partitions, tags)
        # partition items are plain tuples: (partition_index, replicas, adding_replicas, removing_replicas, tags)
        results = []
        for topic_entry in (response.topics or []):
            for part in (topic_entry[1] or []):
                results.append({
                    "topic": topic_entry[0],
                    "partition": part[0],
                    "replicas": list(part[1]),
                    "adding_replicas": list(part[2]),
                    "removing_replicas": list(part[3]),
                })

        return format_response(
            status_code=200,
            message=f"{len(results)} ongoing reassignment(s).",
            data=results
        )

    except Exception as e:
        return format_response(status_code=500, message=f"Failed to fetch reassignment status: {str(e)}")
    finally:
        try:
            if admin:
                admin.close()
        except Exception:
            pass


@frappe.whitelist()
@validate_payload("topic_name", "partitions")
def cancel_reassignment(**kwargs):
    """
    Cancel ongoing partition reassignments by reassigning to the current replica set.

    POST /api/method/kafka_mediator.api.partition.cancel_reassignment
    {
        "topic_name": "my-topic",
        "partitions": [0, 1, 2]
    }
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    partitions = payload.get("partitions")
    timeout_ms = int(payload.get("timeout_ms") or 30000)

    if isinstance(partitions, str):
        try:
            partitions = json.loads(partitions)
        except (ValueError, TypeError):
            return format_response(status_code=400, message="'partitions' must be a JSON array of integers.")

    if not isinstance(partitions, list) or not partitions:
        return format_response(status_code=400, message="'partitions' must be a non-empty list of integers.")

    if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        return format_response(status_code=404, message=f"Topic '{topic_name}' not found.")

    admin = None
    try:
        from confluent_kafka.admin import AdminClient
        from kafka_mediator import _build_cluster_conf

        _, cluster, _ = _get_admin_client(topic_name)

        # Fetch current replica layout via confluent_kafka
        confluent_admin = AdminClient(_build_cluster_conf(cluster))
        metadata = confluent_admin.list_topics(topic=topic_name, timeout=10)
        confluent_admin.close()

        if topic_name not in metadata.topics:
            return format_response(status_code=404, message=f"Topic '{topic_name}' not found in cluster.")

        requested = {int(p) for p in partitions}
        partitions_tuples = []
        for partition_id, partition_meta in metadata.topics[topic_name].partitions.items():
            if partition_id in requested:
                # tuple order: (partition_index, replicas, tags)
                partitions_tuples.append((partition_id, list(partition_meta.replicas), {}))

        if not partitions_tuples:
            return format_response(status_code=400, message="None of the requested partitions were found in the topic.")

        admin = KafkaAdminClient(**_build_kafka_python_conf(cluster))

        request = AlterPartitionReassignmentsRequest_v0(
            timeout_ms=timeout_ms,
            topics=[(topic_name, partitions_tuples, {})],
            tags={},
        )

        response = _send(admin, request)

        if response.error_code != 0:
            return format_response(
                status_code=500,
                message=f"Broker error {response.error_code}: {response.error_message}"
            )

        errors = _collect_alter_errors(response)
        if errors:
            return format_response(status_code=207, message="Cancellation partially failed.", data={"errors": errors})

        cancelled = [t[0] for t in partitions_tuples]
        return format_response(
            status_code=200,
            message=f"Reassignment cancellation sent for {len(cancelled)} partition(s) on '{topic_name}'.",
            data={"topic_name": topic_name, "partitions": cancelled}
        )

    except Exception as e:
        return format_response(status_code=500, message=f"Failed to cancel reassignment: {str(e)}")
    finally:
        try:
            if admin:
                admin.close()
        except Exception:
            pass
