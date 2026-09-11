import threading
import traceback
import json
import uuid
from datetime import datetime, timezone

import logging

import frappe
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient, NewTopic, _ConsumerGroupTopicPartitions, ConfigResource

logger = logging.getLogger(__name__)

from kafka_mediator import (
    get_default_settings,
    validate_payload,
    format_response,
    fetch_live_cluster_stats,
    _build_cluster_conf,
)

@frappe.whitelist()
@validate_payload("topic_name", "num_partitions", "replication_factor")
def create_topic(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    num_partitions = int(payload.get("num_partitions", 6))
    replication_factor = int(payload.get("replication_factor", 3))
    cluster_name = (payload.get("cluster") or "").strip() or None

    settings = get_default_settings()

    if cluster_name:
        if not frappe.db.exists("Kafka Cluster", cluster_name):
            return format_response(
                status_code=404,
                message=f"Cluster '{cluster_name}' not found."
            )
        target_cluster = cluster_name
    else:
        target_cluster = settings.default_cluster

    if frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        return format_response(
            status_code = 400,
            message = f"Topic {topic_name} already exists"
        )

    try:
        topic = frappe.get_doc({
            "doctype": "Kafka Topic",
            "cluster": target_cluster,
            "topic_name": topic_name,
            "num_partitions": num_partitions or settings.default_partitions,
            "replication_factor": replication_factor or settings.default_replication_factor
        })
        topic.insert()

        producer = frappe.get_doc({
            "doctype": "Kafka Producer",
            "cluster": target_cluster,
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

def wrap_message(agent=None, bundle_id=None, source=None, resource=None, callback_url=None):
    return {
        "meta": {
            "event": "bundle_received",
            "source": source or "kafka-mediator",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "traceId": uuid.uuid4().hex[:10],
            "agentId": agent,
            "bundleId": bundle_id,
            "callbackUrl": callback_url,
            "version": "v1"
        },
        "resource": resource
    }

def operation_outcome(issue_severity, issue_code, issue_details,status_code=200, id=None):
    operation_outcome = {
        "resourceType": "OperationOutcome",
        **({"id": id} if id else {}),
        "issue": [
            {
                "severity": issue_severity,
                "code": issue_code,
                "details": {
                    "text": issue_details
                }
            }
        ]
    }
    frappe.local.response.http_status_code = status_code
    frappe.local.response.update(operation_outcome)

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
    callback_url = frappe.request.headers.get("X-HIE-CALLBACK", "")

    bundle_id = None

    # Normalise message to a dict if it arrived as a JSON string
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except (ValueError, TypeError):
            pass

    if isinstance(message, dict) and message.get("resourceType"):
        bundle_id = message.get("id", None)
        if message.get("resourceType") == "Bundle" and not bundle_id:
            return operation_outcome(
                issue_severity="error",
                issue_code="invalid",
                issue_details="Bundle.id is required in the payload when resourceType is Bundle",
                status_code=400
            )
        message = json.dumps(wrap_message(
            agent=agent, 
            bundle_id=bundle_id, 
            source=source,
            resource=message,
            callback_url=callback_url or None
            ))
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
            return operation_outcome(
                issue_severity="information",
                issue_code="informational",
                issue_details="Bundle received and queued for processing. Please use the bundle resource ids to track processing status.",
                status_code=202,
                id=bundle_id
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
@validate_payload("topic_name", "message")
def publish_message(**kwargs):

    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    message = payload.get("message")

    # Wrap any FHIR resource with metadata for downstream processing
    agent = frappe.request.headers.get("X-HIE-AGENT", "")
    source = frappe.request.headers.get("X-HIE-SOURCE", "kafka-mediator")
    callback_url = frappe.request.headers.get("X-HIE-CALLBACK", "")

    bundle_id = None

    # Normalise message to a dict if it arrived as a JSON string
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except (ValueError, TypeError):
            pass

    if isinstance(message, dict) and message.get("resourceType"):
        bundle_id = message.get("id", None)
        message = json.dumps(wrap_message(
            agent=agent,
            bundle_id=bundle_id,
            source=source,
            resource=message,
            callback_url=callback_url or None
            ))
    else:
        if isinstance(message, (dict, list)):
            message = json.dumps(message)

    producer_name = frappe.db.get_value("Kafka Producer", {"topic": topic_name}, "name")
    if not producer_name:
        frappe.log_error("Producer Configuration Error", f"No producer configured for topic: {topic_name}")
        return operation_outcome(
            issue_severity="error",
            issue_code="not-found",
            issue_details="Missing key configuration. Kindly contact support for assistance.",
            status_code=404
        )

    try:
        producer = frappe.get_doc("Kafka Producer", producer_name)
        producer.publish_message(message)

        return operation_outcome(
            issue_severity="information",
            issue_code="informational",
            issue_details="Message received and queued for processing",
            status_code=202,
            id=bundle_id if bundle_id else None
        )

    except Exception:
        frappe.log_error("Failed to publish message", f"Failed to publish message to topic: {topic_name}. {traceback.format_exc()}")
        return operation_outcome(
            issue_severity="error",
            issue_code="exception",
            issue_details="Unexpected error occurred. Please try again later.",
            status_code=500
        )


@frappe.whitelist()
@validate_payload("topic_name")
def get_topic_stats(**kwargs):
    """
    Live stats for a single topic sourced entirely from Kafka.
    Groups come from Kafka's live group list, not Frappe consumer docs.
    """
    payload = kwargs
    payload.pop("cmd", None)
    topic_name = payload.get("topic_name").strip().lower()

    # Determine which cluster this topic lives on
    topic_row = frappe.db.get_value(
        "Kafka Topic", {"topic_name": topic_name}, ["name", "cluster", "replication_factor"], as_dict=True
    )
    if not topic_row:
        return format_response(status_code=404, message=f"Topic '{topic_name}' not found.")

    cluster_name = topic_row.cluster

    try:
        cluster = frappe.get_doc("Kafka Cluster", cluster_name)
        conf = _build_cluster_conf(cluster)
        stats_conf = _build_cluster_conf(cluster, group_id=f"kafka_mediator_live_stats_{cluster_name}")

        admin_client = AdminClient(conf)
        consumer = Consumer(stats_conf)

        try:
            # ── watermarks ───────────────────────────────────────────────────
            meta = consumer.list_topics(topic=topic_name, timeout=10)
            if topic_name not in meta.topics:
                return format_response(status_code=404, message=f"Topic '{topic_name}' not found in Kafka.")

            partition_ids = list(meta.topics[topic_name].partitions.keys())
            high_watermarks = {}
            message_count = 0
            for p in partition_ids:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic_name, p))
                high_watermarks[p] = high
                message_count += max(0, high - low)

            # ── replication factor ────────────────────────────────────────────
            replication_factor = topic_row.replication_factor or 1
            try:
                futures = admin_client.describe_topics(TopicCollection([topic_name]), request_timeout=10)
                desc = futures[topic_name].result()
                if desc.partitions:
                    replication_factor = len(desc.partitions[0].replicas)
            except Exception as e:
                logger.error(f"replication_factor fetch failed for {topic_name}: {e}")

            # ── consumer groups from Kafka ────────────────────────────────────
            try:
                cg_result = admin_client.list_consumer_groups().result()
                all_group_ids = [
                    g.group_id for g in cg_result.valid
                    if not g.group_id.startswith("kafka_mediator_live_stats_")
                    and not g.group_id.startswith("kafka_mediator_stats_")
                ]
            except Exception as e:
                logger.error(f"list_consumer_groups failed: {e}")
                all_group_ids = []

            # member counts
            member_counts = {}
            if all_group_ids:
                try:
                    desc_futures = admin_client.describe_consumer_groups(all_group_ids)
                    for gid, fut in desc_futures.items():
                        try:
                            member_counts[gid] = len(fut.result().members)
                        except Exception:
                            member_counts[gid] = 0
                except Exception as e:
                    logger.error(f"describe_consumer_groups failed: {e}")

            # Frappe docs for thread_status cross-reference
            frappe_consumers = frappe.get_all(
                "Kafka Consumer",
                filters={"cluster": cluster_name},
                fields=["name", "consumer_group", "enabled", "thread_status", "last_heartbeat"]
            )
            frappe_by_group = {}
            for c in frappe_consumers:
                frappe_by_group.setdefault(c.consumer_group, []).append(c)

            # ── per-group lag ─────────────────────────────────────────────────
            total_lag = 0
            consumer_groups = []
            priority = {"error": 3, "restarting": 2, "running": 1, "stopped": 0}

            for group_id in all_group_ids:
                try:
                    offset_futures = admin_client.list_consumer_group_offsets(
                        [_ConsumerGroupTopicPartitions(group_id)]
                    )
                    result = offset_futures[group_id].result()
                except Exception as e:
                    logger.error(f"list_consumer_group_offsets failed for {group_id}: {e}")
                    continue

                group_lag = 0
                subscribes_this_topic = False
                for tp in (result.topic_partitions or []):
                    if tp.topic != topic_name:
                        continue
                    if tp.offset is None or tp.offset < 0:
                        continue
                    high = high_watermarks.get(tp.partition, 0)
                    group_lag += max(0, high - tp.offset)
                    subscribes_this_topic = True

                if not subscribes_this_topic:
                    continue

                total_lag += group_lag

                frappe_members = frappe_by_group.get(group_id, [])
                worst_status = "stopped"
                for fm in frappe_members:
                    st = fm.thread_status or "stopped"
                    if priority.get(st, 0) > priority.get(worst_status, 0):
                        worst_status = st

                consumer_groups.append({
                    "consumer_group":  group_id,
                    "total_lag":       group_lag,
                    "member_count":    member_counts.get(group_id, 0),
                    "thread_status":   worst_status,
                    "frappe_consumers": [
                        {
                            "name":          fm.name,
                            "enabled":       fm.enabled,
                            "thread_status": fm.thread_status,
                            "last_heartbeat": str(fm.last_heartbeat) if fm.last_heartbeat else None,
                        }
                        for fm in frappe_members
                    ],
                })

        finally:
            try:
                consumer.close()
            except Exception:
                pass

        retention_days = _get_topic_retention_days(admin_client, topic_name)

        return format_response(
            status_code=200,
            message="Topic stats retrieved successfully.",
            data={
                "topic_name":         topic_name,
                "cluster":            cluster_name,
                "partitions":         len(partition_ids),
                "replication_factor": replication_factor,
                "message_count":      message_count,
                "lag":                total_lag,
                "retention_days":     retention_days,
                "consumer_groups":    consumer_groups,
                "consumers": [
                    fm
                    for g in consumer_groups
                    for fm in g["frappe_consumers"]
                ],
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
        live = fetch_live_cluster_stats(cluster_name)
        live_topics = live.get("topics", {})
        live_groups = live.get("groups", {})

        # Cross-reference Frappe for thread_status of each consumer doc
        frappe_consumers = frappe.get_all(
            "Kafka Consumer",
            filters={"cluster": cluster_name},
            fields=["name", "topic", "consumer_group", "enabled", "thread_status", "last_heartbeat", "assigned_partitions"]
        )
        frappe_by_group = {}  # consumer_group → list of consumer dicts
        for c in frappe_consumers:
            frappe_by_group.setdefault(c.consumer_group, []).append(c)

        # Build topic list from what Kafka actually has
        total_messages = 0
        total_lag = 0
        total_consumer_members = 0
        topics = []

        for topic_name, ts in sorted(live_topics.items()):
            msg_count = ts["message_count"]
            lag = ts["total_lag"]
            total_messages += msg_count
            total_lag += lag

            # Consumer groups that have committed offsets to this topic (from Kafka)
            consumer_groups = []
            for group_id, gs in live_groups.items():
                if topic_name not in gs["topics"]:
                    continue
                member_count = gs["member_count"]
                total_consumer_members += member_count
                group_lag = ts["lag_by_group"].get(group_id, 0)

                # thread_status from Frappe docs in this group
                frappe_members = frappe_by_group.get(group_id, [])
                priority = {"error": 3, "restarting": 2, "running": 1, "stopped": 0}
                worst_status = "stopped"
                for fm in frappe_members:
                    st = fm.thread_status or "stopped"
                    if priority.get(st, 0) > priority.get(worst_status, 0):
                        worst_status = st

                consumer_groups.append({
                    "consumer_group":  group_id,
                    "total_lag":       group_lag,
                    "member_count":    member_count,   # live members in Kafka
                    "thread_status":   worst_status,   # from Frappe thread tracking
                    "frappe_consumers": [
                        {
                            "name":          fm.name,
                            "enabled":       fm.enabled,
                            "thread_status": fm.thread_status,
                            "last_heartbeat": str(fm.last_heartbeat) if fm.last_heartbeat else None,
                        }
                        for fm in frappe_members
                    ],
                })

            topics.append({
                "topic_name":         topic_name,
                "partitions":         ts["partition_count"],
                "replication_factor": ts["replication_factor"],
                "message_count":      msg_count,
                "lag":                lag,
                "retention_days":     ts.get("retention_days"),
                "consumer_groups":    consumer_groups,
                # legacy field the JS panel uses to render consumer cards
                "consumers": [
                    fm
                    for g in consumer_groups
                    for fm in g["frappe_consumers"]
                ],
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
                "total_consumers": total_consumer_members,
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
                "message_count", "lag", "active_consumers", "last_updated", "creation"
            ],
            order_by="creation desc"
        )

        # Fetch live lag from Kafka (same source as kafka-monitor)
        clusters_seen = {}
        for t in topics:
            c = t.get("cluster")
            if c and c not in clusters_seen:
                clusters_seen[c] = True

        live_map = {}
        for cluster_name in clusters_seen:
            try:
                live = fetch_live_cluster_stats(cluster_name)
                for topic_name, ts in live.get("topics", {}).items():
                    live_map[topic_name] = ts
            except Exception:
                pass

        for topic in topics:
            topic["last_updated"] = str(topic["last_updated"]) if topic.get("last_updated") else None
            topic["creation"] = str(topic["creation"]) if topic.get("creation") else None
            live = live_map.get(topic["topic_name"])
            if live:
                topic["message_count"] = live.get("message_count", topic.get("message_count", 0))
                topic["lag"] = live.get("total_lag", topic.get("lag", 0))
                topic["num_partitions"] = live.get("partition_count", topic.get("num_partitions"))
                topic["replication_factor"] = live.get("replication_factor", topic.get("replication_factor", 1))

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


@frappe.whitelist()
def list_clusters(**kwargs):
    """Return all Kafka Cluster docs (name only)."""
    try:
        clusters = frappe.get_all(
            "Kafka Cluster",
            fields=["name"],
            order_by="name asc"
        )
        return format_response(
            status_code=200,
            message=f"{len(clusters)} cluster(s) found.",
            data=clusters
        )
    except Exception as e:
        return format_response(
            status_code=500,
            message=f"Failed to list clusters: {str(e)}"
        )


def _get_topic_retention_days(admin_client, topic_name):
    """Return retention period in days, or None if unset / infinite."""
    try:
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
        futures = admin_client.describe_configs([resource])
        config = futures[resource].result()
        raw = config.get("retention.ms")
        if raw is None:
            return None
        ms = int(raw.value)
        if ms < 0:
            return None  # -1 means infinite retention
        return round(ms / 86_400_000, 2)
    except Exception:
        return None


@frappe.whitelist()
@validate_payload("topic_name", "retention_days")
def set_topic_retention(**kwargs):
    """
    Set retention period on a Kafka topic.

    POST /api/method/kafka_mediator.api.topic.set_topic_retention
        topic_name=<name>&retention_days=<days>

    Pass retention_days=-1 to set infinite retention.
    """
    payload = kwargs
    payload.pop("cmd", None)

    topic_name = payload.get("topic_name").strip().lower()
    try:
        retention_days = float(payload.get("retention_days"))
    except (ValueError, TypeError):
        return format_response(status_code=400, message="retention_days must be a number.")

    if retention_days != -1 and retention_days < 5:
        return format_response(status_code=400, message="retention_days must be at least 5 (or -1 for infinite).")

    topic_row = frappe.db.get_value("Kafka Topic", {"topic_name": topic_name}, ["name", "cluster"], as_dict=True)
    if not topic_row:
        return format_response(status_code=404, message=f"Topic '{topic_name}' not found.")

    retention_ms = str(int(retention_days * 86_400_000)) if retention_days != -1 else "-1"

    try:
        cluster = frappe.get_doc("Kafka Cluster", topic_row.cluster)
        conf = _build_cluster_conf(cluster)
        admin_client = AdminClient(conf)

        resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name, set_config={"retention.ms": retention_ms})
        futures = admin_client.alter_configs([resource])
        futures[resource].result()

        label = "infinite" if retention_days == -1 else f"{retention_days} day(s)"
        return format_response(
            status_code=200,
            message=f"Retention for '{topic_name}' set to {label}.",
            data={"topic_name": topic_name, "retention_ms": int(retention_ms), "retention_days": retention_days}
        )

    except Exception as e:
        return format_response(status_code=500, message=f"Failed to set retention for '{topic_name}': {str(e)}")


@frappe.whitelist()
def sync_with_kafka(**kwargs):
    """
    Compare Frappe docs against live Kafka state and recreate anything missing.

    Per cluster:
      - Topics in Frappe but absent from Kafka → recreate via AdminClient
      - Consumers whose topic was just recreated → force-restart (even if thread_status = running)
      - Consumers whose consumer_group is absent from Kafka → force-restart
      - Consumers with enabled=1 and thread_status stopped/error → simple restart
    """
    recreated_topics = []
    failed_topics = []
    restarted_consumers = []
    failed_consumers = []

    # consumer_name → "force" | "simple"
    consumers_to_restart = {}

    site = frappe.local.site
    clusters = frappe.get_all("Kafka Cluster", fields=["name"])

    for cluster_row in clusters:
        cluster_name = cluster_row["name"]
        cluster = frappe.get_doc("Kafka Cluster", cluster_name)
        conf = _build_cluster_conf(cluster)

        # fetch live topics and consumer groups in one connection
        try:
            admin_client = AdminClient(conf)
            kafka_topics = set(admin_client.list_topics(timeout=10).topics.keys())
            try:
                cg_result = admin_client.list_consumer_groups().result()
                kafka_groups = {g.group_id for g in cg_result.valid}
            except Exception as e:
                frappe.log_error(f"sync_with_kafka: could not list consumer groups for '{cluster_name}': {e}")
                kafka_groups = None
        except Exception as e:
            frappe.log_error(f"sync_with_kafka: could not reach cluster '{cluster_name}': {e}")
            continue

        # recreate missing topics
        frappe_topics = frappe.get_all(
            "Kafka Topic",
            filters={"cluster": cluster_name},
            fields=["name", "topic_name", "num_partitions", "replication_factor"]
        )

        for t in frappe_topics:
            if t["topic_name"] in kafka_topics:
                continue
            try:
                ac = AdminClient(conf)
                new_topic = NewTopic(
                    t["topic_name"],
                    num_partitions=int(t["num_partitions"] or 1),
                    replication_factor=int(t["replication_factor"] or 1)
                )
                futures = ac.create_topics([new_topic], request_timeout=30)
                futures[t["topic_name"]].result()
                recreated_topics.append(t["topic_name"])
            except Exception as e:
                failed_topics.append({"topic": t["topic_name"], "error": str(e)})
                frappe.log_error(f"sync_with_kafka: failed to recreate topic '{t['topic_name']}': {e}")

        # decide which consumers need restarting for this cluster
        cluster_consumers = frappe.get_all(
            "Kafka Consumer",
            filters={"cluster": cluster_name, "enabled": 1},
            fields=["name", "consumer_group", "topic", "thread_status"]
        )

        for c in cluster_consumers:
            if c["name"] in consumers_to_restart:
                continue  # already scheduled

            topic_missing   = c["topic"] in recreated_topics
            group_missing   = kafka_groups is not None and c["consumer_group"] not in kafka_groups
            thread_dead     = c["thread_status"] in ("stopped", "error", None, "")

            if topic_missing or group_missing:
                # force-restart: signal running thread to stop first
                consumers_to_restart[c["name"]] = "force"
            elif thread_dead:
                consumers_to_restart[c["name"]] = "simple"

    def _force_restart(consumer_name):
        """Disable → wait for thread to exit → re-enable and restart."""
        import frappe as _f, time as _t
        _f.init(site=site)
        _f.connect()
        try:
            _f.db.set_value("Kafka Consumer", consumer_name, "enabled", 0)
            _f.db.commit()
            deadline = _t.monotonic() + 10
            while _t.monotonic() < deadline:
                if _f.db.get_value("Kafka Consumer", consumer_name, "thread_status") in ("stopped", "error", None):
                    break
                _t.sleep(0.5)
            _f.db.set_value("Kafka Consumer", consumer_name, "enabled", 1)
            _f.db.commit()
            _f.get_doc("Kafka Consumer", consumer_name).restart_consumer_thread()
        finally:
            _f.destroy()

    for consumer_name, mode in consumers_to_restart.items():
        try:
            if mode == "force":
                threading.Thread(target=_force_restart, args=(consumer_name,), daemon=True).start()
            else:
                doc = frappe.get_doc("Kafka Consumer", consumer_name)
                threading.Thread(target=doc.consume_messages, args=(site,), daemon=True).start()
            restarted_consumers.append(consumer_name)
        except Exception as e:
            failed_consumers.append({"consumer": consumer_name, "error": str(e)})
            frappe.log_error(f"sync_with_kafka: failed to restart consumer '{consumer_name}': {e}")

    msg_parts = []
    if recreated_topics:
        msg_parts.append(f"{len(recreated_topics)} topic(s) recreated in Kafka")
    if restarted_consumers:
        msg_parts.append(f"{len(restarted_consumers)} consumer thread(s) restarted")
    if failed_topics:
        msg_parts.append(f"{len(failed_topics)} topic(s) failed")
    if failed_consumers:
        msg_parts.append(f"{len(failed_consumers)} consumer(s) failed to restart")
    if not msg_parts:
        msg_parts.append("Everything is in sync — nothing to do")

    return format_response(
        status_code=200,
        message=". ".join(msg_parts) + ".",
        data={
            "recreated_topics": recreated_topics,
            "restarted_consumers": restarted_consumers,
            "failed_topics": failed_topics,
            "failed_consumers": failed_consumers,
        }
    )
