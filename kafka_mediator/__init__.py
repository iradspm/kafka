__version__ = "0.0.1"

import frappe
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient
import time
import logging
import json
from functools import wraps

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEARTBEAT_STALE_SECONDS = 120  # restart consumer if no heartbeat for 2 min


def get_default_settings():
    return frappe.get_doc("Kafka Settings")


# ---------------------------------------------------------------------------
# Dead consumer watchdog — called every minute by scheduler
# ---------------------------------------------------------------------------

def restart_dead_consumers():
    """
    Finds enabled consumers whose last_heartbeat is stale (> HEARTBEAT_STALE_SECONDS old)
    and restarts their thread. Runs every minute via hooks.py scheduler.
    """
    stale_threshold = frappe.utils.add_to_date(
        frappe.utils.now_datetime(), seconds=-HEARTBEAT_STALE_SECONDS
    )

    dead_consumers = frappe.get_all(
        "Kafka Consumer",
        filters=[
            ["enabled", "=", 1],
            ["thread_status", "in", ["running", "error", "restarting"]],
            ["last_heartbeat", "<", stale_threshold]
        ],
        fields=["name", "topic", "cluster"]
    )

    for entry in dead_consumers:
        try:
            consumer_doc = frappe.get_doc("Kafka Consumer", entry["name"])
            frappe.db.set_value("Kafka Consumer", entry["name"], "thread_status", "restarting")
            frappe.db.commit()
            consumer_doc.restart_consumer_thread()
            logger.info(f"Auto-restarted dead consumer {entry['name']}")
        except Exception as e:
            frappe.log_error(f"Failed to auto-restart consumer {entry['name']}: {str(e)}")


# ---------------------------------------------------------------------------
# Auto-scaling monitor — called every 5 minutes by scheduler
# ---------------------------------------------------------------------------

@frappe.whitelist()
def monitor_kafka_metrics():
    settings = get_default_settings()
    consumers = frappe.get_all("Kafka Consumer", fields=[
        "name", "cluster", "topic", "consumer_group", "max_lag_threshold", "min_lag_threshold",
        "max_consumers", "scale_up_cooldown", "scale_down_cooldown", "last_scale_up_time",
        "last_scale_down_time", "low_lag_count"
    ], filters={"enabled": 1})

    # Deduplicate: one scaling decision per (topic, consumer_group) pair.
    # Without this, N consumers sharing the same group each trigger a separate
    # scale decision and can multiply the scale-up N times in one cycle.
    seen_groups = set()

    for consumer in consumers:
        group_key = (consumer.topic, consumer.consumer_group)
        if group_key in seen_groups:
            continue
        seen_groups.add(group_key)

        cluster = frappe.get_doc("Kafka Cluster", consumer.cluster)
        conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            'group.id': consumer.consumer_group
        }
        if cluster.sasl_mechanism:
            conf['sasl.mechanisms'] = cluster.sasl_mechanism
        if cluster.sasl_username:
            conf['sasl.username'] = cluster.sasl_username
        sasl_password = cluster.get_password("sasl_password")
        if sasl_password:
            conf['sasl.password'] = sasl_password
        try:
            lag = check_consumer_lag(conf, consumer.topic, consumer.consumer_group)
            frappe.db.set_value("Kafka Consumer", consumer.name, "current_lag", lag)

            current_time = time.time()
            max_lag_threshold = consumer.max_lag_threshold or settings.max_lag_threshold
            min_lag_threshold = consumer.min_lag_threshold or settings.min_lag_threshold
            max_consumers = consumer.max_consumers or settings.max_consumers
            scale_up_cooldown = consumer.scale_up_cooldown or settings.scale_up_cooldown
            scale_down_cooldown = consumer.scale_down_cooldown or settings.scale_down_cooldown
            messages_per_consumer = settings.messages_per_consumer

            target_consumers = max(1, min(max_consumers, (lag + messages_per_consumer - 1) // messages_per_consumer))
            current_consumers = len(frappe.get_all("Kafka Consumer", filters={
                "topic": consumer.topic, "consumer_group": consumer.consumer_group, "enabled": 1
            }))

            if target_consumers > current_consumers:
                last_scale_up = consumer.last_scale_up_time or 0
                if current_time - last_scale_up > scale_up_cooldown:
                    scale_consumer(consumer.name, consumer.topic, consumer.consumer_group, target_consumers)
                    frappe.db.set_value("Kafka Consumer", consumer.name, "last_scale_up_time", current_time)
                    frappe.db.set_value("Kafka Consumer", consumer.name, "low_lag_count", 0)
                else:
                    logger.info(f"Consumer {consumer.name} scale-up on cooldown (lag: {lag}, target: {target_consumers})")
            elif target_consumers < current_consumers:
                low_lag_count = (consumer.low_lag_count or 0) + 1
                frappe.db.set_value("Kafka Consumer", consumer.name, "low_lag_count", low_lag_count)
                if low_lag_count >= 3:
                    last_scale_down = consumer.last_scale_down_time or 0
                    if current_time - last_scale_down > scale_down_cooldown:
                        scale_down_consumer(consumer.name, consumer.topic, consumer.consumer_group, target_consumers)
                        frappe.db.set_value("Kafka Consumer", consumer.name, "last_scale_down_time", current_time)
                        frappe.db.set_value("Kafka Consumer", consumer.name, "low_lag_count", 0)
                    else:
                        logger.info(f"Consumer {consumer.name} scale-down on cooldown (lag: {lag}, target: {target_consumers})")
                else:
                    logger.info(f"Consumer {consumer.name} low lag count: {low_lag_count}/3 (lag: {lag})")
            else:
                frappe.db.set_value("Kafka Consumer", consumer.name, "low_lag_count", 0)
                logger.info(f"Consumer {consumer.name} lag stable: {lag}, consumers: {current_consumers}")

        except Exception as e:
            frappe.log_error(f"Failed to monitor consumer {consumer.name}: {str(e)}")


def check_consumer_lag(conf, topic, group_id):
    consumer = None
    try:
        consumer = Consumer(conf)
        metadata = consumer.list_topics(topic=topic, timeout=10)
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        committed = consumer.committed(partitions, timeout=10)
        total_lag = 0
        for partition in committed:
            if partition.offset == -1:
                continue
            topic_partition = TopicPartition(topic, partition.partition)
            _, high = consumer.get_watermark_offsets(topic_partition)
            lag = high - partition.offset
            total_lag += lag if lag > 0 else 0
        return total_lag
    except Exception as e:
        frappe.log_error(f"Failed to check consumer lag for topic {topic}: {str(e)}")
        raise
    finally:
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass


def scale_consumer(consumer_name, topic_name, consumer_group, target_consumers):
    consumer = frappe.get_doc("Kafka Consumer", consumer_name)
    topic = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
    current_consumers = len(frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 1
    }))
    total_consumers = len(frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group
    }))
    max_consumers = consumer.max_consumers or frappe.get_doc("Kafka Settings").max_consumers

    if current_consumers >= target_consumers:
        logger.info(f"Target consumers ({target_consumers}) reached for {consumer_name}")
        return

    # Increase partitions first if needed (guard is inside increase_partitions)
    if target_consumers > topic.num_partitions:
        topic.increase_partitions(target_consumers)
        logger.info(f"Increased partitions for {topic_name} to {target_consumers}")

    # Re-enable existing disabled consumers before creating new ones
    disabled_consumers = frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 0
    }, fields=["name"])
    consumers_needed = target_consumers - current_consumers
    for dc in disabled_consumers[:consumers_needed]:
        frappe.db.set_value("Kafka Consumer", dc.name, "enabled", 1)
        frappe.get_doc("Kafka Consumer", dc.name).restart_consumer_thread()
        logger.info(f"Scaled up: enabled existing consumer {dc.name} for topic {topic_name}")
        consumers_needed -= 1

    # Create new consumer docs if still short
    settings = frappe.get_doc("Kafka Settings")
    while consumers_needed > 0 and total_consumers < max_consumers:
        try:
            new_consumer = frappe.get_doc({
                "doctype": "Kafka Consumer",
                "cluster": consumer.cluster,
                "topic": consumer.topic,
                "consumer_group": consumer.consumer_group,
                "api_endpoint": consumer.api_endpoint,
                "api_username": consumer.api_username,
                "api_key": consumer.api_key,
                "output_topic": consumer.output_topic,
                "failure_topic": consumer.failure_topic,
                "max_lag_threshold": settings.max_lag_threshold,
                "min_lag_threshold": settings.min_lag_threshold,
                "max_consumers": settings.max_consumers,
                "scale_up_cooldown": settings.scale_up_cooldown,
                "scale_down_cooldown": settings.scale_down_cooldown,
                "enabled": 1
            })
            new_consumer.insert()
            logger.info(f"Scaled up: new consumer {new_consumer.name} created for topic {topic_name}")
            consumers_needed -= 1
            total_consumers += 1
        except Exception as e:
            frappe.log_error(f"Failed to scale consumer {consumer_name}: {str(e)}")
            break


def scale_down_consumer(consumer_name, topic_name, consumer_group, target_consumers):
    current_consumers = frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 1
    }, fields=["name"])

    if len(current_consumers) <= target_consumers:
        logger.info(f"Target consumers ({target_consumers}) already reached for {consumer_name}")
        return

    try:
        consumers_to_disable = len(current_consumers) - target_consumers
        for consumer in current_consumers[:consumers_to_disable]:
            # Signal the thread to exit on its next poll cycle
            frappe.db.set_value("Kafka Consumer", consumer.name, "enabled", 0)
            frappe.db.commit()

            # Wait up to 30 s for the thread to drain, commit, and mark itself stopped
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status = frappe.db.get_value("Kafka Consumer", consumer.name, "thread_status")
                if status == "stopped":
                    break
                time.sleep(1)
            else:
                logger.warning(
                    f"Consumer {consumer.name} did not stop within 30 s after scale-down signal"
                )

            logger.info(f"Scaled down: disabled consumer {consumer.name} for topic {topic_name}")
    except Exception as e:
        frappe.log_error(f"Failed to scale down consumer {consumer_name}: {str(e)}")


# ---------------------------------------------------------------------------
# Stats jobs — scheduled every 5 min
# ---------------------------------------------------------------------------

@frappe.whitelist()
def cleanup_orphaned_consumers():
    settings = get_default_settings()
    consumer_groups = frappe.get_all("Kafka Consumer", fields=["topic", "consumer_group"], distinct=True)
    for group in consumer_groups:
        disabled_consumers = frappe.get_all("Kafka Consumer", filters={
            "topic": group.topic,
            "consumer_group": group.consumer_group,
            "enabled": 0
        }, fields=["name"], order_by="creation desc")
        if len(disabled_consumers) > settings.max_consumers:
            consumers_to_delete = disabled_consumers[settings.max_consumers:]
            for consumer in consumers_to_delete:
                try:
                    frappe.delete_doc("Kafka Consumer", consumer.name)
                    logger.info(f"Deleted orphaned consumer {consumer.name} for topic {group.topic}")
                except Exception as e:
                    frappe.log_error(f"Failed to delete orphaned consumer {consumer.name}: {str(e)}")


@frappe.whitelist()
def update_topic_stats():
    """
    Update message count, lag, active consumers, and replication factor for all topics.

    Connections are shared per cluster — one Consumer + one AdminClient opened for
    the entire cluster, then reused for every topic on that cluster.
    This keeps Kafka connection count at 2 × num_clusters regardless of topic count.
    """
    topics = frappe.get_all("Kafka Topic", fields=["name", "topic_name", "cluster"])

    # Group topics by cluster to share connections
    topics_by_cluster = {}
    for topic in topics:
        topics_by_cluster.setdefault(topic.cluster, []).append(topic)

    # Pre-fetch active consumer counts and group names in two bulk queries
    # to avoid per-topic DB hits inside the loop.
    all_active = frappe.get_all(
        "Kafka Consumer", filters={"enabled": 1},
        fields=["topic"], order_by=None
    )
    active_count_by_topic = {}
    for row in all_active:
        active_count_by_topic[row.topic] = active_count_by_topic.get(row.topic, 0) + 1

    all_groups = frappe.get_all(
        "Kafka Consumer",
        fields=["topic", "consumer_group"],
        distinct=True
    )
    groups_by_topic = {}
    for row in all_groups:
        groups_by_topic.setdefault(row.topic, []).append(row.consumer_group)

    for cluster_name, cluster_topics in topics_by_cluster.items():
        cluster = frappe.get_doc("Kafka Cluster", cluster_name)
        base_conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            'group.id': f"kafka_mediator_stats_{cluster_name}"
        }
        if cluster.sasl_mechanism:
            base_conf['sasl.mechanisms'] = cluster.sasl_mechanism
        if cluster.sasl_username:
            base_conf['sasl.username'] = cluster.sasl_username
        sasl_password = cluster.get_password("sasl_password")
        if sasl_password:
            base_conf['sasl.password'] = sasl_password

        # One Consumer + AdminClient for all topics on this cluster
        consumer = None
        admin_client = None
        try:
            consumer = Consumer(base_conf)
            admin_client = AdminClient(base_conf)

            # Fetch all topic metadata in one call instead of per-topic
            cluster_metadata = consumer.list_topics(timeout=10)

            # Batch-describe all non-internal topics for replication factors
            topic_names_to_describe = [
                t.topic_name for t in cluster_topics
                if t.topic_name in cluster_metadata.topics
            ]
            describe_futures = {}
            if topic_names_to_describe:
                describe_futures = admin_client.describe_topics(
                    TopicCollection(topic_names_to_describe), request_timeout=30
                )

            for topic in cluster_topics:
                try:
                    if topic.topic_name not in cluster_metadata.topics:
                        logger.warning(f"Topic {topic.topic_name} not found in cluster {cluster_name}")
                        continue

                    partitions = [
                        TopicPartition(topic.topic_name, p)
                        for p in cluster_metadata.topics[topic.topic_name].partitions
                    ]

                    if not partitions:
                        frappe.db.set_value("Kafka Topic", topic.name, {
                            "message_count": 0, "lag": 0,
                            "active_consumers": 0, "replication_factor": 1,
                            "last_updated": frappe.utils.now()
                        })
                        frappe.db.commit()
                        continue

                    # Message count from watermarks
                    message_count = 0
                    for partition in partitions:
                        try:
                            low, high = consumer.get_watermark_offsets(partition)
                            message_count += max(0, high - low)
                        except Exception as e:
                            logger.error(
                                f"Watermark failed for {topic.topic_name}[{partition.partition}]: {e}"
                            )

                    # Lag: one temporary Consumer per consumer group
                    lag = 0
                    for group_id in groups_by_topic.get(topic.topic_name, []):
                        try:
                            group_conf = dict(base_conf, **{'group.id': group_id})
                            lag += check_consumer_lag(group_conf, topic.topic_name, group_id)
                        except Exception as e:
                            logger.error(
                                f"Lag check failed for group {group_id} "
                                f"on {topic.topic_name}: {e}"
                            )

                    # Replication factor from batched describe result
                    replication_factor = 1
                    if topic.topic_name in describe_futures:
                        try:
                            topic_desc = describe_futures[topic.topic_name].result()
                            replication_factor = (
                                len(topic_desc.partitions[0].replicas)
                                if topic_desc.partitions else 1
                            )
                        except Exception as e:
                            logger.error(f"Replication factor failed for {topic.topic_name}: {e}")

                    active_consumers = active_count_by_topic.get(topic.topic_name, 0)

                    frappe.db.set_value("Kafka Topic", topic.name, {
                        "message_count": message_count,
                        "lag": lag,
                        "active_consumers": active_consumers,
                        "replication_factor": replication_factor,
                        "last_updated": frappe.utils.now()
                    })
                    frappe.db.commit()
                    logger.info(
                        f"Stats updated for {topic.topic_name}: "
                        f"messages={message_count}, lag={lag}, "
                        f"consumers={active_consumers}, rf={replication_factor}"
                    )
                except Exception as e:
                    frappe.log_error(f"Failed to update stats for topic {topic.topic_name}: {str(e)}")

        except Exception as e:
            frappe.log_error(f"Failed to connect to cluster {cluster_name} for stats: {str(e)}")
        finally:
            try:
                if consumer:
                    consumer.close()
                if admin_client:
                    admin_client.close()
            except Exception:
                pass


@frappe.whitelist()
def update_cluster_stats():
    """Update cluster-wide statistics."""
    clusters = frappe.get_all("Kafka Cluster", fields=["name"])
    for cluster_ref in clusters:
        cluster = frappe.get_doc("Kafka Cluster", cluster_ref.name)
        conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
        }
        if cluster.sasl_mechanism:
            conf['sasl.mechanisms'] = cluster.sasl_mechanism
        if cluster.sasl_username:
            conf['sasl.username'] = cluster.sasl_username
        sasl_password = cluster.get_password("sasl_password")
        if sasl_password:
            conf['sasl.password'] = sasl_password
        try:
            admin_client = AdminClient(conf)
            topics = admin_client.list_topics(timeout=10)
            total_messages = 0
            total_lag = 0
            total_consumers = 0
            for topic_name in topics.topics:
                if topic_name.startswith('__'):
                    continue
                if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
                    continue
                topic_doc = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
                total_messages += topic_doc.message_count or 0
                total_lag += topic_doc.lag or 0
                total_consumers += topic_doc.active_consumers or 0

            stats_values = {
                "cluster": cluster.name,
                "total_topics": len([t for t in topics.topics if not t.startswith('__')]),
                "total_messages": total_messages,
                "total_lag": total_lag,
                "total_consumers": total_consumers,
                "broker_status": "Connected",
                "last_updated": frappe.utils.now()
            }
            if frappe.db.exists("Kafka Cluster Stats", {"cluster": cluster.name}):
                frappe.db.set_value("Kafka Cluster Stats", {"cluster": cluster.name},
                                    stats_values, update_modified=False)
            else:
                frappe.get_doc({"doctype": "Kafka Cluster Stats", **stats_values}).db_insert()
            frappe.db.commit()
            logger.info(
                f"Updated stats for cluster {cluster.name}: "
                f"topics={stats_values['total_topics']}, messages={total_messages}, "
                f"lag={total_lag}, consumers={total_consumers}"
            )
        except Exception as e:
            frappe.log_error(f"Failed to update stats for cluster {cluster.name}: {str(e)}")
            failed_values = {"broker_status": "Failed", "last_updated": frappe.utils.now()}
            if frappe.db.exists("Kafka Cluster Stats", {"cluster": cluster.name}):
                frappe.db.set_value("Kafka Cluster Stats", {"cluster": cluster.name},
                                    failed_values, update_modified=False)
            else:
                frappe.get_doc({
                    "doctype": "Kafka Cluster Stats", "cluster": cluster.name, **failed_values
                }).db_insert()
            frappe.db.commit()
        finally:
            try:
                admin_client.close() if 'admin_client' in locals() else None
            except Exception:
                pass


# ---------------------------------------------------------------------------
# REST API for external applications
# ---------------------------------------------------------------------------

@frappe.whitelist()
def create_consumer(topic, consumer_group, api_endpoint, api_username=None, api_password=None, api_key=None, output_topic=None, failure_topic=None):
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    settings = get_default_settings()
    username = frappe.session.user.replace("@", "_").replace(".", "_")
    topic_name = f"{username}_{topic}"
    output_topic_name = f"{username}_{output_topic}" if output_topic else None
    failure_topic_name = f"{username}_{failure_topic}" if failure_topic else None

    if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
        frappe.throw(f"Topic {topic_name} does not exist")
    if output_topic and not frappe.db.exists("Kafka Topic", {"topic_name": output_topic_name}):
        frappe.throw(f"Output topic {output_topic_name} does not exist")
    if failure_topic and not frappe.db.exists("Kafka Topic", {"topic_name": failure_topic_name}):
        frappe.throw(f"Failure topic {failure_topic_name} does not exist")

    # Default consumer_group to a deterministic name if not provided
    if not consumer_group:
        consumer_group = f"{topic_name}-consumers"

    try:
        existing_consumers = len(frappe.get_all("Kafka Consumer", filters={
            "topic": topic_name, "consumer_group": consumer_group
        }))
        consumers_to_create = settings.max_consumers - existing_consumers if existing_consumers < settings.max_consumers else 0
        consumer_ids = []

        consumer = frappe.get_doc({
            "doctype": "Kafka Consumer",
            "cluster": settings.default_cluster,
            "topic": topic_name,
            "consumer_group": consumer_group,
            "api_endpoint": api_endpoint,
            "api_username": api_username,
            "api_password": api_password,
            "api_key": api_key,
            "output_topic": output_topic_name,
            "failure_topic": failure_topic_name,
            "max_lag_threshold": settings.max_lag_threshold,
            "min_lag_threshold": settings.min_lag_threshold,
            "max_consumers": settings.max_consumers,
            "scale_up_cooldown": settings.scale_up_cooldown,
            "scale_down_cooldown": settings.scale_down_cooldown,
            "enabled": 1
        })
        consumer.insert()
        consumer_ids.append(consumer.name)

        for _ in range(consumers_to_create - 1):
            consumer = frappe.get_doc({
                "doctype": "Kafka Consumer",
                "cluster": settings.default_cluster,
                "topic": topic_name,
                "consumer_group": consumer_group,
                "api_endpoint": api_endpoint,
                "api_username": api_username,
                "api_password": api_password,
                "api_key": api_key,
                "output_topic": output_topic_name,
                "failure_topic": failure_topic_name,
                "max_lag_threshold": settings.max_lag_threshold,
                "min_lag_threshold": settings.min_lag_threshold,
                "max_consumers": settings.max_consumers,
                "scale_up_cooldown": settings.scale_up_cooldown,
                "scale_down_cooldown": settings.scale_down_cooldown,
                "enabled": 0
            })
            consumer.insert()
            consumer_ids.append(consumer.name)

        return {"consumer_ids": consumer_ids, "status": "success"}
    except Exception as e:
        frappe.log_error(f"Failed to create consumer: {str(e)}")
        return {"status": "error", "message": str(e)}


@frappe.whitelist()
def create_topic(topic_name, num_partitions=None, replication_factor=None):
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    settings = get_default_settings()
    username = frappe.session.user.replace("@", "_").replace(".", "_")
    full_topic_name = f"{username}_{topic_name}"

    if frappe.db.exists("Kafka Topic", {"topic_name": full_topic_name}):
        frappe.throw(f"Topic {full_topic_name} already exists")

    try:
        topic = frappe.get_doc({
            "doctype": "Kafka Topic",
            "cluster": settings.default_cluster,
            "topic_name": full_topic_name,
            "num_partitions": num_partitions or settings.default_partitions,
            "replication_factor": replication_factor or settings.default_replication_factor
        })
        topic.insert()

        producer = frappe.get_doc({
            "doctype": "Kafka Producer",
            "cluster": settings.default_cluster,
            "topic": full_topic_name
        })
        producer.insert()

        return {"topic_name": full_topic_name, "producer_id": producer.name, "status": "success"}
    except Exception as e:
        frappe.log_error(f"Failed to create topic {full_topic_name}: {str(e)}")
        return {"status": "error", "message": str(e)}


@frappe.whitelist()
def publish_message(topic_name, message):
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    username = frappe.session.user.replace("@", "_").replace(".", "_")
    full_topic_name = f"{username}_{topic_name}"

    try:
        producers = frappe.get_all("Kafka Producer", filters={"topic": full_topic_name})
        if producers and producers[0]:
            producer = frappe.get_doc("Kafka Producer", producers[0].get('name'))
            if not isinstance(message, str):
                message = json.dumps(message)
            producer.publish_message(json.dumps(message))
            return {"status": "success"}
        else:
            frappe.throw("No associated producer found for the topic")
    except Exception as e:
        frappe.log_error(f"Failed to publish message: {str(e)}")
        return {"status": "error", "message": str(e)}


@frappe.whitelist()
def get_topic_stats(topic_name):
    """Return statistics for a specific topic."""
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    username = frappe.session.user.replace("@", "_").replace(".", "_")
    full_topic_name = topic_name if username == "Administrator" else f"{username}_{topic_name}"
    topic = frappe.get_doc("Kafka Topic", {"topic_name": full_topic_name})
    return {
        "topic_name": topic.topic_name,
        "message_count": topic.message_count or 0,
        "lag": topic.lag or 0,
        "active_consumers": topic.active_consumers or 0,
        "partition_count": topic.num_partitions,
        "replication_factor": topic.replication_factor,
        "last_updated": topic.last_updated
    }


@frappe.whitelist()
def get_cluster_stats(cluster_name):
    """Return statistics for a specific cluster."""
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw("Authentication required")

    cluster_stats = frappe.get_doc("Kafka Cluster Stats", {"cluster": cluster_name})
    topics = frappe.get_all("Kafka Topic", filters={"cluster": cluster_name}, fields=[
        "topic_name", "message_count", "lag", "active_consumers", "num_partitions",
        "replication_factor", "last_updated"
    ])
    return {
        "cluster_name": cluster_name,
        "total_topics": cluster_stats.total_topics or 0,
        "total_messages": cluster_stats.total_messages or 0,
        "total_lag": cluster_stats.total_lag or 0,
        "total_consumers": cluster_stats.total_consumers or 0,
        "broker_status": cluster_stats.broker_status,
        "last_updated": cluster_stats.last_updated,
        "topics": topics
    }


@frappe.whitelist()
def format_response(success=False, message=None, status_code=200, data=None, pagination=None):
    frappe.local.response.http_status_code = status_code
    response_data = {
        "status": "success" if status_code in (200, 201, 202) else "error",
        "message": message,
        "status_code": status_code,
        "data": data,
    }
    if success and data is not None:
        response_data["data"] = data
    if not success and message is not None:
        response_data["message"] = message
    if success and message is not None:
        response_data["message"] = message
    response_data["pagination"] = pagination or {}
    frappe.local.response.update(response_data)


def validate_payload(*required_fields):
    def decorator(fn):
        @wraps(fn)
        def wrapper(**kwargs):
            missing = [f for f in required_fields if not kwargs.get(f)]
            if missing:
                return format_response(status_code=400, message="Missing required fields.")
            return fn(**kwargs)
        return wrapper
    return decorator
