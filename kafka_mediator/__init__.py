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

HEARTBEAT_STALE_SECONDS = 120


def get_default_settings():
    return frappe.get_doc("Kafka Settings")


def _build_cluster_conf(cluster, group_id=None):
    """Build a confluent-kafka config dict from a Kafka Cluster doc."""
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
    if group_id:
        conf['group.id'] = group_id
    return conf


# ---------------------------------------------------------------------------
# Dead consumer watchdog — called every minute by scheduler
# ---------------------------------------------------------------------------

def restart_dead_consumers():
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

    # One scaling decision per (topic, consumer_group) pair — prevents N-times scale-up
    seen_groups = set()

    for consumer in consumers:
        group_key = (consumer.topic, consumer.consumer_group)
        if group_key in seen_groups:
            continue
        seen_groups.add(group_key)

        cluster = frappe.get_doc("Kafka Cluster", consumer.cluster)
        conf = _build_cluster_conf(cluster, group_id=consumer.consumer_group)

        try:
            lag = check_consumer_lag(conf, consumer.topic, consumer.consumer_group)

            current_time = time.time()
            max_lag_threshold = consumer.max_lag_threshold or settings.max_lag_threshold
            min_lag_threshold = consumer.min_lag_threshold or settings.min_lag_threshold
            max_consumers = consumer.max_consumers or settings.max_consumers
            scale_up_cooldown = consumer.scale_up_cooldown or settings.scale_up_cooldown
            scale_down_cooldown = consumer.scale_down_cooldown or settings.scale_down_cooldown
            messages_per_consumer = settings.messages_per_consumer

            group_members = frappe.get_all("Kafka Consumer", filters={
                "topic": consumer.topic, "consumer_group": consumer.consumer_group
            }, fields=["name"])

            # Write lag to all group members so every consumer row shows current lag
            for member in group_members:
                frappe.db.set_value("Kafka Consumer", member.name, "current_lag", lag)
            frappe.db.commit()

            current_consumers = len(frappe.get_all("Kafka Consumer", filters={
                "topic": consumer.topic, "consumer_group": consumer.consumer_group, "enabled": 1
            }))

            if lag >= max_lag_threshold:
                target_consumers = max(1, min(max_consumers, (lag + messages_per_consumer - 1) // messages_per_consumer))
                if target_consumers > current_consumers:
                    last_scale_up = consumer.last_scale_up_time or 0
                    if current_time - last_scale_up > scale_up_cooldown:
                        scale_consumer(consumer.name, consumer.topic, consumer.consumer_group, target_consumers)
                        frappe.db.set_value("Kafka Consumer", consumer.name, "last_scale_up_time", current_time)
                        for member in group_members:
                            frappe.db.set_value("Kafka Consumer", member.name, "low_lag_count", 0)
                    else:
                        logger.info(f"Consumer {consumer.name} scale-up on cooldown (lag: {lag}, target: {target_consumers})")

            elif lag <= min_lag_threshold:
                target_consumers = max(1, (lag + messages_per_consumer - 1) // messages_per_consumer)
                if target_consumers < current_consumers:
                    low_lag_count = (consumer.low_lag_count or 0) + 1
                    for member in group_members:
                        frappe.db.set_value("Kafka Consumer", member.name, "low_lag_count", low_lag_count)
                    if low_lag_count >= 3:
                        last_scale_down = consumer.last_scale_down_time or 0
                        if current_time - last_scale_down > scale_down_cooldown:
                            scale_down_consumer(consumer.name, consumer.topic, consumer.consumer_group, target_consumers)
                            frappe.db.set_value("Kafka Consumer", consumer.name, "last_scale_down_time", current_time)
                            for member in group_members:
                                frappe.db.set_value("Kafka Consumer", member.name, "low_lag_count", 0)
                        else:
                            logger.info(f"Consumer {consumer.name} scale-down on cooldown (lag: {lag}, target: {target_consumers})")
                    else:
                        logger.info(f"Consumer {consumer.name} low lag count: {low_lag_count}/3 (lag: {lag})")
                else:
                    for member in group_members:
                        frappe.db.set_value("Kafka Consumer", member.name, "low_lag_count", 0)

            else:
                for member in group_members:
                    frappe.db.set_value("Kafka Consumer", member.name, "low_lag_count", 0)
                logger.info(f"Consumer {consumer.name} lag in hold zone: {lag} (min: {min_lag_threshold}, max: {max_lag_threshold}), consumers: {current_consumers}")

            frappe.db.commit()

        except Exception as e:
            frappe.log_error(f"Failed to monitor consumer {consumer.name}: {str(e)}")


def check_consumer_lag(conf, topic, group_id, consumer=None):
    """
    Measure total consumer lag for a group on a topic.
    Pass an existing Consumer instance to reuse its connection; otherwise a
    temporary one is created and closed after the call.
    """
    owns_consumer = consumer is None
    try:
        if owns_consumer:
            consumer = Consumer(conf)
        metadata = consumer.list_topics(topic=topic, timeout=10)
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        committed = consumer.committed(partitions, timeout=10)
        total_lag = 0
        for partition in committed:
            topic_partition = TopicPartition(topic, partition.partition)
            _, high = consumer.get_watermark_offsets(topic_partition)
            committed_offset = partition.offset if partition.offset != -1 else 0
            lag = high - committed_offset
            total_lag += lag if lag > 0 else 0
        return total_lag
    except Exception as e:
        frappe.log_error(f"Failed to check consumer lag for topic {topic}: {str(e)}")
        raise
    finally:
        if owns_consumer and consumer:
            try:
                consumer.close()
            except Exception:
                pass


def scale_consumer(consumer_name, topic_name, consumer_group, target_consumers):
    source = frappe.get_doc("Kafka Consumer", consumer_name)
    topic = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
    current_consumers = len(frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 1
    }))
    total_consumers = len(frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group
    }))
    max_consumers = source.max_consumers or frappe.get_doc("Kafka Settings").max_consumers

    if current_consumers >= target_consumers:
        logger.info(f"Target consumers ({target_consumers}) reached for {consumer_name}")
        return

    if target_consumers > topic.num_partitions:
        topic.increase_partitions(target_consumers)
        logger.info(f"Increased partitions for {topic_name} to {target_consumers}")

    # Re-enable disabled consumers before creating new ones
    disabled_consumers = frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 0
    }, fields=["name"])
    consumers_needed = target_consumers - current_consumers
    for dc in disabled_consumers[:consumers_needed]:
        frappe.db.set_value("Kafka Consumer", dc.name, "enabled", 1)
        frappe.get_doc("Kafka Consumer", dc.name).restart_consumer_thread()
        logger.info(f"Scaled up: enabled existing consumer {dc.name} for topic {topic_name}")
        consumers_needed -= 1

    if consumers_needed <= 0:
        return

    # Decrypt credentials from source consumer once
    api_password = frappe.utils.password.get_decrypted_password(
        "Kafka Consumer", consumer_name, "api_password", raise_exception=False
    )
    api_consumer_key = frappe.utils.password.get_decrypted_password(
        "Kafka Consumer", consumer_name, "api_consumer_key", raise_exception=False
    )
    api_consumer_secret = frappe.utils.password.get_decrypted_password(
        "Kafka Consumer", consumer_name, "api_consumer_secret", raise_exception=False
    )

    settings = frappe.get_doc("Kafka Settings")
    while consumers_needed > 0 and total_consumers < max_consumers:
        try:
            new_consumer = frappe.get_doc({
                "doctype": "Kafka Consumer",
                "cluster": source.cluster,
                "topic": source.topic,
                "consumer_group": source.consumer_group,
                "api_endpoint": source.api_endpoint,
                "api_username": source.api_username,
                "api_password": api_password,
                "api_key": source.api_key,
                "api_consumer_key": api_consumer_key,
                "api_consumer_secret": api_consumer_secret,
                "output_topic": source.output_topic,
                "failure_topic": source.failure_topic,
                "max_retries": source.max_retries,
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
            frappe.db.set_value("Kafka Consumer", consumer.name, "enabled", 0)
            frappe.db.commit()

            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status = frappe.db.get_value("Kafka Consumer", consumer.name, "thread_status")
                if status == "stopped":
                    break
                time.sleep(1)
            else:
                logger.warning(f"Consumer {consumer.name} did not stop within 30 s after scale-down signal")

            logger.info(f"Scaled down: disabled consumer {consumer.name} for topic {topic_name}")
    except Exception as e:
        frappe.log_error(f"Failed to scale down consumer {consumer_name}: {str(e)}")


# ---------------------------------------------------------------------------
# Live stats — called on demand by API endpoints
# ---------------------------------------------------------------------------

def fetch_live_topic_stats(topic_name):
    """Fetch live message_count, lag, partition_count and replication_factor from Kafka."""
    topic_doc = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
    cluster = frappe.get_doc("Kafka Cluster", topic_doc.cluster)
    conf = _build_cluster_conf(cluster, group_id=f"kafka_mediator_live_stats_{cluster.name}")

    groups = frappe.get_all(
        "Kafka Consumer", filters={"topic": topic_name},
        fields=["consumer_group"], distinct=True
    )

    consumer = None
    admin_client = None
    try:
        consumer = Consumer(conf)
        admin_client = AdminClient(conf)

        cluster_metadata = consumer.list_topics(topic=topic_name, timeout=10)
        if topic_name not in cluster_metadata.topics:
            raise ValueError(f"Topic '{topic_name}' not found in cluster")

        partitions = [
            TopicPartition(topic_name, p)
            for p in cluster_metadata.topics[topic_name].partitions
        ]

        message_count = 0
        for partition in partitions:
            try:
                low, high = consumer.get_watermark_offsets(partition)
                message_count += max(0, high - low)
            except Exception as e:
                logger.error(f"Watermark failed for {topic_name}[{partition.partition}]: {e}")

        total_lag = 0
        for g in groups:
            try:
                group_conf = _build_cluster_conf(cluster, group_id=g.consumer_group)
                total_lag += check_consumer_lag(group_conf, topic_name, g.consumer_group)
            except Exception as e:
                logger.error(f"Lag check failed for group {g.consumer_group} on {topic_name}: {e}")

        replication_factor = topic_doc.replication_factor or 1
        try:
            futures = admin_client.describe_topics(TopicCollection([topic_name]), request_timeout=10)
            topic_desc = futures[topic_name].result()
            if topic_desc.partitions:
                replication_factor = len(topic_desc.partitions[0].replicas)
        except Exception as e:
            logger.error(f"Replication factor fetch failed for {topic_name}: {e}")

        active_consumers = frappe.db.count("Kafka Consumer", {"topic": topic_name, "enabled": 1})

        return {
            "message_count": message_count,
            "total_lag": total_lag,
            "partition_count": len(partitions),
            "replication_factor": replication_factor,
            "active_consumers": active_consumers,
        }
    finally:
        try:
            if consumer:
                consumer.close()
            if admin_client:
                admin_client.close()
        except Exception:
            pass


def fetch_live_cluster_stats(cluster_name):
    """
    Fetch live stats for every topic on a cluster in a single connection pass.
    Returns a dict keyed by topic_name.
    """
    cluster = frappe.get_doc("Kafka Cluster", cluster_name)
    conf = _build_cluster_conf(cluster, group_id=f"kafka_mediator_live_stats_{cluster_name}")

    topics = frappe.get_all(
        "Kafka Topic", filters={"cluster": cluster_name},
        fields=["name", "topic_name", "replication_factor"]
    )

    all_groups = frappe.get_all(
        "Kafka Consumer", filters={"cluster": cluster_name},
        fields=["topic", "consumer_group"], distinct=True
    )
    groups_by_topic = {}
    for row in all_groups:
        groups_by_topic.setdefault(row.topic, []).append(row.consumer_group)

    active_counts = {}
    for row in frappe.get_all("Kafka Consumer", filters={"enabled": 1, "cluster": cluster_name}, fields=["topic"]):
        active_counts[row.topic] = active_counts.get(row.topic, 0) + 1

    consumer = None
    admin_client = None
    results = {}
    try:
        consumer = Consumer(conf)
        admin_client = AdminClient(conf)
        cluster_metadata = consumer.list_topics(timeout=10)

        topic_names_to_describe = [
            t.topic_name for t in topics if t.topic_name in cluster_metadata.topics
        ]
        describe_futures = {}
        if topic_names_to_describe:
            describe_futures = admin_client.describe_topics(
                TopicCollection(topic_names_to_describe), request_timeout=30
            )

        for topic in topics:
            if topic.topic_name not in cluster_metadata.topics:
                continue
            try:
                partitions = [
                    TopicPartition(topic.topic_name, p)
                    for p in cluster_metadata.topics[topic.topic_name].partitions
                ]

                message_count = 0
                for partition in partitions:
                    try:
                        low, high = consumer.get_watermark_offsets(partition)
                        message_count += max(0, high - low)
                    except Exception as e:
                        logger.error(f"Watermark failed for {topic.topic_name}[{partition.partition}]: {e}")

                total_lag = 0
                for group_id in groups_by_topic.get(topic.topic_name, []):
                    try:
                        group_conf = _build_cluster_conf(cluster, group_id=group_id)
                        total_lag += check_consumer_lag(group_conf, topic.topic_name, group_id)
                    except Exception as e:
                        logger.error(f"Lag check failed for group {group_id} on {topic.topic_name}: {e}")

                replication_factor = topic.replication_factor or 1
                if topic.topic_name in describe_futures:
                    try:
                        topic_desc = describe_futures[topic.topic_name].result()
                        if topic_desc.partitions:
                            replication_factor = len(topic_desc.partitions[0].replicas)
                    except Exception as e:
                        logger.error(f"Replication factor failed for {topic.topic_name}: {e}")

                results[topic.topic_name] = {
                    "message_count": message_count,
                    "total_lag": total_lag,
                    "partition_count": len(partitions),
                    "replication_factor": replication_factor,
                    "active_consumers": active_counts.get(topic.topic_name, 0),
                }
            except Exception as e:
                logger.error(f"Live stats failed for topic {topic.topic_name}: {e}")

    finally:
        try:
            if consumer:
                consumer.close()
            if admin_client:
                admin_client.close()
        except Exception:
            pass

    return results


# ---------------------------------------------------------------------------
# Stats jobs — update_topic_stats must run before update_cluster_stats
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
            for consumer in disabled_consumers[settings.max_consumers:]:
                try:
                    frappe.delete_doc("Kafka Consumer", consumer.name)
                    logger.info(f"Deleted orphaned consumer {consumer.name} for topic {group.topic}")
                except Exception as e:
                    frappe.log_error(f"Failed to delete orphaned consumer {consumer.name}: {str(e)}")


@frappe.whitelist()
def update_topic_stats():
    """
    Update message count, lag, active consumers, and replication factor for all topics.
    One Consumer + AdminClient per cluster — reused for every topic on that cluster.
    """
    topics = frappe.get_all("Kafka Topic", fields=["name", "topic_name", "cluster"])

    topics_by_cluster = {}
    for topic in topics:
        topics_by_cluster.setdefault(topic.cluster, []).append(topic)

    all_active = frappe.get_all("Kafka Consumer", filters={"enabled": 1}, fields=["topic"], order_by=None)
    active_count_by_topic = {}
    for row in all_active:
        active_count_by_topic[row.topic] = active_count_by_topic.get(row.topic, 0) + 1

    all_groups = frappe.get_all("Kafka Consumer", fields=["topic", "consumer_group"], distinct=True)
    groups_by_topic = {}
    for row in all_groups:
        groups_by_topic.setdefault(row.topic, []).append(row.consumer_group)

    for cluster_name, cluster_topics in topics_by_cluster.items():
        cluster = frappe.get_doc("Kafka Cluster", cluster_name)
        base_conf = _build_cluster_conf(cluster, group_id=f"kafka_mediator_stats_{cluster_name}")

        consumer = None
        admin_client = None
        try:
            consumer = Consumer(base_conf)
            admin_client = AdminClient(base_conf)
            cluster_metadata = consumer.list_topics(timeout=10)

            topic_names_to_describe = [
                t.topic_name for t in cluster_topics if t.topic_name in cluster_metadata.topics
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

                    message_count = 0
                    for partition in partitions:
                        try:
                            low, high = consumer.get_watermark_offsets(partition)
                            message_count += max(0, high - low)
                        except Exception as e:
                            logger.error(f"Watermark failed for {topic.topic_name}[{partition.partition}]: {e}")

                    lag = 0
                    for group_id in groups_by_topic.get(topic.topic_name, []):
                        try:
                            group_conf = _build_cluster_conf(cluster, group_id=group_id)
                            lag += check_consumer_lag(group_conf, topic.topic_name, group_id)
                        except Exception as e:
                            logger.error(f"Lag check failed for group {group_id} on {topic.topic_name}: {e}")

                    replication_factor = 1
                    if topic.topic_name in describe_futures:
                        try:
                            topic_desc = describe_futures[topic.topic_name].result()
                            replication_factor = (
                                len(topic_desc.partitions[0].replicas) if topic_desc.partitions else 1
                            )
                        except Exception as e:
                            logger.error(f"Replication factor failed for {topic.topic_name}: {e}")

                    frappe.db.set_value("Kafka Topic", topic.name, {
                        "message_count": message_count,
                        "lag": lag,
                        "active_consumers": active_count_by_topic.get(topic.topic_name, 0),
                        "replication_factor": replication_factor,
                        "last_updated": frappe.utils.now()
                    })
                    frappe.db.commit()
                    logger.info(
                        f"Stats updated for {topic.topic_name}: "
                        f"messages={message_count}, lag={lag}, rf={replication_factor}"
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
    """
    Aggregate per-topic stats (written by update_topic_stats) into cluster-level totals.
    Must run after update_topic_stats on the same cron tick.
    """
    clusters = frappe.get_all("Kafka Cluster", fields=["name"])
    for cluster_ref in clusters:
        cluster = frappe.get_doc("Kafka Cluster", cluster_ref.name)
        conf = _build_cluster_conf(cluster)
        try:
            admin_client = AdminClient(conf)
            topics = admin_client.list_topics(timeout=10)
            non_internal = [t for t in topics.topics if not t.startswith('__')]
            non_internal_count = len(non_internal)

            total_messages = 0
            total_lag = 0
            total_consumers = 0
            for topic_name in non_internal:
                if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
                    continue
                topic_doc = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
                total_messages += topic_doc.message_count or 0
                total_lag += topic_doc.lag or 0
                total_consumers += topic_doc.active_consumers or 0

            stats_values = {
                "cluster": cluster.name,
                "total_topics": non_internal_count,
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
            frappe.db.set_value("Kafka Cluster", cluster.name, "topic_count", non_internal_count)
            frappe.db.commit()
            logger.info(
                f"Updated stats for cluster {cluster.name}: "
                f"topics={non_internal_count}, messages={total_messages}, "
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
# Shared helpers
# ---------------------------------------------------------------------------

@frappe.whitelist()
def format_response(success=False, message=None, status_code=200, data=None, pagination=None):
    frappe.local.response.http_status_code = status_code
    response_data = {
        "status": "success" if status_code in (200, 201, 202) else "error",
        "message": message,
        "status_code": status_code,
        "data": data,
        "pagination": pagination or {},
    }
    frappe.local.response.update(response_data)


def validate_payload(*required_fields):
    def decorator(fn):
        @wraps(fn)
        def wrapper(**kwargs):
            missing = [f for f in required_fields if not kwargs.get(f)]
            if missing:
                return format_response(
                    status_code=400,
                    message=f"Missing required fields: {', '.join(missing)}"
                )
            return fn(**kwargs)
        return wrapper
    return decorator
