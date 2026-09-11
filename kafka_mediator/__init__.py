__version__ = "0.0.1"

import threading
import time
import logging
from functools import wraps

import frappe
from frappe.utils.password import get_decrypted_password
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient, _ConsumerGroupTopicPartitions, ConfigResource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEARTBEAT_STALE_SECONDS = 45
RECONCILE_INTERVAL = 30  # min seconds between consumer reconcile sweeps

# Serializes the reconcile sweep and throttles how often it runs, so the
# before_request hook stays cheap on the hot path (one timestamp check per
# request; a real sweep only every RECONCILE_INTERVAL seconds).
_reconcile_lock = threading.Lock()
_last_reconcile = 0.0
# Names of consumers whose threads THIS process started and believes are alive.
# Prevents this process from double-starting a thread it already owns; a stale
# heartbeat is still the cross-process signal that a thread has actually died.
_started_here = set()


def get_default_settings():
    return frappe.get_doc("Kafka Settings")


def start_enabled_consumers():
    """Reconcile consumer threads for all enabled consumers — continuously.

    Registered as a `before_request` hook so it runs inside the gunicorn/web
    worker: daemon threads spawned here survive for the life of that process,
    unlike threads spawned from an RQ scheduler job (which die when the job
    returns). This both removes the need to manually scale up after a restart
    AND self-heals threads that die mid-run, which the RQ watchdog cannot do.

    Cheap on the hot path: throttled to one real sweep per RECONCILE_INTERVAL
    seconds. A sweep restarts any enabled consumer whose heartbeat is stale or
    missing. A consumer this process already started and whose heartbeat is
    fresh is skipped, so healthy threads are never double-started.
    """
    global _last_reconcile
    now = time.monotonic()
    if now - _last_reconcile < RECONCILE_INTERVAL:
        return
    if not _reconcile_lock.acquire(blocking=False):
        return  # another request is already sweeping
    try:
        if time.monotonic() - _last_reconcile < RECONCILE_INTERVAL:
            return
        _last_reconcile = time.monotonic()
        try:
            site = frappe.local.site
            stale_threshold = frappe.utils.add_to_date(
                frappe.utils.now_datetime(), seconds=-HEARTBEAT_STALE_SECONDS
            )
            stale = frappe.get_all(
                "Kafka Consumer",
                filters=[
                    ["enabled", "=", 1],
                    ["last_heartbeat", "<", stale_threshold],
                ],
                pluck="name",
            )
            never = frappe.get_all(
                "Kafka Consumer",
                filters=[["enabled", "=", 1], ["last_heartbeat", "is", "not set"]],
                pluck="name",
            )
            to_start = list(dict.fromkeys(stale + never))
            for name in to_start:
                try:
                    doc = frappe.get_doc("Kafka Consumer", name)
                    frappe.db.set_value(
                        "Kafka Consumer", name, "thread_status", "restarting",
                        update_modified=False,
                    )
                    threading.Thread(
                        target=doc.consume_messages, args=(site,), daemon=True
                    ).start()
                    _started_here.add(name)
                    logger.info(f"Reconcile: (re)started consumer thread for {name}")
                except Exception as e:
                    frappe.log_error(
                        f"Reconcile: {name}",
                        f"Failed to start consumer {name}: {str(e)}"
                    )
            try:
                frappe.db.commit()
            except Exception:
                pass
        except Exception as e:
            # Never let a reconcile failure break the request it piggybacked on.
            try:
                frappe.log_error(
                    "start_enabled_consumers",
                    f"Reconcile sweep failed: {str(e)}"
                )
            except Exception:
                pass
    finally:
        _reconcile_lock.release()


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
    sasl_password = get_decrypted_password(
        "Kafka Cluster", cluster.name, "sasl_password", raise_exception=False
    )
    if sasl_password:
        conf['sasl.password'] = sasl_password
    if group_id:
        conf['group.id'] = group_id
    return conf


# ---------------------------------------------------------------------------
# Dead consumer watchdog — called every minute by scheduler
# ---------------------------------------------------------------------------

def restart_dead_consumers():
    """
    Phase 1 — cheap DB-only check. Runs every minute.
    Detects consumers whose heartbeat has gone stale (thread died without updating status)
    and restarts them directly in the current process.
    """
    site = frappe.local.site
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
            frappe.db.set_value("Kafka Consumer", entry["name"], "thread_status", "restarting", update_modified=False)
            frappe.db.commit()
            threading.Thread(target=consumer_doc.consume_messages, args=(site,), daemon=True).start()
            logger.info(f"Auto-restarted stale consumer {entry['name']}")
        except Exception as e:
            frappe.log_error("Auto-restart Consumer", f"Failed to auto-restart consumer {entry['name']}: {str(e)}")


def heal_lagging_groups():
    """
    Phase 2 — Kafka broker round-trips. Runs every 5 minutes.
    Finds consumer groups that have lag but zero live Kafka members (thread died and
    heartbeat is still fresh enough to fool Phase 1) and restarts their consumer threads.
    Kept separate from restart_dead_consumers to avoid broker round-trips every minute.
    """
    site = frappe.local.site
    already_restarted = set()

    # Collect names already restarted by Phase 1 in this same scheduler tick
    stale_threshold = frappe.utils.add_to_date(
        frappe.utils.now_datetime(), seconds=-HEARTBEAT_STALE_SECONDS
    )
    recently_restarted = frappe.get_all(
        "Kafka Consumer",
        filters=[["thread_status", "=", "restarting"]],
        pluck="name"
    )
    already_restarted.update(recently_restarted)

    clusters = frappe.get_all("Kafka Cluster", fields=["name"])
    for cluster_row in clusters:
        cluster_name = cluster_row["name"]
        try:
            cluster = frappe.get_doc("Kafka Cluster", cluster_name)
            conf = _build_cluster_conf(cluster)
            stats_conf = _build_cluster_conf(
                cluster, group_id=f"kafka_mediator_watchdog_{cluster_name}"
            )

            admin_client = AdminClient(conf)

            cg_result = admin_client.list_consumer_groups().result()
            group_ids = [
                g.group_id for g in cg_result.valid
                if not g.group_id.startswith("kafka_mediator_")
            ]
            if not group_ids:
                continue

            # live member count per group
            desc_futures = admin_client.describe_consumer_groups(group_ids)
            member_counts = {}
            for gid, fut in desc_futures.items():
                try:
                    member_counts[gid] = len(fut.result().members)
                except Exception:
                    member_counts[gid] = 0

            # only bother fetching watermarks if at least one group has no members
            empty_groups = [gid for gid in group_ids if member_counts.get(gid, 0) == 0]
            if not empty_groups:
                continue

            wm_consumer = Consumer(stats_conf)
            try:
                cluster_meta = wm_consumer.list_topics(timeout=10)
                high_watermarks = {}
                for topic_name, topic_meta in cluster_meta.topics.items():
                    if topic_name.startswith("__"):
                        continue
                    high_watermarks[topic_name] = {}
                    for p in topic_meta.partitions:
                        try:
                            _, high = wm_consumer.get_watermark_offsets(
                                TopicPartition(topic_name, p), timeout=5
                            )
                            high_watermarks[topic_name][p] = high
                        except Exception:
                            high_watermarks[topic_name][p] = 0
            finally:
                try:
                    wm_consumer.close()
                except Exception:
                    pass

            for gid in empty_groups:
                try:
                    offset_futures = admin_client.list_consumer_group_offsets(
                        [_ConsumerGroupTopicPartitions(gid)]
                    )
                    result = offset_futures[gid].result()
                    group_lag = 0
                    for tp in (result.topic_partitions or []):
                        if tp.offset is None or tp.offset < 0:
                            continue
                        high = high_watermarks.get(tp.topic, {}).get(tp.partition, 0)
                        group_lag += max(0, high - tp.offset)
                except Exception as e:
                    logger.error(f"heal_lagging_groups: lag check failed for {gid}: {e}")
                    continue

                if group_lag <= 0:
                    continue

                consumers = frappe.get_all(
                    "Kafka Consumer",
                    filters={"consumer_group": gid, "enabled": 1},
                    fields=["name"]
                )
                for c in consumers:
                    if c["name"] in already_restarted:
                        continue
                    try:
                        doc = frappe.get_doc("Kafka Consumer", c["name"])
                        frappe.db.set_value("Kafka Consumer", c["name"], "thread_status", "restarting", update_modified=False)
                        frappe.db.commit()
                        threading.Thread(target=doc.consume_messages, args=(site,), daemon=True).start()
                        already_restarted.add(c["name"])
                        logger.info(
                            f"heal_lagging_groups: restarted {c['name']} "
                            f"(group '{gid}' lag={group_lag}, members=0)"
                        )
                    except Exception as e:
                        frappe.log_error(
                            "heal_lagging_groups",
                            f"Failed to restart {c['name']} for group '{gid}': {e}"
                        )

        except Exception as e:
            frappe.log_error("heal_lagging_groups", f"Cluster '{cluster_name}' check failed: {e}")


# ---------------------------------------------------------------------------
# SMS alert helper
# ---------------------------------------------------------------------------

def _send_lag_sms(cluster_name, topic, consumer_group, lag, settings):
    """
    Send an SMS to all enabled Kafka Alert Recipients for the given cluster
    if SMS alerts are enabled, lag exceeds alert_lag_threshold, and the
    per-cluster cooldown has elapsed.
    """
    if not settings.enable_sms_alerts:
        return

    alert_threshold = settings.alert_lag_threshold or settings.max_lag_threshold or 1000
    if lag < alert_threshold:
        return

    cooldown_minutes = settings.sms_alert_cooldown_minutes or 30
    cooldown_seconds = cooldown_minutes * 60

    # Check cooldown via last_sms_sent on Kafka Cluster Stats
    last_sms_sent = frappe.db.get_value(
        "Kafka Cluster Stats", {"cluster": cluster_name}, "last_sms_sent"
    )
    if last_sms_sent:
        elapsed = (frappe.utils.now_datetime() - last_sms_sent).total_seconds()
        if elapsed < cooldown_seconds:
            logger.info(
                f"SMS alert suppressed for cluster '{cluster_name}' — cooldown active "
                f"({int(elapsed)}s / {cooldown_seconds}s)"
            )
            return

    # Fetch recipients: those with matching cluster OR no cluster set
    recipients = frappe.get_all(
        "Kafka Alert Recipient",
        filters={"enabled": 1},
        fields=["mobile_no", "full_name"],
        limit=0,
        order_by="creation asc"
    )
    if not recipients:
        return

    template = (settings.sms_alert_template or "").strip() or (
        "[Kafka Alert] High lag on cluster '{{ cluster_name }}'.\n"
        "Topic: {{ topic }} | Group: {{ consumer_group }}\n"
        "Lag: {{ lag }} (threshold: {{ alert_threshold }})"
    )
    def _normalize_ke(num):
        n = (num or "").strip().replace(" ", "").replace("-", "")
        if n.startswith("0") and len(n) == 10:
            return "+254" + n[1:]
        if n.startswith("254") and not n.startswith("+"):
            return "+" + n
        return n

    try:
        from frappe.core.doctype.sms_settings.sms_settings import send_sms
        sent = 0
        for r in recipients:
            if not r.mobile_no:
                continue
            context = {
                "recipient_name": r.full_name or "",
                "cluster_name": cluster_name,
                "topic": topic,
                "consumer_group": consumer_group,
                "lag": lag,
                "alert_threshold": alert_threshold,
            }
            try:
                message = frappe.render_template(template, context)
            except Exception as e:
                frappe.log_error("SMS Template Error", f"Template render failed for {r.full_name}: {e}")
                message = (
                    f"[Kafka Alert] High lag on cluster '{cluster_name}'.\n"
                    f"Topic: {topic} | Group: {consumer_group}\n"
                    f"Lag: {lag} (threshold: {alert_threshold})"
                )
            send_sms([_normalize_ke(r.mobile_no)], message)
            sent += 1

        if sent:
            if frappe.db.exists("Kafka Cluster Stats", {"cluster": cluster_name}):
                frappe.db.set_value(
                    "Kafka Cluster Stats", {"cluster": cluster_name},
                    "last_sms_sent", frappe.utils.now_datetime(),
                    update_modified=False
                )
            frappe.db.commit()
            logger.info(
                f"SMS lag alert sent for cluster '{cluster_name}' topic '{topic}' "
                f"lag={lag} to {sent} recipient(s)"
            )
    except Exception as e:
        frappe.log_error("SMS Alert Error", f"Failed for cluster '{cluster_name}': {e}")


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
            max_consumers = consumer.max_consumers or settings.max_consumers
            scale_up_cooldown = consumer.scale_up_cooldown or settings.scale_up_cooldown
            messages_per_consumer = settings.messages_per_consumer

            group_members = frappe.get_all("Kafka Consumer", filters={
                "topic": consumer.topic, "consumer_group": consumer.consumer_group
            }, fields=["name"])

            # Write lag to all group members so every consumer row shows current lag
            # Use direct SQL UPDATE to avoid lock contention with consumer threads
            if group_members:
                member_names = [m.name for m in group_members]
                placeholders = ', '.join(['%s'] * len(member_names))
                frappe.db.sql(f"""
                    UPDATE `tabKafka Consumer`
                    SET `current_lag` = %s
                    WHERE `name` IN ({placeholders})
                """, [lag] + member_names, auto_commit=False)

            current_consumers = len(frappe.get_all("Kafka Consumer", filters={
                "topic": consumer.topic, "consumer_group": consumer.consumer_group, "enabled": 1
            }))

            _send_lag_sms(consumer.cluster, consumer.topic, consumer.consumer_group, lag, settings)

            # Scale-up only — consumers are never automatically removed.
            # Once scaled up they stay up until manually disabled.
            if lag >= max_lag_threshold:
                # Scale proportionally to real backlog: one consumer per
                # `messages_per_consumer` of lag, capped at max_consumers.
                target_consumers = max(1, min(max_consumers, (lag + messages_per_consumer - 1) // messages_per_consumer))
                if target_consumers > current_consumers:
                    last_scale_up = consumer.last_scale_up_time or 0
                    if current_time - last_scale_up > scale_up_cooldown:
                        scale_consumer(consumer.name, consumer.topic, consumer.consumer_group, target_consumers)
                        frappe.db.set_value("Kafka Consumer", consumer.name, "last_scale_up_time", current_time, update_modified=False)
                    else:
                        logger.info(f"Consumer {consumer.name} scale-up on cooldown (lag: {lag}, target: {target_consumers})")
            else:
                logger.info(f"Consumer {consumer.name} lag below scale-up threshold: {lag} (max: {max_lag_threshold}), consumers: {current_consumers} — no scale-down")

            frappe.db.commit()

        except Exception as e:
            frappe.log_error("Monitor Consumer", f"Failed to monitor consumer {consumer.name}: {str(e)}")


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
        frappe.log_error("Consumer Lag Check", f"Failed to check lag for topic {topic}: {str(e)}")
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
    settings = frappe.get_doc("Kafka Settings")
    max_consumers = source.max_consumers or settings.max_consumers

    # Restart any enabled consumers whose threads are stale (dead process, server restart, etc.)
    stale_threshold = frappe.utils.add_to_date(
        frappe.utils.now_datetime(), seconds=-HEARTBEAT_STALE_SECONDS
    )
    stale = frappe.get_all("Kafka Consumer", filters=[
        ["topic", "=", topic_name],
        ["consumer_group", "=", consumer_group],
        ["enabled", "=", 1],
        ["thread_status", "in", ["running", "error", "stopped"]],
        ["last_heartbeat", "<", stale_threshold],
    ], fields=["name"])
    site = frappe.local.site
    for s in stale:
        try:
            doc = frappe.get_doc("Kafka Consumer", s.name)
            threading.Thread(target=doc.consume_messages, args=(site,), daemon=True).start()
            logger.info(f"scale_consumer: restarted stale thread for {s.name}")
        except Exception as e:
            frappe.log_error("Scale Consumer", f"Failed to restart stale consumer {s.name}: {e}")

    current_consumers = len(frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 1
    }))
    total_consumers = len(frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group
    }))

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
    # Only retrieve passwords that actually exist in __Auth table
    api_password = get_decrypted_password(
        "Kafka Consumer", consumer_name, "api_password", raise_exception=False
    ) or None
    api_consumer_key = get_decrypted_password(
        "Kafka Consumer", consumer_name, "api_consumer_key", raise_exception=False
    ) or None
    api_consumer_secret = get_decrypted_password(
        "Kafka Consumer", consumer_name, "api_consumer_secret", raise_exception=False
    ) or None

    while consumers_needed > 0 and total_consumers < max_consumers:
        try:
            # Build consumer dict with only non-None password fields
            consumer_dict = {
                "doctype": "Kafka Consumer",
                "cluster": source.cluster,
                "topic": source.topic,
                "consumer_group": source.consumer_group,
                "api_endpoint": source.api_endpoint,
                "api_username": source.api_username,
                "api_key": source.api_key,
                "output_topic": source.output_topic,
                "failure_topic": source.failure_topic,
                "max_retries": source.max_retries,
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

            new_consumer = frappe.get_doc(consumer_dict)
            new_consumer.insert()
            logger.info(f"Scaled up: new consumer {new_consumer.name} created for topic {topic_name}")
            consumers_needed -= 1
            total_consumers += 1
        except Exception as e:
            frappe.log_error("Scale Consumer", f"Failed to scale consumer {consumer_name}: {str(e)}")
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
            # Signal the thread to stop — it checks `enabled` on every poll loop
            frappe.db.set_value("Kafka Consumer", consumer.name, "enabled", 0)
            frappe.db.commit()
            logger.info(f"Scaled down: disabled consumer {consumer.name} for topic {topic_name}")
    except Exception as e:
        frappe.log_error("Scale Down Consumer", f"Failed to scale down consumer {consumer_name}: {str(e)}")


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
        lag_by_group = {}
        for g in groups:
            try:
                group_conf = _build_cluster_conf(cluster, group_id=g.consumer_group)
                group_lag = check_consumer_lag(group_conf, topic_name, g.consumer_group)
                lag_by_group[g.consumer_group] = group_lag
                total_lag += group_lag
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
            "lag_by_group": lag_by_group,
            "partition_count": len(partitions),
            "replication_factor": replication_factor,
            "active_consumers": active_consumers,
        }
    finally:
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass


_live_stats_cache = {}  # {cluster_name: (timestamp, data)}
LIVE_STATS_CACHE_TTL = 60  # seconds


def fetch_live_cluster_stats(cluster_name):
    """
    Fetch ALL stats directly from Kafka — cached for LIVE_STATS_CACHE_TTL seconds.
    """
    now = time.monotonic()
    cached = _live_stats_cache.get(cluster_name)
    if cached and (now - cached[0]) < LIVE_STATS_CACHE_TTL:
        return cached[1]

    cluster = frappe.get_doc("Kafka Cluster", cluster_name)
    base_conf = _build_cluster_conf(cluster)
    stats_conf = _build_cluster_conf(cluster, group_id=f"kafka_mediator_live_stats_{cluster_name}")

    consumer = None
    admin_client = None
    topic_stats = {}
    group_stats = {}

    try:
        consumer = Consumer(stats_conf)
        admin_client = AdminClient(base_conf)

        # ── 1. All topics from Kafka ──────────────────────────────────────────
        cluster_metadata = consumer.list_topics(timeout=10)
        kafka_topics = {
            name: meta
            for name, meta in cluster_metadata.topics.items()
            if not name.startswith("__")
        }

        # Watermarks (message count) + partition list
        high_watermarks = {}  # topic → {partition → high_offset}
        for topic_name, topic_meta in kafka_topics.items():
            partition_ids = list(topic_meta.partitions.keys())
            high_watermarks[topic_name] = {}
            message_count = 0
            for p in partition_ids:
                try:
                    low, high = consumer.get_watermark_offsets(TopicPartition(topic_name, p))
                    high_watermarks[topic_name][p] = high
                    message_count += max(0, high - low)
                except Exception as e:
                    logger.error(f"Watermark failed for {topic_name}[{p}]: {e}")
                    high_watermarks[topic_name][p] = 0

            topic_stats[topic_name] = {
                "message_count": message_count,
                "partition_count": len(partition_ids),
                "replication_factor": 1,
                "lag_by_group": {},
                "total_lag": 0,
                "retention_days": None,
            }

        # Replication factors via describe_topics
        if kafka_topics:
            try:
                futures = admin_client.describe_topics(
                    TopicCollection(list(kafka_topics.keys())), request_timeout=30
                )
                for topic_name, future in futures.items():
                    try:
                        desc = future.result()
                        if desc.partitions:
                            topic_stats[topic_name]["replication_factor"] = len(desc.partitions[0].replicas)
                    except Exception as e:
                        logger.error(f"Replication factor failed for {topic_name}: {e}")
            except Exception as e:
                logger.error(f"describe_topics failed: {e}")

        # Retention periods via describe_configs
        if kafka_topics:
            try:
                config_resources = [ConfigResource(ConfigResource.Type.TOPIC, t) for t in kafka_topics]
                config_futures = admin_client.describe_configs(config_resources)
                for resource, future in config_futures.items():
                    try:
                        cfg = future.result()
                        raw = cfg.get("retention.ms")
                        ms = int(raw.value) if raw is not None else -1
                        topic_stats[resource.name]["retention_days"] = (
                            None if ms < 0 else round(ms / 86_400_000, 2)
                        )
                    except Exception as e:
                        logger.error(f"describe_configs failed for {resource.name}: {e}")
                        topic_stats[resource.name]["retention_days"] = None
            except Exception as e:
                logger.error(f"describe_configs batch failed: {e}")

        # ── 2. All consumer groups from Kafka ─────────────────────────────────
        try:
            cg_result = admin_client.list_consumer_groups().result()
            all_group_ids = [
                g.group_id for g in cg_result.valid
                if not g.group_id.startswith("kafka_mediator_live_stats_")
                and not g.group_id.startswith("kafka_mediator_stats_")
            ]
        except Exception as e:
            logger.error(f"list_consumer_groups failed for {cluster_name}: {e}")
            all_group_ids = []

        # Describe groups to get live member counts
        member_counts = {}
        if all_group_ids:
            try:
                desc_futures = admin_client.describe_consumer_groups(all_group_ids)
                for gid, future in desc_futures.items():
                    try:
                        desc = future.result()
                        member_counts[gid] = len(desc.members)
                    except Exception as e:
                        logger.error(f"describe_consumer_groups failed for {gid}: {e}")
                        member_counts[gid] = 0
            except Exception as e:
                logger.error(f"describe_consumer_groups failed: {e}")

        # Committed offsets → lag via AdminClient (no per-group Consumer needed)
        # list_consumer_group_offsets only accepts one group at a time
        for group_id in all_group_ids:
            try:
                offset_futures = admin_client.list_consumer_group_offsets(
                    [_ConsumerGroupTopicPartitions(group_id)]
                )
                result = offset_futures[group_id].result()
                subscribed_topics = set()
                group_total_lag = 0

                for tp in (result.topic_partitions or []):
                    if tp.offset is None or tp.offset < 0:
                        continue
                    if tp.topic not in high_watermarks:
                        continue
                    high = high_watermarks[tp.topic].get(tp.partition, 0)
                    lag = max(0, high - tp.offset)
                    group_total_lag += lag
                    subscribed_topics.add(tp.topic)

                    if tp.topic in topic_stats:
                        lb = topic_stats[tp.topic]["lag_by_group"]
                        lb[group_id] = lb.get(group_id, 0) + lag
                        topic_stats[tp.topic]["total_lag"] += lag

                group_stats[group_id] = {
                    "total_lag": group_total_lag,
                    "member_count": member_counts.get(group_id, 0),
                    "topics": list(subscribed_topics),
                }
            except Exception as e:
                logger.error(f"list_consumer_group_offsets failed for {group_id}: {e}")
                group_stats[group_id] = {
                    "total_lag": 0,
                    "member_count": member_counts.get(group_id, 0),
                    "topics": [],
                }

    finally:
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass

    result = {"topics": topic_stats, "groups": group_stats}
    _live_stats_cache[cluster_name] = (time.monotonic(), result)
    return result


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
                    frappe.log_error("Cleanup Orphan", f"Failed to delete orphaned consumer {consumer.name}: {str(e)}")


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
                    frappe.log_error("Update Topic Stats", f"Failed to update stats for topic {topic.topic_name}: {str(e)}")

        except Exception as e:
            frappe.log_error("Cluster Stats Connect", f"Failed to connect to cluster {cluster_name} for stats: {str(e)}")
        finally:
            try:
                if consumer:
                    consumer.close()
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
            frappe.log_error("Update Cluster Stats", f"Failed to update stats for cluster {cluster.name}: {str(e)}")
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
            pass  # AdminClient has no close()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

@frappe.whitelist()
def format_response(success=False, message=None, status_code=200, data=None, pagination=None):
    frappe.local.response.http_status_code = status_code
    frappe.local.response.update({
        "status": "success" if status_code in (200, 201, 202) else "error",
        "message": message,
        "status_code": status_code,
        "data": data,
        "pagination": pagination or {},
    })

def validate_payload(*required_fields):
    def decorator(fn):
        @wraps(fn)
        def wrapper(**kwargs):
            missing = [f for f in required_fields if not kwargs.get(f)]
            if missing:
                return format_response(
                    status_code=400,
                    message="Missing required fields"
                )
            return fn(**kwargs)
        return wrapper
    return decorator
