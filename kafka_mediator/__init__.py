__version__ = "0.0.1"

import frappe
import confluent_kafka
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient
import time
import logging
import uuid
import json
from functools import wraps

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_default_settings():
    return frappe.get_doc("Kafka Settings")
# Background Job for Monitoring and Scaling
@frappe.whitelist()
def monitor_kafka_metrics():
    settings = get_default_settings()
    consumers = frappe.get_all("Kafka Consumer", fields=[
        "name", "cluster", "topic", "consumer_group", "max_lag_threshold", "min_lag_threshold",
        "max_consumers", "scale_up_cooldown", "scale_down_cooldown", "last_scale_up_time",
        "last_scale_down_time", "low_lag_count"
    ], filters={"enabled": 1})
    for consumer in consumers:
        cluster = frappe.get_doc("Kafka Cluster", consumer.cluster)
        conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            'sasl.mechanisms': cluster.sasl_mechanism or None,
            'sasl.username': cluster.sasl_username or None,
            'sasl.password': cluster.get_password("sasl_password") or None,
            'group.id': consumer.consumer_group
        }
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

            # Calculate target consumers based on lag
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
        except:
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

    # Ensure enough partitions
    if target_consumers > topic.num_partitions:
        topic.increase_partitions(target_consumers) #May not be necessary with Kafka version 4. We consider removing it unless for backward compatibility reasons.
        logger.info(f"Increased partitions for {topic_name} to {target_consumers}")

    # Enable existing disabled consumers
    disabled_consumers = frappe.get_all("Kafka Consumer", filters={
        "topic": topic_name, "consumer_group": consumer_group, "enabled": 0
    }, fields=["name"])
    consumers_needed = target_consumers - current_consumers
    for dc in disabled_consumers[:consumers_needed]:
        frappe.db.set_value("Kafka Consumer", dc.name, "enabled", 1)
        # Manually restart thread since set_value doesn't trigger after_save
        frappe.get_doc("Kafka Consumer", dc.name).restart_consumer_thread()
        logger.info(f"Scaled up: Enabled existing consumer {dc.name} for topic {topic_name}")
        consumers_needed -= 1

    # Create new consumers if needed
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
                "api_password": consumer.api_password,
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
            logger.info(f"Scaled up: New consumer {new_consumer.name} created for topic {topic_name}")
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
        logger.info(f"Target consumers ({target_consumers}) reached for {consumer_name}")
        return
    try:
        consumers_to_disable = len(current_consumers) - target_consumers
        for consumer in current_consumers[:consumers_to_disable]:
            frappe.db.set_value("Kafka Consumer", consumer.name, "enabled", 0)
            logger.info(f"Scaled down: Disabled consumer {consumer.name} for topic {topic_name}")
    except Exception as e:
        frappe.log_error(f"Failed to scale down consumer {consumer_name}: {str(e)}")

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
    """Update message count, lag, active consumers, and replication factor for all topics."""
    topics = frappe.get_all("Kafka Topic", fields=["name", "topic_name", "cluster"])
    for topic in topics:
        cluster = frappe.get_doc("Kafka Cluster", topic.cluster)
        conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            'sasl.mechanisms': cluster.sasl_mechanism or None,
            'sasl.username': cluster.sasl_username,
            'sasl.password': cluster.get_password("sasl_password") or None,
            'group.id': f"stats_{topic.name}_{uuid.uuid4()}"
        }
        consumer = None
        admin_client = None
        try:
            consumer = Consumer(conf)
            admin_client = AdminClient(conf)
            metadata = consumer.list_topics(topic=topic.topic_name, timeout=10)
            if topic.topic_name not in metadata.topics:
                logger.warning(f"Topic {topic.topic_name} not found in Kafka cluster {topic.cluster}")
                continue
            partitions = [TopicPartition(topic.topic_name, p) for p in metadata.topics[topic.topic_name].partitions]
            if not partitions:
                logger.warning(f"No partitions found for topic {topic.topic_name}")
                frappe.db.set_value("Kafka Topic", topic.name, {
                    "message_count": 0,
                    "lag": 0,
                    "active_consumers": 0,
                    "replication_factor": 1,
                    "last_updated": frappe.utils.now()
                })
                frappe.db.commit()
                continue
            message_count = 0
            for partition in partitions:
                try:
                    low, high = consumer.get_watermark_offsets(partition)
                    message_count += high - low if high > low else 0
                except Exception as e:
                    logger.error(f"Failed to get watermark for partition {partition.partition} of topic {topic.topic_name}: {str(e)}")
                    continue
            active_consumers = len(frappe.get_all("Kafka Consumer", filters={
                "topic": topic.topic_name, "enabled": 1
            }))
            lag = 0
            consumer_groups = frappe.get_all("Kafka Consumer", filters={"topic": topic.topic_name}, fields=["consumer_group"], distinct=True)
            for group in consumer_groups:
                try:
                    conf['group.id'] = group.consumer_group
                    lag += check_consumer_lag(conf, topic.topic_name, group.consumer_group)
                except Exception as e:
                    logger.error(f"Failed to get lag for consumer group {group.consumer_group} for topic {topic.topic_name}: {str(e)}")
                    continue
            # Fetch replication factor
            describe_futures = admin_client.describe_topics(topics=TopicCollection([topic.topic_name]), request_timeout=30)
            replication_factor = 1
            try:
                topic_desc = describe_futures[topic.topic_name].result()
                replication_factor = len(topic_desc.partitions[0].replicas) if topic_desc.partitions else 1
            except Exception as e:
                logger.error(f"Failed to get replication factor for {topic.topic_name}: {str(e)}")
            frappe.db.set_value("Kafka Topic", topic.name, {
                "message_count": message_count,
                "lag": lag,
                "active_consumers": active_consumers,
                "replication_factor": replication_factor,
                "last_updated": frappe.utils.now()
            })
            frappe.db.commit()
            logger.info(f"Updated stats for topic {topic.topic_name}: messages={message_count}, lag={lag}, consumers={active_consumers}, replication_factor={replication_factor}")
        except Exception as e:
            frappe.log_error(f"Failed to update stats for topic {topic.topic_name}: {str(e)}")
        finally:
            try:
                if consumer:
                    consumer.close()
                if admin_client:
                    admin_client.close()
            except:
                pass

@frappe.whitelist()
def update_cluster_stats():
    """Update cluster-wide statistics."""
    clusters = frappe.get_all("Kafka Cluster", fields=["name", "bootstrap_servers", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password"])
    for cluster in clusters:
        conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            'sasl.mechanisms': cluster.sasl_mechanism or None,
            'sasl.username': cluster.sasl_username or None,
            'sasl.password': cluster.get_password("sasl_password") or None,
        }
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
            broker_status = "Connected"
            frappe.db.set_value("Kafka Cluster Stats", {"cluster": cluster.name}, {
                "cluster": cluster.name,
                "total_topics": len([t for t in topics.topics if not t.startswith('__')]),
                "total_messages": total_messages,
                "total_lag": total_lag,
                "total_consumers": total_consumers,
                "broker_status": broker_status,
                "last_updated": frappe.utils.now()
            }, update_modified=False)
            if not frappe.db.exists("Kafka Cluster Stats", {"cluster": cluster.name}):
                frappe.get_doc({
                    "doctype": "Kafka Cluster Stats",
                    "cluster": cluster.name,
                    "total_topics": len([t for t in topics.topics if not t.startswith('__')]),
                    "total_messages": total_messages,
                    "total_lag": total_lag,
                    "total_consumers": total_consumers,
                    "broker_status": broker_status,
                    "last_updated": frappe.utils.now()
                }).db_insert()
            frappe.db.commit()
            logger.info(f"Updated stats for cluster {cluster.name}: topics={len([t for t in topics.topics if not t.startswith('__')])}, messages={total_messages}, lag={total_lag}, consumers={total_consumers}")
        except Exception as e:
            frappe.log_error(f"Failed to update stats for cluster {cluster.name}: {str(e)}")
            frappe.db.set_value("Kafka Cluster Stats", {"cluster": cluster.name}, {
                "broker_status": "Failed",
                "last_updated": frappe.utils.now()
            }, update_modified=False)
            if not frappe.db.exists("Kafka Cluster Stats", {"cluster": cluster.name}):
                frappe.get_doc({
                    "doctype": "Kafka Cluster Stats",
                    "cluster": cluster.name,
                    "broker_status": "Failed",
                    "last_updated": frappe.utils.now()
                }).db_insert()
            frappe.db.commit()
        finally:
            try:
                admin_client.close() if 'admin_client' in locals() else None
            except:
                pass

# REST API for External Applications
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
        frappe.throw(f"Topic {topic_name} does not exist") #Alternatively improve it in future to just create the topic if not existing
    if output_topic and not frappe.db.exists("Kafka Topic", {"topic_name": output_topic_name}):
        frappe.throw(f"Output topic {output_topic_name} does not exist") #Ditto
    if failure_topic and not frappe.db.exists("Kafka Topic", {"topic_name": failure_topic_name}):
        frappe.throw(f"Failure topic {failure_topic_name} does not exist") #Ditto

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
        producers = frappe.get_all("Kafka Producer", filters={
            "topic": full_topic_name
        })
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
    full_topic_name = topic_name if username == "Administrator"  else f"{username}_{topic_name}"
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
        "topic_name", "message_count", "lag", "active_consumers", "num_partitions", "replication_factor", "last_updated"
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
def format_response(success=False, message = None, status_code=200, data=None, pagination=None):
    frappe.local.response.http_status_code = status_code
    response_data = {
        "status": "success" if status_code == 200 or status_code == 201 or status_code == 202 else "error",
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
    if pagination:
        response_data["pagination"] = pagination
    else:
        response_data["pagination"] = {}

    frappe.local.response.update(response_data)

def validate_payload(*required_fields):
    def decorator(fn):
        @wraps(fn)
        def wrapper(**kwargs):
            missing = [f for f in required_fields if not kwargs.get(f)]
            if missing:
                return format_response(
                    status_code=400,
                    message="Missing required fields."
                )
            return fn(**kwargs)
        return wrapper
    return decorator
