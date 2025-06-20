__version__ = "0.0.1"

import frappe
import confluent_kafka
from confluent_kafka import Consumer, TopicPartition
import time
import logging

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
            'sasl.password': cluster.sasl_password or None,
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
def publish_message(producer_id, message):
    if not frappe.session.user or frappe.session.user == "Guest":
        frappe.throw("Authentication required")
    
    try:
        producer = frappe.get_doc("Kafka Producer", producer_id)
        producer.publish_message(message)
        return {"status": "success"}
    except Exception as e:
        frappe.log_error(f"Failed to publish message: {str(e)}")
        return {"status": "error", "message": str(e)}