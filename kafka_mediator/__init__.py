__version__ = "0.0.1"

import frappe
import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Background Job for Monitoring and Scaling
@frappe.whitelist()
def monitor_kafka_metrics():
    consumers = frappe.get_all("Kafka Consumer", fields=["name", "cluster", "topic", "consumer_group", "max_lag_threshold", "max_consumers"])
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
            if lag > (consumer.max_lag_threshold or 1000):
                scale_consumer(consumer.name, consumer.topic, consumer.max_consumers or 5)
            logger.info(f"Consumer {consumer.name} lag: {lag}")
        except Exception as e:
            frappe.log_error(f"Failed to monitor consumer {consumer.name}: {str(e)}")

def check_consumer_lag(conf, topic, group_id):
    consumer = Consumer(conf)
    try:
        # Get topic partitions
        metadata = consumer.list_topics(topic=topic, timeout=10)
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        # Get committed offsets
        committed = consumer.committed(partitions, timeout=10)
        # Get high watermarks
        watermarks = consumer.get_watermark_offsets(partitions[0])
        high_watermark = watermarks[1]
        
        total_lag = 0
        for partition in committed:
            if partition.offset == -1:  # No committed offset
                continue
            # Query end offset for the partition
            topic_partition = TopicPartition(topic, partition.partition)
            _, high = consumer.get_watermark_offsets(topic_partition)
            lag = high - partition.offset
            total_lag += lag if lag > 0 else 0
        return total_lag
    finally:
        consumer.close()

def scale_consumer(consumer_name, topic_name, max_consumers):
    consumer = frappe.get_doc("Kafka Consumer", consumer_name)
    topic = frappe.get_doc("Kafka Topic", {"topic_name": topic_name})
    
    # Check current number of consumers for the topic
    current_consumers = len(frappe.get_all("Kafka Consumer", filters={"topic": topic_name, "enabled": 1}))
    if current_consumers >= max_consumers:
        logger.info(f"Max consumers ({max_consumers}) reached for {consumer_name}")
        return
    
    # Create new consumer instance
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
            "output_producer": consumer.output_producer,
            "max_lag_threshold": consumer.max_lag_threshold,
            "max_consumers": max_consumers,
            "enabled": 1
        })
        new_consumer.insert()
        logger.info(f"Scaled up: New consumer {new_consumer.name} created for topic {topic_name}")
    except Exception as e:
        frappe.log_error(f"Failed to scale consumer {consumer_name}: {str(e)}")
        return

    # Adjust topic partitions if necessary
    current_partitions = topic.num_partitions
    if current_consumers + 1 > current_partitions:
        new_partitions = current_consumers + 1
        topic.increase_partitions(new_partitions)
        logger.info(f"Increased partitions for {topic_name} to {new_partitions}")

# REST API for External Applications
@frappe.whitelist(allow_guest=True)
def create_consumer(cluster, topic, consumer_group, api_endpoint, output_topic=None, output_producer=None, max_lag_threshold=1000, max_consumers=5, api_username=None, api_password=None, api_key=None):
    try:
        consumer = frappe.get_doc({
            "doctype": "Kafka Consumer",
            "cluster": cluster,
            "topic": topic,
            "consumer_group": consumer_group,
            "api_endpoint": api_endpoint,
            "api_username": api_username,
            "api_password": api_password,
            "api_key": api_key,
            "output_topic": output_topic,
            "output_producer": output_producer,
            "max_lag_threshold": max_lag_threshold,
            "max_consumers": max_consumers,
            "enabled": 1
        })
        consumer.insert()
        return {"consumer_id": consumer.name, "status": "success"}
    except Exception as e:
        frappe.log_error(f"Failed to create consumer: {str(e)}")
        return {"status": "error", "message": str(e)}

@frappe.whitelist(allow_guest=True)
def publish_message(producer_id, message):
    try:
        producer = frappe.get_doc("Kafka Producer", producer_id)
        producer.publish_message(message)
        return {"status": "success"}
    except Exception as e:
        frappe.log_error(f"Failed to publish message: {str(e)}")
        return {"status": "error", "message": str(e)}