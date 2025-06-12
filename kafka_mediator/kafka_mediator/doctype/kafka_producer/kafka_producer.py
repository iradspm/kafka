# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka import Producer
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer(Document):
	def after_insert(self):
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
			'sasl.mechanisms': cluster.sasl_mechanism or None,
			'sasl.username': cluster.sasl_username or None,
			'sasl.password': cluster.sasl_password or None,
			'acks': 'all',
			'retries': 3
		}
		try:
			producer = Producer(conf)
			frappe.cache().set_value(f"producer_{self.name}", producer)
			logger.info(f"Producer {self.name} initialized for topic {self.topic}")
		except Exception as e:
			frappe.log_error(f"Failed to initialize producer {self.name}: {str(e)}")
			frappe.throw(f"Failed to initialize producer: {str(e)}")

	def publish_message(self, message):
		producer = frappe.cache().get_value(f"producer_{self.name}")
		if not producer:
			frappe.throw("Producer not initialized")
		try:
			producer.produce(self.topic, value=message.encode('utf-8'))
			producer.flush(timeout=10)
			frappe.msgprint(f"Message published to topic {self.topic}")
			logger.info(f"Message published to {self.topic} by producer {self.name}")
		except Exception as e:
			frappe.log_error(f"Failed to publish message: {str(e)}")
			frappe.throw(f"Failed to publish message: {str(e)}")
