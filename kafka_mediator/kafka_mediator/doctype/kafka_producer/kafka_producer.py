# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer(Document):
	_producer_cache = {}  # class-level cache: producer_name → Producer instance

	def validate(self):
		return

	def _get_producer(self):
		if self.name in KafkaProducer._producer_cache:
			return KafkaProducer._producer_cache[self.name]
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
			'acks': 'all',
			'enable.idempotence': True,
		}
		if cluster.sasl_mechanism:
			conf['sasl.mechanisms'] = cluster.sasl_mechanism
		if cluster.sasl_username:
			conf['sasl.username'] = cluster.sasl_username
		if cluster.sasl_password:
			conf['sasl.password'] = cluster.get_password("sasl_password")
		producer = Producer(conf)
		KafkaProducer._producer_cache[self.name] = producer
		logger.info(f"Producer {self.name} connected to cluster {self.cluster}")
		return producer

	def publish_message(self, message):
		try:
			producer = self._get_producer()
			producer.produce(self.topic, value=message.encode('utf-8'))
			producer.flush(timeout=30)
			logger.info(f"Message published to {self.topic} by producer {self.name}")
		except Exception as e:
			# Evict cached producer on failure so next call reconnects
			KafkaProducer._producer_cache.pop(self.name, None)
			frappe.log_error("Publish Message Error", f"Failed to publish message for producer {self.name}: {str(e)}")
			frappe.throw(f"Failed to publish message: {str(e)}")
