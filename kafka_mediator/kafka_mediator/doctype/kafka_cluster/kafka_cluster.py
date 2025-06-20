# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka.admin import AdminClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaCluster(Document):
	def validate(self):
		try:
			conf = {
				'bootstrap.servers': self.bootstrap_servers,
				'security.protocol': self.security_protocol or 'PLAINTEXT',
				'sasl.mechanisms': self.sasl_mechanism or None,
				'sasl.username': self.sasl_username or None,
				'sasl.password': self.sasl_password or None
			}
			admin_client = AdminClient(conf)
			topics = admin_client.list_topics(timeout=10)
			self.status = "Connected"
			self.topic_count = len(topics.topics)
		except Exception as e:
			self.status = "Failed"
			frappe.log_error(f"Failed to connect to Kafka cluster: {str(e)}")
			frappe.throw(f"Failed to connect to Kafka cluster: {str(e)}")
	
	def after_insert(self):
        # Sync existing Kafka topics
		try:
			conf = {
				'bootstrap.servers': self.bootstrap_servers,
				'security.protocol': self.security_protocol or 'PLAINTEXT',
				'sasl.mechanisms': self.sasl_mechanism or None,
				'sasl.username': self.sasl_username or None,
				'sasl.password': self.sasl_password or None
			}
			admin_client = AdminClient(conf)
			topics = admin_client.list_topics(timeout=10)
			for topic_name in topics.topics:
				# Skip internal Kafka topics
				if topic_name.startswith('__'):
					continue
				# Check if topic already exists
				if not frappe.db.exists("Kafka Topic", {"topic_name": topic_name}):
					frappe.db.insert({
						"doctype": "Kafka Topic",
						"cluster": self.name,
						"topic_name": topic_name,
						"num_partitions": len(topics.topics[topic_name].partitions),
						"replication_factor": 1  # Cannot reliably fetch replication factor
					})
			logger.info(f"Synced {len(topics.topics)} topics for cluster {self.name}")
		except Exception as e:
			frappe.log_error(f"Failed to sync topics for cluster {self.name}: {str(e)}")
		finally:
			try:
				admin_client.close() if 'admin_client' in locals() else None
			except:
				pass
