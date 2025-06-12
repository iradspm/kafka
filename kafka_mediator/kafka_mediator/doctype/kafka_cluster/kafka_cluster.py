# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka.admin import AdminClient

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
