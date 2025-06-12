# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic

class KafkaTopic(Document):
	def after_insert(self):
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
			'sasl.mechanisms': cluster.sasl_mechanism or None,
			'sasl.username': cluster.sasl_username or None,
			'sasl.password': cluster.sasl_password or None
		}
		try:
			admin_client = AdminClient(conf)
			new_topic = NewTopic(
				self.topic_name,
				num_partitions=int(self.num_partitions or 1),
				replication_factor=int(self.replication_factor or 1)
			)
			admin_client.create_topics([new_topic], request_timeout=30)
			frappe.msgprint(f"Topic {self.topic_name} created successfully")
		except Exception as e:
			frappe.log_error(f"Failed to create topic {self.topic_name}: {str(e)}")
			frappe.throw(f"Failed to create topic: {str(e)}")

	def increase_partitions(self, new_partitions):
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
			'sasl.mechanisms': cluster.sasl_mechanism or None,
			'sasl.username': cluster.sasl_username or None,
			'sasl.password': cluster.sasl_password or None
		}
		try:
			admin_client = AdminClient(conf)
			new_topic = NewTopic(
				self.topic_name,
				num_partitions=int(new_partitions),
				replication_factor=int(self.replication_factor)
			)
			admin_client.create_partitions([new_topic], request_timeout=30)
			self.num_partitions = new_partitions
			self.save()
			frappe.msgprint(f"Updated partitions for topic {self.topic_name} to {new_partitions}")
		except Exception as e:
			frappe.log_error(f"Failed to increase partitions for {self.topic_name}: {str(e)}")
			frappe.throw(f"Failed to increase partitions: {str(e)}")
