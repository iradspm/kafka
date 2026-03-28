# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaTopic(Document):
	def before_insert(self):
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
		}
		if cluster.sasl_mechanism:
			conf['sasl.mechanisms'] = cluster.sasl_mechanism
		if cluster.sasl_username:
			conf['sasl.username'] = cluster.sasl_username
		if cluster.sasl_password:
			conf['sasl.password'] = cluster.sasl_password
		try:
			admin_client = AdminClient(conf)
			new_topic = NewTopic(
				self.topic_name,
				num_partitions=int(self.num_partitions or 1),
				replication_factor=int(self.replication_factor or 1)
			)
			# Create topic and wait for result
			future = admin_client.create_topics([new_topic], request_timeout=30)
			result = future[self.topic_name].result()  # Wait for operation to complete
			logger.info(f"Topic creation initiated for {self.topic_name}: {result}")

			# Verify topic exists
			# topics = admin_client.list_topics(timeout=10)
			# if self.topic_name not in topics.topics:
			# 	raise Exception(f"Topic {self.topic_name} was not created in Kafka")
			
			logger.info(f"Topic {self.topic_name} created successfully in Kafka cluster {self.cluster}")
		except Exception as e:
			frappe.log_error(f"Failed to create topic {self.topic_name}: {str(e)}")
			raise frappe.ValidationError(f"Failed to create topic in Kafka: {str(e)}")
		finally:
			# Ensure admin client resources are cleaned up
			try:
				admin_client.close() if 'admin_client' in locals() else None
			except:
				pass

	def increase_partitions(self, new_partitions):
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
		}
		if cluster.sasl_mechanism:
			conf['sasl.mechanisms'] = cluster.sasl_mechanism
		if cluster.sasl_username:
			conf['sasl.username'] = cluster.sasl_username
		if cluster.sasl_password:
			conf['sasl.password'] = cluster.sasl_password
		try:
			admin_client = AdminClient(conf)
			new_topic = NewTopic(
				self.topic_name,
				num_partitions=int(new_partitions),
				replication_factor=int(self.replication_factor)
			)
			future = admin_client.create_partitions([new_topic], request_timeout=30)
			result = future[self.topic_name].result()
			self.num_partitions = new_partitions
			self.save()
			frappe.msgprint(f"Updated partitions for topic {self.topic_name} to {new_partitions}")
			logger.info(f"Updated partitions for {self.topic_name} to {new_partitions}")
		except Exception as e:
			frappe.log_error(f"Failed to increase partitions for {self.topic_name}: {str(e)}")
			frappe.throw(f"Failed to increase partitions: {str(e)}")
		finally:
			try:
				admin_client.close() if 'admin_client' in locals() else None
			except:
				pass
