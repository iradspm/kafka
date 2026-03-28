# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicCollection
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
			}
			if self.sasl_mechanism:
				conf['sasl.mechanisms'] = self.sasl_mechanism
			if self.sasl_username:
				conf['sasl.username'] = self.sasl_username
			if self.sasl_password:
				conf['sasl.password'] = self.sasl_password

			# conf = {
			# 	'bootstrap.servers': self.bootstrap_servers,
			# 	'security.protocol': self.security_protocol or 'PLAINTEXT',
			# 	'sasl.mechanisms': self.sasl_mechanism or None,
			# 	'sasl.username': self.sasl_username or None,
			# 	'sasl.password': self.sasl_password or None
			# }
			admin_client = AdminClient(conf)
			topics = admin_client.list_topics(timeout=10)
			self.status = "Connected"
			self.topic_count = len(topics.topics)
			logger.info(f"Validated cluster {self.name} with {self.topic_count} topics")
		except Exception as e:
			self.status = "Failed"
			#frappe.log_error(f"Failed to connect to Kafka cluster: {str(e)}")
			frappe.throw(f"Failed to connect to Kafka cluster: {str(e)}")
		finally:
			try:
				admin_client.close() if 'admin_client' in locals() else None
			except:
				pass
	
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

			# conf = {
			# 	'bootstrap.servers': self.bootstrap_servers,
			# 	'security.protocol': self.security_protocol or 'PLAINTEXT',
			# 	'sasl.mechanisms': self.sasl_mechanism or None,
			# 	'sasl.username': self.sasl_username or None,
			# 	'sasl.password': self.sasl_password or None
			# }
			admin_client = AdminClient(conf)
			topics = admin_client.list_topics(timeout=10)
			topic_keys = list(topics.topics.keys())
			logger.info(f"Retrieved topics for cluster {self.name}: {topic_keys}")
			if not topic_keys:
				logger.warning(f"No topics found for cluster {self.name}")
				return

			# Fetch replication factors using describe_topics
			non_internal_topics = [t for t in topic_keys if not t.startswith('__')]
			if not non_internal_topics:
				logger.info(f"No non-internal topics found for cluster {self.name}")
				return
			describe_futures = admin_client.describe_topics(TopicCollection(non_internal_topics), request_timeout=10)
			replication_factors = {}
			for topic_name, future in describe_futures.items():
				try:
					topic_desc = future.result()
					replication_factors[topic_name] = len(topic_desc.partitions[0].replicas) if topic_desc.partitions else 1
				except Exception as e:
					logger.error(f"Failed to get replication factor for {topic_name}: {str(e)}")
					replication_factors[topic_name] = 1  # Fallback

			synced_count = 0
			for topic_name in topic_keys:
				logger.info(f"Processing topic: {topic_name}")
				# Skip internal Kafka topics
				if topic_name.startswith('__'):
					logger.info(f"Skipping internal topic: {topic_name}")
					continue
				# Check if topic already exists
				exists = frappe.db.exists("Kafka Topic", {"topic_name": topic_name})
				logger.info(f"Topic {topic_name} exists in doctype: {exists}")
				if not exists:
					try:
						topic_doc = frappe.get_doc({
							"doctype": "Kafka Topic",
							"cluster": self.name,
							"topic_name": topic_name,
							"num_partitions": len(topics.topics[topic_name].partitions),
							"replication_factor": replication_factors.get(topic_name, 1)
						})
						topic_doc.db_insert()
						frappe.db.commit()
						logger.info(f"Synced topic {topic_name} with {len(topics.topics[topic_name].partitions)} partitions, replication factor {replication_factors.get(topic_name, 1)}")
						synced_count += 1
					except Exception as e:
						logger.error(f"Failed to sync topic {topic_name}: {str(e)}")
						continue
				else:
					logger.info(f"Topic {topic_name} already exists, skipping")
			logger.info(f"Completed sync: {synced_count} topics synced for cluster {self.name}")
		except Exception as e:
			frappe.log_error(f"Failed to sync topics for cluster {self.name}: {str(e)}")
		finally:
			try:
				admin_client.close() if 'admin_client' in locals() else None
			except:
				pass