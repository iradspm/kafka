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
	def validate(self):
		#No need to validate topic right now as Kafka takes sometime to propage the topic and may not be available.
		return
		# cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		# conf = {
		# 	'bootstrap.servers': cluster.bootstrap_servers,
		# 	'security.protocol': cluster.security_protocol or 'PLAINTEXT',
		# 	'sasl.mechanisms': cluster.sasl_mechanism or None,
		# 	'sasl.username': cluster.sasl_username or None,
		# 	'sasl.password': cluster.sasl_password or None
		# }
		# try:
		# 	admin_client = AdminClient(conf)
		# 	topics = admin_client.list_topics(timeout=10)
		# 	# if self.topic not in topics.topics:
		# 	# 	frappe.throw(f"Topic {self.topic} does not exist in Kafka cluster {self.cluster}")
		# 	logger.info(f"Validated topic {self.topic} for producer {self.name}")
		# except Exception as e:
		# 	frappe.log_error(f"Failed to validate topic for producer {self.name}: {str(e)}")
		# 	frappe.throw(f"Failed to validate topic: {str(e)}")
		# finally:
		# 	try:
		# 		admin_client.close() if 'admin_client' in locals() else None
		# 	except:
		# 		pass

	def publish_message(self, message):
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
		producer = None
		try:
			producer = Producer(conf)
			producer.produce(self.topic, value=message.encode('utf-8'))
			producer.flush(timeout=10)
			frappe.msgprint(f"Message published to topic {self.topic}")
			logger.info(f"Message published to {self.topic} by producer {self.name}")
		except Exception as e:
			frappe.log_error(f"Failed to publish message for producer {self.name}: {str(e)}")
			frappe.throw(f"Failed to publish message: {str(e)}")
		finally:
			try:
				if producer:
					producer.flush()
			except:
				pass
