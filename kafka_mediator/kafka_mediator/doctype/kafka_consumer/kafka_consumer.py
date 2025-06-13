# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka import  Consumer, Producer
from confluent_kafka.admin import AdminClient
import threading
import logging
import uuid
import base64
import time
import requests, json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumer(Document):
	def validate(self):
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
			topics = admin_client.list_topics(timeout=10)
			if self.topic not in topics.topics:
				frappe.throw(f"Topic {self.topic} does not exist in Kafka cluster {self.cluster}")
			logger.info(f"Validated topic {self.topic} for consumer {self.name}")
		except Exception as e:
			frappe.log_error(f"Failed to validate topic for consumer {self.name}: {str(e)}")
			frappe.throw(f"Failed to validate topic: {str(e)}")
		finally:
			try:
				admin_client.close() if 'admin_client' in locals() else None
			except:
				pass

	def after_insert(self):
		try:
			# Capture site name in HTTP context
			site = frappe.local.site
			# Start the consumer thread, passing the site name
			threading.Thread(target=self.consume_messages, args=(site,), daemon=True).start()
			logger.info(f"Consumer {self.name} thread started for topic {self.topic}")
		except Exception as e:
			frappe.log_error(f"Failed to start consumer thread {self.name}: {str(e)}")
			frappe.throw(f"Failed to start consumer thread: {str(e)}")

	def consume_messages(self, site):
		# Initialize Frappe context using passed site name
		try:
			frappe.init(site=site)
			frappe.connect()
			cluster = frappe.get_doc("Kafka Cluster", self.cluster)
			conf = {
				'bootstrap.servers': cluster.bootstrap_servers,
				'security.protocol': cluster.security_protocol or 'PLAINTEXT',
				'sasl.mechanisms': cluster.sasl_mechanism or None,
				'sasl.username': cluster.sasl_username or None,
				'sasl.password': cluster.sasl_password or None,
				'group.id': self.consumer_group or str(uuid.uuid4()),
				'auto.offset.reset': 'earliest',
				'enable.auto.commit': False
			}
			consumer = None
			try:
				consumer = Consumer(conf)
				consumer.subscribe([self.topic])
				logger.info(f"Consumer {self.name} initialized and subscribed to topic {self.topic}")
				while True:
					try:
						msg = consumer.poll(1.0)
						if msg is None:
							continue
						if msg.error():
							frappe.log_error(f"Consumer {self.name} error: {msg.error()}")
							continue
						message = msg.value().decode('utf-8')
						response = self.call_api_endpoint(message)
						if response and self.output_topic:
							producer_conf = {
								'bootstrap.servers': cluster.bootstrap_servers,
								'security.protocol': cluster.security_protocol or 'PLAINTEXT',
								'sasl.mechanisms': cluster.sasl_mechanism or None,
								'sasl.username': cluster.sasl_username or None,
								'sasl.password': cluster.sasl_password or None,
								'acks': 'all',
								'retries': 3
							}
							producer = Producer(producer_conf)
							try:
								producer.produce(self.output_topic, value=response.encode('utf-8'))
								producer.flush(timeout=10)
								logger.info(f"Response published to {self.output_topic} by consumer {self.name}")
							finally:
								producer.flush()
						consumer.commit()
					except Exception as e:
						frappe.log_error(f"Consumer {self.name} processing failed: {str(e)}")
						time.sleep(5)
			except Exception as e:
				frappe.log_error(f"Failed to initialize consumer {self.name}: {str(e)}")
			finally:
				try:
					if consumer:
						consumer.close()
				except:
					pass
		except Exception as e:
			logger.error(f"Failed to initialize Frappe context for consumer {self.name}: {str(e)}")
		finally:
			# Clean up Frappe context
			try:
				frappe.destroy()
			except:
				pass

	def call_api_endpoint(self, message):
		try:
			headers = {'Content-Type': 'application/json'}
			if self.api_username and self.api_password:
				auth_str = f"{self.api_username}:{self.api_password}"
				headers['Authorization'] = f"Basic {base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')}"
			elif self.api_key:
				headers['Authorization'] = f"Bearer {self.api_key}"

			response = requests.post(self.api_endpoint, data=message, headers=headers, timeout=10)
			if response.status_code == 200:
				logger.info(f"API call successful for consumer {self.name}")
				return response.text
			else:
				frappe.log_error(f"API call failed for consumer {self.name}: {response.status_code}")
				return None
		except Exception as e:
			frappe.log_error(f"API call failed for consumer {self.name}: {str(e)}")
			return None
