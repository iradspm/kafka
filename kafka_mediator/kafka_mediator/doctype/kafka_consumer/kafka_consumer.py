# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import confluent_kafka
from confluent_kafka import  Consumer
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
	def after_insert(self):
		cluster = frappe.get_doc("Kafka Cluster", self.cluster)
		conf = {
			'bootstrap.servers': cluster.bootstrap_servers,
			'security.protocol': cluster.security_protocol or 'PLAINTEXT',
			'sasl.mechanisms': cluster.sasl_mechanism or None,
			'sasl.username': cluster.sasl_username or None,
			'sasl.password_ch': cluster.sasl_password or None,
			'group.id': self.consumer_group or str(uuid.uuid4()),
			'auto.offset.reset': 'earliest',
			'enable.auto.commit': False
		}
		try:
			consumer = Consumer(conf)
			consumer.subscribe([self.topic])
			frappe.cache().set_value(f"consumer_{self.name}", consumer)
			threading.Thread(target=self.consume_messages, daemon=True).start()
			logger.info(f"Consumer {self.name} started for topic {self.topic}")
		except Exception as e:
			frappe.log_error(f"Failed to initialize consumer {self.name}: {str(e)}")
			frappe.throw(f"Failed to initialize consumer: {str(e)}")

	def consume_messages(self):
		consumer = frappe.cache().get_value(f"consumer_{self.name}")
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
					producer = frappe.cache().get_value(f"producer_{self.output_producer}")
					if producer:
						producer.produce(self.output_topic, value=response.encode('utf-8'))
						producer.flush(timeout=10)
						logger.info(f"Response published to {self.output_topic}")
				consumer.commit()
			except Exception as e:
				frappe.log_error(f"Consumer {self.name} failed: {str(e)}")
				time.sleep(5)  # Prevent tight loop on error

	def call_api_endpoint(self, message):
		try:
			headers = {'Content-Type': 'application/json'}
			# Add authentication headers if credentials are provided
			if self.api_username and self.api_password:
				auth_str = f"{self.api_username}:{self.api_password}"
				headers['Authorization'] = f"Basic {base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')}"
			elif self.api_key:
				headers['Authorization'] = f"Bearer {self.api_key}"

			response = requests.post(self.api_endpoint, data=json.dumps({'message': message}), headers=headers, timeout=10)
			if response.status_code == 200:
				logger.info(f"API call successful for consumer {self.name}")
				return response.text
			else:
				frappe.log_error(f"API call failed for consumer {self.name}: {response.status_code}")
				return None
		except Exception as e:
			frappe.log_error(f"API call failed for consumer {self.name}: {str(e)}")
			return None
