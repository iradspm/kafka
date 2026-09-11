# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
import logging

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
            conf['sasl.password'] = cluster.get_password("sasl_password")

        admin_client = None
        try:
            admin_client = AdminClient(conf)
            new_topic = NewTopic(
                self.topic_name,
                num_partitions=int(self.num_partitions or 1),
                replication_factor=int(self.replication_factor or 1)
            )
            futures = admin_client.create_topics([new_topic], request_timeout=30)
            futures[self.topic_name].result()
            logger.info(f"Topic {self.topic_name} created in cluster {self.cluster}")
        except Exception as e:
            frappe.log_error("Create Topic Error", f"Failed to create topic {self.topic_name}: {str(e)}")
            raise frappe.ValidationError(f"Failed to create topic in Kafka: {str(e)}")
        finally:
            pass  # AdminClient has no close()

    def increase_partitions(self, new_partitions):
        new_partitions = int(new_partitions)
        current = int(self.num_partitions or 1)

        if new_partitions <= current:
            logger.info(
                f"Topic {self.topic_name} already has {current} partitions "
                f"(requested {new_partitions}) — skipping."
            )
            return

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
            conf['sasl.password'] = cluster.get_password("sasl_password")

        admin_client = None
        try:
            admin_client = AdminClient(conf)

            new_parts = NewPartitions(self.topic_name, new_total_count=new_partitions)

            futures = admin_client.create_partitions([new_parts], request_timeout=30)
            futures[self.topic_name].result()

            frappe.db.set_value("Kafka Topic", self.name, "num_partitions", new_partitions)
            frappe.db.commit()
            logger.info(
                f"Topic {self.topic_name} partitions increased from {current} to {new_partitions}"
            )
        except frappe.ValidationError:
            raise
        except Exception as e:
            frappe.log_error("Increase Partitions Error", f"Failed to increase partitions for {self.topic_name}: {str(e)}")
            frappe.throw(f"Failed to increase partitions: {str(e)}")
        finally:
            pass  # AdminClient has no close()