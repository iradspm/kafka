# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
import threading
import logging
import base64
import time
import jwt
import requests
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
HEARTBEAT_INTERVAL = 30  # seconds between heartbeat DB writes


class KafkaConsumer(Document):
    def validate(self):
        if not self.consumer_group:
            frappe.throw(
                "Consumer Group is required. All consumers for the same topic "
                "must share the same consumer_group value to enable parallel scaling."
            )
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
        try:
            admin_client = AdminClient(conf)
            topics = admin_client.list_topics(timeout=10)
            if self.topic not in topics.topics:
                frappe.throw(f"Topic {self.topic} does not exist in Kafka cluster {self.cluster}")
            logger.info(f"Validated topic {self.topic} for consumer {self.name}")
        except frappe.ValidationError:
            raise
        except Exception as e:
            frappe.log_error(f"Failed to validate topic for consumer {self.name}: {str(e)}")
            frappe.throw(f"Failed to validate topic: {str(e)}")
        finally:
            try:
                admin_client.close() if 'admin_client' in locals() else None
            except Exception:
                pass

    def after_insert(self):
        try:
            site = frappe.local.site
            threading.Thread(target=self.consume_messages, args=(site,), daemon=True).start()
            logger.info(f"Consumer {self.name} thread started for topic {self.topic}")
        except Exception as e:
            frappe.log_error(f"Failed to start consumer thread {self.name}: {str(e)}")
            frappe.throw(f"Failed to start consumer thread: {str(e)}")

    # Fields whose change requires the consumer thread to be restarted
    # so the new values are picked up from the database.
    RESTART_ON_CHANGE = {
        "api_endpoint", "api_username", "api_password",
        "api_consumer_key", "api_consumer_secret", "api_key",
        "output_topic", "failure_topic", "topic", "cluster"
    }

    def on_update(self):
        """
        If any config field that the running thread reads from self.* has changed,
        restart the thread so it picks up the new values.
        Toggling `enabled` is handled separately — don't restart a disabled consumer.
        """
        if not self.enabled:
            return

        changed = {
            field for field in self.RESTART_ON_CHANGE
            if self.has_value_changed(field)
        }

        if not changed:
            return

        thread_status = frappe.db.get_value("Kafka Consumer", self.name, "thread_status")
        logger.info(
            f"Consumer {self.name}: config fields changed {changed}, "
            f"current thread_status={thread_status} — restarting thread."
        )

        # Signal the running thread to stop, then start a fresh one.
        # The new thread will read all values from the DB via frappe.get_doc.
        frappe.db.set_value("Kafka Consumer", self.name, "enabled", 0)
        frappe.db.commit()

        # Brief pause so the polling loop sees enabled=0 and exits cleanly
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            status = frappe.db.get_value("Kafka Consumer", self.name, "thread_status")
            if status in ("stopped", "error", None):
                break
            time.sleep(0.5)

        frappe.db.set_value("Kafka Consumer", self.name, "enabled", 1)
        frappe.db.commit()

        # Spawn the new thread from a fresh document so self.* has current values
        fresh = frappe.get_doc("Kafka Consumer", self.name)
        fresh.restart_consumer_thread()

    def restart_consumer_thread(self):
        """Restart consumer thread. Only call when the thread is confirmed dead."""
        try:
            site = frappe.local.site
            threading.Thread(target=self.consume_messages, args=(site,), daemon=True).start()
            logger.info(f"Consumer {self.name} thread restarted for topic {self.topic}")
        except Exception as e:
            frappe.log_error(f"Failed to restart consumer thread {self.name}: {str(e)}")

    def consume_messages(self, site):
        try:
            frappe.init(site=site)
            frappe.connect()

            # Mark thread as running and stamp initial heartbeat
            frappe.db.set_value("Kafka Consumer", self.name, {
                "thread_status": "running",
                "last_heartbeat": frappe.utils.now(),
                "last_error": None
            })
            frappe.db.commit()

            cluster = frappe.get_doc("Kafka Cluster", self.cluster)
            conf = {
                'bootstrap.servers': cluster.bootstrap_servers,
                'security.protocol': cluster.security_protocol or 'PLAINTEXT',
                'group.id': self.consumer_group,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            if cluster.sasl_mechanism:
                conf['sasl.mechanisms'] = cluster.sasl_mechanism
            if cluster.sasl_username:
                conf['sasl.username'] = cluster.sasl_username
            if cluster.sasl_password:
                conf['sasl.password'] = cluster.get_password("sasl_password")

            consumer = None
            retry_counts = {}
            last_heartbeat_tick = time.monotonic()

            try:
                consumer = Consumer(conf)
                consumer.subscribe(
                    [self.topic],
                    on_assign=self._on_partition_assign,
                    on_revoke=self._on_partition_revoke
                )
                logger.info(f"Consumer {self.name} subscribed to topic {self.topic}")

                # DB-driven loop: picks up enabled=0 set by scale_down_consumer
                while frappe.db.get_value("Kafka Consumer", self.name, "enabled"):
                    try:
                        msg = consumer.poll(1.0)

                        # Heartbeat: write to DB every HEARTBEAT_INTERVAL seconds
                        if time.monotonic() - last_heartbeat_tick > HEARTBEAT_INTERVAL:
                            frappe.db.set_value("Kafka Consumer", self.name, "last_heartbeat", frappe.utils.now())
                            frappe.db.commit()
                            last_heartbeat_tick = time.monotonic()

                        if msg is None:
                            continue
                        if msg.error():
                            frappe.log_error(f"Consumer {self.name} error: {msg.error()}")
                            continue

                        message = msg.value().decode('utf-8')
                        retry_key = f"{msg.topic()}-{msg.partition()}-{msg.offset()}"
                        retries = retry_counts.get(retry_key, 0)

                        # frappe.log_error("Message", message)

                        response = self.call_api_endpoint(message)

                        if response and self.output_topic:
                            self._produce(cluster, self.output_topic, response)
                            retry_counts.pop(retry_key, None)
                            consumer.commit()

                        elif not response and retries < MAX_RETRIES:
                            # Transient failure — do NOT commit, will retry on next poll
                            retry_counts[retry_key] = retries + 1
                            logger.warning(
                                f"Consumer {self.name}: retry {retries + 1}/{MAX_RETRIES} "
                                f"for offset {msg.offset()}"
                            )

                        elif not response and self.failure_topic:
                            # Exhausted retries — route to DLQ with full context
                            dlq_payload = json.dumps({
                                "original_message": message,
                                "topic": msg.topic(),
                                "partition": msg.partition(),
                                "offset": msg.offset(),
                                "retries": retries,
                                "error": "API call failed after max retries"
                            })
                            self._produce(cluster, self.failure_topic, dlq_payload)
                            logger.error(
                                f"Consumer {self.name}: message routed to DLQ after {retries} retries"
                            )
                            retry_counts.pop(retry_key, None)
                            consumer.commit()

                        else:
                            # No output, no failure topic configured — commit and continue
                            retry_counts.pop(retry_key, None)
                            consumer.commit()

                    except Exception as e:
                        frappe.db.set_value("Kafka Consumer", self.name, "last_error", str(e)[:500])
                        frappe.db.commit()
                        frappe.log_error(f"Consumer {self.name} processing failed: {str(e)}")
                        time.sleep(5)

                # Drain any pending offsets before closing
                try:
                    consumer.commit()
                except Exception:
                    pass

            except Exception as e:
                frappe.log_error(f"Failed to initialize consumer {self.name}: {str(e)}")
                raise
            finally:
                try:
                    if consumer:
                        consumer.close()
                except Exception:
                    pass

            frappe.db.set_value("Kafka Consumer", self.name, "thread_status", "stopped")
            frappe.db.commit()
            logger.info(f"Consumer {self.name} thread stopped cleanly")

        except Exception as e:
            try:
                frappe.db.set_value("Kafka Consumer", self.name, {
                    "thread_status": "error",
                    "last_error": str(e)[:500]
                })
                frappe.db.commit()
            except Exception:
                pass
            logger.error(f"Consumer {self.name} thread fatal error: {str(e)}")
        finally:
            try:
                frappe.destroy()
            except Exception:
                pass

    def _produce(self, cluster, topic, value):
        """Produce a single message to a topic using a short-lived producer."""
        conf = {
            'bootstrap.servers': cluster.bootstrap_servers,
            'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            'acks': 'all',
            'retries': 3
        }
        if cluster.sasl_mechanism:
            conf['sasl.mechanisms'] = cluster.sasl_mechanism
        if cluster.sasl_username:
            conf['sasl.username'] = cluster.sasl_username
        if cluster.sasl_password:
            conf['sasl.password'] = cluster.get_password("sasl_password")
        producer = Producer(conf)
        try:
            producer.produce(topic, value=value.encode('utf-8'))
            producer.flush(timeout=10)
        finally:
            producer.flush()

    def _on_partition_assign(self, consumer, partitions):
        assigned = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(f"Consumer {self.name} assigned partitions: {assigned}")
        try:
            frappe.db.set_value("Kafka Consumer", self.name, "assigned_partitions", json.dumps(assigned))
            frappe.db.commit()
        except Exception:
            pass

    def _on_partition_revoke(self, consumer, partitions):
        revoked = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(f"Consumer {self.name} partitions revoked: {revoked}")
        try:
            frappe.db.set_value("Kafka Consumer", self.name, "assigned_partitions", "[]")
            frappe.db.commit()
        except Exception:
            pass

    def call_api_endpoint(self, message):
        try:
            headers = {'Content-Type': 'application/json'}
            if self.api_username and self.api_password:
                auth_str = f"{self.api_username}:{self.get_password('api_password')}"
                headers['Authorization'] = f"Basic {base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')}"

            elif self.api_consumer_key and self.api_consumer_secret:
                cons_key = self.get_password("api_consumer_key")
                cons_secret = self.get_password("api_consumer_secret")
                token = self.generate_hie_auth_token(consumer_key=cons_key, consumer_secret=cons_secret)
                headers['Authorization'] = f"Bearer {token}"

            elif self.api_key:
                headers['Authorization'] = f"Bearer {self.api_key}"

            # message is already a UTF-8 string decoded from Kafka bytes.
            # Send it as-is so the receiving API gets the original payload.
            # If the Kafka value is valid JSON it arrives as a JSON object;
            # if it is a plain string it arrives as a JSON string — both are valid.
            # frappe.log_error("Endpoint", self.api_endpoint)
            response = requests.post(
                self.api_endpoint,
                data=message.encode("utf-8"),
                headers=headers,
                timeout=10
            )
            if response.status_code == 200:
                logger.info(f"API call successful for consumer {self.name}")
                return response.text

            if response.status_code in (401, 403):
                frappe.log_error(
                    f"Consumer {self.name}: auth failure ({response.status_code}) "
                    f"calling {self.api_endpoint}. Check credentials."
                )
                return None

            frappe.log_error(f"API call failed for consumer {self.name}: {response.status_code}")
            return None
        except requests.exceptions.Timeout:
            frappe.log_error(f"Consumer {self.name}: API call timed out after 10 s")
            return None
        except Exception as e:
            frappe.log_error(f"API call failed for consumer {self.name}: {str(e)}")
            return None

    def generate_hie_auth_token(self, consumer_key, consumer_secret):
        try:
            now = int(time.time())
            payload = {
                "key": consumer_key,
                "iat": now,
                "exp": now + 3600
            }
            headers = {
                "alg": "HS256",
                "typ": "JWT"
            }
            token = jwt.encode(
                payload,
                consumer_secret,
                algorithm="HS256",
                headers=headers
            )
            return token
        except Exception:
            return None
