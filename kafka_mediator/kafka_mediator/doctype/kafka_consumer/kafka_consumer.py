# Copyright (c) 2025, Tiberbu HealthNet Limited and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from concurrent.futures import ThreadPoolExecutor
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
    executor = ThreadPoolExecutor(max_workers=10)
    def validate(self):
        if not self.consumer_group:
            frappe.throw(
                "Consumer Group is required. All consumers for the same topic "
                "must share the same consumer_group value to enable parallel scaling."
            )

        # Enforce consistent requeue_on_failure across the consumer group
        peers = frappe.get_all(
            "Kafka Consumer",
            filters={"consumer_group": self.consumer_group, "name": ["!=", self.name]},
            fields=["name", "requeue_on_failure"],
        )
        if peers:
            peer_values = {p.requeue_on_failure for p in peers}
            if len(peer_values) > 1:
                frappe.throw(
                    f"Consumers in group '{self.consumer_group}' have inconsistent "
                    f"'Requeue on Failure' settings. Fix the existing consumers first."
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
            sasl_password = cluster.get_password("sasl_password")
            if not sasl_password:
                frappe.throw(f"Password not found for Kafka Cluster {self.cluster} sasl_password. Re-save the cluster doc to store it.")
            conf['sasl.password'] = sasl_password
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

    RESTART_ON_CHANGE = {
        "api_endpoint", "api_username", "api_password",
        "api_consumer_key", "api_consumer_secret", "api_key",
        "output_topic", "failure_topic", "requeue_on_failure", "topic", "cluster"
    }

    # Fields that must be identical across all consumers in the same group
    SYNC_ACROSS_GROUP = {"failure_topic", "requeue_on_failure"}

    def on_update(self):
        """
        If any config field that the running thread reads from self.* has changed,
        restart the thread so it picks up the new values.
        Toggling `enabled` is handled separately — don't restart a disabled consumer.
        """
        if not self.enabled:
            return

        # Skip on insert — after_insert already started the thread.
        # get_doc_before_save() is None on first insert, meaning has_value_changed()
        # returns True for every field and would trigger a spurious restart.
        if not self.get_doc_before_save():
            return

        changed = {
            field for field in self.RESTART_ON_CHANGE
            if self.has_value_changed(field)
        }

        # Propagate group-wide fields to all peers silently (no restart needed on peers
        # — their running threads read these values fresh per message via self.*)
        group_changed = changed & self.SYNC_ACROSS_GROUP
        if group_changed:
            peers = frappe.get_all(
                "Kafka Consumer",
                filters={"consumer_group": self.consumer_group, "name": ["!=", self.name]},
                pluck="name",
            )
            update_vals = {f: self.get(f) for f in group_changed}
            for peer in peers:
                frappe.db.set_value("Kafka Consumer", peer, update_vals)
            if peers:
                frappe.db.commit()
                logger.info(
                    f"Consumer {self.name}: synced {group_changed} to "
                    f"{len(peers)} peer(s) in group '{self.consumer_group}'"
                )

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
        self._site = site  # worker threads need this to init their own DB connection
        try:
            frappe.init(site=site)
            frappe.connect()
            cluster = frappe.get_doc("Kafka Cluster", self.cluster)
            # Shared connection settings — valid for both Producer and Consumer
            base_conf = {
                'bootstrap.servers': cluster.bootstrap_servers,
                'security.protocol': cluster.security_protocol or 'PLAINTEXT',
            }
            if cluster.sasl_mechanism:
                base_conf['sasl.mechanisms'] = cluster.sasl_mechanism
            if cluster.sasl_username:
                base_conf['sasl.username'] = cluster.sasl_username
            if cluster.sasl_password:
                sasl_password = cluster.get_password("sasl_password")
                if not sasl_password:
                    frappe.throw(f"Password not found for Kafka Cluster {self.cluster} sasl_password. Re-save the cluster doc to store it.")
                base_conf['sasl.password'] = sasl_password

            producer = Producer({**base_conf, 'acks': 'all', 'retries': 3})
            self._producer = producer  # accessible from _process_message worker threads

            # Mark thread as running
            frappe.db.set_value("Kafka Consumer", self.name, {
                "thread_status": "running",
                "last_heartbeat": frappe.utils.now(),
                "last_error": None
            })
            frappe.db.commit()

            # Consumer config — consumer-only keys added on top of base_conf
            conf = {
                **base_conf,
                'group.id': self.consumer_group,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'group.instance.id': self.name,
                'session.timeout.ms': 60000,
                'heartbeat.interval.ms': 15000,
                'max.poll.interval.ms': 300000,
            }

            consumer = None
            last_heartbeat_tick = time.monotonic()

            try:
                consumer = Consumer(conf)
                consumer.subscribe(
                    [self.topic],
                    on_assign=self._on_partition_assign,
                    on_revoke=self._on_partition_revoke
                )

                logger.info(
                    f"Consumer {self.name} subscribed to topic {self.topic} "
                    f"(instance_id={self.name})"
                )

                while frappe.db.get_value("Kafka Consumer", self.name, "enabled"):
                    try:
                        # DB heartbeat
                        now = time.monotonic()
                        if now - last_heartbeat_tick > HEARTBEAT_INTERVAL:
                            frappe.db.set_value(
                                "Kafka Consumer", self.name,
                                "last_heartbeat", frappe.utils.now()
                            )
                            frappe.db.commit()
                            last_heartbeat_tick = now

                        # Drain up to 50 messages in one shot
                        msgs = consumer.consume(num_messages=50, timeout=1.0)
                        if not msgs:
                            continue

                        valid_msgs = []
                        for msg in msgs:
                            if msg.error():
                                frappe.log_error(f"Consumer {self.name} Kafka error: {msg.error()}")
                            else:
                                valid_msgs.append(msg)

                        if not valid_msgs:
                            continue

                        # Submit all messages to the executor concurrently
                        futures = {
                            self.executor.submit(self._process_message, msg): msg
                            for msg in valid_msgs
                        }

                        # Wait for all, keep Kafka heartbeat alive while waiting
                        results = []  # list of (msg, should_commit bool)
                        pending = set(futures)
                        while pending:
                            consumer.poll(0)
                            done = {f for f in pending if f.done()}
                            pending -= done

                            for future in done:
                                msg = futures[future]
                                try:
                                    results.append((msg, future.result()))
                                except Exception as e:
                                    frappe.log_error(
                                        f"Consumer {self.name}: unhandled error for "
                                        f"offset {msg.offset()}: {str(e)}"
                                    )
                                    results.append((msg, False))

                            if pending:
                                time.sleep(0.05)

                        # Commit only offsets that were successfully handled (success or DLQ).
                        # Messages that returned False have no failure_topic and must be redelivered.
                        offsets_to_commit = [
                            TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                            for msg, ok in results if ok
                        ]
                        skipped = len([m for m, ok in results if not ok])

                        if offsets_to_commit:
                            consumer.commit(offsets=offsets_to_commit)
                            logger.info(
                                f"Consumer {self.name}: committed {len(offsets_to_commit)} "
                                f"offsets, {skipped} left uncommitted for redelivery"
                            )
                        elif skipped:
                            logger.warning(
                                f"Consumer {self.name}: {skipped} messages uncommitted — "
                                "will be redelivered on next poll"
                            )

                    except Exception as e:
                        err_str = str(e)
                        frappe.db.set_value("Kafka Consumer", self.name, "last_error", err_str)
                        frappe.db.commit()

                        frappe.log_error(f"Consumer {self.name} processing error: {err_str}")

                        backoff_end = time.monotonic() + 5
                        while time.monotonic() < backoff_end:
                            consumer.poll(0)
                            time.sleep(0.1)

                logger.info(f"Consumer {self.name}: shutting down cleanly.")

                try:
                    consumer.commit()
                except Exception:
                    pass

            except Exception as e:
                frappe.log_error(
                    f"Consumer {self.name}: failed to initialise Kafka consumer: {str(e)}"
                )
                raise

            finally:
                try:
                    if consumer:
                        consumer.close()
                        logger.info(f"Consumer {self.name}: Kafka consumer closed.")
                except Exception:
                    pass
                try:
                    producer.flush()
                except Exception:
                    pass

            frappe.db.set_value("Kafka Consumer", self.name, "thread_status", "stopped")
            frappe.db.commit()

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

    def _process_message(self, msg):
        """Called in a thread-pool worker. Handles retry + produce/DLQ for one message.
        Returns True if the offset should be committed, False if it should be redelivered."""
        frappe.init(site=self._site)
        frappe.connect()
        try:
            return self._process_message_inner(msg)
        finally:
            frappe.destroy()

    def _process_message_inner(self, msg):
        message = msg.value().decode('utf-8')
        response = False

        for attempt in range(MAX_RETRIES + 1):
            response = self.call_api_endpoint(message)
            if response:
                break
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"Consumer {self.name}: retry {attempt + 1}/{MAX_RETRIES} "
                    f"for offset {msg.offset()} partition {msg.partition()}"
                )
                time.sleep(2 ** attempt)

        if response:
            if self.output_topic:
                self._produce(self._producer, self.output_topic, response)
            logger.info(
                f"Consumer {self.name}: processed offset {msg.offset()} "
                f"partition {msg.partition()}"
            )
            return True

        if self.failure_topic and self.requeue_on_failure:
            # Republish original payload to failure topic so it re-enters the pipeline
            self._produce(self._producer, self.failure_topic, message)
            logger.error(
                f"Consumer {self.name}: message at offset {msg.offset()} "
                f"requeued to '{self.failure_topic}'"
            )
            return True  # requeued — safe to commit

        if self.failure_topic:
            # DLQ: wrap with metadata for inspection/replay
            dlq_payload = json.dumps({
                "original_message": message,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "retries": MAX_RETRIES,
                "consumer": self.name,
                "error": "API call failed after max retries"
            })
            self._produce(self._producer, self.failure_topic, dlq_payload)
            logger.error(
                f"Consumer {self.name}: message at offset {msg.offset()} "
                f"routed to DLQ '{self.failure_topic}'"
            )
            return True  # DLQ'd — safe to commit

        # No failure_topic: do not commit so Kafka redelivers on next poll
        frappe.log_error(
            f"Consumer {self.name}: no failure_topic set, leaving offset {msg.offset()} "
            f"partition {msg.partition()} uncommitted for redelivery"
        )
        return False

    def _produce(self, producer, topic, value):
        try:
            producer.produce(topic, value=value.encode('utf-8'))
            producer.poll(0)
        except Exception as e:
            frappe.log_error(f"Producer error: {str(e)}")

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

            response = requests.post(
                self.api_endpoint,
                data=message.encode("utf-8"),
                headers=headers,
                timeout=10
            )
            frappe.log_error("API Endpoint", f"{self.api_endpoint} - {response}")
            response_status = response.status_code
            if response_status in [200,201,202]:
                logger.info(f"API call successful for consumer {self.name}")
                return True
            return False
        except requests.exceptions.Timeout:
            logger.info(f"API call failed for consumer {self.name} - Timeout")
            return False
        except Exception as e:
            logger.info(f"API call failed for consumer {self.name} - {str(e)}")
            return False

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
