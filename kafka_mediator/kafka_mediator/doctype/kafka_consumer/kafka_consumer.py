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

HEARTBEAT_INTERVAL = 30  # seconds between heartbeat DB writes
ENABLED_CHECK_INTERVAL = 3  # seconds between re-reading the `enabled` flag from DB


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
            pass  # AdminClient has no close()

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
        "output_topic", "failure_topic", "topic", "cluster"
    }

    SYNC_ACROSS_GROUP = {"failure_topic"}

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

        # Signal the running thread to stop, then schedule a restart in a daemon
        # thread so the web worker is not blocked waiting for the old thread to drain.
        frappe.db.set_value("Kafka Consumer", self.name, "enabled", 0)
        frappe.db.commit()

        consumer_name = self.name
        site = frappe.local.site

        def _restart():
            import frappe as _frappe
            _frappe.init(site=site)
            _frappe.connect()
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    status = _frappe.db.get_value("Kafka Consumer", consumer_name, "thread_status")
                    if status in ("stopped", "error", None):
                        break
                    time.sleep(0.5)
                _frappe.db.set_value("Kafka Consumer", consumer_name, "enabled", 1)
                _frappe.db.commit()
                fresh = _frappe.get_doc("Kafka Consumer", consumer_name)
                fresh.restart_consumer_thread()
            finally:
                _frappe.destroy()

        threading.Thread(target=_restart, daemon=True).start()

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
            self._producer = producer

            # Pooled HTTP session — reuses TCP/TLS connections across messages
            # instead of a fresh handshake per requests.post().
            self._session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)

            # Cached HIE bearer token (regenerated only near expiry) + its lock.
            self._auth_token = None
            self._auth_token_exp = 0
            self._auth_lock = threading.Lock()

            # Decrypt auth credentials once, in this thread (which has a DB
            # connection), so worker threads never touch the DB on the hot path.
            self._cached_credentials = self._load_credentials()

            # Per-consumer executor — prevents one slow consumer starving others.
            # Workers are intentionally DB-free (no frappe.connect): the happy path
            # is a pooled HTTP call + Kafka produce, so we avoid the connection
            # explosion that per-worker DB connections would cause
            # (consumers × max_workers could exhaust MariaDB max_connections).
            self._executor = ThreadPoolExecutor(max_workers=25)

            # Mark thread as running
            frappe.db.set_value("Kafka Consumer", self.name, {
                "thread_status": "running",
                "last_heartbeat": frappe.utils.now(),
                "last_error": None
            }, update_modified=False)
            frappe.db.commit()

            # Consumer config — consumer-only keys added on top of base_conf
            conf = {
                **base_conf,
                'group.id': self.consumer_group,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'group.instance.id': self.name,
                'session.timeout.ms': 30000,   # matches HEARTBEAT_INTERVAL — dead member evicted in 30s
                'heartbeat.interval.ms': 3000,  # Kafka default; 1/10 of session timeout
                'max.poll.interval.ms': 300000,
            }

            consumer = None
            last_heartbeat_tick = time.monotonic()
            last_enabled_check = 0.0
            is_enabled = True

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

                while is_enabled:
                    try:
                        # DB heartbeat
                        now = time.monotonic()

                        # Re-read the enabled flag at most every ENABLED_CHECK_INTERVAL
                        # seconds instead of on every single poll iteration.
                        if now - last_enabled_check > ENABLED_CHECK_INTERVAL:
                            is_enabled = bool(
                                frappe.db.get_value("Kafka Consumer", self.name, "enabled")
                            )
                            last_enabled_check = now
                            if not is_enabled:
                                break

                        if now - last_heartbeat_tick > HEARTBEAT_INTERVAL:
                            frappe.db.set_value(
                                "Kafka Consumer", self.name,
                                "last_heartbeat", frappe.utils.now(),
                                update_modified=False
                            )
                            frappe.db.commit()
                            last_heartbeat_tick = now

                            # Re-decrypt credentials on the main thread (which has a DB
                            # connection) so a re-saved password self-heals within one
                            # heartbeat, even if a thread restart was missed. Workers read
                            # self._cached_credentials lock-free; the attribute reassignment
                            # is an atomic reference swap, so they never see a partial dict.
                            self._cached_credentials = self._load_credentials()

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
                            self._executor.submit(self._process_message, msg): msg
                            for msg in valid_msgs
                        }

                        # Wait for all, keep Kafka heartbeat alive while waiting
                        results = []  # list of (msg, should_commit bool, api_response)
                        pending = set(futures)
                        while pending:
                            consumer.poll(0)
                            done = {f for f in pending if f.done()}
                            pending -= done

                            for future in done:
                                msg = futures[future]
                                try:
                                    should_commit, api_response = future.result()
                                    results.append((msg, should_commit, api_response))
                                except Exception as e:
                                    frappe.log_error(
                                        title=f"Consumer {self.name}: unhandled processing error",
                                        message=f"offset {msg.offset()}: {str(e)}",
                                    )
                                    results.append((msg, False, None))

                            if pending:
                                time.sleep(0.05)

                        # This thread has a live DB connection, so it is safe to write the
                        # API responses to the Error Log here (worker threads are DB-free).
                        for msg, ok, api_response in results:
                            frappe.log_error(
                                title=f"Consumer {self.name} API response",
                                message=(
                                    f"topic={msg.topic()} partition={msg.partition()} "
                                    f"offset={msg.offset()}\nsuccess={ok}\nresponse={api_response!r}"
                                ),
                            )
                        frappe.db.commit()

                        # Commit only offsets that were successfully handled (success or DLQ).
                        # Messages that returned False have no failure_topic and must be redelivered.
                        offsets_to_commit = [
                            TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                            for msg, ok, _ in results if ok
                        ]
                        skipped = len([m for m, ok, _ in results if not ok])

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
                        frappe.db.set_value("Kafka Consumer", self.name, "last_error", err_str, update_modified=False)
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
                try:
                    self._executor.shutdown(wait=False)
                except Exception:
                    pass
                try:
                    self._session.close()
                except Exception:
                    pass

            frappe.db.set_value("Kafka Consumer", self.name, "thread_status", "stopped", update_modified=False)
            frappe.db.commit()

        except Exception as e:
            try:
                frappe.db.set_value("Kafka Consumer", self.name, {
                    "thread_status": "error",
                    "last_error": str(e)[:500]
                }, update_modified=False)
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
        Returns a (should_commit, api_response) tuple: should_commit is True if the
        offset should be committed, False if it should be redelivered; api_response is
        the raw API result (str/True/False) for the main thread to log.

        Worker threads are intentionally DB-free — credentials are pre-decrypted on
        the main thread and logging here uses the stdlib logger, not frappe.log_error
        (which writes to the DB). The main thread persists api_response to the Error
        Log. This keeps the hot path off the database entirely."""
        return self._process_message_inner(msg)

    def _process_message_inner(self, msg):
        message = msg.value().decode('utf-8')
        logger.info(f"Consumer {self.name} received message at offset {msg.offset()}: {message}")
        response, detail = self.call_api_endpoint(message)
        logger.info(f"Consumer {self.name} API response for offset {msg.offset()}: {detail}")

        if response:
            if self.output_topic and isinstance(response, str):
                self._produce(self._producer, self.output_topic, response)
            logger.info(
                f"Consumer {self.name}: processed offset {msg.offset()} "
                f"partition {msg.partition()}"
            )
            return True, detail

        if self.failure_topic:
            # Always forward original payload — failure topic consumer processes it identically
            self._produce(self._producer, self.failure_topic, message)
            logger.error(
                f"Consumer {self.name}: message at offset {msg.offset()} "
                f"forwarded to failure topic '{self.failure_topic}' ({detail})"
            )
            return True, detail  # forwarded — safe to commit

        # No failure_topic: leave uncommitted so Kafka redelivers on next poll
        logger.error(
            f"Consumer {self.name}: no failure_topic set, leaving offset {msg.offset()} "
            f"partition {msg.partition()} uncommitted for redelivery ({detail})"
        )
        return False, detail

    def _produce(self, producer, topic, value):
        try:
            if not isinstance(value, str):
                value = json.dumps(value)
            producer.produce(topic, value=value.encode('utf-8'))
            producer.poll(0)
        except Exception as e:
            logger.error(f"Consumer {self.name}: producer error: {str(e)}")

    def _on_partition_assign(self, consumer, partitions):
        assigned = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(f"Consumer {self.name} assigned partitions: {assigned}")
        try:
            frappe.db.set_value("Kafka Consumer", self.name, "assigned_partitions", json.dumps(assigned), update_modified=False)
            frappe.db.commit()
        except Exception:
            pass

    def _on_partition_revoke(self, consumer, partitions):
        revoked = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(f"Consumer {self.name} partitions revoked: {revoked}")
        try:
            frappe.db.set_value("Kafka Consumer", self.name, "assigned_partitions", "[]", update_modified=False)
            frappe.db.commit()
        except Exception:
            pass

    def _load_credentials(self):
        creds = {"basic_header": None}

        pwd = self.get_password("api_password", raise_exception=False)
        consumer_key = self.get_password("api_consumer_key", raise_exception=False)
        consumer_secret = self.get_password("api_consumer_secret", raise_exception=False)

        if self.api_username and pwd:
            auth_str = f"{self.api_username}:{pwd}"
            creds["basic_header"] = "Basic " + base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
        elif consumer_key and consumer_secret:
            creds["consumer_key"] = consumer_key
            creds["consumer_secret"] = consumer_secret

        return creds

    def _get_hie_bearer(self, creds):
        """Return a cached HIE bearer token, regenerating only when it is within
        60s of expiry. Guarded by a lock so concurrent workers mint it once."""
        now = int(time.time())
        if self._auth_token and now < self._auth_token_exp - 60:
            return self._auth_token
        with self._auth_lock:
            if self._auth_token and now < self._auth_token_exp - 60:
                return self._auth_token
            self._auth_token = self.generate_hie_auth_token(
                consumer_key=creds["consumer_key"], consumer_secret=creds["consumer_secret"]
            )
            self._auth_token_exp = now + 3600  # matches token exp in generate_hie_auth_token
            return self._auth_token

    def call_api_endpoint(self, message):
        """Return (result, detail):
          result — truthy on success (response text, or True if body empty),
                   False on any failure. Drives commit + output_topic logic.
          detail — human-readable string (status + body, or the error) for the
                   Error Log so failures are diagnosable instead of a bare False."""
        try:
            headers = {'Content-Type': 'application/json'}
            creds = self._cached_credentials
            if creds.get("basic_header"):
                headers['Authorization'] = creds["basic_header"]

            elif creds.get("consumer_key") and creds.get("consumer_secret"):
                headers['Authorization'] = f"Bearer {self._get_hie_bearer(creds)}"

            elif self.api_key:
                headers['Authorization'] = f"Bearer {self.api_key}"
            frappe.log_error("Header", f"Consumer {self.name} calling API with headers: {headers}")
            response = self._session.post(
                self.api_endpoint,
                data=message.encode("utf-8"),
                headers=headers,
                timeout=10
            )
            response_status = response.status_code
            detail = f"HTTP {response_status}: {response.text}"
            if response_status in [200, 201, 202]:
                logger.info(f"API call successful for consumer {self.name}: {self.api_endpoint} {response_status}")
                return (response.text if response.text else True), detail
            logger.warning(f"API call failed for consumer {self.name} - {detail}")
            return False, detail
        except requests.exceptions.Timeout:
            detail = "Timeout after 10s"
            logger.warning(f"API call failed for consumer {self.name} - {detail}")
            return False, detail
        except Exception as e:
            detail = f"{type(e).__name__}: {str(e)}"
            logger.warning(f"API call failed for consumer {self.name} - {detail}")
            return False, detail

    def generate_hie_auth_token(self, consumer_key, consumer_secret):
        now = int(time.time())
        payload = {"key": consumer_key, "iat": now, "exp": now + 3600}
        return jwt.encode(payload, consumer_secret, algorithm="HS256", headers={"alg": "HS256", "typ": "JWT"})
