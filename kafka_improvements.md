# Kafka Mediator — Improvement Notes

> Reviewed: 2026-04-04  
> Scope: Consumer management, topic management, producer management, auto-scaling

---

## Known Issue — Duplicate FHIR Writes (validation.tiberbu.health & shr.tiberbu.health)

> Reported: 2026-04-04  
> Status: Under investigation — root cause is downstream of Kafka Mediator

### Description

Messages sent to `validation.tiberbu.health` and `shr.tiberbu.health` topics are being processed correctly by their respective consumers and forwarded to the correct DHA API endpoints. However, the same FHIR resource ends up written on **both** FHIR servers — once as version 1 and again as version 2.

### Consumer configuration (confirmed correct)

| Field | Consumer A | Consumer B |
|---|---|---|
| Topic | `validation.tiberbu.health` | `shr.tiberbu.health` |
| Consumer Group | `CG-validation.tiberbu.health` | `CG-shr.tiberbu.health` |
| API Endpoint | `https://uat.dha.go.ke/v1/shr/validation-shr-callback` | `https://uat.dha.go.ke/v1/shr/shr-callback` |
| Cluster | `CLUSTER-001` | `CLUSTER-001` |
| Output Topic | _(none)_ | _(none)_ |

Consumer groups are different, topics are different, no output topic is configured. **Kafka Mediator routing is correct and is not the cause of this problem.**

---

### Actual message flow

```
Producer
  └─► validation.tiberbu.health topic
              │
              ▼
  Consumer (CG-validation.tiberbu.health)
              │
              ▼
  POST https://uat.dha.go.ke/v1/shr/validation-shr-callback
              │
              │   ◄── Kafka Mediator responsibility ends here
              │
              ▼
    DHA backend processes message
              │
    ┌─────────┴──────────────────┐
    ▼                            ▼
validation.tiberbu.health/fhir  shr.tiberbu.health/fhir
      (FHIR resource v1)              (FHIR resource v2)
          ▲                                ▲
          │                                │
          └──── same resource ─────────────┘
                appearing on both servers
```

---

### Root cause — confirmed (shr_mediator/api/common.py)

After reading the `HIE` class in [shr_mediator/api/common.py](../shr_mediator/shr_mediator/api/common.py), the root cause is identified. Both callbacks are implemented correctly in isolation — but **both use the same `self.base_url`** to build their PUT targets, and both write the same `resource` payload.

#### How the callbacks are implemented

```python
# process_to_validation()  — called by validation-shr-callback
put_url = f"{self.base_url}{self.validation_shr_path}/{resource_type}/{resource_id}"
requests.put(put_url, json=resource, ...)

# process_to_shr()  — called by shr-callback
put_url = f"{self.base_url}{self.shr_path}/{resource_type}/{resource_id}"
requests.put(put_url, json=resource, ...)
```

Both methods:
- Read the **same fields** from the Kafka payload: `resource_type` and `resource_id`
- PUT the **same `resource` dict** unchanged to their respective paths
- Use **`self.base_url`** as the host for both requests

#### Settings that drive the URL construction

From `SHR Mediator Settings`:

| Setting field | Used in | Purpose |
|---|---|---|
| `base_url` | Both callbacks | DHA server host — **shared by both** |
| `validation_shr` | `process_to_validation` | Path on DHA server for validation FHIR store |
| `minimal_shr` | `process_to_shr` | Path on DHA server for SHR FHIR store |
| `hapi_validation_base_url` | Profile/identifier URLs only | `validation.tiberbu.health/fhir` |
| `hapi_fhir_base_url` | Profile/identifier URLs only | `shr.tiberbu.health/fhir` |

> `hapi_validation_base_url` and `hapi_fhir_base_url` are only used to build FHIR identifier system strings and profile metadata — **they are never used as HTTP targets**. The actual writes always go to `base_url`.

#### Why v1 and v2 appear on both FHIR servers

```
Producer sends resource (id: "abc-123") to validation.tiberbu.health topic
  │
  └─► Consumer A → process_to_validation()
        PUT {base_url}/validation_shr_path/Patient/abc-123
                │
                ▼
        DHA routes → validation.tiberbu.health/fhir/Patient/abc-123
                           versionId: 1 written

Producer sends SAME resource (id: "abc-123") to shr.tiberbu.health topic
  │
  └─► Consumer B → process_to_shr()
        PUT {base_url}/shr_path/Patient/abc-123
                │
                ▼
        DHA routes → shr.tiberbu.health/fhir/Patient/abc-123
                           versionId: 1 written there too

IF both FHIR paths share the same backend store OR
IF base_url routes both paths to the same HAPI instance:
  → same resource written twice with same id
  → FHIR store increments versionId: 1 → 2
```

The v1/v2 split is FHIR's natural versioning — the same resource ID written twice to the same store produces version 1 then version 2. This is not an error in the write logic itself; it is a consequence of the two paths resolving to the same FHIR store.

---

### Confirmed write targets (verified 2026-04-04)

```
base_url        =  https://uat.dha.go.ke
validation_shr  =  /v1/validation/fhir
minimal_shr     =  /v1/shr/fhir
```

Actual PUT URLs built by the callbacks:

```
process_to_validation()
  PUT https://uat.dha.go.ke/v1/validation/fhir/{resource_type}/{resource_id}

process_to_shr()
  PUT https://uat.dha.go.ke/v1/shr/fhir/{resource_type}/{resource_id}
```

The paths are **different** — the `shr_mediator` routing is correct.

---

### Confirmed root cause — DHA backend infrastructure

The two paths (`/v1/validation/fhir` and `/v1/shr/fhir`) on `uat.dha.go.ke` resolve to the **same underlying HAPI FHIR store**. They are separate URL prefixes on the same server — not separate isolated FHIR instances.

```
https://uat.dha.go.ke
  │
  ├─ /v1/validation/fhir  ─┐
  │                         ├──► same HAPI FHIR instance (single data store)
  └─ /v1/shr/fhir          ─┘
```

When the same `resource_id` is written via both paths:

```
PUT /v1/validation/fhir/Patient/abc-123  →  created  →  versionId: 1
PUT /v1/shr/fhir/Patient/abc-123         →  updated  →  versionId: 2

Both visible at:
  validation.tiberbu.health/fhir/Patient/abc-123  (v1)
  shr.tiberbu.health/fhir/Patient/abc-123         (v2)

These are the same record in the same store — just accessed via different URL prefixes.
```

---

### Second contributing factor — resource ID collision

`patient_fhir_translation()` in `common.py` already handles this correctly — it produces two distinct objects:

| Object | Resource ID used | Destination |
|---|---|---|
| `validation_patient` | `cr_id` (Client Registry ID) | `validation.tiberbu.health/fhir` |
| `shr_patient` | `shr_id` (SHR-specific ID) | `shr.tiberbu.health/fhir` |

If the **sending application** is publishing the same resource with the same `id` to both topics instead of using these two distinct objects, the ID collision makes the problem worse — even if the FHIR stores were separated, both would receive and store the same ID, creating confusing duplicates across stores.

```
WRONG — same resource, same id, sent to both topics:
  validation.tiberbu.health topic  →  Patient { id: "abc-123", ... full detail ... }
  shr.tiberbu.health topic         →  Patient { id: "abc-123", ... full detail ... }

CORRECT — different objects, different ids, sent to respective topics:
  validation.tiberbu.health topic  →  Patient { id: "abc-123",  ... full detail (cr_id) ... }
  shr.tiberbu.health topic         →  Patient { id: "xyz-456",  ... minimal (shr_id) ... }
```

---

### Resolution path

| Fix | Action | Owner | Priority |
|---|---|---|---|
| Isolate FHIR backends | DHA must configure `/v1/validation/fhir` and `/v1/shr/fhir` to route to separate HAPI FHIR instances or separate HAPI partitions with isolated data stores | DHA / Infra team | High |
| Use correct resource IDs per topic | The sending application must publish `validation_patient` (using `cr_id`) to the validation topic and `shr_patient` (using `shr_id`) to the SHR topic — not the same resource to both | Sending application | High |
| Verify `hapi_validation_base_url` and `hapi_fhir_base_url` | Confirm these settings point to the correct isolated FHIR server URLs once DHA separates the backends | SHR Mediator config | After DHA fix |

---

### Correct end-to-end flow (as designed)

```
Sending application calls patient_fhir_translation()
  │
  ├─► validation_patient  { id: cr_id,  full identifiers, all fields }
  │       └─► publish to  validation.tiberbu.health  topic
  │                 └─► Consumer A → process_to_validation()
  │                           └─► PUT https://uat.dha.go.ke/v1/validation/fhir/Patient/{cr_id}
  │                                     └─► isolated validation FHIR store  ✓
  │
  └─► shr_patient         { id: shr_id, minimal fields only }
          └─► publish to  shr.tiberbu.health  topic
                    └─► Consumer B → process_to_shr()
                              └─► PUT https://uat.dha.go.ke/v1/shr/fhir/Patient/{shr_id}
                                        └─► isolated SHR FHIR store  ✓

Different resource IDs + isolated backends = no version collision
```

---

## Message Flow — From Kafka Topic to API

### Overview

When a message lands on a Kafka topic, the assigned consumer thread picks it up and forwards it synchronously to an HTTP API endpoint. The API's response then determines whether the message is routed to a success topic, a failure topic, or retried.

```
┌─────────────────────────────────────────────────────────────────────┐
│                      KAFKA TOPIC (input)                            │
│  partition 0 │ partition 1 │ partition 2 │ ...                      │
└──────┬───────┴──────┬──────┴─────────────┴─────────────────────────┘
       │              │
       ▼              ▼
┌─────────────┐ ┌─────────────┐
│  Consumer   │ │  Consumer   │   one thread per KafkaConsumer doc
│  Thread 0   │ │  Thread 1   │   each assigned to specific partitions
└──────┬──────┘ └──────┬──────┘
       │               │
       └───────┬───────┘
               │
               ▼
     consumer.poll(1.0)          blocking — waits up to 1 s per cycle
               │
               ▼
    ┌──────────────────────┐
    │  msg.value.decode()  │     raw bytes → UTF-8 string
    └──────────┬───────────┘
               │
               ▼
    ┌──────────────────────────────────────────────────────┐
    │  HTTP POST  →  self.api_endpoint                     │
    │                                                      │
    │  Headers:                                            │
    │    Content-Type: application/json                    │
    │    Authorization: <Basic | Bearer | JWT>             │
    │                                                      │
    │  Body:                                               │
    │    { "message": "<kafka message value as string>" }  │
    │                                                      │
    │  Timeout: 10 s (hardcoded)                           │
    └──────────────────────────────────────────────────────┘
               │
       ┌───────┴────────────────────┐
       │                            │
  HTTP 200                    HTTP 4xx/5xx
  response.text                or exception
       │                            │
       ▼                            ▼
 output_topic set?          failure_topic set?
   │ yes    │ no              │ yes       │ no
   ▼        ▼                 ▼           ▼
Produce   (discard        Produce       sleep 5 s
response  response)       original msg  then retry
to                        + error JSON  same message
output_topic              to            (offset NOT
               │          failure_topic  committed)
               │               │
               └───────┬───────┘
                        │
                        ▼
               consumer.commit()     offset committed here only
                        │
                        ▼
               Next poll cycle
```

---

### Payload detail

The API receives the message wrapped in a JSON envelope — not the raw Kafka value:

```json
POST /your-api-endpoint
Content-Type: application/json
Authorization: Bearer <token>

{
  "message": "{\"order_id\": 123, \"status\": \"pending\"}"
}
```

If the original Kafka message was JSON, the receiving API must parse the outer envelope and then parse the inner `message` string again. This double-encoding is a known issue — see Improvement 3 for the proposed fix alongside DLQ routing.

---

### Failure routing payload

When the API call fails and `failure_topic` is configured, the consumer produces this envelope to the failure topic:

```json
{
  "message": "<original kafka message value>",
  "error": "API call failed"
}
```

With the DLQ improvement (Improvement 3), this will also include `topic`, `partition`, `offset`, and `retries` fields for traceability.

---

### Timing characteristics

| Step | Behaviour | Impact |
|---|---|---|
| `consumer.poll(1.0)` | Blocks up to 1 s waiting for a message | Adds up to 1 s latency per idle cycle |
| `call_api_endpoint()` | Synchronous HTTP POST, 10 s timeout | One slow API response stalls the entire partition |
| `consumer.commit()` | Manual, after API call completes | Guarantees at-least-once delivery |
| On exception | Sleep 5 s, retry same message forever | Infinite loop if API is consistently down |

**Throughput ceiling per consumer thread:**

```
max messages/min ≈ 60 / avg_api_response_seconds
```

e.g. if the API averages 2 s → ~30 messages/min per consumer thread.
This is why auto-scaling (adding more consumer threads) directly improves throughput.

---

## Consumer Groups — Strategy & Rules

### What a consumer group is

A consumer group is the unit of parallel consumption in Kafka. All `KafkaConsumer` docs that share the same `consumer_group` value cooperate to consume a topic — Kafka assigns each partition to **exactly one consumer** in the group at a time. Each consumer calls its own API endpoint independently and in parallel.

```
Topic: orders  (4 partitions)
Consumer Group: orders-processor

partition 0 ──► Consumer Thread A  (KafkaConsumer: consumer-001)
partition 1 ──► Consumer Thread B  (KafkaConsumer: consumer-002)
partition 2 ──► Consumer Thread C  (KafkaConsumer: consumer-003)
partition 3 ──► Consumer Thread D  (KafkaConsumer: consumer-004)
```

---

### The hard rule — partitions cap consumers

> You can never have more **active** consumers in a group than partitions on the topic.

```
Topic has 4 partitions, group has 6 consumers
→ 4 consumers each get 1 partition   (processing messages)
→ 2 consumers get nothing            (idle, wasting resources)
```

This is why the auto-scaler must call `increase_partitions()` before adding consumers. Adding consumers first produces idle threads that do nothing until Kafka rebalances after a partition increase.

---

### Scaling states

```
State 1 — Under-provisioned (lag building up)
─────────────────────────────────────────────
Topic: 4 partitions

partition 0 ──┐
partition 1 ──┤──► Consumer Thread A   (overloaded)
partition 2 ──┤──► Consumer Thread B   (overloaded)
partition 3 ──┘

→ Action: increase partitions to 6, spawn 4 more consumers


State 2 — Balanced
──────────────────
Topic: 4 partitions

partition 0 ──► Consumer Thread A
partition 1 ──► Consumer Thread B
partition 2 ──► Consumer Thread C
partition 3 ──► Consumer Thread D

→ No action needed


State 3 — Over-provisioned (lag has cleared)
─────────────────────────────────────────────
Topic: 4 partitions

partition 0 ──► Consumer Thread A
partition 1 ──► Consumer Thread B
partition 2 ──► Consumer Thread C
partition 3 ──► Consumer Thread D
               Consumer Thread E   (idle — no partition assigned)
               Consumer Thread F   (idle — no partition assigned)

→ Action: scale down, disable E and F
```

---

### What happens during rebalance

Every time a consumer joins or leaves a group, Kafka triggers a **rebalance** — all consumers in the group pause consumption briefly while partitions are redistributed. During this window no messages are processed.

```
Rebalance triggered by:
  - New consumer joins the group       (scale-up)
  - Consumer leaves or crashes         (scale-down or failure)
  - Partition count changes            (increase_partitions called)
  - Consumer misses a heartbeat        (Kafka-side timeout)

Rebalance sequence:
  1. All consumers in the group stop polling
  2. Kafka group coordinator revokes all partition assignments
  3. Kafka redistributes partitions evenly across active members
  4. Consumers resume polling on their new assignments
```

This pause is why graceful scale-down (Improvement 6) matters — disabling a consumer mid-poll extends the rebalance window and risks an uncommitted offset being redelivered.

---

### The blank `consumer_group` bug

In [kafka_consumer.py:74](kafka_mediator/kafka_mediator/doctype/kafka_consumer/kafka_consumer.py#L74):

```python
'group.id': self.consumer_group or str(uuid.uuid4()),
```

If `consumer_group` is left blank on the KafkaConsumer doc, each consumer gets a **random unique group ID**. The result is the opposite of parallel scaling:

```
Blank consumer_group — each consumer is its own isolated group:

partition 0 ──► Consumer Thread A  (group: a1b2-...)  processes ALL messages
partition 1 ──► Consumer Thread A
partition 0 ──► Consumer Thread B  (group: c3d4-...)  processes ALL messages again
partition 1 ──► Consumer Thread B

→ Every message is processed N times (once per consumer)
→ Each API endpoint receives every message
→ Auto-scaling makes duplication worse, not better
```

**Fix — enforce `consumer_group` on save:**

```python
# kafka_consumer.py — validate()

def validate(self):
    if not self.consumer_group:
        frappe.throw(
            "Consumer Group is required. All consumers for the same topic "
            "must share the same consumer_group value to enable parallel scaling."
        )
```

And add `consumer_group` as a **mandatory** field in the KafkaConsumer doctype JSON:

```json
{
  "fieldname": "consumer_group",
  "fieldtype": "Data",
  "label": "Consumer Group",
  "reqd": 1,
  "description": "All consumers on the same topic must share this value. Kafka assigns one partition per consumer in the group."
}
```

---

### Correct consumer group strategy for auto-scaling

| Scenario | consumer_group value | Result |
|---|---|---|
| All consumers for topic `orders` | `orders-processor` | Kafka splits partitions across them — parallel processing |
| Each consumer has a unique group | `orders-proc-<uuid>` | Every consumer gets all partitions — duplicate processing |
| Two different topics, same group | `shared-processor` | Do not do this — Kafka mixes partitions from both topics into one group |

**Rule of thumb:** one consumer group per topic. Name it after the topic and its purpose, e.g. `<topic-name>-<api-purpose>`.

---

### Recommended `consumer_group` naming in the API

When `create_consumer()` is called without an explicit group, default to a deterministic name rather than a UUID:

```python
# api/consumer.py — create_consumer()

consumer_group = data.get("consumer_group") or f"{topic_name}-consumers"
```

This ensures all consumers created for the same topic automatically share a group without the caller needing to specify it explicitly.

---

## Architecture Overview

| Layer | Files |
|---|---|
| Doctypes (models) | `kafka_cluster`, `kafka_topic`, `kafka_consumer`, `kafka_producer`, `kafka_settings` |
| REST API | `api/consumer.py`, `api/topic.py` |
| Background jobs | `kafka_mediator/__init__.py` |
| Scheduler config | `hooks.py` |

---

## Issue Severity Summary

| Component | Issue | Severity |
|---|---|---|
| Consumer thread | No lifecycle monitoring / heartbeat | Critical |
| Consumer thread | Daemon thread dies on Frappe restart | Critical |
| Consumer thread | No graceful shutdown | High |
| Offset commit | Skipped on exception → infinite retry | High |
| Scale-down | No partition draining before disable | High |
| Lag calculation | Summed per-consumer instead of per-topic | High |
| Producer | Validation disabled | High |
| Stats jobs | Never scheduled | Medium |
| Partition guard | Missing before `increase_partitions()` | Medium |
| Producer | No connection pooling or batching | Medium |
| Transient groups | Accumulate in Kafka cluster | Medium |
| Cooldown logic | Uses wall clock not monotonic | Low |

---

## Improvement 1 — Consumer Health Tracking & Heartbeat

### Problem

`after_insert()` spawns a daemon thread but nothing tracks whether it is alive. If the thread crashes, `enabled=1` stays in the database but no messages are consumed. There is also no `last_heartbeat` or `thread_status` field to query from the outside.

### Fields to add to `KafkaConsumer` doctype JSON

```json
{ "fieldname": "thread_status",  "fieldtype": "Select",   "options": "stopped\nrunning\nerror", "label": "Thread Status", "read_only": 1 },
{ "fieldname": "last_heartbeat", "fieldtype": "Datetime",  "label": "Last Heartbeat",  "read_only": 1 },
{ "fieldname": "last_error",     "fieldtype": "Small Text", "label": "Last Error",      "read_only": 1 }
```

### Code change — `kafka_consumer.py`

Add a heartbeat update inside the polling loop and set `thread_status` on entry/exit:

```python
# kafka_consumer.py  — consume_messages()

def consume_messages(self, site):
    try:
        frappe.init(site=site)
        frappe.connect()

        # Mark thread as running
        frappe.db.set_value("Kafka Consumer", self.name, {
            "thread_status": "running",
            "last_heartbeat": frappe.utils.now()
        })
        frappe.db.commit()

        cluster = frappe.get_doc("Kafka Cluster", self.cluster)
        conf = self._build_consumer_conf(cluster)

        consumer = Consumer(conf)
        consumer.subscribe(
            [self.topic],
            on_assign=self._on_partition_assign,   # see Improvement 4
            on_revoke=self._on_partition_revoke
        )

        last_heartbeat_update = time.monotonic()
        HEARTBEAT_INTERVAL = 30  # seconds

        while frappe.db.get_value("Kafka Consumer", self.name, "enabled"):
            try:
                msg = consumer.poll(1.0)

                # Update heartbeat every 30 s without hammering the DB
                if time.monotonic() - last_heartbeat_update > HEARTBEAT_INTERVAL:
                    frappe.db.set_value("Kafka Consumer", self.name, "last_heartbeat", frappe.utils.now())
                    frappe.db.commit()
                    last_heartbeat_update = time.monotonic()

                if msg is None:
                    continue
                if msg.error():
                    frappe.log_error(f"Consumer {self.name} error: {msg.error()}")
                    continue

                self._process_message(msg, consumer, cluster)

            except Exception as e:
                frappe.db.set_value("Kafka Consumer", self.name, "last_error", str(e))
                frappe.db.commit()
                frappe.log_error(f"Consumer {self.name} processing failed: {str(e)}")
                time.sleep(5)

        consumer.close()
        frappe.db.set_value("Kafka Consumer", self.name, "thread_status", "stopped")
        frappe.db.commit()

    except Exception as e:
        frappe.db.set_value("Kafka Consumer", self.name, {
            "thread_status": "error",
            "last_error": str(e)
        })
        frappe.db.commit()
        logger.error(f"Consumer {self.name} thread fatal error: {str(e)}")
    finally:
        try:
            frappe.destroy()
        except Exception:
            pass
```

---

## Improvement 2 — Dead Consumer Detector (Auto-Restart)

### Problem

There is no scheduled job that checks whether consumer threads are still alive. After a Frappe worker restart all threads are gone but `enabled=1` and `thread_status='running'` remain stale.

### Scheduled function to add to `__init__.py`

```python
# kafka_mediator/__init__.py

import time
import frappe

HEARTBEAT_STALE_SECONDS = 120  # consider dead after 2 min with no heartbeat

def restart_dead_consumers():
    """
    Scheduled every minute. Finds consumers that are enabled but whose
    last_heartbeat has not been updated recently, then restarts their thread.
    """
    stale_threshold = frappe.utils.add_to_date(frappe.utils.now_datetime(), seconds=-HEARTBEAT_STALE_SECONDS)

    dead_consumers = frappe.get_all(
        "Kafka Consumer",
        filters=[
            ["enabled", "=", 1],
            ["thread_status", "in", ["running", "error"]],
            ["last_heartbeat", "<", stale_threshold]
        ],
        fields=["name", "topic", "cluster"]
    )

    for entry in dead_consumers:
        try:
            consumer_doc = frappe.get_doc("Kafka Consumer", entry["name"])
            frappe.db.set_value("Kafka Consumer", entry["name"], "thread_status", "restarting")
            frappe.db.commit()
            consumer_doc.restart_consumer_thread()
            logger.info(f"Auto-restarted dead consumer {entry['name']}")
        except Exception as e:
            frappe.log_error(f"Failed to auto-restart consumer {entry['name']}: {str(e)}")
```

### Register in `hooks.py`

```python
# hooks.py

scheduler_events = {
    "cron": {
        "*/5 * * * *": [
            "kafka_mediator.kafka_mediator.doctype.kafka_cluster.kafka_cluster.monitor_kafka_metrics",
            "kafka_mediator.kafka_mediator.update_topic_stats",
            "kafka_mediator.kafka_mediator.update_cluster_stats"
        ],
        "* * * * *": [
            "kafka_mediator.kafka_mediator.restart_dead_consumers"
        ],
        "*/30 * * * *": [
            "kafka_mediator.kafka_mediator.cleanup_orphaned_consumers"
        ]
    }
}
```

---

## Improvement 3 — Fix Offset Commit & Add Dead Letter Queue

### Problem

In the current polling loop, any unhandled exception skips `consumer.commit()`, causing the same message to be redelivered indefinitely. There is no retry counter and no dead letter routing after repeated failures.

### Code change — extract `_process_message()` with retry tracking

```python
# kafka_consumer.py

MAX_RETRIES = 3

def _process_message(self, msg, consumer, cluster):
    message_value = msg.value().decode("utf-8")
    retry_key = f"{msg.topic()}-{msg.partition()}-{msg.offset()}"

    # Use a thread-local dict to track per-message retry counts
    if not hasattr(self, "_retry_counts"):
        self._retry_counts = {}

    retries = self._retry_counts.get(retry_key, 0)

    try:
        response = self.call_api_endpoint(message_value)

        if response and self.output_topic:
            self._produce(cluster, self.output_topic, response)
        elif not response and retries < MAX_RETRIES:
            # Transient failure — do NOT commit, will retry on next poll
            self._retry_counts[retry_key] = retries + 1
            logger.warning(f"Consumer {self.name}: retry {retries + 1}/{MAX_RETRIES} for offset {msg.offset()}")
            return  # skip commit
        elif not response and self.failure_topic:
            # Exhausted retries — route to DLQ
            dlq_payload = json.dumps({
                "original_message": message_value,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "retries": retries,
                "error": "API call failed after max retries"
            })
            self._produce(cluster, self.failure_topic, dlq_payload)
            logger.error(f"Consumer {self.name}: message sent to DLQ after {retries} retries")

        # Commit only on success or DLQ routing
        self._retry_counts.pop(retry_key, None)
        consumer.commit()

    except Exception as e:
        self._retry_counts[retry_key] = retries + 1
        raise  # Let the caller log and sleep

def _produce(self, cluster, topic, value):
    conf = self._build_producer_conf(cluster)
    producer = Producer(conf)
    try:
        producer.produce(topic, value=value.encode("utf-8"))
        producer.flush(timeout=10)
    finally:
        producer.flush()
```

---

## Improvement 4 — Partition Assignment Visibility

### Problem

`consumer.subscribe([self.topic])` gives no visibility into which partitions are assigned. If rebalancing is slow or a consumer receives zero partitions, there is no way to detect it.

### Code change — add `on_assign` / `on_revoke` callbacks

```python
# kafka_consumer.py

def _on_partition_assign(self, consumer, partitions):
    assigned = [f"{p.topic}[{p.partition}]" for p in partitions]
    logger.info(f"Consumer {self.name} assigned partitions: {assigned}")
    frappe.db.set_value(
        "Kafka Consumer", self.name,
        "assigned_partitions", json.dumps(assigned)
    )
    frappe.db.commit()

def _on_partition_revoke(self, consumer, partitions):
    revoked = [f"{p.topic}[{p.partition}]" for p in partitions]
    logger.info(f"Consumer {self.name} partitions revoked: {revoked}")
    frappe.db.set_value(
        "Kafka Consumer", self.name,
        "assigned_partitions", "[]"
    )
    frappe.db.commit()

# In consume_messages(), update subscribe call:
consumer.subscribe(
    [self.topic],
    on_assign=self._on_partition_assign,
    on_revoke=self._on_partition_revoke
)
```

Add `assigned_partitions` as a `Small Text / Read Only` field in the KafkaConsumer doctype.

---

## Improvement 5 — Fix Lag Calculation (Per-Topic, Not Per-Consumer)

### Problem

`check_consumer_lag()` sums lag across individual consumer instances. If two consumers share the same group and each processes different partitions, the lag is double-counted. Lag must be measured at the **consumer group level** against the topic's high watermark.

### Corrected function

```python
# kafka_mediator/__init__.py

def check_consumer_lag(consumer_doc):
    """
    Returns total uncommitted lag for the consumer group across all
    partitions of the topic. Uses a temporary AdminClient read — never
    creates a persistent consumer group.
    """
    cluster = frappe.get_doc("Kafka Cluster", consumer_doc.cluster)
    conf = {
        "bootstrap.servers": cluster.bootstrap_servers,
        "security.protocol": cluster.security_protocol or "PLAINTEXT",
        "group.id": consumer_doc.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    if cluster.sasl_mechanism:
        conf["sasl.mechanisms"] = cluster.sasl_mechanism
        conf["sasl.username"] = cluster.sasl_username
        conf["sasl.password"] = cluster.sasl_password

    total_lag = 0
    tmp_consumer = None
    try:
        tmp_consumer = Consumer(conf)
        # Get all partitions for the topic
        metadata = tmp_consumer.list_topics(consumer_doc.topic, timeout=10)
        partitions = [
            confluent_kafka.TopicPartition(consumer_doc.topic, p)
            for p in metadata.topics[consumer_doc.topic].partitions
        ]

        # Fetch committed offsets for this group
        committed = tmp_consumer.committed(partitions, timeout=10)

        # Fetch high watermarks per partition
        for tp in committed:
            _low, high = tmp_consumer.get_watermark_offsets(tp, timeout=5, cached=False)
            committed_offset = tp.offset if tp.offset >= 0 else 0
            total_lag += max(0, high - committed_offset)

    finally:
        if tmp_consumer:
            tmp_consumer.close()

    return total_lag
```

---

## Improvement 6 — Graceful Scale-Down

### Problem

`scale_down_consumer()` calls `frappe.db.set_value(..., "enabled", 0)` while the consumer thread may be mid-poll. The thread's `while self.enabled` check reads a stale in-memory value and does not commit the current message before stopping.

### Code change — signal-based shutdown

```python
# kafka_consumer.py

# Replace: while self.enabled:
# With a database check every iteration so the thread picks up the disable signal:

while frappe.db.get_value("Kafka Consumer", self.name, "enabled"):
    try:
        msg = consumer.poll(1.0)
        # ... process message ...
        consumer.commit()    # always commit before looping
    except Exception as e:
        # ... error handling ...

# After loop exits, commit any pending offsets and close cleanly
try:
    consumer.commit()
except Exception:
    pass
consumer.close()
frappe.db.set_value("Kafka Consumer", self.name, "thread_status", "stopped")
frappe.db.commit()
```

In the scale-down function, add a wait-for-drain step:

```python
# kafka_mediator/__init__.py

def scale_down_consumer(topic_name, count):
    consumers_to_disable = frappe.get_all(
        "Kafka Consumer",
        filters={"topic": topic_name, "enabled": 1},
        fields=["name"],
        limit=count,
        order_by="creation asc"   # disable oldest first
    )
    for c in consumers_to_disable:
        # Signal the thread to stop by setting enabled=0
        frappe.db.set_value("Kafka Consumer", c["name"], "enabled", 0)
        frappe.db.commit()

        # Wait up to 30 s for thread to drain and set thread_status=stopped
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status = frappe.db.get_value("Kafka Consumer", c["name"], "thread_status")
            if status == "stopped":
                break
            time.sleep(1)
        else:
            logger.warning(f"Consumer {c['name']} did not stop within 30 s after scale-down signal")

        logger.info(f"Scale-down: disabled consumer {c['name']}")
```

---

## Improvement 7 — Partition Guard in `increase_partitions()`

### Problem

`monitor_kafka_metrics()` calls `topic.increase_partitions(target_consumers)` without checking whether the topic already has enough partitions. On repeated lag spikes the count grows without bound.

### Code change — `kafka_topic.py`

```python
# kafka_topic.py — increase_partitions()

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
    conf = { "bootstrap.servers": cluster.bootstrap_servers, ... }
    try:
        admin_client = AdminClient(conf)
        future = admin_client.create_partitions(
            [NewPartitions(self.topic_name, new_total_count=new_partitions)],
            request_timeout=30
        )
        future[self.topic_name].result()
        self.num_partitions = new_partitions
        self.save()
        logger.info(f"Topic {self.topic_name} partitions increased from {current} to {new_partitions}")
    except Exception as e:
        frappe.log_error(f"Failed to increase partitions for {self.topic_name}: {str(e)}")
        frappe.throw(f"Failed to increase partitions: {str(e)}")
    finally:
        try:
            admin_client.close()
        except Exception:
            pass
```

---

## Improvement 8 — Producer Connection Pooling

### Problem

`publish_message()` creates a new `Producer` instance on every call and flushes synchronously. For high-throughput scenarios this is expensive.

### Code change — module-level producer pool

```python
# kafka_producer.py

import threading

_producer_pool = {}
_pool_lock = threading.Lock()

def _get_producer(cluster_name):
    """Return a cached Producer for the given cluster, creating one if needed."""
    with _pool_lock:
        if cluster_name not in _producer_pool:
            cluster = frappe.get_doc("Kafka Cluster", cluster_name)
            conf = {
                "bootstrap.servers": cluster.bootstrap_servers,
                "security.protocol": cluster.security_protocol or "PLAINTEXT",
                "acks": "all",
                "retries": 3,
                "linger.ms": 5,        # small batching window
                "compression.type": "snappy",
            }
            if cluster.sasl_mechanism:
                conf["sasl.mechanisms"] = cluster.sasl_mechanism
                conf["sasl.username"] = cluster.sasl_username
                conf["sasl.password"] = cluster.sasl_password
            _producer_pool[cluster_name] = Producer(conf)
        return _producer_pool[cluster_name]


class KafkaProducer(Document):

    def publish_message(self, message, key=None):
        producer = _get_producer(self.cluster)
        try:
            producer.produce(
                self.topic,
                key=key.encode("utf-8") if key else None,
                value=message.encode("utf-8"),
                callback=self._delivery_callback
            )
            producer.poll(0)    # trigger callbacks without blocking
        except BufferError:
            # Local queue full — flush and retry once
            producer.flush(timeout=10)
            producer.produce(self.topic, value=message.encode("utf-8"))
        except Exception as e:
            frappe.log_error(f"Failed to publish message for producer {self.name}: {str(e)}")
            frappe.throw(f"Failed to publish message: {str(e)}")

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            logger.error(f"Delivery failed for {msg.topic()}[{msg.partition()}]: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")
```

---

## Improvement 9 — Restore Producer Validation with Retry

### Problem

`validate()` in `kafka_producer.py` is entirely commented out with the note *"Kafka takes sometime to propagate the topic"*. This allows producers to be saved for non-existent topics with no error.

### Code change — validation with short retry

```python
# kafka_producer.py — validate()

def validate(self):
    cluster = frappe.get_doc("Kafka Cluster", self.cluster)
    conf = {
        "bootstrap.servers": cluster.bootstrap_servers,
        "security.protocol": cluster.security_protocol or "PLAINTEXT",
    }
    if cluster.sasl_mechanism:
        conf["sasl.mechanisms"] = cluster.sasl_mechanism
        conf["sasl.username"] = cluster.sasl_username
        conf["sasl.password"] = cluster.sasl_password

    RETRIES = 5
    WAIT = 2  # seconds between retries

    admin_client = AdminClient(conf)
    try:
        for attempt in range(1, RETRIES + 1):
            topics = admin_client.list_topics(timeout=10)
            if self.topic in topics.topics:
                logger.info(f"Validated topic {self.topic} for producer {self.name}")
                return
            if attempt < RETRIES:
                logger.info(f"Topic {self.topic} not yet visible, retry {attempt}/{RETRIES}...")
                time.sleep(WAIT)
        frappe.throw(f"Topic '{self.topic}' not found in cluster '{self.cluster}' after {RETRIES} attempts.")
    except frappe.ValidationError:
        raise
    except Exception as e:
        frappe.log_error(f"Producer validation error for {self.name}: {str(e)}")
        frappe.throw(f"Failed to validate topic: {str(e)}")
    finally:
        try:
            admin_client.close()
        except Exception:
            pass
```

---

## Improvement 10 — Schedule Orphaned Stats Jobs

### Problem

`update_topic_stats()` and `update_cluster_stats()` exist in `__init__.py` but are never called. All topic/cluster metrics are permanently stale. Also, `cleanup_orphaned_consumers()` has no schedule.

### Change in `hooks.py`

```python
# hooks.py  — replace the current scheduler_events block

scheduler_events = {
    "cron": {
        # Scaling decision + stats refresh every 5 min
        "*/5 * * * *": [
            "kafka_mediator.kafka_mediator.doctype.kafka_cluster.kafka_cluster.monitor_kafka_metrics",
            "kafka_mediator.kafka_mediator.update_topic_stats",
            "kafka_mediator.kafka_mediator.update_cluster_stats",
        ],
        # Dead consumer watchdog every 1 min
        "* * * * *": [
            "kafka_mediator.kafka_mediator.restart_dead_consumers",
        ],
        # Cleanup stale consumer docs every 30 min
        "*/30 * * * *": [
            "kafka_mediator.kafka_mediator.cleanup_orphaned_consumers",
        ],
    }
}
```

---

## Improvement 11 — Fix Transient Consumer Groups in `update_topic_stats()`

### Problem

`update_topic_stats()` generates a new unique `group_id` on every call using `uuid4()`. These transient groups accumulate in the Kafka cluster's `__consumer_offsets` topic and are never cleaned up, eventually causing performance degradation.

### Code change

```python
# kafka_mediator/__init__.py — inside update_topic_stats()

# Replace:
#   'group.id': f'kafka_mediator_stats_{str(uuid.uuid4())}'
# With a stable, per-cluster group ID that Kafka can track and reuse:

conf["group.id"] = f"kafka_mediator_stats_{cluster_name}"
```

---

## Improvement 12 — Fix Cooldown Clock

### Problem

Scale-up/scale-down cooldowns are stored as `time.time()` (wall-clock seconds). An NTP adjustment during operation can make cooldowns skip or last far longer than intended.

### Code change

```python
# kafka_mediator/__init__.py

# Replace all usages of time.time() for cooldown tracking:
# Before: last_scale_up_time = time.time()
# After:
last_scale_up_time = time.monotonic()

# And in the cooldown check:
# Before: if time.time() - last_scale_up_time < scale_up_cooldown:
# After:
if time.monotonic() - last_scale_up_time < scale_up_cooldown:
```

---

---

## Improvement 13 — API Authentication Hardening (Basic, Bearer, JWT)

### Problem

`call_api_endpoint()` currently supports Basic auth and Bearer token but:

- Authentication is entirely optional — a consumer can run with no auth header at all against a protected API and silently fail
- `api_password` and `api_key` are stored as plain text fields — Frappe will display them unmasked in the UI
- There is no JWT support (consumer key + consumer secret → signed token)
- No `auth_type` field to make intent explicit; logic falls through `if/elif` with no enforcement

### Fields to add to `KafkaConsumer` doctype JSON

```json
{ "fieldname": "auth_type",        "fieldtype": "Select",   "label": "Auth Type",
  "options": "none\nbasic\nbearer\njwt",  "default": "none" },

{ "fieldname": "api_username",     "fieldtype": "Data",     "label": "API Username",
  "depends_on": "eval:doc.auth_type == 'basic'" },

{ "fieldname": "api_password",     "fieldtype": "Password", "label": "API Password",
  "depends_on": "eval:doc.auth_type == 'basic'" },

{ "fieldname": "api_key",          "fieldtype": "Password", "label": "Bearer Token",
  "depends_on": "eval:doc.auth_type == 'bearer'" },

{ "fieldname": "jwt_consumer_key",    "fieldtype": "Data",     "label": "JWT Consumer Key",
  "depends_on": "eval:doc.auth_type == 'jwt'" },

{ "fieldname": "jwt_consumer_secret", "fieldtype": "Password", "label": "JWT Consumer Secret",
  "depends_on": "eval:doc.auth_type == 'jwt'" },

{ "fieldname": "jwt_expiry_seconds",  "fieldtype": "Int",      "label": "JWT Expiry (seconds)",
  "default": 3600, "depends_on": "eval:doc.auth_type == 'jwt'" },

{ "fieldname": "jwt_algorithm",       "fieldtype": "Data",     "label": "JWT Algorithm",
  "default": "HS256", "read_only": 1,
  "depends_on": "eval:doc.auth_type == 'jwt'" }
```

> Using `fieldtype: Password` makes Frappe encrypt the value at rest and mask it in the UI.

---

### Validate auth config on save

Add to `KafkaConsumer.validate()` so misconfigured consumers are caught before they run:

```python
# kafka_consumer.py — validate()

def validate(self):
    # ... existing topic validation ...

    auth_type = self.auth_type or "none"

    if auth_type == "basic":
        if not self.api_username or not self.api_password:
            frappe.throw("Basic auth requires both API Username and API Password.")

    elif auth_type == "bearer":
        if not self.api_key:
            frappe.throw("Bearer auth requires a Bearer Token (api_key).")

    elif auth_type == "jwt":
        if not self.jwt_consumer_key or not self.jwt_consumer_secret:
            frappe.throw("JWT auth requires both JWT Consumer Key and JWT Consumer Secret.")
```

---

### JWT token generation

Install `PyJWT` if not already present:

```bash
pip install PyJWT
```

Add a helper method to `KafkaConsumer`:

```python
# kafka_consumer.py

import jwt as pyjwt   # pip install PyJWT
from datetime import datetime, timezone, timedelta

def _generate_jwt_token(self):
    """
    Build a signed JWT from consumer_key (sub) and consumer_secret (signing key).

    The token carries:
      - iss  : consumer key (identifies the calling application)
      - sub  : consumer key
      - iat  : issued-at (UTC)
      - exp  : expiry = iat + jwt_expiry_seconds
      - jti  : unique token ID to prevent replay attacks
    """
    now = datetime.now(tz=timezone.utc)
    expiry = int(self.jwt_expiry_seconds or 3600)

    payload = {
        "iss": self.jwt_consumer_key,
        "sub": self.jwt_consumer_key,
        "iat": now,
        "exp": now + timedelta(seconds=expiry),
        "jti": str(uuid.uuid4()),   # unique per request
    }

    token = pyjwt.encode(
        payload,
        self.get_password("jwt_consumer_secret"),  # use Frappe's decryption
        algorithm="HS256"
    )
    return token
```

> `self.get_password("jwt_consumer_secret")` uses Frappe's built-in decryption so the raw secret is never stored in plaintext.

---

### Updated `call_api_endpoint()` — all four auth types

```python
# kafka_consumer.py

def call_api_endpoint(self, message):
    headers = {"Content-Type": "application/json"}
    auth_type = self.auth_type or "none"

    if auth_type == "basic":
        auth_str = f"{self.api_username}:{self.get_password('api_password')}"
        encoded = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {encoded}"

    elif auth_type == "bearer":
        headers["Authorization"] = f"Bearer {self.get_password('api_key')}"

    elif auth_type == "jwt":
        token = self._generate_jwt_token()
        headers["Authorization"] = f"Bearer {token}"

    # auth_type == "none" — no Authorization header added

    try:
        response = requests.post(
            self.api_endpoint,
            data=json.dumps({"message": message}),
            headers=headers,
            timeout=10
        )
        if response.status_code == 200:
            logger.info(f"API call successful for consumer {self.name}")
            return response.text

        # Surface auth failures explicitly so they aren't silently retried
        if response.status_code in (401, 403):
            frappe.log_error(
                f"Consumer {self.name}: auth failure ({response.status_code}) "
                f"calling {self.api_endpoint}. Check auth_type and credentials."
            )
            return None

        frappe.log_error(
            f"Consumer {self.name}: API returned {response.status_code}"
        )
        return None

    except requests.exceptions.Timeout:
        frappe.log_error(f"Consumer {self.name}: API call timed out after 10 s")
        return None
    except Exception as e:
        frappe.log_error(f"Consumer {self.name}: API call failed — {str(e)}")
        return None
```

---

### JWT token caching (optional but recommended)

Generating a new JWT on every message is wasteful. Cache the token and only regenerate when it is close to expiry:

```python
# kafka_consumer.py

from datetime import datetime, timezone, timedelta

# Thread-local cache: { consumer_name: (token_string, expiry_datetime) }
_jwt_cache = {}
_jwt_cache_lock = threading.Lock()

def _get_jwt_token(self):
    """Return a cached JWT, regenerating it 60 s before expiry."""
    with _jwt_cache_lock:
        cached = _jwt_cache.get(self.name)
        if cached:
            token, expires_at = cached
            if datetime.now(tz=timezone.utc) < expires_at - timedelta(seconds=60):
                return token  # still valid

        # Generate fresh token
        token = self._generate_jwt_token()
        expiry_seconds = int(self.jwt_expiry_seconds or 3600)
        expires_at = datetime.now(tz=timezone.utc) + timedelta(seconds=expiry_seconds)
        _jwt_cache[self.name] = (token, expires_at)
        logger.info(f"Consumer {self.name}: JWT token refreshed, expires at {expires_at}")
        return token
```

Then in `call_api_endpoint()` replace `self._generate_jwt_token()` with `self._get_jwt_token()`.

---

---

## Implementation Order

| Priority | Improvement | Why first |
|---|---|---|
| 1 | Health tracking fields + heartbeat (Imp 1) | Everything else depends on knowing thread state |
| 2 | Fix lag calculation (Imp 5) | Scaling decisions are wrong without this |
| 3 | Graceful scale-down (Imp 6) | Prevents message loss during scaling |
| 4 | Partition guard (Imp 7) | Prevents unbounded partition growth |
| 5 | Dead consumer detector (Imp 2) + hooks (Imp 10) | Auto-recovery only works once 1–4 are in place |
| 6 | Retry + DLQ (Imp 3) | Stops infinite retry loops |
| 7 | API auth hardening + JWT (Imp 13) | Security — credentials currently stored in plaintext |
| 8 | Partition visibility (Imp 4) | Observability, non-blocking |
| 9 | Producer pooling (Imp 8) | Performance, can be done independently |
| 10 | Producer validation (Imp 9) | Correctness fix, low risk |
| 11 | Transient groups (Imp 11) + cooldown clock (Imp 12) | Maintenance / correctness, low urgency |

---

## Visual Summary

### System map — current vs improved

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                        KAFKA MEDIATOR — FULL SYSTEM                        ║
╠══════════════════╦═══════════════════════════════════════════════════════════╣
║   EXTERNAL       ║   FRAPPE / KAFKA MEDIATOR                                ║
║                  ║                                                           ║
║  ┌───────────┐   ║   ┌──────────────────────────────────────────────────┐  ║
║  │  REST API │───║──►│  api/consumer.py   api/topic.py                  │  ║
║  │  Caller   │   ║   │  create / enable / disable / list / produce      │  ║
║  └───────────┘   ║   └───────────────────┬──────────────────────────────┘  ║
║                  ║                       │                                  ║
║                  ║          ┌────────────▼────────────┐                    ║
║                  ║          │      DOCTYPES            │                    ║
║                  ║          │  KafkaCluster            │                    ║
║                  ║          │  KafkaTopic              │                    ║
║                  ║          │  KafkaProducer           │                    ║
║                  ║          │  KafkaConsumer  ◄────────┼── auto-scaler      ║
║                  ║          │  KafkaSettings           │                    ║
║                  ║          └────────────┬────────────┘                    ║
║                  ║                       │                                  ║
║                  ║    ┌──────────────────▼──────────────────────┐          ║
║                  ║    │         CONSUMER THREADS                 │          ║
║                  ║    │  thread_A ──► poll ──► API ──► commit   │          ║
║                  ║    │  thread_B ──► poll ──► API ──► commit   │          ║
║                  ║    │  thread_N ──► poll ──► API ──► commit   │          ║
║                  ║    └──────────────────┬──────────────────────┘          ║
║                  ║                       │                                  ║
║  ┌───────────┐   ║   ┌──────────────────▼──────────────────────┐          ║
║  │  Target   │◄──║───│         HTTP API ENDPOINT                │          ║
║  │  API      │───║──►│  auth: Basic | Bearer | JWT (HS256)      │          ║
║  └───────────┘   ║   └─────────────────────────────────────────┘          ║
║                  ║                                                           ║
╠══════════════════╬═══════════════════════════════════════════════════════════╣
║  KAFKA CLUSTER   ║   SCHEDULER (hooks.py)                                   ║
║                  ║                                                           ║
║  ┌─────────────┐ ║   every 1 min  ──► restart_dead_consumers                ║
║  │  Topic      │ ║   every 5 min  ──► monitor_kafka_metrics                 ║
║  │  partition0 │ ║                    update_topic_stats                    ║
║  │  partition1 │ ║                    update_cluster_stats                  ║
║  │  partition2 │ ║   every 30 min ──► cleanup_orphaned_consumers            ║
║  └─────────────┘ ║                                                           ║
╚══════════════════╩═══════════════════════════════════════════════════════════╝
```

---

### All 13 improvements at a glance

```
┌─────┬──────────────────────────────────────┬────────────┬──────────────────────────────┐
│  #  │  Improvement                         │  Severity  │  File(s) affected            │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  1  │  Consumer health tracking            │  Critical  │  kafka_consumer.py           │
│     │  thread_status, last_heartbeat,      │            │  kafka_consumer.json         │
│     │  last_error fields + heartbeat loop  │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  2  │  Dead consumer detector              │  Critical  │  __init__.py                 │
│     │  Auto-restart stale threads          │            │  hooks.py                    │
│     │  on 1-min schedule                   │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  3  │  Retry limit + Dead Letter Queue     │  High      │  kafka_consumer.py           │
│     │  Max 3 retries then route to         │            │                              │
│     │  failure_topic with full context     │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  4  │  Partition assignment visibility     │  High      │  kafka_consumer.py           │
│     │  on_assign / on_revoke callbacks,    │            │  kafka_consumer.json         │
│     │  assigned_partitions field           │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  5  │  Fix lag calculation                 │  High      │  __init__.py                 │
│     │  Per consumer-group + topic,         │            │                              │
│     │  not summed per consumer doc         │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  6  │  Graceful scale-down                 │  High      │  kafka_consumer.py           │
│     │  DB-driven loop exit + wait-for-     │            │  __init__.py                 │
│     │  drain before disabling consumer     │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  7  │  Partition guard                     │  Medium    │  kafka_topic.py              │
│     │  Skip increase_partitions() if       │            │                              │
│     │  count already sufficient            │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  8  │  Producer connection pooling         │  Medium    │  kafka_producer.py           │
│     │  Module-level pool, async callbacks, │            │                              │
│     │  linger + snappy compression         │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│  9  │  Restore producer validation         │  High      │  kafka_producer.py           │
│     │  Re-enable with 5-retry backoff      │            │                              │
│     │  instead of skipping entirely        │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│ 10  │  Schedule orphaned stats jobs        │  Medium    │  hooks.py                    │
│     │  update_topic_stats,                 │            │                              │
│     │  update_cluster_stats,               │            │                              │
│     │  cleanup_orphaned_consumers          │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│ 11  │  Fix transient consumer groups       │  Medium    │  __init__.py                 │
│     │  Stable group_id per cluster         │            │                              │
│     │  instead of uuid4() each call        │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│ 12  │  Fix cooldown clock                  │  Low       │  __init__.py                 │
│     │  time.monotonic() instead of         │            │                              │
│     │  time.time() for scale cooldowns     │            │                              │
├─────┼──────────────────────────────────────┼────────────┼──────────────────────────────┤
│ 13  │  API auth hardening + JWT            │  High      │  kafka_consumer.py           │
│     │  auth_type field, Password storage,  │            │  kafka_consumer.json         │
│     │  JWT HS256 with token caching        │            │                              │
└─────┴──────────────────────────────────────┴────────────┴──────────────────────────────┘
```

---

### Consumer thread lifecycle — before vs after

```
BEFORE                                  AFTER
──────────────────────────────────────────────────────────────────────
after_insert()                          after_insert()
  └─ spawn daemon thread                  └─ spawn daemon thread
       │                                       │
       ▼                                       ▼
  consume_messages()                      set thread_status = running
       │                                  set last_heartbeat = now
       ▼                                       │
  while self.enabled:  ← stale RAM         while DB.enabled:  ← live DB
       │                                       │
       ├─ poll()                               ├─ poll()
       ├─ call_api()                           ├─ update heartbeat (30s)
       ├─ commit() if OK                       ├─ _process_message()
       └─ sleep(5) on error                    │    ├─ call_api()
            │                                  │    ├─ retry up to 3x
            ▼                                  │    └─ DLQ after 3 fails
       Thread dies?                            ├─ commit() on success/DLQ
       No detection.                           └─ sleep(5) on error
       No restart.                                  │
       enabled stays = 1                            ▼
                                           Thread dies?
                                           restart_dead_consumers()
                                           detects stale heartbeat
                                           → auto-restart in 1 min
                                           thread_status = error
```

---

### Auto-scaling decision flow — before vs after

```
BEFORE                                  AFTER
──────────────────────────────────────────────────────────────────────
monitor_kafka_metrics() [5 min]         monitor_kafka_metrics() [5 min]
  │                                       │
  ├─ sum lag per consumer doc             ├─ compute lag at group level
  │   (WRONG — double counts)             │   per topic (correct)
  │                                       │
  ├─ target = lag / messages_per          ├─ target = lag / messages_per
  │           _consumer                   │           _consumer
  │                                       │
  ├─ increase_partitions(target)          ├─ if target > current_partitions
  │   (no guard — grows unbounded)        │     increase_partitions(target)
  │                                       │
  ├─ enable disabled consumers            ├─ enable disabled consumers
  │                                       │
  └─ scale down?                          └─ scale down?
       disable consumer immediately            signal enabled=0
       (mid-poll, no drain)                    wait up to 30 s for
       → possible message loss                 thread_status=stopped
                                               → safe drain guaranteed
```

---

### Authentication flow — before vs after

```
BEFORE                                  AFTER
──────────────────────────────────────────────────────────────────────
call_api_endpoint(message)             call_api_endpoint(message)
  │                                       │
  ├─ if api_username + api_password        ├─ auth_type == "basic"
  │     Basic auth                        │     Basic (encrypted fields)
  │                                       │
  ├─ elif api_key                          ├─ auth_type == "bearer"
  │     Bearer token                      │     Bearer (encrypted field)
  │                                       │
  └─ else                                 ├─ auth_type == "jwt"
       no auth header                     │     JWT (HS256)
       (silent, no error)                 │       consumer_key → iss/sub
                                          │       consumer_secret → sign
                                          │       cached, refresh 60s
                                          │       before expiry
                                          │
                                          └─ auth_type == "none"
                                               explicit no-auth
                                               (validated on save)

Credentials: plain text fields          Credentials: Password fieldtype
             visible in UI                           encrypted at rest
                                                     masked in UI
```

---

### Consumer group — correct vs incorrect setup

```
CORRECT — shared group, parallel processing
───────────────────────────────────────────
Topic: orders  (4 partitions)
Consumer Group: orders-consumers  (all 4 consumers share this)

partition 0 ──► consumer-001  ──► API call  (independent)
partition 1 ──► consumer-002  ──► API call  (independent)
partition 2 ──► consumer-003  ──► API call  (independent)
partition 3 ──► consumer-004  ──► API call  (independent)

Each message processed once. Throughput = 4x single consumer.


INCORRECT — blank group, duplicate processing
─────────────────────────────────────────────
Topic: orders  (4 partitions)
consumer-001  group: a1b2-uuid  → gets ALL 4 partitions
consumer-002  group: c3d4-uuid  → gets ALL 4 partitions
consumer-003  group: e5f6-uuid  → gets ALL 4 partitions
consumer-004  group: g7h8-uuid  → gets ALL 4 partitions

Every message processed 4 times. API called 4x per message.
Adding more consumers makes duplication worse.
```
