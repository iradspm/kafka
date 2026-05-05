# Kafka Mediator

Kafka Mediator is a Frappe-based application that simplifies integration with Apache Kafka for producing and consuming messages. It provides a user interface for managing Kafka clusters, topics, producers, and consumers, along with a REST API for external applications to publish messages and create consumers.

## Features
- Manage Kafka clusters, topics, producers, and consumers via Frappe UI.
- Create and scale Kafka consumers with automatic lag monitoring.
- Publish messages to Kafka topics via REST API.
- Process consumed messages by calling external APIs.
- Support for secure Kafka connections (SASL/SSL).

## Prerequisites
- **Frappe Framework**: Version 14 or higher.
- **Python**: 3.10 or higher.
- **Apache Kafka**: Accessible cluster with appropriate permissions.
- **Redis**: For Frappe's background jobs and caching.
- **MariaDB/PostgreSQL**: For Frappe's database.
- **Dependencies**: `confluent-kafka`, `requests`.

## Installation

### 1. Set Up Frappe Environment
Ensure you have a Frappe bench installed. If not, follow these steps:

```bash
# Install bench
pip install frappe-bench

# Initialize a new bench
bench init frappe-bench
cd frappe-bench

# Create a new site
bench new-site site1.local
```

### 2. Install Kafka Mediator App
Add the Kafka Mediator app to your Frappe bench:

```bash
# Get the app from the repository
bench get-app kafka_mediator https://github.com/tiberbu/kafka_mediator.git

# Install the app on your site
bench --site site1.local install-app kafka_mediator
```

### 3. Install Python Dependencies
Install the required Python libraries:

```bash
bench --site site1.local pip install confluent-kafka requests
```

### 4. Configure Kafka Cluster
1. Log in to the Frappe UI as an Administrator.
2. Navigate to **Kafka Cluster** doctype and create a new cluster:
   - **Bootstrap Servers**: e.g., `kafka1.example.com:9092,kafka2.example.com:9092`.
   - **Security Protocol**: `PLAINTEXT`, `SASL_PLAINTEXT`, or `SASL_SSL`.
   - **SASL Mechanism**: `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` (if applicable).
   - **SASL Username** and **SASL Password**: Credentials for Kafka authentication.
3. Save and validate the connection (status should be "Connected").

### 5. Create Kafka Topics
1. Go to **Kafka Topic** doctype and create a new topic:
   - **Cluster**: Select the configured Kafka cluster.
   - **Topic Name**: e.g., `test-topic`.
   - **Number of Partitions**: Default is 1.
   - **Replication Factor**: Default is 1.
2. Save to create the topic in Kafka.

### 6. Start Frappe Server
Run the Frappe development server or configure for production:

```bash
# Development
bench start

# Production (using Supervisor and Nginx)
bench setup production frappe
```

## API Documentation

The Kafka Mediator provides a REST API for external applications to create consumers and publish messages. All API endpoints are accessible via HTTP POST and require no authentication unless specified by your Frappe setup.

### Base URL
```
http://your-frappe-site/api/method/kafka_mediator
```

### 1. Create Consumer
Creates a Kafka consumer that subscribes to a topic, processes messages, and optionally publishes responses to another topic.

**Endpoint**: `/create_consumer`

**Method**: POST

**Parameters** (JSON body):
| Parameter            | Type    | Required | Description                                                                 |
|----------------------|---------|----------|-----------------------------------------------------------------------------|
| `cluster`            | String  | Yes      | Name of the Kafka Cluster doctype (e.g., "test-cluster").                   |
| `topic`              | String  | Yes      | Name of the Kafka Topic to consume (e.g., "test-topic").                    |
| `consumer_group`     | String  | No       | Consumer group ID. Auto-generated if not provided.                          |
| `api_endpoint`       | String  | Yes      | URL to POST consumed messages (e.g., "https://api.example.com/process").     |
| `api_username`       | String  | No       | Username for Basic Auth at the API endpoint.                                |
| `api_password`       | String  | No       | Password for Basic Auth at the API endpoint.                                |
| `api_key`            | String  | No       | Bearer token for API endpoint authentication.                               |
| `output_topic`       | String  | No       | Kafka topic to publish API responses (e.g., "output-topic").                |
| `output_producer`    | String  | No       | Name of the Kafka Producer doctype for publishing responses.                |
| `max_lag_threshold`  | Integer | No       | Maximum consumer lag before scaling (default: 1000).                        |
| `max_consumers`      | Integer | No       | Maximum number of consumers for scaling (default: 5).                       |

**Example Request**:
```bash
curl -X POST http://your-frappe-site/api/method/kafka_mediator.create_consumer \
-H "Content-Type: application/json" \
-d '{
    "cluster": "test-cluster",
    "topic": "test-topic",
    "consumer_group": "test-group",
    "api_endpoint": "https://api.example.com/process",
    "api_username": "user",
    "api_password": "pass",
    "output_topic": "output-topic",
    "max_lag_threshold": 1000,
    "max_consumers": 5
}'
```

**Example Response**:
```json
{
    "consumer_id": "test-topic-test-group-001",
    "status": "success"
}
```

**Error Response**:
```json
{
    "status": "error",
    "message": "Failed to create consumer: Topic test-topic does not exist"
}
```

### 2. Publish Message
Publishes a message to a Kafka topic using a configured producer.

**Endpoint**: `/publish_message`

**Method**: POST

**Parameters** (JSON body):
| Parameter      | Type   | Required | Description                                    |
|----------------|--------|----------|------------------------------------------------|
| `topic_name`   | String | Yes      | Name of the Kafka Topic created in previous API call. |
| `message`      | String | Yes      | Message to publish to the topic.               |

**Example Request**:
```bash
curl -X POST http://your-frappe-site/api/method/kafka_mediator.publish_message \
-H "Content-Type: application/json" \
-d '{
    "topic_name": "test_topic",
    "message": "Hello, Kafka!"
}'
```

**Example Response**:
```json
{
    "status": "success"
}
```

**Error Response**:
```json
{
    "status": "error",
    "message": "Failed to publish message: xxxx"
}
```

---

## Partition Reassignment API

Partition reassignment moves topic partitions between brokers — useful when adding/removing brokers or rebalancing load. These endpoints use the Kafka Admin protocol directly (no CLI tools required).

### Base URL
```
http://your-frappe-site/api/method/kafka_mediator.api.partition
```

---

### 1. Reassign Partitions

Initiates a partition reassignment for a topic.

**Endpoint**: `POST /reassign_partitions`

**Parameters**:
| Parameter | Type | Required | Description |
|---|---|---|---|
| `topic_name` | String | Yes | Name of the topic to reassign. |
| `assignments` | Array | Yes | List of `{partition, replicas}` objects (see below). |
| `timeout_ms` | Integer | No | Broker-side timeout in ms (default: `30000`). |

**`assignments` object**:
| Field | Type | Description |
|---|---|---|
| `partition` | Integer | Partition index (0-based). |
| `replicas` | Array of integers | Ordered list of broker IDs. The **first broker is the preferred leader**. |

**Example — move 2 partitions across 3 brokers:**
```bash
curl -X POST http://your-frappe-site/api/method/kafka_mediator.api.partition.reassign_partitions \
-H "Content-Type: application/json" \
-d '{
    "topic_name": "clinical-bundle-entry",
    "assignments": [
        {"partition": 0, "replicas": [1, 2, 3]},
        {"partition": 1, "replicas": [2, 3, 1]}
    ]
}'
```

> **What the replicas list means:**
> `[1, 2, 3]` → partition lives on brokers 1, 2, and 3. Broker **1** is the preferred leader (serves reads/writes); brokers 2 and 3 hold follower replicas. The list length determines the effective replication factor for that partition.

**Responses**:

`200 OK` — reassignment accepted by the broker:
```json
{
    "status": "success",
    "message": "Partition reassignment initiated for 'clinical-bundle-entry'.",
    "data": {
        "topic_name": "clinical-bundle-entry",
        "partitions_reassigned": 2
    }
}
```

`207 Multi-Status` — some partitions failed:
```json
{
    "status": "error",
    "message": "Reassignment partially failed.",
    "data": {
        "errors": {
            "clinical-bundle-entry[0]": "The manual partition assignment includes broker 2, but no such broker is registered."
        }
    }
}
```

> **Single-broker clusters**: A cluster with only one broker (id=`1`) cannot hold replicas on brokers 2 or 3. Use `"replicas": [1]` and a replication factor of 1.

---

### 2. Get Reassignment Status

Lists all in-progress partition reassignments. Scope by topic or by cluster.

**Endpoint**: `GET /get_reassignment_status`

**Parameters** (query string):
| Parameter | Type | Required | Description |
|---|---|---|---|
| `topic_name` | String | One of these | Scope to a specific topic. |
| `cluster_name` | String | One of these | Scope to all topics on a cluster. |
| `timeout_ms` | Integer | No | Broker-side timeout in ms (default: `30000`). |

**Example**:
```bash
curl "http://your-frappe-site/api/method/kafka_mediator.api.partition.get_reassignment_status?topic_name=clinical-bundle-entry"
```

**Response**:
```json
{
    "status": "success",
    "message": "1 ongoing reassignment(s).",
    "data": [
        {
            "topic": "clinical-bundle-entry",
            "partition": 0,
            "replicas": [1, 2, 3],
            "adding_replicas": [2, 3],
            "removing_replicas": []
        }
    ]
}
```

An empty `data` array means no reassignments are in progress.

---

### 3. Cancel Reassignment

Cancels an in-progress reassignment by re-submitting the partition's **current** replica assignment, signalling Kafka to abort the move.

**Endpoint**: `POST /cancel_reassignment`

**Parameters**:
| Parameter | Type | Required | Description |
|---|---|---|---|
| `topic_name` | String | Yes | Topic whose reassignment to cancel. |
| `partitions` | Array of integers | Yes | Partition indexes to cancel. |
| `timeout_ms` | Integer | No | Broker-side timeout in ms (default: `30000`). |

**Example**:
```bash
curl -X POST http://your-frappe-site/api/method/kafka_mediator.api.partition.cancel_reassignment \
-H "Content-Type: application/json" \
-d '{
    "topic_name": "clinical-bundle-entry",
    "partitions": [0, 1]
}'
```

**Response**:
```json
{
    "status": "success",
    "message": "Reassignment cancellation sent for 2 partition(s) on 'clinical-bundle-entry'.",
    "data": {
        "topic_name": "clinical-bundle-entry",
        "partitions": [0, 1]
    }
}
```

---

### Throttling

To limit the network bandwidth used during reassignment (recommended in production), pass a `throttle` value in bytes per second to `reassign_partitions`. Note: throttle enforcement requires the `kafka-reassign-partitions.sh` CLI tool and is not applied by the API itself — set it directly on the broker via:

```bash
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-default \
  --alter --add-config leader.replication.throttled.rate=50000000,follower.replication.throttled.rate=50000000
```

---

## Usage Notes
- **Consumer Scaling**: Consumers automatically scale based on lag, up to `max_consumers`. Ensure the topic has enough partitions to support multiple consumers.
- **API Endpoint**: The `api_endpoint` should accept POST requests with a JSON body: `{"message": "<kafka_message>"}`. Responses are published to `output_topic` if specified.
- **Security**: Configure Frappe with appropriate user roles and API keys for secure access. By default, the API endpoints are open (`allow_guest=True`).
- **Monitoring**: Use the **Kafka Consumer** doctype to view current lag and status.

## Troubleshooting
- **Consumer Thread Errors**: Check Frappe error logs (via UI or `bench logs`) for titles like "Kafka Consumer Initialization Error" or "Kafka Consumer Processing Error".
- **Kafka Connection Issues**: Verify `bootstrap_servers`, `security_protocol`, `sasl_mechanism`, `sasl_username`, and `sasl_password` in the Kafka Cluster doctype.
- **Topic Not Found**: Ensure topics exist in Kafka (`kafka-topics.sh --list --bootstrap-server <server>`).
- **Permissions**: Confirm Kafka users have `PRODUCE`, `CONSUME`, and `DESCRIBE` permissions for topics and consumer groups.
- **Frappe Context**: If threads fail due to context issues, ensure the site name is correctly set in `site_config.json`.

## Contributing
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/new-feature`).
3. Commit changes (`git commit -m "Add new feature"`).
4. Push to the branch (`git push origin feature/new-feature`).
5. Open a pull request.

## Contact
For support, open an issue on the [GitHub repository](https://github.com/tiberbu/kafka_mediator) or contact the maintainer at thomas@tiberbu.com.