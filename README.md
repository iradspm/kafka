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
| `producer_id`  | String | Yes      | Name of the Kafka Producer doctype (e.g., "prod-001"). |
| `message`      | String | Yes      | Message to publish to the topic.               |

**Example Request**:
```bash
curl -X POST http://your-frappe-site/api/method/kafka_mediator.publish_message \
-H "Content-Type: application/json" \
-d '{
    "producer_id": "prod-001",
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
    "message": "Failed to publish message: Producer prod-001 not found"
}
```

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