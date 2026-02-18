# Running the Heartbeat Application

## Option 1: Docker (Recommended)

The application is designed to run in a containerized environment using Docker Compose. This ensures all dependencies (Kafka, Zookeeper, PostgreSQL) are correctly configured and connected.

### Prerequisites
- Docker
- Docker Compose

### Start the Application
To start all services:
```bash
docker-compose up -d --build
```
This will start:
-   Zookeeper & Kafka (Messaging)
-   PostgreSQL (Database)
-   Producer (Data Generator)
-   Consumer (Data Ingester)
-   Dashboard (Frontend) - Access at http://localhost:8501
-   Kafka UI (Monitoring) - Access at http://localhost:8080

### Troubleshooting: "heartbeat_records does not exist"
If you see errors in the consumer logs about missing tables (`relation "heartbeat_records" does not exist`), it means the database volume was initialized before the schema script ran.

**Fix:** Reset the database volume to force schema initialization:
```bash
docker-compose down -v
docker-compose up -d --build
```

---

## Option 2: Local Execution (Advanced)

Running locally requires manually setting up the infrastructure and installing Python dependencies.

### Prerequisites
- Python 3.11+
- Local Kafka & Zookeeper instances running
- Local PostgreSQL instance running (with database `heartbeat_db` and schema loaded)

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run Dashboard
```bash
streamlit run Dashboard.py
```
*Note: You must set environment variables (DB_HOST, DB_USER, etc.) to point to your local services.*
