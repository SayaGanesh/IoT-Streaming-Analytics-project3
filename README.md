# Real-Time IoT Streaming Analytics Solution on Azure

## Overview

An end-to-end **Real-Time IoT Streaming Analytics platform** that ingests high-frequency sensor telemetry from industrial IoT devices, processes it with structured streaming in Azure Databricks, detects anomalies in near real-time, and stores results for both operational alerting and historical analytics.

Use case: Factory floor equipment monitoring — temperature, vibration, pressure sensors on manufacturing machines.

---

## Architecture

```
IoT Devices (MQTT)
       │
       ▼
Azure IoT Hub (Device Ingestion)
       │
       ▼
Azure Event Hubs (Kafka-compatible endpoint)
       │
       ▼
Azure Databricks – Structured Streaming
   ├── Watermarking + Late Data Handling
   ├── Sliding Window Aggregations
   └── Anomaly Detection (Z-score per device)
       │
       ├──► ADLS Gen2 – Delta Lake (historical store)
       │
       └──► Azure Cosmos DB (hot path – real-time alerts)
                │
                ▼
         Power BI Streaming Dataset / Azure Monitor Alerts
```

---

## Tech Stack

| Component | Tool |
|---|---|
| Device Ingestion | Azure IoT Hub |
| Message Bus | Azure Event Hubs (Kafka endpoint) |
| Stream Processing | Azure Databricks Structured Streaming |
| Hot Path Store | Azure Cosmos DB (alerts) |
| Cold Path Store | ADLS Gen2 + Delta Lake |
| Anomaly Detection | Z-score with rolling mean/stddev |
| Language | Python 3.10, PySpark |

---

## Project Structure

```
📁 data/              → Sample IoT sensor payloads (JSON)
📁 notebooks/         → Streaming ingestion, processing, anomaly detection
📁 sql/               → Historical analytics queries on Delta tables
📄 README.md
📄 requirements.txt
```

---

## Streaming Pipeline Detail

### Stage 1 – Event Hub Ingestion
- Reads from Event Hubs using Kafka protocol
- Deserializes JSON payload from binary message body
- Adds watermark of 10 minutes for late arrival tolerance

### Stage 2 – Windowed Aggregations
- 5-minute sliding windows with 1-minute slide
- Metrics per device per window: avg/max/min temp, avg vibration, avg pressure
- Written to Delta Lake in append mode

### Stage 3 – Anomaly Detection
- Per-device rolling mean and stddev computed over 1-hour lookback
- Z-score > 3.0 flagged as anomaly
- Anomalous windows written to Cosmos DB for alerting

### Stage 4 – Historical Delta Store
- All windows persisted to ADLS Gen2 as Delta table
- Enables time-travel queries and trend analysis

---

## Anomaly Detection Logic

```
z_score = (current_avg_temp - rolling_mean_temp) / rolling_stddev_temp

if z_score > 3.0 → flag as ANOMALY
```

---

## Key Learnings

- Watermarking strategy for late IoT data (10-minute tolerance)
- Stateful streaming with `mapGroupsWithState` for per-device metrics
- Exactly-once semantics with Delta Lake checkpointing
- Hot/cold path architecture: Cosmos DB for ops, Delta for analytics
- Handling schema evolution in Event Hub payloads
