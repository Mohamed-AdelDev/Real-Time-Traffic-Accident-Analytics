# Real-Time Traffic Accident Analytics

## 1. Project Overview

Real-Time Traffic Accident Analytics is a Big Data project designed to process and analyze traffic accident data using a real-time streaming architecture.

The system generates synthetic traffic accident events from a prepared dataset, streams them through Apache Kafka, processes and validates them using Apache NiFi, and performs real-time transformations and analytics using Apache Spark Structured Streaming.

The project analyzes accident severity, locations, weather conditions, road conditions, contributing factors, vehicles, and casualties. The processed datasets are stored in Parquet format and loaded into PostgreSQL for relational data storage and further analysis.

## 2. System Architecture

```text
Traffic Accident Dataset
         |
         v
Python Data Generator
         |
         v
Apache Kafka
(raw-accidents)
         |
         v
Apache NiFi
(Validation and ETL)
         |
         v
Apache Kafka
(processed-accidents)
         |
         v
Apache Spark Structured Streaming
         |
         v
Data Transformations and Analytics
         |
         v
Parquet Output
         |
         v
PostgreSQL Database
(17 Tables)
```

**Note:** PostgreSQL integration currently uses batch loading from the final Parquet outputs, not continuous streaming directly from Spark.

## 3. Technologies Used

- Python 3.11
- Apache Kafka 4.2.2
- Apache NiFi 2.12.0
- Apache Spark 4.3.0
- PySpark
- Pandas
- NumPy
- Docker
- Parquet
- PostgreSQL
- JDBC

## 4. Dataset

The project uses a prepared sample containing 7,000 traffic accidents.

The dataset consists of:

| File | Records |
|---|---:|
| Accidents.csv | 7,000 |
| Vehicles.csv | 12,590 |
| Casualties.csv | 11,676 |
| data_dictionary.csv | Dataset documentation |

The sample dataset is stored in:

`data/sample_7000/`

## 5. Project Structure

```text
Traffic-Accident-Analytics/
│
├── data/
│   ├── sample_7000/
│   └── spark_final_output/
│
├── spark_streaming/
│   ├── config.py
│   ├── schemas.py
│   ├── transformations.py
│   ├── streaming_app.py
│   ├── consolidate_outputs.py
│   ├── rebuild_missing_analytics.py
│   ├── test_batch_output.py
│   └── test_transformations.py
│
├── traffic_accident_generator/
│   ├── config.py
│   ├── event_builder_example.py
│   ├── generate_traffic_data.py
│   ├── generators.py
│   ├── reporting.py
│   ├── validation.py
│   └── requirements.txt
│
├── postgresql/
│   └── load_to_postgres.py
│
├── kafka_producer.py
├── create_sample_7000.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 6. Installation

### Prerequisites

- Python 3.11
- Java 21
- Docker Desktop
- Apache NiFi 2.12.0
- PostgreSQL
- Hadoop Windows utilities (`winutils.exe`) for local Spark execution on Windows.

### Install Python Dependencies

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Start Kafka

```powershell
docker compose up -d
```

## 7. Kafka Configuration

The project uses two Kafka topics:

- `raw-accidents`: Receives generated accident events.
- `processed-accidents`: Receives validated and processed events from NiFi.

Kafka runs locally on port `9092`.

## 8. Apache NiFi Workflow

The NiFi workflow performs the following operations:

1. ConsumeKafka: Reads raw accident events.
2. ValidateJson: Validates incoming JSON records.
3. UpdateAttribute: Adds validation metadata.
4. UpdateRecord: Cleans timestamp fields.
5. MergeRecord: Groups events into batches.
6. SplitRecord: Splits batches into individual events.
7. PublishKafka: Sends processed events to Kafka.

Error-handling connections are configured for ingestion, validation, cleaning, grouping, and publishing errors.

## 9. Python Kafka Producer

The producer reads events from the prepared sample dataset and publishes them to `raw-accidents`.

Example:

```powershell
python kafka_producer.py --count 100 --delay 3
```

Available arguments:

| Argument | Description | Default |
|---|---|---|
| `--folder` | Dataset directory | `data/sample_7000` |
| `--delay` | Delay between events in seconds | `3` |
| `--count` | Maximum number of events | `5` |

**Important:** Run the producer only when starting a fresh ingestion run. Running it again sends events from the beginning and can create duplicates.

## 10. Apache Spark Structured Streaming

Spark consumes events from `processed-accidents` and applies transformations and analytics.

Main configuration:

| Setting | Value |
|---|---|
| Kafka Bootstrap Servers | localhost:9092 |
| Kafka Topic | processed-accidents |
| Spark Master | local[2] |
| Starting Offsets | latest |
| Maximum Offsets Per Trigger | 100 |
| Trigger Interval | 10 seconds |

Start Spark using:

```powershell
python -m spark_streaming.streaming_app
```

Start Spark before generating new events so it can receive the incoming stream when using the `latest` starting offset.

## 11. Data Transformations

The project implements both:

- **Narrow Transformations:** Operations that do not require data shuffling between partitions.
- **Wide Transformations:** Operations that require data shuffling, such as aggregations and joins.

The transformation logic is implemented in:

`spark_streaming/transformations.py`

## 12. Analytics Datasets

The project generates 17 datasets:

| # | Dataset |
|---:|---|
| 1 | accidents |
| 2 | vehicles |
| 3 | casualties |
| 4 | accident_analytics |
| 5 | vehicle_analytics |
| 6 | casualty_analytics |
| 7 | accident_vehicle_analytics |
| 8 | cause_analytics |
| 9 | time_analytics |
| 10 | geographic_analytics |
| 11 | weather_analytics |
| 12 | road_analytics |
| 13 | driver_behavior_analytics |
| 14 | speeding_analytics |
| 15 | vehicle_damage_analytics |
| 16 | casualty_demographics_analytics |
| 17 | risk_analytics |

## 13. Output Storage

Spark stores the processed datasets in Parquet format.

Main output directories:

- `data/spark_output/`: Streaming batch outputs.
- `data/spark_final_output/`: Consolidated analytics outputs.

The verified final output contains all 17 datasets.

## 14. PostgreSQL Integration

The project integrates PostgreSQL as a relational database for storing the final processed analytics datasets.

Apache Spark reads the 17 Parquet datasets from `data/spark_final_output/` and loads them into the PostgreSQL database using JDBC.

### Database Configuration

| Setting | Value |
|---|---|
| Database | `traffic_accidents` |
| Host | `localhost` |
| Port | `5432` |
| JDBC Driver | PostgreSQL JDBC 42.7.7 |
| Loading Method | Batch Loading |
| Number of Tables | 17 |

### Environment Variables

The database connection uses the following environment variables:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Database credentials are not stored in the source code.

### Run the PostgreSQL Loader

```powershell
python .\postgresql\load_to_postgres.py
```

The loader reads each Parquet dataset and writes it to its corresponding PostgreSQL table.

It uses overwrite mode, replacing existing table data during each execution.

**Important:** PostgreSQL integration currently uses batch loading from Spark Parquet outputs. It is not a continuous streaming connection.

## 15. Testing and Validation

The project includes:

- `test_transformations.py`: Tests transformation logic.
- `test_batch_output.py`: Tests Parquet batch output.
- `rebuild_missing_analytics.py`: Reconstructs analytics datasets from consolidated accident data when required.
- `consolidate_outputs.py`: Consolidates streaming batch outputs.

## 16. Current Project Results

The final processed data contains:

| Dataset | Records |
|---|---:|
| Accidents | 7,000 |
| Vehicles | 12,590 |
| Casualties | 11,676 |

All 17 final datasets were verified successfully.

All 17 datasets were also loaded into PostgreSQL and their table row counts were verified.

## 17. Future Enhancements

- Continuous streaming integration with PostgreSQL.
- Interactive dashboards using Streamlit.
- Advanced traffic accident prediction.
- Geographic risk visualization.
- Additional real-time monitoring and alerting.
