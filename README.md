\# Real-Time Traffic Accident Analytics



\## 1. Project Overview



Real-Time Traffic Accident Analytics is a Big Data project designed to process and analyze traffic accident data using a real-time streaming architecture.



The system generates synthetic traffic accident events from a prepared dataset, streams them through Apache Kafka, processes and validates them using Apache NiFi, and performs real-time transformations and analytics using Apache Spark Structured Streaming.



The project analyzes accident severity, locations, weather conditions, road conditions, contributing factors, vehicles, and casualties.



\## 2. System Architecture



```text

Traffic Accident Dataset

&#x20;         |

&#x20;         v

Python Data Generator

&#x20;         |

&#x20;         v

Apache Kafka

(raw-accidents)

&#x20;         |

&#x20;         v

Apache NiFi

(Validation and ETL)

&#x20;         |

&#x20;         v

Apache Kafka

(processed-accidents)

&#x20;         |

&#x20;         v

Apache Spark Structured Streaming

&#x20;         |

&#x20;         v

Data Transformations and Analytics

&#x20;         |

&#x20;         v

Parquet Output

```



\## 3. Technologies Used



\- Python 3.11

\- Apache Kafka 4.2.2

\- Apache NiFi 2.12.0

\- Apache Spark 4.3.0

\- PySpark

\- Pandas

\- NumPy

\- Docker

\- Parquet



\## 4. Dataset



The project uses a prepared sample containing 7,000 traffic accidents.



The dataset consists of:



| File | Records |

|---|---:|

| Accidents.csv | 7,000 |

| Vehicles.csv | 12,590 |

| Casualties.csv | 11,676 |

| data\_dictionary.csv | Dataset documentation |



The sample dataset is stored in:



`data/sample\_7000/`



\## 5. Project Structure



```text

Traffic-Accident-Analytics/

│

├── data/

│   ├── sample\_7000/

│   └── spark\_final\_output/

│

├── spark\_streaming/

│   ├── config.py

│   ├── schemas.py

│   ├── transformations.py

│   ├── streaming\_app.py

│   ├── consolidate\_outputs.py

│   ├── rebuild\_missing\_analytics.py

│   ├── test\_batch\_output.py

│   └── test\_transformations.py

│

├── traffic\_accident\_generator/

│   ├── config.py

│   ├── event\_builder\_example.py

│   ├── generate\_traffic\_data.py

│   ├── generators.py

│   ├── reporting.py

│   ├── validation.py

│   └── requirements.txt

│

├── kafka\_producer.py

├── create\_sample\_7000.py

├── docker-compose.yml

├── requirements.txt

└── README.md

```



\## 6. Installation



\### Prerequisites



\- Python 3.11

\- Java 21

\- Docker Desktop

\- Apache NiFi 2.12.0

\- Hadoop Windows utilities (`winutils.exe`) for local Spark execution on Windows.



\### Install Python Dependencies



```powershell

python -m venv .venv

.venv\\Scripts\\Activate.ps1

pip install -r requirements.txt

```



\### Start Kafka



```powershell

docker compose up -d

```



\## 7. Kafka Configuration



The project uses two Kafka topics:



\- `raw-accidents`: Receives generated accident events.

\- `processed-accidents`: Receives validated and processed events from NiFi.



Kafka runs locally on port `9092`.



\## 8. Apache NiFi Workflow



The NiFi workflow performs the following operations:



1\. ConsumeKafka: Reads raw accident events.

2\. ValidateJson: Validates incoming JSON records.

3\. UpdateAttribute: Adds validation metadata.

4\. UpdateRecord: Cleans timestamp fields.

5\. MergeRecord: Groups events into batches.

6\. SplitRecord: Splits batches into individual events.

7\. PublishKafka: Sends processed events to Kafka.



Error-handling connections are configured for ingestion, validation, cleaning, grouping, and publishing errors.



\## 9. Python Kafka Producer



The producer reads events from the prepared sample dataset and publishes them to `raw-accidents`.



Example:



```powershell

python kafka\_producer.py --count 100 --delay 3

```



Available arguments:



| Argument | Description | Default |

|---|---|---|

| `--folder` | Dataset directory | `data/sample\_7000` |

| `--delay` | Delay between events in seconds | `3` |

| `--count` | Maximum number of events | `5` |



\*\*Important:\*\* Run the producer only when starting a fresh ingestion run. Running it again sends events from the beginning and can create duplicates.



\## 10. Apache Spark Structured Streaming



Spark consumes events from `processed-accidents` and applies transformations and analytics.



Main configuration:



| Setting | Value |

|---|---|

| Kafka Bootstrap Servers | localhost:9092 |

| Kafka Topic | processed-accidents |

| Spark Master | local\[2] |

| Starting Offsets | latest |

| Maximum Offsets Per Trigger | 100 |

| Trigger Interval | 10 seconds |



Start Spark using:



```powershell

python -m spark\_streaming.streaming\_app

```



Start Spark before generating new events so it can receive the incoming stream when using the `latest` starting offset.



\## 11. Data Transformations



The project implements both:



\- \*\*Narrow Transformations:\*\* Operations that do not require data shuffling between partitions.

\- \*\*Wide Transformations:\*\* Operations that require data shuffling, such as aggregations and joins.



The transformation logic is implemented in:



`spark\_streaming/transformations.py`



\## 12. Analytics Datasets



The project generates 17 datasets:



| # | Dataset |

|---:|---|

| 1 | accidents |

| 2 | vehicles |

| 3 | casualties |

| 4 | accident\_analytics |

| 5 | vehicle\_analytics |

| 6 | casualty\_analytics |

| 7 | accident\_vehicle\_analytics |

| 8 | cause\_analytics |

| 9 | time\_analytics |

| 10 | geographic\_analytics |

| 11 | weather\_analytics |

| 12 | road\_analytics |

| 13 | driver\_behavior\_analytics |

| 14 | speeding\_analytics |

| 15 | vehicle\_damage\_analytics |

| 16 | casualty\_demographics\_analytics |

| 17 | risk\_analytics |



\## 13. Output Storage



Spark stores the processed datasets in Parquet format.



Main output directories:



\- `data/spark\_output/`: Streaming batch outputs.

\- `data/spark\_final\_output/`: Consolidated analytics outputs.



The verified final output contains all 17 datasets.



\## 14. Testing and Validation



The project includes:



\- `test\_transformations.py`: Tests transformation logic.

\- `test\_batch\_output.py`: Tests Parquet batch output.

\- `rebuild\_missing\_analytics.py`: Reconstructs analytics datasets from consolidated accident data when required.

\- `consolidate\_outputs.py`: Consolidates streaming batch outputs.



\## 15. Current Project Results



The final processed data contains:



| Dataset | Records |

|---|---:|

| Accidents | 7,000 |

| Vehicles | 12,590 |

| Casualties | 11,676 |



All 17 final datasets were verified successfully.



\## 16. Future Enhancements



\- Database integration.

\- Interactive dashboards using Power BI or Streamlit.

\- Advanced traffic accident prediction.

\- Geographic risk visualization.

\- Additional real-time monitoring and alerting.
