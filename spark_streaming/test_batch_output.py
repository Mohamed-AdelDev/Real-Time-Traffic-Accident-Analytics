
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import shutil

from pyspark.sql import SparkSession, functions as F

import spark_streaming.streaming_app as app
from traffic_accident_generator.event_builder_example import iter_events
from spark_streaming.schemas import EVENT_SCHEMA

project_root = Path(__file__).resolve().parent.parent
test_output = project_root / "data" / "spark_test_output"

if test_output.exists():
    shutil.rmtree(test_output)

app.OUTPUT_ROOT = test_output

spark = (
    SparkSession.builder
    .appName("TrafficBatchOutputTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

try:
    event = next(iter_events("data/sample_7000"))

    raw_df = spark.createDataFrame(
        [(json.dumps(event),)],
        ["json_value"]
    )

    batch_df = (
        raw_df
        .select(F.from_json("json_value", EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )

    app.process_batch(batch_df, 999999)

    datasets = [
        "accidents",
        "vehicles",
        "casualties",
        "accident_analytics",
        "vehicle_analytics",
        "casualty_analytics",
        "accident_vehicle_analytics",
        "cause_analytics",
        "time_analytics",
        "geographic_analytics",
        "weather_analytics",
        "road_analytics",
        "driver_behavior_analytics",
        "speeding_analytics",
        "vehicle_damage_analytics",
        "casualty_demographics_analytics",
        "risk_analytics",
    ]

    for name in datasets:
        path = test_output / name / "batch_id=999999"
        result = spark.read.parquet(str(path))
        print(f"{name}: {result.count()} rows")

    print("\nPARQUET OUTPUT TEST SUCCESS")

finally:
    spark.stop()
