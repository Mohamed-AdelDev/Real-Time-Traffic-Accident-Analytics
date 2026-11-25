import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json

from pyspark.sql import SparkSession, functions as F

from traffic_accident_generator.event_builder_example import iter_events
from spark_streaming.schemas import EVENT_SCHEMA
from spark_streaming.transformations import (
    apply_narrow_transformations,
    build_analytics,
)

event = next(iter_events("data/sample_7000"))

spark = (
    SparkSession.builder
    .appName("TrafficTransformationsTest")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

try:
    raw_df = spark.createDataFrame(
        [(json.dumps(event),)],
        ["json_value"]
    )

    parsed_df = (
        raw_df
        .select(F.from_json("json_value", EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    accidents_df = apply_narrow_transformations(parsed_df)

    print("\n========== PARSED ACCIDENT ==========")
    accidents_df.show(truncate=False)

    print("\n========== ANALYTICS ==========")
    analytics = build_analytics(accidents_df)

    for name, dataframe in analytics.items():
        print(f"\n--- {name} ---")
        dataframe.show(truncate=False)

    print("\nTRANSFORMATION RUNTIME TEST SUCCESS")

finally:
    spark.stop()
