from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from spark_streaming.config import (
    APP_NAME,
    SPARK_MASTER,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    STARTING_OFFSETS,
    MAX_OFFSETS_PER_TRIGGER,
    TRIGGER_INTERVAL,
    CHECKPOINT_LOCATION,
)

from spark_streaming.schemas import EVENT_SCHEMA

from spark_streaming.transformations import (
    apply_narrow_transformations,
    build_vehicle_dataframe,
    build_casualty_dataframe,
    build_analytics,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = PROJECT_ROOT / "data" / "spark_output"


def save_batch(dataframe, dataset_name, batch_id):
    batch_path = OUTPUT_ROOT / dataset_name / f"batch_id={batch_id}"

    (
        dataframe.write
        .mode("overwrite")
        .parquet(str(batch_path))
    )


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no records to process.")
        return

    print(f"Processing batch {batch_id}...")

    accidents_df = apply_narrow_transformations(batch_df)

    vehicles_df = build_vehicle_dataframe(accidents_df)
    casualties_df = build_casualty_dataframe(accidents_df)

    analytics = build_analytics(accidents_df)

    save_batch(accidents_df, "accidents", batch_id)
    save_batch(vehicles_df, "vehicles", batch_id)
    save_batch(casualties_df, "casualties", batch_id)

    for name, dataframe in analytics.items():
        save_batch(dataframe, name, batch_id)

    print(f"Batch {batch_id} completed successfully.")


def main():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", STARTING_OFFSETS)
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
            .load()
        )

        parsed_df = (
            kafka_df
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    EVENT_SCHEMA
                ).alias("event")
            )
            .select("event.*")
        )

        query = (
            parsed_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )

        print("Spark Structured Streaming started.")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print(f"Output directory: {OUTPUT_ROOT}")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
