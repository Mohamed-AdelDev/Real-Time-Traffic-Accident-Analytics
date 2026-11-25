
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_ROOT = PROJECT_ROOT / "data" / "spark_output"
OUTPUT_ROOT = PROJECT_ROOT / "data" / "spark_final_output"

DATASETS = [
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


def main():
    spark = (
        SparkSession.builder
        .appName("TrafficAccidentOutputConsolidation")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        if not INPUT_ROOT.exists():
            raise FileNotFoundError(
                f"Spark output directory not found: {INPUT_ROOT}"
            )

        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

        for dataset in DATASETS:
            dataset_input = INPUT_ROOT / dataset

            if not dataset_input.exists():
                print(f"SKIPPED: {dataset} - input directory not found")
                continue

            batch_paths = sorted(
                path for path in dataset_input.glob("batch_id=*")
                if path.is_dir()
            )

            if not batch_paths:
                print(f"SKIPPED: {dataset} - no batch folders found")
                continue

            print(f"\nConsolidating {dataset}...")
            dataframe = spark.read.parquet(
                *(str(path) for path in batch_paths)
            )

            output_path = OUTPUT_ROOT / dataset

            (
                dataframe.write
                .mode("overwrite")
                .parquet(str(output_path))
            )

            row_count = dataframe.count()
            print(
                f"SUCCESS: {dataset} | "
                f"Batches: {len(batch_paths)} | "
                f"Rows: {row_count}"
            )

        print("\nCONSOLIDATION COMPLETED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
