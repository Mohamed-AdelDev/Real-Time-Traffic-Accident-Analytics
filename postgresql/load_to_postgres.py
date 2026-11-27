
import os
from pathlib import Path

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "spark_final_output"

TABLES = [
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
    required = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]

    if any(not os.getenv(key) for key in required):
        raise EnvironmentError("PostgreSQL environment variables are missing.")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TrafficAccidentsPostgreSQLLoader")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.7"
        )
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:postgresql://{os.environ['PGHOST']}:"
        f"{os.environ['PGPORT']}/{os.environ['PGDATABASE']}"
    )

    properties = {
        "user": os.environ["PGUSER"],
        "password": os.environ["PGPASSWORD"],
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
    }

    try:
        for table in TABLES:
            path = DATA_DIR / table

            if not path.exists():
                raise FileNotFoundError(
                    f"Dataset not found: {path}"
                )

            print(f"\nLoading: {table}")

            df = spark.read.parquet(str(path))

            # Nested arrays are stored in separate tables.
            if table == "accidents":
                df = df.drop("vehicles", "casualties")

            count = df.count()

            (
                df.coalesce(1)
                .write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table)
                .options(**properties)
                .mode("overwrite")
                .save()
            )

            print(f"SUCCESS: {table} ({count} rows)")

        print("\nALL 17 DATASETS LOADED SUCCESSFULLY.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
