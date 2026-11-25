
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "data" / "spark_final_output" / "accidents"
OUTPUT_ROOT = PROJECT_ROOT / "data" / "spark_final_output"


def main():
    spark = (
        SparkSession.builder
        .appName("RebuildMissingTrafficAnalytics")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        accidents = spark.read.parquet(str(INPUT_PATH))

        accidents = (
            accidents
            .withColumn("accident_hour", F.hour("event_timestamp_ts"))
            .withColumn(
                "day_of_week",
                F.date_format("event_timestamp_ts", "EEEE")
            )
        )

        vehicles = (
            accidents
            .select(
                "accident_id", "city", "severity", "speed_limit",
                F.explode("vehicles").alias("vehicle")
            )
            .select(
                "accident_id", "city", "severity", "speed_limit",
                F.col("vehicle.vehicle_type").alias("vehicle_type"),
                F.col("vehicle.vehicle_speed_kmh").alias("vehicle_speed_kmh"),
                F.col("vehicle.vehicle_damage_severity").alias(
                    "vehicle_damage_severity"
                ),
            )
            .withColumn(
                "speeding_over_limit",
                F.when(
                    F.col("vehicle_speed_kmh").isNotNull()
                    & F.col("speed_limit").isNotNull(),
                    F.col("vehicle_speed_kmh") > F.col("speed_limit")
                )
            )
        )

        casualties = (
            accidents
            .select(
                "accident_id", "city",
                F.explode("casualties").alias("casualty")
            )
            .select(
                "accident_id", "city",
                F.col("casualty.age").alias("age"),
                F.col("casualty.gender").alias("gender"),
                F.col("casualty.casualty_type").alias("casualty_type"),
                F.col("casualty.injury_severity").alias("injury_severity"),
                F.col("casualty.hospitalized").alias("hospitalized"),
            )
        )

        analytics = {
            "time_analytics": (
                accidents
                .groupBy("day_of_week", "accident_hour")
                .agg(F.count("*").alias("total_accidents"))
            ),

            "geographic_analytics": (
                accidents
                .groupBy(
                    "country", "city", "area",
                    "urban_or_rural", "road_type"
                )
                .agg(
                    F.count("*").alias("total_accidents"),
                    F.sum(
                        F.when(
                            F.col("risk_category").isin("High", "Critical"), 1
                        ).otherwise(0)
                    ).alias("high_risk_accidents"),
                    F.avg("latitude").alias("average_latitude"),
                    F.avg("longitude").alias("average_longitude"),
                )
            ),

            "weather_analytics": (
                accidents
                .groupBy("weather_condition")
                .agg(
                    F.count("*").alias("total_accidents"),
                    F.avg("visibility_km").alias("average_visibility_km"),
                )
            ),

            "road_analytics": (
                accidents
                .groupBy("road_type", "road_surface_condition")
                .agg(F.count("*").alias("total_accidents"))
            ),

            "driver_behavior_analytics": (
                accidents
                .agg(
                    F.count("*").alias("total_accidents"),
                    F.sum(
                        F.when(F.col("speeding_involved") == True, 1)
                        .otherwise(0)
                    ).alias("speeding_accidents"),
                    F.sum(
                        F.when(F.col("distracted_driving") == True, 1)
                        .otherwise(0)
                    ).alias("distracted_driving_accidents"),
                    F.sum(
                        F.when(F.col("drunk_driving") == True, 1)
                        .otherwise(0)
                    ).alias("drunk_driving_accidents"),
                    F.sum(
                        F.when(F.col("weather_related") == True, 1)
                        .otherwise(0)
                    ).alias("weather_related_accidents"),
                )
            ),

            "speeding_analytics": (
                vehicles
                .groupBy("speeding_over_limit")
                .agg(F.count("*").alias("total_vehicles"))
            ),

            "vehicle_damage_analytics": (
                vehicles
                .groupBy("vehicle_type", "vehicle_damage_severity")
                .agg(F.count("*").alias("total_vehicles"))
            ),

            "casualty_demographics_analytics": (
                casualties
                .groupBy("age", "gender", "casualty_type")
                .agg(F.count("*").alias("total_casualties"))
            ),

            "risk_analytics": (
                accidents
                .groupBy("risk_category", "city")
                .agg(
                    F.count("*").alias("total_accidents"),
                    F.avg("number_of_casualties").alias(
                        "average_casualties"
                    ),
                )
            ),
        }

        for name, dataframe in analytics.items():
            output_path = OUTPUT_ROOT / name

            (
                dataframe.write
                .mode("overwrite")
                .parquet(str(output_path))
            )

            print(f"{name}: {dataframe.count()} rows - SUCCESS")

        print("\nALL 9 MISSING ANALYTICS REBUILT SUCCESSFULLY")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
