
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_narrow_transformations(events_df: DataFrame) -> DataFrame:
    """Filter, flatten and derive accident fields."""

    valid_events = events_df.filter(
        (F.col("event_type") == "traffic_accident")
        & F.col("accident.accident_id").isNotNull()
        & F.col("location.city").isNotNull()
        & F.col("accident.accident_severity").isNotNull()
    )

    accidents_df = valid_events.select(
        F.col("event_timestamp"),
        F.to_timestamp("event_timestamp").alias("event_timestamp_ts"),

        F.col("accident.accident_id").alias("accident_id"),
        F.col("accident.accident_date").alias("accident_date"),
        F.col("accident.accident_time").alias("accident_time"),
        F.col("accident.day_of_week").alias("day_of_week"),
        F.col("accident.month").alias("month"),
        F.col("accident.year").alias("year"),
        F.col("accident.accident_severity").alias("severity"),
        F.col("accident.number_of_vehicles").alias("number_of_vehicles"),
        F.col("accident.number_of_casualties").alias("number_of_casualties"),
        F.col("accident.traffic_conditions").alias("traffic_conditions"),
        F.col("accident.light_conditions").alias("light_conditions"),
        F.col("accident.accident_duration_minutes").alias(
            "accident_duration_minutes"
        ),

        F.col("location.country").alias("country"),
        F.col("location.city").alias("city"),
        F.col("location.area").alias("area"),
        F.col("location.latitude").alias("latitude"),
        F.col("location.longitude").alias("longitude"),
        F.col("location.road_name").alias("road_name"),
        F.col("location.road_type").alias("road_type"),
        F.col("location.urban_or_rural").alias("urban_or_rural"),
        F.col("location.junction_type").alias("junction_type"),
        F.col("location.speed_limit").alias("speed_limit"),

        F.col("weather.weather_condition").alias("weather_condition"),
        F.col("weather.temperature_celsius").alias("temperature_celsius"),
        F.col("weather.humidity_percentage").alias("humidity_percentage"),
        F.col("weather.visibility_km").alias("visibility_km"),
        F.col("weather.wind_speed_kmh").alias("wind_speed_kmh"),
        F.col("weather.precipitation_mm").alias("precipitation_mm"),

        F.col("road_conditions.road_surface_condition").alias(
            "road_surface_condition"
        ),
        F.col("road_conditions.road_lighting").alias("road_lighting"),
        F.col("road_conditions.road_hazards").alias("road_hazards"),
        F.col("road_conditions.road_works").alias("road_works"),

        F.col("contributing_factors.primary_cause").alias("primary_cause"),
        F.col("contributing_factors.speeding_involved").alias(
            "speeding_involved"
        ),
        F.col("contributing_factors.distracted_driving").alias(
            "distracted_driving"
        ),
        F.col("contributing_factors.drunk_driving").alias("drunk_driving"),
        F.col("contributing_factors.reckless_driving").alias(
            "reckless_driving"
        ),
        F.col("contributing_factors.poor_road_condition").alias(
            "poor_road_condition"
        ),
        F.col("contributing_factors.weather_related").alias("weather_related"),
        F.col("contributing_factors.traffic_violation").alias(
            "traffic_violation"
        ),
        F.col("contributing_factors.driver_fatigue").alias("driver_fatigue"),

        F.col("vehicles"),
        F.col("casualties"),
    )

    accidents_df = (
        accidents_df
        .withColumn(
            "risk_category",
            F.when(F.col("severity") == "Fatal", "Critical")
            .when(F.col("severity") == "Serious", "High")
            .when(F.col("severity") == "Slight", "Low")
            .otherwise("Unknown")
        )
        .withColumn("accident_hour", F.hour("event_timestamp_ts"))
        .withColumn("accident_day", F.to_date("event_timestamp_ts"))
    )

    return accidents_df


def build_vehicle_dataframe(accidents_df: DataFrame) -> DataFrame:
    """Explode nested vehicles into individual records."""

    return (
        accidents_df
        .select(
            "accident_id", "city", "country", "area", "severity",
            "road_type", "speed_limit",
            F.explode("vehicles").alias("vehicle")
        )
        .select(
            "accident_id", "city", "country", "area", "severity",
            "road_type", "speed_limit",
            F.col("vehicle.vehicle_id").alias("vehicle_id"),
            F.col("vehicle.vehicle_type").alias("vehicle_type"),
            F.col("vehicle.vehicle_make").alias("vehicle_make"),
            F.col("vehicle.vehicle_model").alias("vehicle_model"),
            F.col("vehicle.vehicle_year").alias("vehicle_year"),
            F.col("vehicle.vehicle_age").alias("vehicle_age"),
            F.col("vehicle.vehicle_speed_kmh").alias("vehicle_speed_kmh"),
            F.col("vehicle.driver_age").alias("driver_age"),
            F.col("vehicle.driver_gender").alias("driver_gender"),
            F.col("vehicle.driver_experience_years").alias(
                "driver_experience_years"
            ),
            F.col("vehicle.driver_condition").alias("driver_condition"),
            F.col("vehicle.seatbelt_used").alias("seatbelt_used"),
            F.col("vehicle.helmet_used").alias("helmet_used"),
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


def build_casualty_dataframe(accidents_df: DataFrame) -> DataFrame:
    """Explode nested casualties into individual records."""

    return (
        accidents_df
        .select(
            "accident_id", "city", "country", "area", "severity",
            F.explode("casualties").alias("casualty")
        )
        .select(
            "accident_id", "city", "country", "area", "severity",
            F.col("casualty.casualty_id").alias("casualty_id"),
            F.col("casualty.vehicle_id").alias("vehicle_id"),
            F.col("casualty.casualty_type").alias("casualty_type"),
            F.col("casualty.age").alias("age"),
            F.col("casualty.gender").alias("gender"),
            F.col("casualty.injury_severity").alias("injury_severity"),
            F.col("casualty.hospitalized").alias("hospitalized"),
            F.col("casualty.emergency_response_required").alias(
                "emergency_response_required"
            ),
            F.col("casualty.seatbelt_used").alias("seatbelt_used"),
            F.col("casualty.helmet_used").alias("helmet_used"),
        )
    )


def build_analytics(accidents_df: DataFrame) -> dict:
    """Build wide aggregations and joins for each micro-batch."""

    vehicles_df = build_vehicle_dataframe(accidents_df)
    casualties_df = build_casualty_dataframe(accidents_df)

    accident_analytics = (
        accidents_df.groupBy("city", "severity")
        .agg(
            F.count("*").alias("total_accidents"),
            F.avg("temperature_celsius").alias("average_temperature"),
            F.avg("number_of_casualties").alias("average_casualties"),
        )
    )

    vehicle_analytics = (
        vehicles_df.groupBy("vehicle_type")
        .agg(
            F.count("*").alias("total_vehicles"),
            F.avg("vehicle_speed_kmh").alias("average_speed_kmh"),
            F.avg("driver_age").alias("average_driver_age"),
        )
    )

    casualty_analytics = (
        casualties_df.groupBy("injury_severity", "casualty_type")
        .agg(
            F.count("*").alias("total_casualties"),
            F.sum(F.when(F.col("hospitalized") == True, 1).otherwise(0))
            .alias("hospitalized_count"),
        )
    )

    vehicle_per_accident = (
        vehicles_df.groupBy("accident_id")
        .agg(
            F.count("*").alias("actual_vehicle_count"),
            F.avg("vehicle_speed_kmh").alias("average_vehicle_speed"),
        )
    )

    accident_vehicle_analytics = (
        accidents_df.select(
            "accident_id", "city", "severity", "number_of_vehicles"
        )
        .join(vehicle_per_accident, on="accident_id", how="left")
    )

    cause_analytics = (
        accidents_df.groupBy("primary_cause")
        .agg(F.count("*").alias("total_accidents"))
    )

    time_analytics = (
        accidents_df.groupBy("day_of_week", "accident_hour")
        .agg(F.count("*").alias("total_accidents"))
    )

    geographic_analytics = (
        accidents_df.groupBy(
            "country", "city", "area", "urban_or_rural", "road_type"
        )
        .agg(
            F.count("*").alias("total_accidents"),
            F.sum(
                F.when(F.col("risk_category").isin("High", "Critical"), 1)
                .otherwise(0)
            ).alias("high_risk_accidents"),
            F.avg("latitude").alias("average_latitude"),
            F.avg("longitude").alias("average_longitude"),
        )
    )

    weather_analytics = (
        accidents_df.groupBy("weather_condition")
        .agg(
            F.count("*").alias("total_accidents"),
            F.avg("visibility_km").alias("average_visibility_km"),
            F.avg("wind_speed_kmh").alias("average_wind_speed"),
            F.avg("precipitation_mm").alias("average_precipitation"),
        )
    )

    road_analytics = (
        accidents_df.groupBy(
            "road_type", "road_surface_condition",
            "road_lighting", "light_conditions"
        )
        .agg(F.count("*").alias("total_accidents"))
    )

    driver_behavior_analytics = (
        accidents_df.agg(
            F.count("*").alias("total_accidents"),
            F.sum(F.when(F.col("speeding_involved") == True, 1).otherwise(0))
            .alias("speeding_accidents"),
            F.sum(F.when(F.col("distracted_driving") == True, 1).otherwise(0))
            .alias("distracted_driving_accidents"),
            F.sum(F.when(F.col("drunk_driving") == True, 1).otherwise(0))
            .alias("drunk_driving_accidents"),
            F.sum(F.when(F.col("reckless_driving") == True, 1).otherwise(0))
            .alias("reckless_driving_accidents"),
            F.sum(F.when(F.col("driver_fatigue") == True, 1).otherwise(0))
            .alias("fatigue_accidents"),
            F.sum(F.when(F.col("weather_related") == True, 1).otherwise(0))
            .alias("weather_related_accidents"),
        )
    )

    speeding_analytics = (
        vehicles_df.groupBy("speeding_over_limit")
        .agg(F.count("*").alias("total_vehicles"))
    )

    vehicle_damage_analytics = (
        vehicles_df.groupBy("vehicle_type", "vehicle_damage_severity")
        .agg(F.count("*").alias("total_vehicles"))
    )

    casualty_demographics_analytics = (
        casualties_df.groupBy("age", "gender", "casualty_type")
        .agg(F.count("*").alias("total_casualties"))
    )

    risk_analytics = (
        accidents_df.groupBy("risk_category", "city")
        .agg(
            F.count("*").alias("total_accidents"),
            F.avg("number_of_casualties").alias("average_casualties"),
        )
    )

    return {
        "accident_analytics": accident_analytics,
        "vehicle_analytics": vehicle_analytics,
        "casualty_analytics": casualty_analytics,
        "accident_vehicle_analytics": accident_vehicle_analytics,
        "cause_analytics": cause_analytics,
        "time_analytics": time_analytics,
        "geographic_analytics": geographic_analytics,
        "weather_analytics": weather_analytics,
        "road_analytics": road_analytics,
        "driver_behavior_analytics": driver_behavior_analytics,
        "speeding_analytics": speeding_analytics,
        "vehicle_damage_analytics": vehicle_damage_analytics,
        "casualty_demographics_analytics": casualty_demographics_analytics,
        "risk_analytics": risk_analytics,
    }
