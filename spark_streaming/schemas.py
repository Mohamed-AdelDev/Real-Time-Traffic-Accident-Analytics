
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DoubleType,
    ArrayType,
)


# Accident information
accident_schema = StructType([
    StructField("accident_id", StringType(), True),
    StructField("accident_date", StringType(), True),
    StructField("accident_time", StringType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("accident_severity", StringType(), True),
    StructField("number_of_vehicles", IntegerType(), True),
    StructField("number_of_casualties", IntegerType(), True),
    StructField("accident_duration_minutes", IntegerType(), True),
    StructField("traffic_conditions", StringType(), True),
    StructField("light_conditions", StringType(), True),
])


# Geographic information
location_schema = StructType([
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("area", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("road_name", StringType(), True),
    StructField("road_type", StringType(), True),
    StructField("urban_or_rural", StringType(), True),
    StructField("junction_type", StringType(), True),
    StructField("speed_limit", IntegerType(), True),
])


# Weather information
weather_schema = StructType([
    StructField("weather_condition", StringType(), True),
    StructField("temperature_celsius", DoubleType(), True),
    StructField("humidity_percentage", DoubleType(), True),
    StructField("visibility_km", DoubleType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
])


# Road conditions
road_conditions_schema = StructType([
    StructField("road_surface_condition", StringType(), True),
    StructField("road_lighting", StringType(), True),
    StructField("road_hazards", StringType(), True),
    StructField("road_works", BooleanType(), True),
])


# Contributing factors
contributing_factors_schema = StructType([
    StructField("primary_cause", StringType(), True),
    StructField("speeding_involved", BooleanType(), True),
    StructField("distracted_driving", BooleanType(), True),
    StructField("drunk_driving", BooleanType(), True),
    StructField("reckless_driving", BooleanType(), True),
    StructField("poor_road_condition", BooleanType(), True),
    StructField("weather_related", BooleanType(), True),
    StructField("traffic_violation", BooleanType(), True),
    StructField("driver_fatigue", BooleanType(), True),
])


# Vehicle information
vehicle_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("accident_id", StringType(), True),
    StructField("vehicle_reference", IntegerType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("vehicle_make", StringType(), True),
    StructField("vehicle_model", StringType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("vehicle_age", IntegerType(), True),
    StructField("vehicle_speed_kmh", IntegerType(), True),
    StructField("vehicle_direction", StringType(), True),
    StructField("vehicle_maneuver", StringType(), True),
    StructField("vehicle_damage_severity", StringType(), True),
    StructField("driver_age", IntegerType(), True),
    StructField("driver_gender", StringType(), True),
    StructField("driver_experience_years", IntegerType(), True),
    StructField("seatbelt_used", BooleanType(), True),
    StructField("helmet_used", BooleanType(), True),
    StructField("driver_condition", StringType(), True),
    StructField("vehicle_color", StringType(), True),
    StructField("engine_capacity_cc", IntegerType(), True),
])


# Casualty information
casualty_schema = StructType([
    StructField("casualty_id", StringType(), True),
    StructField("accident_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("casualty_type", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("injury_severity", StringType(), True),
    StructField("hospitalized", BooleanType(), True),
    StructField("emergency_response_required", BooleanType(), True),
    StructField("seatbelt_used", BooleanType(), True),
    StructField("helmet_used", BooleanType(), True),
])


# Complete Kafka event schema
EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("is_synthetic", BooleanType(), True),
    StructField("event_timestamp", StringType(), True),

    StructField("accident", accident_schema, True),
    StructField("location", location_schema, True),
    StructField("weather", weather_schema, True),
    StructField("road_conditions", road_conditions_schema, True),
    StructField("contributing_factors", contributing_factors_schema, True),

    StructField("vehicles", ArrayType(vehicle_schema), True),
    StructField("casualties", ArrayType(casualty_schema), True),
])
