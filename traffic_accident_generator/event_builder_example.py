"""
event_builder_example.py
========================
Shows how a SEPARATE streaming producer can turn the three SYNTHETIC CSV files into
complete JSON accident events. It contains NO Kafka / NiFi / Spark code on purpose:
the dataset generator and the streaming pipeline stay independent.

Run it to print the first event:
    python event_builder_example.py
    python event_builder_example.py --folder traffic_accident_dataset --count 3

To build the real Kafka producer, copy iter_events() and replace the print() line with
something like  producer.send("traffic-accidents", value=event)  (kafka-python or
confluent-kafka). The delay between events is configurable with --delay.
"""
import argparse
import json
import os
import time

import pandas as pd

ACCIDENT_FIELDS = ["accident_id", "accident_date", "accident_time", "day_of_week", "month", "year",
                   "accident_severity", "number_of_vehicles", "number_of_casualties",
                   "accident_duration_minutes", "traffic_conditions", "light_conditions"]
LOCATION_FIELDS = ["country", "city", "area", "latitude", "longitude", "road_name", "road_type",
                   "urban_or_rural", "junction_type", "speed_limit"]
WEATHER_FIELDS = ["weather_condition", "temperature_celsius", "humidity_percentage",
                  "visibility_km", "wind_speed_kmh", "precipitation_mm"]
ROAD_FIELDS = ["road_surface_condition", "road_lighting", "road_hazards", "road_works"]
FACTOR_FIELDS = ["primary_cause", "speeding_involved", "distracted_driving", "drunk_driving",
                 "reckless_driving", "poor_road_condition", "weather_related",
                 "traffic_violation", "driver_fatigue"]


def to_records(df):
    """DataFrame -> list of plain Python dicts, with empty cells converted to None (JSON null)."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def load_tables(folder):
    """Read the CSV files. keep_default_na=False + na_values='' means only EMPTY cells are null."""
    def read(name, int_columns=()):
        return pd.read_csv(os.path.join(folder, name), keep_default_na=False, na_values=[""],
                           dtype={c: "Int64" for c in int_columns})   # keeps ages as 59, not 59.0
    return (read("Accidents.csv"),
            read("Vehicles.csv", ["driver_age", "driver_experience_years", "engine_capacity_cc"]),
            read("Casualties.csv", ["age"]))


def build_event(accident, vehicles, casualties):
    """Combine one accident row with its vehicles and casualties into one JSON-ready dict."""
    pick = lambda fields: {f: accident[f] for f in fields}
    return {
        "event_type": "traffic_accident",
        "is_synthetic": True,                       # never a real accident
        "event_timestamp": f"{accident['accident_date']}T{accident['accident_time']}",
        "accident": pick(ACCIDENT_FIELDS),
        "location": pick(LOCATION_FIELDS),
        "weather": pick(WEATHER_FIELDS),
        "road_conditions": pick(ROAD_FIELDS),
        "contributing_factors": pick(FACTOR_FIELDS),
        "vehicles": vehicles,
        "casualties": casualties,
    }


def iter_events(folder, delay_seconds=0.0):
    """Yield one complete event per accident, in chronological order (IDs are sequential)."""
    accidents, vehicles, casualties = load_tables(folder)
    # Group once so each lookup is instant (important for 100,000 accidents).
    vehicles_by_accident = {k: to_records(g) for k, g in vehicles.groupby("accident_id")}
    casualties_by_accident = {k: to_records(g) for k, g in casualties.groupby("accident_id")}

    for accident in to_records(accidents):
        key = accident["accident_id"]
        yield build_event(accident, vehicles_by_accident.get(key, []), casualties_by_accident.get(key, []))
        if delay_seconds:
            time.sleep(delay_seconds)               # simulate real-time arrival


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print example JSON events from the SYNTHETIC dataset.")
    parser.add_argument("--folder", default="data/sample_7000")
    parser.add_argument("--count", type=int, default=1, help="how many events to print")
    parser.add_argument("--delay", type=float, default=0.0, help="seconds between events")
    args = parser.parse_args()

    for number, event in enumerate(iter_events(args.folder, args.delay), start=1):
        print(json.dumps(event, indent=2, default=str))
        if number >= args.count:
            break
