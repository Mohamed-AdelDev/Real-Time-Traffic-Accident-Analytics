"""
validation.py
=============
Data-quality checks for the SYNTHETIC traffic accident dataset.

run_validation() returns a list of (check name, passed?, detail) tuples.
print_validation_report() prints them as a readable report.

Every check is a few lines of pandas, so you can add your own easily.
"""
import pandas as pd

import config as cfg

SEVERITY_RANK = {"Slight": 1, "Serious": 2, "Fatal": 3}

# Columns that are EXPECTED to contain empty cells (structural "not applicable").
STRUCTURAL_NULLS = {
    "Vehicles": {"seatbelt_used", "helmet_used", "engine_capacity_cc"},
    "Casualties": {"vehicle_id", "seatbelt_used", "helmet_used"},
    "Accidents": set(),
}
MAX_MISSING_SHARE = 0.05      # no regular column may have more than 5% missing values


def run_validation(accidents, vehicles, casualties):
    """Run all checks and return the list of results."""
    results = []
    large = len(accidents) >= 1000      # statistical checks only make sense for larger datasets

    def add(name, condition, detail=""):
        results.append((name, bool(condition), detail))

    def count_bad(mask):
        return f"{int(mask.sum()):,} bad rows" if int(mask.sum()) else ""

    # ------------------------------------------------------------------
    # 1. Structure and primary keys
    # ------------------------------------------------------------------
    add("Accidents columns match the schema", list(accidents.columns) == cfg.ACCIDENT_COLUMNS)
    add("Vehicles columns match the schema", list(vehicles.columns) == cfg.VEHICLE_COLUMNS)
    add("Casualties columns match the schema", list(casualties.columns) == cfg.CASUALTY_COLUMNS)
    add("No duplicate accident_id", accidents["accident_id"].is_unique)
    add("No duplicate vehicle_id", vehicles["vehicle_id"].is_unique)
    add("No duplicate casualty_id", casualties["casualty_id"].is_unique)
    add("No missing primary keys",
        accidents["accident_id"].notna().all() and vehicles["vehicle_id"].notna().all()
        and casualties["casualty_id"].notna().all())
    add("Vehicle reference is unique inside each accident",
        not vehicles.duplicated(["accident_id", "vehicle_reference"]).any())

    # ------------------------------------------------------------------
    # 2. Foreign keys and relationships
    # ------------------------------------------------------------------
    accident_ids = set(accidents["accident_id"])
    add("Vehicles.accident_id exists in Accidents", vehicles["accident_id"].isin(accident_ids).all())
    add("Casualties.accident_id exists in Accidents", casualties["accident_id"].isin(accident_ids).all())

    with_vehicle = casualties[casualties["vehicle_id"].notna()]
    add("Casualties.vehicle_id exists in Vehicles", with_vehicle["vehicle_id"].isin(set(vehicles["vehicle_id"])).all())

    joined = with_vehicle.merge(vehicles[["vehicle_id", "accident_id", "vehicle_type", "driver_age"]],
                                on="vehicle_id", suffixes=("", "_vehicle"))
    add("Casualty and its vehicle belong to the same accident",
        (joined["accident_id"] == joined["accident_id_vehicle"]).all())
    add("Pedestrians have NULL vehicle_id, everybody else has a vehicle",
        ((casualties["casualty_type"] == "Pedestrian") == casualties["vehicle_id"].isna()).all())

    allowed = {"Driver": {"Car", "Van", "Truck", "Bus"}, "Passenger": {"Car", "Van", "Truck", "Bus", "Motorcycle"},
               "Motorcyclist": {"Motorcycle"}, "Cyclist": {"Bicycle"}}
    bad = joined.apply(lambda r: r["vehicle_type"] not in allowed[r["casualty_type"]], axis=1)
    add("Casualty type matches the vehicle type", not bad.any(), count_bad(bad))

    drivers = joined[joined["casualty_type"].isin(["Driver", "Motorcyclist", "Cyclist"])]
    same_age = (drivers["age"].fillna(-1) == drivers["driver_age"].fillna(-1))
    add("Driver/rider casualties have the same age as the vehicle driver", same_age.all(), count_bad(~same_age))

    vehicle_counts = vehicles.groupby("accident_id").size()
    casualty_counts = casualties.groupby("accident_id").size()
    n_vehicles = accidents["accident_id"].map(vehicle_counts).fillna(0).astype(int)
    n_casualties = accidents["accident_id"].map(casualty_counts).fillna(0).astype(int)
    add("Every accident has at least one vehicle", (n_vehicles >= 1).all())
    add("Every accident has at least one casualty", (n_casualties >= 1).all())
    add("number_of_vehicles matches Vehicles.csv", (n_vehicles == accidents["number_of_vehicles"]).all(),
        count_bad(n_vehicles != accidents["number_of_vehicles"]))
    add("number_of_casualties matches Casualties.csv", (n_casualties == accidents["number_of_casualties"]).all(),
        count_bad(n_casualties != accidents["number_of_casualties"]))

    # ------------------------------------------------------------------
    # 3. Severity consistency
    # ------------------------------------------------------------------
    worst = (casualties.assign(rank=casualties["injury_severity"].map(SEVERITY_RANK))
             .groupby("accident_id")["rank"].max())
    accident_rank = accidents["severity_rank"] = accidents["accident_severity"].map(SEVERITY_RANK)
    consistent = accidents["accident_id"].map(worst) == accident_rank
    add("Accident severity equals the worst casualty injury", consistent.all(), count_bad(~consistent))
    add("Casualties do not all share one severity in severe accidents",
        (not large) or casualties[casualties["accident_id"].isin(
            accidents.loc[accidents["accident_severity"] != "Slight", "accident_id"])]
        .groupby("accident_id")["injury_severity"].nunique().gt(1).any())
    accidents.drop(columns="severity_rank", inplace=True)

    # ------------------------------------------------------------------
    # 4. Valid categories
    # ------------------------------------------------------------------
    for table_name, table, categories in (("Accidents", accidents, cfg.ACCIDENT_CATEGORIES),
                                          ("Vehicles", vehicles, cfg.VEHICLE_CATEGORIES),
                                          ("Casualties", casualties, cfg.CASUALTY_CATEGORIES)):
        invalid = [col for col, values in categories.items()
                   if not table[col].dropna().isin(values).all()]
        add(f"{table_name}: all categorical values are valid", not invalid, ", ".join(invalid))

    # ------------------------------------------------------------------
    # 5. Dates, times, coordinates, numeric ranges
    # ------------------------------------------------------------------
    dates = pd.to_datetime(accidents["accident_date"], errors="coerce")
    times = pd.to_datetime(accidents["accident_time"], format="%H:%M:%S", errors="coerce")
    add("All dates and times are valid", dates.notna().all() and times.notna().all())
    add("Dates are inside the configured range",
        (dates.min() >= pd.Timestamp(cfg.START_DATE)) and (dates.max() <= pd.Timestamp(cfg.END_DATE)))
    add("day_of_week / month / year agree with accident_date",
        ((dates.dt.day_name() == accidents["day_of_week"]) & (dates.dt.month == accidents["month"])
         & (dates.dt.year == accidents["year"])).all())

    add("Latitude/longitude are valid numbers",
        accidents["latitude"].between(-90, 90).all() and accidents["longitude"].between(-180, 180).all())
    centers = {c["name"]: c["center"] for c in cfg.CITIES}
    lat_c = accidents["city"].map(lambda n: centers[n][0])
    lon_c = accidents["city"].map(lambda n: centers[n][1])
    inside = ((accidents["latitude"] - lat_c).abs() <= cfg.BBOX_HALF_LAT + 1e-9) & \
             ((accidents["longitude"] - lon_c).abs() <= cfg.BBOX_HALF_LON + 1e-9)
    add("Every coordinate lies inside its city's bounding box", inside.all(), count_bad(~inside))

    add("No negative or impossible numbers (speeds, ages, counts, durations)",
        (vehicles["vehicle_speed_kmh"] >= 0).all() and (vehicles["vehicle_age"] >= 0).all()
        and vehicles["driver_age"].dropna().between(0, 110).all()
        and casualties["age"].dropna().between(0, 110).all()
        and (accidents["number_of_vehicles"] >= 1).all() and (accidents["number_of_casualties"] >= 1).all()
        and (accidents["accident_duration_minutes"] > 0).all() and (accidents["speed_limit"] > 0).all())
    add("Weather values are in realistic ranges",
        accidents["temperature_celsius"].dropna().between(-30, 50).all()
        and accidents["humidity_percentage"].dropna().between(0, 100).all()
        and (accidents["visibility_km"].dropna() >= 0).all()
        and (accidents["wind_speed_kmh"].dropna() >= 0).all()
        and (accidents["precipitation_mm"] >= 0).all())

    # ------------------------------------------------------------------
    # 6. Weather <-> road logic
    # ------------------------------------------------------------------
    weather = accidents["weather_condition"]
    surface = accidents["road_surface_condition"]
    fog = accidents[weather == "Fog"]
    add("Fog always has reduced visibility (< 1 km)", (fog["visibility_km"].dropna() < 1.0).all())
    add("Rain, heavy rain and snow always have precipitation",
        (accidents.loc[weather.isin(["Rain", "Heavy Rain", "Snow"]), "precipitation_mm"] > 0).all())
    add("Clear and cloudy weather has no precipitation",
        (accidents.loc[weather.isin(["Clear", "Cloudy"]), "precipitation_mm"] == 0).all())
    add("Icy roads only occur when it is cold (<= 3 C)",
        (accidents.loc[surface == "Icy", "temperature_celsius"].dropna() <= 3.0).all())
    add("Snowy roads only occur in snow weather", (weather[surface == "Snowy"] == "Snow").all())
    wet_rain = (surface[weather.isin(["Rain", "Heavy Rain"])] == "Wet").mean()
    wet_clear = (surface[weather == "Clear"] == "Wet").mean()
    add("Wet roads are much more common in rain than in clear weather",
        (not large) or wet_rain > 3 * wet_clear,
        f"wet share: rain {wet_rain:.0%} vs clear {wet_clear:.0%}")
    add("weather_related is never True in clear/cloudy weather",
        not accidents.loc[weather.isin(["Clear", "Cloudy"]), "weather_related"].any())

    # ------------------------------------------------------------------
    # 7. Vehicle and driver consistency
    # ------------------------------------------------------------------
    vtype = vehicles["vehicle_type"]
    add("Helmet data only for motorcycles and bicycles (and always present for them)",
        (vehicles["helmet_used"].notna() == vtype.isin(cfg.HELMET_VEHICLES)).all())
    add("Seatbelt data only for vehicles with seatbelts (and always present for them)",
        (vehicles["seatbelt_used"].notna() == vtype.isin(cfg.SEATBELT_VEHICLES)).all())
    merged = vehicles.merge(accidents[["accident_id", "year"]], on="accident_id")
    add("vehicle_year <= accident year and vehicle_age = year - vehicle_year",
        ((merged["vehicle_year"] <= merged["year"])
         & (merged["vehicle_age"] == merged["year"] - merged["vehicle_year"])).all())
    min_age = vtype.map(cfg.MIN_LICENCE_AGE)
    young = vehicles["driver_age"].notna() & (vehicles["driver_age"] < min_age)
    add("Driver age respects the minimum age for the vehicle type", not young.any(), count_bad(young))
    add("Driver experience never exceeds driver age",
        (vehicles["driver_experience_years"].dropna() <= vehicles.loc[vehicles["driver_experience_years"].notna(), "driver_age"]).all())
    add("Bicycles have no engine, motor vehicles do",
        (vehicles["engine_capacity_cc"].isna() == (vtype == "Bicycle")).all())

    # ------------------------------------------------------------------
    # 8. Contributing factors agree with vehicle data
    # ------------------------------------------------------------------
    over_limit = (vehicles.merge(accidents[["accident_id", "speed_limit"]], on="accident_id")
                  .query("vehicle_type != 'Bicycle'")
                  .assign(over=lambda d: d["vehicle_speed_kmh"] > d["speed_limit"])
                  .groupby("accident_id")["over"].any())
    has_over = accidents["accident_id"].map(over_limit).fillna(False).astype(bool)
    add("speeding_involved <=> a motor vehicle exceeded the speed limit",
        (has_over == accidents["speeding_involved"]).all(), count_bad(has_over != accidents["speeding_involved"]))
    impaired = vehicles["driver_condition"].isin(["Alcohol Impaired", "Drug Impaired"])
    has_impaired = accidents["accident_id"].map(impaired.groupby(vehicles["accident_id"]).any()).astype(bool)
    add("drunk_driving <=> an impaired driver exists", (has_impaired == accidents["drunk_driving"]).all())
    cause_to_flag = {label: flag for flag, (label, _) in cfg.FLAG_TO_CAUSE.items()}
    flagged = accidents["primary_cause"].map(cause_to_flag)
    bad = flagged.notna() & ~accidents.apply(lambda r: bool(r[flagged[r.name]]) if pd.notna(flagged[r.name]) else True, axis=1)
    add("primary_cause is backed by its contributing factor", not bad.any(), count_bad(bad))
    add("Multiple contributing factors can occur together",
        (accidents[["speeding_involved", "distracted_driving", "drunk_driving", "reckless_driving",
                    "poor_road_condition", "weather_related", "traffic_violation", "driver_fatigue"]]
         .sum(axis=1) >= 2).mean() > 0.05 or not large)

    # ------------------------------------------------------------------
    # 9. Missing values
    # ------------------------------------------------------------------
    worst_missing = []
    for table_name, table in (("Accidents", accidents), ("Vehicles", vehicles), ("Casualties", casualties)):
        share = table.isna().mean()
        for column, value in share.items():
            if column not in STRUCTURAL_NULLS[table_name] and value > MAX_MISSING_SHARE:
                worst_missing.append(f"{table_name}.{column} {value:.1%}")
    add(f"No regular column has more than {MAX_MISSING_SHARE:.0%} missing values",
        not worst_missing, ", ".join(worst_missing))

    # ------------------------------------------------------------------
    # 10. Distribution sanity checks (only meaningful for reasonably large datasets)
    # ------------------------------------------------------------------
    if len(accidents) < 1000:
        add("Distribution sanity checks skipped (fewer than 1,000 accidents)", True)
        return results

    severity = accidents["accident_severity"].value_counts(normalize=True).reindex(cfg.SEVERITIES).fillna(0)
    add("Slight > Serious > Fatal, with fatal accidents rare (< 5%)",
        severity["Slight"] > severity["Serious"] > severity["Fatal"] and severity["Fatal"] < 0.05,
        " / ".join(f"{k} {severity[k]:.1%}" for k in cfg.SEVERITIES))
    shares = vtype.value_counts(normalize=True).reindex(cfg.VEHICLE_TYPES).fillna(0)
    add("Cars are the most common vehicle; motorcycles, bicycles, trucks and buses are well represented",
        shares.idxmax() == "Car" and shares["Motorcycle"] > 0.05 and shares["Bicycle"] > 0.04
        and shares["Truck"] > 0.03 and shares["Bus"] > 0.015,
        ", ".join(f"{k} {shares[k]:.1%}" for k in cfg.VEHICLE_TYPES))
    per_accident = len(vehicles) / len(accidents)
    add("Vehicles per accident between 1.5 and 2.0", 1.5 <= per_accident <= 2.0, f"{per_accident:.2f}")
    add("Most accidents involve 1-2 vehicles, some involve 3+",
        (accidents["number_of_vehicles"] <= 2).mean() > 0.7 and (accidents["number_of_vehicles"] >= 3).mean() > 0.05)
    return results


def print_validation_report(results):
    """Print the validation results; returns True if every check passed."""
    print("\n" + "=" * 78)
    print("VALIDATION REPORT  (all data is SYNTHETIC)")
    print("=" * 78)
    for name, passed, detail in results:
        suffix = f"  [{detail}]" if detail else ""
        print(f"[{'PASS' if passed else 'FAIL'}] {name}{suffix}")
    failed = [r for r in results if not r[1]]
    print("-" * 78)
    print(f"{len(results) - len(failed)} / {len(results)} checks passed")
    print("=" * 78)
    return not failed
