import shutil
from pathlib import Path

import pandas as pd

SOURCE = Path("data/raw")
OUTPUT = Path("data/sample_7000")
TARGET_COUNT = 7000
RANDOM_STATE = 42

FILES = [
    "Accidents.csv",
    "Vehicles.csv",
    "Casualties.csv",
    "data_dictionary.csv",
]


def load_csv(name):
    return pd.read_csv(
        SOURCE / name,
        keep_default_na=False,
        dtype=str
    )


def validate_unique(df, column, filename):
    if df[column].eq("").any():
        raise ValueError(f"{filename}: empty {column} found")

    if df[column].duplicated().any():
        raise ValueError(f"{filename}: duplicate {column} found")


def validate_relationships(accidents, vehicles, casualties):
    accident_ids = set(accidents["accident_id"])
    vehicle_ids = set(vehicles["vehicle_id"])

    if not vehicles["accident_id"].isin(accident_ids).all():
        raise ValueError("Vehicles contains unknown accident_id values")

    if not casualties["accident_id"].isin(accident_ids).all():
        raise ValueError("Casualties contains unknown accident_id values")

    linked = casualties[casualties["vehicle_id"] != ""]

    if not linked["vehicle_id"].isin(vehicle_ids).all():
        raise ValueError("Casualties contains unknown vehicle_id values")

    vehicle_links = vehicles.set_index("vehicle_id")["accident_id"]

    expected_accidents = linked["vehicle_id"].map(vehicle_links)

    if not expected_accidents.eq(linked["accident_id"]).all():
        raise ValueError("A casualty is linked to a vehicle from another accident")


def stratified_sample(accidents):
    groups = list(
        accidents.groupby("accident_severity", sort=True, dropna=False)
    )

    total = len(accidents)

    quotas = []
    for severity, group in groups:
        exact = len(group) * TARGET_COUNT / total
        quotas.append({
            "severity": severity,
            "group": group,
            "quota": int(exact),
            "fraction": exact - int(exact),
        })

    remaining = TARGET_COUNT - sum(item["quota"] for item in quotas)

    for item in sorted(
        quotas,
        key=lambda x: x["fraction"],
        reverse=True
    )[:remaining]:
        item["quota"] += 1

    samples = []

    for item in quotas:
        if item["quota"] > 0:
            samples.append(
                item["group"].sample(
                    n=item["quota"],
                    random_state=RANDOM_STATE
                )
            )

    result = pd.concat(samples)

    return result.sort_values(
        ["accident_date", "accident_time", "accident_id"]
    ).reset_index(drop=True)


def main():
    if not all((SOURCE / name).is_file() for name in FILES):
        raise FileNotFoundError("One or more original CSV files are missing")

    if OUTPUT.exists():
        raise FileExistsError(
            f"{OUTPUT} already exists. It was not overwritten."
        )

    accidents = load_csv("Accidents.csv")
    vehicles = load_csv("Vehicles.csv")
    casualties = load_csv("Casualties.csv")

    validate_unique(accidents, "accident_id", "Accidents.csv")
    validate_unique(vehicles, "vehicle_id", "Vehicles.csv")
    validate_unique(casualties, "casualty_id", "Casualties.csv")

    if len(accidents) < TARGET_COUNT:
        raise ValueError("There are fewer than 7000 accidents")

    validate_relationships(accidents, vehicles, casualties)

    selected = stratified_sample(accidents)

    selected_ids = set(selected["accident_id"])

    selected_vehicles = vehicles[
        vehicles["accident_id"].isin(selected_ids)
    ].copy()

    selected_casualties = casualties[
        casualties["accident_id"].isin(selected_ids)
    ].copy()

    selected_vehicle_ids = set(selected_vehicles["vehicle_id"])

    if len(selected) != TARGET_COUNT:
        raise ValueError("The sample does not contain exactly 7000 accidents")

    if not selected_vehicles["accident_id"].isin(selected_ids).all():
        raise ValueError("Vehicle filtering failed")

    if not selected_casualties["accident_id"].isin(selected_ids).all():
        raise ValueError("Casualty filtering failed")

    linked = selected_casualties[
        selected_casualties["vehicle_id"] != ""
    ]

    if not linked["vehicle_id"].isin(selected_vehicle_ids).all():
        raise ValueError("Selected casualty-to-vehicle relationship failed")

    OUTPUT.mkdir(parents=True)

    selected.to_csv(OUTPUT / "Accidents.csv", index=False)
    selected_vehicles.to_csv(OUTPUT / "Vehicles.csv", index=False)
    selected_casualties.to_csv(OUTPUT / "Casualties.csv", index=False)

    shutil.copy2(
        SOURCE / "data_dictionary.csv",
        OUTPUT / "data_dictionary.csv"
    )

    # Read the written files again to verify the actual output.
    check_a = load_output("Accidents.csv")
    check_v = load_output("Vehicles.csv")
    check_c = load_output("Casualties.csv")

    if len(check_a) != TARGET_COUNT:
        raise ValueError("Output verification: accident count mismatch")

    validate_unique(check_a, "accident_id", "Output Accidents.csv")
    validate_unique(check_v, "vehicle_id", "Output Vehicles.csv")
    validate_unique(check_c, "casualty_id", "Output Casualties.csv")
    validate_relationships(check_a, check_v, check_c)

    print("\nSUCCESS: Sample created and verified.")
    print("Folder:", OUTPUT)
    print("Accidents:", len(check_a))
    print("Vehicles:", len(check_v))
    print("Casualties:", len(check_c))
    print("\nSeverity distribution:")
    print(check_a["accident_severity"].value_counts().to_string())
    print("\nAll primary and foreign key checks passed.")


def load_output(name):
    return pd.read_csv(
        OUTPUT / name,
        keep_default_na=False,
        dtype=str
    )


if __name__ == "__main__":
    main()
