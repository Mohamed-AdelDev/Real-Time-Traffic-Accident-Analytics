"""
reporting.py
============
Writes the two documentation files that accompany the SYNTHETIC dataset:

* data_dictionary.csv    - every column of every table: type and meaning
* generation_summary.txt - totals, distributions, date range, missing values, validation
"""
import csv
import os
from datetime import datetime

import config as cfg


def write_data_dictionary(output_dir):
    """Write data_dictionary.csv (one row per column of Accidents/Vehicles/Casualties)."""
    path = os.path.join(output_dir, "data_dictionary.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["file_name", "column_name", "data_type", "description"])
        for file_name, schema in (("Accidents.csv", cfg.ACCIDENT_SCHEMA),
                                  ("Vehicles.csv", cfg.VEHICLE_SCHEMA),
                                  ("Casualties.csv", cfg.CASUALTY_SCHEMA)):
            for column, data_type, description in schema:
                writer.writerow([file_name, column, data_type, "[SYNTHETIC] " + description])
    return path


def _distribution(series, order=None):
    """Format value counts as '  value: count (percent)' lines."""
    counts = series.value_counts()
    if order:
        counts = counts.reindex([v for v in order if v in counts.index])
    total = counts.sum()
    return [f"  {str(value):<28}{count:>10,}  ({count / total:6.2%})" for value, count in counts.items()]


def _missing_lines(name, table):
    """List only the columns that contain empty cells."""
    missing = table.isna().sum()
    lines = [f"{name}: {int(missing.sum()):,} empty cells in total"]
    lines += [f"  {col:<28}{int(n):>10,}  ({n / len(table):6.2%})" for col, n in missing.items() if n > 0]
    return lines


def write_generation_summary(output_dir, accidents, vehicles, casualties, validation_results,
                             seed, seconds):
    """Write generation_summary.txt."""
    lines = [
        "=" * 78,
        "TRAFFIC ACCIDENT DATASET - GENERATION SUMMARY",
        "ALL RECORDS ARE SYNTHETIC (computer generated). They are NOT real reported accidents",
        "and must not be used as real-world observations.",
        "=" * 78,
        f"Generated at        : {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Random seed         : {seed}",
        f"Generation time     : {seconds:.1f} seconds",
        "",
        "TOTALS",
        f"  Total accidents   : {len(accidents):,}",
        f"  Total vehicles    : {len(vehicles):,}  ({len(vehicles) / len(accidents):.2f} per accident)",
        f"  Total casualties  : {len(casualties):,}  ({len(casualties) / len(accidents):.2f} per accident)",
        "",
        "DATE RANGE",
        f"  First accident    : {accidents['accident_date'].min()}",
        f"  Last accident     : {accidents['accident_date'].max()}",
        "",
        "ACCIDENT SEVERITY DISTRIBUTION",
        *_distribution(accidents["accident_severity"], cfg.SEVERITIES),
        "",
        "CASUALTY INJURY SEVERITY DISTRIBUTION",
        *_distribution(casualties["injury_severity"], cfg.SEVERITIES),
        "",
        "VEHICLE TYPE DISTRIBUTION",
        *_distribution(vehicles["vehicle_type"]),
        "",
        "WEATHER DISTRIBUTION",
        *_distribution(accidents["weather_condition"]),
        "",
        "CITY DISTRIBUTION",
        *_distribution(accidents["city"]),
        "",
        "MISSING VALUES (empty cells; 'not applicable' values such as pedestrian vehicle_id,",
        "helmet for cars or engine size for bicycles are intentional empties)",
        *_missing_lines("Accidents", accidents),
        *_missing_lines("Vehicles", vehicles),
        *_missing_lines("Casualties", casualties),
        "",
        "VALIDATION RESULTS",
    ]
    for name, passed, detail in validation_results:
        lines.append(f"  [{'PASS' if passed else 'FAIL'}] {name}" + (f"  [{detail}]" if detail else ""))
    passed_count = sum(1 for r in validation_results if r[1])
    lines += ["", f"  {passed_count} / {len(validation_results)} checks passed", ""]

    path = os.path.join(output_dir, "generation_summary.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path
