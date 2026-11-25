"""
generate_traffic_data.py
========================
Entry point of the SYNTHETIC Traffic Accident Dataset Generator.

Usage examples
--------------
    python generate_traffic_data.py                        # 100,000 accidents (default)
    python generate_traffic_data.py --num-accidents 10000  # quick test dataset
    python generate_traffic_data.py -n 50000 --seed 7 --output-dir my_dataset

What it does
------------
    1. generates Accidents, Vehicles and Casualties (generators.py)
    2. runs the data-quality checks (validation.py)
    3. saves everything into the output folder (reporting.py writes the documentation)

All generated records are SYNTHETIC. This script does NOT touch Kafka, NiFi or Spark:
a separate producer reads the CSV files (see event_builder_example.py).
"""
import argparse
import os
import sys
import time

import config as cfg
import generators
import reporting
import validation


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a SYNTHETIC traffic accident dataset.")
    parser.add_argument("-n", "--num-accidents", type=int, default=cfg.NUM_ACCIDENTS,
                        help=f"number of accidents to generate (default {cfg.NUM_ACCIDENTS:,})")
    parser.add_argument("--seed", type=int, default=cfg.RANDOM_SEED,
                        help=f"random seed for reproducible data (default {cfg.RANDOM_SEED})")
    parser.add_argument("--output-dir", default=cfg.OUTPUT_DIR,
                        help=f"output folder (default {cfg.OUTPUT_DIR})")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.num_accidents < 1:
        sys.exit("--num-accidents must be at least 1")

    print(f"Generating {args.num_accidents:,} SYNTHETIC accidents (seed {args.seed}) ...")
    started = time.time()
    accidents, vehicles, casualties = generators.generate_dataset(args.num_accidents, args.seed)

    results = validation.run_validation(accidents, vehicles, casualties)
    all_passed = validation.print_validation_report(results)

    os.makedirs(args.output_dir, exist_ok=True)
    accidents.to_csv(os.path.join(args.output_dir, "Accidents.csv"), index=False)
    vehicles.to_csv(os.path.join(args.output_dir, "Vehicles.csv"), index=False)
    casualties.to_csv(os.path.join(args.output_dir, "Casualties.csv"), index=False)
    reporting.write_data_dictionary(args.output_dir)
    reporting.write_generation_summary(args.output_dir, accidents, vehicles, casualties,
                                       results, args.seed, time.time() - started)

    print(f"\nSaved to '{args.output_dir}/':")
    print(f"  Accidents.csv   {len(accidents):>9,} rows")
    print(f"  Vehicles.csv    {len(vehicles):>9,} rows")
    print(f"  Casualties.csv  {len(casualties):>9,} rows")
    print("  data_dictionary.csv, generation_summary.txt")
    print(f"Done in {time.time() - started:.1f} seconds. Remember: all data is SYNTHETIC.")
    if not all_passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
