#!/usr/bin/env python3
"""
prepare_quake_microfiles.py

Reads the USGS all_month.csv, selects relevant columns, and writes many small CSV files
into a staging directory for use with a file-based streaming source.

Usage:
  python tools/prepare_quake_microfiles.py --in data/raw/all_month.csv --out data/staging --max-records 10000
Options:
  --filter-days N    Optionally keep only the last N days to speed up the lab (default: keep all).
"""

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime, when

def robust_event_ts(time_col):
    """
    Create a TimestampType column from USGS 'time' which may be:
      - epoch milliseconds (most common)
      - ISO8601 string (less common)
    """
    # Detect numeric-looking strings: cast to double; if not null, treat as epoch ms.
    time_d = time_col.cast("double")
    return when(time_d.isNotNull(), to_timestamp(from_unixtime(time_d / 1000.0))) \
             .otherwise(to_timestamp(time_col))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input_path", required=True, help="Path to all_month.csv")
    parser.add_argument("--out", dest="output_dir", required=True, help="Output directory for microfiles (CSV)")
    parser.add_argument("--max-records", dest="max_records", type=int, default=10000, help="Approx rows per file")
    parser.add_argument("--filter-days", dest="filter_days", type=int, default=0, help="Keep only last N days")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("prep-quake-microfiles").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df = (spark.read.option("header", True).csv(args.input_path))

    # Subset of columns
    cols = ["id", "time", "latitude", "longitude", "depth", "mag", "magType", "place", "type"]
    df = df.select(*cols)

    # Optional filter by recent days to reduce volume
    if args.filter_days and args.filter_days > 0:
        now_utc = datetime.now(timezone.utc)
        cutoff_ms = int((now_utc - timedelta(days=args.filter_days)).timestamp() * 1000)
        df = df.where(col("time").cast("double") >= cutoff_ms)

    # Optionally validate/preview
    # df.select("id", "time", "mag", "place").show(5, truncate=False)

    # Write to many small CSV files with header
    (df.write.mode("overwrite")
       .option("header", "true")
       .option("maxRecordsPerFile", args.max_records)
       .csv(args.output_dir))

    print(f"Wrote microfiles to: {args.output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()
