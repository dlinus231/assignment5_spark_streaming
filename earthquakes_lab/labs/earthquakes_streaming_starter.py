#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Earthquakes Streaming Lab — Starter

You will implement a file-based Structured Streaming pipeline using USGS earthquakes data.

What you will build
- Read streaming CSV files from a landing directory (data/landing)
- Parse robust event timestamps from the 'time' column (epoch ms OR ISO8601)
- Derive 'region' from 'place' (last token after comma) and 'magnitude_band' from 'mag'
- Use event-time tumbling windows (15 minutes) with a 30-minute watermark
- Compute counts per (window, region, magnitude_band)
- Write to console (update mode) and to Parquet via foreachBatch with checkpointing

How to run (example)
1) Prepare dirs:
   mkdir -p data/raw data/staging data/landing data/out data/ckpt tools labs
2) Download CSV:
   curl -L -o data/raw/all_month.csv "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
3) Create microfiles:
   python tools/prepare_quake_microfiles.py --in data/raw/all_month.csv --out data/staging --max-records 10000
4) Start drip-feeder in another terminal:
   python tools/drip_feed_files.py data/staging data/landing --interval 1.0
5) Run this streaming job:
   spark-submit labs/earthquakes_streaming_starter.py --landing data/landing --out data/out --ckpt data/ckpt

Deliverables
- A running job that prints updates and writes Parquet in data/out/
- Demonstrate restart behavior: stop and rerun with same --ckpt (no reprocessing of completed files)
- Short note: explain late data behavior and your watermark choice

IMPORTANT
- This starter intentionally raises NotImplementedError in the core functions.
- Replace the TODO sections with working code per the instructions and hints.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import (
    col, to_timestamp, from_unixtime, when, regexp_extract, expr, window
)

def create_spark(app_name="earthquakes-streaming-lab"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Keep event timestamps consistent:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark

def robust_event_ts(df):
    """
    Implement robust event timestamp extraction for the 'time' field and add a new column named 'event_ts' of TimestampType.

    Requirements:
    - The USGS 'time' field may arrive either as:
      1) Milliseconds since UNIX epoch, represented as a numeric-looking string, or
      2) An ISO8601 timestamp string (UTC).
    - First, determine whether the 'time' value can be interpreted as a number. If it can:
      - Treat it as milliseconds since epoch. Convert it to seconds (by dividing by 1000) and then convert to a proper timestamp type.
    - Otherwise:
      - Parse the 'time' value directly as an ISO8601 timestamp string.
    - Create a new column 'event_ts' that holds the resulting TimestampType values.
    - Return the DataFrame with this new column added.

    Constraints and tips:
    - Do not overwrite the original 'time' column; add a separate 'event_ts' column.
    - Use a conditional expression to choose between the numeric path (epoch ms → seconds → timestamp) and the string parse path (ISO8601 → timestamp).
    - Ensure the final type of 'event_ts' is a Spark TimestampType, not a string.
    """
    raise NotImplementedError("Implement robust_event_ts(): derive 'event_ts' from 'time' (epoch ms or ISO8601)")

def derive_region_and_mag(df):
    """
    Add derived columns for 'region', numeric magnitude 'mag_d', and binned 'magnitude_band', then return the augmented DataFrame.

    Implement precisely as follows:
    - region:
      - Interpret the 'place' column as free text like "5 km NW of The Geysers, CA" or "South of Alaska".
      - Extract the substring that appears after the final comma in 'place', trimming any surrounding spaces. Examples:
        * "The Geysers, CA" -> region should become "CA"
        * "South of Alaska" (no comma) -> region should be "Unknown"
      - If there is no comma in 'place' or the extracted portion is empty after trimming, set region to "Unknown".
      - Create a new column named 'region' holding this derived text.
    - mag_d:
      - Create a new column 'mag_d' that converts 'mag' to a numeric type (a double).
    - magnitude_band:
      - Create a new categorical string column 'magnitude_band' based on 'mag_d' using the following bins:
        * If mag_d is null or cannot be parsed: "unknown"
        * If 0 <= mag_d < 2: "[0-2)"
        * If 2 <= mag_d < 4: "[2-4)"
        * If 4 <= mag_d < 6: "[4-6)"
        * If mag_d >= 6: "6+"

    Return:
    - The input DataFrame with the three new columns: 'region', 'mag_d', and 'magnitude_band'.
    """
    raise NotImplementedError("Implement derive_region_and_mag(): 'region', 'mag_d', and 'magnitude_band' columns")

def build_stream(spark, landing_path, out_path, ckpt_path):
    """
    Build and start the streaming pipeline. Return a list of the two active streaming Query objects.

    Implement the following steps:
    1) Define an explicit CSV schema where every incoming field is treated as a string:
       - Columns: id, time, latitude, longitude, depth, mag, magType, place, type.
       - Use a StructType with StringType fields for all of the above.
    2) Create a streaming DataFrame that reads CSV files from the given landing_path:
       - Use the CSV format, enable header parsing, and supply the explicit schema you defined.
       - This DataFrame must be a streaming source (readStream), not a batch read.
    3) Derive columns:
       - Call your robust_event_ts() function to add 'event_ts'.
       - Call your derive_region_and_mag() function to add 'region', 'mag_d', and 'magnitude_band'.
    4) Windowing and watermark:
       - Specify a watermark of 30 minutes on 'event_ts' to bound lateness and state.
       - Use a 15-minute tumbling window on 'event_ts'.
       - Group by the window, the 'region', and the 'magnitude_band', and compute counts.
       - In the final projection, select:
         * window start time (name it 'window_start')
         * window end time (name it 'window_end')
         * 'region'
         * 'magnitude_band'
         * the count
       - Important: The watermark must be applied within the same logical query plan that performs the aggregation (i.e., before the groupBy/aggregation in the transformation chain).
    5) Sinks:
       - Start a console sink in "update" output mode so you can see incremental results.
       - Start a second sink that writes Parquet files using foreachBatch:
         * In the foreachBatch function, append the current micro-batch to the out_path directory (note: in production, you would want to use idempotent writes (e.g., upserts keyed by window+region+magnitude_band)).
         * Configure the streaming query with a checkpointLocation set to ckpt_path (this is required for recovery and proper watermark behavior).
    6) Return:
       - Return both active Query objects in a list in the order [console_query, parquet_query].

    Additional requirements:
    - Use update mode for the aggregated outputs.
    - Do not change the checkpoint path between runs if you want to demonstrate restart behavior.
    - Ensure you start both queries before returning them.
    """
    raise NotImplementedError("Implement build_stream(): schema, readStream, watermark+windowed aggregation, and sinks")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", required=True, help="Directory where drip-fed CSVs arrive (e.g., data/landing)")
    ap.add_argument("--out", required=True, help="Output directory for Parquet (e.g., data/out)")
    ap.add_argument("--ckpt", required=True, help="Checkpoint directory (e.g., data/ckpt)")
    return ap.parse_args()

def main():
    args = parse_args()
    spark = create_spark()
    # NOTE: This will raise NotImplementedError until you complete build_stream()
    queries = build_stream(spark, args.landing, args.out, args.ckpt)
    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
