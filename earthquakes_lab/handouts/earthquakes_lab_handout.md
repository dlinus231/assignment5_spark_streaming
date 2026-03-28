# Hands-on Lab: Spark Structured Streaming from Files (USGS Earthquakes)

Goal
- Build a file-based streaming pipeline with Spark that reads incoming CSV “micro-batches” of earthquake events, parses timestamps, applies event-time windows with watermarking, derives regions and magnitude bands, and writes results to console and Parquet with checkpointing.

Dataset
- USGS Earthquakes (last 30 days), CSV:
  - https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv
- Columns of interest: id, time, latitude, longitude, depth, mag, magType, place, type

What you’ll do
1) Download all_month.csv to data/raw/.
2) Split it into many small CSVs (microfiles) in data/staging/.
3) Drip-feed files from data/staging/ to data/landing/ to simulate a stream.
4) Implement a Spark Structured Streaming job to:
   - Read CSV stream from data/landing/ with an explicit schema
   - Create event_ts (TimestampType) from the “time” column (handle ms-since-epoch robustly)
   - Derive region from place (last token after the last comma)
   - Create magnitude_band buckets
   - Use withWatermark("event_ts","30 minutes") and 15-minute tumbling windows
   - groupBy(window, region, magnitude_band).count()
   - Write to console (update) and to Parquet via foreachBatch with a checkpoint

Key concepts to apply
- Event time vs processing time: Use event time (event_ts) for windowing to account for out-of-order/late arrivals.
- Watermarking: withWatermark bounds lateness and controls when windows are finalized and state is dropped.
- Output mode: “update” for incremental aggregates.
- Checkpointing: Required for stateful queries (aggregation, watermark). Enables restart/recovery.

How to run (summary)
- Prepare directories:
  mkdir -p data/raw data/staging data/landing data/out data/ckpt tools labs
- Download raw CSV:
  curl -L -o data/raw/all_month.csv "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
- Prepare microfiles:
  python tools/prepare_quake_microfiles.py --in data/raw/all_month.csv --out data/staging --max-records 10000
- Start drip feeder (separate terminal):
  python tools/drip_feed_files.py data/staging data/landing --interval 1.0
- Run the streaming job (starter):
  spark-submit labs/earthquakes_streaming_starter.py --landing data/landing --out data/out --ckpt data/ckpt

What to observe
- Console output updates each micro-batch with window_start/window_end, region, magnitude_band, and count.
- Parquet files appear in data/out/.
- Restart behavior: Stop the job, re-run with the same --ckpt path, and it resumes without reprocessing previous files.
- Late data: If you move an “older” file back into staging after windows are finalized, those events may be treated as late (won’t change finalized windows).

Coding considerations
- Schema: Provide explicit schema (don’t infer) for streaming CSV.
- Robust timestamp parsing:
  - USGS’s time is commonly epoch milliseconds; handle both epoch ms and ISO8601.
  - Use when() to detect numeric vs string and to_timestamp(from_unixtime(...)).
- Derivations:
  - region: last token after comma in place. Fallback to “Unknown” if missing.
  - magnitude_band: CASE expression; e.g., unknown, [0–2), [2–4), [4–6), 6+.
- Windowing and watermark:
  - Call withWatermark before groupBy(window(...)) in the same logical pipeline.
  - Watermark choice (30m suggested) = your tolerance for late events vs memory.
- Sinks:
  - Console for visibility (update mode).
  - foreachBatch for Parquet writes. Keep writes idempotent where possible (append is fine for lab).

Troubleshooting
- No output:
  - Drip feeder not moving files? Check paths.
  - Schema mismatch? Validate headers and types.
  - Forgot to set event_ts or withWatermark before aggregation?
- Time parsing nulls:
  - Ensure conversion from ms epoch: to_timestamp(from_unixtime(time_ms/1000.0)).
  - Set spark.sql.session.timeZone to UTC for consistent behavior.
- Restart reprocesses everything:
  - Use the same checkpoint path for the same query. Changing path = fresh run.
- Large state/memory:
  - Reduce window length or increase watermark to finalize sooner. Filter dataset when creating microfiles.

Deliverables
- Running job with:
  - 15-minute tumbling window, 30-minute watermark
  - Aggregation by region x magnitude_band
  - Console + Parquet outputs and checkpoint
- A brief note (2–3 sentences) explaining what happened to late events and how watermark affected behavior.

Stretch ideas (optional)
- Sliding windows (length 30m, slide 10m)
- Partition Parquet by window_start or region
- Top-N regions per window (use foreachBatch + window_start group to compute N)
- Bucket latitude/longitude to coarse grid instead of string-based region

References
- Spark Structured Streaming Programming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

Good luck! Ask for help early if something seems off (share your command, code snippet, and the exact error).
