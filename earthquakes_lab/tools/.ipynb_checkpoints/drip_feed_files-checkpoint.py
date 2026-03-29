#!/usr/bin/env python3
"""
drip_feed_files.py

Move files from a staging directory into a landing directory at a fixed cadence
to simulate a streaming file source for Spark Structured Streaming.

Usage:
  python tools/drip_feed_files.py data/staging data/landing --interval 1.0

Notes:
- Files are moved in lexicographic order by filename.
- Only regular files are moved (skips directories).
- Press Ctrl+C to stop gracefully.
"""
import argparse
import os
import shutil
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("staging", help="Source directory containing many small files")
    parser.add_argument("landing", help="Destination directory (Spark watches this)")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between moves")
    args = parser.parse_args()

    src = os.path.abspath(args.staging)
    dst = os.path.abspath(args.landing)
    os.makedirs(dst, exist_ok=True)

    print(f"Drip-feeding from {src} -> {dst} every {args.interval:.2f}s. Ctrl+C to stop.")
    try:
        while True:
            files = [f for f in sorted(os.listdir(src)) if os.path.isfile(os.path.join(src, f))]
            if not files:
                time.sleep(args.interval)
                continue
            f = files[0]
            src_path = os.path.join(src, f)
            dst_path = os.path.join(dst, f)
            try:
                shutil.move(src_path, dst_path)
                print(f"Moved: {f}")
            except Exception as e:
                print(f"Skipping {f}: {e}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopped.")

if __name__ == "__main__":
    main()
