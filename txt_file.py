import json
import pandas as pd
import numpy as np
from pathlib import Path
import traceback

# --- Configuration ---
INPUT_FILE_PATH = Path("file-data.txt") # Name of your file change data file
OUTPUT_CSV_FILE = Path("file-res.csv") # Set to None to disable CSV output
VERBOSE_LOGGING = False # Set to True for more print statements during processing

def process_file_change_log(file_path: Path, verbose: bool = False):
    if not file_path.is_file():
        print(f"Error: Input file not found at {file_path}")
        return None

    print(f"Processing file change log: {file_path}...")

    extracted_records = []
    line_num = 0
    parsing_errors = 0
    processed_lines = 0

    try:
        with open(file_path, 'r') as f:
            for line in f:
                line_num += 1
                try:
                    data_entry = json.loads(line)
                    dev_id = data_entry.get("DevID")
                    timestamp = data_entry.get("TimeStamp")

                    if not dev_id: continue
                    if timestamp is None: continue

                    extracted_records.append({
                        'DevID': dev_id,
                        'EpochTimeStamp': int(timestamp),

                    })
                    processed_lines += 1

                except Exception as e:
                    parsing_errors += 1
                    if verbose: print(f"Warning: Skipping line {line_num} due to error: {e}")
                    continue

    except Exception as e:
        print(f"An error occurred during file reading: {e}")
        traceback.print_exc()
        return None

    print(f"\n--- File Processing Summary ---")
    print(f" Lines processed: {line_num}")
    if parsing_errors > 0: print(f" Lines skipped (errors/invalid data): {parsing_errors}")
    print(f" Valid records extracted: {len(extracted_records)}")
    print("-" * 30)

    if not extracted_records:
        print("No valid file change records extracted from the file.")
        return None

    # --- Convert to Pandas DataFrame ---
    print("Converting extracted data to DataFrame...")
    df = pd.DataFrame(extracted_records)
    df['TimeStamp'] = pd.to_datetime(df['EpochTimeStamp'], unit='s', errors='coerce')
    df.dropna(subset=['TimeStamp', 'DevID'], inplace=True) # Drop if timestamp conversion failed

    if df.empty:
        print("DataFrame is empty after initial processing.")
        return None

    # --- Identify and Filter Baseline ---
    print("Identifying baseline (records with earliest timestamp)...")
    min_timestamp_epoch = df['EpochTimeStamp'].min()
    min_timestamp_dt = pd.to_datetime(min_timestamp_epoch, unit='s')
    print(f"Earliest timestamp (baseline): {min_timestamp_epoch} ({min_timestamp_dt})")

    # Keep only records strictly *after* the baseline timestamp
    df_changes = df[df['EpochTimeStamp'] > min_timestamp_epoch].copy() # Use .copy() to avoid SettingWithCopyWarning

    if df_changes.empty:
        print("No file changes detected after the initial baseline.")
        # Optionally return a DataFrame indicating zero changes
        dev_ids = df['DevID'].unique()
        zero_summary = pd.DataFrame({
            'DevID': dev_ids,
            'TotalFilesChanged': 0,
            'AvgFilesChangedPerMin': 0.0,
            'PeakFilesChangedPerMin': 0,
            'ChangeTimeSpanMinutes': 0.0
        })
        return zero_summary


    print(f"Found {len(df_changes)} change records after baseline.")
    if verbose:
        print("\n--- Change Records DataFrame Head ---")
        print(df_changes.head())

    # --- Calculate Statistics per DevID for Changes ---
    print("Calculating statistics per DevID...")
    all_dev_summaries = []

    for dev_id, group_df in df_changes.groupby('DevID'):
        if verbose: print(f"  Processing DevID: {dev_id} ({len(group_df)} changes)")

        # W: Total Files Changed
        total_changes_w = len(group_df)

        # --- Peak Calculation ---
        # Group changes by 1-minute intervals and count
        group_df.set_index('TimeStamp', inplace=True) # Set timestamp as index forGrouper
        per_minute_counts = group_df.groupby(pd.Grouper(freq='1min')).size()
        group_df.reset_index(inplace=True) # Reset index if needed later

        peak_per_min = per_minute_counts.max() if not per_minute_counts.empty else 0

        # --- Average Calculation ---
        # Calculate time span of changes for this device
        first_change_time = group_df['TimeStamp'].min()
        last_change_time = group_df['TimeStamp'].max()
        duration_seconds = (last_change_time - first_change_time).total_seconds()

        # Calculate duration in minutes, ensuring minimum of 1 minute for rate calc
        # Add 60 seconds to duration before dividing to correctly represent the span
        # e.g., changes at :01 and :05 are within the same minute span.
        # Consider duration from first change minute start to last change minute end?
        # Simpler: Use duration from first to last change, minimum 60s.
        duration_minutes = max(1.0, (duration_seconds / 60.0))

        avg_per_min_x = total_changes_w / duration_minutes

        all_dev_summaries.append({
            'DevID': dev_id,
            'TotalFilesChanged': total_changes_w,
            'AvgFilesChangedPerMin': avg_per_min_x,
            'PeakFilesChangedPerMin': peak_per_min,
            'ChangeTimeSpanMinutes': duration_minutes # Add duration for context
        })

    # --- Create Final Summary DataFrame ---
    final_summary_df = pd.DataFrame(all_dev_summaries)

    print("Calculations complete.")

    return final_summary_df

# --- Main Execution Block ---
if __name__ == "__main__":
    final_summary = process_file_change_log(INPUT_FILE_PATH, verbose=VERBOSE_LOGGING)

    if final_summary is not None and not final_summary.empty:
        print("\n--- File Change Summary Per DevID (Post-Baseline) ---")
        # Configure pandas display options
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.float_format', '{:.2f}'.format)

        print(final_summary)

        # --- Optional: Save to CSV File ---
        if OUTPUT_CSV_FILE:
            try:
                final_summary.to_csv(OUTPUT_CSV_FILE, index=False)
                print(f"\nSummary saved to: {OUTPUT_CSV_FILE}")
            except Exception as e:
                print(f"\nError saving summary to CSV: {e}")
                traceback.print_exc()
    else:
        print("\nNo file change summary statistics generated.")