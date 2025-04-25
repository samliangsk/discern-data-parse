import json
import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import traceback


INPUT_FILE_PATH = Path("proc-mem-data.txt")
OUTPUT_CSV_FILE = Path("proc_mem_summary.csv")
VERBOSE_LOGGING = False
SORT_BY_COLUMN = 'TimeWeightedAvgVmSizeMiB'

BYTES_TO_MIB = 1024 * 1024

def process_program_vmemory_usage(file_path: Path, verbose: bool = False):

    print(f"Processing started at: {datetime.datetime.now()}")

    if not file_path.is_file():
        print(f"Error: Input file not found at {file_path}")
        return None

    print(f"Processing program memory usage file (Avg: VmSize, Peaks: VmPeak/VmHWM): {file_path}...")

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
                    timestamp = data_entry.get("TimeStamp")
                    name = data_entry.get("Name") # Program Name
                    vm_size_str = data_entry.get("VmSize")
                    vm_peak_str = data_entry.get("VmPeak")
                    vm_hwm_str = data_entry.get("VmHWM")

                    # --- Data Validation ---
                    # Check if all required fields are present
                    if timestamp is None or name is None or vm_size_str is None \
                       or vm_peak_str is None or vm_hwm_str is None:
                        if verbose: print(f"Warning: Skipping line {line_num}. Missing required fields (TimeStamp, Name, VmSize, VmPeak, VmHWM).")
                        continue

                    # Convert memory strings to integer bytes
                    vm_size_bytes = int(vm_size_str)
                    vm_peak_bytes = int(vm_peak_str)
                    vm_hwm_bytes = int(vm_hwm_str)

                    # Append valid data
                    extracted_records.append({
                        'EpochTimeStamp': int(timestamp),
                        'Name': name,
                        'VmSizeBytes': vm_size_bytes, # Used for average calculation
                        'VmPeakBytes': vm_peak_bytes, # Used for Max VmPeak
                        'VmHwmBytes': vm_hwm_bytes   # Used for Max VmHWM
                    })
                    processed_lines += 1

                except (ValueError, TypeError) as e:
                     parsing_errors += 1
                     if verbose: print(f"Warning: Skipping line {line_num} due to numeric conversion error: {e}")
                     continue
                except Exception as e: # Catch other potential errors
                    parsing_errors += 1
                    if verbose: print(f"Warning: Skipping line {line_num} due to error: {e}")
                    continue

    except Exception as e:
        print(f"An error occurred during file reading: {e}")
        traceback.print_exc()
        return None

    print(f"\n--- File Processing Summary ---")
    print(f" Lines read: {line_num}")
    if parsing_errors > 0: print(f" Lines skipped (errors/invalid data): {parsing_errors}")
    print(f" Valid process memory records extracted: {len(extracted_records)}")
    print("-" * 30)


    if not extracted_records:
        print("No valid process memory records extracted.")
        return None

    # --- Convert to Pandas DataFrame ---
    print("Converting extracted data to DataFrame...")
    df = pd.DataFrame(extracted_records)
    df['TimeStamp'] = pd.to_datetime(df['EpochTimeStamp'], unit='s', errors='coerce')
    # Ensure memory columns are numeric
    for col in ['VmSizeBytes', 'VmPeakBytes', 'VmHwmBytes']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=['TimeStamp', 'Name', 'VmSizeBytes', 'VmPeakBytes', 'VmHwmBytes'], inplace=True)

    if df.empty:
        print("DataFrame is empty after initial processing and cleaning.")
        return None

    # --- Calculate Total Observation Duration ---
    print("Calculating overall time span...")
    distinct_timestamps = df['TimeStamp'].sort_values().unique()

    if len(distinct_timestamps) < 2:
         print("Warning: Need >= 2 distinct timestamps for time-weighted average.")
         # Calculate Max values only
         summary = df.groupby('Name').agg(
             MaxVmPeakBytes = ('VmPeakBytes', 'max'),
             MaxVmHwmBytes = ('VmHwmBytes', 'max')
             ).reset_index()
         summary['TimeWeightedAvgVmSizeBytes'] = np.nan
         # Convert max values to MiB
         summary['MaxVmPeakMiB'] = summary['MaxVmPeakBytes'].fillna(0) / BYTES_TO_MIB
         summary['MaxVmHwmMiB'] = summary['MaxVmHwmBytes'].fillna(0) / BYTES_TO_MIB
         summary['TimeWeightedAvgVmSizeMiB'] = np.nan
         return summary[['Name', 'TimeWeightedAvgVmSizeMiB', 'MaxVmPeakMiB', 'MaxVmHwmMiB']]
    else:
         min_ts = pd.to_datetime(distinct_timestamps.min())
         max_ts = pd.to_datetime(distinct_timestamps.max())

    total_duration_seconds = (max_ts - min_ts).total_seconds()
    print(f"Total observation time span: {total_duration_seconds:.2f} seconds (from {min_ts} to {max_ts})")

    if total_duration_seconds <= 0:
        print("Warning: Total duration is zero or negative. Cannot calculate time-weighted average.")
        summary = df.groupby('Name').agg(
             MaxVmPeakBytes = ('VmPeakBytes', 'max'),
             MaxVmHwmBytes = ('VmHwmBytes', 'max')
             ).reset_index()
        summary['TimeWeightedAvgVmSizeBytes'] = np.nan
        summary['MaxVmPeakMiB'] = summary['MaxVmPeakBytes'].fillna(0) / BYTES_TO_MIB
        summary['MaxVmHwmMiB'] = summary['MaxVmHwmBytes'].fillna(0) / BYTES_TO_MIB
        summary['TimeWeightedAvgVmSizeMiB'] = np.nan
        return summary[['Name', 'TimeWeightedAvgVmSizeMiB', 'MaxVmPeakMiB', 'MaxVmHwmMiB']]


    # --- Calculate Time-Weighted Average VmSize ---
    print("Calculating time-weighted average VmSize per program...")

    # 1. Map next overall timestamp
    ts_map = {pd.Timestamp(t): pd.Timestamp(next_t) for t, next_t in zip(distinct_timestamps[:-1], distinct_timestamps[1:])}
    df['NextOverallTimeStamp'] = df['TimeStamp'].map(ts_map)

    # 2. Calculate duration each VmSize record represents (until next overall snapshot)
    df['NextOverallTimeStamp'].fillna(max_ts, inplace=True) # Assume value holds till end? NO - duration should be 0 for last ts
    df['Duration'] = (df['NextOverallTimeStamp'] - df['TimeStamp']).dt.total_seconds()
    # Set duration for records at the last timestamp to 0
    df.loc[df['TimeStamp'] == max_ts, 'Duration'] = 0
    df['Duration'] = df['Duration'].clip(lower=0) # Ensure non-negative

    # 3. Calculate the weighted value: VmSize Bytes * Duration (seconds)
    df['VmSizeWeighted'] = df['VmSizeBytes'] * df['Duration'] # Units: Byte-Seconds

    if verbose:
        print("\n--- DataFrame with Durations and Weights Head (VmSize) ---")
        df_sorted = df.sort_values(by=['Name', 'TimeStamp'])
        print(df_sorted[['Name', 'TimeStamp', 'VmSizeBytes', 'NextOverallTimeStamp', 'Duration', 'VmSizeWeighted']].head(10))

    # --- Aggregate per Program ---
    summary = df.groupby('Name').agg(
        TotalVmSizeWeighted=('VmSizeWeighted', 'sum'),     # Sum of VmSize Byte-Seconds
        MaxVmPeakBytes=('VmPeakBytes', 'max'),             # Max VmPeak observed
        MaxVmHwmBytes=('VmHwmBytes', 'max'),               # Max VmHWM observed
        DataPoints=('VmSizeBytes', 'count')                # Number of measurements
    ).reset_index()

    # Calculate the final Time-Weighted Average VmSize (in Bytes)
    # Sum(VmSize_i * Duration_i) / TotalDuration
    summary['TimeWeightedAvgVmSizeBytes'] = summary['TotalVmSizeWeighted'] / total_duration_seconds

    # --- Convert Bytes to MiB for final presentation ---
    summary['TimeWeightedAvgVmSizeMiB'] = summary['TimeWeightedAvgVmSizeBytes'] / BYTES_TO_MIB
    summary['MaxVmPeakMiB'] = summary['MaxVmPeakBytes'] / BYTES_TO_MIB
    summary['MaxVmHwmMiB'] = summary['MaxVmHwmBytes'] / BYTES_TO_MIB

    print("Calculations complete.")

    # Reorder and select final columns
    summary = summary[[
        'Name', 'TimeWeightedAvgVmSizeMiB', 'MaxVmPeakMiB', 'MaxVmHwmMiB', 'DataPoints'
    ]]

    return summary

# --- Main Execution Block ---
if __name__ == "__main__":
    # Run the processing function
    final_summary = process_program_vmemory_usage(INPUT_FILE_PATH, verbose=VERBOSE_LOGGING)

    if final_summary is not None and not final_summary.empty:
        print("\n--- Program Memory Usage Summary (Avg VmSize, Peak VmPeak/VmHWM in MiB) ---")
        # Configure pandas display options
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1200) # Increased width for more columns
        pd.set_option('display.float_format', '{:.2f}'.format) # Format floats to 2 decimal places for MiB

        # Sort by average usage descending (or change SORT_BY_COLUMN)
        print(final_summary.sort_values(by=SORT_BY_COLUMN, ascending=False))

        # --- Optional: Save to CSV File ---
        if OUTPUT_CSV_FILE:
            try:
                final_summary.sort_values(by=SORT_BY_COLUMN, ascending=False).to_csv(OUTPUT_CSV_FILE, index=False)
                print(f"\nSummary saved to: {OUTPUT_CSV_FILE}")
            except Exception as e:
                print(f"\nError saving summary to CSV: {e}")
                traceback.print_exc()
    else:
        print("\nNo program memory usage summary statistics generated.")