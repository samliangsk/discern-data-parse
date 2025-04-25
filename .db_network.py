#!/bin/python3
# forked from BlankCanvasStudio/collection/analyze/vis-output/network.py

# not functional, still developing
# not functional, still developing
# not functional, still developing


import influxdb_client
import pandas as pd
import numpy as np
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

url = "http://localhost:8086"
token = "BIGElHSa291FOkrliGaBVc7ksnGgQ4vALbkfJzRuH02T2XB8qouH0H3IkYTJACE-XZ-QYV664CH5655LkbQDIQ"
org = "ISI"
bucket = "DISCERN"


# --- Flux Query ---

flux_query = """
// --- 1. Calculate Total Packets Sent (x) per <DevID, DestIP> pair ---
totalPacketsPerPair = from(bucket: "DISCERN")
  |> range(start: -1y)
  |> filter(fn: (r) =>
        r["_measurement"] == "network" and
        exists r["DevID"] and
        exists r["DestIP"]
  )
  // Group by the source-destination pair to count packets for each pair
  |> group(columns: ["DevID", "DestIP"])
  // count() calculates the number of records (rows/packets) in each group
  |> count()
  // count() creates a column named '_value' containing the count. Rename it.
  |> rename(columns: {_value: "TotalPackets"})
  // Keep the identifiers and the calculated total packet count
  |> keep(columns: ["DevID", "DestIP", "TotalPackets"])

// --- 2. Fetch Per-Second Aggregated Bytes Sent per <DevID, DestIP> pair ---
aggregatedBytesPerSecond = from(bucket: "DISCERN")
  |> range(start: -1y)
  |> filter(fn: (r) =>
        r["_measurement"] == "network" and
        r["_field"] == "length" and
        exists r["DevID"] and
        exists r["DestIP"]
  )
  |> group(columns: ["DevID", "DestIP"])
  |> aggregateWindow(every: 1s, fn: sum, createEmpty: false)
  |> rename(columns: {_value: "BytesInSecond"})
  |> keep(columns: ["_time", "DevID", "DestIP", "BytesInSecond"])

// --- Join the total packets with the per-second byte data ---
// Inner join ensures we only process pairs with both packet counts and byte data
finalDataForPython = join(
    tables: {bytesData: aggregatedBytesPerSecond, pktData: totalPacketsPerPair},
    on: ["DevID", "DestIP"],
    method: "inner"
)
  // Prepare the final structure for the output table
  |> map(fn: (r) => ({
        _time: r._time,
        DevID: r.DevID,
        DestIP: r.DestIP,
        TotalPackets: r.TotalPackets,
        BytesInSecond: r.BytesInSecond
    }))
  // Sort for predictable processing order in Python
  |> sort(columns: ["DevID", "DestIP", "_time"])
  // Yield the final table
  |> yield(name: "per_pair_data_for_python")
"""

# --- Main Execution Logic ---
def main():
    """
    Connects to InfluxDB, fetches network data per source-destination pair,
    calculates average/peak rates using Pandas, and prints the summary.
    """
    client = None # Initialize client to None for finally block cleanup
    try:
        # --- Connect to InfluxDB ---
        
        client = influxdb_client.InfluxDBClient(
            url=url,
            token=token,
            org=org,
            timeout=60_000 # Optional: Increased timeout (60s) for potentially long query
        )
        query_api = client.query_api()
        print("Connection successful.")

        # --- Execute Query and Fetch DataFrame ---
        print("Executing Flux query to fetch per-pair data...")
        print("(This query aggregates over 1 year at 1s granularity and may take time)")
        # query_data_frame executes the query and loads results directly into a Pandas DataFrame
        df = query_api.query_data_frame(query=flux_query)
        print(f"Query finished. Received {len(df)} rows.")

        if df.empty:
            print("No data returned from InfluxDB matching the criteria.")
            return

        # --- Data Cleaning & Preparation (Pandas) ---
        print("Preparing data for calculations...")
        # Drop InfluxDB metadata columns if they exist
        df_cleaned = df.drop(columns=['result', 'table'], errors='ignore')

        # Ensure correct data types, handling potential errors during conversion
        df_cleaned['_time'] = pd.to_datetime(df_cleaned['_time'], errors='coerce')
        df_cleaned['TotalPackets'] = pd.to_numeric(df_cleaned['TotalPackets'], errors='coerce').fillna(0).astype(int)
        df_cleaned['BytesInSecond'] = pd.to_numeric(df_cleaned['BytesInSecond'], errors='coerce').fillna(0)

        # Drop rows where essential data might be missing after conversion (optional)
        df_cleaned.dropna(subset=['_time', 'DevID', 'DestIP'], inplace=True)

        if df_cleaned.empty:
            print("Data became empty after cleaning steps.")
            return

        # --- Perform Rate Calculations in Pandas ---
        print("Calculating average and peak rates per source-destination pair...")
        results_per_pair = []
        # Group by the unique source-destination pair
        grouped = df_cleaned.groupby(['DevID', 'DestIP'])

        for name, group_df in grouped:
            dev_id, dest_ip = name

            # Get total packets (same for all rows in the group)
            total_packets = group_df['TotalPackets'].iloc[0]

            # Perform rate calculations only if there's valid byte data for the group
            byte_data_series = group_df['BytesInSecond']
            if not byte_data_series.empty and byte_data_series.notna().any():

                # Calculate Peak Rate (w Mbps)
                # Find the max bytes transferred in any single second for this pair
                max_bytes_in_one_second = byte_data_series.max()
                # Peak Rate = MaxBytesInSecond * 8 bits/byte / 1,000,000 bits/Mb
                peak_rate_mbps = max_bytes_in_one_second * 8.0 / 1_000_000.0

                # Calculate Average Rate (z Mbps)
                # Sum all bytes transferred for this pair over the period
                total_bytes_for_pair = byte_data_series.sum()
                # Duration = number of 1-second intervals data was present for this pair
                total_active_seconds = len(byte_data_series.dropna()) # Count only seconds with data

                if total_active_seconds > 0:
                    # Avg Rate = TotalBytes * 8 / (TotalActiveSeconds * 1e6)
                    avg_rate_mbps = (total_bytes_for_pair * 8.0) / (total_active_seconds * 1_000_000.0)
                else:
                    avg_rate_mbps = 0.0 # Avoid division by zero
            else:
                # No valid byte data found for this group after cleaning
                peak_rate_mbps = 0.0
                avg_rate_mbps = 0.0

            # --- Store Results for this Pair ---
            results_per_pair.append({
                'DevID': dev_id,
                'DestIP': dest_ip,
                'TotalPackets': total_packets,
                'AvgRateMbps': avg_rate_mbps,
                'PeakRateMbps': peak_rate_mbps
            })

        # --- Create Final Summary DataFrame ---
        final_summary_df = pd.DataFrame(results_per_pair)

        # --- Display Final Results ---
        print("\n--- Summary Per Source-Destination Pair ---")
        if final_summary_df.empty:
            print("No summary results generated (check grouping or calculations).")
        else:
            # Configure pandas display options for better readability
            pd.set_option('display.max_rows', 200) # Limit rows shown initially
            pd.set_option('display.max_columns', None) # Show all columns
            pd.set_option('display.width', 1000) # Adjust width for wider terminals
            pd.set_option('display.float_format', '{:.3f}'.format) # Format floats to 3 decimal places
            print(final_summary_df) # Display rounded DataFrame

    except InfluxDBError as e:
        print(f"InfluxDB API Error occurred: {e}")
        if hasattr(e, 'response') and e.response is not None:
            # Provide more context for debugging InfluxDB errors
            print(f"  Status Code: {e.response.status}")
            print(f"  Reason: {e.response.reason}")
            print(f"  Headers: {e.response.headers}")
            # Be cautious printing response data if it could be very large or sensitive
            # print(f"  Response Body Hint: {e.response.data[:200]}...")
        print("  Check InfluxDB logs, query syntax, and network connection.")

    except Exception as e:
        print(f"An unexpected Python error occurred: {e}")
        traceback.print_exc() # Print detailed Python traceback

    finally:
        # --- Ensure Client is Closed ---
        if client:
            client.close()
            print("\nInfluxDB client closed.")

# --- Run the main function ---
if __name__ == "__main__":
    main()

def main():
    """
    Connects to InfluxDB, generate network data per pair
    """
    
    client = None
    try:
        
        client = influxdb_client.InfluxDBClient(
            url=url,
            token=token,
            org=org
        )
        query_api = client.query_api()
        print("Connection successful.")

        # --- Execute Query and Fetch DataFrame ---
        print("Executing Flux query to fetch per-pair data (this might take a while)...")
        # query_data_frame executes the query and returns results directly as a Pandas DataFrame
        df = query_api.query_data_frame(query=flux_query)
        print(f"Query finished. Received {len(df)} rows.")

        if df.empty:
            print("No data returned from InfluxDB matching the criteria.")
            return

        # Optional: Print raw data structure for debugging
        # print("\n--- Raw DataFrame Head ---")
        # print(df.head())
        # print("\n--- Raw DataFrame Info ---")
        # print(df.info())


        # --- Data Cleaning & Preparation (Pandas) ---
        print("Preparing data for calculations...")
        # Drop InfluxDB metadata columns if they exist (query_data_frame might add them)
        df_cleaned = df.drop(columns=['result', 'table'], errors='ignore')

        # Ensure correct data types
        # _time is usually parsed correctly, but explicitly setting timezone might be needed sometimes
        df_cleaned['_time'] = pd.to_datetime(df_cleaned['_time'])
        # Ensure numeric types for calculation columns
        df_cleaned['TotalPackets'] = pd.to_numeric(df_cleaned['TotalPackets'], errors='coerce').fillna(0).astype(int)
        df_cleaned['BytesInSecond'] = pd.to_numeric(df_cleaned['BytesInSecond'], errors='coerce').fillna(0)


        # --- Perform Rate Calculations in Pandas ---
        print("Calculating average and peak rates per pair...")
        results_per_pair = []
        # Group by the unique source-destination pair
        grouped = df_cleaned.groupby(['DevID', 'DestIP'])

        for name, group_df in grouped:
            dev_id, dest_ip = name

            # Get total packets (same for all rows in the group)
            total_packets = group_df['TotalPackets'].iloc[0]

            # Perform rate calculations only if there's byte data for the group
            if not group_df['BytesInSecond'].empty and group_df['BytesInSecond'].notna().any():
                # Calculate Peak Rate (w Mbps)
                max_bytes_in_one_second = group_df['BytesInSecond'].max()
                # Peak Rate = MaxBytesInSecond * 8 bits/byte / 1,000,000 bits/Mb
                peak_rate_mbps = max_bytes_in_one_second * 8.0 / 1_000_000.0

                # Calculate Average Rate (z Mbps)
                total_bytes_for_pair = group_df['BytesInSecond'].sum()
                # Duration = number of 1-second intervals data was present for this pair
                total_active_seconds = len(group_df['BytesInSecond'].dropna()) # Count only non-NA seconds

                if total_active_seconds > 0:
                    # Avg Rate = TotalBytes * 8 / (TotalActiveSeconds * 1e6)
                    avg_rate_mbps = (total_bytes_for_pair * 8.0) / (total_active_seconds * 1_000_000.0)
                else:
                    avg_rate_mbps = 0.0
            else:
                # No valid byte data for this group
                peak_rate_mbps = 0.0
                avg_rate_mbps = 0.0

            # --- Store Results for this Pair ---
            results_per_pair.append({
                'DevID': dev_id,
                'DestIP': dest_ip,
                'TotalPackets': total_packets,
                'AvgRateMbps': avg_rate_mbps,
                'PeakRateMbps': peak_rate_mbps
            })

        # --- Create Final Summary DataFrame ---
        final_summary_df = pd.DataFrame(results_per_pair)

        # --- Display Final Results ---
        print("\n--- Summary Per Source-Destination Pair ---")
        if final_summary_df.empty:
            print("No summary results generated (check intermediate data).")
        else:
            # Configure pandas display options for better readability
            pd.set_option('display.max_rows', None) # Show all rows
            pd.set_option('display.max_columns', None) # Show all columns
            pd.set_option('display.width', 1000) # Adjust width for wider terminals
            pd.set_option('display.float_format', '{:.2f}'.format) # Format floats to 2 decimal places
            print(final_summary_df.round(2)) # Display rounded DataFrame


    except InfluxDBError as e:
        print(f"InfluxDB API Error occurred: {e}")
        if hasattr(e, 'response') and e.response is not None:
             print(f"InfluxDB Response Headers: {e.response.headers}")
             # Avoid printing body directly if it might be huge or sensitive
             # print(f"InfluxDB Response Body: {e.response.data}")
             print("Check InfluxDB logs and query syntax.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback for debugging non-InfluxDB errors

    finally:
        # --- Ensure Client is Closed ---
        if client:
            client.close()
            print("\nInfluxDB client closed.")

# --- Run the main function ---
if __name__ == "__main__":
    main()