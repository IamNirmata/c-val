import os
import argparse
import numpy as np
import sqlite3
import pandas as pd
from collections import Counter
import time

# Broadened keywords to catch any metric with these substrings
NUMERICAL_KEYWORDS = ["norm", "bias", "weight"]

def calculate_robust_thresholds(data_points):
    """
    Analyzes the 1D distribution using percentiles to naturally handle
    skewness and maintain consistent probability-based tier spacing.
    """
    data_points = np.array(data_points)

    # 1. Discrete / Low-Cardinality Check
    unique_vals, counts = np.unique(data_points, return_counts=True)
    if len(unique_vals) <= 10:
        total_points = len(data_points)
        frequencies = counts / total_points

        baseline = unique_vals[np.argmax(counts)]
        valid_bad_vals = unique_vals[frequencies > 0.01]
        bad_threshold = np.max(valid_bad_vals) if len(valid_bad_vals) > 0 else np.max(unique_vals)

        return {
            "baseline": float(baseline),
            "bad_threshold": float(bad_threshold),
            "distribution_type": "discrete"
        }

    # 2. Continuous Data: Percentile-based approach (Handles Skewness & Spacing naturally)
    baseline = np.median(data_points) # 50th percentile

    # Use 90th or 95th percentile for Bad, 99th for Very Bad
    bad_threshold = np.percentile(data_points, 95)

    # Safety check: ensure thresholds don't collapse on top of each other if data is tightly bound
    if bad_threshold == baseline:
        bad_threshold = baseline + 1e-6

    return {
        "baseline": float(baseline),
        "bad_threshold": float(bad_threshold),
        "distribution_type": "percentile_based"
    }

def get_first_table(conn):
    tables_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
    if tables_df.empty:
        return None
    return tables_df['name'].iloc[0]

def get_latest_threshold_timestamps(thresholds_db_path):
    """
    Reads the existing thresholds database and returns a dictionary of the most
    recent timestamp for each (threshold_id, test_plan, layer, metric) combination.
    """
    latest_timestamps = {}
    if not os.path.exists(thresholds_db_path):
        return latest_timestamps

    try:
        conn = sqlite3.connect(thresholds_db_path)
        # Check if the thresholds table exists yet
        tables_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='thresholds';", conn)
        if not tables_df.empty:
            # Get the max timestamp for every unique combo
            query = """
                SELECT threshold_id, test_plan, layer, metric, MAX(timestamp) as max_ts
                FROM thresholds
                GROUP BY threshold_id, test_plan, layer, metric
            """
            df = pd.read_sql_query(query, conn)
            for _, row in df.iterrows():
                key = (row['threshold_id'], row['test_plan'], row['layer'], row['metric'])
                latest_timestamps[key] = row['max_ts']
    except Exception as e:
        print(f"Warning: Could not read existing thresholds to check age: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    return latest_timestamps

def process_databases(runs_db_path, metrics_db_path, thresholds_db_path, update_days, runs_table=None, metrics_table=None):
    # 1. Connect and parse the Runs Database
    print(f"Loading runs database: {runs_db_path}...")
    try:
        runs_conn = sqlite3.connect(runs_db_path)
        if not runs_table:
            runs_table = get_first_table(runs_conn)

        # Filter for 'all' or 'dltest' right in the SQL query to save memory
        query = f"SELECT node, test, timestamp, pytorch_version, cuda_version FROM '{runs_table}' WHERE test IN ('all', 'dltest')"
        runs_df = pd.read_sql_query(query, runs_conn)
    except Exception as e:
        print(f"Failed to load runs database: {e}")
        return
    finally:
        if 'runs_conn' in locals() and runs_conn:
            runs_conn.close()

    if runs_df.empty:
        print("No matching runs ('all' or 'dltest') found in the database.")
        return

    # 2. Construct the cross-database keys
    # Run key format: dltest-<node>-<timestamp>
    runs_df['run_key'] = 'dltest-' + runs_df['node'].astype(str) + '-' + runs_df['timestamp'].astype(str)
    # Threshold ID format modified to include B200 prefix
    runs_df['threshold_id'] = 'B200-' + runs_df['pytorch_version'].astype(str) + '-' + runs_df['cuda_version'].astype(str)

    run_key_to_id = dict(zip(runs_df['run_key'], runs_df['threshold_id']))
    unique_run_keys = tuple(runs_df['run_key'].unique())

    # 3. Connect and query the Metrics Database
    print(f"Loading metrics database: {metrics_db_path}...")
    try:
        metrics_conn = sqlite3.connect(metrics_db_path)
        if not metrics_table:
            metrics_table = get_first_table(metrics_conn)

        # Dynamically build the IN clause depending on the number of keys
        if len(unique_run_keys) == 1:
            placeholders = "?"
            params = (unique_run_keys[0],)
        else:
            placeholders = ",".join("?" * len(unique_run_keys))
            params = unique_run_keys

        # Included 'test_plan' in the SELECT query
        metrics_query = f"SELECT run_key, node, rank, task_name, metric_name, metric_value, test_plan FROM '{metrics_table}' WHERE run_key IN ({placeholders})"
        metrics_df = pd.read_sql_query(metrics_query, metrics_conn, params=params)
    except Exception as e:
        print(f"Failed to load metrics database: {e}")
        return
    finally:
        if 'metrics_conn' in locals() and metrics_conn:
            metrics_conn.close()

    if metrics_df.empty:
        print("No matching metrics data found for the generated run keys.")
        return

    # Map the threshold ID back onto the metrics data
    metrics_df['threshold_id'] = metrics_df['run_key'].map(run_key_to_id)
    metrics_df = metrics_df.dropna(subset=['threshold_id', 'metric_value'])

    print(f"Processing metrics and calculating thresholds...\n")

    # Accumulators
    thresholds_records = []
    all_deviations = []
    current_time = int(time.time())
    age_limit_seconds = update_days * 24 * 60 * 60

    # Fetch latest timestamps to determine if a new calculation is needed
    latest_timestamps = get_latest_threshold_timestamps(thresholds_db_path)

    # 4. Group by environment ID, Layer, and Metric
    grouped = metrics_df.groupby(['threshold_id', 'task_name', 'metric_name'])

    for (t_id, layer, metric), group in grouped:
        is_numerical_test = any(keyword in metric for keyword in NUMERICAL_KEYWORDS)
        all_vals = group['metric_value'].tolist()

        # Grab test_plan from the first row of this group (assuming it's consistent for the task/metric)
        test_plan = group['test_plan'].iloc[0] if 'test_plan' in group.columns else "UNKNOWN"

        if is_numerical_test:
            most_common_val, _ = Counter(all_vals).most_common(1)[0]
            for _, row in group.iterrows():
                dev_val = row['metric_value']
                if dev_val != most_common_val:
                    all_deviations.append({
                        'threshold_id': t_id,
                        'test_plan': test_plan,
                        'node': row.get('node', 'UNKNOWN'),
                        'rank': row.get('rank', 'UNKNOWN'),
                        'layer': layer,
                        'test': metric,
                        'val': dev_val,
                        'diff': dev_val - most_common_val,
                        'expected': most_common_val
                    })
        else:
            # Check if we actually need to calculate and insert a new threshold based on age
            key = (t_id, test_plan, layer, metric)
            last_ts = latest_timestamps.get(key, 0)

            if (current_time - last_ts) > age_limit_seconds:
                data_points = np.array(all_vals)

                # Optional trim to remove astronomical anomalies before calculating
                lower_bound = np.percentile(data_points, 1)
                upper_bound = np.percentile(data_points, 99)
                filtered_data = data_points[(data_points >= lower_bound) & (data_points <= upper_bound)]

                if len(filtered_data) == 0:
                    print(f"Skipping {layer}/{metric}: Trimming removed all data.")
                    continue

                # Calculate dynamic routed thresholds
                thresholds_data = calculate_robust_thresholds(filtered_data)

                # Save record for the SQLite Thresholds DB
                thresholds_records.append({
                    'threshold_id': t_id,
                    'test_plan': test_plan,
                    'timestamp': current_time,
                    'layer': layer,
                    'metric': metric,
                    'baseline': thresholds_data['baseline'],
                    'bad_threshold': thresholds_data['bad_threshold'],
                    'distribution_type': thresholds_data['distribution_type']
                })

    # 5. Save Thresholds to DB
    if thresholds_records:
        try:
            print(f"Saving {len(thresholds_records)} NEW threshold records to {thresholds_db_path}...")
            thresholds_df = pd.DataFrame(thresholds_records)
            t_conn = sqlite3.connect(thresholds_db_path)
            # if_exists='append' ensures we add to the table if it already exists rather than overwriting
            thresholds_df.to_sql('thresholds', t_conn, if_exists='append', index=False)
            t_conn.close()
            print("Successfully saved thresholds to database.")
        except Exception as e:
            print(f"Failed to save to thresholds database: {e}")
    else:
        print("No new thresholds needed saving (all existing thresholds are within the update window).")

    # 6. Final Reporting
    print("\n=== Numerical Tests Summary ===")
    if not all_deviations:
        print("Status: SUCCESS. All runs across all nodes matched the most common value for every numerical test.")
    else:
        print(f"Found {len(all_deviations)} deviating instances:")
        print("-" * 50)
        for dev in all_deviations:
            print(f"Env:    {dev['threshold_id']}")
            print(f"Plan:   {dev['test_plan']}")
            print(f"Node:   {dev['node']} (GPU Rank: {dev['rank']})")
            print(f"Layer:  {dev['layer']}")
            print(f"Test:   {dev['test']}")
            print(f"Value:  {dev['val']} (Diff: {dev['diff']:+f})")
            print(f"Note:   Expected baseline: {dev['expected']}")
            print("-" * 50)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cross-reference runs and metrics DBs to calculate thresholds.")
    parser.add_argument("runs_db", help="Path to the DB containing the test run information.")
    parser.add_argument("metrics_db", help="Path to the DB containing the detailed metric values.")
    parser.add_argument("thresholds_db", help="Path to the output SQLite database to save the calculated thresholds.")

    # New argument to handle threshold update frequency
    parser.add_argument("--update-days", type=int, default=14,
                        help="Number of days before a new threshold is calculated and added to the DB (default: 14)")

    parser.add_argument("--runs-table", default=None, help="Target table in the runs DB (defaults to the first table).")
    parser.add_argument("--metrics-table", default=None, help="Target table in the metrics DB (defaults to the first table).")

    args = parser.parse_args()

    process_databases(
        runs_db_path=args.runs_db,
        metrics_db_path=args.metrics_db,
        thresholds_db_path=args.thresholds_db,
        update_days=args.update_days,
        runs_table=args.runs_table,
        metrics_table=args.metrics_table
    )