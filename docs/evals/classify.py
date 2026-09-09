import os
import sqlite3
import argparse
import pandas as pd
from collections import Counter

# The expected metric databases to check
TARGET_DATABASES = [
    "dltest_collective_performance.db",
    "dltest_compute_performance.db",
    "dltest_overlap_performance.db",
    "dltest_numerical_correctness.db"
]

NUMERICAL_KEYWORDS = ["norm", "bias", "weight"]

def get_first_table(conn):
    """Helper to get the first table name in an SQLite database."""
    tables_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
    if tables_df.empty:
        return None
    return tables_df['name'].iloc[0]

def parse_run_key(run_key):
    """
    Extracts the node and timestamp from a run_key.
    Expected format: dltest-<node_part1>-...-<timestamp>
    """
    parts = run_key.split('-')
    if len(parts) < 3 or parts[0] != 'dltest':
        raise ValueError(f"Invalid run_key format: {run_key}. Expected 'dltest-<node>-<timestamp>'")

    timestamp = parts[-1]
    node = '-'.join(parts[1:-1])
    return node, timestamp

def get_threshold_id(validation_db_path, node, timestamp):
    """Queries validation.db to find pytorch and cuda versions. Returns None if not found."""
    if not os.path.exists(validation_db_path):
        print(f"Warning: {validation_db_path} not found. Falling back to most recent global thresholds.")
        return None

    print(f"Querying {validation_db_path} for Node: {node}, Timestamp: {timestamp}...")
    conn = sqlite3.connect(validation_db_path)

    try:
        val_table = get_first_table(conn)
        if not val_table:
            print("Warning: No tables found in validation.db. Falling back to global thresholds.")
            return None

        query = f"SELECT pytorch_version, cuda_version FROM '{val_table}' WHERE node = ? AND timestamp = ?"
        df = pd.read_sql_query(query, conn, params=(node, timestamp))

        if df.empty:
            print(f"Warning: No entry found in validation.db for node {node} at timestamp {timestamp}. Falling back to global thresholds.")
            return None

        pytorch_version = df['pytorch_version'].iloc[0]
        cuda_version = df['cuda_version'].iloc[0]

        threshold_id = f"B200-{pytorch_version}-{cuda_version}"
        print(f"Matched versions -> Target threshold_id: {threshold_id}")
        return threshold_id
    except Exception as e:
        print(f"Error reading validation.db: {e}. Falling back to global thresholds.")
        return None
    finally:
        conn.close()

def get_latest_thresholds(thresholds_db_path, target_threshold_id):
    """
    Fetches thresholds. Prefers the exact target_threshold_id.
    If missing, falls back to the most recent global threshold for that test_plan/layer/metric.
    """
    print(f"Loading thresholds from {thresholds_db_path}...")
    conn = sqlite3.connect(thresholds_db_path)

    try:
        query = "SELECT threshold_id, test_plan, layer, metric, baseline, bad_threshold, timestamp FROM thresholds"
        df = pd.read_sql_query(query, conn)

        if df.empty:
            print("Warning: Thresholds database is completely empty.")
            return {}

        # Sort by timestamp ascending (so keep='last' retains the most recent)
        df = df.sort_values('timestamp')

        # 1. Get the most recent threshold for EVERY metric across all threshold_ids (Global Fallback)
        global_latest = df.drop_duplicates(subset=['test_plan', 'layer', 'metric'], keep='last')

        # 2. Get the most recent threshold specifically for the target_threshold_id
        if target_threshold_id:
            specific_latest = df[df['threshold_id'] == target_threshold_id].drop_duplicates(subset=['test_plan', 'layer', 'metric'], keep='last')
        else:
            specific_latest = pd.DataFrame()

        thresholds_dict = {}

        # Populate with global fallbacks first
        for _, row in global_latest.iterrows():
            key = (row['test_plan'], row['layer'], row['metric'])
            thresholds_dict[key] = {
                'baseline': row['baseline'],
                'bad_threshold': row['bad_threshold'],
                'source_id': row['threshold_id']
            }

        # Overwrite with specific target_threshold_id matches if they exist
        specific_count = 0
        if not specific_latest.empty:
            for _, row in specific_latest.iterrows():
                key = (row['test_plan'], row['layer'], row['metric'])
                thresholds_dict[key] = {
                    'baseline': row['baseline'],
                    'bad_threshold': row['bad_threshold'],
                    'source_id': row['threshold_id']
                }
                specific_count += 1

        print(f"Loaded {len(thresholds_dict)} total thresholds ({specific_count} exact matches, {len(thresholds_dict) - specific_count} global fallbacks).")
        return thresholds_dict
    finally:
        conn.close()

def evaluate_run(run_key, data_dir, thresholds_dict, output_db_path):
    """Iterates through metric DBs, compares against thresholds, and saves categorizations."""
    categorizations = []

    for db_name in TARGET_DATABASES:
        db_path = os.path.join(data_dir, db_name)
        if not os.path.exists(db_path):
            print(f"Skipping {db_name} (File not found in {data_dir})")
            continue

        print(f"Scanning metrics in {db_name}...")
        conn = sqlite3.connect(db_path)
        try:
            table_name = get_first_table(conn)
            if not table_name:
                continue

            query = f"SELECT run_key, test_plan, node, rank, task_name, metric_name, metric_value FROM '{table_name}' WHERE run_key = ?"
            df = pd.read_sql_query(query, conn, params=(run_key,))

            # Group by task and metric to handle the numerical correctness dynamically
            grouped = df.groupby(['test_plan', 'task_name', 'metric_name'])

            for (test_plan, task_name, metric_name), group in grouped:
                is_numerical = any(keyword in metric_name for keyword in NUMERICAL_KEYWORDS)

                if is_numerical:
                    # Dynamically calculate baseline (most common value across ranks) for this run
                    all_vals = group['metric_value'].tolist()
                    dynamic_baseline = Counter(all_vals).most_common(1)[0][0]

                    for _, row in group.iterrows():
                        val = row['metric_value']
                        tolerance = 0.01 * abs(dynamic_baseline)

                        # Numerical logic: within 1% is "good", otherwise "bad"
                        if abs(val - dynamic_baseline) <= tolerance:
                            category = "good"
                        else:
                            category = "bad"

                        categorizations.append({
                            "run_key": row['run_key'],
                            "test_plan": row['test_plan'],
                            "node": row['node'],
                            "rank": row['rank'],
                            "task_name": row['task_name'],
                            "metric_name": row['metric_name'],
                            "metric_value": val,
                            "baseline": dynamic_baseline,
                            "bad_threshold": dynamic_baseline + tolerance,
                            "threshold_source_id": "DYNAMIC_1_PERCENT",
                            "category": category,
                            "source_db": db_name
                        })
                else:
                    # Standard performance metric logic
                    key = (test_plan, task_name, metric_name)
                    threshold_info = thresholds_dict.get(key)

                    for _, row in group.iterrows():
                        val = row['metric_value']

                        if not threshold_info:
                            category = "MISSING_THRESHOLD"
                            baseline, bad_threshold, source_id = None, None, None
                        else:
                            baseline = threshold_info['baseline']
                            bad_threshold = threshold_info['bad_threshold']
                            source_id = threshold_info['source_id']

                            # Tiered timing logic: excellent, good, bad
                            if val >= bad_threshold:
                                category = "bad"
                            elif val >= baseline:
                                category = "good"
                            else:
                                category = "excellent"

                        categorizations.append({
                            "run_key": row['run_key'],
                            "test_plan": row['test_plan'],
                            "node": row['node'],
                            "rank": row['rank'],
                            "task_name": row['task_name'],
                            "metric_name": row['metric_name'],
                            "metric_value": val,
                            "baseline": baseline,
                            "bad_threshold": bad_threshold,
                            "threshold_source_id": source_id,
                            "category": category,
                            "source_db": db_name
                        })
        except Exception as e:
            print(f"Error reading {db_name}: {e}")
        finally:
            conn.close()

    if not categorizations:
        print("No metrics found matching the provided run_key.")
        return

    # Save to output database
    print(f"\nSaving {len(categorizations)} evaluated metrics to {output_db_path}...")
    out_df = pd.DataFrame(categorizations)

    out_conn = sqlite3.connect(output_db_path)
    try:
        out_df.to_sql('categorizations', out_conn, if_exists='replace', index=False)
        print("Successfully generated categorization database.")
    finally:
        out_conn.close()

    # Summary
    summary = out_df['category'].value_counts().to_dict()
    print("\n=== Categorization Summary ===")
    for cat, count in summary.items():
        print(f"{cat}: {count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Categorize a specific run's metrics against database thresholds.")
    parser.add_argument("run_key", help="The specific run_key to evaluate (e.g., dltest-slc01-cl02-hgx-0001-1781697530)")
    parser.add_argument("data_dir", help="Directory containing the target metric databases")
    parser.add_argument("validation_db", help="Path to validation.db")
    parser.add_argument("thresholds_db", help="Path to thresholds.db")
    parser.add_argument("output_db", help="Path to save the new categorization database")

    args = parser.parse_args()

    try:
        node, timestamp = parse_run_key(args.run_key)

        # 1. Attempt to get target threshold_id, safely returns None if missing/failed
        target_threshold_id = get_threshold_id(args.validation_db, node, timestamp)

        # 2. Load thresholds, using exact match first, falling back to global recent otherwise
        thresholds = get_latest_thresholds(args.thresholds_db, target_threshold_id)

        # 3. Evaluate metrics and create categorization DB
        evaluate_run(args.run_key, args.data_dir, thresholds, args.output_db)

    except Exception as e:
        print(f"\nExecution Failed: {e}")