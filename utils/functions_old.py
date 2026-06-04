import subprocess
import json
import os
import sys
import textwrap
import logging
import datetime
import random

# Configuration variables
DEFAULT_NAMESPACE = "gcr-admin"
DEFAULT_POD = "gcr-admin-pvc-access"
DEFAULT_DB_PATH = "/data/continuous_validation/metadata/validation.db"
JOB_GROUP_LABEL = "hari-gcr-ceval"


def run_command(command, shell=False, check=True):
    """Executes a shell command and returns stdout."""
    try:
        if shell:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=check)
        else:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check)
        return result.stdout.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Stderr: {e.stderr.decode('utf-8')}")
        raise e

def _exec_python_on_pod(python_code, pod, namespace, args=None):
    """Helper to execute python code inside a pod."""
    cmd = ["kubectl", "exec", "-n", namespace, pod, "--", "python3", "-c", python_code]
    if args:
        cmd.extend([str(a) for a in args])
    return run_command(cmd)

def init_db(pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_DB_PATH):
    """
    Initializes the database schema remotely on the pod.
    """
    code = textwrap.dedent(f"""
    import sqlite3, os, sys, socket

    print(f'Running initialization inside pod: {{socket.gethostname()}}')
    db_path = '{db_path}'
    print(f'Target DB path: {{db_path}}')

    try:
        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            print(f'Creating directory: {{db_dir}}')
            os.makedirs(db_dir, exist_ok=True)
        else:
            print(f'Directory {{db_dir}} already exists.')

        conn = sqlite3.connect(db_path)
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        # Table runs
        conn.execute("CREATE TABLE IF NOT EXISTS runs (node TEXT NOT NULL, test TEXT NOT NULL, timestamp INTEGER NOT NULL, result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete')));")
        # Index on runs
        conn.execute('CREATE INDEX IF NOT EXISTS idx_runs_node_test_ts ON runs(node, test, timestamp);')
        # View latest_status
        conn.execute("CREATE VIEW IF NOT EXISTS latest_status AS SELECT r.node, r.test, r.timestamp AS latest_timestamp, r.result FROM runs r JOIN (SELECT node, test, MAX(timestamp) AS max_ts FROM runs GROUP BY node, test) x ON r.node=x.node AND r.test=x.test AND r.timestamp=x.max_ts;")
        conn.commit()
        print(f'Successfully initialized DB at {{db_path}}')
    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace)



# ==========================================
# FLOW STEP 1: Get Free Node List
# ==========================================

def get_free_node_list():
    """
    Returns a list of node names that have ALL GPUs free.
    Strictly returns nodes where free count == allocatable count (e.g., 8/8 free).
    """
    nodes, _ = get_free_nodes()
    # STRICT FILTER: Only return nodes where free == alloc (completely empty)
    return [n['node'] for n in nodes if n['free'] == n['alloc'] and n['alloc'] > 0]

def get_free_nodes(verbose=False):
    """
    Returns details about free nodes (capacity, allocated, used, free).
    Equivalent to: kubectl/cluster/freenodes.sh
    """
    # 1. Get Pods JSON
    cmd_pods = ["kubectl", "get", "pods", "-A", "-o", "json"]
    pods_json = json.loads(run_command(cmd_pods))
    
    node_usage = {}
    
    for pod in pods_json.get('items', []):
        node_name = pod.get('spec', {}).get('nodeName')
        if not node_name:
            continue
        
        phase = pod.get('status', {}).get('phase')
        if phase in ["Succeeded", "Failed"]:
            continue
            
        containers = pod.get('spec', {}).get('containers', [])
        init_containers = pod.get('spec', {}).get('initContainers', [])
        
        app_req = sum(int(c.get('resources', {}).get('requests', {}).get('nvidia.com/gpu', 0)) for c in containers)
        # Handle init containers (use max)
        init_reqs = [int(c.get('resources', {}).get('requests', {}).get('nvidia.com/gpu', 0)) for c in init_containers]
        init_req = max(init_reqs) if init_reqs else 0
        
        # Effective GPU usage for the pod is max(app_req, init_req)
        usage = max(app_req, init_req)
        node_usage[node_name] = node_usage.get(node_name, 0) + usage

    # 2. Get Nodes and calculate free
    # NOTE: Used raw string r"" here to fix SyntaxWarning with backslashes
    cmd_nodes = ["kubectl", "get", "nodes", "--no-headers", "-o", r"custom-columns=NAME:.metadata.name,CAP:.status.capacity.nvidia\.com/gpu,ALLOC:.status.allocatable.nvidia\.com/gpu"]
    # We use check=False because if no nodes match or grep fails elsewhere it could throw
    nodes_output = run_command(cmd_nodes, check=False) 
    
    results = []
    totals = {'cap': 0, 'alloc': 0, 'used': 0, 'free': 0}
    
    for line in nodes_output.split('\n'):
        if not line.strip():
            continue
        
        # Filter for HGX nodes
        if 'hgx' not in line: 
            continue
            
        parts = line.split()
        if len(parts) < 3:
            continue
            
        name = parts[0]
        cap_str = parts[1]
        cap = int(cap_str) if cap_str.isdigit() else 0

        alloc_str = parts[2]
        alloc = int(alloc_str) if alloc_str.isdigit() else 0
        
        used = node_usage.get(name, 0)
        free = alloc - used
        
        # Add to results
        results.append({
            'node': name, 'cap': cap, 'alloc': alloc, 'used': used, 'free': free
        })
        totals['cap'] += cap
        totals['alloc'] += alloc
        totals['used'] += used
        totals['free'] += free
            
    return results, totals


# ==========================================
# FLOW STEP 2: Get DB Latest Status
# ==========================================

def get_db_latest_status(pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_DB_PATH):
    """
    Fetches status from the database inside the pod.
    Returns a string table of results.
    """
    code = textwrap.dedent(f"""
    import sqlite3, datetime, sys
    db_path = '{db_path}'
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        # Create table if not exists to avoid errors on fresh start
        conn.execute("CREATE TABLE IF NOT EXISTS latest_status (node TEXT, test TEXT, latest_timestamp INTEGER, result TEXT, PRIMARY KEY (node, test))")
        rows = conn.execute('SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test').fetchall()

        print('node\\ttest\\tlatest_timestamp_num\\tlatest_timestamp\\tresult')
        for r in rows:
            ts_num = int(r['latest_timestamp']) if r['latest_timestamp'] is not None else ''
            ts_iso = ''
            if r['latest_timestamp'] is not None:
                ts_iso = datetime.datetime.fromtimestamp(
                    r['latest_timestamp'],
                    tz=datetime.timezone.utc
                ).replace(microsecond=0).isoformat().replace('+00:00', 'Z')

            print(f"{{r['node']}}\\t{{r['test']}}\\t{{ts_num}}\\t{{ts_iso}}\\t{{r['result']}}")
    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace)

def parse_db_status_output(output_string):
    """
    Helper: Parses the string output from get_db_latest_status into a Dictionary.
    Returns: { 'node_name': latest_timestamp_int, ... }
    Used to bridge Step 2 and Step 3.
    """
    status_map = {}
    lines = output_string.strip().split('\n')
    # Skip header if present
    if lines and 'node' in lines[0] and 'timestamp' in lines[0]:
        lines = lines[1:]
        
    for line in lines:
        parts = line.split('\t')
        if len(parts) >= 3:
            node = parts[0]
            ts_str = parts[2] # latest_timestamp_num
            
            if ts_str and ts_str.isdigit():
                ts = int(ts_str)
                # If multiple tests exist for a node, we might want the oldest or newest. 
                # For prioritization, we usually want the *most recent* activity to determine if it needs testing.
                # If we want to find nodes that haven't been tested recently, we look at the most recent test.
                current_max = status_map.get(node, 0)
                if ts > current_max:
                    status_map[node] = ts
            else:
                # No timestamp means never run (or 0)
                if node not in status_map:
                    status_map[node] = 0
    return status_map


# ==========================================
# FLOW STEP 3: Build Priority Queue
# ==========================================

def build_priority_queue(free_nodes_list, db_latest_status_map, days_threshold=7, shuffle=False):
    """
    Constructs the job priority queue based on node age.
    
    Args:
        free_nodes_list (list): List of available node names.
        db_latest_status_map (dict): Dict {node: timestamp} of last run times.
        days_threshold (int): Nodes tested more recently than this will be skipped.
        shuffle (bool): If True, randomize the order of the queue instead of sorting by age.
    
    Returns:
        list: [[node_name, priority_rank, job_submitted_status], ...]
    """
    # Current time in UTC timestamp
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    print(f"Building priority queue at {datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).isoformat()} with threshold {days_threshold} days")
    threshold_seconds = days_threshold * 86400
    
    candidate_list = []
    
    for node in free_nodes_list:
        # Get last timestamp (0 if never tested)
        last_ts = db_latest_status_map.get(node, 0)
        
        age = now - last_ts
        
        # LOGIC:
        # 1. If last_ts is 0 (Never tested) -> High Priority
        # 2. If age > threshold -> Add to queue
        # 3. Else -> Skip (Tested recently)
        
        if last_ts == 0 or age > threshold_seconds:
            candidate_list.append({
                'node': node,
                'ts': last_ts
            })
        else:
            print(f"  Skipping node {node}: Last tested at {datetime.datetime.fromtimestamp(last_ts, tz=datetime.timezone.utc).isoformat()} (Age: {age/86400:.2f} days)")
            
    if shuffle:
        # RANDOMIZE: Use random shuffle
        random.shuffle(candidate_list)
    else:
        # SORT: Oldest timestamp first (Ascending). 
        # 0 (Never tested) will be at the top.
        candidate_list.sort(key=lambda x: x['ts'])
    
    # FORMAT OUTPUT
    # [nodename, priority_order, job_submission_status]
    priority_queue = []
    for idx, item in enumerate(candidate_list):
        priority_queue.append([item['node'], idx + 1, False])
        
    return priority_queue


# ==========================================
# FLOW STEP 4 & 5: Job Submission & Monitor
# ==========================================

def create_job(yaml_file):
    """
    Creates a Kubernetes job from a YAML file.
    """
    if not os.path.exists(yaml_file):
        raise FileNotFoundError(f"File '{yaml_file}' does not exist")
    return run_command(["kubectl", "create", "-f", yaml_file])
def get_job_status(job_name, namespace=DEFAULT_NAMESPACE):
    """
    Returns the status of a specific vcjob.
    Potential statuses: Pending, Running, Completed, Failed, Unknown
    """
    cmd = ["kubectl", "get", "vcjob", "-n", namespace, job_name, "-o", "jsonpath={.status.state.phase}"]
    try:
        status = run_command(cmd)
        return status if status else "Unknown"
    except Exception:
        return "Unknown"
def delete_job(job_name, namespace=DEFAULT_NAMESPACE):
    """
    Deletes a specific vcjob.
    """
    return run_command(["kubectl", "delete", "vcjob", "-n", namespace, job_name])

def delete_all_validation_jobs(confirm=False, namespace=DEFAULT_NAMESPACE, tag=JOB_GROUP_LABEL):
    """
    Deletes all validation jobs (containing the specified tag).
    """
    cmd_list = f'kubectl get vcjob -n {namespace} --no-headers -o custom-columns=NAME:.metadata.name | grep "{tag}"'
    try:
        jobs = run_command(cmd_list, shell=True).split('\n')
        jobs = [j.strip() for j in jobs if j.strip()]
    except subprocess.CalledProcessError:
        print("No jobs found to delete.")
        return

    if not jobs:
        print("No jobs found.")
        return

    print("Found jobs to delete:", jobs)
    if not confirm:
        response = input("Do you want to delete these jobs? (y/N): ")
        if response.lower() != 'y':
            print("Operation cancelled.")
            return

    for job in jobs:
        print(f"Deleting job: {job}")
        try:
            run_command(["kubectl", "delete", "vcjob", "-n", namespace, job])
        except Exception as e:
            print(f"Failed to delete {job}: {e}")
    print("Deletion completed.")


# ==========================================
# FLOW STEP 6: Job Execution (Inside Pod)
# ==========================================


def add_result_local(node, test, result, timestamp=None, db_path=DEFAULT_DB_PATH):
    """
    Adds a result to the database (Local Execution).
    NFS/PVC-safe: uses DELETE journaling + busy_timeout to reduce locking issues.
    """
    import os
    import sqlite3
    import datetime

    # --- Timestamp normalization ---
    if timestamp is None:
        timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    else:
        if isinstance(timestamp, str):
            ts = timestamp.strip()
            if ts.isdigit():
                timestamp = int(ts)
            else:
                # Minimal parsing attempt
                if ts.endswith('Z'):
                    ts = ts[:-1] + '+00:00'
                d = datetime.datetime.fromisoformat(ts)
                if d.tzinfo is None:
                    d = d.replace(tzinfo=datetime.timezone.utc)
                timestamp = int(d.timestamp())
        else:
            # e.g. float or numpy types
            timestamp = int(timestamp)

    # --- DB path normalization ---
    db_path = os.path.abspath(str(db_path).strip())
    db_dir = os.path.dirname(db_path) or "."
    os.makedirs(db_dir, exist_ok=True)

    conn = None
    try:
        # Explicitly open read-write-create; add a connect timeout for lock waits
        conn = sqlite3.connect(f"file:{db_path}?mode=rwc", uri=True, timeout=30)

        # NFS/PVC friendly pragmas
        conn.execute("PRAGMA busy_timeout=30000;")   # 30s wait on locks
        conn.execute("PRAGMA journal_mode=DELETE;")  # avoid WAL on NFS
        conn.execute("PRAGMA synchronous=FULL;")

        # Ensure tables exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
              node TEXT NOT NULL,
              test TEXT NOT NULL,
              timestamp INTEGER NOT NULL,
              result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete'))
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_node_test_ts ON runs(node, test, timestamp);")

        conn.execute(
            "INSERT INTO runs(node, test, timestamp, result) VALUES (?,?,?,?)",
            (node, test, int(timestamp), result),
        )

        conn.commit()
        print(f"Added: {node} {test} {result} {timestamp}")

    except Exception as e:
        print(f"Error adding result: {e} (db_path={db_path!r})")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass



# ==========================================
# UTILITY FUNCTIONS (Unused in main flow)
# ==========================================

def get_cordoned_nodes():
    """Returns a list of cordoned nodes."""
    cmd = 'kubectl get nodes -o wide | grep -E "NAME|SchedulingDisabled|Ready.*SchedulingDisabled"'
    return run_command(cmd, shell=True, check=False)

def get_node_status(node, pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_DB_PATH):
    """Fetches status for a specific node."""
    code = textwrap.dedent(f"""
    import sqlite3, datetime, sys
    db_path = '{db_path}'
    node_filter = sys.argv[1]
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        q = 'SELECT node, test, latest_timestamp, result FROM latest_status WHERE node = ? ORDER BY node, test'
        rows = conn.execute(q, (node_filter,)).fetchall()
        print('node\\ttest\\tlatest_timestamp\\tresult')
        for r in rows:
            ts = datetime.datetime.fromtimestamp(r['latest_timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
            print(f"{{r['node']}}\\t{{r['test']}}\\t{{ts}}\\t{{r['result']}}")
    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace, args=[node])

def get_history(limit=20, pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_DB_PATH):
    """Fetches run history."""
    code = textwrap.dedent(f"""
    import sqlite3, datetime, sys
    db_path = '{db_path}'
    limit = int(sys.argv[1])
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT node, test, timestamp, result FROM runs ORDER BY timestamp DESC LIMIT ?', (limit,)).fetchall()
        print('node\\ttest\\ttimestamp\\tresult')
        for r in rows:
            ts = datetime.datetime.fromtimestamp(r['timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
            print(f"{{r['node']}}\\t{{r['test']}}\\t{{ts}}\\t{{r['result']}}")
    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace, args=[limit])

def list_pod_files(target_dir="/data/continuous_validation", pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE):
    """Lists files in a remote directory."""
    return run_command(["kubectl", "-n", namespace, "exec", pod, "--", "ls", "-F", target_dir])

def exec_pod(pod_name, namespace=DEFAULT_NAMESPACE):
    """Start interactive shell in pod."""
    print(f"Starting interactive session in {pod_name}...")
    subprocess.call(["kubectl", "exec", "-it", pod_name, "-n", namespace, "--", "/bin/bash"])


# ==========================================
# test db update script
# ==========================================

import textwrap

def init_storage_db(pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path="/data/continuous_validation/test_storage.db"):
    """
    Initializes the storage performance database and percentile ranking view remotely.
    """
    code = textwrap.dedent(f"""
    import sqlite3, os, sys, socket

    print(f'Running initialization inside pod: {{socket.gethostname()}}')
    db_path = '{db_path}'
    print(f'Target DB path: {{db_path}}')

    try:
        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            print(f'Creating directory: {{db_dir}}')
            os.makedirs(db_dir, exist_ok=True)
        else:
            print(f'Directory {{db_dir}} already exists.')

        conn = sqlite3.connect(db_path)
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')

        # 1. Create Main Performance Table
        # Stores 12 metrics (6 tests * 2 metrics each) per node run
        conn.execute('''
            CREATE TABLE IF NOT EXISTS storage_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                
                -- Test 1: Sequential Read (QD128)
                iodepth_read_1file_iops REAL,
                iodepth_read_1file_bw REAL,
                
                -- Test 2: Sequential Write (QD128)
                iodepth_write_1file_iops REAL,
                iodepth_write_1file_bw REAL,
                
                -- Test 3: Aggregate Read (Numjobs)
                numjobs_read_nfiles_iops REAL,
                numjobs_read_nfiles_bw REAL,
                
                -- Test 4: Aggregate Write (Numjobs)
                numjobs_write_nfiles_iops REAL,
                numjobs_write_nfiles_bw REAL,
                
                -- Test 5: Random Read (4k)
                randread_iops REAL,
                randread_bw REAL,
                
                -- Test 6: Random Write (4k)
                randwrite_iops REAL,
                randwrite_bw REAL,

                PRIMARY KEY (node, timestamp)
            );
        ''')

        # 2. Create Index for fast lookups
        conn.execute('CREATE INDEX IF NOT EXISTS idx_perf_node_ts ON storage_performance(node, timestamp);')

        # 3. Create View with Percentile Rankings (Compare node vs peers)
        # We use a CTE to find the latest timestamp for each node, then join back to get data, 
        # then apply PERCENT_RANK() window function.
        conn.execute('''
            CREATE VIEW IF NOT EXISTS latest_node_performance_stats AS
            WITH latest_runs AS (
                SELECT node, MAX(timestamp) as max_ts
                FROM storage_performance
                GROUP BY node
            ),
            current_stats AS (
                SELECT sp.*
                FROM storage_performance sp
                JOIN latest_runs lr ON sp.node = lr.node AND sp.timestamp = lr.max_ts
            )
            SELECT
                node,
                timestamp AS latest_timestamp,
                
                -- Seq Read Stats & Pct
                iodepth_read_1file_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_read_1file_iops), 2) as iodepth_read_1file_iops_pct,
                iodepth_read_1file_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_read_1file_bw), 2) as iodepth_read_1file_bw_pct,

                -- Seq Write Stats & Pct
                iodepth_write_1file_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_write_1file_iops), 2) as iodepth_write_1file_iops_pct,
                iodepth_write_1file_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_write_1file_bw), 2) as iodepth_write_1file_bw_pct,

                -- Agg Read Stats & Pct
                numjobs_read_nfiles_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_read_nfiles_iops), 2) as numjobs_read_nfiles_iops_pct,
                numjobs_read_nfiles_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_read_nfiles_bw), 2) as numjobs_read_nfiles_bw_pct,

                -- Agg Write Stats & Pct
                numjobs_write_nfiles_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_write_nfiles_iops), 2) as numjobs_write_nfiles_iops_pct,
                numjobs_write_nfiles_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_write_nfiles_bw), 2) as numjobs_write_nfiles_bw_pct,

                -- Rand Read Stats & Pct
                randread_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY randread_iops), 2) as randread_iops_pct,
                randread_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY randread_bw), 2) as randread_bw_pct,

                -- Rand Write Stats & Pct
                randwrite_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY randwrite_iops), 2) as randwrite_iops_pct,
                randwrite_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY randwrite_bw), 2) as randwrite_bw_pct

            FROM current_stats;
        ''')

        conn.commit()
        print(f'Successfully initialized Storage DB at {{db_path}}')
        
    except Exception as e:
        print(f'Error initializing DB: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    
    return _exec_python_on_pod(code, pod, namespace)


# ==========================================
# MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Cluster evaluation Kubectl Functions CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Command: Help
    subparsers.add_parser("help", help="Show detailed usage examples")

    # Command: Free Nodes
    p_free = subparsers.add_parser("freenodes", help="List free nodes in table format")

    # Command: LS
    p_ls = subparsers.add_parser("ls", help="List remote files")
    p_ls.add_argument("path", nargs="?", default="/data/continuous_validation", help="Remote path to list")

    # Command: Exec
    p_exec = subparsers.add_parser("exec", help="Exec into a pod")
    p_exec.add_argument("pod_name", nargs="?", default=DEFAULT_POD, help="Name of the pod")
    p_exec.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE, help="Namespace")

    # Command: Status
    p_status = subparsers.add_parser("status", help="Get DB latest status table")

    # Command: History
    p_hist = subparsers.add_parser("history", help="Get DB run history")
    p_hist.add_argument("limit", nargs="?", default="20", help="Number of rows")

    #c Command: delete-jobs
    p_delete = subparsers.add_parser("delete-jobs", help="Delete all validation jobs")
    p_delete.add_argument("--confirm", action="store_true", help="Confirm deletion without prompt")
    p_delete.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE, help="Namespace")
    p_delete.add_argument("--tag", default=JOB_GROUP_LABEL, help="Tag to filter jobs for deletion")

    # Command: add-result
    p_add = subparsers.add_parser("add-result", help="Add a test result to the local DB")
    p_add.add_argument("node", help="Node name")
    p_add.add_argument("test", help="Test name")
    p_add.add_argument("result", help="Result (pass/fail)")
    p_add.add_argument("timestamp", nargs="?", default=None, help="Timestamp (Unix or ISO)")
    p_add.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to DB")

    # Command: init-db
    p_init = subparsers.add_parser("init-db", help="Initialize remote DB schema")
    p_init.add_argument("--pod", default=DEFAULT_POD, help="Pod to run init on")
    p_init.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE, help="Namespace")
    p_init.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Db path")

    args = parser.parse_args()

    # --- HANDLERS ---

    if args.command == "freenodes":

        nodes, totals = get_free_nodes()
        fmt = "{:<30} {:<6} {:<6} {:<6} {:<6}"
        print("\n" + fmt.format("NODE NAME", "CAP", "ALLOC", "USED", "FREE"))
        print("-" * 60)
        
        if not nodes:
            print("No free nodes found.")
        else:
            for n in nodes:
                # Show all HGX nodes in table
                if n['free'] >= 0:
                    print(fmt.format(n['node'], n['cap'], n['alloc'], n['used'], n['free']))
            print("-" * 60)
            print(fmt.format("TOTAL", totals['cap'], totals['alloc'], totals['used'], totals['free']) + "\n")

    elif args.command == "ls":
        print(list_pod_files(target_dir=args.path))
        
    elif args.command == "exec":
        exec_pod(args.pod_name, namespace=args.namespace)

    elif args.command == "status":
        print(get_db_latest_status())
        
    elif args.command == "history":
        print(get_history(limit=args.limit))
    
    elif args.command == "delete-jobs":
        delete_all_validation_jobs(confirm=args.confirm, namespace=args.namespace, tag=args.tag)

    elif args.command == "add-result":
        add_result_local(args.node, args.test, args.result, args.timestamp, args.db_path)

    elif args.command == "init-db":
        print(init_db(args.pod, args.namespace, args.db_path))

    elif args.command == "help" or args.command is None:
        print("\n" + "="*60)
        print(" CLUSTER VALIDATIONS FUNCTIONS - USAGE GUIDE")
        print("="*60)
        print("\n[CLI USAGE] (Run from terminal)")
        print("  python3 functions.py freenodes      # List free nodes table")
        print("  python3 functions.py status         # View latest DB status")
        print("  python3 functions.py history 50     # View last 50 runs")
        print("  python3 functions.py ls /tmp        # List remote files")
        print("  python3 functions.py exec <pod>     # SSH into pod")

        print("\n" + "="*60)
        print("[PYTHON USAGE] (Import in Job Runner Notebook)")
        print("="*60)
        
        print("\n1. GET FREE NODES (Strictly empty 8/8)")
        print("   from kubectl import functions")
        print("   free_nodes = functions.get_free_node_list()")
        print("   # Output: ['node-01', 'node-05']")

        print("\n2. GET DB STATUS & PARSE")
        print("   db_text = functions.get_db_latest_status()")
        print("   status_map = functions.parse_db_status_output(db_text)")
        print("   # Output: {'node-01': 1704234000, ...}")

        print("\n3. BUILD PRIORITY QUEUE")
        print("   queue = functions.build_priority_queue(free_nodes, status_map, days_threshold=7, shuffle=False)")
        print("   # Output: [['node-01', 1, False], ...]")

        print("\n4. SUBMIT JOB")
        print("   functions.submit_job('generated_job.yaml')")

        print("\n5. ADD RESULT (Run inside job pod)")
        print("   functions.add_result_local('node-01', 'dl_test', 'pass')")
        print("\n" + "="*60 + "\n")