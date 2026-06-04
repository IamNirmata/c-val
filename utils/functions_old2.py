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
DEFAULT_STORAGE_DB_PATH = "/data/continuous_validation/metadata/test-storage.db"
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
    Initializes the standard validation database schema remotely.
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

def init_storage_db(pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_STORAGE_DB_PATH):
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
        conn.execute('''
            CREATE TABLE IF NOT EXISTS storage_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                
                iodepth_read_1file_iops REAL, iodepth_read_1file_bw REAL,
                iodepth_write_1file_iops REAL, iodepth_write_1file_bw REAL,
                numjobs_read_nfiles_iops REAL, numjobs_read_nfiles_bw REAL,
                numjobs_write_nfiles_iops REAL, numjobs_write_nfiles_bw REAL,
                randread_iops REAL, randread_bw REAL,
                randwrite_iops REAL, randwrite_bw REAL,

                PRIMARY KEY (node, timestamp)
            );
        ''')

        # 2. Create Index
        conn.execute('CREATE INDEX IF NOT EXISTS idx_perf_node_ts ON storage_performance(node, timestamp);')

        # 3. Create View with Percentile Rankings
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
                
                iodepth_read_1file_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_read_1file_iops), 2) as iodepth_read_1file_iops_pct,
                iodepth_read_1file_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_read_1file_bw), 2) as iodepth_read_1file_bw_pct,

                iodepth_write_1file_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_write_1file_iops), 2) as iodepth_write_1file_iops_pct,
                iodepth_write_1file_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY iodepth_write_1file_bw), 2) as iodepth_write_1file_bw_pct,

                numjobs_read_nfiles_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_read_nfiles_iops), 2) as numjobs_read_nfiles_iops_pct,
                numjobs_read_nfiles_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_read_nfiles_bw), 2) as numjobs_read_nfiles_bw_pct,

                numjobs_write_nfiles_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_write_nfiles_iops), 2) as numjobs_write_nfiles_iops_pct,
                numjobs_write_nfiles_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY numjobs_write_nfiles_bw), 2) as numjobs_write_nfiles_bw_pct,

                randread_iops,
                ROUND(PERCENT_RANK() OVER (ORDER BY randread_iops), 2) as randread_iops_pct,
                randread_bw,
                ROUND(PERCENT_RANK() OVER (ORDER BY randread_bw), 2) as randread_bw_pct,

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
# FLOW STEP 1: Get Free Node List
# ==========================================

def get_free_node_list():
    nodes, _ = get_free_nodes()
    return [n['node'] for n in nodes if n['free'] == n['alloc'] and n['alloc'] > 0]

def get_free_nodes(verbose=False):
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
        init_reqs = [int(c.get('resources', {}).get('requests', {}).get('nvidia.com/gpu', 0)) for c in init_containers]
        init_req = max(init_reqs) if init_reqs else 0
        
        usage = max(app_req, init_req)
        node_usage[node_name] = node_usage.get(node_name, 0) + usage

    cmd_nodes = ["kubectl", "get", "nodes", "--no-headers", "-o", r"custom-columns=NAME:.metadata.name,CAP:.status.capacity.nvidia\.com/gpu,ALLOC:.status.allocatable.nvidia\.com/gpu"]
    nodes_output = run_command(cmd_nodes, check=False) 
    
    results = []
    totals = {'cap': 0, 'alloc': 0, 'used': 0, 'free': 0}
    
    for line in nodes_output.split('\n'):
        if not line.strip(): continue
        if 'hgx' not in line: continue
            
        parts = line.split()
        if len(parts) < 3: continue
            
        name = parts[0]
        cap = int(parts[1]) if parts[1].isdigit() else 0
        alloc = int(parts[2]) if parts[2].isdigit() else 0
        used = node_usage.get(name, 0)
        free = alloc - used
        
        results.append({'node': name, 'cap': cap, 'alloc': alloc, 'used': used, 'free': free})
        totals['cap'] += cap
        totals['alloc'] += alloc
        totals['used'] += used
        totals['free'] += free
            
    return results, totals


# ==========================================
# FLOW STEP 2: Get DB Latest Status
# ==========================================

def get_db_latest_status(pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_DB_PATH):
    code = textwrap.dedent(f"""
    import sqlite3, datetime, sys
    db_path = '{db_path}'
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE IF NOT EXISTS latest_status (node TEXT, test TEXT, latest_timestamp INTEGER, result TEXT, PRIMARY KEY (node, test))")
        rows = conn.execute('SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test').fetchall()

        print('node\\ttest\\tlatest_timestamp_num\\tlatest_timestamp\\tresult')
        for r in rows:
            ts_num = int(r['latest_timestamp']) if r['latest_timestamp'] is not None else ''
            ts_iso = ''
            if r['latest_timestamp'] is not None:
                ts_iso = datetime.datetime.fromtimestamp(r['latest_timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
            print(f"{{r['node']}}\\t{{r['test']}}\\t{{ts_num}}\\t{{ts_iso}}\\t{{r['result']}}")
    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace)


    code = textwrap.dedent(f"""
    import sqlite3, sys, datetime, os
    
    # Try to import pandas for pretty printing
    try:
        import pandas as pd
        HAS_PANDAS = True
    except ImportError:
        HAS_PANDAS = False

    db_path = '{db_path}'
    try:
        if not os.path.exists(db_path):
            print(f"Storage DB not found at {{db_path}}. Run 'create-test storage' first.")
            sys.exit(0)

        conn = sqlite3.connect(db_path)
        
        # Check if view exists
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='latest_node_performance_stats';")
        if not cursor.fetchone():
            print("View 'latest_node_performance_stats' not found.")
            sys.exit(0)

        if HAS_PANDAS:
            # Use Pandas for clean formatting
            df = pd.read_sql_query('SELECT * FROM latest_node_performance_stats ORDER BY latest_timestamp DESC', conn)
            if df.empty:
                print("No results found in storage DB.")
                sys.exit(0)
                
            # Format timestamp columns
            for col in df.columns:
                if 'timestamp' in col:
                    # Convert integer timestamp to string
                    df[col] = pd.to_datetime(df[col], unit='s', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Formatting options
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            pd.set_option('display.max_colwidth', None)
            
            # Print without index
            print(df.to_string(index=False))
            
        else:
            # Fallback for when Pandas is missing (Align columns manually)
            conn.row_factory = sqlite3.Row
            rows = conn.execute('SELECT * FROM latest_node_performance_stats ORDER BY latest_timestamp DESC').fetchall()

            if rows:
                headers = list(rows[0].keys())
                data = []
                # Pre-process data to strings
                for r in rows:
                    row_data = []
                    for k in headers:
                        val = r[k]
                        if 'timestamp' in k and isinstance(val, int):
                             val = datetime.datetime.fromtimestamp(val, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                        row_data.append(str(val))
                    data.append(row_data)
                
                # Calculate widths
                widths = [len(h) for h in headers]
                for row in data:
                    for i, val in enumerate(row):
                        widths[i] = max(widths[i], len(val))
                
                # Print Header
                header_line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
                print(header_line)
                print("-" * len(header_line))
                
                # Print Rows
                for row in data:
                    print("  ".join(val.ljust(w) for val, w in zip(row, widths)))
            else:
                print("No results found in storage DB.")

    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace)
def get_storage_status_csv(pod=DEFAULT_POD, namespace=DEFAULT_NAMESPACE, db_path=DEFAULT_STORAGE_DB_PATH):
    """
    Fetches the latest VIEW from the storage database in CSV format.
    """
    code = textwrap.dedent(f"""
    import sqlite3, sys, datetime, os, csv

    db_path = '{db_path}'
    try:
        if not os.path.exists(db_path):
            print(f"Storage DB not found at {{db_path}}.")
            sys.exit(0)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='latest_node_performance_stats';")
        if not cursor.fetchone():
            print("View 'latest_node_performance_stats' not found.")
            sys.exit(0)

        rows = conn.execute('SELECT * FROM latest_node_performance_stats ORDER BY latest_timestamp DESC').fetchall()

        if rows:
            # Output CSV
            headers = list(rows[0].keys())
            writer = csv.writer(sys.stdout)
            writer.writerow(headers)
            
            for r in rows:
                vals = []
                for k in headers:
                    val = r[k]
                    # Convert timestamp to readable string for CSV
                    if 'timestamp' in k and isinstance(val, int):
                         val = datetime.datetime.fromtimestamp(val, tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    vals.append(val)
                writer.writerow(vals)
        else:
            print("No results found in storage DB.")

    except Exception as e:
        print(f'Error: {{e}}', file=sys.stderr)
        sys.exit(1)
    """)
    return _exec_python_on_pod(code, pod, namespace)


def parse_db_status_output(output_string):
    status_map = {}
    lines = output_string.strip().split('\n')
    if lines and 'node' in lines[0] and 'timestamp' in lines[0]:
        lines = lines[1:]
    for line in lines:
        parts = line.split('\t')
        if len(parts) >= 3:
            node = parts[0]
            ts_str = parts[2] 
            if ts_str and ts_str.isdigit():
                ts = int(ts_str)
                current_max = status_map.get(node, 0)
                if ts > current_max:
                    status_map[node] = ts
            else:
                if node not in status_map: status_map[node] = 0
    return status_map


# ==========================================
# FLOW STEP 3: Build Priority Queue
# ==========================================

def build_priority_queue(free_nodes_list, db_latest_status_map, days_threshold=7, shuffle=False):
    # import datetime
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    print(f"Building priority queue at {datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).isoformat()} with threshold {days_threshold} days")
    threshold_seconds = days_threshold * 86400
    candidate_list = []
    
    for node in free_nodes_list:
        last_ts = db_latest_status_map.get(node, 0)
        age = now - last_ts
        if last_ts == 0 or age > threshold_seconds:
            candidate_list.append({'node': node, 'ts': last_ts})
        else:
            print(f"  Skipping node {node}: Age {age/86400:.2f} days")
            
    if shuffle:
        random.shuffle(candidate_list)
    else:
        candidate_list.sort(key=lambda x: x['ts'])
    
    priority_queue = []
    for idx, item in enumerate(candidate_list):
        priority_queue.append([item['node'], idx + 1, False])
    return priority_queue


# ==========================================
# FLOW STEP 4 & 5: Job Submission & Monitor
# ==========================================

def create_job(yaml_file):
    if not os.path.exists(yaml_file): raise FileNotFoundError(f"File '{yaml_file}' does not exist")
    return run_command(["kubectl", "create", "-f", yaml_file])

def get_job_status(job_name, namespace=DEFAULT_NAMESPACE):
    cmd = ["kubectl", "get", "vcjob", "-n", namespace, job_name, "-o", "jsonpath={.status.state.phase}"]
    try:
        status = run_command(cmd)
        return status if status else "Unknown"
    except Exception:
        return "Unknown"

def delete_all_validation_jobs(confirm=False, namespace=DEFAULT_NAMESPACE, tag=JOB_GROUP_LABEL):
    cmd_list = f'kubectl get vcjob -n {namespace} --no-headers -o custom-columns=NAME:.metadata.name | grep "{tag}"'
    try:
        jobs = run_command(cmd_list, shell=True).split('\n')
        jobs = [j.strip() for j in jobs if j.strip()]
    except subprocess.CalledProcessError:
        return
    if not jobs: return

    print("Found jobs to delete:", jobs)
    if not confirm:
        response = input("Do you want to delete these jobs? (y/N): ")
        if response.lower() != 'y': return

    for job in jobs:
        try:
            run_command(["kubectl", "delete", "vcjob", "-n", namespace, job])
            print(f"Deleted {job}")
        except Exception:
            print(f"Failed to delete {job}")


# ==========================================
# FLOW STEP 6: Job Execution (Inside Pod)
# ==========================================

def parse_timestamp(timestamp_str):
    """Parses timestamp string from Bash (%Y%m%d_%H%M%S) or ISO format."""
    if timestamp_str is None:
        return int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        
    if isinstance(timestamp_str, (int, float)):
        return int(timestamp_str)
        
    ts = str(timestamp_str).strip()
    if ts.isdigit():
        return int(ts)
        
    # Try ISO Format
    try:
        if ts.endswith('Z'): ts = ts[:-1] + '+00:00'
        d = datetime.datetime.fromisoformat(ts)
        if d.tzinfo is None: d = d.replace(tzinfo=datetime.timezone.utc)
        return int(d.timestamp())
    except ValueError:
        pass

    # Try Bash Format (YYYYMMDD_HHMMSS)
    try:
        d = datetime.datetime.strptime(ts, "%Y%m%d_%H%M%S")
        if d.tzinfo is None: d = d.replace(tzinfo=datetime.timezone.utc)
        return int(d.timestamp())
    except ValueError:
        pass
        
    # Fallback to now if parsing fails
    print(f"Warning: Could not parse timestamp '{timestamp_str}'. Using current time.")
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())

def add_result_local(node, test, result, timestamp=None, db_path=DEFAULT_DB_PATH):
    import os, sqlite3
    
    timestamp = parse_timestamp(timestamp)

    db_path = os.path.abspath(str(db_path).strip())
    db_dir = os.path.dirname(db_path) or "."
    os.makedirs(db_dir, exist_ok=True)

    conn = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=rwc", uri=True, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA synchronous=FULL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
              node TEXT NOT NULL,
              test TEXT NOT NULL,
              timestamp INTEGER NOT NULL,
              result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete'))
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_node_test_ts ON runs(node, test, timestamp);")
        conn.execute("INSERT INTO runs(node, test, timestamp, result) VALUES (?,?,?,?)", (node, test, timestamp, result))
        conn.commit()
        print(f"Added: {node} {test} {result} {timestamp}")
    except Exception as e:
        print(f"Error adding result: {e}")
        raise
    finally:
        if conn: conn.close()

def add_storage_result_local(node, timestamp, results_dir, db_path=DEFAULT_STORAGE_DB_PATH):
    import os, sqlite3, json
    
    timestamp = parse_timestamp(timestamp)

    db_path = os.path.abspath(str(db_path).strip())
    db_dir = os.path.dirname(db_path) or "."
    os.makedirs(db_dir, exist_ok=True)

    metrics = {
        'iodepth_read_1file_iops': 0.0, 'iodepth_read_1file_bw': 0.0,
        'iodepth_write_1file_iops': 0.0, 'iodepth_write_1file_bw': 0.0,
        'numjobs_read_nfiles_iops': 0.0, 'numjobs_read_nfiles_bw': 0.0,
        'numjobs_write_nfiles_iops': 0.0, 'numjobs_write_nfiles_bw': 0.0,
        'randread_iops': 0.0, 'randread_bw': 0.0,
        'randwrite_iops': 0.0, 'randwrite_bw': 0.0,
    }

    file_map = {
        'iodepth_read_1file.json': 'iodepth_read_1file',
        'iodepth_write_1file.json': 'iodepth_write_1file',
        'numjobs_read_nfiles.json': 'numjobs_read_nfiles',
        'numjobs_write_nfiles.json': 'numjobs_write_nfiles',
        'randread.json': 'randread',
        'randwrite.json': 'randwrite'
    }

    print(f"Parsing storage results from: {results_dir}")
    if not os.path.exists(results_dir):
        print(f"Error: Results directory {results_dir} not found.")
        sys.exit(1)

    for fname, prefix in file_map.items():
        fpath = os.path.join(results_dir, fname)
        if os.path.exists(fpath):
            try:
                with open(fpath, 'r') as f:
                    data = json.load(f)
                    job = data['jobs'][0]
                    read_iops = job.get('read', {}).get('iops', 0)
                    write_iops = job.get('write', {}).get('iops', 0)
                    read_bw = job.get('read', {}).get('bw', 0)
                    write_bw = job.get('write', {}).get('bw', 0)
                    
                    metrics[f'{prefix}_iops'] = read_iops + write_iops
                    metrics[f'{prefix}_bw'] = read_bw + write_bw
            except Exception as e:
                print(f"Warning: Failed to parse {fname}: {e}")

    conn = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=rwc", uri=True, timeout=30)
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA synchronous=FULL;")
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS storage_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                iodepth_read_1file_iops REAL, iodepth_read_1file_bw REAL,
                iodepth_write_1file_iops REAL, iodepth_write_1file_bw REAL,
                numjobs_read_nfiles_iops REAL, numjobs_read_nfiles_bw REAL,
                numjobs_write_nfiles_iops REAL, numjobs_write_nfiles_bw REAL,
                randread_iops REAL, randread_bw REAL,
                randwrite_iops REAL, randwrite_bw REAL,
                PRIMARY KEY (node, timestamp)
            );
        ''')
        
        sql = '''
            INSERT OR REPLACE INTO storage_performance (
                node, timestamp,
                iodepth_read_1file_iops, iodepth_read_1file_bw,
                iodepth_write_1file_iops, iodepth_write_1file_bw,
                numjobs_read_nfiles_iops, numjobs_read_nfiles_bw,
                numjobs_write_nfiles_iops, numjobs_write_nfiles_bw,
                randread_iops, randread_bw,
                randwrite_iops, randwrite_bw
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        vals = (
            node, timestamp,
            metrics['iodepth_read_1file_iops'], metrics['iodepth_read_1file_bw'],
            metrics['iodepth_write_1file_iops'], metrics['iodepth_write_1file_bw'],
            metrics['numjobs_read_nfiles_iops'], metrics['numjobs_read_nfiles_bw'],
            metrics['numjobs_write_nfiles_iops'], metrics['numjobs_write_nfiles_bw'],
            metrics['randread_iops'], metrics['randread_bw'],
            metrics['randwrite_iops'], metrics['randwrite_bw']
        )
        conn.execute(sql, vals)
        conn.commit()
        print(f"Successfully added storage results for {node} at {timestamp}")

    except Exception as e:
        print(f"Error adding storage result: {e}")
        raise
    finally:
        if conn: conn.close()


# ==========================================
# UTILITY FUNCTIONS
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
    return run_command(["kubectl", "-n", namespace, "exec", pod, "--", "ls", "-F", target_dir])

def exec_pod(pod_name, namespace=DEFAULT_NAMESPACE):
    print(f"Starting interactive session in {pod_name}...")
    subprocess.call(["kubectl", "exec", "-it", pod_name, "-n", namespace, "--", "/bin/bash"])


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
    p_free = subparsers.add_parser("freenodes", help="List free nodes")

    # Command: LS
    p_ls = subparsers.add_parser("ls", help="List remote files")
    p_ls.add_argument("path", nargs="?", default="/data/continuous_validation", help="Remote path")

    # Command: Exec
    p_exec = subparsers.add_parser("exec", help="Exec into a pod")
    p_exec.add_argument("pod_name", nargs="?", default=DEFAULT_POD, help="Pod name")
    p_exec.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE, help="Namespace")

    # Command: Status (General)
    p_status = subparsers.add_parser("status", help="Get Main DB status")

    # Command: History
    p_hist = subparsers.add_parser("history", help="Get Main DB history")
    p_hist.add_argument("limit", nargs="?", default="20", help="Limit rows")

    # Command: delete-jobs
    p_delete = subparsers.add_parser("delete-jobs", help="Delete validation jobs")
    p_delete.add_argument("--confirm", action="store_true", help="Confirm deletion")
    p_delete.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE, help="Namespace")
    p_delete.add_argument("--tag", default=JOB_GROUP_LABEL, help="Tag filter")

    # Command: add-result (Local)
    p_add = subparsers.add_parser("add-result", help="Add result to local DB")
    p_add.add_argument("node")
    p_add.add_argument("test")
    p_add.add_argument("result")
    p_add.add_argument("timestamp", nargs="?", default=None)
    p_add.add_argument("--db-path", default=DEFAULT_DB_PATH)

    # Command: add-storage-result (Local) - NEW
    p_add_store = subparsers.add_parser("add-storage-result", help="Parse and add storage results to local DB")
    p_add_store.add_argument("node")
    p_add_store.add_argument("timestamp")
    p_add_store.add_argument("results_dir")
    p_add_store.add_argument("--db-path", default=DEFAULT_STORAGE_DB_PATH)

    # Command: init-db (General)
    p_init = subparsers.add_parser("init-db", help="Initialize Main DB")
    p_init.add_argument("--pod", default=DEFAULT_POD)
    p_init.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE)
    p_init.add_argument("--db-path", default=DEFAULT_DB_PATH)

    # Command: create-test (New Intializer)
    p_create = subparsers.add_parser("create-test", help="Initialize a specific test DB")
    p_create.add_argument("type", choices=["storage"], help="Test type (e.g., storage)")
    p_create.add_argument("--pod", default=DEFAULT_POD)
    p_create.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE)
    p_create.add_argument("--db-path", default=DEFAULT_STORAGE_DB_PATH)

    # Command: storage (New Viewer)
    p_storage = subparsers.add_parser("storage", help="View Storage DB results")
    p_storage.add_argument("--pod", default=DEFAULT_POD)
    p_storage.add_argument("--namespace", "-n", default=DEFAULT_NAMESPACE)
    p_storage.add_argument("--db-path", default=DEFAULT_STORAGE_DB_PATH)

    args = parser.parse_args()

    # --- HANDLERS ---

    if args.command == "freenodes":
        nodes, totals = get_free_nodes()
        fmt = "{:<30} {:<6} {:<6} {:<6} {:<6}"
        print("\n" + fmt.format("NODE NAME", "CAP", "ALLOC", "USED", "FREE"))
        print("-" * 60)
        if not nodes: print("No free nodes found.")
        else:
            for n in nodes:
                if n['free'] >= 0: print(fmt.format(n['node'], n['cap'], n['alloc'], n['used'], n['free']))
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
    elif args.command == "add-storage-result":
        add_storage_result_local(args.node, args.timestamp, args.results_dir, args.db_path)
    elif args.command == "init-db":
        print(init_db(args.pod, args.namespace, args.db_path))

    # New Handlers
    elif args.command == "create-test":
        if args.type == "storage":
            print(init_storage_db(args.pod, args.namespace, args.db_path))
    

    elif args.command == "storage":
        print("Fetching storage data from cluster...")
        # 1. Fetch data string
        data = get_storage_status_csv(args.pod, args.namespace, args.db_path)
        
        if "latest_timestamp" in data and "," in data:
            # 2. Create filename
            filename = "storage.csv"
            
            # 3. Save to CURRENT DIRECTORY ("w" mode without path)
            with open(filename, "w") as f:
                f.write(data)
            
            # 4. Print the absolute path so you can see it
            print(f"✅ Storage report saved to: {os.path.abspath(filename)}")


    elif args.command == "help" or args.command is None:
        print("\n" + "="*60)
        print(" CLUSTER VALIDATIONS FUNCTIONS - USAGE GUIDE")
        print("="*60)
        print("  python3 functions.py freenodes      # List free nodes table")
        print("  python3 functions.py status         # View Main DB status")
        print("  python3 functions.py storage        # View Storage DB results")
        print("  python3 functions.py create-test storage # Init Storage DB")
        print("  python3 functions.py init-db       # Initialize Main DB")
        print("  python3 functions.py ls [path]     # List files in pod")
        print("  python3 functions.py exec [pod]    # Exec into a pod")
        print("  python3 functions.py delete-jobs   # Delete all validation jobs")
        print("  python3 functions.py add-result NODE TEST RESULT [TIMESTAMP] [--db-path PATH]  # Add result to local DB")
        print("  python3 functions.py create-test storage --pod POD --namespace NAMESPACE --db-path PATH  # Initialize Storage DB remotely")
        print("\n" + "="*60 + "\n")