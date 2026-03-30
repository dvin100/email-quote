#!/usr/bin/env python3
"""
Load all generated SQL batch files into Databricks via SQL Statements API.
Executes batches in parallel (up to 10 concurrent) for speed.
"""
import subprocess
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import WAREHOUSE_ID, DATABRICKS_PROFILE as PROFILE
from pathlib import Path

DATA_DIR = str(Path(__file__).resolve().parent / "data")

def execute_sql(sql_file):
    """Execute a SQL file via the Databricks SQL Statements API."""
    with open(sql_file) as f:
        sql = f.read()

    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s"
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
        json.dump(payload, tmp)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            ["databricks", "api", "post", "/api/2.0/sql/statements",
             "--profile", PROFILE, "--json", f"@{tmp_path}"],
            capture_output=True, text=True, timeout=120
        )

        if result.stdout.strip():
            resp = json.loads(result.stdout)
            state = resp.get("status", {}).get("state", "UNKNOWN")
            if state == "SUCCEEDED":
                return True, os.path.basename(sql_file), "OK"
            elif state == "PENDING":
                # Poll for completion
                stmt_id = resp.get("statement_id")
                for _ in range(30):
                    time.sleep(2)
                    poll = subprocess.run(
                        ["databricks", "api", "get",
                         f"/api/2.0/sql/statements/{stmt_id}",
                         "--profile", PROFILE],
                        capture_output=True, text=True, timeout=30
                    )
                    if poll.stdout.strip():
                        presp = json.loads(poll.stdout)
                        pstate = presp.get("status", {}).get("state", "UNKNOWN")
                        if pstate == "SUCCEEDED":
                            return True, os.path.basename(sql_file), "OK"
                        elif pstate == "FAILED":
                            err = presp.get("status", {}).get("error", {}).get("message", "unknown")
                            return False, os.path.basename(sql_file), err[:200]
                return False, os.path.basename(sql_file), "TIMEOUT polling"
            else:
                err = resp.get("status", {}).get("error", {}).get("message", "unknown")
                return False, os.path.basename(sql_file), err[:200]
        else:
            return False, os.path.basename(sql_file), result.stderr[:200]
    except Exception as e:
        return False, os.path.basename(sql_file), str(e)[:200]
    finally:
        os.unlink(tmp_path)


# Load manifest
with open(f"{DATA_DIR}/manifest.json") as f:
    manifest = json.load(f)

files = [os.path.join(DATA_DIR, fn) for fn in manifest["files"]]
total = len(files)
print(f"Loading {total} SQL batch files into Databricks...")

# Group by table for ordered display
from collections import defaultdict
table_files = defaultdict(list)
for fp in files:
    tname = "_".join(os.path.basename(fp).split("_")[:-1])
    table_files[tname].append(fp)

succeeded = 0
failed = 0
errors = []

for table_name, tfiles in table_files.items():
    print(f"\n{'='*60}")
    print(f"Loading {table_name} ({len(tfiles)} batches)...")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(execute_sql, f): f for f in tfiles}
        batch_ok = 0
        batch_fail = 0
        for future in as_completed(futures):
            ok, fname, msg = future.result()
            if ok:
                batch_ok += 1
                succeeded += 1
            else:
                batch_fail += 1
                failed += 1
                errors.append((fname, msg))

            done = succeeded + failed
            if done % 20 == 0 or (batch_ok + batch_fail) == len(tfiles):
                print(f"  Progress: {batch_ok}/{len(tfiles)} batches OK  |  Overall: {done}/{total}")

    if batch_fail > 0:
        print(f"  WARNING: {batch_fail} batches failed for {table_name}")

print(f"\n{'='*60}")
print(f"COMPLETE: {succeeded}/{total} succeeded, {failed} failed")

if errors:
    print(f"\nFailed batches:")
    for fname, msg in errors[:20]:
        print(f"  {fname}: {msg}")
