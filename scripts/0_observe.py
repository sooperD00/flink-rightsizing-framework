#!/usr/bin/env python3
"""
Phase 0: Observe

Don't tune. Don't guess. Instrument.

Collects in ONE API pass:
- backpressure (per operator)
- checkpoint durations & state size
- CPU utilization (per TaskManager)
- heap usage (per TaskManager)
- slot utilization (per TaskManager)
- operator-level bottlenecks

Outputs to: data/staging/snapshot_<timestamp>.json

Usage:
    python 0_observe.py                                 # default endpoint
    python 0_observe.py --endpoint http://flink-jm:8081
    python 0_observe.py --output data/staging/          # specify output dir
    python 0_observe.py --stdout                        # print to stdout instead

# scripts/run_dev.sh
python 0_observe.py --endpoint http://localhost:8081 --output data/staging/

# scripts/run_prod.sh
python 0_observe.py --endpoint http://flink-prod.internal:8081 --output /data/prod/staging/

# crontab: every 5 minutes
*/5 * * * * /opt/flink-rightsizing/scripts/run_prod.sh && python 1_classify.py

"""

import argparse  # so you can run from setup/script.sh with different options
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from flink_client import FlinkClient  # our wrapper for Flink REST API


def collect_snapshot(client: FlinkClient) -> dict:
    """
    Collect rightsizing observability metrics from Flink REST API in one pass.
    
    Return a complete snapshot of cluster state for the 5 rightsizing metrics defined in `flink_client.py`
    """
    timestamp = datetime.now().isoformat()
    
    # =========================================================================
    # 1. Cluster overview - flink_client.py says "slots" are checked here...
    # =========================================================================
    cluster = client.get_cluster_overview()
    
    # =================
    # 2. No "Jobs" metrics? only a by-grouping?
    # =================

    # =========================================================================
    # 3. TaskManager metrics (CPU, heap, slots) - but you are checking slots here?
    # =========================================================================
    taskmanagers = []
    for tm in client.get_taskmanagers():
        tm_id = tm["id"]
        util = client.get_taskmanager_utilization(tm_id)
        
        slots_total = tm["slotsNumber"]
        slots_free = tm["freeSlots"]
        slots_used = slots_total - slots_free
        
        taskmanagers.append({
            "taskmanager_id": tm_id,
            "taskmanager_id_short": tm_id[:8],
            
            # Slots
            "slots_total": slots_total,
            "slots_used": slots_used,
            "slots_free": slots_free,
            "slot_utilization_pct": round(slots_used / slots_total * 100, 1) if slots_total > 0 else 0,
            
            # CPU
            "cpu_load_pct": util["cpu_load_pct"],
            
            # Heap (this is your "heap usage")
            "heap_used_mb": util["heap_used_mb"],
            "heap_max_mb": util["heap_max_mb"],
            "heap_utilization_pct": util["heap_utilization_pct"],
            
            # Non-heap (metaspace, etc.)
            "nonheap_used_mb": util["nonheap_used_mb"],
            "nonheap_max_mb": util["nonheap_max_mb"],
        })
    
    # =========================================================================
    # 4-5. Per-job metrics (backpressure, checkpoints, operators)
    # hmmmm, does this .py script sections not map 100% cleanly to our "5 metric areas"?
    # =========================================================================
    jobs = []
    for job in client.get_running_jobs():
        job_id = job["jid"]
        job_name = job["name"]
        
        # Get operators (vertices) - another GROUP BY field?
        vertices = client.get_job_vertices(job_id)
        
        # =================
        # 4. Backpressure
        # =================
        # Backpressure per operator (this is your "operator-level bottlenecks")
        operators = []
        for vertex in vertices:
            bp = client.get_backpressure(job_id, vertex["id"])
            operators.append({
                "operator_name": vertex["name"],
                "vertex_id": vertex["id"],
                "parallelism": vertex["parallelism"],
                "status": vertex["status"],
                "backpressure_level": bp.get("backpressure-level", "unknown"),
                "backpressure_ratio": bp.get("ratio", None),  # if available
            })
        
        # =================
        # 5. Checkpoint
        # =================
        # Checkpoint stats
        ckpt_raw = client.get_checkpoint_stats(job_id)
        counts = ckpt_raw.get("counts", {})
        latest = ckpt_raw.get("latest", {})
        history = ckpt_raw.get("history", [])
        
        checkpoint = {
            "completed_count": counts.get("completed", 0),
            "failed_count": counts.get("failed", 0),
            "in_progress_count": counts.get("in_progress", 0),
        }
        
        # Latest completed checkpoint
        if latest.get("completed"):
            lc = latest["completed"]
            checkpoint.update({
                "latest_duration_ms": lc["end_to_end_duration"],
                "latest_state_size_bytes": lc["state_size"],
                "latest_state_size_mb": round(lc["state_size"] / (1024 * 1024), 2),
            })
        
        # Trend from history
        completed_history = [h for h in history if h.get("status") == "COMPLETED"]
        if completed_history:
            durations = [h["end_to_end_duration"] for h in completed_history]
            sizes = [h["state_size"] for h in completed_history]
            checkpoint.update({
                "avg_duration_ms": round(sum(durations) / len(durations), 1),
                "max_duration_ms": max(durations),
                "avg_state_size_mb": round(sum(sizes) / len(sizes) / (1024 * 1024), 2),
                "history_sample_count": len(completed_history),
            })
        
        jobs.append({
            "job_id": job_id,
            "job_name": job_name,
            "state": job["state"],
            "start_time": job.get("start-time"),
            "operators": operators,
            "checkpoint": checkpoint,
        })
    
    # =========================================================================
    # Assemble snapshot
    # =========================================================================
    return {
        "snapshot_timestamp": timestamp,
        "endpoint": client.endpoint,
        "cluster": {
            "taskmanager_count": cluster["taskmanagers"],
            "slots_total": cluster["slots-total"],
            "slots_available": cluster["slots-available"],
            "jobs_running": cluster["jobs-running"],
            "jobs_finished": cluster["jobs-finished"],
            "jobs_failed": cluster["jobs-failed"],
            "flink_version": cluster.get("flink-version", "unknown"),
        },
        "taskmanagers": taskmanagers,
        "jobs": jobs,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Phase 0: Observe — collect Flink cluster metrics"
    )
    parser.add_argument(
        "--endpoint", "-e",
        default="http://localhost:8081",
        help="Flink REST API endpoint (default: http://localhost:8081)"
    )
    parser.add_argument(
        "--output", "-o",
        default="data/staging",
        help="Output directory for snapshot JSON (default: data/staging)"
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print to stdout instead of file"
    )
    args = parser.parse_args()

    # Connect
    client = FlinkClient(args.endpoint)
    
    try:
        print(f"Connecting to {args.endpoint}...", file=sys.stderr)
        snapshot = collect_snapshot(client)
        print(f"Collected: {len(snapshot['taskmanagers'])} TaskManagers, {len(snapshot['jobs'])} jobs", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Output
    json_output = json.dumps(snapshot, indent=2)
    
    if args.stdout:
        print(json_output)
    else:
        # Create output directory if needed
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Filename with timestamp
        ts = snapshot["snapshot_timestamp"].replace(":", "-").split(".")[0]
        filename = output_dir / f"snapshot_{ts}.json"
        
        with open(filename, "w") as f:
            f.write(json_output)
        
        print(f"Snapshot written to: {filename}", file=sys.stderr)
        
        # Also write a "latest" symlink/copy for easy access
        latest = output_dir / "snapshot_latest.json"
        with open(latest, "w") as f:
            f.write(json_output)
        print(f"Latest copy: {latest}", file=sys.stderr)


if __name__ == "__main__":
    main()
