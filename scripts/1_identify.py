#!/usr/bin/env python3
"""
Phase 1: Identify

Don't fix. Classify. Different problems need different fixes.

Reads: data/staging/snapshot_latest.json  (output of 0_observe.py)
Outputs: data/marts/classification_<timestamp>.json

Classifications (per TaskManager and per job, not mutually exclusive):
    overprovisioned  — paying for capacity you're not using
    cpu_bound        — CPU is the bottleneck
    memory_starved   — heap pressure, GC risk
    checkpoint_thrash — checkpoints failing or duration growing
    sink_blocked     — downstream backpressure (sink or slow operator)

Usage:
    python 1_identify.py                                    # default paths
    python 1_identify.py --input data/staging/snapshot_latest.json
    python 1_identify.py --stdout                           # print to stdout instead

# chained with observe:
python 0_observe.py && python 1_identify.py

# crontab: every 5 minutes
*/5 * * * * cd /opt/flink-rightsizing && python 0_observe.py && python 1_identify.py
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


# =============================================================================
# Thresholds
# =============================================================================
# These are starting points. Tune them for your workloads.
# The "right" thresholds depend on your SLAs and cost tolerance.
#
# Conservative = fewer false positives, miss some savings
# Aggressive   = catch more savings, risk flagging things that are fine
#
# Start conservative. Tighten after you see real data.
# =============================================================================

THRESHOLDS = {
    # --- CPU ---
    # Below this = overprovisioned (paying for idle cores)
    # Above cpu_bound = bottleneck (need more CPU or less parallelism per TM)
    "cpu_overprovisioned_below": 30,    # %
    "cpu_bound_above": 80,              # %

    # --- Heap ---
    # Below this = overprovisioned (allocated RAM sitting unused)
    # Above pressure = GC pauses likely, OOM risk
    "heap_overprovisioned_below": 40,   # %
    "heap_pressure_above": 80,          # %

    # --- Checkpoints ---
    # Fail rate = failed / (completed + failed). 0 is ideal.
    # Duration is "how long is the cluster pausing to snapshot state?"
    "checkpoint_fail_rate_above": 0.05,     # 5% — should be ~0 in healthy systems
    "checkpoint_duration_warn_ms": 10_000,  # 10 seconds — depends on state size
    "checkpoint_min_sample": 20,            # ignore fail rate until this many checkpoints

    # --- Slots ---
    # Slot utilization below this = TaskManagers allocated but not doing work
    "slot_underutilized_below": 50,     # %
}


# =============================================================================
# Classification logic
# =============================================================================
#
# Rightsizing Metrics (same 5 from flink_client.py):
#     1. Cluster — What's the shape of the whole thing?
#     2. Jobs — What's running on it?
#     3. TaskManagers — Who's doing the work?
#     4. Backpressure — Where are the bottlenecks?
#     5. Checkpoints — Is state healthy?
#
# 0_observe.py collects all five. Now we correlate them.
#
# Why correlation matters:
#     Low CPU + backpressure OK        → overprovisioned (safe to reduce)
#     Low CPU + backpressure HIGH      → idle because BLOCKED, not because overpaid
#     High CPU + backpressure OK       → working hard, keeping up (healthy)
#     High CPU + backpressure HIGH     → saturated AND backed up (undersized)
#
# Single-metric classification is dangerous (see THEORY.md).
# Every classification here checks at least two signals.
# =============================================================================


def classify_taskmanager(tm: dict) -> dict:
    """
    Classify a single TaskManager's resource state.

    Input: one entry from snapshot["taskmanagers"]
    Output: dict with classifications and supporting evidence
    """
    cpu = tm["cpu_load_pct"]
    heap_pct = tm["heap_utilization_pct"]
    slot_pct = tm["slot_utilization_pct"]

    # Step 1: Compute flags (what did we measure?)
    flags = {
        "cpu_low":  cpu < THRESHOLDS["cpu_overprovisioned_below"],
        "cpu_high": cpu > THRESHOLDS["cpu_bound_above"],
        "heap_low": heap_pct < THRESHOLDS["heap_overprovisioned_below"],
        "heap_high": heap_pct > THRESHOLDS["heap_pressure_above"],
        "slot_low": slot_pct < THRESHOLDS["slot_underutilized_below"],
    }

    # Step 2: Classification rules (what does it mean?)
    # Scan top to bottom — not mutually exclusive, all matching rules fire
    RULES = [
        #  classification             condition                                    reason
        ("overprovisioned",           flags["cpu_low"] and flags["heap_low"],      "CPU and heap both below threshold"),
        ("overprovisioned_cpu",       flags["cpu_low"] and not flags["heap_low"],  "CPU underutilized (heap is fine)"),
        ("overprovisioned_memory",    flags["heap_low"] and not flags["cpu_low"],  "Heap underutilized (CPU is fine)"),
        ("slots_underutilized",       flags["slot_low"],                           "Slots allocated but not in use"),
        ("cpu_bound",                 flags["cpu_high"],                           "CPU near saturation"),
        ("memory_starved",            flags["heap_high"],                          "Heap utilization high — GC pressure likely"),
    ]

    classifications = [name for name, triggered, _ in RULES if triggered]

    # Step 3: Build evidence (why did each rule fire?)
    # Evidence includes the flags that triggered + the actual values
    evidence = {}
    for name, triggered, reason in RULES:
        if triggered:
            evidence[name] = {
                "reason": reason,
                "flags": {k: v for k, v in flags.items() if v},  # only True flags
                "values": {"cpu_pct": cpu, "heap_pct": heap_pct, "slot_pct": slot_pct},
            }

    if not classifications:
        classifications.append("healthy")
        evidence["healthy"] = {
            "reason": "All metrics within normal range",
            "values": {"cpu_pct": cpu, "heap_pct": heap_pct, "slot_pct": slot_pct},
        }

    return {
        "taskmanager_id": tm["taskmanager_id"],
        "taskmanager_id_short": tm["taskmanager_id_short"],
        "classifications": classifications,
        "evidence": evidence,
        "metrics": {
            "cpu_load_pct": cpu,
            "heap_utilization_pct": heap_pct,
            "heap_used_mb": tm["heap_used_mb"],
            "heap_max_mb": tm["heap_max_mb"],
            "slot_utilization_pct": slot_pct,
            "slots_used": tm["slots_used"],
            "slots_total": tm["slots_total"],
        },
    }


def classify_job(job: dict) -> dict:
    """
    Classify a single job's health state.

    Input: one entry from snapshot["jobs"]
    Output: dict with classifications and supporting evidence

    Jobs are where backpressure and checkpoints live.
    TM classification tells you about resource provisioning.
    Job classification tells you about workload health.
    """

    # =========================================================================
    # Step 1: Analyze — positional logic that can't be reduced to thresholds
    # =========================================================================
    #
    # Backpressure appears on the operator BEFORE the bottleneck (flink_client.py):
    #     Source → Parse → Enrich → Sink
    #                         ↑ slow
    #     Parse shows HIGH (it's waiting on Enrich)
    #     Enrich shows OK (it IS the bottleneck)
    #
    # So: find the last HIGH operator. The one AFTER it is the bottleneck.
    # If the last operator itself is HIGH, the sink can't keep up.
    operators = job.get("operators", [])
    high_bp_ops = [op for op in operators if op["backpressure_level"] == "high"]
    low_bp_ops = [op for op in operators if op["backpressure_level"] == "low"]

    suspected_bottleneck = None
    sink_is_bottleneck = False
    internal_bottleneck = False

    if high_bp_ops:
        high_indices = [i for i, op in enumerate(operators) if op["backpressure_level"] == "high"]
        last_high_idx = max(high_indices)

        if last_high_idx == len(operators) - 1:
            sink_is_bottleneck = True
            suspected_bottleneck = operators[-1]
        elif last_high_idx + 1 < len(operators):
            internal_bottleneck = True
            suspected_bottleneck = operators[last_high_idx + 1]

    # Checkpoint stats
    ckpt = job.get("checkpoint", {})
    completed = ckpt.get("completed_count", 0)
    failed = ckpt.get("failed_count", 0)
    total = completed + failed
    enough_samples = total > THRESHOLDS["checkpoint_min_sample"]
    fail_rate = failed / total if total > 0 else 0
    avg_duration = ckpt.get("avg_duration_ms")

    # =========================================================================
    # Step 2: Flags (what did we find?)
    # =========================================================================
    flags = {
        # Backpressure
        "sink_blocked":        sink_is_bottleneck,
        "internal_bottleneck": internal_bottleneck,
        "bp_watch":            bool(low_bp_ops) and not bool(high_bp_ops),
        "all_bp_ok":           all(op["backpressure_level"] == "ok" for op in operators),

        # Checkpoints (only after enough data — ignore startup noise)
        "ckpt_failing":        enough_samples and fail_rate > THRESHOLDS["checkpoint_fail_rate_above"],
        "ckpt_slow":           enough_samples and avg_duration is not None and avg_duration > THRESHOLDS["checkpoint_duration_warn_ms"],
    }

    # =========================================================================
    # Step 3: Rules (what does it mean?)
    # =========================================================================
    # durability_risk is a combined signal — processing healthy + checkpoints not.
    # This won't show up if you only look at backpressure OR only checkpoints.
    RULES = [
        #  classification         condition                                                          reason
        ("sink_blocked",          flags["sink_blocked"],                                             "Last operator shows backpressure — sink can't keep up"),
        ("bottleneck",            flags["internal_bottleneck"],                                      "Upstream operator(s) backpressured — downstream is slow"),
        ("backpressure_watch",    flags["bp_watch"],                                                 "Low-level backpressure detected — may escalate"),
        ("checkpoint_thrash",     flags["ckpt_failing"],                                             "Checkpoint failure rate exceeds threshold"),
        ("checkpoint_slow",       flags["ckpt_slow"],                                                "Average checkpoint duration exceeds threshold"),
        ("durability_risk",       flags["all_bp_ok"] and (flags["ckpt_failing"] or flags["ckpt_slow"]),  "Processing healthy but checkpoints are not — crash recovery would lose progress"),
    ]

    classifications = [name for name, triggered, _ in RULES if triggered]

    # =========================================================================
    # Step 4: Evidence (why did each rule fire?)
    # =========================================================================
    evidence = {}
    for name, triggered, reason in RULES:
        if not triggered:
            continue
        entry = {
            "reason": reason,
            "flags": {k: v for k, v in flags.items() if v},
        }
        # Backpressure rules — include operator names
        if name in ("sink_blocked", "bottleneck"):
            entry["suspected_bottleneck"] = suspected_bottleneck["operator_name"] if suspected_bottleneck else None
            entry["backpressured_operators"] = [op["operator_name"] for op in high_bp_ops]
        if name == "backpressure_watch":
            entry["operators"] = [op["operator_name"] for op in low_bp_ops]
        # Checkpoint rules — include rates and counts
        if name in ("checkpoint_thrash", "checkpoint_slow", "durability_risk"):
            entry["fail_rate"] = round(fail_rate, 3)
            entry["completed"] = completed
            entry["failed"] = failed
            if avg_duration is not None:
                entry["avg_duration_ms"] = avg_duration
        evidence[name] = entry

    if not classifications:
        classifications.append("healthy")
        evidence["healthy"] = {
            "reason": "No backpressure, checkpoints healthy",
            "backpressure_levels": [op["backpressure_level"] for op in operators],
            "checkpoint_fail_rate": round(fail_rate, 3),
        }

    return {
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "state": job["state"],
        "classifications": classifications,
        "evidence": evidence,
        "operators_summary": [
            {
                "name": op["operator_name"],
                "parallelism": op["parallelism"],
                "backpressure": op["backpressure_level"],
            }
            for op in operators
        ],
        "checkpoint_summary": {
            "completed": completed,
            "failed": failed,
            "avg_duration_ms": avg_duration,
            "avg_state_size_mb": ckpt.get("avg_state_size_mb"),
        },
    }


# =============================================================================
# Cluster-level summary
# =============================================================================
# Individual TM and job classifications are useful for drill-down.
# The summary answers: "What's the ONE thing I should look at first?"
# =============================================================================

def summarize(tm_classifications: list[dict], job_classifications: list[dict]) -> dict:
    """
    Produce a cluster-level summary from individual classifications.

    Priority order (highest severity first):
        1. Any job with sink_blocked or bottleneck — fix before reducing
        2. Any job with checkpoint_thrash — durability risk
        3. TMs with memory_starved or cpu_bound — under-resourced
        4. TMs overprovisioned — cost savings available
        5. Everything healthy — check back later
    """
    # Collect all classifications across TMs and jobs
    all_tm = set()
    for tm in tm_classifications:
        all_tm.update(tm["classifications"])

    all_job = set()
    for job in job_classifications:
        all_job.update(job["classifications"])

    # Priority rules — first match wins (highest severity first)
    PRIORITY_RULES = [
        #  priority            condition                                                                        action
        ("bottleneck",         "sink_blocked" in all_job or "bottleneck" in all_job,                            "Fix backpressure before reducing resources"),
        ("checkpoint_health",  "checkpoint_thrash" in all_job,                                                  "Investigate checkpoint failures — durability at risk"),
        ("under_resourced",    "memory_starved" in all_tm or "cpu_bound" in all_tm,                             "Increase resources for constrained TaskManagers"),
        ("cost_reduction",     "overprovisioned" in all_tm or "overprovisioned_cpu" in all_tm or "overprovisioned_memory" in all_tm,  "Safe to experiment with reducing resources"),
    ]

    priority, action = "healthy", "No action needed — check back after workload changes"
    for p, triggered, a in PRIORITY_RULES:
        if triggered:
            priority, action = p, a
            break  # enforce highest severity

    return {
        "priority": priority,
        "action": action,
        "tm_classifications": sorted(all_tm),
        "job_classifications": sorted(all_job),
        "tm_count": len(tm_classifications),
        "job_count": len(job_classifications),
    }


# =============================================================================
# Main
# =============================================================================

def classify_snapshot(snapshot: dict) -> dict:
    """
    Run all classification logic on a snapshot.

    Input: full snapshot from 0_observe.py
    Output: classification document for data/marts/
    """
    tm_results = [classify_taskmanager(tm) for tm in snapshot["taskmanagers"]]
    job_results = [classify_job(job) for job in snapshot["jobs"]]
    summary = summarize(tm_results, job_results)

    return {
        "classification_timestamp": datetime.now().isoformat(),
        "snapshot_timestamp": snapshot["snapshot_timestamp"],
        "endpoint": snapshot["endpoint"],
        "summary": summary,
        "taskmanagers": tm_results,
        "jobs": job_results,
        "thresholds_used": THRESHOLDS,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Phase 1: Identify — classify Flink cluster state"
    )
    parser.add_argument(
        "--input", "-i",
        default="data/staging/snapshot_latest.json",
        help="Input snapshot JSON (default: data/staging/snapshot_latest.json)"
    )
    parser.add_argument(
        "--output", "-o",
        default="data/marts",
        help="Output directory for classification JSON (default: data/marts)"
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print to stdout instead of file"
    )
    args = parser.parse_args()

    # Read snapshot
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: {input_path} not found. Run 0_observe.py first.", file=sys.stderr)
        sys.exit(1)

    with open(input_path) as f:
        snapshot = json.load(f)

    print(f"Classifying snapshot from {snapshot['snapshot_timestamp']}...", file=sys.stderr)

    # Classify
    result = classify_snapshot(snapshot)

    # Report summary
    s = result["summary"]
    print(f"Priority: {s['priority']} — {s['action']}", file=sys.stderr)
    print(f"  TMs: {s['tm_classifications']}", file=sys.stderr)
    print(f"  Jobs: {s['job_classifications']}", file=sys.stderr)

    # Output
    json_output = json.dumps(result, indent=2)

    if args.stdout:
        print(json_output)
    else:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)

        ts = result["classification_timestamp"].replace(":", "-").split(".")[0]
        filename = output_dir / f"classification_{ts}.json"

        with open(filename, "w") as f:
            f.write(json_output)

        print(f"Classification written to: {filename}", file=sys.stderr)

        # Latest copy
        latest = output_dir / "classification_latest.json"
        with open(latest, "w") as f:
            f.write(json_output)
        print(f"Latest copy: {latest}", file=sys.stderr)


if __name__ == "__main__":
    main()