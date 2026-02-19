#!/usr/bin/env python3
"""
Create a Flink REST API Client - Wrap Flink's long REST endpoints in meaningful abstractions:
    https://flink.apache.org/docs/
Flink REST API:
    requests.get("http://localhost:8081/jobs/xxx/vertices/yyy/backpressure") 
Wrapper: (readable python functions)
    client.get_backpressure(job_id, vertex_id)

Usage:
    from flink_client import FlinkClient
    
    client = FlinkClient("http://localhost:8081")
    jobs = client.get_running_jobs()
    bp = client.get_backpressure(job_id, vertex_id)

What we grabbed: from Flink's "monitoring API"
    full list here:
        https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/
    we grabbed maybe 20% of this - all the stuff relevant to observability
"""
# HTTP library (requests.get, post, delete, put / status_code / etc)
# no Flink SDK for Python - use anything that can do HTTP
import requests  

# generates __init__ for you
from dataclasses import dataclass  

# documentation that the IDE can check 
# Pydantic (validates at runtime) overkill for now
from typing import Optional 


@dataclass
class FlinkClient:
    """
    Wrapper for Flink REST API
    """
    endpoint: str
    timeout: int = 10  # seconds
    
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET request with error handling."""
        resp = requests.get(                # Makes the request, stores response
            f"{self.endpoint}{path}",
            params=params,
            timeout=self.timeout
        )
        resp.raise_for_status()             # Fail fast: If status code is 4xx or 5xx, raise an exception
        return resp.json()                  # Only reaches here if status was 2xx (success)

    """
    Rightsizing Metrics
        1. Cluster — What's the shape of the whole thing?
        2. Jobs — What's running on it?
        3. TaskManagers — Who's doing the work?
        4. Backpressure — Where are the bottlenecks?
        5. Checkpoints — Is state healthy?    
    """

    # =========================================================================
    # 1. Cluster - top-level summary ("at a glance" view)
    # =========================================================================
    """
    Rightsizing Metrics
        → 1. Cluster — What's the shape of the whole thing?
        2. Jobs — What's running on it?
        3. TaskManagers — Who's doing the work?
        4. Backpressure — Where are the bottlenecks?
        5. Checkpoints — Is state healthy? 

    Cluster = Multiple machines (or processes) working together as one system.
        local setup: The "cluster" is all running inside Docker containers on your laptop.
        production: separate VMs or physical servers.
    
    "The Flink cluster"
        │
        ├── JobManager (1)        ← Coordinator, runs on one node
        │
        ├── TaskManager (N)       ← Worker, runs on node 1
        ├── TaskManager (N)       ← Worker, runs on node 2
        └── TaskManager (N)       ← Worker, runs on node 3

    You talk to the JobManager. You submit a job to "the cluster." 
    -> *Check cluster overview before drilling into specific TaskManagers or jobs*

    Endpoint metrics:
        taskmanagers        → How many worker JVMs are running
        slots-total         → Total parallel capacity (sum of all TM slots)
        slots-available     → Unused capacity (slots not assigned to tasks)
        jobs-running        → Active jobs
        jobs-finished       → Completed jobs
        jobs-cancelled      → Manually stopped
        jobs-failed         → Crashed

    Cluster Health Checks (overview):
        slots-available high, jobs-running low  → Overprovisioned, paying for idle capacity
        slots-available = 0, jobs-running > 0   → At capacity — no room for new jobs
        jobs-failed > 0                         → Something broke, go investigate

    Then Drill Down:
        - TaskManagers (which ones are idle?)
        - Jobs (which one is backpressured?)
        - Checkpoints (is state healthy?)
    """
    def get_cluster_overview(self) -> dict:
        """Get cluster-level stats (slots, TaskManagers, jobs)."""
        return self._get("/overview")
    
    # =========================================================================
    # 2. Jobs - the streaming apps running on the cluster
    # =========================================================================
    """
    Rightsizing Metrics
        1. Cluster — What's the shape of the whole thing?
        → 2. Jobs — What's running on it?
        3. TaskManagers — Who's doing the work?
        4. Backpressure — Where are the bottlenecks?
        5. Checkpoints — Is state healthy? 
    
    A job is your code — the DAG (directed acyclic graph) of operators you wrote,
    compiled, and submitted to the cluster.

        Your code:
            source.map(parse).keyBy(id).window(5min).aggregate(sum).sink(kafka)

        Becomes a job with vertices (operators):
            ┌────────┐   ┌───────┐   ┌────────┐   ┌───────────┐   ┌──────┐
            │ Source │ → │ Parse │ → │ KeyBy/ │ → │ Aggregate │ → │ Sink │
            │        │   │ (map) │   │ Window │   │           │   │      │
            └────────┘   └───────┘   └────────┘   └───────────┘   └──────┘
               vertex      vertex      vertex        vertex        vertex

    Job states:
        RUNNING     → Active, processing records
        FINISHED    → Completed (bounded streams only)
        FAILED      → Crashed — check logs
        CANCELLED   → Manually stopped
        RESTARTING  → Recovering from failure

    Terminology:
        job         → The whole pipeline
        vertex      → One operator in the pipeline (Flink calls them "vertices")
        subtask     → Parallel instance of a vertex (parallelism=4 → 4 subtasks)

    Why we care for rightsizing:
    Jobs are the unit of analysis
        - Each job consumes slots (capacity)
        - Backpressure is measured per-vertex within a job
        - Checkpoints are per-job
        - One badly-tuned job can starve others of resources

    *The responses live one level deeper. But...*        
    Rightsizing Responses:
        Many jobs, slots-available = 0   → Capacity crunch — need more TMs or reduce parallelism
        One job using most slots         → Check if parallelism is justified by throughput
        Jobs in FAILED/RESTARTING        → Stability issue — fix before optimizing cost
        Job's parallelism >> actual load → Overparallelized — reduce, free up slots
    """

    def get_jobs_overview(self) -> list[dict]:
        """Get all jobs (running, finished, failed, etc.)."""
        return self._get("/jobs/overview")["jobs"]
    
    def get_running_jobs(self) -> list[dict]:
        """Get only running jobs."""
        return [j for j in self.get_jobs_overview() if j["state"] == "RUNNING"]
    
    def get_job_details(self, job_id: str) -> dict:
        """Get job execution graph (vertices/operators)."""
        return self._get(f"/jobs/{job_id}")
    
    def get_job_vertices(self, job_id: str) -> list[dict]:
        """Get all operators (vertices) for a job."""
        return self.get_job_details(job_id)["vertices"]

    # =========================================================================
    # 3. TaskManagers - the worker JVMs that execute your operators
    # =========================================================================
    """
    Rightsizing Metrics
        1. Cluster — What's the shape of the whole thing?
        2. Jobs — What's running on it?
        → 3. TaskManagers — Who's doing the work?
        4. Backpressure — Where are the bottlenecks?
        5. Checkpoints — Is state healthy? 
    
    Flink architecture:
        JobManager (1)      → The brain. Schedules work, coordinates checkpoints.
        TaskManagers (N)    → The workers. Actually run your operators.

    Each TaskManager is:
        - One JVM process
        - Running in one K8s pod (in your setup)
        - Allocated CPU + RAM that you're paying for
        - Hosts "slots" — fixed capacity for parallel operator instances

    Slots:
        TaskManager with 4 slots can run 4 parallel tasks (operator instances).
        If you have parallelism=8, you need at least 2 TaskManagers with 4 slots each.

        TaskManager A          TaskManager B
        ┌────┬────┬────┬────┐  ┌────┬────┬────┬────┐
        │ T1 │ T2 │ T3 │ T4 │  │ T5 │ T6 │ T7 │ T8 │
        └────┴────┴────┴────┘  └────┴────┴────┴────┘
          4 slots occupied       4 slots occupied

    Factory analogy (from THEORY.md):
        TaskManager  →  Tool/station on the floor
        Slot         →  Capacity of that station
        Utilization  →  Is this tool doing useful work, or sitting idle?

    Key metrics we pull:
        Status.JVM.CPU.Load         → Are we paying for idle cores?
        Status.JVM.Memory.Heap.*    → Overprovisioned RAM?
        slots used / slots total    → Allocated capacity sitting empty?

    Rightsizing Responses:
        Low CPU across all TMs       → Overprovisioned — reduce replicas or CPU requests
        Low heap utilization         → Overprovisioned — reduce memory requests
        Slots unused                 → Parallelism lower than capacity — reduce TMs or slots
        One TM high, others low      → Unbalanced — check partition skew or operator placement
    """
    def get_taskmanagers(self) -> list[dict]:
        """Get all TaskManagers."""
        return self._get("/taskmanagers")["taskmanagers"]
    
    def get_taskmanager_metrics(self, tm_id: str, metrics: list[str]) -> dict:
        """
        Get specific metrics for a TaskManager.
        
        Args:
            tm_id: TaskManager ID
            metrics: List of metric names, e.g. ["Status.JVM.CPU.Load"]
        
        Returns:
            Dict of {metric_name: value}
        """
        params = {"get": ",".join(metrics)}
        raw = self._get(f"/taskmanagers/{tm_id}/metrics", params)
        return {m["id"]: m["value"] for m in raw}
    
    # Convenience: Standard TM metrics bundle
    """
    Flink exposes hundreds of metrics. The full list is at:
       https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/
       not pulling: network, GC, thread count, operator-level metrics
    """
    STANDARD_TM_METRICS = [  #  Start with CPU + memory metrics
        "Status.JVM.CPU.Load",           # Are cores idle?
        "Status.JVM.Memory.Heap.Used",   # How much RAM is actually used?
        "Status.JVM.Memory.Heap.Max",    # How much RAM is allocated?
        "Status.JVM.Memory.NonHeap.Used", # JVM overhead (class metadata, etc.)
        "Status.JVM.Memory.NonHeap.Max",
    ]

    # Define TaskManager raw "Utilization" (factory term -- see THEORY.md)
    def get_taskmanager_utilization(self, tm_id: str) -> dict:
        """Get standard utilization metrics for a TaskManager."""
        raw = self.get_taskmanager_metrics(tm_id, self.STANDARD_TM_METRICS)
        
        cpu_load = float(raw.get("Status.JVM.CPU.Load", 0))
        heap_used = int(raw.get("Status.JVM.Memory.Heap.Used", 0))
        heap_max = int(raw.get("Status.JVM.Memory.Heap.Max", 1))
        nonheap_used = int(raw.get("Status.JVM.Memory.NonHeap.Used", 0))
        nonheap_max = int(raw.get("Status.JVM.Memory.NonHeap.Max", 1))
        
        # heap_used / heap_max --> What % of allocated RAM is in use right now?
        return {
            # Raw CPU (0.0 - 1.0 from JVM)
            "cpu_load": cpu_load,                       # Raw (0.0 - 1.0)
            "cpu_load_pct": round(cpu_load * 100, 1),   # Percentage (0 - 100)

            # Raw memory (bytes)
            "heap_used_bytes": heap_used,
            "heap_max_bytes": heap_max,

            # Unit conversion (bytes → MB)
            "heap_used_mb": round(heap_used / (1024 * 1024), 1),
            "heap_max_mb": round(heap_max / (1024 * 1024), 1),
            "nonheap_used_mb": round(nonheap_used / (1024 * 1024), 1),
            "nonheap_max_mb": round(nonheap_max / (1024 * 1024), 1),

            # Utilization calculation (used / max)
            "heap_utilization_pct": round(heap_used / heap_max * 100, 1) if heap_max > 0 else 0
            # "nonheap_utilization_pct": nonheap is JVM's own bookkeeping - not actionable for cost optimization
        }
    
    # Define TaskManager "Effective Utilization" == Ue (factory term -- see THEORY.md)
    #
    # Raw utilization (above) answers: "What % of allocated resources are in use?"
    # Effective utilization (Ue) answers: "What % of time is this TM doing useful work
    #                                      when there IS work available to do?"
    #
    # Ue requires correlating multiple signals:
    #   - Was input actually flowing? (source rate > 0)
    #   - Was the slot assigned work? (not idle waiting for a job)
    #   - Was the TM healthy? (not restarting, not in backpressure from downstream)
    #
    # Why not here?
    #   flink_client.py collects raw metrics — one API call, one response.
    #   Ue requires combining: utilization + backpressure + job state + input rate.
    #   That's a classification problem, not a data collection problem.
    #
    # See 1_identify.py for:
    #   - Correlating signals across endpoints
    #   - Distinguishing "idle because overprovisioned" vs "idle because starved"
    #   - Outputting actionable classifications (overprovisioned / CPU-bound / etc.)


    # =========================================================================
    # 4. Backpressure - what % of the time are your threads blocked by a downstream operator?
    # =========================================================================
    """ 
    Rightsizing Metrics
        1. Cluster — What's the shape of the whole thing?
        2. Jobs — What's running on it?
        3. TaskManagers — Who's doing the work?
        → 4. Backpressure — Where are the bottlenecks?
        5. Checkpoints — Is state healthy? 
    
    Flink's TaskManager (TM) periodically samples the stack traces of task threads
    and calculates if a thread is "frequently" blocked when trying to push data to
    the next operator's input buffer. These are typically operators/tasks running 
    as threads within the same process (the TaskManager JVM).

        Ratio | Level
        ------|--------
        < 0.10 | ok
        0.10 - 0.50 | low
        > 0.50 | high

        Operator A  →  [buffer: 3 slots]  →  Operator B

        A produces fast, B consumes slow:
        Time : Bounded Buffer (buffer = allocated RAM before write; bounded = fixed max size)
        Time 0:  [ _ _ _ ]     buffer empty
        Time 1:  [ x _ _ ]     A writes
        Time 2:  [ x x _ ]     A writes
        Time 3:  [ x x x ]     A writes, buffer full
        Time 4:  A blocks      can't write, waits for B to drain
                ↑
                backpressure

    Backpressure appears on the operator *before* the bottleneck
        Source → Parse → Enrich → Sink
                            ↑
                    slow (database lookup)
             
    Result: Parse shows HIGH backpressure (it's waiting)
            Enrich shows OK (it's the bottleneck, not backed up)

    Rightsizing Responses:
        All operators "ok"   → Overprovisioned — room to cut
        One operator "high"  → Bottleneck identified — fix just that one
        Everything "high"    → Undersized, or sink can't keep up
    """
    
    
    def get_backpressure(self, job_id: str, vertex_id: str) -> dict:
        """
        Get backpressure status for a specific operator.
        
        Returns dict with 'backpressure-level': 'ok' | 'low' | 'high'
        """
        return self._get(f"/jobs/{job_id}/vertices/{vertex_id}/backpressure")
    
    # =========================================================================
    # 5. Checkpoints - periodic snapshot of all state written to durable storage
    # =========================================================================
    """
    Rightsizing Metrics
        1. Cluster — What's the shape of the whole thing?
        2. Jobs — What's running on it?
        3. TaskManagers — Who's doing the work?
        4. Backpressure — Where are the bottlenecks?
        → 5. Checkpoints — Is state healthy? 

    Flink periodically snapshots all operator state (counters, windows, aggregations)
    to durable storage (GCS, S3, HDFS) so that if the job crashes, it  can restart 
    from the last checkpoint instead of reprocessing everything from the beginning.

    How it works:
        1. JobManager injects "barrier" markers into the stream  (in-band signaling)
            Regular record:  [type marker: DATA]    [your payload bytes]
            Barrier:         [type marker: BARRIER] [checkpoint ID, timestamp, config]
        2. When an operator sees a barrier, it snapshots its state -> write to storage (configurable)
        3. All operators report success → checkpoint complete

        Source  →  [barrier]  →  Parse  →  [barrier]  →  Enrich  →  [barrier]  →  Sink
                       ↓              ↓                     ↓
                   snapshot       snapshot              snapshot
                       ↓              ↓                     ↓
                   ┌─────────────────────────────────────────────┐
                   │  Durable Storage (GCS/S3)                   │
                   │  checkpoint-00047/                          │
                   │    ├── _metadata                            │
                   │    ├── operator-1-state                     │
                   │    └── operator-2-state                     │
                   └─────────────────────────────────────────────┘

    Hidden costs (not visible in "processing time"):
        - CPU cycles for serializing state to bytes
        - Memory bandwidth during snapshot
        - Network I/O writing to storage
        - Storage costs ($$$ per GB)
        - Brief pauses while barriers align (especially with exactly-once)

    Key signals from the API:
        end_to_end_duration  → How long the checkpoint took (ms)
        state_size           → How many bytes were written
        counts.completed     → How many checkpoints succeeded
        counts.failed        → How many failed (should be 0)

    Rightsizing Responses:
        Duration increasing     → State growing, or memory pressure causing slow serialization
        Failures > 0            → Timeout, disk pressure, or OOM during checkpoint
        State size large        → Paying for storage + serialization CPU; consider TTL or incremental
        Duration spiky          → GC pauses or resource contention; check heap utilization
    """

    def get_checkpoint_stats(self, job_id: str) -> dict:
        """Get checkpoint statistics for a job."""
        return self._get(f"/jobs/{job_id}/checkpoints")
    
    # =========================================================================
    # 9999. Job Metrics (commented out — not needed for initial rightsizing)
    # =========================================================================
    
    # NOTE: Job metrics vs job status are different things:
    #   /jobs/overview         → metadata (state, name, start time)
    #   /jobs/{id}/metrics     → measurements (records in/out, bytes, watermarks)
    #
    # Rightsizing cares about backpressure, CPU, memory, checkpoint health.
    # Job metrics (throughput, latency) matter more for "is my pipeline keeping up"
    # than "am I overprovisioned." Add these back if you need throughput tracking.
    #
    # def list_job_metrics(self, job_id: str) -> list[str]:
    #     """
    #     List all available metric names for a job.
    #     
    #     Call this first to see what you can query.
    #     """
    #     raw = self._get(f"/jobs/{job_id}/metrics")
    #     return [m["id"] for m in raw]
    #
    # def get_job_metrics(self, job_id: str, metrics: list[str]) -> dict:
    #     """
    #     Get specific metrics for a job.
    #     
    #     Args:
    #         job_id: Job ID
    #         metrics: List of metric names, e.g. ["numRecordsIn", "numRecordsOut"]
    #     
    #     Returns:
    #         Dict of {metric_name: value}
    #     
    #     Example:
    #         >>> client.get_job_metrics(job_id, ["numRecordsIn", "numRecordsOut"])
    #         {"numRecordsIn": "42857", "numRecordsOut": "42857"}
    #     """
    #     params = {"get": ",".join(metrics)}
    #     raw = self._get(f"/jobs/{job_id}/metrics", params)
    #     return {m["id"]: m["value"] for m in raw}
    

    
    

# =============================================================================
# Quick test if run directly
# =============================================================================

if __name__ == "__main__":
    import sys
    
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8081"
    client = FlinkClient(endpoint)
    
    try:
        overview = client.get_cluster_overview()
        print(f"Connected to Flink at {endpoint}")
        print(f"  TaskManagers: {overview['taskmanagers']}")
        print(f"  Slots: {overview['slots-available']}/{overview['slots-total']}")
        print(f"  Running jobs: {overview['jobs-running']}")
    except requests.exceptions.ConnectionError:
        print(f"Could not connect to {endpoint}")
        sys.exit(1)
