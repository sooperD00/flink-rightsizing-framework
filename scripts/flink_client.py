#!/usr/bin/env python3
"""
Flink REST API Client

All Flink API calls live here. Import into phase scripts.

Usage:
    from flink_client import FlinkClient
    
    client = FlinkClient("http://localhost:8081")
    jobs = client.get_running_jobs()
    bp = client.get_backpressure(job_id, vertex_id)
"""

import requests
from dataclasses import dataclass
from typing import Optional


@dataclass
class FlinkClient:
    """Wrapper for Flink REST API."""
    
    endpoint: str
    timeout: int = 10
    
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET request with error handling."""
        resp = requests.get(
            f"{self.endpoint}{path}",
            params=params,
            timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()
    
    # =========================================================================
    # Jobs
    # =========================================================================
    
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
    # Backpressure
    # =========================================================================
    
    def get_backpressure(self, job_id: str, vertex_id: str) -> dict:
        """
        Get backpressure status for a specific operator.
        
        Returns dict with 'backpressure-level': 'ok' | 'low' | 'high'
        """
        return self._get(f"/jobs/{job_id}/vertices/{vertex_id}/backpressure")
    
    # =========================================================================
    # Checkpoints
    # =========================================================================
    
    def get_checkpoint_stats(self, job_id: str) -> dict:
        """Get checkpoint statistics for a job."""
        return self._get(f"/jobs/{job_id}/checkpoints")
    
    # =========================================================================
    # TaskManagers
    # =========================================================================
    
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
    
    # =========================================================================
    # Cluster
    # =========================================================================
    
    def get_cluster_overview(self) -> dict:
        """Get cluster-level stats (slots, TaskManagers, jobs)."""
        return self._get("/overview")
    
    # =========================================================================
    # Convenience: Standard TM metrics bundle
    # =========================================================================
    
    STANDARD_TM_METRICS = [
        "Status.JVM.CPU.Load",
        "Status.JVM.Memory.Heap.Used",
        "Status.JVM.Memory.Heap.Max",
        "Status.JVM.Memory.NonHeap.Used",
        "Status.JVM.Memory.NonHeap.Max",
    ]
    
    def get_taskmanager_utilization(self, tm_id: str) -> dict:
        """Get standard utilization metrics for a TaskManager."""
        raw = self.get_taskmanager_metrics(tm_id, self.STANDARD_TM_METRICS)
        
        cpu_load = float(raw.get("Status.JVM.CPU.Load", 0))
        heap_used = int(raw.get("Status.JVM.Memory.Heap.Used", 0))
        heap_max = int(raw.get("Status.JVM.Memory.Heap.Max", 1))
        nonheap_used = int(raw.get("Status.JVM.Memory.NonHeap.Used", 0))
        nonheap_max = int(raw.get("Status.JVM.Memory.NonHeap.Max", 1))
        
        return {
            "cpu_load": cpu_load,
            "cpu_load_pct": round(cpu_load * 100, 1),
            "heap_used_bytes": heap_used,
            "heap_max_bytes": heap_max,
            "heap_used_mb": round(heap_used / (1024 * 1024), 1),
            "heap_max_mb": round(heap_max / (1024 * 1024), 1),
            "heap_utilization_pct": round(heap_used / heap_max * 100, 1) if heap_max > 0 else 0,
            "nonheap_used_mb": round(nonheap_used / (1024 * 1024), 1),
            "nonheap_max_mb": round(nonheap_max / (1024 * 1024), 1),
        }


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
