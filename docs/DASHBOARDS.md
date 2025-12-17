# Dashboard Specifications

ASCII mockups for the observability dashboards. Hand this to your analytics team.

Data source: `data/marts/` (output from the framework scripts)

---

## Dashboard 1: Backpressure Status

**Purpose:** Triage view. What needs attention right now?

**Refresh:** Every 5 minutes

**Data source:** `0_observe.py` → `data/staging/` → `1_identify.py` → `data/marts/backpressure_status.json`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  BACKPRESSURE STATUS                                          Last updated: 5m  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Summary: 12 operators monitored │ 10 OK │ 1 LOW │ 1 HIGH                       │
│                                                                                 │
│  ┌─ HIGH BACKPRESSURE (Action Required) ────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Job              Operator              Since      Severity   Action     │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  calendar-sync    JsonParser            2h 15m     ████████   BOTTLENECK │   │
│  │                                                               Do NOT     │   │
│  │                                                               reduce     │   │
│  │                                                               resources  │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ LOW BACKPRESSURE (Watch) ───────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Job              Operator              Since      Severity   Action     │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  event-enrich     UserLookup            45m        ████░░░░   WATCH      │   │
│  │                                                               May become │   │
│  │                                                               bottleneck │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ OK (Candidates for Reduction) ──────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Job              Operator              OK Since   Confidence  Action    │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  calendar-sync    KafkaSource           7d+        ★★★★★       REDUCE    │   │
│  │  calendar-sync    WindowAgg             7d+        ★★★★★       REDUCE    │   │
│  │  calendar-sync    PostgresSink          7d+        ★★★★★       REDUCE    │   │
│  │  event-enrich     KafkaSource           7d+        ★★★★★       REDUCE    │   │
│  │  event-enrich     EventFilter           3d         ★★★★☆       REDUCE    │   │
│  │  event-enrich     OutputMapper          3d         ★★★★☆       REDUCE    │   │
│  │  notifications    KafkaSource           7d+        ★★★★★       REDUCE    │   │
│  │  notifications    TemplateRender        5d         ★★★★★       REDUCE    │   │
│  │  notifications    EmailSink             7d+        ★★★★★       REDUCE    │   │
│  │  notifications    PushSink              7d+        ★★★★★       REDUCE    │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  Confidence = f(consecutive_ok_hours). 7d+ = high confidence.            │   │
│  │  Auto-reduction eligible after: 7d OK + CPU < 30% + Mem < 50%            │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key interactions:**
- Click operator → drill down to subtask-level backpressure
- Click job → show all operators for that job
- Sort by "Since" to find longest-running issues

---

## Dashboard 2: Resource Utilization

**Purpose:** Where is money burning? Gap between allocated and used.

**Refresh:** Every 5 minutes

**Data source:** `0_observe.py` → `data/marts/utilization_summary.json`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  RESOURCE UTILIZATION                                        Last updated: 5m   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Cluster: 8 TaskManagers │ 32 slots total │ 24 slots used                       │
│                                                                                 │
│  ┌─ CPU ────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  TaskManager    Allocated    Used        Utilization                     │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  tm-001         4 cores      0.8 cores   ████░░░░░░░░░░░░░░░░  20%       │   │
│  │  tm-002         4 cores      1.2 cores   ██████░░░░░░░░░░░░░░  30%       │   │
│  │  tm-003         4 cores      0.5 cores   ██░░░░░░░░░░░░░░░░░░  12%       │   │
│  │  tm-004         4 cores      3.1 cores   ███████████████░░░░░  78%  ⚠️   │   │
│  │  tm-005         4 cores      0.9 cores   ████░░░░░░░░░░░░░░░░  22%       │   │
│  │  tm-006         4 cores      0.6 cores   ███░░░░░░░░░░░░░░░░░  15%       │   │
│  │  tm-007         4 cores      1.0 cores   █████░░░░░░░░░░░░░░░  25%       │   │
│  │  tm-008         4 cores      0.4 cores   ██░░░░░░░░░░░░░░░░░░  10%       │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  TOTAL          32 cores     8.5 cores   ██████░░░░░░░░░░░░░░  27%       │   │
│  │                                                                          │   │
│  │  💰 You're paying for 32 cores, using 8.5. Potential 73% reduction.      │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ MEMORY (Heap) ──────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  TaskManager    Allocated    Used        Utilization                     │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  tm-001         16 GB        4.2 GB      █████░░░░░░░░░░░░░░░  26%       │   │
│  │  tm-002         16 GB        5.8 GB      ███████░░░░░░░░░░░░░  36%       │   │
│  │  tm-003         16 GB        3.1 GB      ████░░░░░░░░░░░░░░░░  19%       │   │
│  │  tm-004         16 GB        12.4 GB     ███████████████░░░░░  78%  ⚠️   │   │
│  │  ...                                                                     │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  TOTAL          128 GB       48 GB       ███████░░░░░░░░░░░░░  38%       │   │
│  │                                                                          │   │
│  │  💰 You're paying for 128 GB, using 48. Potential 62% reduction.         │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ SLOTS ──────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Total: 32 │ Used: 24 │ Free: 8                                          │   │
│  │                                                                          │   │
│  │  8 idle slots = potentially 2 unnecessary TaskManagers                   │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key interactions:**
- Click TaskManager → show all jobs/operators running on it
- Time range selector → see utilization trends (7d, 30d)
- Alert threshold: flag any TM with utilization < 30%

---

## Dashboard 3: Checkpoint Health

**Purpose:** The sneaky cost driver. Catch problems before they break.

**Refresh:** Every 15 minutes

**Data source:** `0_observe.py` → `data/marts/checkpoint_health.json`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  CHECKPOINT HEALTH                                           Last updated: 15m  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─ SUMMARY ────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Jobs monitored: 5                                                       │   │
│  │  Total checkpoints (24h): 1,440                                          │   │
│  │  Failed checkpoints (24h): 3  ⚠️                                         │   │
│  │  Avg checkpoint duration: 2.3s                                           │   │
│  │  Total state size: 4.2 GB                                                │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ PER-JOB DETAIL ─────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Job              State Size   Avg Duration   Trend        Status        │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  calendar-sync    1.8 GB       3.2s           ↗ +15%/wk    ⚠️ GROWING    │   │
│  │  event-enrich     0.9 GB       1.1s           → stable     ✓ HEALTHY     │   │
│  │  notifications    0.4 GB       0.8s           → stable     ✓ HEALTHY     │   │
│  │  analytics-agg    1.0 GB       2.8s           → stable     ✓ HEALTHY     │   │
│  │  user-sessions    0.1 GB       0.4s           → stable     ✓ HEALTHY     │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ DURATION TREND (calendar-sync) ─────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  5s │                                                          ╭───      │   │
│  │     │                                              ╭───────────╯         │   │
│  │  3s │                          ╭───────────────────╯                     │   │
│  │     │      ╭───────────────────╯                                         │   │
│  │  1s │──────╯                                                             │   │
│  │     └────────────────────────────────────────────────────────────────    │   │
│  │      -30d                      -15d                           today      │   │
│  │                                                                          │   │
│  │  ⚠️ Duration increasing ~15%/week. Investigate state growth.             │   │
│  │     Consider: state TTL, incremental checkpoints, RocksDB tuning         │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key interactions:**
- Click job → show checkpoint history (last 100)
- Click "GROWING" status → show state size breakdown by operator
- Alert threshold: duration > 30s or failed > 5/day

---

## Dashboard 4: Auto-Scaling Candidates

**Purpose:** Recommended reductions with phased rollout schedule.

**Refresh:** Daily (recommendations don't need real-time)

**Data source:** `1_identify.py` → `data/marts/reduction_candidates.json`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  AUTO-SCALING CANDIDATES                                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Eligibility: 7d backpressure OK + CPU < 30% + Memory < 50%                     │
│                                                                                 │
│  ┌─ CANDIDATE: calendar-sync.KafkaSource ───────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Current parallelism: 8                                                  │   │
│  │  Recommended: 4 (based on 7d OK @ avg 12% CPU)                           │   │
│  │  Estimated savings: $420/month                                           │   │
│  │                                                                          │   │
│  │  Reduction schedule (exponential backoff):                               │   │
│  │  ┌────────────────────────────────────────────────────────────────────┐  │   │
│  │  │  Step   Parallelism   Wait Period   Gate                           │  │   │
│  │  │  ──────────────────────────────────────────────────────────────    │  │   │
│  │  │  1      8 → 6         24h           backpressure == OK             │  │   │
│  │  │  2      6 → 5         48h           backpressure == OK             │  │   │
│  │  │  3      5 → 4         72h           backpressure == OK             │  │   │
│  │  │  ──────────────────────────────────────────────────────────────    │  │   │
│  │  │  If backpressure != OK at any step → rollback + alert              │  │   │
│  │  └────────────────────────────────────────────────────────────────────┘  │   │
│  │                                                                          │   │
│  │  [Start Reduction]  [Dismiss]  [Snooze 7d]                               │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ CANDIDATE: event-enrich.EventFilter ────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Current parallelism: 4                                                  │   │
│  │  Recommended: 2 (based on 3d OK @ avg 18% CPU)                           │   │
│  │  Estimated savings: $180/month                                           │   │
│  │  Confidence: ★★★★☆ (3d is borderline — consider waiting)                 │   │
│  │                                                                          │   │
│  │  [Start Reduction]  [Dismiss]  [Snooze 7d]                               │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ SUMMARY ────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Total candidates: 6                                                     │   │
│  │  Total estimated monthly savings: $1,840                                 │   │
│  │  High confidence (7d+): 4                                                │   │
│  │  Medium confidence (3-7d): 2                                             │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key interactions:**
- "Start Reduction" → triggers `3_phase_in.py` workflow
- Rollback alert → Slack/PagerDuty integration
- History view → show past reductions and outcomes

---

## Dashboard 5: Experiment Results

**Purpose:** DoE parameter sweep visualization. Find the knee in the curve.

**Refresh:** After experiment runs

**Data source:** `2_experiment.py` → `data/marts/experiment_results.json`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  EXPERIMENT RESULTS: calendar-sync                                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─ PARAMETER SWEEP ────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Config              Parallelism  Memory   CPU Util  Backpressure  Cost  │   │
│  │  ─────────────────────────────────────────────────────────────────────   │   │
│  │  baseline            8            4096 MB  27%       OK            $$$   │   │
│  │  half_parallelism    4            4096 MB  48%       OK            $$    │   │
│  │  quarter_parallel    2            4096 MB  89%       HIGH ⚠️       $     │   │
│  │  half_memory         8            2048 MB  27%       OK            $$    │   │
│  │  half_both           4            2048 MB  52%       OK            $     │   │
│  │  aggressive          2            1024 MB  95%       HIGH ⚠️       $     │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ COST vs BACKPRESSURE (the knee) ────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Backpressure                                                            │   │
│  │  HIGH │                                              ●  ●                │   │
│  │       │                                          ╭───╯                   │   │
│  │       │                                      ╭───╯                       │   │
│  │   LOW │                                  ╭───╯                           │   │
│  │       │                          ╭───────╯                               │   │
│  │    OK │──────●─────────●─────────●                                       │   │
│  │       └──────────────────────────────────────────────────────────────    │   │
│  │       $$$        $$          $         $0                                │   │
│  │       (baseline)             ↑                                           │   │
│  │                              │                                           │   │
│  │                    OPTIMAL: half_both                                    │   │
│  │                    (lowest cost before backpressure)                     │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─ RECOMMENDATION ─────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Optimal config: half_both (parallelism=4, memory=2048MB)                │   │
│  │  Estimated savings vs baseline: 62%                                      │   │
│  │  Safe margin: 48% CPU headroom before backpressure                       │   │
│  │                                                                          │   │
│  │  [Apply to Staging]  [Export Config]  [Run Again]                        │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Key interactions:**
- Hover on data point → show full metrics for that config
- "Apply to Staging" → generates flink-conf.yaml diff
- "Export Config" → download recommended configuration

---

## Implementation Notes

**For PowerBI/Grafana/Superset:**
- JSON data sources from `data/marts/`
- Refresh via scheduled script runs or Airflow DAG
- Alerting via threshold triggers on key metrics

**Color coding:**
- 🟢 Green: OK / Healthy / Candidate for reduction
- 🟡 Yellow: Watch / Medium confidence / Approaching threshold
- 🔴 Red: Action required / Bottleneck / Failed

**Mobile considerations:**
- Dashboard 1 (Backpressure) is the priority for mobile/on-call
- Collapse detail sections by default on small screens
