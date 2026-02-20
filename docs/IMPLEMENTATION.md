# Implementation Plan

Target: 6 weeks to publishable results

---

## Week 1 — Local Environment + Phase 0 Scripts

**Goal:** Working local Flink cluster, observation scripts tested against real data.

- [x] Install Docker Desktop, enable Kubernetes
- [x] Install helm, kubectl
- [x] Run `bin/local_cluster.sh` — Flink operator + example job
- [x] Test `flink_client.py` connects to local cluster
- [x] Run `0_observe.py`, verify JSON output in `data/staging/`
- [x] Run `1_classify.py`, verify classification output in `data/marts/`
- [x] Commit: "feat: Phase 0 observe working locally"

✅ Confirmed on macOS Sequoia 15.3.1 (Apple Silicon, Mac Mini) — 2026-02-19

**Exit criteria:** `snapshot_latest.json` contains backpressure, utilization, and checkpoint data.

---

## Week 2 — Phase 1 Identification + Nexmark Setup

**Goal:** Classification logic working, Nexmark benchmark deployed locally.

- [x] Write `1_classify.py` — reads staging, outputs classification to `data/marts/`
- [x] Decision rules: overprovisioned / CPU-bound / memory-starved / checkpoint-thrash / sink-blocked
- [ ] Deploy Nexmark benchmark suite locally (standard Flink streaming benchmark)
- [ ] Collect baseline Nexmark metrics with default config
- [ ] Commit: "feat: Phase 1 identify + Nexmark baseline"

**Exit criteria:** Can run Nexmark queries, classify their state automatically.

---

## Week 3 — Phase 2 Experiment Framework

**Goal:** DoE parameter sweep working on Nexmark workloads.

- [ ] Write `2_experiment.py` — grid search over parallelism, memory, slots
- [ ] Extract thresholds to `config/thresholds.yaml` — editable w/o touching logic
- [ ] Define stage schemas (`schemas.py`)
	- dataclasses for observe → identify → experiment contracts
- [ ] `2_experiment.py` varies thresholds programmatically during sweeps
	- (reads from config, not hardcoded)
- [ ] Implement metrics collection per experiment run
- [ ] Implement "find the floor" logic (backpressure threshold detection)
- [ ] Run sweep on 2-3 Nexmark queries
- [ ] Output: `data/marts/experiment_results.json`
- [ ] Commit: "feat: Phase 2 DoE sweep framework"

**Exit criteria:** Can identify optimal config for a Nexmark query with data to prove it.

---

## Week 4 — GKE Deployment + Real Benchmarks

**Goal:** Framework running on real cloud infrastructure, not just local.

- [ ] Provision GKE cluster (small, 3-node)
- [ ] Deploy Flink operator to GKE
- [ ] Deploy Nexmark benchmark suite to GKE
- [ ] Run full observation → identification → experiment cycle on GKE
- [ ] Collect cost data (actual $ from GCP billing)
- [ ] Commit: "feat: GKE deployment validated"

**Exit criteria:** Have real cloud cost numbers before/after optimization.

---

## Week 5 — Head-to-Head: DoE vs. Flink Autoscaler

**Goal:** Comparative data for the paper.

- [ ] Enable Flink Kubernetes Operator autoscaler on same Nexmark workloads
- [ ] Run identical load patterns with autoscaler
- [ ] Collect metrics: cost, latency, throughput, stability
- [ ] Run identical load patterns with DoE-tuned static config
- [ ] Collect same metrics
- [ ] Document methodology, edge cases, surprises
- [ ] Commit: "data: head-to-head benchmark results"

**Exit criteria:** Side-by-side comparison data, charts drafted.

---

## Week 6 — Paper Draft + Publish Results

**Goal:** Shareable writeup with real data.

- [ ] Write `docs/RESULTS.md` — charts, methodology, findings
- [ ] Draft paper outline (Introduction, Methodology, Results, Discussion)
- [ ] Write first draft of paper (target: ~4-6 pages)
- [ ] Publish results to repo (charts, raw data, analysis notebooks)
- [ ] Commit: "docs: benchmark results and paper draft"

**Exit criteria:** Public repo with real results, paper draft ready for review.

---

## After Week 6

- [ ] Incorporate feedback from community/collaborators
- [ ] Refine paper for submission
- [ ] Phase 3 + 4 scripts (phase-in, sustain)

---

## Cleanup / Tech Debt

Discovered during local setup (2026-02-19):

- [x] Helm repo URL: Apache moves older releases from `downloads.apache.org` to `archive.apache.org`. Updated `local_cluster.sh` to use 1.13.0 archive URL. Add a note in setup docs that this URL will need updating when new operator versions ship.
- [x] Service account + RBAC: The Flink operator doesn't create a `flink` service account in the test namespace. Added `kubectl create serviceaccount` + namespace-scoped role/rolebinding to `local_cluster.sh`.
- [x] JobManager timeout bug: The wait loop in `deploy_example_job()` would print "deployed!" even if the JobManager never started. Added `JM_READY` flag with `exit 1` on timeout.
- [x] Backpressure retry: The `/backpressure` REST endpoint returns 500 for ~30s on a fresh cluster. Added retry loop (3 attempts, 10s apart) in `0_observe.py` that degrades gracefully to `"unknown"` instead of crashing.
- [ ] Atomic writes (write-to-temp-then-rename) for `_latest.json` files — needed before continuous/cron mode
- [ ] `local_cluster.sh` header and docs updated for cross-platform (macOS + Windows)

---

## Benchmarking Notes

**Why Nexmark:**
- Industry-standard streaming benchmark (used by Flink, Beam, Kafka Streams comparisons)
- Multiple query patterns (windowed aggregations, joins, filtering)
- Reproducible, well-documented
- Results are comparable to published literature

**What we're measuring:**
| Metric | Source | Why |
|--------|--------|-----|
| Throughput | Flink metrics | Does optimization hurt processing speed? |
| Latency (p99) | Flink metrics | Does optimization hurt responsiveness? |
| Cost | GCP billing | The whole point |
| Stability | Backpressure over time | Does autoscaler thrash? |
| Time to optimal | Wall clock | How long to reach steady state? |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| GKE costs balloon | Set billing alerts, use preemptible nodes, tear down nightly |
| Nexmark doesn't expose the problem | Also test on synthetic "spiky load" patterns |
| Autoscaler outperforms DoE | That's a valid result — publish it honestly |
| Week slippage | Phases 3-4 scripts are optional for paper; can defer |
