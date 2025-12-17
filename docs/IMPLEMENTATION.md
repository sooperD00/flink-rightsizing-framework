# Implementation Plan

Target: 6 weeks to publishable results

---

## Week 1 — Local Environment + Phase 0 Scripts

**Goal:** Working local Flink cluster, observation scripts tested against real data.

- [ ] Install Docker Desktop, enable Kubernetes
- [ ] Install helm, kubectl
- [ ] Run `setup/local_cluster.sh` — Flink operator + example job
- [ ] Test `flink_client.py` connects to local cluster
- [ ] Run `0_observe.py`, verify JSON output in `data/staging/`
- [ ] Commit: "feat: Phase 0 observe working locally"

**Exit criteria:** `snapshot_latest.json` contains backpressure, utilization, and checkpoint data.

---

## Week 2 — Phase 1 Identification + Nexmark Setup

**Goal:** Classification logic working, Nexmark benchmark deployed locally.

- [ ] Write `1_identify.py` — reads staging, outputs classification to `data/marts/`
- [ ] Decision rules: overprovisioned / CPU-bound / memory-starved / checkpoint-thrash / sink-blocked
- [ ] Deploy Nexmark benchmark suite locally (standard Flink streaming benchmark)
- [ ] Collect baseline Nexmark metrics with default config
- [ ] Commit: "feat: Phase 1 identify + Nexmark baseline"

**Exit criteria:** Can run Nexmark queries, classify their state automatically.

---

## Week 3 — Phase 2 Experiment Framework

**Goal:** DoE parameter sweep working on Nexmark workloads.

- [ ] Write `2_experiment.py` — grid search over parallelism, memory, slots
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
- [ ] Post to Reddit/HN/Flink community for feedback
- [ ] Commit: "docs: benchmark results and paper draft"

**Exit criteria:** Public repo with real results, paper draft ready for co-author review.

---

## After Week 6

- [ ] Incorporate feedback from community/collaborators
- [ ] Refine paper for submission (IEEE? VLDB? blog post first?)
- [ ] Phase 3 + 4 scripts (phase-in, sustain) — as needed for consulting engagements
- [ ] Identify pilot consulting clients from inbound interest

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

---

## Social Media

Build in public. Each post is a milestone, not a sales pitch.

| Platform | Use For | Timing |
|----------|---------|--------|
| **Reddit** (r/dataengineering, r/apacheflink, r/devops) | Primary audience. Share progress, ask questions, help others. | Already have account (sooperD00). Post when scripts work, again when benchmarks run. |
| **Hacker News** | "Show HN" when there's something real to show. | Wait until Week 4+ (GKE results). HN is brutal on vaporware. |
| **LinkedIn** | Different audience — managers, decision-makers. Journey posts. | Cross-post milestones. |
| **dbt Slack** | Mention in relevant channels, connect with data eng community. | Opportunistic — when Flink topics come up. |
| **Data Engineering Slack** | Same as above, more Flink-specific folks. | Same. |

**Skip for now:** Upwork/Toptal (job boards, not communities), Locally Optimistic (invite-gated), Flink Zulip (tiny).

**Posting cadence:**
1. "I have an idea" (done — README exists)
2. "I implemented the scripts" (Week 1-2)
3. "I spun up K8s and ran it" (Week 4)
4. "Here's what happened: DoE vs autoscaler" (Week 5-6)
5. Invite collaboration at each step

Don't post marketing copy. Show work. Let people come to you.

---

## Paper

**Target venues (in order of fit):**

| Venue | Type | Why |
|-------|------|-----|
| **ACM/SPEC ICPE** (Int'l Conf on Performance Engineering) | Conference | Performance optimization focus. Multiple Flink autoscaling papers here. Good fit for DoE angle. |
| **ACM DEBS** (Distributed and Event-based Systems) | Conference | Stream processing specific. Right audience. |
| **IEEE IPDPS** (Int'l Parallel and Distributed Processing Symposium) | Conference | Where AuTraScale was published. Solid venue. |
| **IEEE TPDS** (Trans. on Parallel and Distributed Systems) | Journal | Where GML (ML-based Flink tuning) was published. Reach goal — high bar. |
| **Cluster Computing** (Springer) | Journal | Lower bar, still credible. Recent Flink DQN paper landed here. |

**Recommendation:** Target **ICPE** or **DEBS** first. Conference route is faster (months vs. 6-18 month journal cycle), builds credibility sooner, and the performance engineering framing fits your angle.

**Co-author question:** Sole author is fine for a methods paper with real benchmarks. But a co-author with Flink production experience or academic affiliation strengthens it. (Jimmy? Benchmarking partner? Someone from Reddit who contributes test cases?)

**Timeline:** Draft by Week 6, submit after community feedback.