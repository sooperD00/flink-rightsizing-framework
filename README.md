# Flink Rightsizing Framework

A systematic approach to Flink cluster cost optimization using Design of Experiments.

---

## The Problem

You're running Flink on Kubernetes. It worked fine. Then you added workloads, and now your bill is up 50-60% and you're not sure why. The resources look overprovisioned, but you don't know what's safe to cut.

The usual options:
- **Scale up and hope** — expensive, doesn't solve the underlying issue
- **Turn on autoscaling** — but do you know what it's actually doing? Does it optimize for *your* cost constraints?
- **Hire a Flink specialist** — works, but creates a single point of failure and doesn't leave your team any smarter

This repo is a different approach: **measure first, then experiment systematically, then phase in changes with rollback triggers.**

The methodology comes from Design of Experiments (DoE) — the same framework used in semiconductor manufacturing, clinical trials, and A/B testing infrastructure. It's not new or clever. It's just rigorous.

---

## The Approach

### Phase 0 — Observe

Don't tune. Don't guess. Instrument.

Collect 24-48 hours of:
- Backpressure (per operator)
- CPU and heap utilization (per TaskManager)
- Checkpoint duration and state size
- Slot utilization

This tells you where the bottlenecks actually are — or whether you have any at all.

### Phase 1 — Identify

Classify what's happening:
- Overprovisioned and safe to reduce?
- CPU-bound at a specific operator?
- Memory pressure?
- Checkpoint thrash?
- Downstream sink backpressure?

Different problems need different fixes. This step prevents you from optimizing the wrong thing.

### Phase 2 — Experiment

Pick one non-critical job. Sweep 2-3 parameters:
- Parallelism
- TaskManager memory
- Task slots

Find the floor — the point where backpressure appears. Then back off to a safe margin.

This is where you find the *actual* minimum cost, not the guess.

### Phase 3 — Phase In

Roll out changes incrementally:
- One job at a time
- Automatic rollback if backpressure exceeds threshold
- 3-5 day observation windows between changes

No 2am surprises.

### Phase 4 — Sustain

Workloads change. The framework keeps running:
```
observe() → identify() → recommend()
```

The goal isn't to solve this once — it's to have a system that keeps the cost/performance ratio healthy over time.

---

## Current Status

- [x] Repo scaffolded, README drafted
- [x] Phase 0 observation scripts (`flink_client.py`, `0_observe.py`)
- [ ] Phase 1 identification logic
- [ ] Phase 2 experiment framework
- [ ] Local Flink cluster for testing
- [ ] Nexmark benchmark baseline
- [ ] Head-to-head: DoE framework vs. Flink autoscaler
- [ ] Results published (charts, data, warts and all)

This is a work in progress. Following commits will show the build-out.

---

## Repo Structure
```
flink-rightsizing-framework/
├── README.md
├── requirements.txt
├── background/               # Research existing frameworks
├── docs/
│   ├── THEORY.md             # Why this works (the thinking)
│   ├── DASHBOARDS.md         # What to build (specs for analytics team)  
│   ├── IMPLEMENTATION.md     # How to execute (timeline, checklist)
│   └── WINDOWS_SETUP.md     # Local dev setup
├── scripts/
│   ├── flink_client.py       # How to talk to Flink (Flink REST API wrapper)
│   ├── 0_observe.py          # Collect metrics → data/staging/
│   ├── 1_identify.py         # Read from staging, correlate, classify → data/marts/
│   ├── 2_experiment.py       # DoE parameter sweeps
│   ├── 3_phase_in.py         # Incremental rollout logic
│   └── 4_sustain.py          # Continuous monitoring
├── setup/
│   ├── local_cluster.sh      # Flink-on-K8s: start, forward, status, stop
│   ├── run_observe.sh        # Wrapper for Phase 0 (one-shot or continuous)
│   └── validate_env.sh       # Check prerequisites before you start
├── data/
│   ├── staging/              # Raw snapshots
│   ├── intermediate/         # Transformations
│   └── marts/                # Dashboard-ready aggregates
└── examples/
    └── sample_snapshot.json
```

---

## Getting Started
```bash
# 1. Check your environment
./setup/validate_env.sh

# 2. Start local Flink cluster
./setup/local_cluster.sh

# 3. In another terminal, port-forward
./setup/local_cluster.sh forward

# 4. Test connection
python scripts/flink_client.py

# 5. Collect your first snapshot
./setup/run_observe.sh

# 6. See what you got
cat data/staging/snapshot_latest.json | head -50
```

---

## Why This Framing

I spent 13 years in semiconductor manufacturing building systems that balance tool utilization against availability. The core question — "am I paying for capacity I'm not using, and how do I know what's safe to cut?" — is the same whether you're managing lithography tools or Flink TaskManagers.

This framework applies that methodology to streaming infrastructure:
1. You can't decide what to fix until you measure utilization vs. availability
2. You don't reduce resources on a station that's already backed up
3. You don't experiment on your highest-volume workload first
4. You run controlled pilots before rolling out changes
5. You measure multiple signals (utilization AND backpressure AND checkpoint health), not just one

---

## Contributing

Contributions welcome — issues, PRs, or "this assumption is wrong" feedback.

If you're running Flink on K8s and want to test the framework against real workloads, I'd be interested in comparing results. The long-term goal is a publishable comparison of DoE-based tuning against Flink's built-in autoscaler.

---

## About

**Nicole Rowsey** — Staff Data Platform Engineer, PhD EE  
13 years building distributed systems and data platforms at Intel. Now applying factory optimization methodology to cloud infrastructure.

- **GitHub:** [@sooperD00](https://github.com/sooperD00)
- **LinkedIn:** [in/nicole-rowsey](https://linkedin.com/in/nicole-rowsey)
- **Email:** nicole.rowsey@gmail.com
