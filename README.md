# Flink, Autoscaling, Backpressure, and Cost Overruns

## The Problem

You have Flink on Kubernetes on GCP. It ran fine. But then you added some workloads. Suddenly your Flink bill is 50-60% of overhead 📈💸🔥 You see overprovisioned resources... what now?

---

**Option 1:**

Just scale up, pay more, and hope it helps.

**Option 2:**

Hire a "Senior Data Engineer" with "8+ years deep Flink experience." You find your unicorn for $230k+ salary plus equity. He sits at the Flink UI and turns knobs. He scales up, you pay more. He scales down, maybe breaks things. It's okay, you hired the expert, you can blame him. But is it really okay?

**Option 3:**

Turn on autoscaling. What does this give you? Do you know what it's doing? Do you trust Flink to scale best for *your* financial needs?

**Option 4: This Approach**

Create a straightforward systematic data platform using the Python skills your junior engineers already have. Implement systematic observation, controlled experiments, find the actual (dynamic) scale floor, and automatically phase in changes. This is the same methodology that was outlined in 1929 by Ronald Fisher for crop yield calculations and that is still today to optimize billion-dollar semiconductor fabs, Netflix A/B testing infrastructure, and NASA mission planning.

---

## This Approach

**Phase 0: Observe**
- Don't touch anything, just listen -> you get: the bottlenecks
- `bp = requests.get(f"{FLINK}/jobs/{job_id}/vertices/{vertex_id}/backpressure").json()`

**Phase 1: Identify**
- Differentiate: overprovisioned? at capacity? memory leak? -> you get: correct action at the time you need it
- `if backpressure == "ok" and cpu_util < 0.3:
    action = "REDUCE"`

**Phase 2: Experiment**
- Sweep your params on non-critical workloads
- Find the actual floor -- where backpressure appears
- `configs = [
    {"parallelism": 8, "memory_mb": 4096},  # baseline
    {"parallelism": 4, "memory_mb": 2048},  # target
]`
- Back off to a safe margin
>This is the Design of Experiments that gets drugs through FDA approval

**Phase 3: Phase In**
- Roll out changes incrementally
- Use automatic rollback triggers
- `if backpressure_ratio > threshold:
    rollback_to_previous()`
>No 2am pages, no "who changed what"

**Phase 4: Sustain**
- The system changes. New workloads get added. The framework is the same.
- keeps running, keeps observing, keeps recommending
- `while True:
    observe() → identify() → recommend()`
>Don't hire an expert to sit at the dashboard — build the dashboard that tells you what the expert would say

---

## Don't Believe Me?

***Hold my beer.***

- [x] Repo scaffolded, README kicked off (you are here)
- [ ] Observation + experiment scripts populated
- [ ] GKE cluster + Flink operator deployed
- [ ] Nexmark benchmark baseline collected
- [ ] Flink autoscaler test run (their approach)
- [ ] DoE framework test run (this approach)
- [ ] Head-to-head comparison analysis
- [ ] Results published (charts, data, warts and all)
- [ ] Paper submitted

**Follow along:** [link to commits / blog / whatever you want]

---

## About

**Nicole Rowsey**

sooperD00 · she/her

Staff Data Platform Engineer | Distributed Systems | Real-Time Analytics | PhD EE | --verbose

---

## Call me

**Email:** [nicole.rowsey@gmail.com](mailto:nicole@gmail.com)  
**LinkedIn:** [in/nicole-rowsey](https://www.linkedin.com/in/nicole-rowsey)
**GitHub:** [@sooperD00](https://github.com/sooperD00)