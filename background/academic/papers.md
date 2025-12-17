# Academic Papers — Flink Autoscaling & Stream Processing

Background research for Flink Rightsizing Framework. Papers focused on autoscaling, benchmarking, and optimization.

---

## Autoscaling Approaches

### TALOS: Task Level Autoscaler for Apache Flink
- **Source:** ResearchGate, Feb 2025
- **URL:** https://www.researchgate.net/publication/388989812_TALOS_Task_Level_Autoscaler_for_Apache_Flink
- **Key idea:** Scales individual tasks (not whole job) based on per-task data processing needs. Claims better performance-to-cost ratio than Flink K8s Operator's built-in autoscaler.
- **Relevance:** Direct competitor/comparison point. They're solving the same problem but with different methodology (reactive per-task scaling vs. our DoE-based systematic tuning).

### HYAS: Hybrid Job Autoscaling Controller
- **Cited in:** TALOS paper
- **Key idea:** Rule and threshold policies based on operator idleness, backpressure, and input record lag. Inspired by Varga et al.
- **Relevance:** Another autoscaler approach. Rules-based rather than ML or DoE.

### DS2: Automatic Task Scaling Agent
- **Cited in:** TALOS paper
- **Key idea:** Automatic scaling agent for Flink.
- **TODO:** Find primary source, get details.

### Smilax
- **Cited in:** TALOS paper  
- **Key idea:** Statistical machine learning autoscaler for Flink applications.
- **TODO:** Find primary source — this is the ML approach like Grab's.

---

## Benchmarking Studies

### Benchmarking Distributed Stream Data Processing Systems
- **Source:** arXiv (PDF)
- **URL:** https://arxiv.org/pdf/1802.08496
- **Key idea:** Compares Storm, Spark, Flink on windowed aggregations and joins. Flink bounded by network bandwidth at scale. Measures sustainable throughput and latency distributions.
- **Relevance:** Benchmarking methodology reference. They use on-the-fly data generation rather than message brokers for controlled experiments.

### Benchmarking Scalability of Stream Processing Frameworks (ScienceDirect)
- **Source:** ScienceDirect, Oct 2023
- **URL:** https://www.sciencedirect.com/science/article/pii/S0164121223002741
- **Key idea:** 740+ hours of experiments on K8s (GKE + private cloud), up to 110 microservice instances, 1M msg/sec. Compares Flink, Kafka Streams, Samza, Hazelcast Jet, Apache Beam.
- **Relevance:** Shows "choice of framework and deployment has considerable impact on cost." Provides replication package. Good methodology reference for our benchmarking phase.

### SProBench: Stream Processing Benchmark for HPC
- **Source:** arXiv, April 2025
- **URL:** https://arxiv.org/html/2504.02364v1
- **Key idea:** Benchmark suite for Flink/Spark/Kafka Streams on HPC clusters. Defines pass-through, CPU-intensive, memory-intensive pipelines. Claims 10x throughput over existing benchmarks.
- **Relevance:** Pipeline definitions could inform our test workloads.

---

## Architecture & Design

### FLIP-271: Autoscaling (Apache Flink Design Doc)
- **Source:** Apache Flink Confluence
- **URL:** https://cwiki.apache.org/confluence/display/FLINK/FLIP-271:+Autoscaling
- **Key idea:** Official design doc for Flink K8s Operator autoscaling. Explains why job-level parallelism scaling isn't sufficient (heterogeneous operator loads). Describes vertex-level scaling algorithm based on predicting downstream capacity changes.
- **Relevance:** This is the "official" approach we're benchmarking against. Understanding their algorithm is critical.

### Real-time Event Joining in Practice (Kafka + Flink)
- **Source:** arXiv, Oct 2024
- **URL:** https://arxiv.org/html/2410.15533v1
- **Key idea:** Migration case study from batch to streaming. Discusses autoscaling as "future work" — they currently provision for max traffic.
- **Relevance:** Shows real teams are still manually provisioning, validates the problem space.

---

## To Find / Read Later

- [ ] Varga et al. — cited as inspiration for HYAS
- [ ] DS2 primary paper
- [ ] Smilax primary paper
- [ ] ESPBench — enterprise stream processing benchmark
- [ ] OSPBench — mentioned in SProBench as comparison

---

*Last updated: 2024-12-17*
