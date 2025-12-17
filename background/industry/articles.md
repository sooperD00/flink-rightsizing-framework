# Industry Articles — Flink Optimization & Operations

Engineering blogs, vendor docs, and practitioner content. More implementation-focused than academic papers.

---

## Engineering Blogs (Big Tech)

### Machine-learning Predictive Autoscaling for Flink (Grab)
- **Source:** Grab Engineering Blog
- **URL:** https://engineering.grab.com/ml-predictive-autoscaling-for-flink
- **Key idea:** ML models trained on traffic patterns to predict CPU needs. Deployed on 50%+ of their Kafka-sourced Flink apps. Claims >35% infrastructure cost savings.
- **Approach:** Predicts load ahead of time vs. reactive scaling. Addresses the "scaling spike" problem (CPU/latency spikes after restart from checkpoint).
- **Relevance:** HIGH. Different approach than ours (ML prediction vs. DoE parameter optimization), but solving same cost problem. Good comparison point. They mention memory tuning as "next frontier" — we could address that.

### 3 (More) Tips for Optimizing Apache Flink Applications (Shopify)
- **Source:** Shopify Engineering
- **URL:** https://shopify.engineering/optimizing-apache-flink-tips-part-two
- **Key idea:** Parallelism config, sink bottleneck detection, key distribution/bucketing for skewed data.
- **Relevance:** Practical tips, but ad-hoc rather than systematic. Shows the "tribal knowledge" problem we're addressing.

### Apache Flink on Kubernetes (Airbnb)
- **Source:** Airbnb Engineering (Medium), July 2024
- **URL:** https://medium.com/airbnb-engineering/apache-flink-on-kubernetes-84425d66ee11
- **Key idea:** Migration from YARN to K8s. Mentions cost savings from shared K8s cluster and "streamlining infrastructure complexity." Future plans include autoscaling via Reactive Mode + Flink K8s Operator.
- **Relevance:** Shows big companies are still figuring this out. They haven't solved autoscaling yet — it's "future work."

### Ten Flink Gotchas We Wish We Had Known (Contentsquare)
- **Source:** Contentsquare Engineering Blog
- **URL:** https://engineering.contentsquare.com/2021/ten-flink-gotchas/
- **Key idea:** maxParallelism defaults cause uneven key group distribution. Skewed data causes multi-hour processing hangs. Added preventive logging for slow windows.
- **Relevance:** Real war stories. The maxParallelism gotcha is exactly the kind of thing systematic observation would catch.

---

## Consulting / Vendor Content

### Best Practices for Running Flink on Kubernetes (BigData Boutique)
- **Source:** BigData Boutique Blog
- **URL:** https://bigdataboutique.com/blog/best-practices-for-running-flink-on-kubernetes-b10336
- **Key idea:** Meetup talk summary. Architecture overview, fault tolerance, real customer case study (blockchain analytics).
- **Relevance:** Shows there's a consulting market here. They're selling Flink expertise.

### Mastering Apache Flink in Production: Monitoring & Optimization (BigData Boutique)
- **Source:** BigData Boutique Blog
- **URL:** https://bigdataboutique.com/blog/mastering-apache-flink-in-production-a-guide-to-monitoring-and-optimization-0b50d7
- **Key idea:** JVM CPU <80-85%, monitor GC time, use JSON logging in production. Focus metrics: backpressure, checkpoint duration, consumer lag.
- **Relevance:** Good checklist of what to monitor. Aligns with our Phase 0 observation metrics.

### Flink vs. Kafka and Their Role in Streaming (Redpanda)
- **Source:** Redpanda Guides, Feb 2025
- **URL:** https://www.redpanda.com/guides/event-stream-processing-flink-vs-kafka
- **Key idea:** Kafka = ingestion layer, Flink = processing layer. Kafka Streams vs Flink comparison.
- **Relevance:** Background/context, not directly relevant to tuning.

---

## Cloud Vendor Documentation

### Optimizing Apache Flink on Amazon EKS using EC2 Spot Instances (AWS)
- **Source:** AWS Compute Blog, Nov 2021
- **URL:** https://aws.amazon.com/blogs/compute/optimizing-apache-flink-on-amazon-eks-using-amazon-ec2-spot-instances/
- **GitHub:** https://github.com/aws-samples/cost-optimized-flink-on-kubernetes
- **Key idea:** Use Spot instances for TaskManagers, EKS managed node groups. Cost optimization via cheaper compute, not tuning.
- **Relevance:** Orthogonal approach (cheaper instances vs. right-sized instances). Could combine with our framework.

### Using Autoscaler for Flink Applications (Amazon EMR)
- **Source:** AWS EMR Docs
- **URL:** https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/jobruns-flink-autoscaler.html
- **Key idea:** EMR's wrapper around Flink K8s Operator autoscaler. Recommends max-parallelism as multiples of 60 (120, 180, 240, 360, 720).
- **Relevance:** The "multiples of 60" tip is useful. Shows AWS is just wrapping the standard autoscaler.

### Configuring Flink Application Resources (Cloudera)
- **Source:** Cloudera Docs
- **URL:** https://docs.cloudera.com/csa/1.4.0/development/topics/csa-other-configs.html
- **Key idea:** "Use higher parallelism and fewer resources per TaskManager instead of fewer TaskManagers with large resources."
- **Relevance:** Rule of thumb, but no systematic justification. This is the kind of advice we can validate or refute.

---

## Tutorial / Overview Content

### Get Running with Apache Flink on Kubernetes (Decodable)
- **Source:** Decodable Blog
- **URL:** https://www.decodable.co/blog/get-running-with-apache-flink-on-kubernetes-1
- **Key idea:** Two-part tutorial on Flink K8s Operator setup. Covers installation, custom resources, HA, savepoints.
- **Relevance:** Good reference for our local cluster setup, but not optimization-focused.

### Running Apache Flink on Kubernetes (Empathy.co)
- **Source:** Medium, April 2021
- **URL:** https://medium.com/empathyco/running-apache-flink-on-kubernetes-10815a26559e
- **Key idea:** Migration case study. Mentions Reactive Mode (Flink 1.13) as "nice to have for cost savings" — future work.
- **Relevance:** Another "we haven't solved autoscaling yet" data point.

---

## Key Themes

1. **Everyone mentions cost optimization as a goal, few have systematic approaches**
2. **Autoscaling is either "turn it on" (reactive) or "we'll do it later" (manual provisioning)**
3. **Grab is the most sophisticated with ML prediction, but that's a different paradigm than DoE**
4. **No one is doing controlled experiments with rollback triggers**
5. **Consulting market exists (BigData Boutique) — validates the opportunity**

---

*Last updated: 2024-12-17*
