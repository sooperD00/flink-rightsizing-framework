# 12/17/2025

site:reddit.com/r/apacheflink optimization
site:reddit.com/r/apacheflink cost
site:reddit.com/r/apacheflink autoscaler
site:reddit.com/r/apacheflink tuning parallelism
site:reddit.com/r/dataengineering flink cost
site:reddit.com/r/dataengineering flink tuning
site:reddit.com/r/dataengineering flink kubernetes

---

# site:reddit.com/r/apacheflink optimization

## Follow Up

### #1 "Optimizing PyFlink For Processing Time-Series Data" 

9 upvotes, asking about latency optimization without "blowing up resources." That's your exact problem space.

```
Optimizing PyFlink For Processing Time-Series Data : r/apacheflink

Reddit
https://www.reddit.com › apacheflink › comments › opt...
Is there any options for optimization in the steps in my pipeline to mitigate latency, without having to blow up resources. Thanks. Upvote 9. Downvote 0 Go ...
```

```
r/apacheflink
•
9mo ago
raikirichidori255

Optimizing PyFlink For Processing Time-Series Data
Hi all. I have a Kafka stream that produces around 5 million records per minute and has 50 partitions, Each Kafka record, once deserialized is a json record, where the values for keys 'a','b', and 'c' rpepresent the unique machine for the time series data, and value of key 'data_value' represent the float value of the record. All the records in this stream are coming in order. I am using PyFlink to compute specific 30-second aggregations on certain machines within my.

I also have another config kafka stream, where each element in the stream represents the latest machines to monitor. I join this stream with my time-series kafka stream using a broadcast process operator, and filter down records from my raw time-series kafka stream to only ones from relevant machines in the config kafka stream.

Once I filter down my records, I then key my filtered stream by machine (keys 'a','b', and 'c' for each record), and call my Keyed Process Operator. In my Process function, I trigger a timer event in 30 seconds once the first record is received and then append all the subsequent time-series values in my process value state (I set it up as list). Once the timer is triggered, I compute multiple aggregation functions on the time-series values in my value state.

I'm facing a lot of latency issues with the way I have currently structured my PyFlink job. I currently have 85 threads, with 5 threads per task manager, and each task manager using 2 CPU and 4 GB RAM. This works fine when in my config kafka stream has very few machines, and I filter my raw Kafka stream from 5 million per minute to 70k records per minute. However, when more machines get added to my config Kafka stream, and I start filtering less records, the latency really starts to pile up, to the point where the event_time and processing_time of my records are almost hours apart after running for a few hours even close. My theory is it's due to keying my filtered stream since I've heard that can be expensive.

I'm wondering if there is any chances for optimizing my PyFlink pipeline, since I've heard Flink should be able to handle way more than 5 million records per minute. In an ideal world, even if no records are filtered from my raw time-series kafka stream, I want my PyFlink pipeline to still be able to process all these records without huge amounts of latency piling up, and without having to explode the resources.

In short, the steps in my Flink pipeline after receiving the raw Kafka stream are:

Deserialize record

Join and filter on Config Kafka Stream using Broadcast Process Operator

Key by fields 'a','b', and 'c' and call Process Function to execute aggregation in 30 seconds

Is there any options for optimization in the steps in my pipeline to mitigate latency, without having to blow up resources. Thanks.


Upvote
9

Downvote

0
Go to comments


Share
```

This is gold. Note what's happening:

**The pain:**
- 5M records/min, latency "piling up to hours"
- They're guessing it's the keying operation
- Config: 85 threads, 5 slots/TM, 2 CPU, 4GB RAM — but no mention of monitoring backpressure or where the bottleneck actually is

**The gap your framework fills:**
They jumped straight to "how do I optimize" without "where is the bottleneck." No mention of checking backpressure per operator, no utilization metrics, no checkpoint health. Classic "tune and hope."

Are there comments? If someone gave a good answer, that's useful context. If not, that's evidence the community doesn't have a systematic approach either.
Either way — save this URL for your research notes. This is a perfect "before" example of why measure-first matters.

no comments
nobody answered for 9 months

***SAVE THIS LINK***
https://www.reddit.com/r/apacheflink/comments/1j8mlqt/optimizing_pyflink_for_processing_timeseries_data/

### #3 "Data Streaming Academy"

it mentions "Guide to Flink tuning and optimizations" which might link to useful content.

```
Announcing Data Streaming Academy with Advanced ...

Reddit · r/apacheflink
1 comment · 1 month ago
Guide to Flink tuning and optimizations. A framework for building reliable, observable, upgrade-safe streaming systems. If you're even remotely ...Read more
```

```
r/apacheflink
•
2mo ago
sap1enz

Announcing Data Streaming Academy with Advanced Apache Flink Bootcamp
r/apacheflink - Announcing Data Streaming Academy with Advanced Apache Flink Bootcamp
streamacademy.io

Open
Announcing an upcoming Advanced Apache Flink Bootcamp.

This bootcamp goes beyond the basics: learn the best practices in Flink pipeline design, go deep into the DataStream and Table APIs, know what it means to run Flink in production at scale. The author ran Flink in production in several organizations and managed hundreds of Flink pipelines (with terabytes of state).

You’ll Walk Away With:
Confidence using state and timers to build low-level operators

Ability to reason about and debug Flink SQL query plans

Practical understanding of connector internals

Guide to Flink tuning and optimizations

A framework for building reliable, observable, upgrade-safe streaming systems

If you’re even remotely interested in learning Flink or other data streaming technologies, join the waitlist - it’s the only way to get early access (and discounted pricing).


Upvote
4

Downvote

1
Go to comments


Share
u/PerfectScale-io avatar
PerfectScale-io
•
Promoted

[eBook]Struggling to keep up with traffic spikes? Learn how Kubernetes autoscaling handles it—so you can relax while your pods hustle. Download your copy now!
info.perfectscale.io
Thumbnail image: [eBook]Struggling to keep up with traffic spikes? Learn how Kubernetes autoscaling handles it—so you can relax while your pods hustle. Download your copy now!
Join the conversation
Sort by:

Best

Search Comments
Expand comment search
Comments Section
sap1enz
OP
•
1mo ago
The Advanced Apache Flink Bootcamp is now open for registration! The first cohort is scheduled for January 21st - 22nd, 2026.

This intensive 2-day bootcamp takes you deep into Apache Flink internals and production best practices. You'll learn how Flink really works by studying the source code, master both DataStream and Table APIs, and gain hands-on experience building custom operators and production-ready pipelines.

This is an advanced bootcamp. Most courses just repeat what’s already in the documentation. This bootcamp is different: you won’t just learn what a sliding window is — you’ll learn the core building blocks that let you design any windowing strategy from the ground up.

Learning objectives:

- Understand Flink internals by studying source code and execution flow
- Master DataStream API with state, timers, and custom low-level operators
- Know how SQL and Table API pipelines are planned and executed
- Design efficient end-to-end data flows
- Deploy, monitor, and tune Flink applications in production
```

That's a course ad, not a discussion. But useful signal: someone's charging for "tune Flink applications in production" as a premium skill. Market validation.

## Skip

Skip the rest — conference announcements, course ads, and unrelated tools.


## Raw Results

site:reddit.com/r/apacheflink optimization

```
Optimizing PyFlink For Processing Time-Series Data : r/apacheflink

Reddit
https://www.reddit.com › apacheflink › comments › opt...
Is there any options for optimization in the steps in my pipeline to mitigate latency, without having to blow up resources. Thanks. Upvote 9. Downvote 0 Go ...
Is Restate the new superhero that takes down Apache Flink ...

Reddit · r/apacheflink
4 comments · 4 months ago
Restate is built for workflows, agents, stateful service orchestration, transactional event-driven applications. It exposes a programming model ...Read more
4 answers
 
·
 
Top answer: 
I can chime in - I am one of the original creators of Flink and StateFun and one of the ...
Announcing Data Streaming Academy with Advanced ...

Reddit · r/apacheflink
1 comment · 1 month ago
Guide to Flink tuning and optimizations. A framework for building reliable, observable, upgrade-safe streaming systems. If you're even remotely ...Read more
Flink missing Windowing TVFs in Table API : r/apacheflink

Reddit · r/apacheflink
4 months ago
This is important because Flink optimises Windowing TVFs with Mini-Batch and Local Aggregation optimizations. However, the regular Group ...Read more
Announcing Flink Forward Barcelona 2025! : r/apacheflink

Reddit · r/apacheflink
2 comments · 9 months ago
This 2-day program is specifically designed for Apache Flink users with 1-2 years of experience, focusing on advanced concepts like state ...Read more
Current 2025 New Orleans CfP is open until 15th June

Reddit · r/apacheflink
6 months ago
Performance Optimization: Low-latency processing, exactly-once semantics. Future of Streaming: Emerging trends and technologies, federated ...Read more
Replacement of sortGroup dataset operation : r/apacheflink

Reddit · r/apacheflink
1 comment · 1 year ago
I'm interested in migrating off of Beam, as there are several optimizations that are possible in Flink but not using Beam. What I'm ...Read more
Ververica Academy Live! Master Apache Flink® in Just 2 ...

Reddit · r/apacheflink
9 months ago
The curriculum covers practical skills many of us work with daily - advanced windowing, state management optimization, exactly-once processing, ...Read more
Proton OSS v3 - Fast vectorized C++ Streaming SQL engine

Reddit
https://www.reddit.com › apacheflink › comments › pr...
A professional community to discuss OpenShift and OKD, Red Hat's auto-scaling Platform as a Services (PaaS) for applications. 10K Members ...
```

---

---

# site:reddit.com/r/dataengineering flink tuning

## Follow Up

***Click #5 "Evaluating real-time analytics solutions"***

30+ comments, only 4 weeks old, and the preview already mentions "Flink + Starrocks, pretty cheap but a bit more maintenance." That's cost/complexity discussion.

```
Evaluating real-time analytics solutions for streaming data

Reddit · r/dataengineering
30+ comments · 4 weeks ago
we use Flink + Starrocks. pretty cheap but a bit more maintenance and work for changes.Read more
```

```
r/dataengineering icon
Go to dataengineering
r/dataengineering
•
1mo ago
EmbarrassedBalance73

Evaluating real-time analytics solutions for streaming data
Discussion
Scale:

50-100GB/day ingestion (Kafka)

~2-3TB total stored

5-10K events/sec peak

Need: <30 sec data freshness

Use case: Internal dashboards + operational monitoring

Considering:

Apache Pinot (powerful but seems complex for our scale?)

ClickHouse (simpler, but how's real-time performance?)

Apache Druid (similar to Pinot?)

Materialize (streaming focus, but pricey?)

Team context: ~100 person company, small data team (3 engineers). Operational simplicity matters more than peak performance.

Questions:

Is Pinot overkill at this scale? Or is complexity overstated?

Anyone using ClickHouse for real-time streams at similar scale?

Other options we're missing?


Upvote
59

Downvote

39
Go to comments


Share
u/comcastbusiness avatar
comcastbusiness
•
Promoted

Give your business the boost it deserves. Introducing the 5 Year Business Boost from Comcast Business.
Learn More
business.comcast.com
Thumbnail image: Give your business the boost it deserves. Introducing the 5 Year Business Boost from Comcast Business.
Join the conversation
Sort by:

Best

Search Comments
Expand comment search
Comments Section
u/Dry-Aioli-6138 avatar
Dry-Aioli-6138
•
1mo ago
Profile Badge for the Achievement Top 1% Commenter Top 1% Commenter
Flink and then two streams: one for realtime dashboards, the other to blob storage/lakehouse?



Upvote
28

Downvote

Reply

Award

Share

u/joaomnetopt avatar
joaomnetopt
•
1mo ago
This is the way.

OP do you really need 3 TB of data with 30 sec freshness? What percentage of that data changes after x time?

One stream to a Postgres DB with finite retention for realtime dashboards and another stream for lakehouse (hive, iceberg, whatever).



Upvote
8

Downvote

Reply

Award

Share

u/EmbarrassedBalance73 avatar
EmbarrassedBalance73
OP
•
1mo ago
0.5 % of data changes everyday


Upvote
3

Downvote

Reply

Award

Share

Commercial_Dig2401
•
1mo ago
This but you can leverage TimescaleDB/TigerData if you have big datasets because of how you can manage older data points. You usually query using where clause for recent data points and want sum for older data. Hypertable can do both under the hood. It’s been a long time since I use this but it made a lit of sense. You rarely going to search for a specific value for data older than x min/hours/days depending on your usecase. You’ll probably want stats for older data rather than specific records.


Upvote
2

Downvote

Reply

Award

Share

eMperror_
•
1mo ago
Does flink replace something like debezium?



Upvote
1

Downvote

Reply

Award

Share

u/Dry-Aioli-6138 avatar
Dry-Aioli-6138
•
1mo ago
Profile Badge for the Achievement Top 1% Commenter Top 1% Commenter
No. Rather it transforms streaming data "on the fly"

https://flink.apache.org/what-is-flink/flink-architecture/


Upvote
2

Downvote

Reply

Award

Share

u/Exorde_Mathias avatar
Exorde_Mathias
•
28d ago
Am I the only one who finds flink hardly maintenable? Bytewax and new frameworks are like a dream compares to it. Perhaps less efficient


Upvote
1

Downvote

Reply

Award

Share

u/harshachv avatar
harshachv
•
1mo ago
Option: RisingWave True streaming SQL from Kafka, 5-10s latency guaranteed, Postgres-compatible. Live in <2 weeks, zero headaches.

Option : ClickHouse + Kafka engine Direct pull from Kafka + materialized views, 15-60s latency . minimal tuning.


Upvote
7

Downvote

Reply

Award

Share

Grandpabart
•
1mo ago
For point 3, add Firebolt to your considerations. You can just start using it without having to deal with a sales team.


Upvote
16

Downvote

Reply

Award

Share

u/Vantage avatar
u/Vantage
•
Promoted

Stop w/ ClickOps: Cloud cost management via MCP and a Terraform Provider
Learn More
vantage.sh
Thumbnail image: Stop w/ ClickOps: Cloud cost management via MCP and a Terraform Provider
u/sdairs_ch avatar
sdairs_ch
•
1mo ago
(I work for ClickHouse)

This scale is very easy for ClickHouse, as is 30s freshness.

Pinot will also handle this very easily. (My biased take fwiw: both will handle this load equally well, in that regard neither are the wrong choice. If you're intending to self-host OSS, Pinot is just a bit more complex to manage.)

I used to work for a vendor that sold Druid back in 2020, and at that time we were already deprecating it as a product and advising that it was no longer worth adopting.

I don't think Materialize is the right fit for your use case.



Upvote
14

Downvote

Reply

Award

Share

u/EmbarrassedBalance73 avatar
EmbarrassedBalance73
OP
•
1mo ago
what is the fastest freshness. can it go less than 5 - 10 seconds. I don’t have this requirement but it’s good to know the scaling limits.



Upvote
3

Downvote

Reply

Award

Share

u/sdairs_ch avatar
sdairs_ch
•
1mo ago
Yeah, there're many people doing single-digit second freshness with ClickHouse


Upvote
2

Downvote

Reply

Award

Share

Icy_Clench
•
1mo ago
I am always genuinely curious as to what people do with real-time analytics. Like, does it really matter if the data comes in after 30 seconds as opposed to 1 minute? What kind of business decisions do they make staring at the screen with rapt fascination like that?



Upvote
5

Downvote

Reply

Award

Share

Thin_Smile7941
•
1mo ago
Real-time only matters if someone acts within minutes; otherwise batch it. For OP’s ops monitoring, 30 seconds catches runaway ad spend, fraud spikes, checkout errors, and SLA breaches so on-call can roll back or hit a kill switch before costs pile up. We run ClickHouse with Grafana for anomaly dashboards, Datadog for alerts; DreamFactory exposes curated DB views as simple REST for internal tools. If nobody will act inside a few minutes, skip sub-30-second pipelines.



Upvote
5

Downvote

Reply

Award

Share

u/Recent-Blackberry317 avatar
Recent-Blackberry317
•
28d ago
Yeah but this stuff should be mostly automated (kill switch, rollback, etc.) otherwise you’re paying a bunch of people to stare at a screen and wait for a spike? And then the time it takes for them to properly react. I get the need for real time data but I feel like it’s rare to have a valid use case for sub 1 minute dashboard latency.. I guess it’s a nice to have for monitoring though


Upvote
3

Downvote

Reply

Award

Share


[deleted]
•
28d ago
Arm1end
•
27d ago
•
Edited 7d ago
We serve a lot of users with similar use cases. They usually set up Kafka->GlassFlow (for transformations)-> ClickHouse (cloud).

Kafka = Ingest + buffer. Takes the firehose of events and keeps producers/consumers decoupled.

GlassFlow = Real-time transforms. Clean, filter, enrich, and prep the stream so ClickHouse only gets analytics-ready data. Easier to use than Flink.

ClickHouse (cloud) = Fast and gives sub-second queries for dashboards/analytics.

Discloser: I am one of the GlassFlow founders.



Upvote
3

Downvote

Reply

Award

Share

ArgenEgo
•
10d ago
It would be cool for you to disclose that you are GlassFlow founder?



Upvote
1

Downvote

Reply

Award

Share

Arm1end
•
7d ago
I didn't want to confuse anyone. I thought it was clear by using “we serve”. I added a discloser to the post.


Upvote
1

Downvote

Reply

Award

Share

u/tonkotsu-ai avatar
u/tonkotsu-ai
•
Promoted

Disciplined multi-agent engineering built for professionals. Free download.
Learn More
tonkotsu.ai
Thumbnail image: Disciplined multi-agent engineering built for professionals. Free download.
volodymyr_runbook
•
1mo ago
For this scale I'd do kafka → clickhouse for dashboards + another sink to lakehouse.


Upvote
2

Downvote

Reply

Award

Share

u/Certain_Leader9946 avatar
Certain_Leader9946
•
1mo ago
•
Edited 1mo ago
Use postgres notifications unless you expect this scale to continue indefinitely. Not sure how you got from 100GB / day to 3TB total stored. Something wrong there, you're not storing 100GB a day so where are you getting that metric from, this could be massively overengineered. But modern postgres will chew through this scale.

EDIT* If you have a metric you keep updating you could just keep a Postgres table you keep firing UPDATE statements to of cumulative sum and then archive the historical data if you still care about it after the fact.


Upvote
3

Downvote

Reply

Award

Share

u/fishylord01 avatar
fishylord01
•
1mo ago
we use Flink + Starrocks. pretty cheap but a bit more maintenance and work for changes.


Upvote
1

Downvote

Reply

Award

Share

u/Big_Specialist1474 avatar
Big_Specialist1474
•
1mo ago
Maybe -> Flink or Dinky + Apache Doris ?


Upvote
0

Downvote

Reply

Award

Share

segmentationsalt
•
1mo ago
So why exactly do you need real time? Do you work in healthcare or HFT?


Upvote
0

Downvote

Reply

Award

Share

MyRottingBunghole
•
1mo ago
Starrocks


Upvote
0

Downvote

Reply

Award

Share

u/geoheil avatar
geoheil
•
28d ago
mod
Starrocks?

Or https://fluss.apache.org/



Upvote
0

Downvote

Reply

Award

Share

u/geoheil avatar
geoheil
•
28d ago
mod
https://risingwave.com/


Upvote
2

Downvote

Reply

Award

Share


[deleted]
•
1mo ago
ephemeral404
•
1mo ago
Out of these options for the given use case, I'd have chosen Pinot or Clickhouse. Reliable and suitable for this scale. And to keep it simple, I'd have then further chosen Clickhouse. Having said that, consider Postgres as a viable choice. RudderStack uses it to successfully process 100k events/sec, using these techniques/configs.


Upvote
1

Downvote

Reply

Award

Share

u/Due_Carrot_3544 avatar
Due_Carrot_3544
•
1mo ago
What is the partition key and what are the number of unique writers per second? The cardinality of that key is everything (your entropy budget).


Upvote
1

Downvote

Reply

Award

Share

u/RoleAffectionate4371 avatar
RoleAffectionate4371
•
28d ago
Having done this as a small team, I recommend keeping it stupid simple to start.

Just do Kafka straight into Clickhouse cloud.

Don’t do Flink + some self-hosted db. There is so much tuning and maintenance work downstream of this. And a lot of pain. It’s better to wait until you absolutely need to do that for cost or performance reasons.


Upvote
1

Downvote

Reply

Award

Share

u/Exorde_Mathias avatar
Exorde_Mathias
•
28d ago
I do use clickhouse for RT ingestion (2k rows/s). Latest version. Works really well. We had druid before and it was, for a small team, terrible choice (complex af). Clickhouse can just do it all in one beefy node. Do you need real time analytics like on data thats sub 1 min ingested?


Upvote
1

Downvote

Reply

Award

Share

u/raghvyd avatar
raghvyd
•
27d ago
Pinot would be a good choice for the use case. It is also real time in true sense as opposed to click house's micro batch ingestion. Operational Complexity for pinot is over stated.

FYI: I am a Apache Pinot Contributor.


Upvote
1

Downvote

Reply

Award

Share

u/KineticaDB avatar
KineticaDB
•
22d ago
(Shameless self plug) These kinds of real-time pipelines are what Kinetica handles, especially when GPU acceleration helps with the hard parts (ingest, joins, streaming queries). We’re not open source, so no worries if that’s a stopper, but happy to share what’s worked for us.


Upvote
1

Downvote

Reply

Award

Share


```

This is great. Key quotes for your research:

Pain points:
- "Am I the only one who finds flink hardly maintenable?" — u/Exorde_Mathias
- "Don't do Flink + some self-hosted db. There is so much tuning and maintenance work downstream of this." — u/RoleAffectionate4371

**The pattern:** People recommend Flink for the processing layer, but warn about the operational complexity. Nobody's offering a systematic solution — just "keep it simple" or "wait until you need it."

Your framework directly addresses that "tuning and maintenance work" comment. Save this URL.

You've got enough for now:

1. Unanswered PyFlink optimization question (9 months, 9 upvotes, 0 comments)
2. This thread showing Flink's reputation as "powerful but hard to maintain"
3. A course selling "tune Flink in production" as premium content

The market signal is clear: real pain, no systematic solution. Go post your r/dataengineering question, then prep for the 4pm call.

***SAVE THIS LINK***
https://www.reddit.com/r/dataengineering/comments/1p0j209/evaluating_realtime_analytics_solutions_for/


## Skip

The rest are either Spark-focused, announcements, or too old. #5 is your best signal for what r/dataengineering is actually discussing right now.


## Raw Results

```

Apache Flink Serialization Tuning: Choosing your Serializer

Reddit · r/dataengineering
5 years ago
News & discussion on Data Engineering topics, including but not limited to: data pipelines, databases, data formats, storage, data modeling, ...Read more
Optimizing Streaming Analytics with Apache Flink and Fluss

Reddit · r/dataengineering
2 comments · 9 months ago
Learn why streaming analytics require columnar streams, and how Fluss and Flink provides sub-second read/write latency that offers 10x read throughput ...Read more
Apache Flink 2.0.0 is out and has deep integration with ...

Reddit · r/dataengineering
4 comments · 8 months ago
Apache Flink 2.0.0 is out and has deep integration with Apache Paimon - strengthening the Streaming Lakehouse architecture, making Flink a leading solution for ...Read more
What to Learn About Spark Performance Tuning?

Reddit · r/dataengineering
10+ comments · 2 years ago
What to Learn About Spark Performance Tuning? · Usually I will start by reviewing the actual error message. · Spark UI - what do I look for here?Read more
10 answers
 
·
 
Top answer: 
Stages, catalyst, cartesian join optimizations(there are big ones even the biggest companies ...
Evaluating real-time analytics solutions for streaming data

Reddit · r/dataengineering
30+ comments · 4 weeks ago
we use Flink + Starrocks. pretty cheap but a bit more maintenance and work for changes.Read more
Does anyone have some resource to learn the internals of ...

Reddit · r/dataengineering
10+ comments · 1 year ago
Could you look into Map-Reduce and learn how common joins and group by can be achieved using Map-Reduce? That will give you an idea of how it is ...Read more
11 answers
 
·
 
Top answer: 
I have a repo that goes over data proc/storage/ concepts + a warehouse project here: https://github. ...
Real-time data pipeline with late arriving IoT

Reddit · r/dataengineering
10+ comments · 4 months ago
For ingestion + late event handling, Flink or Kafka Streams give you full control over event-time windows and watermarking but they come with ...Read more
Realtime OLAP database with transactional-level query ...

Reddit · r/dataengineering
30+ comments · 6 months ago
I'm currently exploring real-time OLAP solutions and could use some guidance. My background is mostly in traditional analytics stacks like Hive, Spark, ...Read more
How is spark really used and deployed in production ...

Reddit · r/dataengineering
40+ comments · 1 year ago
I learnt and passed the spark certification, but I don't have idea how spark is typically used in companies and how it is deployed.Read more
44 answers
 
·
 
Top answer: 
We use spark on private (no internet) on prem HDP Hadoop cluster (mostly just HDFS for storage ...
Good resource to understand Spark ui for optimisation

Reddit · r/dataengineering
3 comments · 1 year ago
Spark book from databricks can usually be found for free and has a chapter on that and also talks about out performance optimization throughout.Read more
```
---


# site:reddit.com/r/apacheflink cost
## Raw Results

```

```
---


# site:reddit.com/r/apacheflink autoscaler
## Raw Results

```

```
---


# site:reddit.com/r/apacheflink tuning parallelism
## Raw Results

```

```
---


# site:reddit.com/r/dataengineering flink cost
## Raw Results

```

```


---


# site:reddit.com/r/dataengineering flink kubernetes
## Raw Results

```

```
---

