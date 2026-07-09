# Part 2/3 Design: Operationalizing Billions of URLs

This document answers Part 2 and Part 3 of the BrightEdge take-home. It extends the Part 1 crawler into a scalable collection and metadata-processing system for billions of URLs.

## Table of Contents

- [Executive Summary](#executive-summary)
- [High-Level Architecture](#high-level-architecture)
- [Assignment Questions: Short Answers](#assignment-questions-short-answers)
  - [Part 2 Short Answers](#part-2-short-answers)
  - [Part 3 Short Answers](#part-3-short-answers)
- [Example Screenshots from My GCP/Hadoop Coursework](#example-screenshots-from-my-gcphadoop-coursework)
  - [Screenshot 1: Cloud Storage Bucket as Landing/Raw/Output Zone](#screenshot-1-cloud-storage-bucket-as-landingrawoutput-zone)
  - [Screenshot 2: HDFS Output Directory After Hadoop Job](#screenshot-2-hdfs-output-directory-after-hadoop-job)
  - [Screenshot 3: Hadoop Result Copied Back to Cloud Storage](#screenshot-3-hadoop-result-copied-back-to-cloud-storage)
- [Personal Technical Connection](#personal-technical-connection)
- [Detailed Design](#detailed-design)
  - [Part 1 Code Reuse](#part-1-code-reuse)
  - [GCP Service Choices](#gcp-service-choices)
  - [Kafka Topic and Queue Design](#kafka-topic-and-queue-design)
  - [Message Schemas](#message-schemas)
  - [Storage Design](#storage-design)
  - [Unified Data Schema](#unified-data-schema)
  - [Hadoop / Dataproc Usage](#hadoop--dataproc-usage)
  - [Distributed Top-N Topic Aggregation](#distributed-top-n-topic-aggregation)
  - [Topic Inverted Index](#topic-inverted-index)
  - [Cost, Reliability, Performance, and Scale](#cost-reliability-performance-and-scale)
  - [SLOs and SLAs](#slos-and-slas)
  - [Monitoring Metrics and Tools](#monitoring-metrics-and-tools)
- [Proof of Concept and Release Plan](#proof-of-concept-and-release-plan)
  - [POC Scope](#poc-scope)
  - [POC Evaluation Criteria](#poc-evaluation-criteria)
  - [Known vs Non-Trivial Work](#known-vs-non-trivial-work)
  - [Potential Blockers](#potential-blockers)
  - [Implementation Schedule](#implementation-schedule)
  - [Release Plan](#release-plan)
- [Final Interview Narrative](#final-interview-narrative)

## Executive Summary

The Part 1 crawler is a working single-URL metadata service. For Part 2, I would operationalize the same core crawler code as a distributed data pipeline:

part1:

<img width="1506" height="593" alt="image" src="https://github.com/user-attachments/assets/1cbebca4-a958-4d01-88bb-f96362f32e59" />

part2:

<img width="1412" height="666" alt="image" src="https://github.com/user-attachments/assets/b1cb0aa5-ad16-4e91-ba72-de68f3519d79" />

Kafka and map-reduce:


<img width="1402" height="367" alt="image" src="https://github.com/user-attachments/assets/cd347b4b-e0c0-44e8-a446-b3a9d9bdaa85" />
<img width="1384" height="753" alt="image" src="https://github.com/user-attachments/assets/8abc66d9-7ec9-4ad0-b449-c2aabd506b98" />
<img width="1366" height="510" alt="image" src="https://github.com/user-attachments/assets/de2538b6-5ece-4adb-a54a-0535cfa57411" />

Output Store:


<img width="1388" height="628" alt="image" src="https://github.com/user-attachments/assets/1d2cd7c6-8963-4950-9e89-6745f3742349" />
top-N:


<img width="1371" height="756" alt="image" src="https://github.com/user-attachments/assets/1dbe0b0f-1e74-426c-91e1-8445051aa469" />
Inverted-index:


<img width="1366" height="828" alt="image" src="https://github.com/user-attachments/assets/939a09e9-527b-40c3-86fe-4462e3563c1e" />


```text
Billions of URLs for a domain/month
  -> ingestion producer
  -> Kafka/PubSub crawl-task queue
  -> horizontally scaled crawler workers
  -> raw HTML in Cloud Storage
  -> Hadoop top-N aggregation
  -> monitoring dashboards, SLOs, retries, and release controls
```

The main design goal is not only to crawl more URLs. It is to make every URL outcome observable, retryable, cost-controlled, and queryable.

GCP implementation view:

```text
Cloud Storage / MySQL
  -> Kafka on GKE or Pub/Sub
  -> Cloud Run crawler workers
  -> Cloud Storage raw/parsed zones
  -> Hadoop analytics
  -> Cloud Monitoring + Logging + Alerting
```

## Assignment Questions: Short Answers

### Part 2 Short Answers

| Assignment question | Short answer |
|---|---|
| How to operationalize collection of billions of URLs? | Split the monthly URL input into one message per URL, publish to a queue, and process with horizontally scaled crawler workers using the Part 1 code. |
| Input source? | Text files in Cloud Storage and/or MySQL tables partitioned by `domain` and `crawl_month`. |
| Unified data schema? | A shared schema centered on `job_id`, `url`, `domain`, `crawl_month`, HTTP result, parsed metadata, page type, topics, content hash, status, attempts, and timestamps. |
| How to optimize cost? | Compress raw HTML, avoid storing raw HTML in BigQuery, use autoscale workers. |
| How to optimize reliability? | Retry transient failures, validate schemas at queue boundaries. |
| How to optimize performance? | Use async fetchers, queue partitioning, worker autoscaling, and batch aggregation for top-N reports. |
| SLOs? | 95% of submitted URLs should be fetched and processed within 24 hours. Metadata should be queryable within 5 minutes after extraction. |
| SLAs? | API availability should be 99.95%. |
| Monitoring? | Cloud Monitoring/Logging, Kafka lag metrics, BigQuery operational tables, dead-letter counts, domain error rates, latency, storage bytes, and budget alerts. |

### Part 3 Short Answers

| Assignment question | Short answer |
|---|---|
| How to proceed to POC? | Build a small end-to-end version for 10k-100k URLs using producer, queue, crawler workers, Cloud Storage, BigQuery, top-N aggregation, and monitoring. |
| How to evaluate POC? | Check metadata success rate, terminal state accounting, retry/dead-letter behavior, top-N output correctness, dashboard visibility, and rerun/idempotency. |
| Known/trivial work? | Reuse Part 1 crawler code, define queue schemas, create Cloud Storage layout, create BigQuery tables, and add basic dashboards. |
| Potential blockers? | `403/429` blocking, JS-heavy pages, raw HTML storage cost, noisy topics, MySQL extraction scale, and retry/idempotency correctness. |
| Estimates? | 7-12 engineering days for a meaningful POC, depending on Kafka/Dataproc availability and whether JS rendering is in scope. |
| Successful release? | Start with a domain/month pilot, monitor closely, ramp traffic gradually, preserve replayability, and roll back worker images or pause producers if error budget is exceeded. |

## Example Screenshots from My GCP/Hadoop Coursework

These screenshots are useful as supporting evidence/examples I have already worked with the same operational concepts this design uses: Cloud Storage as a landing/output zone, HDFS output partitions from Hadoop, and cloud bucket output after a batch job completes.

### Screenshot 1: Cloud Storage Bucket as Landing/Raw/Output Zone

What the screenshot shows:

- A GCP Cloud Storage bucket.
- Input-like folders such as `data/`.
- Script files such as `mapper.py` and `reducer.py`.
- Output-like folders such as `results/`.

How it maps to this assignment:

```text
data/
  -> input URL text files for a domain/month

mapper.py / reducer.py
  -> Hadoop/Dataproc batch processing scripts

results/
  -> batch output such as topic top-N or inverted index output
```

BrightEdge production version:

```text
gs://brightedge-url-input/domain=amazon.com/crawl_month=2026-07/part-*.txt
gs://brightedge-raw-html/domain=amazon.com/crawl_month=2026-07/*.html.gz
gs://brightedge-parsed-pages/domain=amazon.com/crawl_month=2026-07/*.jsonl.gz
gs://brightedge-analytics-output/domain_monthly_topic_top_n/crawl_month=2026-07/*.jsonl
```

Suggested caption:

```text
Example from my GCP Hadoop coursework: Cloud Storage acting as the data lake layer for input data, processing scripts, and output folders. In the BrightEdge design, this maps to URL input files, raw HTML snapshots, parsed page records, and aggregated topic outputs partitioned by domain and crawl month.
```

<img width="1280" height="630" alt="Cloud Storage bucket showing input scripts and results folders" src="https://github.com/user-attachments/assets/937b7aac-f70e-452d-a193-df7b1f5204f8" />

### Screenshot 2: HDFS Output Directory After Hadoop Job

What the screenshot shows:

- A Hadoop command listing `/output/`.
- `_SUCCESS` marker.
- Multiple `part-0000x` files.
- This means a distributed MapReduce job completed and reducers wrote partitioned output files.

How it maps to this assignment:

```text
/output/_SUCCESS
  -> job completed successfully

/output/part-00000 ... part-00006
  -> reducer output partitions
```

BrightEdge equivalent:

```text
domain_monthly_topic_top_n/
  _SUCCESS
  part-00000
  part-00001
  ...
```

This is the right mental model for large-scale topic aggregation:

```text
page_topics records
  -> mapper emits (domain, month, topic)
  -> reducer aggregates scores/counts
  -> top-N job writes partitioned result files
```

Suggested caption:

```text
Example Hadoop output from my coursework: a successful MapReduce job writes an `_SUCCESS` marker and multiple reducer output partitions. In the BrightEdge design, the same pattern can be used for large top-N topic aggregation or inverted index generation.
```

<img width="1280" height="641" alt="HDFS output directory showing success marker and reducer part files" src="https://github.com/user-attachments/assets/1ef4af17-9700-4e68-80f6-0c0576563f4d" />

### Screenshot 3: Hadoop Result Copied Back to Cloud Storage

What the screenshot shows:

- A Cloud Storage folder such as `Q2_Results/`.
- A merged result file such as `Q2_MergedOutput.txt`.
- This demonstrates the pattern of running distributed processing in Hadoop/HDFS and then exporting final results to cloud object storage.

How it maps to this assignment:

```text
HDFS reducer output
  -> merged/compacted result file
  -> Cloud Storage analytics output
  -> BigQuery load or downstream reporting
```

BrightEdge equivalent:

```text
Dataproc/Hadoop output:
  hdfs:///output/domain_monthly_topic_top_n/

Exported result:
  gs://brightedge-analytics-output/domain_monthly_topic_top_n/crawl_month=2026-07/

Queryable table:
  BigQuery brightedge_crawler.domain_monthly_topic_top_n
```

Suggested caption:

```text
Example from my Hadoop-on-GCP work: reducer output is merged and stored back in Cloud Storage. For BrightEdge, the same pattern can publish top-N topic summaries or inverted index shards into a durable analytics output zone.
```

<img width="2344" height="904" alt="Cloud Storage folder showing merged Hadoop output file" src="https://github.com/user-attachments/assets/8c46b72c-dcc7-43f6-b709-eca580f752ef" />

## Personal Technical Connection

This design is influenced by three prior cloud infrastructure patterns I have implemented:

1. Kafka producer/consumer pipeline from my YouTube API project:
   - API source produced messages into Kafka topics.
   - Consumers processed messages by group.
   - Later stages filtered, aggregated, and forwarded records to another queue.

2. Hadoop/MapReduce top-N and word-count style jobs:
   - Mapper tokenized input and emitted key-value records.
   - Reducer aggregated counts.
   - Top-N reducer used heap-based ranking.
   - Combiner reduced shuffle volume and improved runtime.

3. GCP infrastructure work:
   - Cloud Storage as data lake.
   - Dataproc/HDFS for batch processing.
   - Dockerized services and Terraform-style infrastructure thinking.

For this assignment, I apply the same mental model:

```text
billions of URL inputs
  -> producer
  -> Kafka topics / queues
  -> crawler consumer groups
  -> raw + parsed + final metadata storage
  -> Hadoop/Dataproc/Dataflow/BigQuery aggregation
  -> monitoring and release controls
```

## Detailed Design

### Part 1 Code Reuse

The Part 1 code already separates the crawler into reusable functions:

```text
fetch_html(url)
  -> raw HTML fetch stage

parse_html(html)
  -> structured metadata extraction stage

classify_page(final_url, parsed)
  -> page-type enrichment stage

extract_topics(parsed)
  -> topic feature extraction stage

content_hash(parsed.body_text)
  -> deduplication and change-detection stage
```

In Part 1, these run in one synchronous API request. In Part 2, I would reuse the same logic inside workers that consume URL messages from a queue.

### GCP Service Choices

| Layer | Service choice | Why |
|---|---|---|
| Input files | Cloud Storage | Natural landing zone for monthly URL text files |
| MySQL input | Cloud SQL for MySQL or external MySQL via Dataflow JDBC | Handles tabular URL source |
| Orchestration | Cloud Composer | Airflow-compatible DAGs for domain/month workflows |
| Queue | Kafka on GKE or managed Kafka; Pub/Sub as GCP-native alternative | Kafka matches my coursework and supports topics/consumer groups; Pub/Sub reduces ops burden |
| Crawler workers | Cloud Run workers or GKE deployments | Cloud Run is simpler and scales to zero; GKE works well if already running Kafka |
| Raw content | Cloud Storage | Cheap object storage for raw HTML snapshots |
| Metadata warehouse | BigQuery | Queryable unified metadata schema |
| Batch processing | Dataproc Hadoop/Spark, Dataflow, or BigQuery SQL | Dataproc matches Hadoop coursework; BigQuery is simplest for SQL analytics |
| Monitoring | Cloud Monitoring, Cloud Logging, Error Reporting, Alerting | GCP-native metrics, logs, dashboards, alerts |
| Infrastructure | Terraform | Reproducible GCP deployment |

### Kafka Topic and Queue Design

I would design the pipeline around explicit topics. Each topic represents a stable contract between stages.

| Topic | Producer | Consumer | Message meaning |
|---|---|---|---|
| `crawl-url-requests` | URL ingestion producer | Crawler workers | One URL crawl task |
| `crawl-fetch-results` | Crawler workers | Parser/enrichment workers | HTTP result plus raw HTML pointer |
| `page-metadata-events` | Parser/enrichment workers | Storage writer, analytics jobs | Parsed metadata, topics, classification, hash |
| `crawl-retry-requests` | Failure handler | Crawler workers | Retryable URL tasks with backoff |
| `crawl-dead-letter` | Any stage | Operations/replay tooling | Terminal failures needing review |
| `crawl-progress-events` | All workers | Monitoring sink | Lightweight progress and status events |

Topic partitioning strategy:

```text
partition key = normalized_domain or hash(normalized_url)
```

Tradeoff:

- Partitioning by domain makes domain-level throttling and ordering easier.
- Partitioning by URL hash spreads load more evenly.
- I would start with domain-based partitioning plus per-domain rate limiting, because crawler politeness and blocking risk matter more than perfect load balance.

Consumer groups:

| Consumer group | Work |
|---|---|
| `url-validator-group` | Validate, normalize, dedupe URL tasks |
| `fetcher-group` | Fetch HTML, follow redirects, record status |
| `parser-group` | Parse HTML into `ParsedPage` |
| `feature-group` | Classify page, extract topics, compute content hash |
| `storage-writer-group` | Write raw HTML, metadata, topics, failures |
| `monitoring-sink-group` | Convert progress events into metrics |

### Message Schemas

Input URL task:

```json
{
  "job_id": "amazon-2026-07",
  "url": "https://www.amazon.com/Cuisinart-CPT-122-Compact-2-Slice-Toaster/dp/B009GQ034C",
  "normalized_url": "https://www.amazon.com/Cuisinart-CPT-122-Compact-2-Slice-Toaster/dp/B009GQ034C",
  "domain": "amazon.com",
  "crawl_month": "2026-07",
  "source_type": "mysql",
  "source_uri": "mysql://url_input.amazon_2026_07",
  "priority": "normal",
  "attempt": 0,
  "created_at": "2026-07-01T00:00:00Z"
}
```

Fetch result:

```json
{
  "job_id": "amazon-2026-07",
  "url": "https://www.amazon.com/...",
  "final_url": "https://www.amazon.com/...",
  "domain": "amazon.com",
  "crawl_month": "2026-07",
  "status_code": 200,
  "content_type": "text/html",
  "raw_html_gcs_uri": "gs://brightedge-raw-html/domain=amazon.com/crawl_month=2026-07/hash.html.gz",
  "fetch_latency_ms": 842,
  "attempt": 1,
  "fetched_at": "2026-07-08T19:06:31Z"
}
```

Final metadata record:

```json
{
  "job_id": "amazon-2026-07",
  "url": "https://www.amazon.com/...",
  "final_url": "https://www.amazon.com/...",
  "domain": "amazon.com",
  "crawl_month": "2026-07",
  "status_code": 200,
  "crawl_status": "success",
  "title": "Cuisinart CPT-122 Compact Plastic 2-Slice Toaster",
  "description": "Online shopping for kitchen appliances...",
  "canonical_url": "https://www.amazon.com/.../dp/B009GQ034C",
  "language": "en-us",
  "page_type": "product",
  "topics": [
    {
      "topic": "toaster",
      "score": 1.0,
      "evidence": ["title", "body"]
    }
  ],
  "content_hash": "sha256...",
  "raw_html_gcs_uri": "gs://brightedge-raw-html/...",
  "body_text_gcs_uri": "gs://brightedge-parsed-pages/...",
  "attempt": 1,
  "fetched_at": "2026-07-08T19:06:31Z",
  "processed_at": "2026-07-08T19:06:33Z"
}
```

Failure record:

```json
{
  "job_id": "rei-2026-07",
  "url": "https://www.rei.com/blog/...",
  "domain": "rei.com",
  "crawl_month": "2026-07",
  "crawl_status": "blocked",
  "status_code": 403,
  "error_type": "http_403",
  "error_message": "HTTP 403 from server",
  "attempt": 2,
  "next_retry_at": "2026-07-08T20:00:00Z",
  "terminal": false
}
```

### Storage Design

The storage design separates raw content, parsed content, metadata, topics, and failures. This keeps cost controllable and lets different workloads use the right storage format.

Cloud Storage layout:

```text
gs://brightedge-url-input/
  source=mysql_export/domain=amazon.com/crawl_month=2026-07/part-*.csv
  source=text_file/domain=walmart.com/crawl_month=2026-07/part-*.txt

gs://brightedge-raw-html/
  domain=amazon.com/crawl_month=2026-07/status=200/hash.html.gz
  domain=rei.com/crawl_month=2026-07/status=403/hash-or-url.json

gs://brightedge-parsed-pages/
  domain=amazon.com/crawl_month=2026-07/part-*.jsonl.gz

gs://brightedge-analytics-output/
  domain_monthly_topic_top_n/crawl_month=2026-07/part-*.jsonl
```

BigQuery tables:

| Table | Purpose | Partition | Cluster |
|---|---|---|---|
| `url_tasks` | Input task tracking | `crawl_month` | `domain`, `job_id` |
| `crawl_results` | One row per URL crawl attempt/result | `DATE(fetched_at)` | `domain`, `crawl_status` |
| `page_metadata` | Title, description, canonical URL, language, page type | `crawl_month` | `domain`, `page_type` |
| `page_topics` | Repeated topic rows for analytics | `crawl_month` | `domain`, `topic` |
| `crawl_failures` | Failures, blocked URLs, retry/dead-letter info | `DATE(failed_at)` | `domain`, `error_type` |
| `domain_monthly_topic_top_n` | Aggregated top-N topics by domain/month | `crawl_month` | `domain` |
| `crawl_slo_daily` | Daily progress and SLO metrics | `metric_date` | `domain` |

### Unified Data Schema

I would keep a unified logical page schema, even if physically split across tables for cost/performance.

| Field | Type | Notes |
|---|---|---|
| `job_id` | STRING | Domain/month crawl job |
| `url` | STRING | Original URL |
| `normalized_url` | STRING | Canonicalized for dedupe |
| `final_url` | STRING | After redirects |
| `domain` | STRING | e.g. `amazon.com` |
| `crawl_month` | STRING | e.g. `2026-07` |
| `status_code` | INT64 | HTTP status |
| `crawl_status` | STRING | `success`, `blocked`, `timeout`, `invalid`, `dead_letter` |
| `title` | STRING | Parsed from HTML |
| `description` | STRING | Meta/OG/Twitter description |
| `canonical_url` | STRING | Parsed canonical link |
| `language` | STRING | HTML language |
| `page_type` | STRING | `product`, `article`, `news`, `unknown` |
| `topics` | ARRAY<STRUCT<topic STRING, score FLOAT64, evidence ARRAY<STRING>>> | Derived from lexical topic extractor |
| `content_hash` | STRING | SHA-256 of cleaned body text |
| `raw_html_gcs_uri` | STRING | Pointer to raw HTML object |
| `body_text_gcs_uri` | STRING | Pointer to cleaned text object |
| `attempt` | INT64 | Retry attempt |
| `error_type` | STRING | Populated for failures |
| `fetched_at` | TIMESTAMP | Fetch time |
| `processed_at` | TIMESTAMP | Final processing time |

### Hadoop / Dataproc Usage

I would not use Hadoop for fetching individual URLs. Fetching is I/O-bound and better handled by horizontally scaled workers. I would use Hadoop/Dataproc for large batch analytics after metadata has been collected.

Good Hadoop/Dataproc workloads:

- Top-N topics per domain/month.
- Inverted index over page topics or body text.
- Duplicate content groups by `content_hash`.
- Domain-level crawl summary.
- Large backfills where Spark/Hadoop is more cost-effective than many small API workers.

MapReduce-style topic aggregation:

```text
input:
  page_topics records from BigQuery export or Cloud Storage JSONL

mapper:
  emit((domain, crawl_month, topic), score)

combiner:
  locally sum topic scores to reduce shuffle volume

reducer:
  aggregate total score and page count per domain/month/topic

top-N:
  keep top 100 topics per domain/month

output:
  domain_monthly_topic_top_n table or JSONL files
```

### Distributed Top-N Topic Aggregation

If the dataset is small, one machine can load all topic counts and sort them. That does not work for billions of pages and potentially billions of topic observations. For a production crawler, Top-N should be computed as a distributed job.

The exact version is a two-job MapReduce pipeline:

```text
Job 1: global counting
  input: page_topics records
  mapper: emit((domain, crawl_month, topic), score)
  combiner: locally sum scores for the same key
  reducer: compute total_score and page_count for each domain/month/topic
  output: aggregated_topic_counts

Job 2: Top-N selection
  mapper: keep local top-N candidates per domain/month partition
  reducer: merge candidates and emit final top-N topics per domain/month
  output: domain_monthly_topic_top_n
```

Why two stages:

- A normal reducer answers: "what is the final value for each key?"
- A Top-N reducer answers: "among all aggregated keys, which N records rank highest?"
- The first job produces complete counts.
- The second job ranks those counts without requiring one machine to sort the entire dataset.

Correctness note:

```text
If each mapper only keeps local top-N before global aggregation, it can miss a globally important topic
that is moderately frequent on many machines but never appears in any one machine's local top-N.
```

To guarantee exact results, I would first aggregate complete counts by key, then apply Top-N.

Top-N input schema:

```json
{
  "url": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai",
  "domain": "cnn.com",
  "crawl_month": "2026-07",
  "page_type": "news",
  "topic": "AI",
  "score": 1.0,
  "evidence": ["title", "description", "body"]
}
```

Top-N output schema:

```json
{
  "domain": "cnn.com",
  "crawl_month": "2026-07",
  "rank": 1,
  "topic": "AI",
  "total_score": 30000000.0,
  "page_count": 30000000,
  "generated_at": "2026-07-31T23:59:00Z"
}
```

### Topic Inverted Index

An inverted index maps a term to the documents or URLs where that term appears.

Normal document storage is:

```text
URL -> topics
```

An inverted index reverses that relationship:

```text
topic -> URLs
```

Example:

```text
toaster -> [amazon_url]
camping -> [rei_url]
AI      -> [cnn_url]
```

This matters for search and content retrieval. If a downstream service asks for pages about `AI`, the system should not scan billions of pages. It can directly look up:

```text
AI -> [cnn.com/article1, google.com/blog2, ...]
```

Inverted index MapReduce design:

```text
input:
  {url, topics: [{topic, score}]}

mapper:
  emit(topic, {url, domain, crawl_month, score})

shuffle:
  group all URLs by topic

reducer:
  emit(topic, postings_list)
```

Word Count vs Top-N vs Inverted Index:

| Pattern | Question answered | Output example | BrightEdge use |
|---|---|---|---|
| Word count | How often does each term/topic appear? | `AI -> 30,000,000` | Topic frequency by domain/month |
| Top-N | Which terms/topics are most frequent? | `Top 100 topics in July` | SEO trend reports and domain summaries |
| Inverted index | Which URLs contain this term/topic? | `AI -> [url1, url2]` | Fast lookup of relevant URLs |

### Cost, Reliability, Performance, and Scale

Cost:

- Store raw HTML in compressed Cloud Storage, not BigQuery.
- Keep BigQuery tables for queryable metadata and topic rows.
- Use lifecycle policies to move old raw HTML to cheaper storage or expire it.
- Use Cloud Run autoscaling for crawler workers so idle capacity does not cost money.
- Use per-domain throttles to avoid wasteful retries against blocked domains.
- Use `content_hash` to skip expensive downstream reprocessing when content has not changed.

Reliability:

- Every URL must end in a terminal state: `success`, `invalid`, `blocked`, `timeout`, or `dead_letter`.
- Use retry topic with exponential backoff for transient errors.
- Use dead-letter topic for repeated failures.
- Store failure records in BigQuery for analysis.
- Make workers idempotent by using `job_id + normalized_url + attempt`.
- Use schema validation at every topic boundary.

Performance:

- Partition queue messages by domain or URL hash.
- Use async HTTP fetching inside workers.
- Tune Cloud Run/GKE worker concurrency based on external site latency.
- Use domain-level rate limits to avoid `429`.
- Keep raw HTML out of hot query path.
- Use BigQuery partitioning and clustering for large metadata queries.

Scale:

- Billions of URLs are split into independent crawl tasks.
- Queue backlog absorbs spikes.
- Crawler workers scale horizontally.
- Storage is partitioned by `domain` and `crawl_month`.
- Batch jobs aggregate results after crawl completion.
- Monitoring tracks lag, throughput, and failure rate by domain.

### SLOs and SLAs

| Area | SLO |
|---|---|
| Ingestion | `99.9%` of valid input URLs are accepted into the task queue |
| Crawl accounting | `100%` of accepted URLs reach one terminal state |
| Batch freshness | `95%` of domain/month crawl jobs complete within `24h` for planned capacity |
| Metadata availability | `99.9%` availability for querying completed metadata tables |
| Worker reliability | `<1%` worker crash/error rate excluding target-site errors |
| Data quality | `<1%` malformed final metadata records after schema validation |
| Cost | Alert at `50%`, `90%`, `100%` of monthly crawl budget |

External SLA expectation:

- The system can provide an internal SLA for metadata availability after data collection completes.
- It should not promise full crawl success for every external URL, because target sites can block, rate limit, remove, or change content.
- Instead, the SLA should guarantee accurate accounting of every URL outcome.

### Monitoring Metrics and Tools

GCP tools:

- Cloud Monitoring dashboards and alerting.
- Cloud Logging structured logs.
- Error Reporting for unhandled exceptions.
- BigQuery operational tables for crawl progress.
- Kafka monitoring through Confluent Control Center, Prometheus/Grafana, or GKE metrics if Kafka runs on GKE.
- Cloud Billing budgets and alerts.

Key metrics:

| Metric | Why it matters |
|---|---|
| URL tasks produced per minute | Ingestion throughput |
| Queue lag / oldest unconsumed message age | Whether workers are falling behind |
| Crawl success rate by domain | Domain-level health |
| HTTP status distribution | Detect `403`, `429`, `5xx` spikes |
| Retry count and dead-letter count | Reliability and blocker visibility |
| Worker p50/p95/p99 latency | Performance |
| Worker CPU/memory/concurrency | Capacity tuning |
| Raw HTML bytes stored per domain/month | Cost control |
| BigQuery rows written and failed inserts | Storage reliability |
| Topic extraction coverage | Whether pages produce usable topics |
| Duplicate rate by `content_hash` | Content quality and dedupe opportunity |
| Budget burn rate | Cost guardrail |

## Proof of Concept and Release Plan

### POC Scope

```text
Goal:
Demonstrate an end-to-end scaled version of the Part 1 crawler using a small domain/month batch.

Input:
10k-100k URLs from one text file and one MySQL table.

Pipeline:
Producer -> queue -> crawler workers -> Cloud Storage + BigQuery -> top-N aggregation -> dashboard.
```

### POC Evaluation Criteria

- At least `95%` of valid, reachable URLs produce metadata records.
- `100%` of accepted URLs have a terminal state.
- Top-N topic aggregation runs successfully.
- Dashboard shows backlog, success/failure rates, latency, and cost signals.
- System can be rerun for the same input without corrupting data.
- Retry and dead-letter behavior is demonstrated.

### Known vs Non-Trivial Work

Known/trivial:

| Work | Why straightforward | Estimate |
|---|---|---:|
| Reuse Part 1 parser/classifier/topic extractor in worker | Code is already modular | 0.5 day |
| Define JSON schemas for Kafka messages | Based on existing Pydantic models | 0.5 day |
| Create Cloud Storage partition layout | Standard data lake pattern | 0.5 day |
| Create BigQuery metadata tables | Schema is defined above | 0.5 day |
| Add basic Cloud Monitoring dashboard | GCP built-in metrics | 0.5-1 day |

Non-trivial:

| Work | Why harder | Estimate |
|---|---|---:|
| Per-domain crawl policy and rate limiting | Different sites block/rate-limit differently | 1-2 days |
| Retry and dead-letter correctness | Must avoid duplicate writes and infinite retries | 1 day |
| JS-heavy page handling | May require browser rendering tier | 2-4 days for POC |
| Large-scale load tuning | Needs real latency/error distribution | 2-3 days |
| Topic quality improvement | Needs stop words, taxonomy, evaluation data | 2-5 days |
| Schema evolution and backfills | Requires compatibility and governance | 1-3 days |

### Potential Blockers

- Target sites return `403`, `429`, captcha, or bot-detection pages from cloud IPs.
- Some pages rely on JavaScript rendering.
- Raw HTML storage cost can grow quickly.
- Queue backlog can grow if domain-level throttles are too strict.
- MySQL source extraction can become slow without pagination/chunking.
- Topic quality can be noisy for e-commerce pages with navigation-heavy HTML.

### Implementation Schedule

| Phase | Deliverable | Estimate |
|---|---|---:|
| P0 | Finalize schemas, topic names, storage partitions, SLO targets | 0.5 day |
| P1 | Build URL ingestion producer from text file and MySQL | 1 day |
| P2 | Set up Kafka/PubSub topics, retry topic, dead-letter topic | 0.5-1 day |
| P3 | Convert Part 1 code into worker mode | 1 day |
| P4 | Write raw HTML to Cloud Storage and metadata/topics/failures to BigQuery | 1-2 days |
| P5 | Add retry, backoff, idempotency, and terminal-state tracking | 1-2 days |
| P6 | Implement top-N aggregation with BigQuery SQL or Dataproc Hadoop job | 1 day |
| P7 | Build monitoring dashboard and alerts | 0.5-1 day |
| P8 | Run 10k-100k URL load test and tune concurrency/rate limits | 1-2 days |
| P9 | Write release notes, runbook, rollback plan, and POC evaluation report | 0.5-1 day |

Total POC estimate:

```text
7-12 engineering days depending on whether Kafka/Dataproc already exists and whether JS rendering is in scope.
```

### Release Plan

Pre-release:

- Unit tests for parser, classifier, topic extractor.
- Integration test with sample URL file.
- Schema validation for every message type.
- Idempotency test: replay the same URL messages safely.
- Retry/dead-letter test.
- Load test with 10k-100k URLs.
- Cost estimate and billing alert.

Release:

- Deploy workers with small `max-instances`.
- Run one domain/month pilot.
- Monitor backlog, p95 latency, success rate, `403/429`, and BigQuery insert errors.
- Increase concurrency gradually.
- Freeze or rollback if error budget is exceeded.

Rollback:

- Stop producers.
- Pause worker consumers.
- Keep queue messages for replay.
- Roll back worker container image.
- Preserve raw/failure records for debugging.

Definition of high-quality release:

- Data is complete enough for evaluation.
- Failures are visible, not hidden.
- Costs are bounded.
- All URLs are accounted for.
- Operators can explain progress from dashboard metrics.
- The system can be replayed safely.

## Final Interview Narrative

If asked how I would connect Part 1 to Part 2:

```text
Part 1 gives me a modular crawler worker: fetch, parse, classify, extract topics, and hash content.
For Part 2, I would operationalize it as a producer-consumer data pipeline. Monthly URL inputs
from text files or MySQL become queue messages. Crawler workers process messages in parallel,
store raw HTML in Cloud Storage, store metadata and topics in BigQuery, and send failures to retry
or dead-letter queues. For analytics, I would use a Hadoop/Dataproc or BigQuery top-N aggregation
similar to my previous word-count/top-N coursework. The key design goal is not only crawling more
URLs, but making every URL outcome observable, retryable, and cost-controlled.
```
