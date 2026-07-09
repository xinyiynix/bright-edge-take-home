# Part 2/3 Walkthrough: Scaling the Crawler

This is the short version I would use for the 15-minute walkthrough. The detailed design is in [part2-3-detail.md](part2-3-detail.md).

## Table of Contents

- [My Framing](#my-framing)
- [One-Sentence Architecture](#one-sentence-architecture)
- [Why These Techniques Help](#why-these-techniques-help)
- [Part 2: Design Answer](#part-2-design-answer)
- [Part 3: POC and Release Answer](#part-3-poc-and-release-answer)
- [Personal Connection](#personal-connection)
- [Optional Visuals](#optional-visuals)
- [Detail Document](#detail-document)
- [15-Minute Talk Track](#15-minute-talk-track)

## My Framing

To be honest, Part 2 and Part 3 are very broad and open-ended. There are many ways to implement a system like this, and it can feel too abstract if I only list a lot of cloud products or system design terms.

So my approach is simple: I want to scale the Part 1 crawler using strategies I have already practiced.

1. Kafka-style producer/consumer processing: turn each URL into a small task, put tasks into a queue, and let many workers process them in parallel.
2. MapReduce-style batch analysis: after pages are crawled, use distributed aggregation to compute things like Top-N topics or topic-to-URL indexes.

## One-Sentence Architecture

```text
Monthly URL input file or MySQL table
  -> producer creates one task per URL
  -> Topic 1 buffers URL tasks
  -> fetcher workers download pages
  -> Topic 2 stores fetched-page events
  -> metadata/topic/classification consumers run in parallel
  -> raw content goes to Cloud Storage
  -> metadata goes to a queryable table
  -> MapReduce/BigQuery jobs compute summary analytics
  -> dashboards track progress, failures, latency, and cost
```

The core idea is: every URL should either produce metadata or a clear failure reason.

## Why These Techniques Help

One important clarification: Kafka "topic" and SEO/page "topic" are different things.

- Kafka topic means a queue/channel for messages, like `url-tasks`.
- Page topic means extracted content topic, like `toaster`, `camping`, or `AI`.

Part 1 is step-by-step for one API request:

```text
fetch_html -> parse_html -> classify_page -> extract_topics -> return response
```

At scale, I do not have to keep everything as one strict function chain. I can split the work into two Kafka topics and several consumer groups:

```text
Topic 1: crawl-url-requests
  producer publishes one URL task per message
  fetcher workers consume URL tasks
  fetcher workers run fetch_html(url)
  fetcher workers store raw HTML and publish a fetched-page event

Topic 2: fetched-page-events
  metadata consumers run parse_html(html) and store title/description/body fields
  topic consumers extract_topics(...) and store page topics
  classifier consumers run classify_page(...) and store page type
  monitoring consumers record status, latency, and failures
```

All of these outputs use the same key, such as `job_id + normalized_url`, so they can be joined or upserted into the same final page record.

Why Kafka-style queues help:

- Parallelism: many workers can crawl different URLs at the same time.
- Buffering: if input arrives faster than workers can process it, the queue holds the backlog.
- Independent scaling: if fetching is slow, add more fetcher workers; if topic extraction is slow, add more topic consumers.
- Failure handling: failed URLs can be retried or sent to a failed-URL queue instead of being lost.

Do I really need many Kafka topics? Not at the beginning.

- For a POC, one topic like `crawl-url-requests` is enough. It stores URL tasks, and workers consume from it.
- A slightly cleaner POC can use two topics: `crawl-url-requests` for URL tasks and `fetched-page-events` for pages that have already been downloaded.
- With two topics, multiple consumer groups can read the same fetched-page event and do different work in parallel.
- For production, I would split into even more topics only when the pipeline gets harder to operate.
- The reason to split is not speed by itself. The reason is separation: fetch results, final metadata, retries, dead-letter failures, and progress events have different consumers and different retention needs.
- This also helps debugging. If fetching works but parsing is slow, I can see which stage is backed up instead of treating the whole system as one black box.
- So my practical plan is: start with one or two topics for the POC, then split more topics only when I need clearer monitoring, retries, or independent scaling.

Why Top-N helps:

- Without Top-N aggregation, finding the most common topics means scanning and sorting a huge amount of data on one machine.
- With MapReduce-style aggregation, many machines count topics in parallel, then reducers merge the counts and keep only the most important topics.
- This is similar to my word-count/top-N coursework, but the "words" are now extracted page topics.

Why reverse index helps:

- Normal storage is `URL -> topics`.
- Reverse index is `topic -> URLs`.
- If someone asks "show pages about AI", the system can look up `AI` directly instead of scanning billions of URL records.

## Part 2: Design Answer

| Question | Simple answer |
|---|---|
| Input | Monthly URL batches from a text file or MySQL, grouped by domain and month, such as `amazon.com / 2026-07`. |
| How to scale the crawler | Turn each URL into a small queue message, then let many workers crawl URLs in parallel. |
| Storage design | Store raw HTML/body text in Cloud Storage; store searchable metadata and errors in a table such as BigQuery. |
| Unified schema | Use one page record format: URL, domain, month, status, title, description, page type, topics, content hash, timestamps, and error reason. |
| Cost / reliability / performance | Compress raw files, avoid unlimited retries, autoscale workers, throttle per domain, and make every URL end in a clear final state. |
| SLOs / SLAs | SLO: most reachable URLs finish within the target time and 100% get a final state. SLA: promise system availability and clear accounting, not that every external site will allow crawling. |
| Monitoring | Track submitted/completed URLs, queue backlog, success/failure rate, retry count, `403/429` rate, latency, storage size, and cost. |

## Part 3: POC and Release Answer

| Question | Simple answer |
|---|---|
| POC next step | Run a small end-to-end batch, around 10k-100k URLs, through input, queue, crawler workers, storage, and one Top-N topic job. |
| Evaluation | Check metadata quality, final status for every URL, retry behavior, failed URL visibility, and whether Top-N topics look reasonable. |
| Known/easy work | Reuse the Part 1 crawler functions and add storage, queue messages, and basic monitoring around them. |
| Blockers | `403/429` blocks, JavaScript-heavy pages, duplicate URLs, noisy extracted text, and storage/crawl cost. |
| Estimate | Around 7-12 engineering days for a useful POC, depending on retry logic, monitoring, and load testing depth. |
| Release plan | Start with one domain/month, low worker count, monitor closely, then increase load gradually; pause producers or roll back workers if errors spike. |

## Personal Connection

This design is not meant to sound abstract. It comes from two patterns I have already used in coursework.

Kafka pattern:

```text
producer reads input
  -> sends messages to a topic
  -> consumers process messages
  -> later stages aggregate or store results
```

For this assignment:

```text
producer reads URL file/MySQL
  -> sends one URL task per message
  -> crawler consumers fetch and parse pages
  -> storage/analytics jobs save and summarize results
```

MapReduce pattern:

```text
mapper emits small key-value pairs
reducer groups and counts them
Top-N job finds the most important terms
```

For this assignment:

```text
mapper emits (domain, month, topic)
reducer counts topic frequency
Top-N job finds the most important topics per domain/month
```

## Optional Visuals

I would not walk through every image in a 15-minute conversation, but these are useful if the interviewer asks how my coursework connects to this design.


Part 1 crawler flow:

<img width="1506" height="593" alt="Part 1 crawler flow" src="https://github.com/user-attachments/assets/1cbebca4-a958-4d01-88bb-f96362f32e59" />



Part 2 scale-out flow:



<img width="1421" height="786" alt="image" src="https://github.com/user-attachments/assets/fda06a83-b977-4226-9aa4-3b446625456b" />


Kafka queue/message example:



<img width="1284" height="710" alt="image" src="https://github.com/user-attachments/assets/7fcbca48-5c36-442b-b84d-02a26bd899ee" />

<img width="1384" height="753" alt="Kafka queue message example" src="https://github.com/user-attachments/assets/8abc66d9-7ec9-4ad0-b449-c2aabd506b98" />



Cloud Storage / Hadoop output example:



<img width="1280" height="630" alt="Cloud Storage bucket showing input scripts and results folders" src="https://github.com/user-attachments/assets/937b7aac-f70e-452d-a193-df7b1f5204f8" />

<img width="1280" height="641" alt="HDFS output directory showing success marker and reducer part files" src="https://github.com/user-attachments/assets/1ef4af17-9700-4e68-80f6-0c0576563f4d" />

<img width="2344" height="904" alt="Cloud Storage folder showing merged Hadoop output file" src="https://github.com/user-attachments/assets/8c46b72c-dcc7-43f6-b709-eca580f752ef" />



Top-N example:



<img width="1371" height="756" alt="Top-N aggregation example" src="https://github.com/user-attachments/assets/1dbe0b0f-1e74-426c-91e1-8445051aa469" />

Inverted-index example:

<img width="1366" height="828" alt="Inverted index example" src="https://github.com/user-attachments/assets/939a09e9-527b-40c3-86fe-4462e3563c1e" />


## Architecture

- **Frontend (`client.py`)**: A Flask app that lets users input data and talk with backend.
- **Backend (`server.py`)**: A Flask app that runs MapReduce jobs, works with Hadoop and Kafka, and gives APIs for search and top-N queries.
- **MapReduce Scripts**: Python scripts that define mapper and reducer for Hadoop jobs.

<img width="1520" height="1114" alt="image" src="https://github.com/user-attachments/assets/38e3d466-1d74-4857-b532-93753ba01c59" />


- **Infrastructure (`main.tf`)**: Terraform config that sets up GCP resources like Dataproc cluster, Kafka on GKE, and Compute Engine instances with Docker containers.

<img width="1236" height="888" alt="image" src="https://github.com/user-attachments/assets/7715f228-07d8-4190-a319-48400eda4025" />





## Detail Document

For deeper follow-up questions, see [part2-3-detail.md](part2-3-detail.md).

That document contains the longer details:

- Kafka topics and consumer groups.
- Example message schemas.
- Cloud Storage and metadata table layout.
- MapReduce Top-N topic aggregation.
- Inverted index idea.
- SLO/SLA details.
- Monitoring metrics.
- Detailed POC schedule.

## 15-Minute Talk Track

If I had to explain my design quickly:

```text
Part 1 is a working single-URL crawler. For Part 2, I would turn it into workers
inside a larger pipeline. A producer reads monthly URL input from text files or MySQL
and puts each URL into a URL-task topic. Fetcher workers download pages and publish
fetched-page events. Then metadata, topic extraction, classification, and monitoring
consumers can read those fetched-page events in parallel.

I would store large raw content in Cloud Storage and store queryable metadata in a table.
For analytics, I would use the same MapReduce idea I practiced before: count topics across
many pages and compute Top-N topics per domain/month.

For Part 3, I would not start with billions of URLs. I would first build a POC with
10k-100k URLs, prove that every URL gets either metadata or a clear failure status,
then gradually increase scale while watching queue backlog, error rate, latency, and cost.
```
