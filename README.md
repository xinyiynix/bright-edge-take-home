# bright-edge-take-home

Live demo: https://brightedge-crawler-pzv2s4qwdq-uc.a.run.app/

Reference pictures:

<img width="1378" height="853" alt="BrightEdge crawler demo" src="https://github.com/user-attachments/assets/897816f5-8912-4cf2-9c1b-c2dabe23c639" />
<img width="1870" height="424" alt="image" src="https://github.com/user-attachments/assets/ed79e0fe-e534-4113-8935-94d9b38ec6bd" />
<img width="983" height="296" alt="image" src="https://github.com/user-attachments/assets/32f8781b-8869-4324-aefd-922d09608099" />
<img width="921" height="281" alt="image" src="https://github.com/user-attachments/assets/f0fdb7f0-c8ff-46d0-a8ae-1ffc86cc488a" />
<img width="1869" height="865" alt="image" src="https://github.com/user-attachments/assets/b6dc401d-d42a-45ea-844d-ab5d57470d85" />

## Overview

This repository contains Part 1 of the BrightEdge Engineering Developer Candidate Assignment. It implements a core URL metadata crawler that accepts a URL, fetches the HTML page, extracts page metadata, classifies the page, and returns relevant topics.

The service is deployed on Google Cloud Run and can be tested through either a browser UI or a REST-style JSON API.

## What This Crawler Does

Given a URL, the service returns structured metadata including:

- final URL after redirects
- HTTP status code
- title
- meta description
- canonical URL
- language
- page type, such as `product`, `article`, `news`, or `unknown`
- ranked topics with scores and evidence fields
- headings
- body excerpt
- content hash
- fetch timestamp

The crawler is intentionally lightweight for the take-home demo. It focuses on one URL at a time, while the separate design document will describe how the same worker logic can scale to billions of URLs.

## Live Demo

Open this URL in a browser:

```text
https://brightedge-crawler-pzv2s4qwdq-uc.a.run.app/
```

The web UI lets a reviewer:

1. Enter any URL.
2. Click `Crawl URL`.
3. View the returned metadata and topics as formatted JSON.

Sample buttons are included for quick testing:

- CNN article
- Amazon product page
- REI article page

Note: Some sites may block server-side crawlers from cloud IPs. In that case, the service returns a clear HTTP error instead of incorrectly extracting topics from an access-denied page. This behavior is treated as a production crawler failure mode in the scale design.

## API Design

The service exposes a small REST-style HTTP API.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Browser UI for manual testing |
| `GET` | `/health` | Health check |
| `POST` | `/crawl` | Crawl one URL and return metadata/topics |
| `GET` | `/docs` | FastAPI-generated OpenAPI docs |

The main endpoint is `POST /crawl`. It is REST-style in the practical sense: the client sends a JSON representation of a crawl request, and the server returns a JSON representation of the extracted page resource/metadata. I used `POST` because crawling is an active operation with network side effects and a request body, rather than a simple cacheable lookup.

## API Usage

### Health Check

```bash
curl https://brightedge-crawler-pzv2s4qwdq-uc.a.run.app/health
```

Expected response:

```json
{"status":"ok"}
```

### Crawl Request

```bash
curl -X POST "https://brightedge-crawler-pzv2s4qwdq-uc.a.run.app/crawl" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai"}'
```

Request body:

```json
{
  "url": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai"
}
```

Example response shape:

```json
{
  "url": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai",
  "final_url": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai",
  "status_code": 200,
  "title": "Google says 90% of tech workers are now using AI at work | CNN Business",
  "description": "The overwhelming majority of tech industry workers use artificial intelligence on the job for tasks like writing and modifying code, a new Google study has found.",
  "canonical_url": "https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai",
  "language": "en",
  "page_type": "news",
  "topics": [
    {
      "topic": "google",
      "score": 1.0,
      "evidence": ["body", "description", "title"]
    },
    {
      "topic": "tech",
      "score": 1.0,
      "evidence": ["body", "description", "title"]
    }
  ],
  "headings": {
    "h1": [],
    "h2": [],
    "h3": []
  },
  "body_excerpt": "...",
  "content_hash": "...",
  "fetched_at": "2026-07-08T19:06:31.224517Z"
}
```

## Input and Output

### Input

The input is any HTTP or HTTPS URL:

```json
{
  "url": "https://example.com/page"
}
```

### Output

The output is a structured JSON document. Important fields:

| Field | Description |
|---|---|
| `url` | Original submitted URL |
| `final_url` | URL after redirects |
| `status_code` | HTTP response code |
| `title` | HTML title |
| `description` | Meta description or Open Graph description |
| `canonical_url` | Canonical URL if present |
| `language` | HTML language attribute |
| `page_type` | Simple classification: `product`, `article`, `news`, or `unknown` |
| `topics` | Ranked topic list with normalized score and evidence |
| `headings` | Extracted `h1`, `h2`, and `h3` text |
| `body_excerpt` | First portion of cleaned page text |
| `content_hash` | SHA-256 hash of extracted body text |
| `fetched_at` | UTC timestamp |

## Technical Stack

### Backend

- Python 3.11
- FastAPI
- Pydantic request/response models
- httpx for HTTP fetching
- BeautifulSoup + lxml for HTML parsing

### Frontend

- Lightweight server-rendered HTML page returned by FastAPI at `/`
- Plain JavaScript calls `POST /crawl`
- No separate frontend framework is required for the demo

### Deployment

- Docker container
- Google Cloud Run
- Cloud Build source deployment
- Cloud Run settings:
  - `min-instances=0`
  - `max-instances=1`
  - `memory=512Mi`
  - `cpu=1`
  - unauthenticated public access enabled for the demo

## Code Structure

```text
src/
  main.py              FastAPI app, routes, and browser UI
  crawler.py           URL fetching, redirects, timeouts, content-type handling
  parser.py            HTML metadata/body/headings extraction
  topic_extractor.py   Topic scoring using weighted term frequency and evidence fields
  classifier.py        Simple page-type classification rules
  models.py            Pydantic request/response models
  config.py            Request timeout, response size, and browser-like headers

scripts/
  generate_sample_outputs.py   Regenerates sample JSON outputs
  verify_api.py                API smoke test
  verify_ui_api.py             UI/API smoke test

tests/
  test_parser.py
  test_topic_extractor.py
```

## Core Crawler Flow

```text
POST /crawl
  -> validate URL with Pydantic
  -> fetch HTML with httpx
  -> follow redirects
  -> reject non-HTML content
  -> parse title, description, canonical URL, headings, language, body
  -> classify page type
  -> extract ranked topics
  -> return structured JSON response
```

## Topic Extraction Approach

The topic extractor is deterministic and explainable:

1. Extract text from title, meta description, headings, and body.
2. Tokenize and remove stop words.
3. Score terms and two-word phrases.
4. Weight important fields more heavily:
   - title
   - description
   - `h1`
   - `h2`
   - body
5. Return ranked topics with normalized scores and evidence fields.

I chose this approach because it is cheap, testable, explainable, and appropriate for a billion-URL crawler baseline. A later production system could selectively add embeddings or LLM enrichment for ambiguous or high-value pages.

## Page Classification

The current classifier uses lightweight rules based on URL patterns and metadata:

- product pages: product-like paths or product metadata
- articles/blogs: blog/article paths or guide/how-to titles
- news: news domains or news-like paths
- unknown: fallback

This is intentionally simple for the Part 1 demo. The scale design can later replace or augment it with a trained classifier or taxonomy service.

## Running Locally

Create an environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Start the service:

```bash
uvicorn src.main:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

## Testing

Run unit tests:

```bash
pytest -q
```

Run API smoke test:

```bash
python scripts/verify_api.py
```

Run UI/API smoke test:

```bash
python scripts/verify_ui_api.py
```

Generate sample outputs:

```bash
python scripts/generate_sample_outputs.py
```

## Sample Outputs

Sample outputs are generated for:

- Amazon product page
- CNN news article
- REI article page

They are stored under:

```text
sample_outputs/
  amazon.json
  cnn.json
  rei.json
```

## Deployment Summary

The live demo is deployed to Google Cloud Run as a Dockerized FastAPI service.

Deployment command used:

```bash
gcloud run deploy brightedge-crawler \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 512Mi \
  --cpu 1 \
  --timeout 60 \
  --min-instances 0 \
  --max-instances 1 \
  --concurrency 10
```

The resource limits keep the demo small and cost controlled while still allowing public testing.

## Deployment Validation Commands

These commands can be run in Google Cloud Shell to verify that the Cloud Run deployment is healthy.

Set the project, region, and service name:

```bash
PROJECT_ID=cmu-cloud-infra
REGION=us-central1
SERVICE_NAME=brightedge-crawler

gcloud config set project $PROJECT_ID
```

Get the deployed service URL:

```bash
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
  --region $REGION \
  --format='value(status.url)')

echo $SERVICE_URL
```

Expected output:

```text
https://brightedge-crawler-pzv2s4qwdq-uc.a.run.app
```

Verify the health endpoint:

```bash
curl "$SERVICE_URL/health"
```

Expected output:

```json
{"status":"ok"}
```

Verify the crawler API with the CNN sample URL:

```bash
curl -X POST "$SERVICE_URL/crawl" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai"}'
```

Verify the Cloud Run service configuration:

```bash
gcloud run services describe $SERVICE_NAME \
  --region $REGION \
  --format='table(
    metadata.name,
    status.url,
    spec.template.spec.containers[0].resources.limits.cpu,
    spec.template.spec.containers[0].resources.limits.memory,
    spec.template.metadata.annotations.autoscaling.knative.dev/minScale,
    spec.template.metadata.annotations.autoscaling.knative.dev/maxScale,
    spec.template.metadata.annotations.run.googleapis.com/execution-environment
  )'
```

List revisions:

```bash
gcloud run revisions list \
  --service $SERVICE_NAME \
  --region $REGION
```

View recent request logs:

```bash
gcloud run services logs read $SERVICE_NAME \
  --region $REGION \
  --limit 20
```

Open the Cloud Run service page in the console:

```bash
echo "https://console.cloud.google.com/run/detail/$REGION/$SERVICE_NAME/metrics?project=$PROJECT_ID"
```

Useful screenshots for review:

- Cloud Run service details page showing service URL and latest revision
- Metrics page showing request count, latency, CPU, memory, and instance count
- Logs page showing successful `/health` and `/crawl` requests
- Cloud Build history showing the successful container build
- Billing budget alert showing cost control for the demo project

## Cloud Run Architecture and Resource Tradeoffs

This deployment uses Google Cloud Run, which is a serverless container platform. The application is packaged as a Docker container, but I do not manage VMs, Kubernetes nodes, load balancers, or instance lifecycle manually. Cloud Run handles HTTPS routing, revision rollout, autoscaling, and scale-to-zero behavior.

Current demo configuration:

| Setting | Value | Why I chose it |
|---|---:|---|
| Region | `us-central1` | Low-cost default GCP region and close to common US traffic |
| CPU | `1 vCPU` | Enough for one lightweight FastAPI process and HTML parsing |
| Memory | `512Mi` | Enough for fetching/parsing bounded HTML responses |
| Timeout | `60s` | Prevents slow external sites from holding requests forever |
| Minimum instances | `0` | Allows scale to zero and keeps demo cost low |
| Maximum instances | `1` | Hard cost guardrail for the public take-home demo |
| Concurrency | `10` | Allows up to 10 simultaneous requests on the single instance |
| Authentication | Public / unauthenticated | Makes the assignment easy for reviewers to test |

### What CPU and memory mean here

CPU controls how much compute a container instance gets while it is processing requests. For this crawler, CPU is used for request handling, HTML parsing, text cleanup, topic scoring, and JSON serialization.

Memory controls how much RAM the container can use. For this crawler, memory is mainly used by the FastAPI process, HTTP response bodies, BeautifulSoup parse trees, and response JSON. The code also limits response size so one large page cannot consume unbounded memory.

### Why Cloud Run for Part 1

Cloud Run is a good fit for the Part 1 demo because:

- It runs a standard container, so the local and production runtimes are similar.
- It supports public HTTPS endpoints without extra load balancer setup.
- It scales to zero when idle, which keeps a take-home project cheap.
- It can scale horizontally later by increasing `max-instances`.
- It gives built-in logs, metrics, revisions, and rollbacks.

For a production crawler, I would not use this single synchronous service as the entire system. I would keep this crawler code as a worker, then run it behind Pub/Sub or Cloud Tasks, store results in GCS/BigQuery/Spanner, and use separate queues for retries and blocked domains.

### How many users can this demo support?

With the current demo settings:

```text
max-instances = 1
concurrency = 10
theoretical active concurrent requests = 1 * 10 = 10
```
That means Cloud Run can route up to 10 simultaneous requests to the single container instance. Because each crawl waits on an external website, real throughput depends heavily on target-site latency. If an average crawl takes 2 seconds, this setup can roughly handle about 5 crawls/second while fully occupied. If an average crawl takes 10 seconds, it is closer to 1 crawl/second.

This is enough for a reviewer demo, but intentionally conservative for cost control.

### What happens if multiple users use it at once?

Multiple users can call the crawler at the same time. Up to the configured concurrency, requests share the same container CPU and memory. If all slots are busy, additional requests may wait until a slot opens. Since `max-instances` is currently set to `1`, Cloud Run will not add more instances for this demo.

The tradeoff is deliberate:

- Lower `max-instances` means lower cost risk.
- Higher `max-instances` means better burst handling.
- Lower `concurrency` gives each crawl more isolated CPU/memory.
- Higher `concurrency` improves throughput for I/O-bound workloads, but can increase latency if CPU or memory becomes constrained.

For a public interview demo, `max-instances=1` and `concurrency=10` is a safe configuration. For production, I would load test and tune these values by domain latency, parser CPU time, error rates, and budget.

### What would I change for more scale?

For a heavier demo:

```bash
gcloud run services update brightedge-crawler \
  --region us-central1 \
  --max-instances 5 \
  --concurrency 20
```

That would allow up to:

```text
5 instances * 20 concurrency = 100 active concurrent requests
```

For production-scale crawling, I would move away from direct user-triggered synchronous crawling and use:

- Pub/Sub or Cloud Tasks for queueing URLs
- Cloud Run workers for crawling
- Cloud Storage for raw HTML snapshots
- BigQuery for analytics-friendly metadata
- Firestore/Spanner/Cloud SQL for job state and URL status tracking
- Cloud Monitoring dashboards and alerts
- Dead-letter queues for repeated `403`, `429`, DNS, timeout, and parsing failures

### Cost Estimate

The current configuration is designed to be inexpensive and uses Cloud Run's default request-based billing model.

This service is **request-based**, not instance-based, because:

- It is deployed as a normal Cloud Run service.
- I did not opt into instance-based billing / always-allocated CPU.
- `min-instances=0`, so Cloud Run can scale the service to zero when idle.
- CPU and memory are mainly billed while an instance is starting, shutting down, or actively processing requests.

This is the right model for an interview demo and an on-demand API because the service may sit idle for long periods.

Instance-based billing would make more sense if the container needed background work between requests, local caching that must stay warm, or consistently high traffic where an always-running instance is cheaper or operationally simpler.

For the deployed region `us-central1`, the Cloud Run request-based pricing table lists:

| Resource | Request-based price in `us-central1` |
|---|---:|
| CPU active time | `$0.000024` per vCPU-second |
| Memory active time | `$0.0000025` per GiB-second |
| Requests | `$0.40` per 1 million requests |

The monthly free tier for request-based Cloud Run services includes:

| Free tier item | Monthly amount |
|---|---:|
| CPU | first `180,000` vCPU-seconds |
| Memory | first `360,000` GiB-seconds |
| Requests | first `2 million` requests |

For this crawler:

```text
1 request with 1 vCPU, 512Mi memory, and 2 seconds latency:

CPU cost    = 1 vCPU * 2 sec * $0.000024      = $0.000048
Memory cost = 0.5 GiB * 2 sec * $0.0000025    = $0.0000025
Request fee = $0.40 / 1,000,000 requests      = $0.0000004

Approximate cost before free tier:
$0.0000509 per crawl
```

Rough monthly examples before free tier:

| Monthly crawls | Assumed avg crawl time | Approx cost before free tier | Practical expectation |
|---:|---:|---:|---|
| 1,000 | 2 sec | about `$0.05` | likely `$0` after free tier |
| 10,000 | 2 sec | about `$0.51` | likely `$0` after free tier |
| 100,000 | 2 sec | about `$5.09` | roughly `$0.48` after CPU free tier, if no other Cloud Run usage consumes the same billing-account free tier |
| 10,000 | 10 sec | about `$2.53` | likely `$0` after free tier |

These are estimates, not a billing guarantee. Actual cost depends on request latency, response size, retries, network egress, and whether the billing account has already used the monthly Cloud Run free tier.

- `min-instances=0` means no always-on container cost while idle.
- Cloud Run charges mainly by request count, CPU time, memory time, and network egress.
- The public demo has a hard cap of `max-instances=1`, limiting worst-case compute spend.
- A billing budget alert is configured separately in GCP for account-level cost visibility.

For light reviewer usage, expected Cloud Run cost should be near zero or very small. The main cost risk would come from many public requests, long-running crawls, large responses, retries, or high outbound network traffic. For production, I would use the GCP pricing calculator with measured average crawl duration, average response size, expected monthly request volume, and expected retry rate.

## References

- [Deploy a Python FastAPI service to Cloud Run](https://docs.cloud.google.com/run/docs/quickstarts/build-and-deploy/deploy-python-fastapi-service)
- [Cloud Run pricing](https://cloud.google.com/run/pricing)
- [Cloud Run autoscaling](https://docs.cloud.google.com/run/docs/about-instance-autoscaling)
- [Cloud Run concurrency](https://docs.cloud.google.com/run/docs/about-concurrency)

## Known Limitations

- Some sites may block Cloud Run or other data-center IPs with `403` or `429`.
- JavaScript-heavy pages may have incomplete server-rendered HTML.
- Topic extraction is deterministic and does not use semantic embeddings or LLMs.
- The page classifier is rule-based and intentionally simple for the Part 1 demo.
- The service processes one URL per request; large-scale queueing and storage are covered in the design document.

Blocked URLs should be tracked separately in a production crawler, including URL, domain, HTTP status, error reason, attempt count, and next retry time. These records can be retried with backoff, routed to a browser-rendering tier if allowed, or reviewed by domain policy.


https://brightedge-crawler-pzv2s4qwdq-uc.a.run.app/




