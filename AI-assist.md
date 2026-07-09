# AI Usage for BrightEdge Take-Home

This document summarizes how I used AI assistance during the take-home assignment.

I used AI as a coding and design assistant, not as a replacement for the assignment itself. The crawler implementation, system design decisions, tradeoff review, and final submission were still reviewed and owned by me.

## Tools Used

- ChatGPT / Codex: planning, code review, implementation help, debugging, documentation drafting, and interview preparation.

## How AI Helped

1. Overall planning

   AI helped me break the assignment into a step-by-step plan:

   ```text
   understand requirements
   -> design Part 1 crawler
   -> implement API and UI
   -> install dependencies
   -> run local tests
   -> generate sample outputs
   -> deploy to GCP Cloud Run
   -> prepare Part 2/3 design documentation
   -> prepare final delivery checklist
   ```

2. Reviewing my past projects

   AI helped me review my previous cloud infrastructure coursework and connect it to this assignment.

   Examples:

   - Kafka project: I previously used producer/consumer logic to process YouTube video data and calculate the most-liked videos.
   - Hadoop/MapReduce project: I previously counted words from Shakespeare text, where each word occurrence counted as `1`.
   - BrightEdge connection: for page topics, the same counting idea can be extended with weighted scoring, where title/description/body evidence can contribute different weights.

3. Dependency and package setup

   AI helped identify and install the Python packages needed for the crawler service, including FastAPI, BeautifulSoup, lxml, httpx, and testing tools.

4. Tests and verification

   AI helped write and run tests for parsing and topic extraction logic. It also helped verify the crawler API locally and after deployment.

5. GCP deployment support

   AI helped prepare the Cloud Run deployment steps, debug deployment issues, and verify the live service URL and `/health` endpoint.

6. Cost and pricing estimation

   AI helped reason through Cloud Run resource settings, such as CPU, memory, max instances, request-based billing behavior, and rough monthly cost expectations for a small demo deployment.

7. Brainstorming and documentation research

   AI helped brainstorm crawler scaling options, GCP deployment choices, monitoring metrics, and release planning. It also helped organize reference material and documentation notes.

8. Documentation generation

   AI helped draft and refine:

   - README for Part 1 crawler usage.
   - Part 2/3 walkthrough document.
   - Technical detail appendix.
   - Data flow visualization.
   - Interview talk track and likely questions.

9. System design and schema design

   AI helped structure the large-scale design around:

   - URL task ingestion.
   - Kafka-style producer/consumer processing.
   - Cloud Storage for raw content.
   - Queryable metadata tables.
   - Unified page metadata schema.
   - Retry and failed URL handling.
   - Monitoring metrics.
   - MapReduce-style Top-N topic aggregation.
   - Reverse index design for topic-to-URL lookup.
