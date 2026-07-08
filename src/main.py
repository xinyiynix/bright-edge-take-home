from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from .classifier import classify_page
from .crawler import CrawlError, content_hash, fetch_html
from .models import CrawlRequest, CrawlResponse
from .parser import parse_html
from .topic_extractor import extract_topics

app = FastAPI(
    title="BrightEdge URL Metadata Crawler",
    version="0.1.0",
    description="Fetch a URL and return HTML metadata, page type, and relevant topics.",
)


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>BrightEdge URL Metadata Crawler</title>
    <style>
      :root {
        color-scheme: light;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: #f6f7f9;
        color: #172033;
      }
      body {
        margin: 0;
        min-height: 100vh;
        display: flex;
        align-items: flex-start;
        justify-content: center;
      }
      main {
        width: min(980px, calc(100vw - 32px));
        margin: 48px auto;
      }
      h1 {
        margin: 0 0 8px;
        font-size: 32px;
        line-height: 1.15;
      }
      p {
        margin: 0 0 24px;
        color: #526070;
      }
      form {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: 12px;
        margin-bottom: 18px;
      }
      input {
        min-width: 0;
        padding: 12px 14px;
        border: 1px solid #c9d0da;
        border-radius: 6px;
        font: inherit;
      }
      button {
        padding: 12px 18px;
        border: 0;
        border-radius: 6px;
        background: #1a73e8;
        color: white;
        font: inherit;
        font-weight: 650;
        cursor: pointer;
      }
      button:disabled {
        opacity: 0.65;
        cursor: wait;
      }
      .examples {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 20px;
      }
      .examples button {
        background: #e8f0fe;
        color: #174ea6;
        padding: 8px 10px;
        font-size: 14px;
      }
      pre {
        overflow: auto;
        min-height: 280px;
        max-height: 560px;
        padding: 18px;
        border: 1px solid #d9dee7;
        border-radius: 8px;
        background: #101828;
        color: #d7e2f0;
        white-space: pre-wrap;
        word-break: break-word;
      }
      .links {
        margin-top: 14px;
        font-size: 14px;
      }
      .links a {
        color: #174ea6;
        margin-right: 14px;
      }
      @media (max-width: 680px) {
        form {
          grid-template-columns: 1fr;
        }
        button {
          width: 100%;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <h1>BrightEdge URL Metadata Crawler</h1>
      <p>Enter a URL to fetch HTML metadata, classify the page, and return relevant topics.</p>
      <form id="crawl-form">
        <input id="url-input" type="url" required value="https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai" />
        <button id="submit-button" type="submit">Crawl URL</button>
      </form>
      <div class="examples">
        <button type="button" data-url="https://www.cnn.com/2025/09/23/tech/google-study-90-percent-tech-jobs-ai">CNN sample</button>
        <button type="button" data-url="http://www.amazon.com/Cuisinart-CPT-122-Compact-2-Slice-Toaster/dp/B009GQ034C/ref=sr_1_1?s=kitchen&ie=UTF8&qid=1431620315&sr=1-1&keywords=toaster">Amazon sample</button>
        <button type="button" data-url="http://blog.rei.com/camp/how-to-introduce-your-indoorsy-friend-to-the-outdoors/">REI sample</button>
      </div>
      <pre id="result">Click "Crawl URL" to run the crawler.</pre>
      <div class="links">
        <a href="/docs">OpenAPI docs</a>
        <a href="/health">Health check</a>
      </div>
    </main>
    <script>
      const form = document.getElementById("crawl-form");
      const input = document.getElementById("url-input");
      const result = document.getElementById("result");
      const submitButton = document.getElementById("submit-button");

      async function crawl(url) {
        submitButton.disabled = true;
        result.textContent = "Crawling " + url + " ...";
        try {
          const response = await fetch("/crawl", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({url})
          });
          const data = await response.json();
          result.textContent = JSON.stringify(data, null, 2);
        } catch (error) {
          result.textContent = "Request failed: " + error;
        } finally {
          submitButton.disabled = false;
        }
      }

      form.addEventListener("submit", event => {
        event.preventDefault();
        crawl(input.value);
      });

      document.querySelectorAll("[data-url]").forEach(button => {
        button.addEventListener("click", () => {
          input.value = button.dataset.url;
          crawl(input.value);
        });
      });
    </script>
  </body>
</html>
"""


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/crawl", response_model=CrawlResponse)
async def crawl(request: CrawlRequest) -> CrawlResponse:
    url = str(request.url)
    try:
        html, final_url, status_code, _content_type = await fetch_html(url)
        parsed = parse_html(html)
    except CrawlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to crawl URL: {exc}") from exc

    return CrawlResponse(
        url=url,
        final_url=final_url,
        status_code=status_code,
        title=parsed.title,
        description=parsed.description,
        canonical_url=parsed.canonical_url,
        language=parsed.language,
        page_type=classify_page(final_url, parsed),
        topics=extract_topics(parsed),
        headings=parsed.headings,
        body_excerpt=parsed.body_text[:1000],
        content_hash=content_hash(parsed.body_text),
        fetched_at=datetime.now(timezone.utc),
    )
