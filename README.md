# SpiderFor91

If you want to write a crawler script that can run smoothly in Project 91, you can refer to the following two steps. You can directly send the prompt words of the following two steps to AI

---

First
```
Help me retrieve, for each video on the xx website, the direct video URL, video name, direct cover image URL, and a unique identifier.
Ensure that the corresponding video and cover image can be downloaded directly via these URLs. The script must not rely on browser automation tools such as Selenium or Playwright, as they would make the script too heavy.
```

Second
````
# Custom Python Crawler Protocol (`crawler.v2`)
This document defines the `crawler.v2` interface between a custom Python crawler and the Go backend.
If you want your script to run smoothly in the 91 project, then you also need to comply with crawler.v2.

## 1. Script file and metadata

The crawler must be a single `.py` file with these top-level constants:

```python
CRAWLER_NAME = "Human-readable crawler name"
CRAWLER_PROTOCOL = "crawler.v2"
```

- Both values must be plain string literals, not computed values or concatenations.
- `CRAWLER_NAME` must be non-empty and at most 80 characters.
- `CRAWLER_PROTOCOL` must be exactly `"crawler.v2"` for a new script.
- The backend reads these constants statically when importing the file.

## 2. Entry point

The backend starts the script with:

```bash
python3 crawler_name.py --job /absolute/path/to/job.json
```

The script must:

- Require the `--job` argument.
- Read the job as UTF-8 JSON.
- Verify that `job["protocol"] == CRAWLER_PROTOCOL`.
- Run without interactive input.
- Use paths from the job instead of assuming a fixed working directory.

## 3. Job format

```json
{
  "protocol": "crawler.v2",
  "mode": "crawl",
  "run_id": "20260723T120000Z",
  "crawler_id": "example",
  "target_new": 100,
  "unique_target": 10,
  "candidate_budget": 100,
  "seen_source_ids_file": "/data/scriptcrawlers/example/.crawl/seen.txt",
  "output_dir": "/data/scriptcrawlers/example/output",
  "config": {},
  "network": {
    "proxy_url": "http://127.0.0.1:7890"
  },
  "limits": {
    "max_runtime_seconds": 10800,
    "deadline_at": "2026-07-23T15:00:00Z",
    "progress_interval_seconds": 60,
    "idle_timeout_seconds": 300,
    "candidate_idle_timeout_seconds": 1800
  }
}
```

Fields:

- `unique_target`: Number of content-deduplicated new videos the user wants imported.
- `candidate_budget`: Maximum number of `item` events the script may emit.
- `target_new`: Alias of `candidate_budget` included by the backend. Prefer `candidate_budget` when reading the job.
- `seen_source_ids_file`: Text file containing one previously processed `source_id` per line.
- `output_dir`: The only directory where the script may create local media files.
- `config`: Crawler-specific administrator configuration.
- `network.proxy_url`: Optional administrator-configured proxy.
- `limits`: Limits the script must honor; the backend also enforces them independently.

Read the candidate budget defensively:

```python
def positive_int(value, default=10):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


candidate_budget = positive_int(
    job.get("candidate_budget") or job.get("target_new")
)
```

## 4. Candidate and seen-ID rules

The script produces candidates; the backend performs content deduplication.

The script must:

- Read `seen_source_ids_file` before crawling.
- Skip a video whose `source_id` is already present.
- Avoid emitting the same `source_id` twice in one run.
- Emit no more than `candidate_budget` items.
- Skip known IDs before opening detail pages or downloading data whenever the ID is available on the listing page.

The script must not perform content-fingerprint deduplication or try to predict which candidates the backend will deduplicate.

### `source_id`

Each item must have a stable source-site identifier, preferably the site's native video ID, view key, or permanent detail-page slug.

Do not use random values, expiring media URLs, session-specific values, or request-specific query parameters.

Normalize IDs to ASCII letters, digits, underscores, hyphens, and dots. The result must contain at least one ASCII letter or digit and should not exceed 160 characters. If hashing is necessary, hash a stable site-native value, never an expiring media URL.

## 5. stdout and stderr

`stdout` is a strict JSON Lines protocol channel:

- Every stdout line must be one complete JSON object.
- Blank stdout lines are forbidden.
- Only `item`, `progress`, and `done` event types are valid.
- Event type values are case-sensitive.
- Each event must be flushed immediately.

```python
def emit(event):
    print(json.dumps(event, ensure_ascii=False), flush=True)
```

All logs, warnings, errors, debug output, and tracebacks must go to `stderr`.

Do not emit `type=error`. On an unrecoverable error, write the error to `stderr` and exit with a non-zero status.

## 6. `item` event

Emit each candidate as soon as it is ready:

```json
{
  "type": "item",
  "source_id": "stable-video-id",
  "title": "Video title",
  "media_url": "https://example.com/video.mp4",
  "thumbnail_url": "https://example.com/thumb.jpg",
  "detail_url": "https://example.com/detail/xxx",
  "headers": {
    "Referer": "https://example.com/",
    "User-Agent": "Mozilla/5.0 ..."
  }
}
```

Required fields:

- `type`: Exactly `"item"`.
- `source_id`: Stable identifier as defined above.
- `title`: Non-empty title.
- One of `media_url` or `media_local_file`.

Recommended fields:

- `thumbnail_url`
- `detail_url`

Optional fields:

- `author`
- `tags`
- `duration_seconds`
- `description`
- `published_at`
- `quality`

### Request headers

Use `headers` when media and thumbnail share the same request headers. When they differ, use `media_headers` and `thumbnail_headers` separately.

Header names and values must be strings. Do not log credentials or sensitive headers.

## 7. Proxy and media handling

Read the proxy from the job and apply it to all relevant requests:

```python
proxy_url = (job.get("network") or {}).get("proxy_url")
proxies = None
if proxy_url:
    proxies = {"http": proxy_url, "https": proxy_url}
```

Do not hardcode or replace the administrator's proxy. Every network request must have a finite timeout; 30 seconds or less is recommended.

Normally, the script should emit media and thumbnail URLs instead of downloading them. The backend handles downloading, fingerprints, deduplication, ingestion, thumbnails, previews, and upload.

If the script must materialize a video locally, the file must be complete, non-empty, and located inside `job["output_dir"]` before emitting:

```json
{
  "type": "item",
  "source_id": "stable-id",
  "title": "Video title",
  "media_local_file": "/absolute/path/inside/output_dir/video.mp4"
}
```

The script must not write crawler media outside `output_dir`.

## 8. `progress` event

While actively crawling, emit an `item` or `progress` event at least every `limits.progress_interval_seconds`, normally 60 seconds:

```json
{
  "type": "progress",
  "checked": 20,
  "emitted": 3,
  "message": "Scanning page 2"
}
```

- `checked` and `emitted` must be non-negative integers.
- `emitted` should equal the number of item events emitted so far.
- A progress event is a heartbeat but does not reset the no-candidate timer.

The backend stops a v2 script after `idle_timeout_seconds`, normally 300 seconds, without a valid `item`, `progress`, or `done` event.

## 9. `done` event

On normal completion, emit exactly one terminal `done` event:

```json
{
  "type": "done",
  "stats": {
    "checked": 50,
    "emitted": 10
  }
}
```

- `stats.checked` and `stats.emitted` are required non-negative integers.
- `checked` must not be less than `emitted`.
- `emitted` must exactly match the number of item events emitted in this run.
- Do not write anything to stdout after `done`.
- Exit within 5 seconds after emitting `done`.

Do not emit `done` after an unrecoverable error.

The backend may intentionally stop the script after receiving enough candidates or the first dry-run item. If stdout closes early, handle `BrokenPipeError` quietly and exit; no `done` event is required for a backend-enforced early stop.

## 10. Termination rules

The script must stop normally when any of these conditions is met:

- `emitted >= candidate_budget`.
- The source has no more pages or candidates.
- `limits.deadline_at` or `limits.max_runtime_seconds` is reached.
- No item has been emitted for `limits.candidate_idle_timeout_seconds`.

Pagination must also detect applicable end conditions such as an empty page, missing or repeated next cursor, repeated page signature, or repeated source-ID set. Do not use an unbounded pagination loop.

Use monotonic time for elapsed durations. Exit quietly on `KeyboardInterrupt` and `BrokenPipeError`.

## 11. Backend-enforced limits

The backend enforces these safeguards for `crawler.v2`:

- Maximum run time: 3 hours.
- Maximum time without an item: 30 minutes.
- Maximum item events: `candidate_budget`.
- Maximum total stdout: 64 MiB.
- Maximum stdout line: 1 MiB.
- Maximum recorded stderr: 8 KiB per line and 1 MiB total.
- The crawler process and its child process tree are terminated when the run stops.
- Strict JSON Lines with valid event types and required fields.
- Maximum time without a valid heartbeat event: 5 minutes.
- A terminal `done` event on natural success.
- Process exit within 5 seconds after `done`.
````

---
If you want the author to support more website crawler scripts, you can submit issues
You are also welcome to share your crawler script through a pull request
