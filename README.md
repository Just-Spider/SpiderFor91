# SpiderFor91

If you want to write a crawler script that can run smoothly in Project 91, you can refer to the following two steps. You can directly send the prompt words of the following two steps to AI

---

First
```
Help me obtain the video link, video name, video cover image link, and unique identifier for each video on xx website
You need to ensure that the corresponding video and cover image can be downloaded directly through the direct link, and the script should not use browser automation tools such as Selenium or Playwright, as this will make the script heavy.
```

Second
````
1. The script must be a single `.py` file.

2. A static crawler name must be declared at the top of the file:

   ```python
   CRAWLER_NAME = "your crawler name here"
   ```

   Notes:
   - Must be a string literal
   - No dynamic concatenation
   - The backend reads this value as the crawler name when importing the script

3. The script must support the following command:

   ```
   python3 crawler_name.py --job /path/to/job.json
   ```

4. The `job.json` format is roughly as follows:

   ```json
   {
     "protocol": "crawler.v1",
     "mode": "crawl",
     "run_id": "20260609T120000Z",
     "crawler_id": "example",
     "target_new": 100,
     "unique_target": 10,
     "candidate_budget": 100,
     "seen_source_ids_file": "/data/scriptcrawlers/example/.crawl/seen.txt",
     "output_dir": "/data/scriptcrawlers/example/output",
     "config": {},
     "network": {
       "proxy_url": "http://127.0.0.1:7890"
     }
   }
   ```

5. **Quantity semantics:**

   - `unique_target`: the number of content-deduplicated new videos the user ultimately wants imported
   - `candidate_budget`: the maximum number of candidate videos the script should output
   - `target_new`: a legacy field; the current backend sets it equal to `candidate_budget`

   The script must **not** perform its own deduplication. Content deduplication is handled by the Go backend. The script is only responsible for outputting at most `candidate_budget` candidate videos that are **not** in the seen list.

   Read the candidate count like this:

   ```python
   candidate_budget = (
       job.get("candidate_budget")
       or job.get("target_new")
       or 10
   )
   ```

   Then cast to a positive integer. If parsing fails or the value is ≤ 0, default to `10`.

6. **The script must read `seen_source_ids_file`.**

   This file contains one `source_id` per line, representing video source IDs that the backend has already processed, deleted, or confirmed as duplicates.

   If a video's `source_id` is already in the seen file:
   - It must be skipped
   - Do not output that video
   - Do not re-download or re-parse its detail page — unless parsing the detail page is the only way to obtain the `source_id`

7. **`source_id` requirements:**

   - Must be stable
   - Must uniquely identify this video on the site
   - Do not use random numbers
   - Do not use video direct links with expiring tokens
   - Do not use URL parameters that change on every request
   - Recommended: use the site's native ID, detail page slug, video ID, viewkey, etc.

   If the raw ID contains special characters, sanitize it to include only:
   - Letters
   - Digits
   - Underscores `_`
   - Hyphens `-`
   - Dots `.`

   Recommended maximum length: 160 characters.

8. **`stdout` / `stderr` rules — critically important:**

   - `stdout` must output **JSON Lines only**
   - Every line on `stdout` must be a complete JSON object
   - All regular logs, debug info, error messages, and tracebacks must go to `stderr`
   - Do not write logs like `print("Requesting...")` to `stdout`

9. As soon as each candidate video is found, immediately write one JSON line to `stdout` and flush:

   ```python
   print(json.dumps(event, ensure_ascii=False), flush=True)
   ```

10. **Recommended `item` output format:**

    ```json
    {
      "type": "item",
      "source_id": "stable-video-id",
      "title": "Video title",
      "media_url": "https://example.com/video.mp4",
      "thumbnail_url": "https://example.com/thumb.jpg",
      "detail_url": "https://example.com/detail/xxx",
      "headers": {
        "Referer": "https://example.com/"
      }
    }
    ```

11. **Field requirements:**

    Required:
    - `type`: `"item"`
    - `source_id`
    - `title`
    - `media_url`

    Recommended:
    - `thumbnail_url`
    - `detail_url`

    Optional:
    - `author`
    - `tags`
    - `category`
    - `duration_seconds`
    - `description`
    - `published_at`
    - `quality`

12. **Header rules:**

    If the video or thumbnail requires a hotlink referer, include `headers`:

    ```json
    "headers": {
      "Referer": "https://example.com/",
      "User-Agent": "..."
    }
    ```

    If the video and thumbnail require different headers, use:

    ```json
    "media_headers": {
      "Referer": "..."
    }
    ```

    ```json
    "thumbnail_headers": {
      "Referer": "..."
    }
    ```

13. **Proxy rules:**

    Read the proxy from the job:

    ```python
    proxy_url = job.get("network", {}).get("proxy_url")
    ```

    If `proxy_url` is non-empty, pass it to `requests`:

    ```python
    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }
    ```

    Do not hardcode a proxy address in the script.

14. **Do not download videos yourself.**

    Under normal circumstances, the script only needs to output `media_url`. The backend handles:
    - Downloading the video
    - Downloading the thumbnail
    - Content fingerprint deduplication
    - Database ingestion
    - Thumbnail generation
    - Preview video generation
    - Upload and migration

    Only if the video *must* be downloaded by the script to obtain it, download it into the `job["output_dir"]` directory and then output:

    ```json
    "media_local_file": "/path/inside/output_dir/video.mp4"
    ```

    Notes:
    - The local file must be inside `output_dir`
    - Do not write to any other directory

15. **Progress events are optional.**

    You may periodically output:

    ```json
    {
      "type": "progress",
      "checked": 20,
      "emitted": 3,
      "message": "Scanning page 2"
    }
    ```

    At the end, you may output:

    ```json
    {
      "type": "done",
      "stats": {
        "checked": 50,
        "emitted": 10
      }
    }
    ```

16. **Do not output `type=error` to `stdout`.**

    If an error occurs:
    - Write to `stderr`
    - Call `sys.exit(1)` if necessary

    The current Go backend only cares about `item` / `progress` / `done`; error details belong on `stderr`.

17. **Script termination conditions:**

    - Stop when `emitted >= candidate_budget`
    - Stop when there are no more pages
    - Exit quietly on `KeyboardInterrupt` or `BrokenPipeError`

18. If the original script **saves a JSON file after crawling is complete**, rewrite it to **stream JSON Lines instead**:

    - Emit each `item` as soon as it is found
    - Do not wait until all crawling is done to output everything at once

19. **Preserve the core crawling logic of the original script as much as possible.** Only change:
    - The command-line entry point
    - `job.json` reading
    - Seen-file filtering
    - `stdout` JSON Lines output
    - `stderr` logging
    - `candidate_budget` control
    - `proxy_url` support

20. Finalize the complete script without omitting any code, then **run a test** after writing it.
````

---
If you want the author to support more website crawler scripts, you can submit issues
You are also welcome to share your crawler script through a pull request
