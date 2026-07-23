#!/usr/bin/env python3
CRAWLER_NAME = "pimpbunny"
CRAWLER_PROTOCOL = "crawler.v2"

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone

LIST_URL = "https://pimpbunny.com/videos/?sort_by=video_viewed"
BASE_URL = "https://pimpbunny.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

QUALITY_RANK = {
    "1440p": 6,
    "1080p": 5,
    "720p": 4,
    "360p": 3,
    "original": 2,
    "standard": 1,
}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def emit(event):
    try:
        print(json.dumps(event, ensure_ascii=False), flush=True)
    except BrokenPipeError:
        sys.exit(0)


def positive_int(value, default=10):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def deadline_reached(limits, start_mono, last_item_mono, emitted):
    limits = limits or {}
    max_runtime = limits.get("max_runtime_seconds")
    if max_runtime:
        try:
            if time.monotonic() - start_mono >= float(max_runtime):
                return True
        except (TypeError, ValueError):
            pass
    deadline_at = limits.get("deadline_at")
    if deadline_at:
        try:
            text = str(deadline_at).replace("Z", "+00:00")
            deadline = datetime.fromisoformat(text)
            if deadline.tzinfo is None:
                return datetime.utcnow() >= deadline
            return datetime.now(timezone.utc) >= deadline.astimezone(timezone.utc)
        except Exception:
            pass
    idle = limits.get("candidate_idle_timeout_seconds")
    if idle:
        try:
            anchor = last_item_mono if emitted > 0 else start_mono
            if time.monotonic() - anchor >= float(idle):
                return True
        except (TypeError, ValueError):
            pass
    return False


def load_seen(path):
    seen = set()
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    seen.add(line)
    return seen


def clean_source_id(raw):
    cleaned = re.sub(r"[^a-zA-Z0-9_.\-]", "-", str(raw or ""))
    if not re.search(r"[A-Za-z0-9]", cleaned):
        return ""
    return cleaned[:160]


def get_quality(url):
    if "_1440p" in url:
        return "1440p"
    if "_1080p" in url:
        return "1080p"
    if "_720p" in url:
        return "720p"
    if "_360p" in url:
        return "360p"
    m = re.search(r"/get_file/(\d+)/", url)
    if m and m.group(1) == "1":
        return "original"
    return "standard"


def curl_get(url, cookie_jar, referer="", max_time=10, proxy_url=""):
    try:
        cmd = [
            "curl",
            "-s",
            "-L",
            "--max-time",
            str(max_time),
            "-H",
            "User-Agent: " + UA,
            "-H",
            "Accept-Language: en-US,en;q=0.9",
        ]
        if referer:
            cmd += ["-H", "Referer: " + referer]
        if proxy_url:
            cmd += ["-x", proxy_url]
        if os.path.exists(cookie_jar):
            cmd += ["-b", cookie_jar, "-c", cookie_jar]
        else:
            cmd += ["-c", cookie_jar]
        cmd.append(url)
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=max_time + 5,
            encoding="utf-8",
            errors="replace",
        )
        return result.stdout or ""
    except Exception:
        return ""


def test_url(url, cookie_jar, referer, proxy_url=""):
    try:
        cmd = [
            "curl",
            "-s",
            "-L",
            "--max-time",
            "10",
            "-b",
            cookie_jar,
            "-c",
            cookie_jar,
            "-H",
            "Referer: " + referer,
            "-H",
            "User-Agent: " + UA,
            "-H",
            "Range: bytes=0-65535",
            "-o",
            "NUL",
            "-w",
            "%{http_code}|%{size_download}|%{content_type}",
        ]
        if proxy_url:
            cmd += ["-x", proxy_url]
        cmd.append(url)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=12,
            encoding="utf-8",
            errors="replace",
        )
        parts = result.stdout.strip().split("|")
        code = int(parts[0]) if parts[0].isdigit() else 0
        size = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        ctype = parts[2] if len(parts) > 2 else ""
        return code in (200, 206) and "video" in ctype and size > 1000, size
    except Exception:
        return False, 0


def scrape_listing_page(proxy_url=""):
    log("[+] Scraping listing page: " + LIST_URL)
    cmd = [
        "curl",
        "-s",
        "-L",
        "--max-time",
        "15",
        "-H",
        "User-Agent: " + UA,
        LIST_URL,
    ]
    if proxy_url:
        cmd = cmd[:-1] + ["-x", proxy_url, LIST_URL]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=20,
        encoding="utf-8",
        errors="replace",
    )
    html = result.stdout
    if not html:
        log("[!] Failed to load listing page")
        return []

    videos = []
    seen = set()
    pattern = re.compile(
        r'<a[^>]*href="(?:https://pimpbunny\.com)?(/videos/([a-z0-9][a-z0-9-]+?)/)"[^>]*>'
        r'(?:.*?<img[^>]*src="([^"]+)"[^>]*alt="([^"]*)")?',
        re.DOTALL | re.IGNORECASE,
    )
    for m in pattern.finditer(html):
        slug = m.group(2)
        if slug in seen:
            continue
        seen.add(slug)
        title = m.group(4) or slug.replace("-", " ").title()
        thumbnail = m.group(3) or ""
        if thumbnail.startswith("data:") or len(thumbnail) < 10:
            thumbnail = ""
        videos.append(
            {
                "slug": slug,
                "title": title,
                "thumbnail": thumbnail,
                "page_url": BASE_URL + "/videos/" + slug + "/",
            }
        )

    log("[+] Found " + str(len(videos)) + " videos on listing page")
    return videos


def process_video(video, cookie_jar, proxy_url=""):
    slug = video["slug"]
    page_url = video["page_url"]

    html = curl_get(
        page_url, cookie_jar, referer=LIST_URL, max_time=10, proxy_url=proxy_url
    )
    if not html:
        return None

    all_urls = re.findall(
        r"https://pimpbunny\.com/get_file/\d+/[a-f0-9]+/\d+/\d+[^\"'<>\s]+",
        html,
    )

    groups = defaultdict(list)
    for url in all_urls:
        m = re.search(r"/get_file/\d+/[a-f0-9]+/(\d+)/(\d+)", url)
        if m and "_preview" not in url:
            groups[m.group(2)].append(url)

    main_urls = []
    real_video_id = ""
    for vid, urls in groups.items():
        if len(urls) > len(main_urls):
            main_urls = sorted(set(u.rsplit("?", 1)[0] for u in urls))
            real_video_id = vid

    working_urls = []
    for url in main_urls:
        ok, size = test_url(url, cookie_jar, page_url, proxy_url=proxy_url)
        if ok:
            q = get_quality(url)
            working_urls.append(
                {
                    "url": url,
                    "quality": q,
                    "size_mb": round(size / 1024 / 1024, 1),
                }
            )

    working_urls.sort(key=lambda u: QUALITY_RANK.get(u["quality"], 0), reverse=True)

    if not working_urls:
        return None

    best = working_urls[0]
    return {
        "media_url": best["url"],
        "media_quality": best["quality"],
        "real_video_id": real_video_id,
        "all_qualities": working_urls,
    }


def emit_item(video, item):
    source_id = clean_source_id(video["slug"])
    title = (video.get("title") or "").strip()
    media_url = (item.get("media_url") or "").strip()
    if not source_id or not title or not media_url:
        return False

    out = {
        "type": "item",
        "source_id": source_id,
        "title": title,
        "media_url": media_url,
        "thumbnail_url": video.get("thumbnail", ""),
        "detail_url": video["page_url"],
        "headers": {
            "Referer": BASE_URL + "/",
            "User-Agent": UA,
        },
        "thumbnail_headers": {
            "Referer": BASE_URL + "/",
            "User-Agent": UA,
        },
    }
    if item.get("media_quality"):
        out["quality"] = item["media_quality"]
    emit(out)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, help="Path to job.json")
    args = parser.parse_args()

    try:
        with open(args.job, "r", encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        log(f"[!] Failed to load job: {e}")
        sys.exit(1)

    if job.get("protocol") != CRAWLER_PROTOCOL:
        log(
            f"[!] Unsupported protocol: {job.get('protocol')!r} "
            f"(need {CRAWLER_PROTOCOL!r})"
        )
        sys.exit(1)
    if job.get("mode") not in ("", None, "crawl"):
        log(f"[!] Unsupported mode: {job.get('mode')!r}")
        sys.exit(1)

    candidate_budget = positive_int(
        job.get("candidate_budget") or job.get("target_new"),
        default=10,
    )

    seen_path = job.get("seen_source_ids_file", "")
    proxy_url = (job.get("network") or {}).get("proxy_url", "") or ""
    limits = job.get("limits") if isinstance(job.get("limits"), dict) else {}
    progress_interval = positive_int(limits.get("progress_interval_seconds"), default=60)

    log(
        "[+] Job loaded: candidate_budget="
        + str(candidate_budget)
        + " seen="
        + str(seen_path)
        + " proxy="
        + str(proxy_url or "none")
    )

    seen = load_seen(seen_path)
    log("[+] Loaded " + str(len(seen)) + " seen IDs")

    if proxy_url:
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        log("[+] Proxy configured: " + proxy_url)

    videos = scrape_listing_page(proxy_url=proxy_url)
    if not videos:
        log("[!] No videos found")
        emit({"type": "done", "stats": {"checked": 0, "emitted": 0}})
        return

    checked = 0
    emitted = 0
    cookie_jar = tempfile.mktemp(suffix=".txt")
    start_mono = time.monotonic()
    last_item_mono = start_mono
    last_progress_mono = start_mono

    def maybe_progress(message=""):
        nonlocal last_progress_mono
        now = time.monotonic()
        if not message and now - last_progress_mono < progress_interval:
            return
        emit(
            {
                "type": "progress",
                "checked": checked,
                "emitted": emitted,
                "message": message or f"checked={checked} emitted={emitted}",
            }
        )
        last_progress_mono = now

    try:
        for video in videos:
            source_id = clean_source_id(video["slug"])

            if deadline_reached(limits, start_mono, last_item_mono, emitted):
                log("[+] Reached job deadline/limits, stopping")
                break

            if not source_id or source_id in seen:
                log("[-] Skip seen: " + (source_id or video.get("slug", "")))
                checked += 1
                maybe_progress()
                continue

            if emitted >= candidate_budget:
                log(
                    "[+] Reached candidate_budget="
                    + str(candidate_budget)
                    + ", stopping"
                )
                break

            log("[*] Processing: " + video["title"][:55])

            item = process_video(video, cookie_jar, proxy_url=proxy_url)
            checked += 1

            if item is None:
                log("[-] No working URL for: " + video["slug"])
                maybe_progress()
                continue

            if emit_item(video, item):
                emitted += 1
                seen.add(source_id)
                last_item_mono = time.monotonic()
                last_progress_mono = last_item_mono
                log(
                    "[+] Emitted #"
                    + str(emitted)
                    + ": "
                    + source_id
                    + " ["
                    + item.get("media_quality", "?")
                    + "]"
                )
            maybe_progress()
            time.sleep(0.3)
    finally:
        try:
            os.unlink(cookie_jar)
        except Exception:
            pass

    emit(
        {
            "type": "done",
            "stats": {
                "checked": checked,
                "emitted": emitted,
            },
        }
    )

    log("[+] Finished: checked=" + str(checked) + " emitted=" + str(emitted))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[!] Interrupted")
        sys.exit(0)
    except BrokenPipeError:
        sys.exit(0)
    except Exception as e:
        log(f"[!] Fatal error: {e}")
        sys.exit(1)
