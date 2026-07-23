#!/usr/bin/env python3
# -*- coding: utf-8 -*-

CRAWLER_NAME = "MemoJav"
CRAWLER_PROTOCOL = "crawler.v2"

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HOST = "https://memojav.com"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Safari/537.36"
)

# 仅保留 MILF 分类
CLASSES = [
    {"type_id": "categories/milf", "type_name": "MILF"},
]


def format_pic(pic):
    if not pic:
        return ""
    if pic.startswith("//"):
        return "https:" + pic
    if pic.startswith("http"):
        return pic
    if pic.startswith("/"):
        return HOST + pic
    return HOST + "/" + pic


def sanitize_source_id(raw):
    sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "", str(raw or ""))
    if not re.search(r"[A-Za-z0-9]", sanitized):
        return ""
    return sanitized[:160]


def emit(event):
    try:
        print(json.dumps(event, ensure_ascii=False), flush=True)
    except BrokenPipeError:
        sys.exit(0)


def log(msg):
    print(msg, file=sys.stderr, flush=True)


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


def fetch_html(url, session, retries=3):
    if not url.startswith("http"):
        url = HOST + (url if url.startswith("/") else "/" + url)

    last_err = None
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=12)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e
            time.sleep(1)

    log(f"[MemoJav] HTTP failed for {url}: {last_err}")
    return ""


def parse_list(html, limit=20):
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    for a in soup.select("a.video-item"):
        if len(items) >= limit:
            break

        href = a.get("href") or ""
        m = re.search(r"/video/([A-Z]+-\d+[A-Z]?)$", href, re.I)
        if not m:
            continue

        vod_id = sanitize_source_id(m.group(1).upper())
        if not vod_id or vod_id in seen:
            continue
        seen.add(vod_id)

        img = a.select_one("img.video-poster")
        img_src = ""
        if img:
            img_src = img.get("src") or img.get("data-src") or ""

        meta_el = a.select_one(".video-metadata")
        meta = meta_el.get_text(strip=True) if meta_el else ""

        title_el = a.select_one(".video-title")
        title = title_el.get_text(strip=True) if title_el else ""

        items.append({
            "vod_id": vod_id,
            "vod_name": title or vod_id,
            "vod_pic": format_pic(img_src),
            "vod_remarks": meta,
        })

    return items


def parse_page_count(html, default=1):
    if not html:
        return default

    cur = default
    m = re.search(r"pageNav-page--current[^>]*>.*?page-(\d+)", html)
    if m:
        cur = int(m.group(1))

    pages = re.findall(r"page-(\d+)", html)
    max_page = cur
    for p in pages:
        n = int(p)
        if n > max_page:
            max_page = n
    return max_page or 1


def main():
    parser = argparse.ArgumentParser(description="MemoJav crawler")
    parser.add_argument("--job", required=True, help="Path to job.json")
    args = parser.parse_args()

    try:
        with open(args.job, "r", encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        log(f"Failed to load job.json: {e}")
        sys.exit(1)

    if job.get("protocol") != CRAWLER_PROTOCOL:
        log(f"Unsupported protocol: {job.get('protocol')!r} (need {CRAWLER_PROTOCOL!r})")
        sys.exit(1)
    if job.get("mode") not in ("", None, "crawl"):
        log(f"Unsupported mode: {job.get('mode')!r}")
        sys.exit(1)

    candidate_budget = positive_int(
        job.get("candidate_budget") or job.get("target_new"),
        default=10,
    )

    seen_file = job.get("seen_source_ids_file")
    seen = set()
    if seen_file and os.path.exists(seen_file):
        with open(seen_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    seen.add(line)

    proxy_url = (job.get("network") or {}).get("proxy_url")
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    limits = job.get("limits") if isinstance(job.get("limits"), dict) else {}
    progress_interval = positive_int(limits.get("progress_interval_seconds"), default=60)

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Referer": HOST + "/"})
    if proxies:
        session.proxies.update(proxies)

    emitted = 0
    emitted_ids = set()
    checked_total = 0
    start_mono = time.monotonic()
    last_item_mono = start_mono
    last_progress_mono = start_mono

    def maybe_progress(message=""):
        nonlocal last_progress_mono
        now = time.monotonic()
        if not message and now - last_progress_mono < progress_interval:
            return
        emit({
            "type": "progress",
            "checked": checked_total,
            "emitted": emitted,
            "message": message or f"checked={checked_total} emitted={emitted}",
        })
        last_progress_mono = now

    def emit_item(video):
        nonlocal emitted, last_item_mono, last_progress_mono
        source_id = sanitize_source_id(video["vod_id"])
        title = (video.get("vod_name") or "").strip()
        if not source_id or not title:
            return

        if source_id in seen or source_id in emitted_ids:
            return

        item = {
            "type": "item",
            "source_id": source_id,
            "title": title,
            "media_url": (
                f"https://video10.memojav.net/stream/{source_id.upper()}/master.m3u8"
            ),
            "thumbnail_url": video["vod_pic"],
            "detail_url": f"{HOST}/video/{source_id}",
            "headers": {
                "Referer": HOST + "/",
                "User-Agent": UA,
            },
        }

        meta = video.get("vod_remarks", "")
        if meta:
            parts = [p.strip() for p in meta.split("•")]
            if len(parts) >= 3:
                actress = parts[-1]
                if actress:
                    item["author"] = actress

        emit(item)
        emitted += 1
        emitted_ids.add(source_id)
        last_item_mono = time.monotonic()
        last_progress_mono = last_item_mono

    target = CLASSES[0]
    tid = target["type_id"]
    pg = 1

    while True:
        if emitted >= candidate_budget:
            break
        if deadline_reached(limits, start_mono, last_item_mono, emitted):
            log("Reached job deadline/limits, stopping")
            break

        if tid == "best":
            url = "/best/" if pg == 1 else f"/best/page-{pg}"
        else:
            url = f"/{tid}/" if pg == 1 else f"/{tid}/page-{pg}"

        html = fetch_html(url, session)
        if not html:
            break

        videos = parse_list(html, limit=20)
        if not videos:
            break

        for v in videos:
            checked_total += 1
            if v["vod_id"] not in seen and v["vod_id"] not in emitted_ids:
                emit_item(v)
            maybe_progress()
            if emitted >= candidate_budget:
                break
            if deadline_reached(limits, start_mono, last_item_mono, emitted):
                break

        maybe_progress(f"Scanned {tid} page {pg}")
        page_count = parse_page_count(html, pg)
        if pg >= page_count:
            break
        pg += 1

    emit({
        "type": "done",
        "stats": {
            "checked": checked_total,
            "emitted": emitted,
        },
    })


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
        sys.exit(0)
    except BrokenPipeError:
        sys.exit(0)
    except Exception as e:
        log(f"Fatal error: {e}")
        sys.exit(1)
