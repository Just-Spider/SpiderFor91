#!/usr/bin/env python3
"""WatchPorn.to crawler — KTPayer embed / flashvars extraction (JSON Lines streaming)."""

import argparse
import json
import os
import re
import sys

import requests
from bs4 import BeautifulSoup

CRAWLER_NAME = "WatchPorn"

HOST = "https://watchporn.to"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
CATEGORIES = ["latest-updates"]
REQUEST_TIMEOUT = 15


def sanitize_source_id(raw):
    sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '', str(raw))
    return sanitized[:160]


def clean_text(t):
    if not t:
        return ""
    t = re.sub(r'<[^>]+>', '', t)
    t = re.sub(r'\s*/\s*', ' ', t)
    t = re.sub(r'\s+', ' ', t)
    return t.strip()


def format_pic(pic):
    if not pic:
        return ""
    if pic.startswith("//"):
        return "https:" + pic
    if pic.startswith("http"):
        return pic
    return HOST + ("/" if not pic.startswith("/") else "") + pic


def parse_list_page(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_ids = set()

    for el in soup.select("div.thumb.item, div.video-item, div.item"):
        v = _parse_thumb_item(el)
        if v and v["id"] not in seen_ids:
            seen_ids.add(v["id"])
            items.append(v)

    if not items:
        for a in soup.select('a[href*="/video/"]'):
            href = a.get("href", "")
            m = re.search(r"/video/(\d+)", href)
            if not m or m.group(1) in seen_ids:
                continue
            vid = m.group(1)
            seen_ids.add(vid)
            img = a.select_one("img")
            pic = format_pic(img.get("data-original", "") or img.get("src", "")) if img else ""
            dur_el = a.select_one(".duration, .time")
            duration = clean_text(dur_el.get_text()) if dur_el else ""
            title = clean_text(
                a.get("title", "")
                or (img.get("alt", "") if img else "")
                or a.get_text()
            )
            items.append({"id": vid, "title": title, "pic": pic, "duration": duration})

    return items


def _parse_thumb_item(el):
    link = el.select_one('a[href*="/video/"]')
    if not link:
        link = el.select_one("a")
    if not link:
        return None
    href = link.get("href", "")
    m = re.search(r"/video/(\d+)", href)
    if not m:
        return None
    vid = m.group(1)

    title_el = el.select_one(".thumb__title")
    title = clean_text(title_el.get_text()) if title_el else ""
    if not title:
        img = el.select_one("img")
        if img:
            title = clean_text(img.get("alt", ""))
    if not title:
        title = clean_text(link.get("title", ""))
    if not title:
        title = ""

    img = el.select_one("img")
    pic = ""
    if img:
        pic = format_pic(img.get("data-original", "") or img.get("src", ""))

    dur_el = el.select_one(".thumb__info-item")
    if not dur_el:
        dur_el = el.select_one(".thumb__meta-item")
    duration = clean_text(dur_el.get_text()) if dur_el else ""

    return {"id": vid, "title": title, "pic": pic, "duration": duration}


def extract_page_count(html):
    soup = BeautifulSoup(html, "html.parser")
    max_page = 1
    for a in soup.select("ul.pagination__holder a[href], .pagination a[href]"):
        href = a.get("href", "")
        m = re.search(r"/(\d+)/?$", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def fetch_embed_media_url(session, vid, proxies):
    url = f"{HOST}/embed/{vid}"
    headers = {"User-Agent": UA, "Referer": f"{HOST}/"}
    resp = session.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    html = resp.text

    # Extract video_url from flashvars
    m = re.search(r"video_url\s*:\s*'([^']+)'", html)
    if m:
        video_url = m.group(1)
        if video_url.startswith("//"):
            video_url = "https:" + video_url
        return video_url

    # Fallback: look for video_url in JSON-like context
    m = re.search(r'"video_url"\s*:\s*"([^"]+)"', html)
    if m:
        return m.group(1)

    return None


def fetch_embed_metadata(session, vid, proxies):
    url = f"{HOST}/embed/{vid}"
    headers = {"User-Agent": UA, "Referer": f"{HOST}/"}
    resp = session.get(url, headers=headers, proxies=proxies, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    html = resp.text

    meta = {}

    # Extract video_url
    m = re.search(r"video_url\s*:\s*'([^']+)'", html)
    if m:
        video_url = m.group(1)
        if video_url.startswith("//"):
            video_url = "https:" + video_url
        meta["media_url"] = video_url

    # Extract preview_url
    m = re.search(r"preview_url\s*:\s*'([^']+)'", html)
    if m:
        meta["thumbnail_url"] = m.group(1)

    # Extract title from og:title
    soup = BeautifulSoup(html, "html.parser")
    og_title = soup.select_one('meta[property="og:title"]')
    if og_title:
        meta["title"] = og_title.get("content", "")

    # Extract categories/tags/models
    m = re.search(r"video_categories\s*:\s*'([^']*)'", html)
    if m:
        meta["categories"] = m.group(1)

    m = re.search(r"video_tags\s*:\s*'([^']*)'", html)
    if m:
        meta["tags"] = m.group(1)

    m = re.search(r"video_models\s*:\s*'([^']*)'", html)
    if m:
        meta["models"] = m.group(1)

    # Duration from JSON-LD
    dm = re.search(r'"duration"\s*:\s*"([^"]+)"', html)
    if dm:
        meta["duration_iso"] = dm.group(1)

    return meta


def parse_iso_duration(iso):
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return None
    h = int(m.group(1) or 0)
    mm = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mm * 60 + s


def emit(event):
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def log(msg, *args):
    line = msg % args if args else msg
    print(line, file=sys.stderr, flush=True)


def main():
    parser = argparse.ArgumentParser(description="WatchPorn.to crawler")
    parser.add_argument("--job", required=True, help="Path to job.json")
    args = parser.parse_args()

    # Load job
    try:
        with open(args.job, "r") as f:
            job = json.load(f)
    except Exception as e:
        log("Failed to load job file: %s", e)
        sys.exit(1)

    # Parse candidate_budget
    candidate_budget = (
        job.get("candidate_budget")
        or job.get("target_new")
        or 10
    )
    try:
        candidate_budget = int(candidate_budget)
        if candidate_budget <= 0:
            candidate_budget = 10
    except (ValueError, TypeError):
        candidate_budget = 10

    seen_file = job.get("seen_source_ids_file", "")
    output_dir = job.get("output_dir", "/tmp")
    proxy_url = job.get("network", {}).get("proxy_url", "")

    # Read seen IDs
    seen_ids = set()
    if seen_file and os.path.isfile(seen_file):
        try:
            with open(seen_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        seen_ids.add(line)
            log("Loaded %d seen IDs from %s", len(seen_ids), seen_file)
        except Exception as e:
            log("Warning: failed to read seen file %s: %s", seen_file, e)

    # Setup proxy
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
        log("Using proxy: %s", proxy_url)

    session = requests.Session()
    checked = 0
    emitted = 0

    try:
        for cat in CATEGORIES:
            if emitted >= candidate_budget:
                break

            page = 1
            while emitted < candidate_budget:
                # Fetch listing page
                url = f"{HOST}/{cat}/"
                if page > 1:
                    url += f"{page}/"

                log("Fetching %s", url)
                try:
                    resp = session.get(
                        url,
                        headers={"User-Agent": UA},
                        proxies=proxies,
                        timeout=REQUEST_TIMEOUT,
                    )
                    if resp.status_code != 200:
                        log("Non-200 (%d) for %s, skipping category", resp.status_code, url)
                        break
                except requests.RequestException as e:
                    log("Request failed for %s: %s", url, e)
                    break

                items = parse_list_page(resp.text)
                if not items:
                    log("No items found on %s", url)
                    break

                page_count = extract_page_count(resp.text)
                log("Found %d items on %s (page %d/%d)", len(items), cat, page, page_count)

                for item in items:
                    if emitted >= candidate_budget:
                        break

                    checked += 1
                    source_id = sanitize_source_id(item["id"])

                    if source_id in seen_ids:
                        continue

                    # Fetch embed page for video URL
                    log("Fetching embed for vid=%s title=%s", source_id, item["title"][:50])
                    try:
                        meta = fetch_embed_metadata(session, source_id, proxies)
                    except Exception as e:
                        log("Failed to fetch embed for %s: %s", source_id, e)
                        continue

                    media_url = meta.get("media_url", "")
                    if not media_url:
                        log("No media_url found for vid=%s, skipping", source_id)
                        continue

                    # Build event
                    event = {
                        "type": "item",
                        "source_id": source_id,
                        "title": meta.get("title") or item["title"],
                        "media_url": media_url,
                        "thumbnail_url": meta.get("thumbnail_url") or item["pic"],
                        "detail_url": f"{HOST}/video/{source_id}",
                        "headers": {
                            "Referer": f"{HOST}/",
                            "User-Agent": UA,
                        },
                    }

                    # Optional fields
                    if meta.get("categories"):
                        event["category"] = meta["categories"]
                    if meta.get("tags"):
                        event["tags"] = [t.strip() for t in meta["tags"].split(",") if t.strip()]
                    if meta.get("models"):
                        event["author"] = meta["models"]
                    if meta.get("duration_iso"):
                        dur_sec = parse_iso_duration(meta["duration_iso"])
                        if dur_sec:
                            event["duration_seconds"] = dur_sec

                    emit(event)
                    emitted += 1

                # Progress event
                emit({
                    "type": "progress",
                    "checked": checked,
                    "emitted": emitted,
                    "message": f"Scanned {cat} page {page}",
                })

                if page >= page_count:
                    break
                page += 1

        # Done
        emit({
            "type": "done",
            "stats": {
                "checked": checked,
                "emitted": emitted,
            },
        })
        log("Crawl complete: checked=%d emitted=%d", checked, emitted)

    except KeyboardInterrupt:
        log("Interrupted by user")
        sys.exit(0)
    except BrokenPipeError:
        log("Broken pipe")
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
        sys.exit(0)
    except BrokenPipeError:
        log("Broken pipe, exiting")
        sys.exit(0)
    except Exception as e:
        log("Fatal error: %s", e)
        sys.exit(1)
