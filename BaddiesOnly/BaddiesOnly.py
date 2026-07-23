#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

CRAWLER_NAME = "BaddiesOnly"
CRAWLER_PROTOCOL = "crawler.v2"

HOST = "https://baddiesonly.tv"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
CATEGORIES = [
    "latest-updates",   # 最新
    # "most-popular",   # 热门
    # "top-rated",      # 最佳
]
REQUEST_TIMEOUT = 15
ITEM_LIMIT = 12


def sanitize_source_id(raw):
    sanitized = re.sub(r'[^a-zA-Z0-9_.-]', '', str(raw))
    return sanitized[:160]


def clean_text(t):
    if not t:
        return ""
    t = re.sub(r'https?://\S+', '', t)
    t = re.sub(r'\s+', ' ', t)
    return t.strip() or "Video"


def format_url(url):
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return HOST + url
    return url


def _kvs_get_license_token(license_code):
    license_code = license_code.replace("$", "")
    license_values = [int(c) for c in license_code]
    modlicense = license_code.replace("0", "1")
    center = len(modlicense) // 2
    fronthalf = int(modlicense[:center + 1])
    backhalf = int(modlicense[center:])
    modlicense = str(4 * abs(fronthalf - backhalf))[:center + 1]
    return [
        (license_values[index + offset] + current) % 10
        for index, current in enumerate(int(c) for c in modlicense)
        for offset in range(4)
    ]


def _kvs_get_real_url(video_url, license_code):
    if not video_url.startswith("function/0/"):
        return video_url
    parsed = urllib.parse.urlparse(video_url[len("function/0/"):])
    license_token = _kvs_get_license_token(license_code)
    urlparts = parsed.path.split("/")
    hash_length = 32
    hash_ = urlparts[3][:hash_length]
    indices = list(range(hash_length))
    accum = 0
    for src in reversed(range(hash_length)):
        accum += license_token[src]
        dest = (src + accum) % hash_length
        indices[src], indices[dest] = indices[dest], indices[src]
    urlparts[3] = "".join(hash_[idx] for idx in indices) + urlparts[3][hash_length:]
    return urllib.parse.urlunparse(parsed._replace(path="/".join(urlparts)))


def extract_flashvars(html):
    m = re.search(r"flashvars\s*=\s*\{([^;]+)\};", html, re.DOTALL)
    if not m:
        return {}
    text = "{" + m.group(1) + "}"
    fv = {}
    for kv in re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*'([^']*)'", text):
        fv[kv[0]] = kv[1]
    return fv


def extract_media_url(detail_html):
    flashvars = extract_flashvars(detail_html)
    if not flashvars:
        return ""
    license_code = flashvars.get("license_code", "")
    if not license_code:
        return ""
    # Prefer alt_url (720p) over video_url (480p)
    for key in ("video_alt_url", "video_url"):
        raw = flashvars.get(key, "")
        if raw and "/get_file/" in raw:
            real = _kvs_get_real_url(raw, license_code)
            if real:
                return real
    return ""


def parse_list_page(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for el in soup.select("div.item"):
        a = el.select_one('a[href*="/videos/"]')
        if not a:
            continue
        href = a.get("href", "")
        m = re.search(r"/videos/(\d+)/([a-zA-Z0-9_-]+)/?$", href)
        if not m:
            continue
        vid = m.group(1)
        slug = m.group(2)
        title = clean_text(a.get("title", ""))
        if not title:
            img = el.select_one("img.thumb")
            if img:
                title = clean_text(img.get("alt", ""))
        pic = ""
        img = el.select_one("img.thumb")
        if img:
            pic = format_url(img.get("src", ""))
        dur_el = el.select_one(".duration-time, .duration")
        duration = dur_el.get_text(strip=True) if dur_el else ""
        items.append({"id": vid, "slug": slug, "title": title, "pic": pic, "duration": duration})
    return items


def emit(event):
    try:
        sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    except BrokenPipeError:
        sys.exit(0)


def log(msg, *args):
    line = msg % args if args else msg
    print(line, file=sys.stderr, flush=True)


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


def main():
    parser = argparse.ArgumentParser(description="BaddiesOnly.tv crawler")
    parser.add_argument("--job", required=True, help="Path to job.json")
    args = parser.parse_args()

    try:
        with open(args.job, "r", encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        log("Failed to load job file: %s", e)
        sys.exit(1)

    if job.get("protocol") != CRAWLER_PROTOCOL:
        log("Unsupported protocol: %r (need %r)", job.get("protocol"), CRAWLER_PROTOCOL)
        sys.exit(1)
    if job.get("mode") not in ("", None, "crawl"):
        log("Unsupported mode: %r", job.get("mode"))
        sys.exit(1)

    candidate_budget = positive_int(
        job.get("candidate_budget") or job.get("target_new"),
        default=10,
    )

    seen_file = job.get("seen_source_ids_file", "")
    proxy_url = (job.get("network") or {}).get("proxy_url", "")
    limits = job.get("limits") if isinstance(job.get("limits"), dict) else {}
    progress_interval = positive_int(limits.get("progress_interval_seconds"), default=60)

    seen_ids = set()
    if seen_file and os.path.isfile(seen_file):
        try:
            with open(seen_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        seen_ids.add(line)
            log("Loaded %d seen IDs from %s", len(seen_ids), seen_file)
        except Exception as e:
            log("Warning: failed to read seen file %s: %s", seen_file, e)

    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
        log("Using proxy: %s", proxy_url)

    session = requests.Session()
    checked = 0
    emitted = 0
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
            "checked": checked,
            "emitted": emitted,
            "message": message or f"checked={checked} emitted={emitted}",
        })
        last_progress_mono = now

    try:
        for cat in CATEGORIES:
            if emitted >= candidate_budget or deadline_reached(limits, start_mono, last_item_mono, emitted):
                break

            page = 1
            while emitted < candidate_budget:
                if deadline_reached(limits, start_mono, last_item_mono, emitted):
                    log("Reached job deadline/limits, stopping")
                    break

                url = f"{HOST}/{cat}/{page}/"
                log("Fetching %s", url)

                try:
                    resp = session.get(
                        url,
                        headers={"User-Agent": UA, "Referer": f"{HOST}/"},
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

                log("Found %d items on %s (page %d)", len(items), cat, page)

                for item in items:
                    if emitted >= candidate_budget:
                        break
                    if deadline_reached(limits, start_mono, last_item_mono, emitted):
                        break

                    checked += 1
                    source_id = sanitize_source_id(item["id"])
                    if not source_id:
                        continue

                    if source_id in seen_ids:
                        log("Skipping seen source_id=%s", source_id)
                        maybe_progress()
                        continue

                    detail_url = f"{HOST}/videos/{item['id']}/{item['slug']}/"
                    title = (item.get("title") or "").strip() or "Video"

                    log("Fetching detail for vid=%s title=%s", source_id, title[:50])
                    try:
                        detail_resp = session.get(
                            detail_url,
                            headers={"User-Agent": UA, "Referer": f"{HOST}/"},
                            proxies=proxies,
                            timeout=REQUEST_TIMEOUT,
                        )
                        detail_html = detail_resp.text if detail_resp.status_code == 200 else ""
                    except requests.RequestException as e:
                        log("Failed to fetch detail for %s: %s", source_id, e)
                        maybe_progress()
                        continue

                    media_url = extract_media_url(detail_html) if detail_html else ""
                    if not media_url:
                        log("No media_url found for vid=%s, skipping", source_id)
                        maybe_progress()
                        continue

                    event = {
                        "type": "item",
                        "source_id": source_id,
                        "title": title,
                        "media_url": media_url,
                        "thumbnail_url": item["pic"],
                        "detail_url": detail_url,
                        "headers": {
                            "Referer": f"{HOST}/",
                            "User-Agent": UA,
                        },
                    }

                    emit(event)
                    emitted += 1
                    seen_ids.add(source_id)
                    last_item_mono = time.monotonic()
                    last_progress_mono = last_item_mono

                maybe_progress(f"Scanned {cat} page {page}")

                if len(items) < ITEM_LIMIT:
                    break
                page += 1

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
