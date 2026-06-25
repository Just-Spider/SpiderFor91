#!/usr/bin/env python3
# -*- coding: utf-8 -*-

CRAWLER_NAME = "MemoJav"

import sys
import json
import os
import re
import time
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

# ---------------------------------------------------------------------
# Helpers (不变)
# ---------------------------------------------------------------------

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

def clean_text(html):
    if not html:
        return ""
    t = str(html)
    t = re.sub(r"<[^>]+>", "", t)
    t = t.replace("/", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

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

    sys.stderr.write(f"[MemoJav] HTTP failed for {url}: {last_err}\n")
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

        vod_id = m.group(1).upper()
        if vod_id in seen:
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
    m = re.search(r'pageNav-page--current[^>]*>.*?page-(\d+)', html)
    if m:
        cur = int(m.group(1))

    pages = re.findall(r"page-(\d+)", html)
    max_page = cur
    for p in pages:
        n = int(p)
        if n > max_page:
            max_page = n
    return max_page or 1

# ---------------------------------------------------------------------
# Main – 只爬 MILF
# ---------------------------------------------------------------------

def main():
    if len(sys.argv) != 3 or sys.argv[1] != "--job":
        sys.stderr.write("Usage: python3 crawler.py --job /path/to/job.json\n")
        sys.exit(1)

    job_path = sys.argv[2]
    try:
        with open(job_path, "r") as f:
            job = json.load(f)
    except Exception as e:
        sys.stderr.write(f"Failed to load job.json: {e}\n")
        sys.exit(1)

    # Candidate budget
    candidate_budget = job.get("candidate_budget") or job.get("target_new") or 10
    try:
        candidate_budget = int(candidate_budget)
    except Exception:
        candidate_budget = 10
    if candidate_budget <= 0:
        candidate_budget = 10

    # Seen set
    seen_file = job.get("seen_source_ids_file")
    seen = set()
    if seen_file and os.path.exists(seen_file):
        with open(seen_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    seen.add(line)

    # Proxy
    proxy_url = job.get("network", {}).get("proxy_url")
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}

    # Session
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Referer": HOST + "/"})
    if proxies:
        session.proxies.update(proxies)

    emitted = 0
    emitted_ids = set()
    checked_total = 0

    def emit_item(video):
        nonlocal emitted
        source_id = video["vod_id"]

        if source_id in seen or source_id in emitted_ids:
            return

        item = {
            "type": "item",
            "source_id": source_id,
            "title": video["vod_name"],
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

        print(json.dumps(item, ensure_ascii=False), flush=True)
        emitted += 1
        emitted_ids.add(source_id)

    # 只爬 MILF 分类
    target = CLASSES[0]  # {"type_id": "categories/milf", ...}
    tid = target["type_id"]
    pg = 1

    while True:
        if emitted >= candidate_budget:
            break

        # 构建 URL（和原逻辑一致）
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
            if emitted >= candidate_budget:
                break

        page_count = parse_page_count(html, pg)
        if pg >= page_count:
            break
        pg += 1

    # Done event
    done = {
        "type": "done",
        "stats": {
            "checked": checked_total,
            "emitted": emitted,
        },
    }
    print(json.dumps(done, ensure_ascii=False), flush=True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.stderr.write("Interrupted by user\n")
        sys.exit(130)
    except BrokenPipeError:
        sys.stderr.write("Broken pipe, exiting\n")
        sys.exit(1)