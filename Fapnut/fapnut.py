#!/usr/bin/env python3
"""
Fapnut.net Video Crawler
=========================
Scrapes video direct links, names, cover images, and unique IDs from fapnut.net
without browser automation (pure requests + BeautifulSoup).

Usage:
    python3 fapnut.py --job /path/to/job.json

    # For manual testing:
    python3 fapnut.py --url "https://fapnut.net/some-video-slug/"
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import threading

CRAWLER_NAME = "fapnut"

# ── Configuration ──────────────────────────────────────────────────────────

BASE_URL = "https://fapnut.net"
VIDEOS_PER_PAGE = 20
DEFAULT_WORKERS = 5
REQUEST_TIMEOUT = 30
DELAY_BETWEEN_PAGES = 0.5
DELAY_BETWEEN_VIDEOS = 0.3
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def log(msg):
    """Write a log message to stderr."""
    print(f"[{CRAWLER_NAME}] {msg}", file=sys.stderr, flush=True)


def emit(obj):
    """Write a JSON Lines object to stdout and flush."""
    print(json.dumps(obj, ensure_ascii=False), flush=True)


# ── HTTP helper ────────────────────────────────────────────────────────────

def create_session(proxies=None):
    """Create a requests session with headers and optional proxy."""
    session = requests.Session()
    session.headers.update(HEADERS)
    if proxies:
        session.proxies.update(proxies)
    return session


def fetch_page(session, url, max_retries=MAX_RETRIES):
    """Fetch a page with retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                time.sleep(wait)
            else:
                raise e
    return None


# ── Pagination helper ──────────────────────────────────────────────────────

def get_total_pages(session):
    """Get the total number of listing pages from the first page."""
    resp = fetch_page(session, BASE_URL)
    soup = BeautifulSoup(resp.text, "html.parser")

    # The pagination is: div.pagination > ul > li > a
    # The last-page link has text "Last" and href="/page/NNN/"
    pagination_links = soup.select(".pagination a")
    for link in pagination_links:
        if link.text.strip().lower() == "last":
            href = link.get("href", "")
            match = re.search(r"/page/(\d+)", href)
            if match:
                return int(match.group(1))

    # Fallback: find the largest page number in pagination links
    max_page = 1
    for link in pagination_links:
        href = link.get("href", "")
        match = re.search(r"/page/(\d+)", href)
        if match:
            max_page = max(max_page, int(match.group(1)))

    return max_page


# ── Listing page scraper ───────────────────────────────────────────────────

def scrape_listing_page(session, page_num):
    """
    Scrape a single listing page for basic video metadata.

    Returns a list of dicts with keys:
        post_id, title, cover_image, duration, page_url, categories, actor,
        duration_seconds
    """
    url = BASE_URL if page_num == 1 else f"{BASE_URL}/page/{page_num}/"
    resp = fetch_page(session, url)
    soup = BeautifulSoup(resp.text, "html.parser")

    videos = []
    articles = soup.select("article.loop-video")

    for article in articles:
        try:
            post_id = article.get("data-post-id", "")
            cover_image = article.get("data-main-thumb", "")

            # Detail page URL
            link = article.find("a")
            page_url = link.get("href", "") if link else ""

            # Title
            title_elem = article.select_one("header.entry-header span")
            title = title_elem.text.strip() if title_elem else ""

            # Duration string (e.g., "32:59")
            duration_elem = article.select_one("span.duration")
            duration_str = ""
            if duration_elem:
                duration_str = duration_elem.text.strip()

            # Parse duration to seconds
            duration_seconds = None
            if duration_str:
                parts = duration_str.split(":")
                if len(parts) == 2:
                    duration_seconds = int(parts[0]) * 60 + int(parts[1])
                elif len(parts) == 3:
                    duration_seconds = (
                        int(parts[0]) * 3600
                        + int(parts[1]) * 60
                        + int(parts[2])
                    )

            # HD indicator
            hd_elem = article.select_one("span.hd-video")
            is_hd = hd_elem is not None

            # Categories from CSS classes
            classes = article.get("class", [])
            category_slugs = [
                c.replace("category-", "")
                for c in classes
                if c.startswith("category-")
            ]

            # Actor
            actors = [
                c.replace("actors-", "")
                for c in classes
                if c.startswith("actors-")
            ]
            actor = actors[0] if actors else ""

            videos.append(
                {
                    "post_id": post_id,
                    "title": title,
                    "cover_image": cover_image,
                    "cover_640x360": "",
                    "duration_str": duration_str,
                    "duration_seconds": duration_seconds,
                    "is_hd": is_hd,
                    "video_url": "",
                    "page_url": page_url,
                    "upload_date": "",
                    "categories": category_slugs,
                    "actor": actor,
                }
            )
        except Exception as e:
            log(f"WARN: Failed to parse article on page {page_num}: {e}")
            continue

    return videos


# ── Detail page scraper ────────────────────────────────────────────────────

def scrape_detail_page(session, page_url):
    """
    Scrape a single video detail page for:
        - video m3u8 URL  (from <meta itemprop="contentURL">)
        - 640x360 cover    (from <meta itemprop="thumbnailUrl">)
        - upload date      (from <meta itemprop="uploadDate">)
        - ISO duration     (from <meta itemprop="duration">)

    Returns a dict with keys:
        video_url, cover_640x360, upload_date, duration_iso
    """
    result = {
        "video_url": "",
        "cover_640x360": "",
        "upload_date": "",
        "duration_iso": "",
    }

    try:
        resp = fetch_page(session, page_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Schema.org VideoObject meta tags ────────────────────────────
        content_url = soup.select_one('meta[itemprop="contentURL"]')
        if content_url:
            result["video_url"] = content_url.get("content", "")

        thumbnail_url = soup.select_one('meta[itemprop="thumbnailUrl"]')
        if thumbnail_url:
            result["cover_640x360"] = thumbnail_url.get("content", "")

        upload_date = soup.select_one('meta[itemprop="uploadDate"]')
        if upload_date:
            result["upload_date"] = upload_date.get("content", "")

        duration_meta = soup.select_one('meta[itemprop="duration"]')
        if duration_meta:
            result["duration_iso"] = duration_meta.get("content", "")

        # ── JSON-LD fallback ────────────────────────────────────────────
        if not result["video_url"]:
            jsonld = soup.select_one('script[type="application/ld+json"]')
            if jsonld:
                try:
                    data = json.loads(jsonld.string)
                    graph = data.get("@graph", [data])
                    for item in graph:
                        if item.get("@type") == "VideoObject":
                            result["video_url"] = item.get("contentUrl", "")
                            result["cover_640x360"] = item.get(
                                "thumbnailUrl", ""
                            )
                            result["duration_iso"] = item.get("duration", "")
                            result["upload_date"] = item.get(
                                "uploadDate", ""
                            )
                            break
                except (json.JSONDecodeError, KeyError):
                    pass

    except Exception as e:
        log(f"WARN: Failed to scrape detail page {page_url}: {e}")

    return result


# ── Item builder ───────────────────────────────────────────────────────────

def build_item(video):
    """
    Build a crawler item from video data.

    Args:
        video: dict with keys from listing + detail scraping

    Returns:
        dict suitable for stdout JSON Lines output
    """
    item = {
        "type": "item",
        "source_id": video["post_id"],
        "title": video["title"],
        "media_url": video["video_url"],
        "thumbnail_url": video["cover_image"] or video["cover_640x360"],
        "detail_url": video["page_url"],
        "headers": {
            "Referer": "https://fapnut.net/",
        },
    }

    # Optional fields
    if video.get("actor"):
        item["author"] = video["actor"]

    if video.get("categories"):
        item["tags"] = video["categories"]
        # Use first category as primary
        if video["categories"]:
            item["category"] = video["categories"][0]

    if video.get("duration_seconds"):
        item["duration_seconds"] = video["duration_seconds"]
    elif video.get("duration_str"):
        item["duration"] = video["duration_str"]

    if video.get("upload_date"):
        item["published_at"] = video["upload_date"]

    quality = "720p" if video.get("is_hd") else "480p"
    item["quality"] = quality

    return item


# ── Read seen file ─────────────────────────────────────────────────────────

def load_seen_ids(seen_file_path):
    """
    Load seen source IDs from a text file (one ID per line).

    Returns a set of source_id strings.
    """
    seen = set()
    if not seen_file_path:
        return seen

    if not os.path.exists(seen_file_path):
        log(f"Seen file does not exist yet: {seen_file_path}")
        return seen

    try:
        with open(seen_file_path, "r", encoding="utf-8") as f:
            for line in f:
                sid = line.strip()
                if sid:
                    seen.add(sid)
        log(f"Loaded {len(seen)} seen IDs from {seen_file_path}")
    except Exception as e:
        log(f"WARN: Failed to read seen file: {e}")

    return seen


# ── Main job runner ────────────────────────────────────────────────────────

def run_job(job_path):
    """Run the crawler job from a job.json file."""

    # ── Parse job config ─────────────────────────────────────────────────
    if not os.path.exists(job_path):
        log(f"ERROR: job file not found: {job_path}")
        sys.exit(1)

    with open(job_path, "r", encoding="utf-8") as f:
        job = json.load(f)

    # candidate_budget - maximum number of candidates to output
    candidate_budget = job.get("candidate_budget") or job.get("target_new") or 10
    try:
        candidate_budget = int(candidate_budget)
        if candidate_budget <= 0:
            candidate_budget = 10
    except (ValueError, TypeError):
        candidate_budget = 10

    # seen file
    seen_file = job.get("seen_source_ids_file", "")

    # proxy
    proxy_url = job.get("network", {}).get("proxy_url", "")
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
        log(f"Using proxy: {proxy_url}")

    # output_dir (for reference, not used for normal operation)
    output_dir = job.get("output_dir", "")

    log(f"Job started: candidate_budget={candidate_budget}, "
        f"seen_file={seen_file}, proxy={'yes' if proxy_url else 'no'}")

    # ── Load seen IDs ────────────────────────────────────────────────────
    seen = load_seen_ids(seen_file)

    # ── Setup ────────────────────────────────────────────────────────────
    session = create_session(proxies)

    # Auto-detect total pages
    try:
        max_pages = get_total_pages(session)
        log(f"Found {max_pages} listing pages ({max_pages * VIDEOS_PER_PAGE} "
            f"videos estimated)")
    except Exception as e:
        log(f"ERROR: Failed to detect total pages: {e}")
        sys.exit(1)

    # ── Crawl ────────────────────────────────────────────────────────────
    emitted = 0
    checked = 0
    page_num = 1
    stopped_early = False

    # When fetching detail pages: limit parallelism
    detail_lock = threading.Lock()

    for page_num in range(1, max_pages + 1):
        if emitted >= candidate_budget:
            stopped_early = True
            break

        # ── Fetch listing page ─────────────────────────────────────────
        try:
            page_videos = scrape_listing_page(session, page_num)
        except Exception as e:
            log(f"ERROR: Failed to scrape listing page {page_num}: {e}")
            # Skip this page and continue
            if page_num < max_pages:
                time.sleep(DELAY_BETWEEN_PAGES)
            continue

        checked += len(page_videos)

        # ── Filter already-seen videos ──────────────────────────────────
        unseen = [
            v for v in page_videos if v["post_id"] not in seen
        ]

        if not unseen:
            log(f"Page {page_num}/{max_pages}: {len(page_videos)} videos, "
                f"0 new (all already seen)")
            if page_num < max_pages:
                time.sleep(DELAY_BETWEEN_PAGES)
            continue

        # Only fetch detail pages for videos we can still emit
        remaining = candidate_budget - emitted
        if len(unseen) > remaining:
            unseen = unseen[:remaining]

        log(f"Page {page_num}/{max_pages}: {len(page_videos)} videos, "
            f"{len(unseen)} new → need detail pages")

        # ── Fetch detail pages (with thread pool) ───────────────────────
        def fetch_and_build(video):
            """Fetch detail page and build an item."""
            detail = scrape_detail_page(session, video["page_url"])
            video["video_url"] = detail["video_url"]
            video["cover_640x360"] = detail["cover_640x360"]
            video["upload_date"] = detail["upload_date"]
            time.sleep(DELAY_BETWEEN_VIDEOS)
            return build_item(video)

        with ThreadPoolExecutor(max_workers=DEFAULT_WORKERS) as executor:
            # Submit all tasks
            future_to_video = {
                executor.submit(fetch_and_build, v): v
                for v in unseen
            }

            # Collect results as they complete
            for future in as_completed(future_to_video):
                if emitted >= candidate_budget:
                    stopped_early = True
                    # Cancel remaining futures
                    for f in future_to_video:
                        f.cancel()
                    break

                v = future_to_video[future]
                try:
                    item = future.result()
                except Exception as e:
                    log(f"WARN: Failed detail page for "
                        f"post {v['post_id']}: {e}")
                    continue

                # Mark as seen (in-memory) and output
                with detail_lock:
                    if v["post_id"] in seen:
                        continue  # Race condition guard
                    seen.add(v["post_id"])
                    emit(item)
                    emitted += 1

        # ── Progress event ──────────────────────────────────────────────
        emit(
            {
                "type": "progress",
                "checked": checked,
                "emitted": emitted,
                "message": f"Scanned page {page_num}/{max_pages}",
            }
        )

        if emitted >= candidate_budget:
            stopped_early = True
            break

        # Brief pause between pages (be respectful)
        if page_num < max_pages:
            time.sleep(DELAY_BETWEEN_PAGES)

    # If not stopped early and there are more pages, note we exhausted them
    if not stopped_early:
        page_num = min(page_num, max_pages)

    # ── Done event ───────────────────────────────────────────────────────
    emit(
        {
            "type": "done",
            "stats": {
                "checked": checked,
                "emitted": emitted,
                "pages_scanned": page_num,
            },
        }
    )

    log(f"Job complete: checked={checked}, emitted={emitted}, "
        f"pages={page_num}/{max_pages}")


# ── Single-video test mode ─────────────────────────────────────────────────

def scrape_single_video(url):
    """
    Scrape a single video by its detail page URL (for testing).

    Outputs one item JSON to stdout.
    """
    session = create_session()

    resp = fetch_page(session, url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract from article tag
    article = soup.select_one("article.post")
    if not article:
        log("ERROR: Could not find article.post element on page")
        sys.exit(1)

    post_id = article.get("id", "").replace("post-", "")

    # Meta tags helper
    def get_meta(itemprop):
        tag = soup.select_one(f'meta[itemprop="{itemprop}"]')
        return tag.get("content", "") if tag else ""

    # Title
    title = get_meta("name")
    if not title:
        title_elem = soup.select_one("h1.entry-title")
        title = title_elem.text.strip() if title_elem else ""

    # Categories & actor
    classes = article.get("class", [])
    categories = [
        c.replace("category-", "")
        for c in classes
        if c.startswith("category-")
    ]
    actors = [
        c.replace("actors-", "") for c in classes if c.startswith("actors-")
    ]
    actor = actors[0] if actors else ""

    # Duration
    duration_str = ""
    duration_seconds = None
    iso_dur = get_meta("duration")
    if iso_dur:
        match = re.match(
            r"P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso_dur
        )
        if match:
            d, h, m, s = match.groups()
            h = int(h or 0) + int(d or 0) * 24
            m = int(m or 0)
            s = int(s or 0)
            duration_str = (
                f"{h:02d}:{m:02d}:{s:02d}"
                if h > 0
                else f"{m:02d}:{s:02d}"
            )
            duration_seconds = h * 3600 + m * 60 + s

    # Full-size cover from JSON-LD
    cover_image = ""
    jsonld = soup.select_one('script[type="application/ld+json"]')
    if jsonld:
        try:
            data = json.loads(jsonld.string)
            graph = data.get("@graph", [data])
            for item in graph:
                if (
                    item.get("@type") == "ImageObject"
                    and item.get("width", 0) > 640
                ):
                    cover_image = item.get("contentUrl", "")
                    break
        except (json.JSONDecodeError, KeyError):
            pass

    if not cover_image:
        cover_640 = get_meta("thumbnailUrl")
        cover_image = re.sub(r"-640x360(?=\.\w+$)", "", cover_640)

    video = {
        "post_id": post_id,
        "title": title,
        "video_url": get_meta("contentURL"),
        "cover_image": cover_image,
        "cover_640x360": get_meta("thumbnailUrl"),
        "duration_str": duration_str,
        "duration_seconds": duration_seconds,
        "is_hd": True,
        "page_url": url,
        "upload_date": get_meta("uploadDate"),
        "categories": categories,
        "actor": actor,
    }

    item = build_item(video)
    emit(item)
    return item


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=f"{CRAWLER_NAME} - Fapnut.net Video Crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--job", "-j",
        type=str,
        default=None,
        help="Path to job.json for crawler orchestration",
    )
    parser.add_argument(
        "--url", "-u",
        type=str,
        default=None,
        help="Scrape a single video by its detail page URL (test mode)",
    )

    args = parser.parse_args()

    if args.job:
        run_job(args.job)
    elif args.url:
        log(f"Test mode: scraping single video {args.url}")
        scrape_single_video(args.url)
    else:
        parser.print_help()
        print(
            "\n[ERROR] Specify --job (job.json) or --url (test mode)",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
        sys.exit(0)
    except BrokenPipeError:
        # stdout closed by reader - exit silently
        sys.exit(0)
