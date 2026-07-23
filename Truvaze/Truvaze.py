#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Truvaze.com 视频爬虫 - 获取 X(Twitter) 成人视频直链。
网站: https://truvaze.com
所有视频实际托管于 Twitter CDN (video.twimg.com)。

用法:
    python crawler.py --job /path/to/job.json
"""

import sys
import json
import re
import time
import os
import argparse
from datetime import datetime, timezone
from urllib.parse import urljoin, unquote, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

# ============================================================
# 爬虫名称（必须声明）
# ============================================================
CRAWLER_NAME = "Truvaze"
CRAWLER_PROTOCOL = "crawler.v2"

# ============================================================
# 常量
# ============================================================
BASE_URL = "https://truvaze.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = 30
MIN_DELAY = 1.0  # 请求间隔（秒）
MAX_DELAY = 3.0
MAX_RETRIES = 3


def log(msg: str) -> None:
    """输出日志到 stderr。"""
    print(f"[{datetime.now().isoformat()}] {msg}", file=sys.stderr, flush=True)


def emit(obj: dict) -> None:
    """输出一行 JSON 到 stdout。"""
    try:
        print(json.dumps(obj, ensure_ascii=False), flush=True)
    except BrokenPipeError:
        sys.exit(0)


def positive_int(*values, default: int) -> int:
    """Return the first positive integer from values, otherwise default."""
    for value in values:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return default


# ============================================================
# 辅助函数
# ============================================================


def clean_source_id(raw: str) -> str:
    """
    清洗 source_id：只保留字母、数字、下划线、中划线、点号。
    超过 160 字符则截断，和后端 source_id 规范保持一致。
    """
    cleaned = re.sub(r"[^a-zA-Z0-9_\-.]", "", str(raw or ""))
    if not re.search(r"[A-Za-z0-9]", cleaned):
        return ""
    if len(cleaned) > 160:
        cleaned = cleaned[:160]
    return cleaned


def deadline_reached(limits, start_mono, last_item_mono, emitted) -> bool:
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


def parse_duration(text: str) -> int | None:
    """
    将时长文本（如 "3:18", "1:35:23"）转换为秒数。
    返回 None 表示解析失败。
    """
    if not text:
        return None
    text = text.strip()
    parts = text.split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, TypeError):
        pass
    return None


def parse_int(text: str) -> int | None:
    """解析可能包含逗号或空格的数字文本。"""
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "")
    try:
        return int(text)
    except (ValueError, TypeError):
        return None


def extract_next_image_url(srcset: str) -> str | None:
    """
    从 Next.js img srcset 中提取原始 thumbnail URL。
    例如: /_next/image?url=https%3A%2F%2Fpbs.twimg.com%2F...&w=128&q=60
    """
    if not srcset:
        return None
    # 取第一个 srcset 条目（最小尺寸）
    first = srcset.split(",")[0].strip()
    # 格式: "URL 128w" → 取 URL 部分
    url_part = first.rsplit(" ", 1)[0].strip()
    if "/_next/image" in url_part:
        parsed = urlparse(url_part)
        params = parse_qs(parsed.query)
        raw_url = params.get("url", [None])[0]
        if raw_url:
            return unquote(raw_url)
    return url_part


def load_seen_ids(filepath: str | None) -> set:
    """读取已处理的 source_id 集合。"""
    seen = set()
    if filepath and os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                sid = line.strip()
                if sid:
                    seen.add(sid)
        log(f"已加载 {len(seen)} 个 seen source_id")
    return seen


# ============================================================
# 请求会话
# ============================================================


def create_session(proxy_url: str | None = None) -> requests.Session:
    """创建带代理和请求头的 requests.Session。"""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )

    if proxy_url:
        session.proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        log(f"使用代理: {proxy_url}")

    return session


def fetch(
    session: requests.Session, url: str, retries: int = MAX_RETRIES
) -> str | None:
    """GET 请求，带重试逻辑。返回 HTML 文本或 None。"""
    for attempt in range(1, retries + 1):
        try:
            log(f"GET {url} (尝试 {attempt}/{retries})")
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            # 检查是否被重定向到非 truvaze 域名（广告跳转）
            if "truvaze.com" not in resp.url and attempt < retries:
                log(f"被重定向到 {resp.url}，将重试...")
                time.sleep(1)
                continue
            return resp.text
        except requests.RequestException as e:
            log(f"请求失败: {e}")
            if attempt < retries:
                delay = MIN_DELAY * (2 ** (attempt - 1))
                log(f"等待 {delay:.1f}s 后重试...")
                time.sleep(delay)
    return None


# ============================================================
# 列表页解析
# ============================================================


def parse_listing_page(html: str, base_url: str) -> list[dict]:
    """
    解析视频列表页 HTML，返回视频基础信息列表。
    每个条目包含: video_code, thumbnail_url, duration_seconds, views, likes, detail_url
    """
    soup = BeautifulSoup(html, "html.parser")
    videos = []

    # 视频卡片在 <a> 标签中，href 包含 /movie/
    movie_links = soup.select('a[href*="/movie/"]')
    seen_codes = set()

    for link in movie_links:
        href = link.get("href", "")
        if not href:
            continue

        # 提取 video_code
        match = re.search(r"/movie/([^/?#]+)", href)
        if not match:
            continue
        video_code = match.group(1)
        # 跳过不是 video_code 格式的链接（如纯数字等）
        if not re.match(r"^[a-zA-Z0-9_\-]+$", video_code):
            continue
        if video_code in seen_codes:
            continue
        seen_codes.add(video_code)

        # 找到包含此链接的卡片容器
        # 卡片结构: div.bg-white > div.relative > a > div > img
        card = link
        for _ in range(4):
            parent = card.parent
            if parent is None:
                break
            card = parent
            if card.name == "div" and "bg-white" in (card.get("class") or []):
                break

        # 提取缩略图 - 从 img srcset 中获取
        img = link.find("img")
        thumbnail_url = None
        if img:
            srcset = img.get("srcset", "")
            thumbnail_url = extract_next_image_url(srcset)
            if not thumbnail_url:
                thumbnail_url = img.get("src", "")

        # 提取时长 - 在卡片内的 absolute positioned div 中
        duration_el = card.select_one(".absolute.bottom-2")
        duration_text = duration_el.get_text(strip=True) if duration_el else None
        duration_seconds = parse_duration(duration_text)

        # 提取浏览数和点赞数
        views = None
        likes = None
        eye_icons = card.select('img[alt="閲覧数"], img[alt="view"]')
        if eye_icons:
            views_span = eye_icons[0].parent
            if views_span:
                views_text = views_span.get_text(strip=True)
                views = parse_int(views_text)
        heart_icons = card.select(
            'img[alt="お気に入り"], img[alt="favorite"], img[alt="like"]'
        )
        if heart_icons:
            # 点赞数在 span 中
            like_btn = heart_icons[0].parent
            if like_btn:
                like_span = like_btn.find("span")
                likes_text = (
                    like_span.get_text(strip=True)
                    if like_span
                    else like_btn.get_text(strip=True)
                )
                likes = parse_int(likes_text)

        detail_url = urljoin(base_url, href)

        videos.append(
            {
                "video_code": video_code,
                "thumbnail_url": thumbnail_url,
                "duration_seconds": duration_seconds,
                "views": views,
                "likes": likes,
                "detail_url": detail_url,
            }
        )

    return videos


# ============================================================
# 详情页解析
# ============================================================


def parse_detail_page(html: str) -> dict | None:
    """
    解析视频详情页 HTML，提取直链和标签。
    返回 {'media_url': ..., 'tags': [...], 'category': ...} 或 None。
    """
    soup = BeautifulSoup(html, "html.parser")

    # 提取视频直链
    video_link = soup.select_one('a[href*="video.twimg.com"]')
    if not video_link:
        # 尝试更宽松的匹配
        video_link = soup.find("a", href=re.compile(r"video\.twimg\.com"))
    media_url = video_link.get("href") if video_link else None

    # 提取标签/分类
    tags = []
    category_links = soup.select('a[href*="/category/"]')
    for a in category_links:
        tag_text = a.get_text(strip=True).lstrip("#")
        if tag_text:
            tags.append(tag_text)

    # 提取分类（第一个标签可作为分类）
    category = tags[0] if tags else None

    # 提取描述 (meta description)
    desc_meta = soup.select_one('meta[name="description"]')
    description = desc_meta.get("content", "") if desc_meta else ""

    return {
        "media_url": media_url,
        "tags": tags,
        "category": category,
        "description": description,
    }


# ============================================================
# 主爬取逻辑
# ============================================================


def crawl(job: dict) -> None:
    """主爬取入口。"""
    run_id = job.get("run_id", "unknown")
    candidate_budget = positive_int(
        job.get("candidate_budget"),
        job.get("target_new"),
        default=10,
    )
    unique_target = positive_int(job.get("unique_target"), default=0)
    seen_file = job.get("seen_source_ids_file")
    config = job.get("config") if isinstance(job.get("config"), dict) else {}
    limits = job.get("limits") if isinstance(job.get("limits"), dict) else {}
    progress_interval = positive_int(
        limits.get("progress_interval_seconds"), default=60
    )

    # 可配置项
    sort_order = config.get("sort", "add")  # add=最新, favorite=点赞, view=观看数
    time_filter = config.get("time_filter", "")  # ''=日, weekly, monthly, all
    max_pages = positive_int(config.get("max_pages"), default=20)

    log(f"=== 开始爬取 Truvaze ===")
    log(
        f"run_id={run_id}, unique_target={unique_target or 'unknown'}, "
        f"candidate_budget={candidate_budget}, sort={sort_order}, "
        f"time_filter={time_filter or 'daily'}"
    )

    seen = load_seen_ids(seen_file)

    proxy_url = (job.get("network") or {}).get("proxy_url")
    session = create_session(proxy_url)

    if time_filter:
        list_path = f"/zh-CN/{time_filter}"
    else:
        list_path = "/zh-CN"

    emitted = 0
    checked = 0
    page = 1
    start_mono = time.monotonic()
    last_item_mono = start_mono
    last_progress_mono = start_mono

    def maybe_progress(message: str = ""):
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

    while emitted < candidate_budget and page <= max_pages:
        if deadline_reached(limits, start_mono, last_item_mono, emitted):
            log("达到 job limits 截止条件，停止")
            break

        list_url = urljoin(BASE_URL, f"{list_path}?sort={sort_order}&page={page}")
        maybe_progress(f"正在扫描第 {page} 页 (sort={sort_order})")

        html = fetch(session, list_url)
        if not html:
            log(f"第 {page} 页加载失败，跳过")
            page += 1
            time.sleep(MIN_DELAY)
            continue

        videos = parse_listing_page(html, list_url)
        log(f"第 {page} 页解析到 {len(videos)} 个视频")

        if not videos:
            log("没有更多视频，停止翻页")
            break

        for v in videos:
            if emitted >= candidate_budget:
                break
            if deadline_reached(limits, start_mono, last_item_mono, emitted):
                break

            video_code = v["video_code"]
            source_id = clean_source_id(video_code)

            checked += 1
            if not source_id:
                continue

            if source_id in seen:
                log(f"跳过 seen: {source_id}")
                maybe_progress()
                continue

            time.sleep(MIN_DELAY + (emitted % 3) * 0.5)
            detail_html = fetch(session, v["detail_url"])
            if not detail_html:
                log(f"详情页加载失败: {video_code}")
                detail = {}
            else:
                detail = parse_detail_page(detail_html) or {}

            media_url = detail.get("media_url")
            if not media_url:
                log(f"未找到直链: {video_code}，跳过")
                seen.add(source_id)
                maybe_progress()
                continue

            tags = detail.get("tags", [])
            title = video_code

            item = {
                "type": "item",
                "source_id": source_id,
                "title": title,
                "media_url": media_url,
                "detail_url": v["detail_url"],
                "tags": tags,
                "duration_seconds": v.get("duration_seconds"),
                "description": detail.get("description", ""),
                "media_headers": {
                    "User-Agent": USER_AGENT,
                    "Referer": "https://x.com/",
                    "Origin": "https://x.com",
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                },
            }

            if v.get("thumbnail_url"):
                item["thumbnail_url"] = v["thumbnail_url"]

            stats = {}
            if v.get("views"):
                stats["views"] = v["views"]
            if v.get("likes"):
                stats["likes"] = v["likes"]
            if stats:
                stats_str = " | ".join(f"{k}: {val}" for k, val in stats.items())
                if item.get("description"):
                    item["description"] = f"{stats_str} | {item['description']}"
                else:
                    item["description"] = stats_str

            emit(item)
            emitted += 1
            seen.add(source_id)
            last_item_mono = time.monotonic()
            last_progress_mono = last_item_mono
            log(f"[{emitted}/{candidate_budget}] 输出候选: {video_code}")

        page += 1

    emit({"type": "done", "stats": {"emitted": emitted, "checked": checked}})
    log(f"=== 爬取完成: 检查 {checked} 个，输出 {emitted} 个 ===")


# ============================================================
# 命令行入口
# ============================================================


def main():
    parser = argparse.ArgumentParser(description=f"{CRAWLER_NAME} - --job 模式爬虫")
    parser.add_argument("--job", type=str, required=True, help="job.json 文件路径")
    args = parser.parse_args()

    # 读取 job.json
    job_path = args.job
    log(f"读取 job 配置: {job_path}")
    try:
        with open(job_path, "r", encoding="utf-8") as f:
            job = json.load(f)
    except Exception as e:
        log(f"无法读取 job.json: {e}")
        sys.exit(1)

    protocol = job.get("protocol", "")
    if protocol != CRAWLER_PROTOCOL:
        log(f"错误: protocol 不是 {CRAWLER_PROTOCOL}，而是 {protocol}")
        sys.exit(1)
    if job.get("mode") not in ("", None, "crawl"):
        log(f"错误: 不支持的 mode: {job.get('mode')!r}")
        sys.exit(1)

    try:
        crawl(job)
    except (KeyboardInterrupt, BrokenPipeError):
        sys.exit(0)
    except Exception as e:
        log(f"爬取异常: {e}")
        import traceback

        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
