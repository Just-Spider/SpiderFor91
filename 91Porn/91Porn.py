#!/usr/bin/env python3
"""
脚本名称: 91Porn
用途: 爬取 91Porn 列表中的视频标题、视频下载直链、封面图直链和唯一标识，
并按 crawler.v3 协议响应项目的 discover、resolve 和 stop 命令。

默认抓取本月最热(category=top)。
任务通过 feed_id 选择栏目；独立运行时仍可使用 --category。
"""

CRAWLER_NAME = "91Porn"
CRAWLER_PROTOCOL = "crawler.v3"
CRAWLER_FEEDS = '[{"id":"top","label":"本月最热","default":true},{"id":"new","label":"最新"}]'

import argparse
import requests
import re
import time
import random
import json
import os
import socket
import sys
import html
import traceback
import unicodedata
import queue
import threading
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from datetime import datetime, timezone
from urllib3.util import Timeout

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("错误: 缺少依赖库 beautifulsoup4", file=sys.stderr)
    print("请运行: pip install beautifulsoup4 lxml", file=sys.stderr)
    sys.exit(1)


def prefer_ipv4_for_plain_socks5_proxy():
    proxy_envs = (
        os.environ.get("HTTPS_PROXY", ""),
        os.environ.get("HTTP_PROXY", ""),
        os.environ.get("https_proxy", ""),
        os.environ.get("http_proxy", ""),
    )
    uses_plain_socks5 = any(v.strip().lower().startswith("socks5://") for v in proxy_envs)
    if not uses_plain_socks5 or getattr(socket, "_spider91_ipv4_first", False):
        return

    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4_first(*args, **kwargs):
        infos = original_getaddrinfo(*args, **kwargs)
        return sorted(infos, key=lambda info: 0 if info[0] == socket.AF_INET else 1)

    socket.getaddrinfo = getaddrinfo_ipv4_first
    socket._spider91_ipv4_first = True

BASE_URL = "https://www.91porn.com/v.php"
LIST_PARAMS = {
    "category": "top",
    "viewtype": "basic"
}
DEFAULT_CATEGORY = "top"
FEED_CATEGORIES = {"top": "top", "new": "mr"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

MIN_PAGE_DELAY = 3.0
MAX_PAGE_DELAY = 6.0
MIN_DETAIL_DELAY = 2.0
MAX_DETAIL_DELAY = 5.0

MAX_RETRIES = 3
RETRY_DELAY = 5.0

OUTPUT_FILE = "91porn_videos.json"
MAX_PAGES = None
RESUME = True
MAX_EMPTY_PAGES = 2


def decode_strencode2(value: str) -> str:
    """源站 m2.js 使用 unescape，按 UTF-16 码元解码 %XX 和 %uXXXX。"""
    decoded = re.sub(
        r"%u([0-9A-Fa-f]{4})|%([0-9A-Fa-f]{2})",
        lambda match: chr(int(match.group(1) or match.group(2), 16)),
        value,
    )
    return decoded.encode("utf-16-le", errors="surrogatepass").decode("utf-16-le", errors="replace")


def write_jsonl(event: dict):
    try:
        print(json.dumps(event, ensure_ascii=False), flush=True)
    except BrokenPipeError:
        sys.exit(0)


class Porn91Spider:
    def __init__(
        self,
        output_file: str = None,
        start_page: int = 1,
        max_pages: int = None,
        resume: bool = None,
        max_empty_pages: int = None,
        quiet: bool = False,
        target_new: int = None,
        seen_viewkeys: list = None,
        stream_output: bool = False,
        category: str = DEFAULT_CATEGORY,
        automatic_retries: bool = True,
    ):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.session.cookies.set("mode", "d")

        self.output_file = output_file if output_file is not None else OUTPUT_FILE
        self.start_page = max(1, int(start_page or 1))
        self.max_pages = max_pages if max_pages is None or max_pages > 0 else None
        self.resume = RESUME if resume is None else bool(resume)
        self.max_empty_pages = (
            MAX_EMPTY_PAGES if max_empty_pages is None else int(max_empty_pages)
        )
        self.target_new = target_new if target_new and target_new > 0 else None
        self.quiet = bool(quiet)
        self.stream_output = bool(stream_output)
        self.category = (
            category.strip()
            if isinstance(category, str) and category.strip()
            else DEFAULT_CATEGORY
        )

        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            retry_strategy = Retry(
                total=MAX_RETRIES if automatic_retries else 0,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        except ImportError:
            pass

        self.results = []
        self.pages_crawled = 0
        self.processed_videos = 0
        self.skipped_videos = 0
        self.failed_videos = 0
        self.checked = 0
        self.emitted = 0
        self.skip_viewkeys = set()
        self._last_progress_at = time.monotonic()
        self._start_monotonic = time.monotonic()
        self._last_item_at = time.monotonic()
        self.limits = {}

        if seen_viewkeys:
            for vk in seen_viewkeys:
                if not vk:
                    continue
                vk = vk.strip()
                if vk:
                    self.skip_viewkeys.add(vk)

        if self.resume and os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                existing_videos = existing_data.get('videos', [])
                self.results = existing_videos
                for v in existing_videos:
                    vk = v.get('viewkey', '')
                    if vk:
                        self.skip_viewkeys.add(vk)
                self.processed_videos = existing_data.get('successful', 0)
                self.failed_videos = existing_data.get('failed', 0)
                self.log(f"加载已有数据: {len(self.results)} 个视频, 将跳过已处理项")
            except Exception:
                pass

    def log(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}"
        if self.stream_output:
            print(line, file=sys.stderr, flush=True)
        else:
            print(line)

    def _deadline_reached(self) -> bool:
        limits = self.limits or {}
        max_runtime = limits.get("max_runtime_seconds")
        if max_runtime:
            try:
                if time.monotonic() - self._start_monotonic >= float(max_runtime):
                    return True
            except (TypeError, ValueError):
                pass
        deadline_at = limits.get("deadline_at")
        if deadline_at:
            try:
                # Accept trailing Z
                text = str(deadline_at).replace("Z", "+00:00")
                deadline = datetime.fromisoformat(text)
                if deadline.tzinfo is None:
                    now = datetime.utcnow()
                    return now >= deadline
                from datetime import timezone
                return datetime.now(timezone.utc) >= deadline.astimezone(timezone.utc)
            except Exception:
                pass
        idle = limits.get("candidate_idle_timeout_seconds")
        if idle and self.emitted == 0:
            try:
                if time.monotonic() - self._start_monotonic >= float(idle):
                    return True
            except (TypeError, ValueError):
                pass
        if idle and self.emitted > 0:
            try:
                if time.monotonic() - self._last_item_at >= float(idle):
                    return True
            except (TypeError, ValueError):
                pass
        return False

    def _maybe_progress(self, message: str = ""):
        if not self.stream_output:
            return
        interval = 60
        try:
            interval = int((self.limits or {}).get("progress_interval_seconds") or 60)
        except (TypeError, ValueError):
            interval = 60
        if interval <= 0:
            interval = 60
        now = time.monotonic()
        if now - self._last_progress_at < interval and not message:
            return
        write_jsonl({
            "type": "progress",
            "checked": self.checked,
            "emitted": self.emitted,
            "message": message or f"checked={self.checked} emitted={self.emitted}",
        })
        self._last_progress_at = now

    def emit_stream_video(self, video: dict) -> bool:
        if not self.stream_output:
            return False
        try:
            write_jsonl(video)
            self.emitted += 1
            self._last_item_at = time.monotonic()
            self._last_progress_at = time.monotonic()
            return True
        except BrokenPipeError:
            sys.exit(0)
        except Exception as e:
            print(f"[stream] emit failed: {e}", file=sys.stderr, flush=True)
            return False

    def random_sleep(self, min_sec: float, max_sec: float):
        delay = random.uniform(min_sec, max_sec)
        if not self.quiet:
            self.log(f"  随机延时 {delay:.2f} 秒...")
        time.sleep(delay)

    def fetch_page(self, url: str, description: str = "", referer: str = "") -> str:
        headers_extra = {}
        if referer:
            headers_extra["Referer"] = referer

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.log(f"正在请求: {description or url} (尝试 {attempt}/{MAX_RETRIES})")
                response = self.session.get(url, timeout=30, headers=headers_extra)

                if response.status_code == 403:
                    self.log("警告: 收到 403 Forbidden，可能被拦截")
                    if attempt < MAX_RETRIES:
                        self.random_sleep(RETRY_DELAY, RETRY_DELAY + 3)
                        continue
                    return ""

                response.raise_for_status()

                try:
                    html_content = response.content.decode('utf-8', errors='replace')
                except Exception:
                    html_content = response.text

                is_cf_challenge = (
                    "Just a moment" in html_content and
                    len(html_content) < 8000
                )
                if is_cf_challenge:
                    self.log("警告: 页面被Cloudflare挑战拦截，需要浏览器环境或正确cookie")
                    if attempt < MAX_RETRIES:
                        self.random_sleep(RETRY_DELAY, RETRY_DELAY + 5)
                        continue
                    return ""

                return html_content
            except requests.exceptions.HTTPError as e:
                self.log(f"HTTP错误: {e}")
                if attempt < MAX_RETRIES:
                    self.random_sleep(RETRY_DELAY, RETRY_DELAY + 3)
                else:
                    return ""
            except requests.exceptions.RequestException as e:
                self.log(f"请求失败: {e}")
                if attempt < MAX_RETRIES:
                    self.random_sleep(RETRY_DELAY, RETRY_DELAY + 3)
                else:
                    self.log(f"达到最大重试次数，放弃: {url}")
                    return ""
        return ""

    def parse_list_page(self, html: str, base_url: str = BASE_URL) -> list:
        videos = []
        soup = BeautifulSoup(html, 'lxml')

        video_cards = (soup.select('div.col-xs-12.col-sm-4.col-md-3.col-lg-3')
                       or soup.select('a[href*="view_video.php"]'))

        seen_cards = set()

        for card in video_cards:
            link = card if card.name == 'a' else card.find(
                'a', href=re.compile(r'view_video\.php\?'))
            if not link:
                continue
            href = link.get('href', '')
            if not href:
                continue

            viewkeys = parse_qs(urlparse(href).query).get('viewkey', [])
            if not viewkeys or not viewkeys[0]:
                continue
            viewkey = viewkeys[0]

            detail_url = urljoin(base_url, href)

            title = self._extract_title(link)

            thumb_url = ""
            source_id = ""
            overlay = link.find(id=re.compile(r'^playvthumb_\d+$'))
            if overlay:
                source_id = overlay.get('id', '').rsplit('_', 1)[-1]
            img = link.find('img', class_=re.compile(r'img-responsive'))
            if img:
                thumb_url = img.get('src', '') or img.get('data-original', '')
                if thumb_url:
                    thumb_url = urljoin(base_url, thumb_url)
            if not source_id and thumb_url:
                source_id = self._extract_thumb_source_id(thumb_url)

            card_key = viewkey
            if card_key in seen_cards:
                continue
            seen_cards.add(card_key)

            videos.append({
                "title": title,
                "detail_url": detail_url,
                "thumb_url": thumb_url,
                "viewkey": viewkey,
                "source_id": source_id
            })

        return videos

    def _extract_title(self, link) -> str:
        title_el = link.find('span', class_=re.compile(r'video-title'))
        if title_el:
            title = title_el.get_text(strip=True)
            if title:
                return html.unescape(title)

        title = link.get('title', '').strip()
        if title:
            return html.unescape(title)

        text = link.get_text(separator=' ', strip=True)
        text = re.sub(r'^(HD\s+|91\s+)?\d{2}:\d{2}:\d{2}\s*', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return html.unescape(text)[:120]

    def parse_detail_page(self, html_text: str, base_url: str = BASE_URL) -> dict:
        result = {}

        if not html_text:
            return result

        title = self._extract_detail_title(html_text)
        if title:
            result["title"] = title

        soup = BeautifulSoup(html_text, 'lxml')
        video = soup.find('video')
        image = soup.find('meta', property='og:image')
        thumb_url = (video.get('poster', '') if video else '') or (
            image.get('content', '') if image else '')
        if thumb_url:
            result["thumb_url"] = urljoin(base_url, html.unescape(thumb_url))

        strencode_match = re.search(r'strencode2\s*\(\s*["\']([^"\']+)["\']\s*\)', html_text)
        if strencode_match:
            encoded = strencode_match.group(1)
            try:
                decoded = decode_strencode2(encoded)

                src_match = re.search(r"src=['\"]([^'\"]+)['\"]", decoded)
                if src_match:
                    video_url = urljoin(base_url, html.unescape(src_match.group(1)))
                    video_url = re.sub(r'(https?://[^/]+)//+', r'\1/', video_url)
                    result["video_url"] = video_url
                    result["source_id"] = self._extract_source_id(video_url)
                    return result
            except Exception as e:
                self.log(f"  解码 strencode2 失败: {e}")

        source = soup.select_one('video source[src], video[src], source[src]')
        if source:
            video_url = urljoin(base_url, html.unescape(source.get('src', '')))
            if video_url:
                result["video_url"] = video_url
                result["source_id"] = self._extract_source_id(video_url)
                return result

        mp4_match = re.search(
            r'https?://[^\s"\'<>]+\.(?:mp4|m3u8)[^\s"\'<>]*',
            html_text
        )
        if mp4_match:
            url = mp4_match.group(0)
            if 'kwai' not in url and 'ad-' not in url.lower():
                result["video_url"] = html.unescape(url)
                result["source_id"] = self._extract_source_id(url)
                return result

        return result

    def _extract_detail_title(self, html_text: str) -> str:
        soup = BeautifulSoup(html_text, 'lxml')
        title_el = soup.find('title')
        if not title_el:
            return ""
        title = title_el.get_text(" ", strip=True)
        title = re.sub(r'\s*-\s*91porn.*$', '', title, flags=re.IGNORECASE).strip()
        return html.unescape(title)[:160]

    def _extract_source_id(self, video_url: str) -> str:
        path = urlparse(video_url or "").path
        name = os.path.basename(path)
        stem, ext = os.path.splitext(name)
        if ext.lower() not in {".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi"}:
            return ""
        source_id = re.sub(r'[^0-9]+', '', stem)
        if not source_id or source_id != stem:
            return ""
        return source_id

    def _extract_thumb_source_id(self, thumb_url: str) -> str:
        path = urlparse(thumb_url or "").path
        match = re.search(r'/thumb/(\d+)\.[A-Za-z0-9]+$', path)
        return match.group(1) if match else ""

    def _thumb_url_for_source(self, thumb_url: str, source_id: str) -> str:
        if not thumb_url or not source_id:
            return thumb_url
        parsed = urlparse(thumb_url)
        match = re.search(r'/thumb/([^/?#]+)\.[A-Za-z0-9]+$', parsed.path)
        if not match:
            return thumb_url
        current = match.group(1)
        if current == source_id:
            return thumb_url
        path = re.sub(
            r'/thumb/[^/?#]+\.[A-Za-z0-9]+$',
            f'/thumb/{source_id}.jpg',
            parsed.path,
        )
        return parsed._replace(path=path, query="", fragment="").geturl()

    def crawl(self):
        self.log("=" * 60)
        self.log("91porn 视频爬虫启动")
        self.log("=" * 60)
        self.log(f"配置: 列表页延时 {MIN_PAGE_DELAY}-{MAX_PAGE_DELAY}s, 详情页延时 {MIN_DETAIL_DELAY}-{MAX_DETAIL_DELAY}s")
        self.log(f"配置: 最大重试 {MAX_RETRIES} 次, 连续空页上限 {self.max_empty_pages}")
        self.log(f"配置: 起始页 {self.start_page}, 最大爬取页数 {self.max_pages if self.max_pages else '不限'}")
        if self.target_new:
            self.log(f"配置: 目标新增视频数 {self.target_new}")
        self.log(f"配置: 输出文件 {os.path.abspath(self.output_file)}")
        if self.skip_viewkeys:
            self.log(f"配置: 已跳过 {len(self.skip_viewkeys)} 个已知 viewkey")
        self.log(f"配置: 分类 category={self.category}")
        self.log("")

        page_num = self.start_page
        consecutive_empty = 0
        crawled_in_session = 0

        while True:
            if self.max_pages is not None and crawled_in_session >= self.max_pages:
                self.log(f"达到配置的页数上限 {self.max_pages}，停止")
                break
            if consecutive_empty >= self.max_empty_pages:
                self.log(f"连续 {self.max_empty_pages} 页无结果，已达到末尾")
                break
            if self.target_new is not None and self.processed_videos >= self.target_new:
                self.log(f"已累计 {self.processed_videos} 个新视频，达到目标 {self.target_new}，停止")
                break
            if self.stream_output and self.target_new is not None and self.emitted >= self.target_new:
                self.log(f"已输出 {self.emitted} 个候选，达到 budget，停止")
                break
            if self._deadline_reached():
                self.log("达到 job limits 截止条件，停止")
                break

            base_url = f"{BASE_URL}?category={self.category}&viewtype=basic"

            if page_num == 1:
                page_url = base_url
            else:
                page_url = f"{base_url}&page={page_num}"

            if crawled_in_session > 0:
                self.log("")
                self.random_sleep(MIN_PAGE_DELAY, MAX_PAGE_DELAY)

            self.log(f"[页 {page_num}] 请求: {page_url}")
            page_html = self.fetch_page(page_url, f"列表页 第{page_num}页")

            if not page_html:
                self.log(f"[页 {page_num}] 获取失败，跳过")
                consecutive_empty += 1
                page_num += 1
                crawled_in_session += 1
                self._maybe_progress(f"list page {page_num - 1} fetch failed")
                continue

            page_videos = self.parse_list_page(page_html)

            if not page_videos:
                self.log(f"[页 {page_num}] 页面无视频，可能已到末尾")
                consecutive_empty += 1
                page_num += 1
                crawled_in_session += 1
                self._maybe_progress(f"empty list page {page_num - 1}")
                continue

            consecutive_empty = 0

            new_videos = [v for v in page_videos if v['viewkey'] not in self.skip_viewkeys]
            skipped_on_page = len(page_videos) - len(new_videos)

            if skipped_on_page > 0:
                self.log(f"[页 {page_num}] 发现 {len(page_videos)} 个链接, 其中 {skipped_on_page} 个已处理, {len(new_videos)} 个新视频")
            else:
                self.log(f"[页 {page_num}] 发现 {len(new_videos)} 个视频")

            if new_videos:
                self._process_video_list(new_videos, referer=page_url)
            self.pages_crawled += 1
            self._maybe_progress(f"Scanning page {page_num}")
            page_num += 1
            crawled_in_session += 1

        self._save_results()
        self._print_summary()

    def _process_video_list(self, videos: list, referer: str = ""):
        for idx, video in enumerate(videos, 1):
            if self.target_new is not None and self.processed_videos >= self.target_new:
                return
            if self.stream_output and self.target_new is not None and self.emitted >= self.target_new:
                return
            if self._deadline_reached():
                return
            if video['viewkey'] in self.skip_viewkeys:
                self.log(f"  [SKIP] 已处理过: {video['viewkey']}")
                self.skipped_videos += 1
                self.checked += 1
                self._maybe_progress()
                continue

            self.checked += 1
            self.log(f"  处理视频 {idx}/{len(videos)}: {video['title'][:40]}...")

            if idx > 1:
                self.random_sleep(MIN_DETAIL_DELAY, MAX_DETAIL_DELAY)

            detail_html = self.fetch_page(video['detail_url'], f"详情页 viewkey={video['viewkey']}", referer=referer)

            if not detail_html:
                self.log(f"  [FAIL] 详情页获取失败: {video['viewkey']}")
                video["video_url"] = ""
                self.results.append(video)
                self.skip_viewkeys.add(video['viewkey'])
                self.failed_videos += 1
                self._maybe_progress()
                continue

            detail_info = self.parse_detail_page(detail_html)

            if detail_info.get("video_url"):
                video["video_url"] = detail_info["video_url"]
                if detail_info.get("title"):
                    video["title"] = detail_info["title"]
                list_source_id = video.get("source_id", "")
                detail_source_id = detail_info.get("source_id", "")
                if list_source_id and detail_source_id and list_source_id != detail_source_id:
                    self.log(
                        f"  [FAIL] 详情页视频源不匹配: list_source_id={list_source_id} "
                        f"detail_source_id={detail_source_id} viewkey={video['viewkey']}"
                    )
                    self.failed_videos += 1
                    self.skip_viewkeys.add(video['viewkey'])
                    self._maybe_progress()
                    continue
                if not list_source_id and detail_source_id:
                    video["source_id"] = detail_source_id
                if video.get("source_id"):
                    video["thumb_url"] = self._thumb_url_for_source(
                        video.get("thumb_url", ""),
                        video["source_id"],
                    )
                    if video["source_id"] in self.skip_viewkeys:
                        self.log(f"  [SKIP] 已处理过 source_id: {video['source_id']}")
                        self.skipped_videos += 1
                        self._maybe_progress()
                        continue
                self.results.append(video)
                self.skip_viewkeys.add(video['viewkey'])
                if video.get("source_id"):
                    self.skip_viewkeys.add(video["source_id"])
                self.processed_videos += 1
                self.log(f"  [OK] 成功提取视频直链")
                self.emit_stream_video(video)
                self._maybe_progress()
            else:
                self.log(f"  [FAIL] 未找到视频直链: {video['viewkey']}")
                video["video_url"] = ""
                self.results.append(video)
                self.skip_viewkeys.add(video['viewkey'])
                self.failed_videos += 1
                self._maybe_progress()

    def _save_results(self):
        output_data = {
            "crawl_time": datetime.now().isoformat(),
            "source_url": BASE_URL,
            "pages_crawled": self.pages_crawled,
            "total_videos": len(self.results),
            "successful": self.processed_videos,
            "skipped": self.skipped_videos,
            "failed": self.failed_videos,
            "videos": self.results
        }

        try:
            out_path = self.output_file
            parent = os.path.dirname(os.path.abspath(out_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            tmp_path = out_path + ".part"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, out_path)
            self.log(f"结果已保存到: {os.path.abspath(out_path)}")
        except Exception as e:
            self.log(f"保存文件失败: {e}")
            backup_out = sys.stderr if self.stream_output else sys.stdout
            print("\n--- 备份输出 ---", file=backup_out, flush=True)
            print(json.dumps(output_data, ensure_ascii=False, indent=2), file=backup_out, flush=True)

    def _print_summary(self):
        self.log("")
        self.log("=" * 60)
        self.log("爬取完成!")
        self.log("=" * 60)
        self.log(f"爬取页数: {self.pages_crawled}")
        self.log(f"总视频数: {len(self.results)}")
        self.log(f"成功提取直链: {self.processed_videos}")
        self.log(f"跳过(已处理): {self.skipped_videos}")
        self.log(f"失败/缺失直链: {self.failed_videos}")
        self.log(f"输出文件: {os.path.abspath(self.output_file)}")
        self.log("=" * 60)


def utf8_prefix(value: str, max_bytes: int) -> str:
    return value.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")


def protocol_identifier(value, name: str) -> str:
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value.encode("utf-8")) > 512
            or any(unicodedata.category(char) == "Cc" for char in value)):
        raise ValueError(f"{name} 必须是 1–512 字节且不含控制字符或首尾空白的字符串")
    return value


def http_url(value) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 8192:
        raise ValueError("URL 必须是最多 8192 字节的 HTTP(S) 地址")
    parsed = urlparse(value)
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or any(char.isspace() or unicodedata.category(char) == "Cc" for char in value)):
        raise ValueError("URL 必须是无内嵌凭据的绝对 HTTP(S) 地址")
    parsed.port  # 同时检查端口格式。
    return value


def protocol_headers(headers) -> dict:
    if not isinstance(headers, dict) or len(headers) > 64:
        raise ValueError("headers 必须是最多 64 项的对象")
    for name, value in headers.items():
        if (not isinstance(name, str) or len(name.encode("utf-8")) > 256
                or not re.fullmatch(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+", name)
                or not isinstance(value, str) or len(value.encode("utf-8")) > 8192
                or any(char in value for char in "\r\n\0")):
            raise ValueError("请求头名称或值不符合协议")
    return headers


class SourceError(Exception):
    def __init__(self, scope, code, message, retryable=False, retry_after=None):
        super().__init__(message)
        self.fields = {
            "scope": scope, "code": code,
            "message": utf8_prefix(str(message), 8192) or "请求失败",
            "retryable": bool(retryable),
        }
        if retry_after is not None:
            self.fields["retry_after_seconds"] = max(0, min(86400, int(retry_after)))


class CommandDeadline:
    """用单调时钟跟踪命令预算，预留最终 JSON 响应的时间。"""
    def __init__(self, deadline_at, scope):
        deadline = datetime.fromisoformat(deadline_at.replace("Z", "+00:00"))
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            raise ValueError("deadline_at 必须包含时区")
        seconds = (deadline - datetime.now(timezone.utc)).total_seconds()
        self.expires_at = time.monotonic() + seconds - 0.05
        self.scope = scope
        self.cancelled = threading.Event()

    def remaining(self):
        seconds = self.expires_at - time.monotonic()
        if self.cancelled.is_set() or seconds <= 0:
            raise SourceError(self.scope, "source_unavailable", "命令截止时间已到", True)
        return seconds

    def timeout(self):
        return min(30.0, self.remaining())


def run_before_deadline(operation, deadline, *args):
    # requests 的读取超时不限制持续分块传输的总时长；主线程额外限制整个命令。
    outcomes = queue.Queue(maxsize=1)

    def execute():
        try:
            outcomes.put((operation(*args, deadline), None))
        except Exception as error:
            outcomes.put((None, error))

    deadline.remaining()
    worker = threading.Thread(target=execute, daemon=True)
    worker.start()
    try:
        result, error = outcomes.get(timeout=deadline.remaining())
    except queue.Empty:
        deadline.cancelled.set()
        raise SourceError(deadline.scope, "source_unavailable", "命令截止时间已到", True)
    deadline.remaining()
    if error is not None:
        raise error
    return result


class Porn91ProtocolCrawler:
    def __init__(self, job):
        self.feed_id = job["feed_id"]
        self.category = FEED_CATEGORIES[self.feed_id]
        config = job.get("config", {})
        self.base_url = http_url(config.get("base_url", "https://www.91porn.com")).rstrip("/") + "/"
        self.list_url = urljoin(self.base_url, "v.php")
        self.custom_headers = protocol_headers(config.get("headers", {}))
        self.max_pages = config.get("max_pages")
        if self.max_pages is not None and (type(self.max_pages) is not int or self.max_pages < 1):
            raise ValueError("config.max_pages 必须是正整数")
        self.spider = Porn91Spider(
            resume=False, quiet=True, stream_output=True,
            category=self.category, automatic_retries=False,
        )
        self.session = self.spider.session
        self.session.headers.update(self.custom_headers)
        self.session.cookies.clear()
        self.session.cookies.set("mode", "d", domain=urlparse(self.base_url).hostname, path="/")
        proxy_url = job.get("network", {}).get("proxy_url")
        if proxy_url:
            self.session.proxies.update({"http": proxy_url, "https": proxy_url})
        self.pages = {}
        self.metadata = {}

    def page_url(self, page):
        return self.list_url + "?" + urlencode({
            "category": self.category, "viewtype": "basic", "page": page,
        })

    @staticmethod
    def retry_after(value):
        if not value:
            return None
        try:
            return max(0, min(86400, int(value)))
        except (TypeError, ValueError):
            try:
                date = parsedate_to_datetime(value)
                return max(0, min(86400, int((date - datetime.now(timezone.utc)).total_seconds())))
            except (TypeError, ValueError, OverflowError):
                return None

    def fetch_page(self, url, deadline, scope, referer):
        # 每次抓取使用独立 Session，过期命令不会把迟到的 Cookie 写入后续命令。
        with requests.Session() as session:
            session.headers.update(self.session.headers)
            session.cookies.update(self.session.cookies)
            session.proxies.update(self.session.proxies)
            session.trust_env = self.session.trust_env
            for _ in range(11):
                deadline.remaining()
                try:
                    with session.get(
                        http_url(url), headers={"Referer": referer},
                        timeout=Timeout(total=deadline.timeout()),
                        allow_redirects=False, stream=True,
                    ) as response:
                        deadline.remaining()
                        status = response.status_code
                        if status in {301, 302, 303, 307, 308}:
                            location = response.headers.get("Location")
                            if not location:
                                raise SourceError(scope, "parse_failed", "重定向缺少 Location")
                            url = http_url(urljoin(url, location))
                            continue
                        if status >= 400:
                            code = {401: "auth_required", 403: "auth_required",
                                    404: "not_found", 410: "not_found",
                                    429: "rate_limited"}.get(status, "source_unavailable")
                            error_scope = "source" if code in {
                                "auth_required", "rate_limited", "source_unavailable",
                            } else scope
                            raise SourceError(
                                error_scope, code, f"源站返回 HTTP {status}",
                                code in {"rate_limited", "source_unavailable"},
                                self.retry_after(response.headers.get("Retry-After"))
                                if code in {"rate_limited", "source_unavailable"} else None,
                            )
                        chunks = []
                        size = 0
                        for chunk in response.iter_content(chunk_size=65536):
                            deadline.remaining()
                            size += len(chunk)
                            if size > 8 * 1024 * 1024:
                                raise SourceError(scope, "parse_failed", "源站 HTML 超过 8 MiB")
                            chunks.append(chunk)
                        text = b"".join(chunks).decode("utf-8", errors="replace")
                        deadline.remaining()
                        self.check_page(text)
                        self.session.cookies.update(session.cookies)
                        return text
                except requests.exceptions.RequestException as error:
                    raise SourceError("source", "source_unavailable", "源站网络请求失败", True) from error
            raise SourceError("source", "source_unavailable", "源站重定向次数过多", True)

    @staticmethod
    def check_page(text):
        if ("Just a moment" in text and len(text) < 8000) or (
            "cf-chl-" in text and "/cdn-cgi/challenge-platform/" in text
        ):
            raise SourceError("source", "auth_required", "源站要求通过访问验证或提供有效 Cookie")
        if (re.search(r"请先登录|請先登錄|需要登录|需要登入|please\s+log\s*in", text, re.IGNORECASE)
                and not re.search(r"view_video\.php|<video\b|<source\b|strencode2\(", text)):
            raise SourceError("source", "auth_required", "源站要求登录或登录已失效")

    def next_page(self, text, page):
        if self.max_pages is not None and page >= self.max_pages:
            return None
        soup = BeautifulSoup(text, "lxml")
        later_pages = []
        for link in soup.select("a[href]"):
            if link.get("aria-disabled") == "true" or "disabled" in link.get("class", []):
                continue
            address = urlparse(urljoin(self.list_url, link["href"]))
            if address.path != urlparse(self.list_url).path:
                continue
            query = parse_qs(address.query)
            if query.get("category", [self.category])[0] != self.category:
                continue
            try:
                number = int(query.get("page", ["1"])[0])
            except ValueError:
                continue
            if number > page:
                later_pages.append(number)
        # 某些分页控件只显示“末页”；仍逐页推进，避免跳过中间候选。
        return page + 1 if later_pages else None

    def discover(self, cursor, limit, deadline):
        deadline.remaining()
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit 必须是 1–100 的整数")
        if cursor is None:
            page, offset = 1, 0
        else:
            protocol_identifier(cursor, "cursor")
            match = re.fullmatch(r"page:([1-9][0-9]*):(0|[1-9][0-9]*)", cursor)
            if not match:
                raise ValueError("无效的分页游标")
            page, offset = map(int, match.groups())
        if page not in self.pages:
            text = self.fetch_page(self.page_url(page), deadline, "source", self.list_url)
            rows = self.spider.parse_list_page(text, self.list_url)
            soup = BeautifulSoup(text, "lxml")
            if not rows and not (
                soup.select_one(".row, .pagination, #videobox")
                or re.search(r"暂无视频|暫無影片|没有视频|no\s+videos", soup.get_text(), re.IGNORECASE)
            ):
                raise SourceError("source", "parse_failed", "未识别到视频列表结构")
            previous_keys = {
                key for number, cached in self.pages.items() if number < page
                for key in cached["keys"]
            }
            items = []
            keys = set()
            for row in rows:
                viewkey = protocol_identifier(row["viewkey"], "viewkey")
                key = protocol_identifier("viewkey:" + viewkey, "discovery_key")
                keys.add(key)
                if key in previous_keys:
                    continue
                items.append({
                    "discovery_key": key, "source_id": viewkey,
                    "locator": {"viewkey": viewkey},
                })
            following = self.next_page(text, page) if items else None
            deadline.remaining()
            self.pages[page] = {"items": items, "keys": keys, "next_page": following}
            for row in rows:
                self.metadata.setdefault(row["viewkey"], row)
        cached = self.pages[page]
        if offset > len(cached["items"]):
            raise ValueError("分页游标偏移超出范围")
        selected = cached["items"][offset:offset + limit]
        end = offset + len(selected)
        next_cursor = (
            f"page:{page}:{end}" if end < len(cached["items"])
            else f"page:{cached['next_page']}:0" if cached["next_page"] is not None else None
        )
        deadline.remaining()
        return {"items": selected, "next_cursor": next_cursor}

    def media_object(self, url, referer):
        url = http_url(url)
        headers = {"User-Agent": self.session.headers["User-Agent"], "Referer": referer}
        headers.update(self.custom_headers)
        prepared = self.session.prepare_request(requests.Request("GET", url, headers=headers))
        if prepared.headers.get("Cookie"):
            headers["Cookie"] = prepared.headers["Cookie"]
        return {"type": "url", "url": url, "headers": protocol_headers(headers)}

    def resolve(self, candidate, deadline):
        deadline.remaining()
        key = protocol_identifier(candidate["discovery_key"], "discovery_key")
        viewkey = protocol_identifier(candidate["locator"]["viewkey"], "locator.viewkey")
        source_id = protocol_identifier(candidate.get("source_id", viewkey), "source_id")
        detail_url = urljoin(self.base_url, "view_video.php") + "?" + urlencode({"viewkey": viewkey})
        text = self.fetch_page(detail_url, deadline, "item", self.list_url)
        detail = self.spider.parse_detail_page(text, detail_url)
        if not detail.get("video_url"):
            visible_text = BeautifulSoup(text, "lxml").get_text(" ", strip=True)
            if re.search(r"视频不存在|影片不存在|视频已删除|video\s+(?:not\s+found|does\s+not\s+exist)",
                         visible_text, re.IGNORECASE):
                raise SourceError("item", "not_found", "该视频不存在或已被删除")
            raise SourceError("item", "parse_failed", "未能从详情页解析视频地址")
        row = self.metadata.get(viewkey, {})
        list_id, media_id = row.get("source_id"), detail.get("source_id")
        if list_id and media_id and list_id != media_id:
            raise SourceError("item", "parse_failed", "详情页媒体与列表视频不匹配")
        title = utf8_prefix(str(detail.get("title") or row.get("title") or "").strip(), 4096)
        if not title:
            raise SourceError("item", "parse_failed", "未能解析非空视频标题")
        result = {
            "discovery_key": key, "source_id": source_id, "title": title,
            "detail_url": detail_url,
            "media": self.media_object(detail["video_url"], detail_url),
        }
        thumbnail = detail.get("thumb_url") or row.get("thumb_url")
        if thumbnail:
            result["thumbnail"] = self.media_object(thumbnail, detail_url)
        deadline.remaining()
        return result


class ProtocolWriter:
    def __init__(self):
        self.total_bytes = 0

    def write(self, response):
        line = json.dumps(response, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
        size = len(line.encode("utf-8"))
        if size > 1024 * 1024 or self.total_bytes + size > 64 * 1024 * 1024:
            raise ValueError("协议输出超过大小限制")
        sys.stdout.write(line)
        sys.stdout.flush()
        self.total_bytes += size


def unique_json_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON 包含重复字段: {key}")
        result[key] = value
    return result


def load_protocol_json(text):
    def reject_constant(value):
        raise ValueError(f"JSON 包含非法常量: {value}")

    value = json.loads(text, object_pairs_hook=unique_json_keys, parse_constant=reject_constant)
    if not isinstance(value, dict):
        raise ValueError("协议 JSON 必须是对象")
    return value


def print_help():
    print("""
================================================
    91porn 视频爬虫 (crawler.v3)
================================================

本脚本将爬取 91porn 列表下的所有视频信息：
  - 视频名称
  - 封面图直链
  - 视频直链 (MP4)

依赖安装:
    pip install requests beautifulsoup4 lxml PySocks

使用方法:
    python 91Porn.py --job /absolute/path/to/job.json
    python 91Porn.py
    python 91Porn.py --category top

配置说明 (编辑脚本内 "配置区域"):
    MIN_PAGE_DELAY / MAX_PAGE_DELAY : 列表页请求间隔 (默认 3-6 秒)
    MIN_DETAIL_DELAY / MAX_DETAIL_DELAY : 详情页请求间隔 (默认 2-5 秒)
    MAX_PAGES : 限制最大爬取页数 (None=不限, 如 5=只爬前5页)
    OUTPUT_FILE : 输出文件名 (默认 91porn_videos.json)

按 Ctrl+C 可随时中断并保存已爬取的数据

注意:
    1. 视频直链包含时效性token，会过期，需定期重新爬取
    2. 脚本已内置随机延时，请勿移除，避免对服务器造成压力
    3. 如遇到Cloudflare拦截，需要先通过浏览器获取Cookie
    4. 本脚本仅供学习交流，请遵守当地法律法规
================================================
""")


def run_job(job_path: str):
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    with open(job_path, "r", encoding="utf-8") as file:
        job = load_protocol_json(file.read())
    if job.get("protocol") != CRAWLER_PROTOCOL:
        raise ValueError(f"不支持的协议，需要 {CRAWLER_PROTOCOL}")
    if job.get("feed_id") not in {feed["id"] for feed in json.loads(CRAWLER_FEEDS)}:
        raise ValueError("不支持的 feed_id")
    for field in ("task_id", "crawler_id"):
        if not isinstance(job.get(field), str) or not job[field].strip():
            raise ValueError(f"{field} 必须是非空字符串")
    if not isinstance(job.get("work_dir"), str) or not os.path.isabs(job["work_dir"]):
        raise ValueError("work_dir 必须是绝对路径")
    for field in ("config", "network"):
        if field not in job:
            job[field] = {}
        if not isinstance(job[field], dict):
            raise ValueError(f"{field} 必须是对象")
    proxy_url = job["network"].get("proxy_url")
    if proxy_url is not None and (not isinstance(proxy_url, str) or not proxy_url.strip()):
        raise ValueError("network.proxy_url 必须是非空字符串")
    if proxy_url:
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ[name] = proxy_url
        os.environ["NO_PROXY"] = ""
        os.environ["no_proxy"] = ""
    prefer_ipv4_for_plain_socks5_proxy()
    crawler = Porn91ProtocolCrawler(job)
    writer = ProtocolWriter()
    try:
        while True:
            line = sys.stdin.readline(1024 * 1024 + 1)
            if not line:
                raise EOFError("收到 stop 前 stdin 已关闭")
            if len(line.encode("utf-8")) > 1024 * 1024:
                raise ValueError("命令行超过 1 MiB")
            command = load_protocol_json(line)
            request_id = protocol_identifier(command.get("request_id"), "request_id")
            kind = command.get("type")
            response = {"request_id": request_id}
            if kind == "stop":
                writer.write({"type": "stopped", **response})
                return
            scope = "source" if kind == "discover" else "item"
            deadline = None
            try:
                deadline = CommandDeadline(command["deadline_at"], scope)
                if kind == "discover":
                    fields = run_before_deadline(
                        crawler.discover, deadline, command["cursor"], command["limit"],
                    )
                    response.update(type="page", **fields)
                elif kind == "resolve":
                    fields = run_before_deadline(crawler.resolve, deadline, command["candidate"])
                    response.update(type="item", **fields)
                else:
                    raise ValueError("未知命令类型")
            except SourceError as error:
                response.update(type="error", **error.fields)
            except Exception as error:
                traceback.print_exc(file=sys.stderr)
                response.update(
                    type="error", scope=scope, code="parse_failed",
                    message=utf8_prefix(str(error), 8192) or "解析失败", retryable=False,
                )
            finally:
                if deadline is not None:
                    deadline.cancelled.set()
            writer.write(response)
    finally:
        crawler.session.close()


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ('-h', '--help', 'help'):
        print_help()
        return

    parser = argparse.ArgumentParser(
        prog="spider_91porn.py",
        description="91porn 视频元数据爬虫",
        add_help=False,
    )
    parser.add_argument("--page", type=int, default=None,
                        help="只爬指定页（单页模式，配合 --output 用于定时任务）")
    parser.add_argument("--output", type=str, default=None,
                        help="输出 JSON 路径，覆盖默认 OUTPUT_FILE")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="单页模式下，从 --page 起最多再爬几页（默认 1）")
    parser.add_argument("--no-resume", action="store_true",
                        help="禁用断点续爬（单页模式默认禁用）")
    parser.add_argument("--quiet", action="store_true",
                        help="压缩日志，每条视频只输出关键事件")
    parser.add_argument("--target-new", type=int, default=None,
                        help="目标新增模式：从 page 1 起翻页直到累计处理这么多新源视频后停止（backend 凌晨任务用）")
    parser.add_argument("--seen-viewkeys-file", type=str, default=None,
                        help="文件路径，每行一个已处理过的 viewkey 或 mp4 源 ID；脚本会跳过这些视频")
    parser.add_argument("--stream-output", action="store_true",
                        help="流式模式：每解析一条视频直链就立即把它作为一行 JSON 写到 stdout 并 flush；"
                             "日志改走 stderr。项目协议集成请使用 --job。")
    parser.add_argument("--job", type=str, default=None,
                        help="crawler.v3 job JSON 路径；逐行读取 stdin 命令并响应。")
    parser.add_argument("--category", type=str, default=DEFAULT_CATEGORY,
                        help="分类，默认 top；例如 --category top")

    args, _ = parser.parse_known_args()
    if args.job:
        run_job(args.job)
        return

    cli_out = sys.stderr if args.stream_output else sys.stdout
    prefer_ipv4_for_plain_socks5_proxy()

    print("""
================================================
    91porn 视频爬虫启动中...
================================================
按 Ctrl+C 可随时中断并保存进度
""", file=cli_out)

    seen_viewkeys = []
    if args.seen_viewkeys_file:
        try:
            with open(args.seen_viewkeys_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        seen_viewkeys.append(line)
        except FileNotFoundError:
            print(f"警告: --seen-viewkeys-file 不存在: {args.seen_viewkeys_file}", file=cli_out)
        except Exception as e:
            print(f"警告: 读取 --seen-viewkeys-file 失败: {e}", file=cli_out)

    if args.target_new is not None:
        spider = Porn91Spider(
            output_file=args.output,
            start_page=1,
            max_pages=None,
            resume=False,
            quiet=args.quiet,
            target_new=args.target_new,
            seen_viewkeys=seen_viewkeys,
            stream_output=args.stream_output,
            category=args.category,
        )
    elif args.page is not None:
        start_page = max(1, args.page)
        max_pages = args.max_pages if args.max_pages and args.max_pages > 0 else 1
        spider = Porn91Spider(
            output_file=args.output,
            start_page=start_page,
            max_pages=max_pages,
            resume=False,
            quiet=args.quiet,
            seen_viewkeys=seen_viewkeys,
            stream_output=args.stream_output,
            category=args.category,
        )
    else:
        spider = Porn91Spider(
            output_file=args.output,
            resume=False if args.no_resume else None,
            quiet=args.quiet,
            seen_viewkeys=seen_viewkeys,
            stream_output=args.stream_output,
            category=args.category,
        )

    try:
        spider.crawl()
    except KeyboardInterrupt:
        spider.log("\n用户中断，正在保存已爬取的数据...")
        spider._save_results()
        spider._print_summary()
        sys.exit(0)
    except Exception as e:
        spider.log(f"发生未预料的错误: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        spider._save_results()
        raise


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        # 避免解释器退出时再次向已关闭的 stdout 刷新。
        sys.stdout = open(os.devnull, "w")
        sys.exit(0)
